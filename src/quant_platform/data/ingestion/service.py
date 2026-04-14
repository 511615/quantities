from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_platform.common.hashing.digest import file_digest
from quant_platform.data.connectors import (
    BinanceSpotKlinesConnector,
    BitstampArchiveConnector,
    ContractOnlyConnector,
    DefiLlamaConnector,
    FredSeriesConnector,
    GdeltSentimentConnector,
    GNewsSentimentConnector,
    InternalSmokeMarketConnector,
    NewsArchiveSentimentConnector,
    RedditArchiveSentimentConnector,
)
from quant_platform.data.contracts.ingestion import (
    DataConnectorError,
    DataConnector,
    DomainIngestionService,
    IngestionRequest,
)
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.data.contracts.series import NormalizedSeriesPoint

_GNEWS_RSS_MAX_LOOKBACK_DAYS = 7
_REDDIT_PUBLIC_MAX_LOOKBACK_DAYS = 31
_SENTIMENT_VENDOR_ALIASES: dict[str, str] = {
    "reddit_history_csv": "reddit_archive",
    "reddit_pullpush": "reddit_archive",
    "reddit_public": "reddit_archive",
}


@dataclass(frozen=True)
class CacheSnapshot:
    snapshot_id: int
    data_domain: str
    vendor: str
    identifier: str
    frequency: str
    start_time: str
    end_time: str
    raw_uri: str
    normalized_uri: str
    status: str


class DomainIngestionCoordinator(DomainIngestionService):
    def __init__(self, artifact_root: Path, *, register_defaults: bool = True) -> None:
        self.artifact_root = artifact_root.resolve()
        self.cache_root = self.artifact_root / "data_cache"
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.cache_root / "ingestion.sqlite3"
        self._connectors: dict[tuple[str, str], DataConnector] = {}
        self._init_db()
        if register_defaults:
            self._register_defaults()

    def register(self, connector: DataConnector) -> None:
        key = (
            connector.registration.data_domain,
            self._normalize_vendor(connector.registration.data_domain, connector.registration.vendor),
        )
        self._connectors[key] = connector

    def resolve(self, data_domain: str, vendor: str) -> DataConnector | None:
        return self._connectors.get((data_domain, self._normalize_vendor(data_domain, vendor)))

    def fetch_market_bars(
        self,
        *,
        symbol: str,
        vendor: str,
        exchange: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[list[NormalizedMarketBar], str]:
        rows, status = self._load_or_ingest(
            data_domain="market",
            vendor=vendor,
            identifier=symbol.upper(),
            frequency=frequency,
            start_time=start_time,
            end_time=end_time,
            options={"exchange": exchange},
            model_key="rows",
        )
        return [NormalizedMarketBar.model_validate(item) for item in rows], status

    def fetch_series_points(
        self,
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, Any] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        rows, status = self._load_or_ingest(
            data_domain=data_domain,
            vendor=vendor,
            identifier=identifier,
            frequency=frequency,
            start_time=start_time,
            end_time=end_time,
            options=options or {},
            model_key="rows",
        )
        return [NormalizedSeriesPoint.model_validate(item) for item in rows], status

    def _load_or_ingest(
        self,
        *,
        data_domain: str,
        vendor: str,
        identifier: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, Any],
        model_key: str,
    ) -> tuple[list[dict[str, Any]], str]:
        vendor = self._normalize_vendor(data_domain, vendor)
        self._enforce_vendor_window_policy(
            data_domain=data_domain,
            vendor=vendor,
            identifier=identifier,
            start_time=start_time,
            end_time=end_time,
        )
        connector = self.resolve(data_domain, vendor)
        if connector is None:
            raise DataConnectorError(
                data_domain=data_domain,  # type: ignore[arg-type]
                vendor=vendor,
                identifier=identifier,
                message="No connector registered for this domain/vendor.",
                retryable=False,
                code="connector_not_registered",
            )
        existing = self._list_snapshots(
            data_domain=data_domain,
            vendor=vendor,
            identifier=identifier,
            frequency=frequency,
        )
        missing_ranges = self._missing_ranges(start_time, end_time, existing)
        fetch_status = "cache_hit"
        if missing_ranges:
            fetch_status = "live_fetch" if not existing else "incremental_refresh"
            for gap_start, gap_end in missing_ranges:
                request = IngestionRequest(
                    data_domain=data_domain,
                    vendor=vendor,
                    request_id=f"{data_domain}-{vendor}-{identifier}-{gap_start.isoformat()}",
                    time_range={"start": gap_start, "end": gap_end},
                    identifiers=[identifier],
                    frequency=frequency,
                    options=options,
                )
                try:
                    result = connector.ingest(request)
                except DataConnectorError as exc:
                    if data_domain == "sentiment_events" and getattr(exc, "code", None) == "empty_result":
                        self._persist_snapshot(
                            data_domain=data_domain,
                            vendor=vendor,
                            identifier=identifier,
                            frequency=frequency,
                            start_time=gap_start,
                            end_time=gap_end,
                            raw_payload={
                                "identifier": identifier,
                                "frequency": frequency,
                                "rows": [],
                                "error_message": str(exc),
                            },
                            normalized_rows=[],
                        )
                        continue
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise DataConnectorError(
                        data_domain=data_domain,  # type: ignore[arg-type]
                        vendor=vendor,
                        identifier=identifier,
                        message=f"Ingestion failed: {exc}",
                        retryable=True,
                        code="connector_ingest_failed",
                    ) from exc
                rows = result.metadata.get(model_key)
                if not isinstance(rows, list) or not rows:
                    if data_domain == "sentiment_events":
                        self._persist_snapshot(
                            data_domain=data_domain,
                            vendor=vendor,
                            identifier=identifier,
                            frequency=frequency,
                            start_time=gap_start,
                            end_time=gap_end,
                            raw_payload=result.metadata,
                            normalized_rows=[],
                        )
                        continue
                    raise DataConnectorError(
                        data_domain=data_domain,  # type: ignore[arg-type]
                        vendor=vendor,
                        identifier=identifier,
                        message=(
                            "Connector returned no rows and no synthetic fallback is allowed."
                        ),
                        retryable=False,
                        code="empty_result",
                    )
                self._persist_snapshot(
                    data_domain=data_domain,
                    vendor=vendor,
                    identifier=identifier,
                    frequency=frequency,
                    start_time=gap_start,
                    end_time=gap_end,
                    raw_payload=result.metadata,
                    normalized_rows=rows,
                )
            existing = self._list_snapshots(
                data_domain=data_domain,
                vendor=vendor,
                identifier=identifier,
                frequency=frequency,
            )
        rows = self._load_rows(existing, start_time=start_time, end_time=end_time)
        if not rows:
            raise DataConnectorError(
                data_domain=data_domain,  # type: ignore[arg-type]
                vendor=vendor,
                identifier=identifier,
                message="No cached rows exist for the requested time range.",
                retryable=False,
                code="cache_miss_after_ingest",
            )
        return rows, fetch_status

    def _normalize_vendor(self, data_domain: str, vendor: str) -> str:
        if data_domain != "sentiment_events":
            return vendor
        return _SENTIMENT_VENDOR_ALIASES.get(vendor, vendor)

    def _vendor_lookup_values(self, data_domain: str, vendor: str) -> list[str]:
        normalized = self._normalize_vendor(data_domain, vendor)
        if data_domain != "sentiment_events" or normalized != "reddit_archive":
            return [normalized]
        return [normalized, *sorted(alias for alias, target in _SENTIMENT_VENDOR_ALIASES.items() if target == normalized)]

    def _enforce_vendor_window_policy(
        self,
        *,
        data_domain: str,
        vendor: str,
        identifier: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        if data_domain != "sentiment_events":
            return
        span_days = (end_time - start_time).total_seconds() / 86400
        if vendor == "gnews" and not os.getenv("GNEWS_API_KEY", "").strip():
            if span_days > _GNEWS_RSS_MAX_LOOKBACK_DAYS:
                raise DataConnectorError(
                    data_domain=data_domain,
                    vendor=vendor,
                    identifier=identifier,
                    message=(
                        "Google News RSS fallback is near-real-time only and cannot satisfy historical "
                        "ranges longer than 7 days. Existing cached snapshots from older runs are no longer "
                        "accepted for this request. Configure GNEWS_API_KEY for historical coverage."
                    ),
                    retryable=False,
                    code="historical_range_not_supported",
                )
        if vendor == "reddit_public" and span_days > _REDDIT_PUBLIC_MAX_LOOKBACK_DAYS:
            raise DataConnectorError(
                data_domain=data_domain,
                vendor=vendor,
                identifier=identifier,
                message=(
                    "reddit_public currently exposes only recent public search windows up to about 31 days. "
                    "Existing cached snapshots from older runs are no longer accepted for this request."
                ),
                retryable=False,
                code="historical_range_not_supported",
            )

    def _persist_snapshot(
        self,
        *,
        data_domain: str,
        vendor: str,
        identifier: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        raw_payload: dict[str, Any],
        normalized_rows: list[dict[str, Any]],
    ) -> None:
        if data_domain == "sentiment_events" and not normalized_rows:
            return
        slug = self._slugify(identifier)
        prefix = f"{data_domain}/{vendor}/{slug}/{frequency}_{start_time:%Y%m%d%H%M%S}_{end_time:%Y%m%d%H%M%S}"
        raw_path = self.cache_root / f"raw/{prefix}.json"
        normalized_path = self.cache_root / f"normalized/{prefix}.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(raw_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        normalized_path.write_text(
            json.dumps({"rows": normalized_rows}, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        actual_start, actual_end = self._rows_bounds(normalized_rows)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (
                    data_domain, vendor, identifier, frequency, start_time, end_time,
                    raw_uri, normalized_uri, content_hash, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data_domain,
                    vendor,
                    identifier,
                    frequency,
                    actual_start.isoformat() if actual_start else start_time.isoformat(),
                    actual_end.isoformat() if actual_end else end_time.isoformat(),
                    str(raw_path.resolve()),
                    str(normalized_path.resolve()),
                    file_digest(normalized_path),
                    "success",
                    datetime.now(UTC).isoformat(),
                ),
            )
            conn.commit()

    def _list_snapshots(
        self,
        *,
        data_domain: str,
        vendor: str,
        identifier: str,
        frequency: str,
    ) -> list[CacheSnapshot]:
        lookup_vendors = self._vendor_lookup_values(data_domain, vendor)
        vendor_placeholders = ", ".join("?" for _ in lookup_vendors)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, data_domain, vendor, identifier, frequency, start_time, end_time,
                       raw_uri, normalized_uri, status
                FROM snapshots
                WHERE data_domain = ? AND vendor IN ({vendor_placeholders}) AND identifier = ? AND frequency = ?
                  AND status = 'success'
                ORDER BY start_time ASC
                """,
                (data_domain, *lookup_vendors, identifier, frequency),
            ).fetchall()
        snapshots = [CacheSnapshot(*row) for row in rows]
        return [snapshot for snapshot in snapshots if self._snapshot_is_usable(snapshot)]

    def _load_rows(
        self,
        snapshots: list[CacheSnapshot],
        *,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for snapshot in snapshots:
            payload = json.loads(Path(snapshot.normalized_uri).read_text(encoding="utf-8"))
            rows = payload.get("rows", [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                event_time = self._parse_dt(row.get("event_time"))
                if event_time is None or event_time < start_time or event_time > end_time:
                    continue
                key = (
                    str(row.get("symbol") or row.get("series_key") or row.get("entity_key") or ""),
                    event_time.isoformat(),
                )
                by_key[key] = row
        return [by_key[key] for key in sorted(by_key, key=lambda item: item[1])]

    def _missing_ranges(
        self,
        start_time: datetime,
        end_time: datetime,
        snapshots: list[CacheSnapshot],
    ) -> list[tuple[datetime, datetime]]:
        intervals: list[tuple[datetime, datetime]] = []
        for snapshot in snapshots:
            snap_start, snap_end = self._snapshot_bounds(snapshot)
            if snap_start is None or snap_end is None:
                continue
            if snap_end < start_time or snap_start > end_time:
                continue
            intervals.append((max(start_time, snap_start), min(end_time, snap_end)))
        if not intervals:
            return [(start_time, end_time)]
        intervals.sort(key=lambda item: item[0])
        missing: list[tuple[datetime, datetime]] = []
        cursor = start_time
        for interval_start, interval_end in intervals:
            if interval_start > cursor:
                missing.append((cursor, interval_start))
            if interval_end > cursor:
                cursor = interval_end
        if cursor < end_time:
            missing.append((cursor, end_time))
        return [(gap_start, gap_end) for gap_start, gap_end in missing if gap_end > gap_start]

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_domain TEXT NOT NULL,
                    vendor TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    raw_uri TEXT NOT NULL,
                    normalized_uri TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _register_defaults(self) -> None:
        self.register(BinanceSpotKlinesConnector())
        self.register(BitstampArchiveConnector())
        self.register(InternalSmokeMarketConnector())
        self.register(FredSeriesConnector())
        self.register(DefiLlamaConnector())
        self.register(GdeltSentimentConnector())
        self.register(GNewsSentimentConnector())
        self.register(NewsArchiveSentimentConnector())
        self.register(RedditArchiveSentimentConnector(self.artifact_root))
        self.register(ContractOnlyConnector(data_domain="derivatives"))
        self.register(
            ContractOnlyConnector(data_domain="sentiment_events", vendor="contract_only")
        )

    def _snapshot_bounds(self, snapshot: CacheSnapshot) -> tuple[datetime | None, datetime | None]:
        payload = json.loads(Path(snapshot.normalized_uri).read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            return self._parse_dt(snapshot.start_time), self._parse_dt(snapshot.end_time)
        bounds = self._rows_bounds(rows)
        if bounds == (None, None):
            return self._parse_dt(snapshot.start_time), self._parse_dt(snapshot.end_time)
        return bounds

    def _snapshot_is_usable(self, snapshot: CacheSnapshot) -> bool:
        normalized_path = Path(snapshot.normalized_uri)
        if not normalized_path.exists():
            return False
        if snapshot.data_domain == "sentiment_events" and snapshot.vendor == "gnews":
            try:
                text = normalized_path.read_text(encoding="utf-8")
            except OSError:
                return False
            if "<a href=" in text or "&nbsp;" in text:
                return False
        return True

    def _rows_bounds(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[datetime | None, datetime | None]:
        event_times = [
            parsed
            for parsed in (self._parse_dt(row.get("event_time")) for row in rows if isinstance(row, dict))
            if parsed is not None
        ]
        if not event_times:
            return None, None
        return min(event_times), max(event_times)

    @staticmethod
    def _parse_dt(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _slugify(value: str) -> str:
        return "".join(ch.lower() if ch.isalnum() else "_" for ch in value).strip("_")
