from __future__ import annotations

import csv
import json
import os
from datetime import UTC, datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from quant_platform.data.contracts.ingestion import (
    DataConnectorError,
    ConnectorRegistration,
    DataConnector,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.market import NormalizedMarketBar


class BinanceSpotKlinesConnector(DataConnector):
    BASE_URL = "https://api.binance.com"

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="binance",
            display_name="Binance Spot REST klines",
            capabilities=["historical_klines", "incremental_fetch"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = self._require_identifier(request)
        interval = request.frequency or "1h"
        rows = self._fetch_klines(
            symbol=symbol,
            interval=interval,
            start_time=request.time_range.start,
            end_time=request.time_range.end,
        )
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri="",
            normalized_uri="",
            coverage=IngestionCoverage(
                start_time=(rows[0].event_time if rows else None),
                end_time=(rows[-1].event_time if rows else None),
                complete=bool(rows),
            ),
            metadata={
                "symbol": symbol,
                "exchange": request.options.get("exchange", "binance"),
                "frequency": interval,
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )

    def _fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[NormalizedMarketBar]:
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        cursor = start_ms
        items: list[NormalizedMarketBar] = []
        while cursor < end_ms:
            query = urlencode(
                {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                }
            )
            payload = self._request_json(f"{self.base_url}/api/v3/klines?{query}")
            if not isinstance(payload, list):
                raise DataConnectorError(
                    data_domain="market",
                    vendor="binance",
                    identifier=symbol.upper(),
                    message="Binance klines response is not a list.",
                    retryable=True,
                    code="invalid_payload",
                )
            if not payload:
                break
            batch = [self._row_to_bar(symbol.upper(), row) for row in payload if isinstance(row, list)]
            items.extend(batch)
            last_open_time = int(payload[-1][0])
            if last_open_time < cursor:
                raise DataConnectorError(
                    data_domain="market",
                    vendor="binance",
                    identifier=symbol.upper(),
                    message="Binance klines cursor did not advance.",
                    retryable=True,
                    code="cursor_stalled",
                )
            cursor = last_open_time + 1
            if len(payload) < 1000:
                break
        items = [row for row in items if start_time <= row.event_time <= end_time]
        items.sort(key=lambda row: row.event_time)
        return items

    def _request_json(self, url: str) -> object:
        with urlopen(url, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def _row_to_bar(self, symbol: str, row: list[object]) -> NormalizedMarketBar:
        event_time = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
        close_time = datetime.fromtimestamp(int(row[6]) / 1000, tz=UTC)
        return NormalizedMarketBar(
            event_time=event_time,
            available_time=max(event_time, close_time),
            symbol=symbol,
            venue="binance",
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="market",
                vendor="binance",
                message="Binance connector requires at least one symbol identifier.",
                retryable=False,
                code="identifier_required",
            )
        return request.identifiers[0]


class InternalSmokeMarketConnector(DataConnector):
    def __init__(self) -> None:
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="internal_smoke",
            display_name="Internal smoke market bars",
            capabilities=["deterministic_smoke_data"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = request.identifiers[0] if request.identifiers else "BTCUSDT"
        rows = self._build_rows(symbol.upper(), request.time_range.start, request.time_range.end)
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri="",
            normalized_uri="",
            coverage=IngestionCoverage(
                start_time=(rows[0].event_time if rows else None),
                end_time=(rows[-1].event_time if rows else None),
                complete=bool(rows),
            ),
            metadata={
                "symbol": symbol.upper(),
                "exchange": request.options.get("exchange", "binance"),
                "frequency": request.frequency or "1h",
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )

    def _build_rows(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[NormalizedMarketBar]:
        base = datetime(2024, 1, 1, tzinfo=UTC)
        factor = 1.0 + (sum(ord(ch) for ch in symbol) % 7) / 20
        rows: list[NormalizedMarketBar] = []
        previous_close = 100.0 * factor
        for index in range(120):
            event_time = base.replace(day=base.day + index // 24, hour=index % 24)
            if event_time < start_time or event_time > end_time:
                continue
            wave = ((index % 9) - 4) * 0.35
            regime = 0.2 if (index // 24) % 2 == 0 else -0.1
            close = max(50.0, previous_close + wave + regime)
            open_price = previous_close
            rows.append(
                NormalizedMarketBar(
                    event_time=event_time,
                    available_time=event_time,
                    symbol=symbol,
                    venue="binance",
                    open=open_price,
                    high=max(open_price, close) + 0.5,
                    low=min(open_price, close) - 0.5,
                    close=close,
                    volume=(10.0 + (index % 7) * 1.3) * factor,
                )
            )
            previous_close = close
        return rows


class BitstampArchiveConnector(DataConnector):
    DEFAULT_ARCHIVE_URL = "https://www.cryptodatadownload.com/cdd/Bitstamp_BTCUSD_1h.csv"
    ENV_ARCHIVE_URL = "QUANT_PLATFORM_BITSTAMP_ARCHIVE_URL"

    def __init__(self, archive_url: str | None = None) -> None:
        self.archive_url = (
            archive_url
            or os.getenv(self.ENV_ARCHIVE_URL, "").strip()
            or self.DEFAULT_ARCHIVE_URL
        )
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="bitstamp_archive",
            display_name="Bitstamp BTC/USD historical archive",
            capabilities=["historical_ohlcv", "real_market_bars"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = self._require_identifier(request)
        normalized_symbol = symbol.upper().replace("/", "")
        if normalized_symbol not in {"BTCUSD", "BTCUSDT"}:
            raise DataConnectorError(
                data_domain="market",
                vendor="bitstamp_archive",
                identifier=symbol,
                message="bitstamp_archive currently supports only BTCUSD/BTCUSDT.",
                retryable=False,
                code="unsupported_symbol",
            )
        frequency = (request.frequency or "1h").lower()
        if frequency != "1h":
            raise DataConnectorError(
                data_domain="market",
                vendor="bitstamp_archive",
                identifier=symbol,
                message="bitstamp_archive v1 currently supports only 1h frequency.",
                retryable=False,
                code="unsupported_frequency",
            )
        rows = self._fetch_archive_rows(
            start_time=request.time_range.start,
            end_time=request.time_range.end,
        )
        if not rows:
            raise DataConnectorError(
                data_domain="market",
                vendor="bitstamp_archive",
                identifier=symbol,
                message="No Bitstamp archive bars matched the requested time range.",
                retryable=False,
                code="empty_result",
            )
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri=self.archive_url,
            normalized_uri=self.archive_url,
            coverage=IngestionCoverage(
                start_time=rows[0].event_time,
                end_time=rows[-1].event_time,
                complete=True,
            ),
            metadata={
                "symbol": "BTCUSD",
                "exchange": request.options.get("exchange", "bitstamp"),
                "frequency": frequency,
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )

    def _fetch_archive_rows(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
    ) -> list[NormalizedMarketBar]:
        request = Request(
            self.archive_url,
            headers={"User-Agent": "quant-platform/0.1 (+https://localhost)"},
        )
        rows: list[NormalizedMarketBar] = []
        with urlopen(request, timeout=60) as response:
            decoded_lines = (
                line.decode("utf-8", errors="replace")
                for line in response.readlines()
            )
            reader = csv.DictReader(line for line in decoded_lines if not line.startswith("https://"))
            for row in reader:
                event_time = self._parse_event_time(row.get("date"), row.get("unix"))
                if event_time is None:
                    continue
                if event_time < start_time or event_time > end_time:
                    continue
                try:
                    rows.append(
                        NormalizedMarketBar(
                            event_time=event_time,
                            available_time=event_time,
                            symbol="BTCUSD",
                            venue="bitstamp",
                            open=float(row.get("open", 0.0) or 0.0),
                            high=float(row.get("high", 0.0) or 0.0),
                            low=float(row.get("low", 0.0) or 0.0),
                            close=float(row.get("close", 0.0) or 0.0),
                            volume=float(row.get("Volume BTC", 0.0) or 0.0),
                        )
                    )
                except (TypeError, ValueError) as exc:
                    raise DataConnectorError(
                        data_domain="market",
                        vendor="bitstamp_archive",
                        message=f"Invalid Bitstamp archive row: {exc}",
                        retryable=False,
                        code="invalid_payload",
                    ) from exc
        rows.sort(key=lambda item: item.event_time)
        return rows

    @staticmethod
    def _parse_event_time(date_value: object, unix_value: object) -> datetime | None:
        if isinstance(date_value, str) and date_value.strip():
            try:
                return datetime.fromisoformat(date_value.strip()).replace(tzinfo=UTC)
            except ValueError:
                pass
        if isinstance(unix_value, str) and unix_value.strip():
            try:
                return datetime.fromtimestamp(int(unix_value.strip()), tz=UTC)
            except ValueError:
                return None
        return None

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="market",
                vendor="bitstamp_archive",
                message="Bitstamp archive connector requires at least one symbol identifier.",
                retryable=False,
                code="identifier_required",
            )
        return request.identifiers[0]
