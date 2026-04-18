from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen

from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    DataConnectorError,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.series import NormalizedSeriesPoint


class BinanceFuturesMetricsConnector(DataConnector):
    BASE_URL = "https://fapi.binance.com"
    PAGE_LIMIT = 500
    DEFAULT_METRICS = (
        "funding_rate",
        "open_interest",
        "global_long_short_ratio",
        "taker_buy_sell_ratio",
    )

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.registration = ConnectorRegistration(
            data_domain="derivatives",
            vendor="binance_futures",
            display_name="Binance USD-M futures market metrics",
            capabilities=[
                "funding_rate_history",
                "open_interest_history",
                "global_long_short_ratio",
                "taker_buy_sell_ratio",
            ],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = self._require_identifier(request).upper()
        frequency = (request.frequency or "1h").lower()
        if frequency != "1h":
            raise DataConnectorError(
                data_domain="derivatives",
                vendor="binance_futures",
                identifier=symbol,
                message="binance_futures currently supports only 1h snapshots.",
                retryable=False,
                code="unsupported_frequency",
            )
        metrics = request.options.get("metrics")
        resolved_metrics = (
            [metric for metric in metrics if isinstance(metric, str)]
            if isinstance(metrics, list)
            else list(self.DEFAULT_METRICS)
        )
        rows: list[NormalizedSeriesPoint] = []
        for metric_name in resolved_metrics:
            rows.extend(
                self._fetch_metric_rows(
                    symbol=symbol,
                    metric_name=metric_name,
                    start_time=request.time_range.start,
                    end_time=request.time_range.end,
                    frequency=frequency,
                )
            )
        rows.sort(key=lambda item: (item.event_time, item.metric_name))
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
                "identifier": symbol,
                "frequency": frequency,
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )

    def _fetch_metric_rows(
        self,
        *,
        symbol: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
        frequency: str,
    ) -> list[NormalizedSeriesPoint]:
        rows: list[NormalizedSeriesPoint] = []
        seen_keys: set[tuple[str, str]] = set()
        cursor_start = start_time
        while cursor_start <= end_time:
            endpoint, params = self._endpoint_for_metric(
                symbol=symbol,
                metric_name=metric_name,
                start_time=cursor_start,
                end_time=end_time,
            )
            with urlopen(f"{self.base_url}{endpoint}?{urlencode(params)}", timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, list):
                raise DataConnectorError(
                    data_domain="derivatives",
                    vendor="binance_futures",
                    identifier=symbol,
                    message=f"Unexpected payload for metric '{metric_name}'.",
                    retryable=True,
                    code="invalid_payload",
                )
            batch_max_time: datetime | None = None
            batch_size = 0
            for item in payload:
                if not isinstance(item, dict):
                    continue
                parsed = self._parse_metric_row(
                    symbol=symbol,
                    metric_name=metric_name,
                    item=item,
                    frequency=frequency,
                )
                if parsed is None or parsed.event_time < start_time or parsed.event_time > end_time:
                    continue
                batch_size += 1
                batch_max_time = (
                    parsed.event_time
                    if batch_max_time is None or parsed.event_time > batch_max_time
                    else batch_max_time
                )
                dedupe_key = (parsed.metric_name, parsed.event_time.isoformat())
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                rows.append(parsed)
            if batch_max_time is None:
                break
            next_cursor = batch_max_time + timedelta(milliseconds=1)
            if next_cursor <= cursor_start:
                break
            cursor_start = next_cursor
            if batch_size == 0:
                break
        rows.sort(key=lambda item: item.event_time)
        return rows

    def _endpoint_for_metric(
        self,
        *,
        symbol: str,
        metric_name: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[str, dict[str, object]]:
        params: dict[str, object] = {
            "symbol": symbol,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": self.PAGE_LIMIT,
        }
        if metric_name == "funding_rate":
            return "/fapi/v1/fundingRate", params
        params["period"] = "1h"
        if metric_name == "open_interest":
            return "/futures/data/openInterestHist", params
        if metric_name == "global_long_short_ratio":
            return "/futures/data/globalLongShortAccountRatio", params
        if metric_name == "taker_buy_sell_ratio":
            return "/futures/data/takerlongshortRatio", params
        raise DataConnectorError(
            data_domain="derivatives",
            vendor="binance_futures",
            identifier=symbol,
            message=f"Unsupported derivatives metric '{metric_name}'.",
            retryable=False,
            code="unsupported_metric",
        )

    def _parse_metric_row(
        self,
        *,
        symbol: str,
        metric_name: str,
        item: dict[str, object],
        frequency: str,
    ) -> NormalizedSeriesPoint | None:
        raw_timestamp = item.get("fundingTime") or item.get("timestamp") or item.get("time")
        if not isinstance(raw_timestamp, (int, float, str)):
            return None
        try:
            timestamp = int(float(raw_timestamp))
        except ValueError:
            return None
        event_time = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
        value_keys = {
            "funding_rate": ("fundingRate",),
            "open_interest": ("sumOpenInterest", "sumOpenInterestValue", "openInterest"),
            "global_long_short_ratio": ("longShortRatio",),
            "taker_buy_sell_ratio": ("buySellRatio",),
        }
        value = None
        for candidate in value_keys.get(metric_name, ()):
            raw_value = item.get(candidate)
            if isinstance(raw_value, (int, float, str)):
                try:
                    value = float(raw_value)
                    break
                except ValueError:
                    continue
        if value is None:
            return None
        return NormalizedSeriesPoint(
            event_time=event_time,
            available_time=event_time,
            series_key=f"{symbol}:{metric_name}",
            entity_key=symbol,
            domain="derivatives",
            vendor="binance_futures",
            metric_name=metric_name,
            frequency=frequency,
            value=value,
            dimensions={"symbol": symbol, "metric_name": metric_name},
        )

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="derivatives",
                vendor="binance_futures",
                message="binance_futures requires at least one perpetual symbol identifier.",
                retryable=False,
                code="identifier_required",
            )
        return str(request.identifiers[0]).strip()
