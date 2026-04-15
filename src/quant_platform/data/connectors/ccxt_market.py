from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    DataConnectorError,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.market import NormalizedMarketBar

_FREQUENCY_DELTAS: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
}


class CcxtMarketConnector(DataConnector):
    def __init__(
        self,
        exchange_factory: Callable[[str, dict[str, Any]], Any] | None = None,
    ) -> None:
        self._exchange_factory = exchange_factory
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="ccxt",
            display_name="CCXT market OHLCV",
            capabilities=["historical_ohlcv", "incremental_fetch", "multi_exchange"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = self._require_identifier(request)
        exchange_id = self._require_exchange(request)
        timeframe = self._normalize_timeframe(request.frequency or "1h")
        market_type = str(request.options.get("market_type", "spot")).strip().lower() or "spot"
        rows = self._fetch_ohlcv(
            exchange_id=exchange_id,
            symbol=symbol,
            timeframe=timeframe,
            market_type=market_type,
            start_time=request.time_range.start,
            end_time=request.time_range.end,
            limit=int(request.options.get("limit", 1000) or 1000),
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
                "exchange": exchange_id,
                "market_type": market_type,
                "frequency": timeframe,
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )

    def _fetch_ohlcv(
        self,
        *,
        exchange_id: str,
        symbol: str,
        timeframe: str,
        market_type: str,
        start_time: datetime,
        end_time: datetime,
        limit: int,
    ) -> list[NormalizedMarketBar]:
        exchange = self._build_exchange(exchange_id=exchange_id, market_type=market_type)
        since_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        step = self._timeframe_delta(timeframe)
        rows: list[NormalizedMarketBar] = []
        cursor = since_ms
        while cursor <= end_ms:
            try:
                payload = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
            except Exception as exc:  # noqa: BLE001
                raise DataConnectorError(
                    data_domain="market",
                    vendor="ccxt",
                    identifier=f"{exchange_id}:{symbol}",
                    message=f"CCXT fetch_ohlcv failed: {exc}",
                    retryable=True,
                    code="connector_ingest_failed",
                ) from exc
            if not isinstance(payload, list):
                raise DataConnectorError(
                    data_domain="market",
                    vendor="ccxt",
                    identifier=f"{exchange_id}:{symbol}",
                    message="CCXT fetch_ohlcv response is not a list.",
                    retryable=True,
                    code="invalid_payload",
                )
            if not payload:
                break
            batch = [
                self._row_to_bar(exchange_id=exchange_id, symbol=symbol, timeframe=timeframe, row=row)
                for row in payload
                if isinstance(row, list) and len(row) >= 6
            ]
            rows.extend(batch)
            if batch and batch[-1].available_time >= end_time:
                break
            last_open_time = int(payload[-1][0])
            next_cursor = last_open_time + int(step.total_seconds() * 1000)
            if next_cursor <= cursor:
                raise DataConnectorError(
                    data_domain="market",
                    vendor="ccxt",
                    identifier=f"{exchange_id}:{symbol}",
                    message="CCXT OHLCV pagination cursor did not advance.",
                    retryable=True,
                    code="cursor_stalled",
                )
            cursor = next_cursor
        filtered = [row for row in rows if start_time <= row.event_time <= end_time]
        filtered.sort(key=lambda item: item.event_time)
        return filtered

    def _build_exchange(self, *, exchange_id: str, market_type: str) -> Any:
        if self._exchange_factory is not None:
            return self._exchange_factory(exchange_id, {"market_type": market_type})
        try:
            ccxt = importlib.import_module("ccxt")
        except ModuleNotFoundError as exc:
            raise DataConnectorError(
                data_domain="market",
                vendor="ccxt",
                message="CCXT support requires the optional 'exchange' dependencies.",
                retryable=False,
                code="dependency_missing",
            ) from exc
        exchange_cls = getattr(ccxt, exchange_id, None)
        if exchange_cls is None:
            raise DataConnectorError(
                data_domain="market",
                vendor="ccxt",
                identifier=exchange_id,
                message="Unsupported CCXT exchange id.",
                retryable=False,
                code="unsupported_exchange",
            )
        init_kwargs: dict[str, Any] = {"enableRateLimit": True}
        default_type = self._default_type(market_type)
        if default_type is not None:
            init_kwargs["options"] = {"defaultType": default_type}
        return exchange_cls(init_kwargs)

    def _row_to_bar(
        self,
        *,
        exchange_id: str,
        symbol: str,
        timeframe: str,
        row: list[object],
    ) -> NormalizedMarketBar:
        event_time = datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC)
        close_time = event_time + self._timeframe_delta(timeframe)
        return NormalizedMarketBar(
            event_time=event_time,
            available_time=max(event_time, close_time),
            symbol=symbol,
            venue=exchange_id,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )

    @staticmethod
    def _default_type(market_type: str) -> str | None:
        mapping = {
            "spot": "spot",
            "future": "future",
            "futures": "future",
            "swap": "swap",
            "margin": "margin",
        }
        return mapping.get(market_type)

    @staticmethod
    def _normalize_timeframe(value: str) -> str:
        timeframe = value.strip().lower()
        if timeframe not in _FREQUENCY_DELTAS:
            raise DataConnectorError(
                data_domain="market",
                vendor="ccxt",
                identifier=timeframe,
                message="Unsupported CCXT timeframe.",
                retryable=False,
                code="unsupported_frequency",
            )
        return timeframe

    @staticmethod
    def _timeframe_delta(timeframe: str) -> timedelta:
        return _FREQUENCY_DELTAS.get(timeframe, timedelta(hours=1))

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="market",
                vendor="ccxt",
                message="CCXT connector requires at least one symbol identifier.",
                retryable=False,
                code="identifier_required",
            )
        return request.identifiers[0]

    @staticmethod
    def _require_exchange(request: IngestionRequest) -> str:
        exchange = str(request.options.get("exchange", "")).strip().lower()
        if not exchange:
            raise DataConnectorError(
                data_domain="market",
                vendor="ccxt",
                message="CCXT connector requires an exchange option.",
                retryable=False,
                code="exchange_required",
            )
        return exchange
