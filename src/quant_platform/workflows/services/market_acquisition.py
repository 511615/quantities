from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.workflows.contracts.requests import (
    DatasetAcquisitionRequest,
    DatasetAcquisitionSourceRequest,
)
from quant_platform.workflows.runtime import WorkflowRuntime


@dataclass(frozen=True)
class MarketAcquisitionResult:
    bars: list[NormalizedMarketBar]
    request_origin: str


class MarketAcquisitionHandler:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def build_market_panel(
        self,
        *,
        request: DatasetAcquisitionRequest,
        source: DatasetAcquisitionSourceRequest,
        symbols: list[str],
    ) -> MarketAcquisitionResult:
        bars: list[NormalizedMarketBar] = []
        statuses: list[str] = []
        fetch_end_time = request.time_window.end_time + (
            self.frequency_delta(source.frequency) * (request.build_config.label_horizon + 1)
        )
        exchange = source.exchange or request.exchange or source.vendor
        market_type = self.market_type(source)
        for symbol in symbols:
            symbol_bars, status = self.runtime.ingestion_service.fetch_market_bars(
                symbol=symbol,
                vendor=source.vendor,
                exchange=exchange,
                frequency=source.frequency,
                start_time=request.time_window.start_time,
                end_time=fetch_end_time,
                market_type=market_type,
            )
            bars.extend(symbol_bars)
            statuses.append(status)
        bars.sort(key=lambda item: (item.event_time, item.symbol))
        if not bars:
            raise ValueError("market ingestion returned no bars for the requested symbols")
        return MarketAcquisitionResult(
            bars=bars,
            request_origin=",".join(sorted(set(statuses))),
        )

    @staticmethod
    def market_type(source: DatasetAcquisitionSourceRequest) -> str:
        symbol_type = (source.symbol_selector.symbol_type if source.symbol_selector else "spot").lower()
        if symbol_type in {"spot", "future", "futures", "swap", "margin"}:
            return symbol_type
        return "spot"

    @staticmethod
    def frequency_delta(frequency: str) -> timedelta:
        mapping = {
            "1m": timedelta(minutes=1),
            "3m": timedelta(minutes=3),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "2h": timedelta(hours=2),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }
        return mapping.get(frequency, timedelta(hours=1))
