from __future__ import annotations

from dataclasses import dataclass

from quant_platform.data.contracts.market import NormalizedMarketBar


@dataclass(frozen=True)
class MarketEvent:
    event_time: object
    available_time: object
    instrument: str
    venue: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def mid(self) -> float:
        return (self.open + self.close) / 2.0


def bars_to_market_events(
    rows: list[NormalizedMarketBar],
    volume_multiplier: float = 1.0,
) -> list[MarketEvent]:
    return [
        MarketEvent(
            event_time=row.event_time,
            available_time=row.available_time,
            instrument=row.symbol,
            venue=row.venue,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume * volume_multiplier,
        )
        for row in rows
    ]
