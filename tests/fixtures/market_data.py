from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.data.contracts.market import NormalizedMarketBar


def build_market_bars() -> list[NormalizedMarketBar]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 100.5, 102.0, 103.5, 102.5]
    volumes = [10.0, 11.0, 9.5, 15.0, 16.0, 12.0]
    rows: list[NormalizedMarketBar] = []
    for idx, close in enumerate(closes):
        open_price = closes[idx - 1] if idx > 0 else close
        rows.append(
            NormalizedMarketBar(
                event_time=base.replace(hour=idx),
                available_time=base.replace(hour=idx),
                symbol="BTCUSDT",
                venue="binance",
                open=open_price,
                high=max(open_price, close) + 0.5,
                low=min(open_price, close) - 0.5,
                close=close,
                volume=volumes[idx],
            )
        )
    return rows
