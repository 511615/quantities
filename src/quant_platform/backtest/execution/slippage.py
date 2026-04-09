from __future__ import annotations

from quant_platform.backtest.contracts.backtest import CostModel
from quant_platform.backtest.contracts.order import Urgency


def slippage_bps_for_order(
    cost_model: CostModel,
    urgency: Urgency,
    participation_rate: float,
) -> float:
    urgency_multiplier = {"passive": 0.5, "normal": 1.0, "aggressive": 1.5}[urgency]
    return (
        cost_model.slippage_bps + cost_model.spread_bps + cost_model.impact_bps * participation_rate
    ) * urgency_multiplier


def execution_price(base_price: float, side: str, slippage_bps: float) -> float:
    sign = 1.0 if side == "BUY" else -1.0
    return base_price * (1.0 + sign * slippage_bps / 10000.0)
