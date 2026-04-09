from __future__ import annotations

from quant_platform.backtest.contracts.backtest import CostModel


def fee_cost(notional: float, cost_model: CostModel) -> float:
    return abs(notional) * cost_model.fee_bps / 10000.0


def borrow_cost(notional: float, days_held: float, cost_model: CostModel) -> float:
    return abs(notional) * cost_model.borrow_bps_per_day / 10000.0 * max(days_held, 0.0)


def funding_cost(notional: float, days_held: float, cost_model: CostModel) -> float:
    return abs(notional) * cost_model.funding_bps_per_day / 10000.0 * max(days_held, 0.0)
