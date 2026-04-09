from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from quant_platform.common.types.core import FrozenModel

RiskAction = Literal["CLIP_TARGET", "REJECT_ORDER", "REDUCE_ORDER", "FORCE_DELEVER", "LIQUIDATE"]


class PositionSnapshot(FrozenModel):
    instrument: str
    quantity: float
    side: Literal["long", "short", "flat"]
    avg_entry_price: float = Field(ge=0.0)
    mark_price: float = Field(gt=0.0)
    market_value: float
    notional: float = Field(ge=0.0)
    unrealized_pnl: float
    realized_pnl_cum: float
    last_fill_time: datetime | None = None
    weight: float
    initial_margin: float = Field(ge=0.0)
    maintenance_margin: float = Field(ge=0.0)


class PortfolioSnapshot(FrozenModel):
    timestamp: datetime
    nav: float
    equity: float
    cash_free: float
    cash_locked: float
    realized_pnl: float
    unrealized_pnl: float
    fees_paid: float
    slippage_cost: float
    funding_pnl: float
    borrow_cost: float
    gross_exposure: float
    net_exposure: float
    long_exposure: float
    short_exposure: float
    gross_leverage: float
    net_leverage: float
    turnover_1d: float
    margin_used: float
    maintenance_margin: float
    liquidation_buffer: float
    drawdown: float
    positions: list[PositionSnapshot]


class RiskConstraintSet(FrozenModel):
    mode: Literal["long_only", "long_short"] = "long_short"
    max_gross_leverage: float = Field(gt=0.0)
    max_net_leverage: float = Field(gt=0.0)
    max_position_weight: float = Field(gt=0.0)
    max_single_name_notional: float = Field(gt=0.0)
    max_turnover_per_rebalance: float = Field(gt=0.0)
    max_daily_turnover: float = Field(gt=0.0)
    max_drawdown_hard_stop: float = Field(ge=0.0)
    max_concentration_hhi: float = Field(gt=0.0)
    min_cash_buffer: float = Field(ge=0.0)
    max_participation_rate: float = Field(gt=0.0, le=1.0)
    max_order_notional: float = Field(gt=0.0)
    allow_short: bool = True
    allow_fractional_qty: bool = True


class RiskEvent(FrozenModel):
    timestamp: datetime
    action: RiskAction
    instrument: str | None = None
    message: str
