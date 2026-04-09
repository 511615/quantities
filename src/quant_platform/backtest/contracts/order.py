from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from quant_platform.common.types.core import FrozenModel

TargetType = Literal["weight", "notional", "quantity"]
Urgency = Literal["passive", "normal", "aggressive"]
OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT", "POST_ONLY"]
TimeInForce = Literal["IOC", "GTC", "DAY"]
ChildOrderStatus = Literal[
    "NEW", "ACKED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"
]


class TargetInstruction(FrozenModel):
    timestamp: datetime
    instrument: str
    target_type: TargetType
    target_value: float
    max_participation_rate: float = Field(ge=0.0, le=1.0)
    urgency: Urgency
    reason_code: str
    signal_ref: str


class ParentOrder(FrozenModel):
    parent_order_id: str
    created_time: datetime
    instrument: str
    target_type: TargetType
    target_value: float
    current_value: float
    delta_value: float
    signal_ref: str
    reason_code: str


class ChildOrder(FrozenModel):
    order_id: str
    parent_order_id: str
    created_time: datetime
    eligible_time: datetime
    instrument: str
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    limit_price: float | None = None
    quantity: float = Field(gt=0.0)
    max_slippage_bps: float = Field(ge=0.0)
    participation_cap: float = Field(ge=0.0, le=1.0)
    reduce_only: bool = False
    status: ChildOrderStatus = "NEW"
    rejection_reason: str | None = None

    @model_validator(mode="after")
    def validate_limit_order(self) -> "ChildOrder":
        if self.order_type in {"LIMIT", "POST_ONLY"} and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        return self


class FillEvent(FrozenModel):
    fill_id: str
    order_id: str
    instrument: str
    side: OrderSide
    fill_time: datetime
    quantity: float = Field(gt=0.0)
    price: float = Field(gt=0.0)
    notional: float = Field(gt=0.0)
    fee: float = Field(ge=0.0)
    slippage_cost: float = Field(ge=0.0)
    liquidity_flag: Literal["maker", "taker"] = "taker"
