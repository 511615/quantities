from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from quant_platform.backtest.contracts.portfolio import RiskConstraintSet
from quant_platform.backtest.contracts.scenario import ScenarioSpec
from quant_platform.common.types.core import FrozenModel


class StrategyConfig(FrozenModel):
    name: str
    mode: str = "prediction_to_position"
    signal_type: Literal["score", "expected_return", "probability", "target_weight"] = "score"
    direction_mode: Literal["long_only", "long_short"] = "long_short"
    target_type: Literal["weight", "notional", "quantity"] = "weight"
    urgency: Literal["passive", "normal", "aggressive"] = "normal"
    normalize_gross: bool = True


class PortfolioConfig(FrozenModel):
    initial_cash: float = Field(gt=0)
    base_currency: str = "USDT"
    max_gross_leverage: float = Field(gt=0)
    max_net_leverage: float = Field(default=1.0, gt=0)
    max_position_weight: float = Field(gt=0)
    max_turnover_per_rebalance: float = Field(default=1.0, gt=0)
    allow_fractional_qty: bool = True


class CostModel(FrozenModel):
    fee_bps: float = Field(ge=0)
    slippage_bps: float = Field(ge=0)
    spread_bps: float = Field(default=0.0, ge=0.0)
    impact_bps: float = Field(default=0.0, ge=0.0)
    borrow_bps_per_day: float = Field(default=0.0, ge=0.0)
    funding_bps_per_day: float = Field(default=0.0, ge=0.0)


class LatencyConfig(FrozenModel):
    signal_delay_seconds: int = Field(default=0, ge=0)
    order_delay_seconds: int = Field(default=0, ge=0)
    ack_delay_seconds: int = Field(default=0, ge=0)


class ExecutionConfig(FrozenModel):
    latency_config: LatencyConfig = Field(default_factory=LatencyConfig)
    fill_price: Literal["next_bar_open", "next_bar_mid"] = "next_bar_open"
    participation_cap: float = Field(default=1.0, gt=0.0, le=1.0)
    allow_partial_fills: bool = True
    max_slippage_bps: float = Field(default=1000.0, ge=0.0)


class BenchmarkSpec(FrozenModel):
    name: str
    symbol: str


class CalendarSpec(FrozenModel):
    timezone: str
    frequency: str


class SignalRow(FrozenModel):
    entity_keys: dict[str, str]
    timestamp: datetime
    target_weight: float


class BacktestRequest(FrozenModel):
    input_ref: str | None = None
    input_type: Literal["prediction_frame", "signal_frame"] = "prediction_frame"
    prediction_frame_uri: str | None = None
    engine_type: Literal["research", "simulation"] = "research"
    strategy_config: StrategyConfig
    portfolio_config: PortfolioConfig
    cost_model: CostModel
    execution_config: ExecutionConfig = Field(default_factory=ExecutionConfig)
    risk_constraints: RiskConstraintSet | None = None
    benchmark_spec: BenchmarkSpec
    calendar_spec: CalendarSpec
    market_data_refs: list[str] = Field(default_factory=list)
    scenario_specs: list[ScenarioSpec] = Field(default_factory=list)
    leakage_checks_enabled: bool = True

    @model_validator(mode="after")
    def validate_inputs(self) -> "BacktestRequest":
        if self.input_ref is None and self.prediction_frame_uri is None:
            raise ValueError("either input_ref or prediction_frame_uri must be provided")
        return self


class BacktestResult(FrozenModel):
    backtest_id: str
    engine_type: Literal["research", "simulation"]
    orders_uri: str
    fills_uri: str
    positions_uri: str
    pnl_uri: str
    risk_metrics: dict[str, float]
    report_uri: str
    diagnostics_uri: str | None = None
    leakage_audit_uri: str | None = None
    scenario_summary_uri: str | None = None
