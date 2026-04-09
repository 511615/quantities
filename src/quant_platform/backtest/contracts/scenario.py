from __future__ import annotations

from pydantic import Field

from quant_platform.common.types.core import FrozenModel


class ScenarioSpec(FrozenModel):
    name: str
    description: str
    fee_multiplier: float = Field(default=1.0, gt=0.0)
    slippage_multiplier: float = Field(default=1.0, gt=0.0)
    latency_multiplier: float = Field(default=1.0, gt=0.0)
    participation_multiplier: float = Field(default=1.0, gt=0.0)
    volume_multiplier: float = Field(default=1.0, gt=0.0)
    volatility_multiplier: float = Field(default=1.0, gt=0.0)
    direction_mode_override: str | None = None


class ScenarioResult(FrozenModel):
    scenario_name: str
    metrics_delta: dict[str, float]
    execution_delta: dict[str, float]
    risk_trigger_count: int
    pnl_delta: float
    failure_summary: str | None = None
