from __future__ import annotations

from quant_platform.backtest.contracts.order import TargetInstruction
from quant_platform.backtest.contracts.portfolio import RiskConstraintSet, RiskEvent


def hhi(weights: list[float]) -> float:
    return sum(weight * weight for weight in weights)


def clip_target(
    target: TargetInstruction,
    risk_constraints: RiskConstraintSet,
) -> tuple[TargetInstruction, list[RiskEvent]]:
    risk_events: list[RiskEvent] = []
    target_value = target.target_value
    if risk_constraints.mode == "long_only" and target_value < 0.0:
        risk_events.append(
            RiskEvent(
                timestamp=target.timestamp,
                action="CLIP_TARGET",
                instrument=target.instrument,
                message="negative target clipped in long_only mode",
            )
        )
        target_value = 0.0
    if abs(target_value) > risk_constraints.max_position_weight:
        risk_events.append(
            RiskEvent(
                timestamp=target.timestamp,
                action="CLIP_TARGET",
                instrument=target.instrument,
                message="target exceeds max_position_weight",
            )
        )
        target_value = max(
            -risk_constraints.max_position_weight,
            min(risk_constraints.max_position_weight, target_value),
        )
    return target.model_copy(update={"target_value": target_value}), risk_events
