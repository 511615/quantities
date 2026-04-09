from __future__ import annotations

from collections import defaultdict

from quant_platform.backtest.contracts.backtest import PortfolioConfig, StrategyConfig
from quant_platform.backtest.contracts.order import TargetInstruction
from quant_platform.backtest.contracts.portfolio import RiskConstraintSet
from quant_platform.backtest.contracts.signal import SignalFrame, SignalRecord
from quant_platform.backtest.strategy.signal_router import normalize_signal_value
from quant_platform.backtest.strategy.sizers import scale_signal_by_confidence


def build_target_instructions(
    signal_frame: SignalFrame,
    strategy_config: StrategyConfig,
    portfolio_config: PortfolioConfig,
    risk_constraints: RiskConstraintSet,
) -> list[TargetInstruction]:
    grouped: dict[object, list[SignalRecord]] = defaultdict(list)
    for row in signal_frame.rows:
        grouped[row.tradable_from].append(row)
    instructions: list[TargetInstruction] = []
    for timestamp in sorted(grouped):
        rows = grouped[timestamp]
        strengths = {
            row.signal_id: scale_signal_by_confidence(
                normalize_signal_value(row),
                row.confidence,
            )
            for row in rows
        }
        if strategy_config.direction_mode == "long_only":
            denom = sum(max(0.0, value) for value in strengths.values()) or 1.0
            target_values = {
                row.signal_id: min(
                    risk_constraints.max_position_weight,
                    max(0.0, strengths[row.signal_id]) / denom,
                )
                for row in rows
            }
        else:
            gross = sum(abs(value) for value in strengths.values()) or 1.0
            scale = min(
                portfolio_config.max_gross_leverage,
                risk_constraints.max_gross_leverage,
            )
            target_values = {
                row.signal_id: max(
                    -risk_constraints.max_position_weight,
                    min(
                        risk_constraints.max_position_weight,
                        (strengths[row.signal_id] / gross) * scale,
                    ),
                )
                for row in rows
            }
        for row in rows:
            instructions.append(
                TargetInstruction(
                    timestamp=row.tradable_from,
                    instrument=row.instrument,
                    target_type=strategy_config.target_type,
                    target_value=target_values[row.signal_id],
                    max_participation_rate=risk_constraints.max_participation_rate,
                    urgency=strategy_config.urgency,
                    reason_code="signal_rebalance",
                    signal_ref=row.signal_id,
                )
            )
    return instructions
