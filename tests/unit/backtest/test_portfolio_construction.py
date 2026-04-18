from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.backtest.contracts.backtest import PortfolioConfig, StrategyConfig
from quant_platform.backtest.contracts.portfolio import RiskConstraintSet
from quant_platform.backtest.contracts.signal import SignalFrame, SignalRecord
from quant_platform.backtest.strategy.portfolio_construction import build_target_instructions


def _signal(
    *,
    signal_id: str,
    tradable_from: datetime,
    raw_value: float,
    normalized_value: float,
) -> SignalRecord:
    return SignalRecord(
        signal_id=signal_id,
        model_run_id="run-1",
        instrument="BTCUSDT",
        venue="binance",
        signal_time=tradable_from,
        available_time=tradable_from,
        tradable_from=tradable_from,
        signal_type="score",
        raw_value=raw_value,
        normalized_value=normalized_value,
        confidence=1.0,
        direction_mode="long_short",
        meta={},
    )


def test_proportional_single_asset_preserves_signal_magnitude() -> None:
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    frame = SignalFrame(
        rows=[
            _signal(
                signal_id="sig-1",
                tradable_from=timestamp,
                raw_value=0.18,
                normalized_value=0.18,
            )
        ]
    )
    instructions = build_target_instructions(
        signal_frame=frame,
        strategy_config=StrategyConfig(name="sign_strategy", direction_mode="long_short"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
        ),
        risk_constraints=RiskConstraintSet(
            mode="long_short",
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=1.0,
            max_single_name_notional=100000.0,
            max_turnover_per_rebalance=1.0,
            max_daily_turnover=24.0,
            max_drawdown_hard_stop=1.0,
            max_concentration_hhi=1.0,
            min_cash_buffer=0.0,
            max_participation_rate=1.0,
            max_order_notional=100000.0,
            allow_short=True,
            allow_fractional_qty=True,
        ),
    )
    assert len(instructions) == 1
    assert 0.0 < instructions[0].target_value < 0.25


def test_proportional_turnover_budget_shrinks_flip_between_rebalances() -> None:
    first_timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    second_timestamp = datetime(2024, 1, 1, 1, tzinfo=timezone.utc)
    frame = SignalFrame(
        rows=[
            _signal(
                signal_id="sig-1",
                tradable_from=first_timestamp,
                raw_value=1.0,
                normalized_value=1.0,
            ),
            _signal(
                signal_id="sig-2",
                tradable_from=second_timestamp,
                raw_value=-1.0,
                normalized_value=-1.0,
            ),
        ]
    )
    instructions = build_target_instructions(
        signal_frame=frame,
        strategy_config=StrategyConfig(name="sign_strategy", direction_mode="long_short"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
            max_turnover_per_rebalance=0.2,
        ),
        risk_constraints=RiskConstraintSet(
            mode="long_short",
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=1.0,
            max_single_name_notional=100000.0,
            max_turnover_per_rebalance=0.2,
            max_daily_turnover=24.0,
            max_drawdown_hard_stop=1.0,
            max_concentration_hhi=1.0,
            min_cash_buffer=0.0,
            max_participation_rate=1.0,
            max_order_notional=100000.0,
            allow_short=True,
            allow_fractional_qty=True,
        ),
    )
    assert len(instructions) == 2
    assert instructions[0].target_value > 0.0
    assert instructions[1].target_value >= 0.0
    assert abs(instructions[1].target_value - instructions[0].target_value) <= 0.21
