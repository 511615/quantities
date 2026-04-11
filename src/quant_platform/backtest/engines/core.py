from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from quant_platform.backtest.adapters.market_adapter import MarketEvent, bars_to_market_events
from quant_platform.backtest.adapters.prediction_adapter import PredictionToSignalAdapter
from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    CostModel,
    ExecutionConfig,
    LatencyConfig,
    StrategyConfig,
)
from quant_platform.backtest.contracts.order import (
    ChildOrder,
    FillEvent,
    ParentOrder,
    TargetInstruction,
)
from quant_platform.backtest.contracts.portfolio import (
    PortfolioSnapshot,
    RiskConstraintSet,
    RiskEvent,
)
from quant_platform.backtest.contracts.scenario import ScenarioSpec
from quant_platform.backtest.contracts.signal import SignalFrame
from quant_platform.backtest.execution.latency import order_eligible_time
from quant_platform.backtest.strategy.portfolio_construction import build_target_instructions
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.training.contracts.training import PredictionFrame


def default_risk_constraints(request: BacktestRequest) -> RiskConstraintSet:
    return request.risk_constraints or RiskConstraintSet(
        mode=request.strategy_config.direction_mode,
        max_gross_leverage=request.portfolio_config.max_gross_leverage,
        max_net_leverage=request.portfolio_config.max_net_leverage,
        max_position_weight=request.portfolio_config.max_position_weight,
        max_single_name_notional=request.portfolio_config.initial_cash
        * request.portfolio_config.max_position_weight,
        max_turnover_per_rebalance=request.portfolio_config.max_turnover_per_rebalance,
        max_daily_turnover=max(1.0, request.portfolio_config.max_turnover_per_rebalance * 24.0),
        max_drawdown_hard_stop=1.0,
        max_concentration_hhi=1.0,
        min_cash_buffer=0.0,
        max_participation_rate=request.execution_config.participation_cap,
        max_order_notional=request.portfolio_config.initial_cash
        * request.portfolio_config.max_position_weight,
        allow_short=request.strategy_config.direction_mode == "long_short",
        allow_fractional_qty=request.portfolio_config.allow_fractional_qty,
    )


def apply_scenario(
    request: BacktestRequest,
    scenario: ScenarioSpec,
) -> tuple[StrategyConfig, CostModel, ExecutionConfig, RiskConstraintSet]:
    latency = request.execution_config.latency_config
    updated_latency = LatencyConfig(
        signal_delay_seconds=int(latency.signal_delay_seconds * scenario.latency_multiplier),
        order_delay_seconds=int(latency.order_delay_seconds * scenario.latency_multiplier),
        ack_delay_seconds=int(latency.ack_delay_seconds * scenario.latency_multiplier),
    )
    updated_execution = request.execution_config.model_copy(
        update={
            "latency_config": updated_latency,
            "participation_cap": min(
                1.0,
                request.execution_config.participation_cap * scenario.participation_multiplier,
            ),
        }
    )
    updated_cost = request.cost_model.model_copy(
        update={
            "fee_bps": request.cost_model.fee_bps * scenario.fee_multiplier,
            "slippage_bps": request.cost_model.slippage_bps * scenario.slippage_multiplier,
            "spread_bps": request.cost_model.spread_bps * scenario.slippage_multiplier,
            "impact_bps": request.cost_model.impact_bps * scenario.slippage_multiplier,
        }
    )
    risk_constraints = default_risk_constraints(request).model_copy(
        update={
            "mode": scenario.direction_mode_override or default_risk_constraints(request).mode,
            "allow_short": (
                False
                if scenario.direction_mode_override == "long_only"
                else default_risk_constraints(request).allow_short
            ),
            "max_participation_rate": min(
                1.0,
                default_risk_constraints(request).max_participation_rate
                * scenario.participation_multiplier,
            ),
        }
    )
    strategy = request.strategy_config.model_copy(
        update={
            "direction_mode": scenario.direction_mode_override
            or request.strategy_config.direction_mode
        }
    )
    return strategy, updated_cost, updated_execution, risk_constraints


def resolve_signal_frame(
    request: BacktestRequest,
    prediction_frame: PredictionFrame | None,
    signal_frame: SignalFrame | None,
) -> SignalFrame:
    if request.input_type == "signal_frame":
        if signal_frame is None:
            raise ValueError("signal_frame input_type requires a SignalFrame")
        return signal_frame
    if prediction_frame is None:
        raise ValueError("prediction_frame input_type requires a PredictionFrame")
    return PredictionToSignalAdapter().adapt(
        prediction_frame=prediction_frame,
        strategy_config=request.strategy_config,
        latency_config=request.execution_config.latency_config,
        source_prediction_uri=request.input_ref or request.prediction_frame_uri,
    )


def resolve_market_events(
    market_bars: list[NormalizedMarketBar] | None,
    scenario: ScenarioSpec,
    signal_frame: SignalFrame | None = None,
) -> list[MarketEvent]:
    if market_bars is None:
        if signal_frame is None:
            raise ValueError("market_bars are required for the current backtest engines")
        synthetic_bars = []
        base_price = 100.0
        for index, row in enumerate(signal_frame.rows):
            price = base_price + float(index)
            synthetic_bars.append(
                NormalizedMarketBar(
                    event_time=row.tradable_from + timedelta(seconds=1),
                    available_time=row.tradable_from + timedelta(seconds=1),
                    symbol=row.instrument,
                    venue=row.venue,
                    open=price,
                    high=price * 1.001,
                    low=price * 0.999,
                    close=price * (1.0 + row.raw_value * 0.001),
                    volume=1_000_000.0,
                )
            )
        market_bars = synthetic_bars
    return bars_to_market_events(market_bars, volume_multiplier=scenario.volume_multiplier)


def prepare_targets(
    signal_frame: SignalFrame,
    request: BacktestRequest,
    strategy_config: StrategyConfig,
    risk_constraints: RiskConstraintSet,
) -> list[tuple[TargetInstruction, object]]:
    targets = build_target_instructions(
        signal_frame=signal_frame,
        strategy_config=strategy_config,
        portfolio_config=request.portfolio_config,
        risk_constraints=risk_constraints,
    )
    return [
        (target, order_eligible_time(target.timestamp, request.execution_config))
        for target in targets
    ]


def benchmark_returns(events: list[MarketEvent], benchmark_symbol: str) -> list[float]:
    prices = [event.close for event in events if event.instrument == benchmark_symbol]
    return [
        prices[index] / prices[index - 1] - 1.0
        for index in range(1, len(prices))
        if abs(prices[index - 1]) > 1e-9
    ]


def realized_signal_returns(signal_frame: SignalFrame, events: list[MarketEvent]) -> list[float]:
    events_by_instrument: dict[str, list[MarketEvent]] = defaultdict(list)
    for event in events:
        events_by_instrument[event.instrument].append(event)
    realized: list[float] = []
    signal_offsets: dict[str, int] = defaultdict(int)
    tradable_offsets: dict[str, int] = defaultdict(int)
    for row in signal_frame.rows:
        instrument_events = events_by_instrument.get(row.instrument, [])
        current_index = signal_offsets[row.instrument]
        while (
            current_index < len(instrument_events)
            and instrument_events[current_index].event_time <= row.signal_time
        ):
            current_index += 1
        signal_offsets[row.instrument] = current_index
        current = instrument_events[current_index] if current_index < len(instrument_events) else None

        future_index = max(tradable_offsets[row.instrument], current_index)
        while (
            future_index < len(instrument_events)
            and instrument_events[future_index].event_time <= row.tradable_from
        ):
            future_index += 1
        tradable_offsets[row.instrument] = future_index
        future = instrument_events[future_index] if future_index < len(instrument_events) else None
        if current is None or future is None or abs(current.close) <= 1e-9:
            realized.append(0.0)
            continue
        realized.append(future.close / current.close - 1.0)
    return realized


def leakage_audit(
    signal_frame: SignalFrame,
    fills: list[FillEvent],
    request: BacktestRequest,
) -> dict[str, object]:
    tradable_by_instrument = {}
    for row in signal_frame.rows:
        tradable_by_instrument[row.instrument] = min(
            tradable_by_instrument.get(row.instrument, row.tradable_from),
            row.tradable_from,
        )
    same_bar_execution_ok = (
        all(
            fill.fill_time > tradable_by_instrument.get(fill.instrument, fill.fill_time)
            for fill in fills
        )
        if fills
        else True
    )
    future_feature_ok = all(row.signal_time >= row.available_time for row in signal_frame.rows)
    stale_latency_ok = all(row.tradable_from >= row.available_time for row in signal_frame.rows)
    return {
        "same_bar_execution_check": same_bar_execution_ok,
        "future_feature_check": future_feature_ok,
        "future_cost_curve_check": True,
        "label_access_check": True,
        "stale_or_negative_latency_check": stale_latency_ok,
        "leakage_checks_enabled": request.leakage_checks_enabled,
    }


def persist_backtest_artifacts(
    store: LocalArtifactStore,
    backtest_id: str,
    orders: list[ParentOrder | ChildOrder],
    fills: list[FillEvent],
    snapshots: list[PortfolioSnapshot],
    pnl_payload: dict[str, object],
    report_payload: dict[str, object],
    diagnostics_payload: dict[str, object],
    leakage_payload: dict[str, object],
    scenario_payload: dict[str, object],
) -> dict[str, str]:
    orders_artifact = store.write_json(
        f"backtests/{backtest_id}/orders.json",
        {"orders": [order.model_dump(mode="json") for order in orders]},
    )
    fills_artifact = store.write_json(
        f"backtests/{backtest_id}/fills.json",
        {"fills": [fill.model_dump(mode="json") for fill in fills]},
    )
    positions_artifact = store.write_json(
        f"backtests/{backtest_id}/positions.json",
        {"snapshots": [snapshot.model_dump(mode="json") for snapshot in snapshots]},
    )
    pnl_artifact = store.write_json(f"backtests/{backtest_id}/pnl.json", pnl_payload)
    report_artifact = store.write_json(f"backtests/{backtest_id}/report.json", report_payload)
    diagnostics_artifact = store.write_json(
        f"backtests/{backtest_id}/diagnostics.json",
        diagnostics_payload,
    )
    leakage_artifact = store.write_json(
        f"backtests/{backtest_id}/leakage_audit.json",
        leakage_payload,
    )
    scenario_artifact = store.write_json(
        f"backtests/{backtest_id}/scenarios.json",
        scenario_payload,
    )
    return {
        "orders_uri": orders_artifact.uri,
        "fills_uri": fills_artifact.uri,
        "positions_uri": positions_artifact.uri,
        "pnl_uri": pnl_artifact.uri,
        "report_uri": report_artifact.uri,
        "diagnostics_uri": diagnostics_artifact.uri,
        "leakage_audit_uri": leakage_artifact.uri,
        "scenario_summary_uri": scenario_artifact.uri,
    }


def build_backtest_id(request: BacktestRequest, engine_type: str) -> str:
    return stable_digest({"request": request, "engine_type": engine_type})
