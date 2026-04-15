from __future__ import annotations

import importlib
from collections import defaultdict
from datetime import datetime

import numpy as np

from quant_platform.backtest.contracts.backtest import PortfolioConfig, StrategyConfig
from quant_platform.backtest.contracts.order import TargetInstruction
from quant_platform.backtest.contracts.portfolio import RiskConstraintSet
from quant_platform.backtest.contracts.signal import SignalFrame, SignalRecord
from quant_platform.backtest.strategy.signal_router import normalize_signal_value
from quant_platform.backtest.strategy.sizers import scale_signal_by_confidence
from quant_platform.data.contracts.market import NormalizedMarketBar

_MIN_SKFOLIO_SAMPLES = 8
_SKFOLIO_LOOKBACK_BARS = 48


def build_target_instructions(
    signal_frame: SignalFrame,
    strategy_config: StrategyConfig,
    portfolio_config: PortfolioConfig,
    risk_constraints: RiskConstraintSet,
    market_bars: list[NormalizedMarketBar] | None = None,
) -> list[TargetInstruction]:
    grouped: dict[object, list[SignalRecord]] = defaultdict(list)
    for row in signal_frame.rows:
        grouped[row.tradable_from].append(row)
    instructions: list[TargetInstruction] = []
    previous_weights: dict[str, float] = {}
    for timestamp in sorted(grouped):
        rows = grouped[timestamp]
        strengths = {
            row.signal_id: scale_signal_by_confidence(
                normalize_signal_value(row),
                row.confidence,
            )
            for row in rows
        }
        if strategy_config.portfolio_method == "skfolio_mean_risk":
            target_values = _build_skfolio_target_values(
                timestamp=timestamp if isinstance(timestamp, datetime) else None,
                rows=rows,
                strengths=strengths,
                portfolio_config=portfolio_config,
                risk_constraints=risk_constraints,
                strategy_config=strategy_config,
                market_bars=market_bars,
                previous_weights=previous_weights,
            )
        else:
            target_values = _build_proportional_target_values(
                rows=rows,
                strengths=strengths,
                portfolio_config=portfolio_config,
                risk_constraints=risk_constraints,
                strategy_config=strategy_config,
            )
        for row in rows:
            target_value = target_values[row.signal_id]
            previous_weights[row.instrument] = target_value
            instructions.append(
                TargetInstruction(
                    timestamp=row.tradable_from,
                    instrument=row.instrument,
                    target_type=strategy_config.target_type,
                    target_value=target_value,
                    max_participation_rate=risk_constraints.max_participation_rate,
                    urgency=strategy_config.urgency,
                    reason_code="signal_rebalance",
                    signal_ref=row.signal_id,
                )
            )
    return instructions


def _build_proportional_target_values(
    *,
    rows: list[SignalRecord],
    strengths: dict[str, float],
    portfolio_config: PortfolioConfig,
    risk_constraints: RiskConstraintSet,
    strategy_config: StrategyConfig,
) -> dict[str, float]:
    if strategy_config.direction_mode == "long_only":
        denom = sum(max(0.0, value) for value in strengths.values()) or 1.0
        return {
            row.signal_id: min(
                risk_constraints.max_position_weight,
                max(0.0, strengths[row.signal_id]) / denom,
            )
            for row in rows
        }
    gross = sum(abs(value) for value in strengths.values()) or 1.0
    scale = min(
        portfolio_config.max_gross_leverage,
        risk_constraints.max_gross_leverage,
    )
    return {
        row.signal_id: max(
            -risk_constraints.max_position_weight,
            min(
                risk_constraints.max_position_weight,
                (strengths[row.signal_id] / gross) * scale,
            ),
        )
        for row in rows
    }


def _build_skfolio_target_values(
    *,
    timestamp: datetime | None,
    rows: list[SignalRecord],
    strengths: dict[str, float],
    portfolio_config: PortfolioConfig,
    risk_constraints: RiskConstraintSet,
    strategy_config: StrategyConfig,
    market_bars: list[NormalizedMarketBar] | None,
    previous_weights: dict[str, float],
) -> dict[str, float]:
    if timestamp is None:
        raise ValueError("skfolio_mean_risk requires timestamped signal rows")
    symbols = [row.instrument for row in rows]
    if len(set(symbols)) < 2:
        raise ValueError("skfolio_mean_risk requires at least two instruments")
    if market_bars is None:
        raise ValueError("skfolio_mean_risk requires aligned market bars")
    returns_matrix = _build_returns_matrix(
        market_bars=market_bars,
        symbols=symbols,
        timestamp=timestamp,
    )
    alpha = np.array([strengths[row.signal_id] for row in rows], dtype=float)
    if strategy_config.direction_mode == "long_only":
        alpha = np.maximum(alpha, 0.0)
        if float(alpha.sum()) <= 1e-12:
            raise ValueError("skfolio_mean_risk has no positive alpha signal to optimize")
    weights = _solve_skfolio_weights(
        returns_matrix=returns_matrix,
        alpha=alpha,
        symbols=symbols,
        previous_weights=previous_weights,
        portfolio_config=portfolio_config,
        risk_constraints=risk_constraints,
        strategy_config=strategy_config,
    )
    return {
        row.signal_id: float(weights[index])
        for index, row in enumerate(rows)
    }


def _build_returns_matrix(
    *,
    market_bars: list[NormalizedMarketBar],
    symbols: list[str],
    timestamp: datetime,
) -> np.ndarray:
    symbol_set = set(symbols)
    prices_by_timestamp: dict[datetime, dict[str, float]] = defaultdict(dict)
    for bar in market_bars:
        if bar.symbol not in symbol_set or bar.event_time > timestamp:
            continue
        prices_by_timestamp[bar.event_time][bar.symbol] = bar.close
    aligned_timestamps = [
        bar_time
        for bar_time, price_map in sorted(prices_by_timestamp.items())
        if all(symbol in price_map for symbol in symbols)
    ]
    if len(aligned_timestamps) < _MIN_SKFOLIO_SAMPLES + 1:
        raise ValueError("skfolio_mean_risk requires more aligned market history")
    aligned_timestamps = aligned_timestamps[-(_SKFOLIO_LOOKBACK_BARS + 1) :]
    price_matrix = np.array(
        [[prices_by_timestamp[bar_time][symbol] for symbol in symbols] for bar_time in aligned_timestamps],
        dtype=float,
    )
    if np.any(price_matrix[:-1] <= 0.0):
        raise ValueError("skfolio_mean_risk encountered non-positive close prices")
    returns = price_matrix[1:] / price_matrix[:-1] - 1.0
    if returns.shape[0] < _MIN_SKFOLIO_SAMPLES:
        raise ValueError("skfolio_mean_risk requires at least 8 aligned return samples")
    return returns


def _solve_skfolio_weights(
    *,
    returns_matrix: np.ndarray,
    alpha: np.ndarray,
    symbols: list[str],
    previous_weights: dict[str, float],
    portfolio_config: PortfolioConfig,
    risk_constraints: RiskConstraintSet,
    strategy_config: StrategyConfig,
) -> np.ndarray:
    try:
        optimization = importlib.import_module("skfolio.optimization")
        measures = importlib.import_module("skfolio.measures")
        cp = importlib.import_module("cvxpy")
    except ModuleNotFoundError as exc:
        raise ValueError(
            "skfolio_mean_risk requires the optional 'portfolio_opt' dependencies"
        ) from exc
    min_weights = (
        0.0
        if strategy_config.direction_mode == "long_only"
        else -risk_constraints.max_position_weight
    )
    max_weights = risk_constraints.max_position_weight
    previous_weight_vector = np.array(
        [previous_weights.get(symbol, 0.0) for symbol in symbols],
        dtype=float,
    )
    optimizer = optimization.MeanRisk(
        objective_function=optimization.ObjectiveFunction.MAXIMIZE_UTILITY,
        risk_measure=measures.RiskMeasure.VARIANCE,
        min_weights=min_weights,
        max_weights=max_weights,
        budget=1.0 if strategy_config.direction_mode == "long_only" else None,
        min_budget=0.0 if strategy_config.direction_mode == "long_only" else -portfolio_config.max_net_leverage,
        max_budget=1.0 if strategy_config.direction_mode == "long_only" else portfolio_config.max_net_leverage,
        max_short=0.0 if strategy_config.direction_mode == "long_only" else portfolio_config.max_gross_leverage,
        max_long=1.0 if strategy_config.direction_mode == "long_only" else portfolio_config.max_gross_leverage,
        max_turnover=risk_constraints.max_turnover_per_rebalance,
        previous_weights=previous_weight_vector,
        transaction_costs=0.0,
        overwrite_expected_return=lambda weights, alpha_values=alpha: cp.sum(
            cp.multiply(alpha_values, weights)
        ),
    )
    fitted = optimizer.fit(returns_matrix)
    weights = np.array(fitted.weights_, dtype=float)
    if weights.shape[0] != len(symbols):
        raise ValueError("skfolio_mean_risk returned an unexpected weight vector")
    return np.clip(weights, min_weights, max_weights)
