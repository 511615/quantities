from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.backtest.contracts.portfolio import RiskConstraintSet
from quant_platform.backtest.contracts.signal import SignalFrame, SignalRecord
from quant_platform.backtest.strategy.portfolio_construction import build_target_instructions


class _FakeVectorbtPortfolio:
    def __init__(self, close: pd.DataFrame) -> None:
        self._close = close

    def value(self) -> pd.Series:
        values = [100_000.0 + float(index) * 500.0 for index in range(len(self._close.index))]
        return pd.Series(values, index=self._close.index)


class _FakeVectorbtModule:
    class Portfolio:
        @staticmethod
        def from_orders(*, close, **kwargs):  # type: ignore[no-untyped-def]
            _ = kwargs
            return _FakeVectorbtPortfolio(close)


def _multi_asset_market_bars():
    base = datetime(2024, 1, 1, tzinfo=UTC)
    rows = []
    for offset, btc_close, eth_close in [
        (0, 100.0, 80.0),
        (1, 101.0, 80.5),
        (2, 102.5, 81.5),
        (3, 103.0, 82.0),
        (4, 104.0, 83.0),
        (5, 105.0, 84.0),
        (6, 106.0, 84.5),
        (7, 107.0, 85.0),
        (8, 108.0, 86.0),
        (9, 109.0, 87.0),
    ]:
        event_time = base.replace(hour=offset)
        rows.extend(
            [
                _bar(event_time, "BTCUSDT", btc_close),
                _bar(event_time, "ETHUSDT", eth_close),
            ]
        )
    return rows


def _bar(event_time: datetime, symbol: str, close: float):
    from quant_platform.data.contracts.market import NormalizedMarketBar

    return NormalizedMarketBar(
        event_time=event_time,
        available_time=event_time,
        symbol=symbol,
        venue="binance",
        open=close - 0.5,
        high=close + 0.5,
        low=close - 1.0,
        close=close,
        volume=100.0,
    )


def _signal_frame() -> SignalFrame:
    timestamp = datetime(2024, 1, 1, 9, tzinfo=UTC)
    return SignalFrame(
        rows=[
            SignalRecord(
                signal_id="btc",
                model_run_id="run-1",
                instrument="BTCUSDT",
                venue="binance",
                signal_time=timestamp,
                available_time=timestamp,
                tradable_from=timestamp,
                signal_type="score",
                raw_value=0.8,
                confidence=0.9,
                direction_mode="long_short",
                meta={},
            ),
            SignalRecord(
                signal_id="eth",
                model_run_id="run-1",
                instrument="ETHUSDT",
                venue="binance",
                signal_time=timestamp,
                available_time=timestamp,
                tradable_from=timestamp,
                signal_type="score",
                raw_value=0.4,
                confidence=0.8,
                direction_mode="long_short",
                meta={},
            ),
        ]
    )


def test_vectorbt_backend_runs_when_explicitly_selected(monkeypatch: pytest.MonkeyPatch, facade) -> None:
    fit_result = facade.train_smoke()
    prediction_frame = facade.build_prediction_frame(fit_result)

    def _fake_import(name: str):
        if name == "pandas":
            return pd
        if name == "vectorbt":
            return _FakeVectorbtModule
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        "quant_platform.backtest.engines.vectorbt_adapter.importlib.import_module",
        _fake_import,
    )

    request = BacktestRequest(
        prediction_frame_uri=str(facade.artifact_root / "predictions" / "smoke.json"),
        research_backend="vectorbt",
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=0.0, slippage_bps=0.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
    )

    result = facade.backtest_facade.run_research(
        request=request,
        prediction_frame=prediction_frame,
        market_bars=facade.build_smoke_market_bars(),
    )

    diagnostics = facade.store.read_json(result.diagnostics_uri or "")
    assert result.backtest_id
    assert diagnostics["performance_metrics"]["cumulative_return"] > 0.0


def test_skfolio_portfolio_method_builds_multi_asset_weights(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeMeanRisk:
        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            self.kwargs = kwargs
            self.weights_ = np.array([0.6, 0.4], dtype=float)

        def fit(self, X):  # type: ignore[no-untyped-def]
            assert X.shape[1] == 2
            return self

    def _fake_import(name: str):
        if name == "skfolio.optimization":
            return SimpleNamespace(
                MeanRisk=_FakeMeanRisk,
                ObjectiveFunction=SimpleNamespace(MAXIMIZE_UTILITY="MAXIMIZE_UTILITY"),
            )
        if name == "skfolio.measures":
            return SimpleNamespace(RiskMeasure=SimpleNamespace(VARIANCE="variance"))
        if name == "cvxpy":
            return SimpleNamespace(
                sum=lambda value: value,
                multiply=lambda left, right: (left, right),
            )
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        "quant_platform.backtest.strategy.portfolio_construction.importlib.import_module",
        _fake_import,
    )

    instructions = build_target_instructions(
        signal_frame=_signal_frame(),
        strategy_config=StrategyConfig(name="sign_strategy", portfolio_method="skfolio_mean_risk"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=0.7,
        ),
        risk_constraints=RiskConstraintSet(
            mode="long_short",
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=0.7,
            max_single_name_notional=70000.0,
            max_turnover_per_rebalance=1.0,
            max_daily_turnover=24.0,
            max_drawdown_hard_stop=1.0,
            max_concentration_hhi=1.0,
            min_cash_buffer=0.0,
            max_participation_rate=1.0,
            max_order_notional=70000.0,
        ),
        market_bars=_multi_asset_market_bars(),
    )

    weights = {item.instrument: item.target_value for item in instructions}
    assert weights == {"BTCUSDT": 0.6, "ETHUSDT": 0.4}


def test_skfolio_portfolio_method_rejects_single_asset() -> None:
    single_asset_frame = SignalFrame(rows=[_signal_frame().rows[0]])

    with pytest.raises(ValueError, match="at least two instruments"):
        build_target_instructions(
            signal_frame=single_asset_frame,
            strategy_config=StrategyConfig(name="sign_strategy", portfolio_method="skfolio_mean_risk"),
            portfolio_config=PortfolioConfig(
                initial_cash=100000.0,
                max_gross_leverage=1.0,
                max_net_leverage=1.0,
                max_position_weight=0.7,
            ),
            risk_constraints=RiskConstraintSet(
                mode="long_short",
                max_gross_leverage=1.0,
                max_net_leverage=1.0,
                max_position_weight=0.7,
                max_single_name_notional=70000.0,
                max_turnover_per_rebalance=1.0,
                max_daily_turnover=24.0,
                max_drawdown_hard_stop=1.0,
                max_concentration_hhi=1.0,
                min_cash_buffer=0.0,
                max_participation_rate=1.0,
                max_order_notional=70000.0,
            ),
            market_bars=_multi_asset_market_bars(),
        )
