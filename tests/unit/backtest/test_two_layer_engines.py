from __future__ import annotations

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    ExecutionConfig,
    LatencyConfig,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.io.files import LocalArtifactStore


def test_research_and_simulation_engines_are_consistent_in_frictionless_mode(facade) -> None:
    fit_result = facade.train_smoke()
    prediction_frame = facade.build_prediction_frame(fit_result)
    deep_liquidity_bars = [
        bar.model_copy(update={"volume": 1_000_000.0}) for bar in facade.build_smoke_market_bars()
    ]
    request = BacktestRequest(
        prediction_frame_uri=str(facade.artifact_root / "backtests" / "smoke_predictions.json"),
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=0.0, slippage_bps=0.0),
        execution_config=ExecutionConfig(latency_config=LatencyConfig()),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
    )
    research = facade.backtest_facade.run_research(
        request=request,
        prediction_frame=prediction_frame,
        market_bars=deep_liquidity_bars,
    )
    simulation = facade.backtest_facade.run_simulation(
        request=request,
        prediction_frame=prediction_frame,
        market_bars=deep_liquidity_bars,
    )
    store = LocalArtifactStore(facade.artifact_root)
    research_pnl = store.read_json(research.pnl_uri)
    simulation_pnl = store.read_json(simulation.pnl_uri)
    assert research.risk_metrics["gross_exposure"] == simulation.risk_metrics["gross_exposure"]
    assert research_pnl["alpha_pnl"] == simulation_pnl["alpha_pnl"]
