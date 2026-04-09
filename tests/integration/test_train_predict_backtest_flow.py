from __future__ import annotations

from pathlib import Path

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)


def test_train_predict_backtest_flow(facade) -> None:
    fit_result = facade.train_smoke()
    prediction_frame = facade.build_prediction_frame(fit_result)
    backtest_request = BacktestRequest(
        prediction_frame_uri=str(facade.artifact_root / "backtests" / "smoke_predictions.json"),
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
    )
    backtest_result = facade.backtest_engine.run(backtest_request, prediction_frame)
    assert Path(fit_result.model_artifact_uri).exists()
    assert Path(fit_result.train_manifest_uri).exists()
    assert prediction_frame.sample_count == len(facade.dataset_store["smoke_dataset"])
    assert Path(backtest_result.orders_uri).exists()
    assert Path(backtest_result.fills_uri).exists()
    assert Path(backtest_result.positions_uri).exists()
    assert Path(backtest_result.pnl_uri).exists()
    assert Path(backtest_result.report_uri).exists()
    assert Path(backtest_result.diagnostics_uri).exists()
    assert Path(backtest_result.leakage_audit_uri).exists()
    assert backtest_result.risk_metrics["position_count"] >= 1.0
    assert backtest_result.risk_metrics["gross_exposure"] >= 0.0
