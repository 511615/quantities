from __future__ import annotations

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.io.files import LocalArtifactStore


def test_prediction_and_backtest_consume_only_prediction_timestamps(facade) -> None:
    fit_result = facade.train_smoke()
    prediction_frame = facade.build_prediction_frame(fit_result)
    dataset_samples = facade.dataset_store["smoke_dataset"]
    assert [row.timestamp for row in prediction_frame.rows] == [
        sample.timestamp for sample in dataset_samples
    ]
    backtest_request = BacktestRequest(
        prediction_frame_uri=str(facade.artifact_root / "backtests" / "smoke_predictions.json"),
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0, max_gross_leverage=1.0, max_position_weight=1.0
        ),
        cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
    )
    result = facade.backtest_engine.run(backtest_request, prediction_frame)
    assert result.risk_metrics["position_count"] >= 1.0
    leakage_audit = LocalArtifactStore(facade.artifact_root).read_json(result.leakage_audit_uri)
    assert leakage_audit["future_feature_check"] is True
    assert leakage_audit["stale_or_negative_latency_check"] is True
