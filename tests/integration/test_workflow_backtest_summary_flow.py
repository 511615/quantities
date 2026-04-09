from __future__ import annotations

from pathlib import Path

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
from quant_platform.training.contracts.training import (
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    PredictWorkflowRequest,
    PredictionInputRef,
    TrainWorkflowRequest,
)


def test_workflow_backtest_produces_dual_engine_summary_artifacts(facade) -> None:
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    train_result = facade.train_workflow.train(
        TrainWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            model_specs=[facade.build_smoke_model_spec()],
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=32,
                deterministic=True,
            ),
            tracking_context=TrackingContext(
                backend="file",
                experiment_name="workflow-backtest-summary",
                tracking_uri=str(facade.artifact_root / "tracking"),
            ),
            seed=7,
            run_id_prefix="workflow-backtest-summary",
        )
    )
    predict_result = facade.predict_workflow.predict(
        PredictWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            fit_results=[train_result.items[0].fit_result],
            prediction_scope=PredictionScope(
                scope_name="full",
                as_of_time=prepare_result.dataset_ref.feature_view_ref.as_of_time,
            ),
        )
    )
    deep_liquidity_bars = [
        bar.model_copy(update={"volume": 1_000_000.0}) for bar in facade.build_smoke_market_bars()
    ]
    result = facade.backtest_workflow.backtest(
        BacktestWorkflowRequest(
            prediction_inputs=[
                PredictionInputRef(
                    model_name=train_result.items[0].model_name,
                    run_id=train_result.items[0].fit_result.run_id,
                    prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
                )
            ],
            backtest_request_template=BacktestRequest(
                prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
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
            ),
            dataset_ref=prepare_result.dataset_ref,
            benchmark_name="smoke_protocol",
            data_source="smoke",
            market_bars=deep_liquidity_bars,
        )
    )

    assert result.stage_result.summary.startswith("executed 1 backtest")
    assert result.backtest_summary is not None
    assert result.backtest_summary.dataset_id == prepare_result.dataset_ref.dataset_id
    assert len(result.items) == 1
    item = result.items[0]
    assert item.backtest_result.engine_type == "research"
    assert item.research_backtest_result.engine_type == "research"
    assert item.simulation_backtest_result.engine_type == "simulation"
    assert item.summary_row is not None
    assert "simulation_minus_research_cumulative_return" in item.summary_row.divergence_metrics
    assert "stress_fail_count" in item.summary_row.scenario_metrics
    assert result.leaderboard[0].model_name == train_result.items[0].model_name
    summary_artifacts = {artifact.kind: artifact.uri for artifact in result.summary_artifacts}
    assert Path(summary_artifacts["backtest_summary_json"]).exists()
    assert Path(summary_artifacts["backtest_summary_markdown"]).exists()
    assert Path(summary_artifacts["backtest_summary_csv"]).exists()
