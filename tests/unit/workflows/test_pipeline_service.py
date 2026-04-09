from __future__ import annotations

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.enums.core import AgentTaskType
from quant_platform.training.contracts.training import (
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    PredictWorkflowRequest,
    ReviewWorkflowRequest,
    TrainWorkflowRequest,
    WorkflowRunRequest,
)
from quant_platform.workflows.contracts.state import WorkflowStageName, WorkflowRunStatus
from quant_platform.workflows.services.prepare import PrepareWorkflowService


def test_pipeline_service_runs_prepare_train_predict_backtest_review(workflow_pipeline) -> None:
    prepare_service = PrepareWorkflowService(workflow_pipeline.runtime)
    prepare_request = prepare_service.build_smoke_request()

    result = workflow_pipeline.run(
        WorkflowRunRequest(
            workflow_id="unit-pipeline",
            stages=[
                WorkflowStageName.PREPARE,
                WorkflowStageName.TRAIN,
                WorkflowStageName.PREDICT,
                WorkflowStageName.BACKTEST,
                WorkflowStageName.REVIEW,
            ],
            prepare=prepare_request,
            train=TrainWorkflowRequest(
                dataset_ref=None,
                model_specs=[prepare_service.build_smoke_model_spec()],
                trainer_config=TrainerConfig(
                    runner="local",
                    epochs=1,
                    batch_size=32,
                    deterministic=True,
                ),
                tracking_context=TrackingContext(
                    backend="file",
                    experiment_name="unit-pipeline",
                    tracking_uri=str(workflow_pipeline.runtime.artifact_root / "tracking"),
                ),
                seed=7,
                run_id_prefix="unit-pipeline",
            ),
            predict=PredictWorkflowRequest(
                dataset_ref=None,
                prediction_scope=PredictionScope(
                    scope_name="full",
                    as_of_time=prepare_request.as_of_time,
                ),
            ),
            backtest=BacktestWorkflowRequest(
                backtest_request_template=BacktestRequest(
                    prediction_frame_uri=str(
                        workflow_pipeline.runtime.artifact_root / "predictions" / "placeholder.json"
                    ),
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
            ),
            review=ReviewWorkflowRequest(
                request_id="unit-pipeline-review",
                goal="Review smoke workflow outputs",
                task_type=AgentTaskType.SUMMARIZE_EXPERIMENT.value,
                comparison_mode="report_summary",
                allowed_tools=[
                    "research.read_experiment_summary",
                    "research.compare_backtest_reports",
                ],
            ),
        )
    )

    assert result.status == WorkflowRunStatus.SUCCESS
    assert result.completed_stages == ["prepare", "train", "predict", "backtest", "review"]
    assert set(result.stage_results) == {"prepare", "train", "predict", "backtest", "review"}
    assert result.artifact_refs
