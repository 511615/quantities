from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.orchestration import OrchestrationRequest
from quant_platform.agents.contracts.research import ResearchAgentRequest
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
from quant_platform.workflows.contracts.state import WorkflowStageName
from quant_platform.workflows.services.prepare import PrepareWorkflowService


def test_orchestration_blocks_unauthorized_tool_and_writes_audit_bundle(workflow_runtime) -> None:
    result = workflow_runtime.agent_service.orchestrate(
        OrchestrationRequest(
            request=ResearchAgentRequest(
                request_id="blocked-auth-review",
                task_type="summarize_experiment",
                goal="review with unauthorized tool",
                allowed_tools=["unknown.tool"],
            )
        )
    )
    assert result.final_response.status == "blocked"
    assert result.audit_bundle.failure_events
    assert result.audit_bundle.failure_events[0].code == "authorization_blocked"
    assert Path(result.final_response.audit_log_uri).exists()


def test_workflow_review_uses_complete_audit_bundle(workflow_pipeline) -> None:
    prepare_service = PrepareWorkflowService(workflow_pipeline.runtime)
    prepare_request = prepare_service.build_smoke_request()

    result = workflow_pipeline.run(
        WorkflowRunRequest(
            workflow_id="orchestration-review",
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
                    experiment_name="orchestration-review",
                    tracking_uri=str(workflow_pipeline.runtime.artifact_root / "tracking"),
                ),
                seed=7,
                run_id_prefix="orchestration-review",
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
                request_id="orchestration-review-request",
                goal="Summarize workflow outputs",
                task_type=AgentTaskType.SUMMARIZE_EXPERIMENT.value,
                comparison_mode="report_summary",
                allowed_tools=[
                    "research.read_experiment_summary",
                    "research.compare_backtest_reports",
                ],
            ),
        )
    )

    audit_refs = [
        artifact.uri for artifact in result.artifact_refs if artifact.kind == "review_audit"
    ]
    assert audit_refs
    payload = workflow_pipeline.runtime.store.read_json(audit_refs[0])
    assert payload["completeness_checks"]["is_complete"] is True
    assert payload["route_decision"]["resolved_agent"] == "research"
