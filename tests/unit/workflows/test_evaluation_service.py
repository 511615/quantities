from __future__ import annotations

from pathlib import Path

from quant_platform.backtest.contracts.report import (
    BacktestSummaryArtifact,
    BacktestSummaryRow,
    BenchmarkSummaryArtifact,
    BenchmarkSummaryRow,
)
from quant_platform.workflows.contracts.results import (
    BacktestWorkflowResult,
    BenchmarkResultRow,
    BenchmarkWorkflowResult,
)
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.services.evaluation import EvaluationWorkflowService


def test_evaluation_service_writes_promotion_decision_artifacts(workflow_runtime, facade) -> None:
    dataset_ref = facade.build_smoke_dataset()
    benchmark_summary = BenchmarkSummaryArtifact(
        benchmark_name="baseline_family_walk_forward",
        dataset_id=dataset_ref.dataset_id,
        data_source="smoke",
        window_count=1,
        ranking_metric="mean_test_mae",
        leaderboard=[
            BenchmarkSummaryRow(
                rank=1,
                model_name="elastic_net",
                family="linear",
                advanced_kind="baseline",
                backend="native",
                mean_valid_mae=0.1,
                mean_test_mae=0.2,
                artifact_uri="artifact://benchmark",
            )
        ],
        reference_consistency={"top_model_consistent": True},
        selected_top_k=2,
        official_benchmark=True,
        baseline_model_names=["elastic_net"],
        admission_gates={"elastic_net": {"default_gate_passed": True}},
    )
    backtest_summary = BacktestSummaryArtifact(
        summary_id="summary-1",
        dataset_id=dataset_ref.dataset_id,
        prediction_scope="full",
        data_source="smoke",
        benchmark_name="baseline_family_walk_forward",
        request_digest="digest-1",
        rows=[
            BacktestSummaryRow(
                model_name="elastic_net",
                run_id="run-1",
                prediction_frame_uri="artifact://prediction",
                research_result_uri="artifact://research",
                simulation_result_uri="artifact://simulation",
                research_metrics={
                    "max_drawdown": 0.10,
                    "turnover_total": 1.0,
                    "risk_trigger_count": 0.0,
                },
                simulation_metrics={
                    "max_drawdown": 0.12,
                    "turnover_total": 1.2,
                    "risk_trigger_count": 0.0,
                },
                divergence_metrics={
                    "simulation_minus_research_alpha_pnl": -5.0,
                    "simulation_minus_research_cumulative_return": -0.01,
                    "simulation_minus_research_drawdown": 0.02,
                    "simulation_minus_research_shortfall": 0.01,
                },
                scenario_metrics={
                    "cost_x2_return_delta": -0.02,
                    "cost_x5_return_delta": -0.05,
                    "latency_shock_return_delta": -0.03,
                    "liquidity_drought_return_delta": -0.04,
                    "worst_scenario_return_delta": -0.10,
                    "stress_fail_count": 0.0,
                },
                passed_consistency_checks=True,
            )
        ],
        comparison_warnings=[],
    )
    benchmark_result = BenchmarkWorkflowResult(
        stage_result=_stage_result(WorkflowStageName.BENCHMARK),
        dataset_ref=dataset_ref,
        benchmark_name="baseline_family_walk_forward",
        data_source="smoke",
        results=[
            BenchmarkResultRow(
                rank=1,
                model_name="elastic_net",
                family="linear",
                backend="native",
                window_count=1,
                mean_valid_mae=0.1,
                mean_test_mae=0.2,
                artifact_uri="artifact://benchmark",
            )
        ],
        leaderboard=[
            BenchmarkResultRow(
                rank=1,
                model_name="elastic_net",
                family="linear",
                backend="native",
                window_count=1,
                mean_valid_mae=0.1,
                mean_test_mae=0.2,
                artifact_uri="artifact://benchmark",
            )
        ],
        benchmark_summary=benchmark_summary,
    )
    backtest_result = BacktestWorkflowResult(
        stage_result=_stage_result(WorkflowStageName.BACKTEST),
        items=[],
        backtest_summary=backtest_summary,
    )

    decisions, artifacts = EvaluationWorkflowService(workflow_runtime).build_promotion_decision(
        benchmark_result,
        backtest_result,
    )

    assert decisions[0].decision == "PROMOTE"
    assert decisions[0].default_gate_passed is True
    artifact_map = {artifact.kind: artifact.uri for artifact in artifacts}
    assert Path(artifact_map["promotion_decision_json"]).exists()
    assert Path(artifact_map["promotion_decision_markdown"]).exists()


def _stage_result(stage: WorkflowStageName) -> WorkflowStageResult:
    return WorkflowStageResult(
        stage=stage,
        status=WorkflowStageStatus.SUCCESS,
        request_digest="digest",
    )
