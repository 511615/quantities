from __future__ import annotations

from datetime import datetime

from pydantic import Field

from quant_platform.agents.contracts.research import ResearchAgentResponse
from quant_platform.backtest.contracts.backtest import BacktestResult
from quant_platform.backtest.contracts.report import (
    BacktestSummaryArtifact,
    BacktestSummaryRow,
    BenchmarkSummaryArtifact,
    PromotionDecisionArtifact,
)
from quant_platform.common.types.core import ArtifactRef, FrozenModel
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.datasets.contracts.dataset import DatasetRef
from quant_platform.features.contracts.feature_view import FeatureViewRef
from quant_platform.training.contracts.training import FitResult, PredictionFrame
from quant_platform.workflows.contracts.state import WorkflowRunStatus, WorkflowStageResult


class PrepareWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    data_asset_ref: DataAssetRef
    feature_view_ref: FeatureViewRef
    dataset_ref: DatasetRef
    dataset_manifest_uri: str
    quality_report_uri: str | None = None


class TrainWorkflowItem(FrozenModel):
    model_name: str
    fit_result_uri: str
    fit_result: FitResult


class TrainLeaderboardEntry(FrozenModel):
    rank: int
    model_name: str
    metric_name: str
    metric_value: float
    fit_result_uri: str


class TrainWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    dataset_ref: DatasetRef
    items: list[TrainWorkflowItem]
    leaderboard: list[TrainLeaderboardEntry]


class PredictWorkflowItem(FrozenModel):
    model_name: str
    run_id: str
    prediction_frame_uri: str
    prediction_frame: PredictionFrame


class PredictWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    dataset_ref: DatasetRef
    items: list[PredictWorkflowItem]


class BenchmarkResultRow(FrozenModel):
    rank: int = 0
    model_name: str
    family: str
    advanced_kind: str = "baseline"
    backend: str
    window_count: int
    mean_valid_mae: float
    mean_test_mae: float
    artifact_uri: str


class BenchmarkWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    dataset_ref: DatasetRef
    benchmark_name: str
    benchmark_type: str = "workflow"
    data_source: str | None = None
    results: list[BenchmarkResultRow]
    leaderboard: list[BenchmarkResultRow]
    deep_backend_comparison: list[dict[str, object]] = Field(default_factory=list)
    validation_summary: dict[str, object] = Field(default_factory=dict)
    benchmark_summary: BenchmarkSummaryArtifact | None = None
    summary_artifacts: list[ArtifactRef] = Field(default_factory=list)


class BacktestWorkflowItem(FrozenModel):
    model_name: str
    run_id: str
    prediction_frame_uri: str
    backtest_result_uri: str
    backtest_result: BacktestResult
    research_backtest_result_uri: str
    research_backtest_result: BacktestResult
    simulation_backtest_result_uri: str
    simulation_backtest_result: BacktestResult
    summary_row: BacktestSummaryRow | None = None


class BacktestWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    items: list[BacktestWorkflowItem]
    leaderboard: list[BacktestSummaryRow] = Field(default_factory=list)
    backtest_summary: BacktestSummaryArtifact | None = None
    summary_artifacts: list[ArtifactRef] = Field(default_factory=list)
    promotion_decisions: list[PromotionDecisionArtifact] = Field(default_factory=list)


class ReviewWorkflowResult(FrozenModel):
    stage_result: WorkflowStageResult
    response_uri: str
    response: ResearchAgentResponse


class ReviewWorkflowRecord(FrozenModel):
    request_id: str
    goal: str
    created_at: datetime
    experiment_refs: list[ArtifactRef] = Field(default_factory=list)
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    comparison_mode: str | None = None
    response_uri: str
    audit_log_uri: str


class WorkflowRunResult(FrozenModel):
    workflow_id: str
    completed_stages: list[str]
    stage_results: dict[str, WorkflowStageResult]
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    status: WorkflowRunStatus
