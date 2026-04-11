from __future__ import annotations

from typing import Literal

from pydantic import Field

from quant_platform.common.types.core import FrozenModel


class BacktestDiagnostics(FrozenModel):
    performance_metrics: dict[str, float] = Field(default_factory=dict)
    execution_metrics: dict[str, float] = Field(default_factory=dict)
    risk_metrics: dict[str, float] = Field(default_factory=dict)
    signal_metrics: dict[str, float] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class BacktestReport(FrozenModel):
    summary: str
    diagnostics: BacktestDiagnostics
    artifact_uris: dict[str, str] = Field(default_factory=dict)


class BenchmarkSummaryRow(FrozenModel):
    rank: int
    model_name: str
    family: str
    advanced_kind: str = "baseline"
    backend: str
    mean_valid_mae: float
    mean_test_mae: float
    artifact_uri: str


class BenchmarkSummaryArtifact(FrozenModel):
    benchmark_name: str
    dataset_id: str
    data_source: str | None = None
    window_count: int
    ranking_metric: str
    leaderboard: list[BenchmarkSummaryRow]
    reference_consistency: dict[str, object] = Field(default_factory=dict)
    selected_top_k: int = 2
    official_benchmark: bool = False
    baseline_model_names: list[str] = Field(default_factory=list)
    admission_gates: dict[str, dict[str, object]] = Field(default_factory=dict)


class BacktestSummaryRow(FrozenModel):
    model_name: str
    run_id: str
    prediction_frame_uri: str
    research_result_uri: str
    simulation_result_uri: str
    research_metrics: dict[str, float] = Field(default_factory=dict)
    simulation_metrics: dict[str, float] = Field(default_factory=dict)
    divergence_metrics: dict[str, float] = Field(default_factory=dict)
    scenario_metrics: dict[str, float] = Field(default_factory=dict)
    passed_consistency_checks: bool = True
    protocol_metadata: dict[str, object] = Field(default_factory=dict)


class BacktestSummaryArtifact(FrozenModel):
    summary_id: str
    dataset_id: str
    prediction_scope: str
    data_source: str | None = None
    benchmark_name: str | None = None
    request_digest: str
    rows: list[BacktestSummaryRow]
    comparison_warnings: list[str] = Field(default_factory=list)


class PromotionDecisionArtifact(FrozenModel):
    decision_id: str
    dataset_id: str
    model_name: str
    run_id: str
    decision: Literal["PROMOTE", "HOLD", "REJECT"]
    benchmark_gate_passed: bool
    backtest_gate_passed: bool
    default_gate_passed: bool | None = None
    hard_failures: list[str] = Field(default_factory=list)
    soft_warnings: list[str] = Field(default_factory=list)
    supporting_artifacts: dict[str, str] = Field(default_factory=dict)
