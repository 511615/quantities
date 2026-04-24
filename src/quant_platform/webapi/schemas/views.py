from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeepLinkView(ApiModel):
    kind: str
    label: str
    href: str
    api_path: str | None = None


class StableSummaryView(ApiModel):
    status: str | None = None
    headline: str
    detail: str | None = None
    highlights: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)


class ArtifactView(ApiModel):
    kind: str
    label: str
    uri: str
    exists: bool = True
    previewable: bool = True


class GlossaryHintView(ApiModel):
    key: str
    term: str
    short: str


class WarningSummaryView(ApiModel):
    level: str = "none"
    count: int = 0
    items: list[str] = Field(default_factory=list)


class ModalityQualityView(ApiModel):
    modality: str
    status: str = "unknown"
    blocking_reasons: list[str] = Field(default_factory=list)
    usable_count: int | None = None
    coverage_ratio: float | None = None
    duplicate_ratio: float | None = None
    max_gap_bars: int | None = None
    freshness_lag_days: float | None = None
    non_null_coverage_ratio: float | None = None
    required_feature_names: list[str] = Field(default_factory=list)
    observed_feature_names: list[str] = Field(default_factory=list)
    source_frequency: str | None = None
    training_frequency: str | None = None
    alignment_policy: str | None = None
    forward_fill_enabled: bool | None = None
    hours_since_update: float | None = None
    ffill_span: int | None = None


class PredictionArtifactView(ApiModel):
    scope: str
    sample_count: int
    uri: str


class RelatedBacktestView(ApiModel):
    backtest_id: str
    model_name: str
    run_id: str
    annual_return: float | None = None
    max_drawdown: float | None = None
    passed_consistency_checks: bool | None = None
    research_backend: str | None = None
    portfolio_method: str | None = None


class PipelineStageView(ApiModel):
    stage: str
    status: str
    summary: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    links: list[DeepLinkView] = Field(default_factory=list)


class PipelineSummaryView(ApiModel):
    status: str
    current_stage: str | None = None
    requested_stages: list[str] = Field(default_factory=list)
    completed_stages: list[str] = Field(default_factory=list)
    stages: list[PipelineStageView] = Field(default_factory=list)


class ReviewSummaryView(ApiModel):
    request_id: str | None = None
    status: str
    title: str | None = None
    summary: str | None = None
    summary_view: StableSummaryView | None = None
    observations: list[str] = Field(default_factory=list)
    proposed_actions: list[str] = Field(default_factory=list)
    suggested_actions: list[str] = Field(default_factory=list)
    audit_log_uri: str | None = None


class DatasetFreshnessView(ApiModel):
    as_of_time: datetime | None = None
    data_start_time: datetime | None = None
    data_end_time: datetime | None = None
    lag_seconds: float | None = None
    status: str = "unknown"
    summary: str = ""


class DataFreshnessView(ApiModel):
    dataset_id: str | None = None
    as_of_time: datetime | None = None
    freshness: str = "unknown"
    source: str | None = None


class DatasetLinkView(ApiModel):
    dataset_id: str
    label: str
    href: str
    api_path: str | None = None
    role: str | None = None
    modality: str | None = None


class RunCompositionSourceView(ApiModel):
    run_id: str
    model_name: str
    modality: str | None = None
    weight: float | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)


class RunCompositionView(ApiModel):
    fusion_strategy: str
    source_runs: list[RunCompositionSourceView] = Field(default_factory=list)
    rules: list[str] = Field(default_factory=list)
    requested_fusion_strategy: str | None = None
    effective_fusion_strategy: str | None = None
    attention_summary: dict[str, Any] = Field(default_factory=dict)
    explainability_uri: str | None = None
    required_modalities: list[str] = Field(default_factory=list)
    required_feature_names: list[str] = Field(default_factory=list)
    aligned_prediction_sample_count: int | None = None
    official_contract: dict[str, Any] = Field(default_factory=dict)


class BacktestAlignmentView(ApiModel):
    fusion_strategy: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)
    alignment_status: str | None = None
    notes: list[str] = Field(default_factory=list)


class DatasetSummaryView(ApiModel):
    dataset_id: str
    display_name: str | None = None
    subtitle: str | None = None
    dataset_category: str | None = None
    data_domain: str | None = None
    data_domains: list[str] = Field(default_factory=list)
    dataset_type: str | None = None
    asset_id: str | None = None
    data_source: str | None = None
    frequency: str | None = None
    as_of_time: datetime | None = None
    requested_at: datetime | None = None
    sample_count: int | None = None
    row_count: int | None = None
    feature_count: int | None = None
    label_count: int | None = None
    label_horizon: int | None = None
    split_strategy: str | None = None
    time_range_label: str | None = None
    is_smoke: bool = False
    freshness: DatasetFreshnessView
    temporal_safety_summary: str
    source_vendor: str | None = None
    exchange: str | None = None
    entity_scope: str | None = None
    entity_count: int | None = None
    symbols_preview: list[str] = Field(default_factory=list)
    snapshot_version: str | None = None
    quality_status: str | None = None
    readiness_status: str | None = None
    build_status: str | None = None
    request_origin: str | None = None
    is_system_recommended: bool = False
    is_protected: bool = False
    deletion_policy: str | None = None
    download_available: bool = True
    links: list[DeepLinkView] = Field(default_factory=list)


class RecommendedActionView(ApiModel):
    key: str = ""
    action_id: str = ""
    title: str
    description: str
    target_path: str | None = None
    href: str | None = None
    severity: str = "info"


class ExperimentListItem(ApiModel):
    run_id: str
    model_name: str
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)
    primary_dataset_id: str | None = None
    composition: RunCompositionView | None = None
    family: str | None = None
    backend: str | None = None
    status: str
    created_at: datetime | None = None
    primary_metric_name: str | None = None
    primary_metric_value: float | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    backtest_count: int = 0
    prediction_scopes: list[str] = Field(default_factory=list)
    official_template_eligible: bool | None = None
    official_blocking_reasons: list[str] = Field(default_factory=list)
    feature_scope_modality: str | None = None
    feature_scope_feature_names: list[str] = Field(default_factory=list)
    source_dataset_quality_status: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class ExperimentsResponse(ApiModel):
    items: list[ExperimentListItem]
    total: int
    page: int
    per_page: int
    available_models: list[str] = Field(default_factory=list)
    available_datasets: list[str] = Field(default_factory=list)
    available_statuses: list[str] = Field(default_factory=list)


class RunDetailView(ApiModel):
    run_id: str
    model_name: str
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)
    primary_dataset_id: str | None = None
    composition: RunCompositionView | None = None
    task_type: str | None = None
    artifact_format_status: str = "legacy"
    missing_artifacts: list[str] = Field(default_factory=list)
    family: str | None = None
    backend: str | None = None
    status: str
    created_at: datetime | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    tracking_params: dict[str, str] = Field(default_factory=dict)
    manifest_metrics: dict[str, float] = Field(default_factory=dict)
    repro_context: dict[str, Any] = Field(default_factory=dict)
    dataset_summary: dict[str, Any] = Field(default_factory=dict)
    evaluation_summary: dict[str, Any] = Field(default_factory=dict)
    evaluation_artifacts: list[ArtifactView] = Field(default_factory=list)
    prediction_summary: dict[str, Any] = Field(default_factory=dict)
    time_range: dict[str, Any] = Field(default_factory=dict)
    feature_importance: dict[str, float] = Field(default_factory=dict)
    predictions: list[PredictionArtifactView] = Field(default_factory=list)
    related_backtests: list[RelatedBacktestView] = Field(default_factory=list)
    artifacts: list[ArtifactView] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    official_template_eligible: bool | None = None
    official_blocking_reasons: list[str] = Field(default_factory=list)
    feature_scope_modality: str | None = None
    feature_scope_feature_names: list[str] = Field(default_factory=list)
    source_dataset_quality_status: str | None = None
    lstm_window_spec: dict[str, Any] = Field(default_factory=dict)
    lstm_subsequence_spec: dict[str, Any] = Field(default_factory=dict)
    rolling_window_spec: dict[str, Any] = Field(default_factory=dict)
    effective_alignment_policy: str | None = None
    feature_frequency_profile: dict[str, Any] = Field(default_factory=dict)
    summary: StableSummaryView | None = None
    pipeline_summary: PipelineSummaryView | None = None
    review_summary: ReviewSummaryView | None = None
    warning_summary: WarningSummaryView | None = None
    glossary_hints: list[GlossaryHintView] = Field(default_factory=list)


class BenchmarkRowView(ApiModel):
    rank: int
    model_name: str
    family: str
    advanced_kind: str = "baseline"
    backend: str
    window_count: int = 0
    mean_valid_mae: float
    mean_test_mae: float
    artifact_uri: str | None = None


class BacktestTemplateView(ApiModel):
    template_id: str
    name: str
    description: str | None = None
    source: str = "system"
    read_only: bool = True
    official: bool = False
    protocol_version: str | None = None
    output_contract_version: str | None = None
    fixed_prediction_scope: str | None = None
    ranking_policy: str | None = None
    slice_policy: str | None = None
    scenario_bundle: list[str] = Field(default_factory=list)
    eligibility_rules: list[str] = Field(default_factory=list)
    eligibility_rule_keys: list[str] = Field(default_factory=list)
    required_metadata: list[str] = Field(default_factory=list)
    required_metadata_keys: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    note_keys: list[str] = Field(default_factory=list)


class GateResultView(ApiModel):
    key: str
    label: str
    label_key: str | None = None
    passed: bool | None = None
    severity: str = "info"
    detail: str | None = None
    detail_key: str | None = None


class RankComponentView(ApiModel):
    key: str
    label: str
    value: float | None = None
    detail: str | None = None


class ProtocolGateFailureView(ApiModel):
    key: str
    label: str
    label_key: str | None = None
    severity: str = "info"
    reasons: list[str] = Field(default_factory=list)
    reason_keys: list[str] = Field(default_factory=list)


class BacktestExecutionDiagnosticsSummaryView(ApiModel):
    signal_count: float | None = None
    order_count: float | None = None
    eligible_order_count: float | None = None
    blocked_order_count: float | None = None
    fill_count: float | None = None
    position_open_count: float | None = None
    block_reasons: list[str] = Field(default_factory=list)


class BacktestProtocolResultView(ApiModel):
    template: BacktestTemplateView | None = None
    gate_status: str | None = None
    gate_results: list[GateResultView] = Field(default_factory=list)
    protocol_gate_failures: list[ProtocolGateFailureView] = Field(default_factory=list)
    rank_components: list[RankComponentView] = Field(default_factory=list)
    slice_id: str | None = None
    slice_coverage: list[str] = Field(default_factory=list)
    lookback_bucket: str | None = None
    metadata_summary: dict[str, str | None] = Field(default_factory=dict)
    missing_required_metadata_keys: list[str] = Field(default_factory=list)
    missing_required_metadata_labels: list[str] = Field(default_factory=list)
    missing_stress_scenarios: list[str] = Field(default_factory=list)
    required_modalities: list[str] = Field(default_factory=list)
    official_dataset_ids: list[str] = Field(default_factory=list)
    actual_market_start_time: datetime | None = None
    actual_market_end_time: datetime | None = None
    actual_backtest_start_time: datetime | None = None
    actual_backtest_end_time: datetime | None = None
    actual_nlp_start_time: datetime | None = None
    actual_nlp_end_time: datetime | None = None
    nlp_gate_status: str | None = None
    nlp_gate_reasons: list[str] = Field(default_factory=list)
    nlp_gate_reason_keys: list[str] = Field(default_factory=list)
    modality_quality_summary: dict[str, ModalityQualityView] = Field(default_factory=dict)
    quality_blocking_reasons: list[str] = Field(default_factory=list)
    official_benchmark_version: str | None = None
    official_window_days: int | None = None
    official_window_start_time: datetime | None = None
    official_window_end_time: datetime | None = None
    official_market_dataset_id: str | None = None
    official_multimodal_dataset_id: str | None = None


class BenchmarkListItemView(ApiModel):
    benchmark_name: str
    dataset_id: str
    data_source: str | None = None
    benchmark_type: str = "workflow"
    updated_at: datetime
    top_model_name: str | None = None
    top_model_score: float | None = None


class BenchmarkDetailView(ApiModel):
    benchmark_name: str
    dataset_id: str
    data_source: str | None = None
    benchmark_type: str = "workflow"
    updated_at: datetime
    window_count: int = 0
    leaderboard: list[BenchmarkRowView] = Field(default_factory=list)
    results: list[BenchmarkRowView] = Field(default_factory=list)
    deep_backend_comparison: list[dict[str, object]] = Field(default_factory=list)
    validation_summary: dict[str, object] = Field(default_factory=dict)
    artifacts: list[ArtifactView] = Field(default_factory=list)
    summary: StableSummaryView | None = None
    review_summary: ReviewSummaryView | None = None
    warning_summary: WarningSummaryView | None = None
    glossary_hints: list[GlossaryHintView] = Field(default_factory=list)


class BacktestListItemView(ApiModel):
    backtest_id: str
    run_id: str | None = None
    model_name: str | None = None
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)
    primary_dataset_id: str | None = None
    status: str
    template_id: str | None = None
    official: bool = False
    protocol_version: str | None = None
    gate_status: str | None = None
    research_backend: str | None = None
    portfolio_method: str | None = None
    passed_consistency_checks: bool | None = None
    annual_return: float | None = None
    max_drawdown: float | None = None
    warning_count: int = 0
    updated_at: datetime | None = None


class BacktestsResponse(ApiModel):
    items: list[BacktestListItemView]
    total: int
    page: int
    per_page: int
    available_statuses: list[str] = Field(default_factory=list)


class TimeValuePoint(ApiModel):
    label: str
    value: float


class ScenarioDeltaView(ApiModel):
    scenario_name: str
    cumulative_return_delta: float


class BacktestEngineView(ApiModel):
    backtest_id: str
    engine_type: str
    report_summary: str | None = None
    metrics: dict[str, float] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    pnl_snapshot: dict[str, float] = Field(default_factory=dict)
    positions: list[TimeValuePoint] = Field(default_factory=list)
    scenarios: list[ScenarioDeltaView] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    artifacts: list[ArtifactView] = Field(default_factory=list)


class BacktestReportView(ApiModel):
    backtest_id: str
    model_name: str | None = None
    run_id: str | None = None
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    datasets: list[DatasetLinkView] = Field(default_factory=list)
    primary_dataset_id: str | None = None
    alignment: BacktestAlignmentView | None = None
    template_id: str | None = None
    official: bool = False
    protocol_version: str | None = None
    research_backend: str | None = None
    portfolio_method: str | None = None
    protocol: BacktestProtocolResultView | None = None
    modality_quality_summary: dict[str, ModalityQualityView] = Field(default_factory=dict)
    quality_blocking_reasons: list[str] = Field(default_factory=list)
    passed_consistency_checks: bool | None = None
    comparison_warnings: list[str] = Field(default_factory=list)
    divergence_metrics: dict[str, float] = Field(default_factory=dict)
    scenario_metrics: dict[str, float] = Field(default_factory=dict)
    research: BacktestEngineView | None = None
    simulation: BacktestEngineView | None = None
    execution_diagnostics_summary: BacktestExecutionDiagnosticsSummaryView | None = None
    artifacts: list[ArtifactView] = Field(default_factory=list)
    summary: StableSummaryView | None = None
    pipeline_summary: PipelineSummaryView | None = None
    review_summary: ReviewSummaryView | None = None
    warning_summary: WarningSummaryView | None = None
    glossary_hints: list[GlossaryHintView] = Field(default_factory=list)


class BacktestDeleteResponse(ApiModel):
    backtest_id: str
    status: str
    message: str
    deleted_files: list[str] = Field(default_factory=list)


class BenchmarkSelection(ApiModel):
    benchmark_name: str
    model_names: list[str] = Field(default_factory=list)


class ModelComparisonRequest(ApiModel):
    run_ids: list[str] = Field(default_factory=list)
    benchmark_selections: list[BenchmarkSelection] = Field(default_factory=list)
    template_id: str | None = None
    official_only: bool = False


class ComparisonRowView(ApiModel):
    row_id: str
    source_type: str
    label: str
    model_name: str
    dataset_id: str | None = None
    backend: str | None = None
    status: str | None = None
    template_id: str | None = None
    official: bool = False
    protocol_version: str | None = None
    gate_status: str | None = None
    train_mae: float | None = None
    mean_valid_mae: float | None = None
    mean_test_mae: float | None = None
    annual_return: float | None = None
    max_drawdown: float | None = None
    turnover_total: float | None = None
    implementation_shortfall: float | None = None


class ModelComparisonView(ApiModel):
    rows: list[ComparisonRowView]
    metrics: list[str] = Field(
        default_factory=lambda: [
            "train_mae",
            "mean_valid_mae",
            "mean_test_mae",
            "annual_return",
            "max_drawdown",
            "turnover_total",
            "implementation_shortfall",
        ]
    )


class JobResultView(ApiModel):
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    base_dataset_id: str | None = None
    fusion_dataset_id: str | None = None
    run_ids: list[str] = Field(default_factory=list)
    backtest_ids: list[str] = Field(default_factory=list)
    benchmark_names: list[str] = Field(default_factory=list)
    template_id: str | None = None
    template_name: str | None = None
    official: bool = False
    protocol_version: str | None = None
    research_backend: str | None = None
    portfolio_method: str | None = None
    fit_result_uris: list[str] = Field(default_factory=list)
    summary_artifacts: list[str] = Field(default_factory=list)
    prediction_scope: str | None = None
    requested_stages: list[str] = Field(default_factory=list)
    deeplinks: dict[str, str | None] = Field(default_factory=dict)
    summary: StableSummaryView | None = None
    pipeline_summary: PipelineSummaryView | None = None
    result_links: list[DeepLinkView] = Field(default_factory=list)
    review_summary: ReviewSummaryView | None = None


class JobStageView(ApiModel):
    name: str
    status: str
    summary: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None


class JobStatusView(ApiModel):
    job_id: str
    job_type: str
    status: str
    created_at: datetime
    updated_at: datetime
    stages: list[JobStageView] = Field(default_factory=list)
    result: JobResultView = Field(default_factory=JobResultView)
    error_message: str | None = None


class RecentJobView(ApiModel):
    job_id: str
    job_type: str
    status: str
    updated_at: datetime
    dataset_id: str | None = None
    summary: StableSummaryView | None = None
    result_links: list[DeepLinkView] = Field(default_factory=list)
    primary_stage: str | None = None
    deeplinks: dict[str, str] = Field(default_factory=dict)


class JobListResponse(ApiModel):
    items: list[JobStatusView]


class WorkbenchOverviewView(ApiModel):
    generated_at: datetime | None = None
    data_updated_at: datetime | None = None
    recent_runs: list[ExperimentListItem] = Field(default_factory=list)
    recent_backtests: list[BacktestListItemView] = Field(default_factory=list)
    recent_benchmarks: list[BenchmarkListItemView] = Field(default_factory=list)
    recent_jobs: list[RecentJobView] = Field(default_factory=list)
    data_freshness: DataFreshnessView | None = None
    datasets: list[DatasetSummaryView] = Field(default_factory=list)
    recommended_actions: list[RecommendedActionView] = Field(default_factory=list)


class ArtifactPreviewResponse(ApiModel):
    uri: str
    kind: str
    is_json: bool
    content: Any


class ModelTemplateView(ApiModel):
    template_id: str
    name: str
    model_name: str
    description: str | None = None
    source: str = "registry"
    hyperparams: dict[str, Any] = Field(default_factory=dict)
    trainer_preset: str = "fast"
    dataset_preset: str = "smoke"
    read_only: bool = False
    model_registered: bool = True
    deleted_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ModelTemplateCreateRequest(ApiModel):
    name: str
    model_name: str
    description: str | None = None
    hyperparams: dict[str, Any] = Field(default_factory=dict)
    trainer_preset: str = "fast"
    dataset_preset: str = "smoke"


class ModelTemplateUpdateRequest(ApiModel):
    name: str | None = None
    description: str | None = None
    hyperparams: dict[str, Any] | None = None
    trainer_preset: str | None = None
    dataset_preset: str | None = None


class ModelTemplateListResponse(ApiModel):
    items: list[ModelTemplateView]
    total: int
    model_options_source: str = "registry"


class TrainedModelSummaryView(ApiModel):
    run_id: str
    model_name: str
    family: str | None = None
    dataset_id: str | None = None
    created_at: datetime | None = None
    status: str = "success"
    metrics: dict[str, float] = Field(default_factory=dict)
    note: str | None = None
    is_deleted: bool = False
    official_template_eligible: bool | None = None
    official_blocking_reasons: list[str] = Field(default_factory=list)
    links: list[DeepLinkView] = Field(default_factory=list)


class TrainedModelDetailView(ApiModel):
    run_id: str
    model_name: str
    family: str | None = None
    dataset_id: str | None = None
    created_at: datetime | None = None
    status: str = "success"
    metrics: dict[str, float] = Field(default_factory=dict)
    note: str | None = None
    is_deleted: bool = False
    official_template_eligible: bool | None = None
    official_blocking_reasons: list[str] = Field(default_factory=list)
    artifacts: list[ArtifactView] = Field(default_factory=list)
    tracking_params: dict[str, str] = Field(default_factory=dict)
    model_spec: dict[str, Any] = Field(default_factory=dict)
    links: list[DeepLinkView] = Field(default_factory=list)


class TrainedModelListResponse(ApiModel):
    items: list[TrainedModelSummaryView]
    total: int


class TrainedModelUpdateRequest(ApiModel):
    note: str | None = None


class DatasetListResponse(ApiModel):
    items: list[DatasetSummaryView]
    total: int
    page: int
    per_page: int


class DatasetFieldGroupView(ApiModel):
    key: str
    label: str
    description: str
    count: int = 0
    columns: list[str] = Field(default_factory=list)


class DatasetQualitySummaryView(ApiModel):
    status: str = "unknown"
    summary: str = ""
    missing_ratio: float | None = None
    duplicate_ratio: float | None = None
    duplicate_rows: int | None = None
    checks: list[str] = Field(default_factory=list)


class DatasetDetailView(ApiModel):
    dataset: DatasetSummaryView
    display_name: str | None = None
    subtitle: str | None = None
    summary: str | None = None
    intended_use: str | None = None
    risk_note: str | None = None
    row_count: int | None = None
    feature_count: int | None = None
    label_count: int | None = None
    feature_columns_preview: list[str] = Field(default_factory=list)
    label_columns: list[str] = Field(default_factory=list)
    feature_groups: list[DatasetFieldGroupView] = Field(default_factory=list)
    quality_summary: DatasetQualitySummaryView | None = None
    glossary_hints: list[GlossaryHintView] = Field(default_factory=list)
    label_spec: dict[str, Any] = Field(default_factory=dict)
    split_manifest: dict[str, Any] = Field(default_factory=dict)
    sample_policy: dict[str, Any] = Field(default_factory=dict)
    quality: dict[str, Any] = Field(default_factory=dict)
    acquisition_profile: dict[str, Any] = Field(default_factory=dict)
    build_profile: dict[str, Any] = Field(default_factory=dict)
    schema_profile: dict[str, Any] = Field(default_factory=dict)
    readiness_profile: dict[str, Any] = Field(default_factory=dict)
    training_profile: dict[str, Any] = Field(default_factory=dict)
    download_href: str | None = None
    links: list[DeepLinkView] = Field(default_factory=list)


class DatasetNlpKeywordView(ApiModel):
    term: str
    score: float | None = None
    count: int | None = None
    weight: float | None = None


class DatasetNlpSourceBreakdownView(ApiModel):
    source: str
    count: int
    share: float


class DatasetNlpTimelinePointView(ApiModel):
    label: str
    event_count: int
    avg_sentiment: float | None = None


class DatasetNlpEventPreviewView(ApiModel):
    event_id: str
    title: str
    snippet: str
    source: str
    source_type: str | None = None
    symbol: str | None = None
    event_time: datetime
    available_time: datetime | None = None
    sentiment_score: float | None = None
    url: str | None = None


class DatasetNlpInspectionView(ApiModel):
    dataset_id: str
    contains_nlp: bool = False
    coverage_summary: str | None = None
    requested_start_time: datetime | None = None
    requested_end_time: datetime | None = None
    actual_start_time: datetime | None = None
    actual_end_time: datetime | None = None
    source_vendors: list[str] = Field(default_factory=list)
    keyword_summary: list[DatasetNlpKeywordView] = Field(default_factory=list)
    word_cloud_terms: list[DatasetNlpKeywordView] = Field(default_factory=list)
    source_breakdown: list[DatasetNlpSourceBreakdownView] = Field(default_factory=list)
    event_timeline: list[DatasetNlpTimelinePointView] = Field(default_factory=list)
    sentiment_distribution: list[TimeValuePoint] = Field(default_factory=list)
    recent_event_previews: list[DatasetNlpEventPreviewView] = Field(default_factory=list)
    sample_feature_preview: dict[str, float | None] = Field(default_factory=dict)
    official_template_gate_status: str | None = None
    official_template_gate_reasons: list[str] = Field(default_factory=list)
    official_template_eligible: bool | None = None
    archival_source_only: bool | None = None
    coverage_ratio: float | None = None
    test_coverage_ratio: float | None = None
    max_consecutive_empty_bars: int | None = None
    duplicate_ratio: float | None = None
    entity_link_coverage_ratio: float | None = None
    market_window_start_time: datetime | None = None
    market_window_end_time: datetime | None = None
    official_backtest_start_time: datetime | None = None
    official_backtest_end_time: datetime | None = None


class DatasetRequestOptionView(ApiModel):
    value: str
    label: str
    description: str | None = None
    recommended: bool = False


class DatasetAcquisitionTimeWindow(ApiModel):
    start_time: datetime
    end_time: datetime


class DatasetSymbolSelectorView(ApiModel):
    symbol_type: str
    selection_mode: str
    symbols: list[str] = Field(default_factory=list)
    symbol_count: int | None = None
    tags: list[str] = Field(default_factory=list)


class DatasetBuildConfigView(ApiModel):
    feature_set_id: str
    label_horizon: int
    label_kind: str
    split_strategy: str
    sample_policy_name: str = "training_panel_strict"
    alignment_policy_name: str = "event_time_inner"
    missing_feature_policy_name: str = "drop_if_missing"
    sample_policy: dict[str, Any] = Field(default_factory=dict)
    alignment_policy: dict[str, Any] = Field(default_factory=dict)
    missing_feature_policy: dict[str, Any] = Field(default_factory=dict)


class DatasetAcquisitionSourceRequest(ApiModel):
    data_domain: str
    source_vendor: str | None = Field(
        default=None,
        validation_alias=AliasChoices("source_vendor", "vendor"),
    )
    exchange: str | None = None
    frequency: str
    symbol_selector: DatasetSymbolSelectorView | None = None
    identifier: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)

    @property
    def vendor(self) -> str:
        return self.source_vendor or "internal"


class DatasetAcquisitionRequest(ApiModel):
    request_name: str
    data_domain: str | None = "market"
    dataset_type: str = "training_panel"
    asset_mode: str = "single_asset"
    time_window: DatasetAcquisitionTimeWindow
    symbol_selector: DatasetSymbolSelectorView | None = None
    selection_mode: str = "explicit"
    source_vendor: str | None = None
    exchange: str | None = None
    frequency: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    build_config: DatasetBuildConfigView
    sources: list[DatasetAcquisitionSourceRequest] = Field(default_factory=list)
    merge_policy_name: str | None = None

    @model_validator(mode="after")
    def validate_contract(self) -> "DatasetAcquisitionRequest":
        if self.sources:
            return self
        missing: list[str] = []
        if self.symbol_selector is None:
            missing.append("symbol_selector")
        if not self.source_vendor:
            missing.append("source_vendor")
        if not self.exchange:
            missing.append("exchange")
        if not self.frequency:
            missing.append("frequency")
        if missing:
            raise ValueError(
                "dataset acquisition request requires either sources[] or legacy single-domain "
                f"fields: {', '.join(missing)}"
            )
        return self


class DatasetFusionSourceRequest(ApiModel):
    data_domain: str
    vendor: str
    identifier: str
    frequency: str
    feature_name: str | None = None
    exchange: str | None = None
    metric_name: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetFusionRequest(ApiModel):
    request_name: str
    base_dataset_id: str
    dataset_type: str = "fusion_training_panel"
    sample_policy_name: str = "fusion_training_panel_strict"
    alignment_policy_name: str = "available_time_safe_asof"
    missing_feature_policy_name: str = "drop_if_missing"
    sample_policy: dict[str, Any] = Field(default_factory=dict)
    alignment_policy: dict[str, Any] = Field(default_factory=dict)
    missing_feature_policy: dict[str, Any] = Field(default_factory=dict)
    sources: list[DatasetFusionSourceRequest] = Field(default_factory=list)


class DatasetPipelineFusionConfig(ApiModel):
    enabled: bool = False
    request_name: str | None = None
    sources: list[DatasetFusionSourceRequest] = Field(default_factory=list)
    alignment_policy_name: str = "available_time_safe_asof"
    missing_feature_policy_name: str = "drop_if_missing"
    min_feature_coverage_ratio: float = 0.5
    alignment_policy: dict[str, Any] = Field(default_factory=dict)
    missing_feature_policy: dict[str, Any] = Field(default_factory=dict)


class DatasetPipelineTrainingConfig(ApiModel):
    enabled: bool = False
    template_id: str | None = None
    template_overrides: dict[str, Any] = Field(default_factory=dict)
    model_names: list[str] = Field(default_factory=list)
    trainer_preset: str = "fast"
    experiment_name: str = "workbench-pipeline-train"
    run_id_prefix: str | None = None
    seed: int = 7


class DatasetPipelineRequest(ApiModel):
    base_request: DatasetAcquisitionRequest
    fusion: DatasetPipelineFusionConfig = Field(default_factory=DatasetPipelineFusionConfig)
    training: DatasetPipelineTrainingConfig = Field(
        default_factory=DatasetPipelineTrainingConfig
    )


class DatasetPipelinePlanView(ApiModel):
    job_id: str
    status: str
    job_api_path: str | None = None
    tracking_token: str | None = None
    submitted_at: datetime | None = None
    requested_stages: list[str] = Field(default_factory=list)
    final_stage: str
    fusion_enabled: bool = False
    training_enabled: bool = False
    base_request_name: str


class DatasetRequestOptionsView(ApiModel):
    domains: list[DatasetRequestOptionView] = Field(default_factory=list)
    asset_modes: list[DatasetRequestOptionView] = Field(default_factory=list)
    selection_modes: list[DatasetRequestOptionView] = Field(default_factory=list)
    symbol_types: list[DatasetRequestOptionView] = Field(default_factory=list)
    source_vendors: list[DatasetRequestOptionView] = Field(default_factory=list)
    exchanges: list[DatasetRequestOptionView] = Field(default_factory=list)
    frequencies: list[DatasetRequestOptionView] = Field(default_factory=list)
    feature_sets: list[DatasetRequestOptionView] = Field(default_factory=list)
    label_horizons: list[DatasetRequestOptionView] = Field(default_factory=list)
    split_strategies: list[DatasetRequestOptionView] = Field(default_factory=list)
    sample_policies: list[DatasetRequestOptionView] = Field(default_factory=list)
    alignment_policies: list[DatasetRequestOptionView] = Field(default_factory=list)
    missing_feature_policies: list[DatasetRequestOptionView] = Field(default_factory=list)
    domain_capabilities: dict[str, dict[str, Any]] = Field(default_factory=dict)
    constraints: dict[str, Any] = Field(default_factory=dict)


class DatasetReadinessSummaryView(ApiModel):
    dataset_id: str
    data_domains: list[str] = Field(default_factory=list)
    build_status: str
    readiness_status: str
    blocking_issues: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    raw_row_count: int | None = None
    usable_row_count: int | None = None
    dropped_row_count: int | None = None
    feature_count: int | None = None
    feature_schema_hash: str | None = None
    feature_dimension_consistent: bool = False
    entity_scope: str | None = None
    entity_count: int | None = None
    alignment_status: str | None = None
    missing_feature_status: str | None = None
    label_alignment_status: str | None = None
    split_integrity_status: str | None = None
    temporal_safety_status: str | None = None
    freshness_status: str | None = None
    recommended_next_actions: list[str] = Field(default_factory=list)
    official_template_eligible: bool | None = None
    official_nlp_gate_status: str | None = None
    official_nlp_gate_reasons: list[str] = Field(default_factory=list)
    modality_quality_summary: dict[str, ModalityQualityView] = Field(default_factory=dict)
    aligned_multimodal_quality: ModalityQualityView | None = None
    archival_nlp_source_only: bool | None = None
    nlp_requested_start_time: datetime | None = None
    nlp_requested_end_time: datetime | None = None
    nlp_actual_start_time: datetime | None = None
    nlp_actual_end_time: datetime | None = None
    market_window_start_time: datetime | None = None
    market_window_end_time: datetime | None = None
    official_backtest_start_time: datetime | None = None
    official_backtest_end_time: datetime | None = None
    nlp_coverage_ratio: float | None = None
    nlp_test_coverage_ratio: float | None = None
    nlp_max_consecutive_empty_bars: int | None = None
    nlp_duplicate_ratio: float | None = None
    nlp_entity_link_coverage_ratio: float | None = None


class TrainingDatasetSummaryView(ApiModel):
    dataset_id: str
    display_name: str
    dataset_type: str = "training_panel"
    data_domain: str | None = None
    data_domains: list[str] = Field(default_factory=list)
    snapshot_version: str | None = None
    entity_scope: str | None = None
    universe_summary: dict[str, Any] = Field(default_factory=dict)
    sample_count: int | None = None
    feature_count: int | None = None
    label_count: int | None = None
    label_horizon: int | None = None
    split_strategy: str | None = None
    source_vendor: str | None = None
    frequency: str | None = None
    freshness_status: str | None = None
    quality_status: str | None = None
    readiness_status: str | None = None
    readiness_reason: str | None = None


class DatasetFusionBuildResponse(ApiModel):
    dataset_id: str
    status: str
    message: str
    detail_href: str
    training_href: str
    feature_view_uri: str | None = None
    dataset_manifest_uri: str | None = None
    training_summary: TrainingDatasetSummaryView | None = None
    readiness: DatasetReadinessSummaryView | None = None


class OhlcvBarView(ApiModel):
    event_time: datetime
    available_time: datetime | None = None
    symbol: str
    venue: str | None = None
    open: float
    high: float
    low: float
    close: float
    volume: float


class OhlcvBarsResponse(ApiModel):
    dataset_id: str
    asset_id: str | None = None
    symbol: str | None = None
    frequency: str | None = None
    total: int
    page: int
    per_page: int
    start_time: datetime | None = None
    end_time: datetime | None = None
    items: list[OhlcvBarView] = Field(default_factory=list)


class DatasetFeatureSeriesPointView(ApiModel):
    timestamp: datetime
    available_time: datetime | None = None
    value: float | None = None


class DatasetFeatureSeriesView(ApiModel):
    feature_name: str
    label: str
    data_domain: str
    points: list[DatasetFeatureSeriesPointView] = Field(default_factory=list)


class DatasetFeatureSeriesResponse(ApiModel):
    dataset_id: str
    total_rows: int
    max_points: int
    downsampled: bool = False
    items: list[DatasetFeatureSeriesView] = Field(default_factory=list)


class TrainingDatasetsResponse(ApiModel):
    items: list[TrainingDatasetSummaryView] = Field(default_factory=list)
    total: int


class DatasetFacetBucketView(ApiModel):
    value: str
    label: str
    count: int


class DatasetFacetsView(ApiModel):
    domains: list[DatasetFacetBucketView] = Field(default_factory=list)
    dataset_types: list[DatasetFacetBucketView] = Field(default_factory=list)
    source_vendors: list[DatasetFacetBucketView] = Field(default_factory=list)
    frequencies: list[DatasetFacetBucketView] = Field(default_factory=list)
    readiness_statuses: list[DatasetFacetBucketView] = Field(default_factory=list)


class DatasetSliceView(ApiModel):
    slice_id: str
    label: str
    slice_kind: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    row_count: int | None = None
    sample_count: int | None = None
    readiness_status: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetSlicesResponse(ApiModel):
    dataset_id: str
    items: list[DatasetSliceView] = Field(default_factory=list)


class DatasetSeriesView(ApiModel):
    series_key: str
    label: str
    series_kind: str
    data_domain: str | None = None
    entity_key: str | None = None
    frequency: str | None = None
    coverage: dict[str, datetime | None] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetSeriesResponse(ApiModel):
    dataset_id: str
    items: list[DatasetSeriesView] = Field(default_factory=list)


class DatasetDependencyView(ApiModel):
    dependency_kind: str
    dependency_id: str
    dependency_label: str | None = None
    target_dataset_id: str | None = None
    direction: str = "depends_on"
    blocking: bool = False
    href: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetDependenciesResponse(ApiModel):
    dataset_id: str
    items: list[DatasetDependencyView] = Field(default_factory=list)
    can_delete: bool = True
    deletion_reason: str | None = None
    blocking_items: list[DatasetDependencyView] = Field(default_factory=list)


class DatasetDeleteResponse(ApiModel):
    dataset_id: str
    status: str
    message: str
    blocking_items: list[DatasetDependencyView] = Field(default_factory=list)
    deleted_files: list[str] = Field(default_factory=list)


class DatasetRenameRequest(ApiModel):
    display_name: str


class DatasetRenameResponse(ApiModel):
    dataset_id: str
    display_name: str
    previous_display_name: str | None = None
    message: str
