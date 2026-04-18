export type ArtifactView = {
  kind: string;
  label: string;
  uri: string;
  exists: boolean;
  previewable: boolean;
};

export type DeepLinkView = {
  kind: string;
  label: string;
  href: string;
  api_path: string | null;
};

export type GlossaryHintView = {
  key: string;
  term: string;
  short: string;
};

export type ReviewSummaryView = {
  status: string;
  title: string;
  summary: string;
  suggested_actions: string[];
};

export type WarningSummaryView = {
  level: string;
  count: number;
  items: string[];
};

export type ModalityQualityView = {
  modality: string;
  status: string;
  blocking_reasons: string[];
  usable_count?: number | null;
  coverage_ratio?: number | null;
  duplicate_ratio?: number | null;
  max_gap_bars?: number | null;
  freshness_lag_days?: number | null;
  non_null_coverage_ratio?: number | null;
  required_feature_names?: string[];
  observed_feature_names?: string[];
};

export type PredictionArtifactView = {
  scope: string;
  sample_count: number;
  uri: string;
};

export type DatasetReferenceView = {
  dataset_id: string;
  label?: string | null;
  href?: string | null;
  api_path?: string | null;
  role?: string | null;
  modality?: string | null;
};

export type RelatedBacktestView = {
  backtest_id: string;
  model_name: string;
  run_id: string;
  annual_return: number | null;
  max_drawdown: number | null;
  passed_consistency_checks: boolean | null;
  research_backend?: string | null;
  portfolio_method?: string | null;
};

export type ExperimentListItem = {
  run_id: string;
  model_name: string;
  dataset_id: string | null;
  dataset_ids?: string[];
  datasets?: DatasetReferenceView[];
  primary_dataset_id?: string | null;
  composition?: Record<string, unknown> | null;
  family: string | null;
  backend: string | null;
  status: string;
  created_at: string | null;
  primary_metric_name: string | null;
  primary_metric_value: number | null;
  metrics: Record<string, number>;
  backtest_count: number;
  prediction_scopes: string[];
  official_template_eligible?: boolean | null;
  official_blocking_reasons?: string[];
  feature_scope_modality?: string | null;
  feature_scope_feature_names?: string[];
  source_dataset_quality_status?: string | null;
  tags: Record<string, string>;
};

export type ExperimentsResponse = {
  items: ExperimentListItem[];
  total: number;
  page: number;
  per_page: number;
  available_models: string[];
  available_datasets: string[];
  available_statuses: string[];
};

export type RunDetailView = {
  run_id: string;
  model_name: string;
  dataset_id: string | null;
  dataset_ids?: string[];
  datasets?: DatasetReferenceView[];
  composition?: {
    fusion_strategy?: string;
    source_runs?: Array<{
      run_id: string;
      model_name?: string;
      modality?: string;
      weight?: number;
      dataset_ids?: string[];
    }>;
    rules?: string[];
  } | null;
  source_run_ids?: string[];
  task_type?: string | null;
  artifact_format_status?: string;
  missing_artifacts?: string[];
  family: string | null;
  backend: string | null;
  status: string;
  created_at: string | null;
  metrics: Record<string, number>;
  tracking_params: Record<string, string>;
  manifest_metrics: Record<string, number>;
  repro_context: Record<string, unknown>;
  dataset_summary?: Record<string, unknown>;
  evaluation_summary?: Record<string, unknown>;
  evaluation_artifacts?: ArtifactView[];
  prediction_summary?: Record<string, unknown>;
  time_range?: Record<string, unknown>;
  feature_importance: Record<string, number>;
  predictions: PredictionArtifactView[];
  related_backtests: RelatedBacktestView[];
  artifacts: ArtifactView[];
  notes: string[];
  official_template_eligible?: boolean | null;
  official_blocking_reasons?: string[];
  feature_scope_modality?: string | null;
  feature_scope_feature_names?: string[];
  source_dataset_quality_status?: string | null;
  review_summary: ReviewSummaryView | null;
  warning_summary: WarningSummaryView | null;
  glossary_hints: GlossaryHintView[];
};

export type BenchmarkRowView = {
  rank: number;
  model_name: string;
  family: string;
  advanced_kind: string;
  backend: string;
  window_count: number;
  mean_valid_mae: number;
  mean_test_mae: number;
  artifact_uri: string | null;
};

export type BacktestTemplateView = {
  template_id: string;
  name: string;
  description: string | null;
  source: string;
  read_only: boolean;
  official: boolean;
  protocol_version: string | null;
  output_contract_version: string | null;
  fixed_prediction_scope: string | null;
  ranking_policy: string | null;
  slice_policy: string | null;
  scenario_bundle: string[];
  eligibility_rules: string[];
  eligibility_rule_keys?: string[];
  required_metadata: string[];
  required_metadata_keys?: string[];
  notes: string[];
  note_keys?: string[];
};

export type GateResultView = {
  key: string;
  label: string;
  label_key?: string | null;
  passed: boolean | null;
  severity: string;
  detail: string | null;
  detail_key?: string | null;
};

export type RankComponentView = {
  key: string;
  label: string;
  value: number | null;
  detail: string | null;
};

export type ProtocolGateFailureView = {
  key: string;
  label: string;
  label_key?: string | null;
  severity: string;
  reasons: string[];
  reason_keys?: string[];
};

export type BacktestExecutionDiagnosticsSummaryView = {
  signal_count?: number | null;
  order_count?: number | null;
  eligible_order_count?: number | null;
  blocked_order_count?: number | null;
  fill_count?: number | null;
  position_open_count?: number | null;
  block_reasons: string[];
};

export type BacktestProtocolResultView = {
  template: BacktestTemplateView | null;
  gate_status: string | null;
  gate_results: GateResultView[];
  protocol_gate_failures?: ProtocolGateFailureView[];
  rank_components: RankComponentView[];
  slice_id: string | null;
  slice_coverage: string[];
  lookback_bucket: string | null;
  metadata_summary: Record<string, string | null>;
  missing_required_metadata_keys?: string[];
  missing_required_metadata_labels?: string[];
  missing_stress_scenarios?: string[];
  required_modalities?: string[];
  official_dataset_ids?: string[];
  actual_market_start_time?: string | null;
  actual_market_end_time?: string | null;
  actual_backtest_start_time?: string | null;
  actual_backtest_end_time?: string | null;
  actual_nlp_start_time?: string | null;
  actual_nlp_end_time?: string | null;
  nlp_gate_status?: string | null;
  nlp_gate_reasons?: string[];
  nlp_gate_reason_keys?: string[];
  modality_quality_summary?: Record<string, ModalityQualityView>;
  quality_blocking_reasons?: string[];
  official_benchmark_version?: string | null;
  official_window_days?: number | null;
  official_window_start_time?: string | null;
  official_window_end_time?: string | null;
  official_market_dataset_id?: string | null;
  official_multimodal_dataset_id?: string | null;
};

export type BenchmarkListItemView = {
  benchmark_name: string;
  dataset_id: string;
  data_source: string | null;
  benchmark_type: string;
  updated_at: string;
  top_model_name: string | null;
  top_model_score: number | null;
};

export type BenchmarkDetailView = {
  benchmark_name: string;
  dataset_id: string;
  data_source: string | null;
  benchmark_type: string;
  updated_at: string;
  window_count: number;
  leaderboard: BenchmarkRowView[];
  results: BenchmarkRowView[];
  deep_backend_comparison: Array<Record<string, unknown>>;
  validation_summary: Record<string, unknown>;
  artifacts: ArtifactView[];
  review_summary: ReviewSummaryView | null;
  warning_summary: WarningSummaryView | null;
  glossary_hints: GlossaryHintView[];
};

export type BacktestListItemView = {
  backtest_id: string;
  run_id: string | null;
  model_name: string | null;
  status: string;
  template_id?: string | null;
  official?: boolean;
  protocol_version?: string | null;
  gate_status?: string | null;
  research_backend?: string | null;
  portfolio_method?: string | null;
  passed_consistency_checks: boolean | null;
  annual_return: number | null;
  max_drawdown: number | null;
  warning_count: number;
  updated_at: string | null;
};

export type BacktestsResponse = {
  items: BacktestListItemView[];
  total: number;
  page: number;
  per_page: number;
  available_statuses: string[];
};

export type TimeValuePoint = {
  label: string;
  value: number;
};

export type ScenarioDeltaView = {
  scenario_name: string;
  cumulative_return_delta: number;
};

export type BacktestEngineView = {
  backtest_id: string;
  engine_type: string;
  report_summary: string | null;
  metrics: Record<string, number>;
  diagnostics: Record<string, unknown>;
  pnl_snapshot: Record<string, number>;
  positions: TimeValuePoint[];
  scenarios: ScenarioDeltaView[];
  warnings: string[];
  artifacts: ArtifactView[];
};

export type BacktestReportView = {
  backtest_id: string;
  model_name: string | null;
  run_id: string | null;
  dataset_id?: string | null;
  dataset_ids?: string[];
  alignment?: {
    fusion_strategy?: string | null;
    dataset_ids?: string[];
    datasets?: DatasetReferenceView[];
    alignment_status?: string | null;
    notes?: string[];
  } | null;
  template_id?: string | null;
  official?: boolean;
  protocol_version?: string | null;
  research_backend?: string | null;
  portfolio_method?: string | null;
  protocol?: BacktestProtocolResultView | null;
  modality_quality_summary?: Record<string, ModalityQualityView>;
  quality_blocking_reasons?: string[];
  passed_consistency_checks: boolean | null;
  comparison_warnings: string[];
  divergence_metrics: Record<string, number>;
  scenario_metrics: Record<string, number>;
  research: BacktestEngineView | null;
  simulation: BacktestEngineView | null;
  execution_diagnostics_summary?: BacktestExecutionDiagnosticsSummaryView | null;
  artifacts: ArtifactView[];
  review_summary: ReviewSummaryView | null;
  warning_summary: WarningSummaryView | null;
  glossary_hints: GlossaryHintView[];
};

export type BacktestDeleteResponse = {
  backtest_id: string;
  status: string;
  message: string;
  deleted_files: string[];
};

export type TrainedModelDetailView = {
  run_id: string;
  model_name: string;
  family?: string | null;
  dataset_id?: string | null;
  created_at?: string | null;
  status: string;
  metrics: Record<string, number>;
  note?: string | null;
  is_deleted: boolean;
};

export type BenchmarkSelection = {
  benchmark_name: string;
  model_names: string[];
};

export type ComparisonQuery = {
  run_ids: string[];
  benchmark_selections: BenchmarkSelection[];
  template_id?: string;
  official_only?: boolean;
};

export type ComparisonRowView = {
  row_id: string;
  source_type: string;
  label: string;
  model_name: string;
  dataset_id: string | null;
  backend: string | null;
  status: string | null;
  template_id?: string | null;
  official?: boolean;
  protocol_version?: string | null;
  gate_status?: string | null;
  train_mae: number | null;
  mean_valid_mae: number | null;
  mean_test_mae: number | null;
  annual_return: number | null;
  max_drawdown: number | null;
  turnover_total: number | null;
  implementation_shortfall: number | null;
};

export type ModelComparisonView = {
  rows: ComparisonRowView[];
  metrics: string[];
};

export type JobResultLinks = {
  run_detail: string | null;
  backtest_detail: string | null;
  review_detail: string | null;
  dataset_detail?: string | null;
  base_dataset_detail?: string | null;
  fusion_dataset_detail?: string | null;
};

export type JobResultView = {
  dataset_id: string | null;
  base_dataset_id?: string | null;
  fusion_dataset_id?: string | null;
  run_ids: string[];
  backtest_ids: string[];
  benchmark_names?: string[];
  fit_result_uris: string[];
  summary_artifacts: string[];
  prediction_scope?: string | null;
  requested_stages?: string[];
  deeplinks: JobResultLinks;
  result_links?: DeepLinkView[];
  summary?: Record<string, unknown> | null;
  pipeline_summary?: Record<string, unknown> | null;
};

export type ModelTemplateView = {
  template_id: string;
  name: string;
  model_name: string;
  description: string | null;
  source: string;
  hyperparams: Record<string, unknown>;
  trainer_preset: string;
  dataset_preset: string;
  read_only: boolean;
  model_registered: boolean;
  deleted_at: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type ModelTemplateListResponse = {
  items: ModelTemplateView[];
  total: number;
  model_options_source: string;
};

export type ModelTemplateCreateRequest = {
  name: string;
  model_name: string;
  description?: string | null;
  hyperparams?: Record<string, unknown>;
  trainer_preset?: string;
  dataset_preset?: string;
};

export type ModelTemplateUpdateRequest = {
  name?: string;
  description?: string | null;
  hyperparams?: Record<string, unknown>;
  trainer_preset?: string;
  dataset_preset?: string;
};

export type LaunchTrainRequest = {
  dataset_preset?: "smoke" | "real_benchmark";
  dataset_id?: string;
  template_id?: string;
  template_overrides?: Record<string, unknown>;
  model_names?: string[];
  trainer_preset?: "fast";
  feature_scope_modality?: "market" | "macro" | "on_chain" | "derivatives" | "nlp";
  seed: number;
  experiment_name: string;
  run_id_prefix?: string;
};

export type LaunchModelCompositionRequest = {
  source_run_ids: string[];
  composition_name: string;
  dataset_ids?: string[];
};

export type LaunchDatasetMultimodalTrainRequest = {
  dataset_id: string;
  selected_modalities: Array<"market" | "macro" | "on_chain" | "derivatives" | "nlp">;
  template_by_modality: Partial<Record<"market" | "macro" | "on_chain" | "derivatives" | "nlp", string>>;
  trainer_preset?: "fast";
  experiment_name_prefix: string;
  seed: number;
  fusion_strategy?: "late_score_blend";
  composition_name?: string | null;
  auto_launch_official_backtest?: boolean;
  official_window_days?: 30 | 90 | 180 | 365 | null;
};

export type LaunchBacktestRequest = {
  run_id: string;
  mode?: "official" | "custom";
  template_id?: string;
  official_window_days?: 30 | 90 | 180 | 365;
  dataset_id?: string;
  dataset_ids?: string[];
  dataset_preset?: "smoke" | "real_benchmark";
  prediction_scope: "full" | "test";
  strategy_preset: "sign";
  portfolio_preset: "research_default";
  cost_preset: "standard";
  research_backend?: "native" | "vectorbt";
  portfolio_method?: "proportional" | "skfolio_mean_risk";
  benchmark_symbol: string;
};

export type LaunchBacktestPreflightRequest = {
  run_id: string;
  mode?: "official" | "custom";
  template_id?: string;
  official_window_days?: 30 | 90 | 180 | 365;
};

export type LaunchJobResponse = {
  job_id: string;
  status: "queued" | "running" | "success" | "failed";
  job_api_path?: string | null;
  tracking_token: string;
  submitted_at: string;
};

export type PresetOptionView = {
  value: string;
  label: string;
  description: string | null;
  recommended: boolean;
};

export type TrainLaunchOptionsView = {
  dataset_presets: PresetOptionView[];
  model_options: PresetOptionView[];
  template_options: PresetOptionView[];
  trainer_presets: PresetOptionView[];
  feature_scope_modalities?: PresetOptionView[];
  default_seed: number;
  constraints: Record<string, unknown>;
};

export type BacktestLaunchOptionsView = {
  default_mode?: "official" | "custom";
  official_template_id?: string | null;
  official_multimodal_schema_version?: string | null;
  official_multimodal_feature_names?: string[];
  template_options?: BacktestTemplateView[];
  official_window_options?: PresetOptionView[];
  dataset_presets: PresetOptionView[];
  prediction_scopes: PresetOptionView[];
  strategy_presets: PresetOptionView[];
  portfolio_presets: PresetOptionView[];
  cost_presets: PresetOptionView[];
  research_backends?: PresetOptionView[];
  portfolio_methods?: PresetOptionView[];
  default_benchmark_symbol: string;
  default_official_window_days?: number;
  constraints: Record<string, unknown>;
};

export type BacktestLaunchPreflightView = {
  compatible: boolean;
  mode: "official" | "custom";
  template_id?: string | null;
  official_window_days?: number | null;
  official_benchmark_version?: string | null;
  official_market_dataset_id?: string | null;
  official_multimodal_dataset_id?: string | null;
  official_dataset_ids?: string[];
  required_modalities?: string[];
  official_window_start_time?: string | null;
  official_window_end_time?: string | null;
  requires_text_features: boolean;
  requires_nlp_features?: boolean;
  requires_auxiliary_features?: boolean;
  requires_multimodal_benchmark?: boolean;
  required_feature_names: string[];
  available_official_feature_names: string[];
  missing_official_feature_names: string[];
  blocking_reasons: string[];
  blocking_reason_codes?: string[];
  modality_quality_summary?: Record<string, ModalityQualityView>;
  quality_blocking_reasons?: string[];
  missing_required_metadata_keys?: string[];
  missing_required_metadata_labels?: string[];
  missing_stress_scenarios?: string[];
  nlp_gate_status?: string | null;
  nlp_gate_reasons: string[];
  nlp_gate_reason_codes?: string[];
};

export type JobStageView = {
  name: string;
  status: "queued" | "running" | "success" | "failed";
  summary: string;
  started_at: string | null;
  finished_at: string | null;
};

export type JobStatusView = {
  job_id: string;
  job_type: string;
  status: "queued" | "running" | "success" | "failed";
  created_at: string;
  updated_at: string;
  stages: JobStageView[];
  result: JobResultView;
  error_message: string | null;
};

export type JobListResponse = {
  items: JobStatusView[];
};

export type JobOverviewItem = {
  job_id: string;
  job_type: string;
  status: string;
  updated_at: string;
  primary_stage: string | null;
  deeplinks: Record<string, string>;
};

export type DataFreshnessView = {
  dataset_id: string | null;
  as_of_time: string | null;
  freshness: string;
  source: string | null;
};

export type DatasetFreshnessView = {
  as_of_time: string | null;
  data_start_time: string | null;
  data_end_time: string | null;
  lag_seconds: number | null;
  status: string;
  summary: string;
};

export type DatasetSummaryView = {
  dataset_id: string;
  display_name: string | null;
  subtitle: string | null;
  dataset_category: string | null;
  data_domain?: string | null;
  data_domains?: string[];
  dataset_type?: string | null;
  asset_id: string | null;
  data_source: string | null;
  frequency: string | null;
  as_of_time: string | null;
  sample_count: number | null;
  row_count: number | null;
  feature_count: number | null;
  label_count: number | null;
  label_horizon: number | null;
  split_strategy: string | null;
  time_range_label: string | null;
  source_vendor?: string | null;
  exchange?: string | null;
  entity_scope?: string | null;
  entity_count?: number | null;
  symbols_preview?: string[];
  snapshot_version?: string | null;
  quality_status?: string | null;
  readiness_status?: string | null;
  build_status?: string | null;
  request_origin?: string | null;
  is_smoke: boolean;
  freshness: DatasetFreshnessView;
  temporal_safety_summary: string;
  links?: DeepLinkView[];
};

export type DatasetListResponse = {
  items: DatasetSummaryView[];
  total: number;
  page: number;
  per_page: number;
};

export type DatasetFieldGroupView = {
  key: string;
  label: string;
  description: string;
  count: number;
  columns: string[];
};

export type DatasetQualitySummaryView = {
  status: string;
  summary: string;
  missing_ratio: number | null;
  duplicate_ratio: number | null;
  duplicate_rows: number | null;
  checks: string[];
};

export type DatasetDetailView = {
  dataset: DatasetSummaryView;
  display_name: string | null;
  subtitle: string | null;
  summary: string | null;
  intended_use: string | null;
  risk_note: string | null;
  row_count: number | null;
  feature_count: number | null;
  label_count: number | null;
  feature_columns_preview: string[];
  label_columns: string[];
  feature_groups: DatasetFieldGroupView[];
  quality_summary: DatasetQualitySummaryView | null;
  glossary_hints: GlossaryHintView[];
  label_spec: Record<string, unknown>;
  split_manifest: Record<string, unknown>;
  sample_policy: Record<string, unknown>;
  quality: Record<string, unknown>;
  acquisition_profile?: Record<string, unknown> | null;
  build_profile?: Record<string, unknown> | null;
  schema_profile?: Record<string, unknown> | null;
  readiness_profile?: Record<string, unknown> | null;
  training_profile?: Record<string, unknown> | null;
  links: DeepLinkView[];
};

export type DatasetOptionValueView = {
  value: string;
  label: string;
  description?: string | null;
  recommended?: boolean;
};

export type DatasetDomainCapabilityView = {
  source_vendors: DatasetOptionValueView[];
  supported_vendors?: string[];
  exchanges?: DatasetOptionValueView[];
  supported_exchanges?: string[];
  frequencies: DatasetOptionValueView[];
  supported_frequencies?: string[];
  symbol_types?: DatasetOptionValueView[];
  supported_symbol_types?: string[];
  selection_modes?: DatasetOptionValueView[];
  supported_selection_modes?: string[];
  supported_dataset_types?: string[];
  supports_real_ingestion?: boolean;
};

export type DatasetSymbolSelectorView = {
  symbol_type?: string;
  selection_mode?: string;
  symbols?: string[];
  symbol_count?: number | null;
  tags?: string[];
};

export type DatasetRequestOptionsView = {
  domains: DatasetOptionValueView[];
  asset_modes: DatasetOptionValueView[];
  symbol_types: DatasetOptionValueView[];
  selection_modes?: DatasetOptionValueView[];
  source_vendors: DatasetOptionValueView[];
  exchanges: DatasetOptionValueView[];
  frequencies: DatasetOptionValueView[];
  feature_sets: DatasetOptionValueView[];
  label_horizons: DatasetOptionValueView[];
  split_strategies: DatasetOptionValueView[];
  sample_policies?: DatasetOptionValueView[];
  alignment_policies?: DatasetOptionValueView[];
  missing_feature_policies?: DatasetOptionValueView[];
  domain_capabilities?: Record<string, DatasetDomainCapabilityView>;
  constraints: Record<string, unknown>;
};

export type DatasetAcquisitionSourceRequest = {
  data_domain: string;
  source_vendor: string;
  exchange?: string | null;
  frequency: string;
  symbol_selector?: DatasetSymbolSelectorView;
  identifier?: string | null;
  filters?: Record<string, string | string[]> | null;
};

export type DatasetAcquisitionRequest = {
  request_name: string;
  data_domain?: string;
  dataset_type?: string;
  asset_mode: "single_asset" | "multi_asset";
  time_window: {
    start_time: string | null;
    end_time: string | null;
  };
  symbol_selector?: {
    symbol_type: string;
    selection_mode: string;
    symbols: string[];
    symbol_count: number | null;
    tags: string[];
  };
  selection_mode?: string;
  source_vendor?: string;
  exchange?: string;
  frequency?: string;
  filters: Record<string, string | string[]>;
  sources?: DatasetAcquisitionSourceRequest[];
  merge_policy_name?: string | null;
  build_config: {
    feature_set_id: string;
    label_horizon: number;
    label_kind: string;
    split_strategy: string;
    sample_policy_name?: string;
    alignment_policy_name?: string;
    missing_feature_policy_name?: string;
    sample_policy?: Record<string, unknown>;
    alignment_policy?: Record<string, unknown>;
    missing_feature_policy?: Record<string, unknown>;
  };
};

export type DatasetFusionSourceRequest = {
  data_domain: string;
  vendor: string;
  identifier: string;
  frequency: string;
  feature_name?: string | null;
  exchange?: string | null;
  metric_name?: string | null;
  options?: Record<string, unknown>;
};

export type DatasetFusionRequest = {
  request_name: string;
  base_dataset_id: string;
  dataset_type?: string;
  sample_policy_name?: string;
  alignment_policy_name?: string;
  missing_feature_policy_name?: string;
  sample_policy?: Record<string, unknown>;
  alignment_policy?: Record<string, unknown>;
  missing_feature_policy?: Record<string, unknown>;
  sources: DatasetFusionSourceRequest[];
};

export type DatasetPipelineFusionConfig = {
  enabled: boolean;
  request_name?: string | null;
  sources?: DatasetFusionSourceRequest[];
  alignment_policy_name?: string;
  missing_feature_policy_name?: string;
  min_feature_coverage_ratio?: number;
  alignment_policy?: Record<string, unknown>;
  missing_feature_policy?: Record<string, unknown>;
};

export type DatasetPipelineTrainingConfig = {
  enabled: boolean;
  template_id?: string;
  template_overrides?: Record<string, unknown>;
  model_names?: string[];
  trainer_preset?: "fast";
  experiment_name?: string;
  run_id_prefix?: string;
  seed?: number;
};

export type DatasetPipelineRequest = {
  base_request: DatasetAcquisitionRequest;
  fusion?: DatasetPipelineFusionConfig;
  training?: DatasetPipelineTrainingConfig;
};

export type DatasetPipelinePlanView = {
  job_id: string;
  status: "queued" | "running" | "success" | "failed";
  job_api_path?: string | null;
  tracking_token?: string | null;
  submitted_at?: string | null;
  requested_stages: string[];
  final_stage: string;
  fusion_enabled: boolean;
  training_enabled: boolean;
  base_request_name: string;
};

export type DatasetReadinessSummaryView = {
  dataset_id: string;
  data_domains?: string[];
  build_status: string;
  readiness_status: "not_ready" | "warning" | "ready";
  blocking_issues: string[];
  warnings: string[];
  raw_row_count: number | null;
  usable_row_count: number | null;
  dropped_row_count: number | null;
  feature_count: number | null;
  feature_schema_hash: string | null;
  feature_dimension_consistent: boolean | null;
  entity_scope: string | null;
  entity_count: number | null;
  alignment_status: string | null;
  missing_feature_status: string | null;
  label_alignment_status: string | null;
  split_integrity_status: string | null;
  temporal_safety_status: string | null;
  freshness_status: string | null;
  recommended_next_actions: string[];
  official_template_eligible?: boolean | null;
  official_nlp_gate_status?: string | null;
  official_nlp_gate_reasons?: string[];
  modality_quality_summary?: Record<string, ModalityQualityView>;
  aligned_multimodal_quality?: ModalityQualityView | null;
  archival_nlp_source_only?: boolean | null;
  nlp_requested_start_time?: string | null;
  nlp_requested_end_time?: string | null;
  nlp_actual_start_time?: string | null;
  nlp_actual_end_time?: string | null;
  market_window_start_time?: string | null;
  market_window_end_time?: string | null;
  official_backtest_start_time?: string | null;
  official_backtest_end_time?: string | null;
  nlp_coverage_ratio?: number | null;
  nlp_test_coverage_ratio?: number | null;
  nlp_max_consecutive_empty_bars?: number | null;
  nlp_duplicate_ratio?: number | null;
  nlp_entity_link_coverage_ratio?: number | null;
};

export type DatasetDependencyView = {
  dependency_kind: string;
  dependency_id: string;
  dependency_label: string | null;
  target_dataset_id: string | null;
  direction?: string;
  blocking?: boolean;
  href?: string | null;
  metadata: Record<string, unknown>;
};

export type DatasetDependenciesResponse = {
  dataset_id: string;
  items: DatasetDependencyView[];
  can_delete?: boolean;
  blocking_items?: DatasetDependencyView[];
  delete_block_reasons?: string[];
  protection_reason?: string | null;
  protection_kind?: string | null;
};

export type DatasetDeleteResponse = {
  dataset_id: string;
  status: string;
  message: string;
  can_delete?: boolean;
  blocking_items?: DatasetDependencyView[];
  delete_block_reasons?: string[];
  protection_reason?: string | null;
  protection_kind?: string | null;
  deleted_files: string[];
};

export type DatasetNlpInspectionView = {
  dataset_id: string;
  contains_nlp: boolean;
  coverage_summary?: string | null;
  requested_start_time?: string | null;
  requested_end_time?: string | null;
  actual_start_time?: string | null;
  actual_end_time?: string | null;
  source_vendors?: string[];
  keyword_summary?: Array<{ term: string; score?: number | null; count?: number | null; weight?: number | null }>;
  word_cloud_terms?: Array<{ term: string; score?: number | null; count?: number | null; weight?: number | null }>;
  source_breakdown?: Array<{ source: string; count: number; share: number }>;
  event_timeline?: Array<{ label: string; event_count: number; avg_sentiment: number | null }>;
  sentiment_distribution?: Array<{ label: string; value: number }>;
  recent_event_previews?: Array<{
    event_id: string;
    title: string;
    snippet: string;
    source: string;
    symbol: string | null;
    event_time: string;
    available_time: string | null;
    sentiment_score: number | null;
    url?: string | null;
  }>;
  sample_feature_preview?: Record<string, number | null>;
  official_template_gate_status?: string | null;
  official_template_gate_reasons?: string[];
  official_template_eligible?: boolean | null;
  archival_source_only?: boolean | null;
  coverage_ratio?: number | null;
  test_coverage_ratio?: number | null;
  max_consecutive_empty_bars?: number | null;
  duplicate_ratio?: number | null;
  entity_link_coverage_ratio?: number | null;
  market_window_start_time?: string | null;
  market_window_end_time?: string | null;
  official_backtest_start_time?: string | null;
  official_backtest_end_time?: string | null;
};

export type TrainingDatasetSummaryView = {
  dataset_id: string;
  display_name: string | null;
  dataset_type?: string | null;
  data_domain?: string | null;
  data_domains?: string[];
  snapshot_version: string | null;
  entity_scope: string | null;
  universe_summary: string | Record<string, unknown> | null;
  sample_count: number | null;
  feature_count: number | null;
  label_count: number | null;
  label_horizon: string | number | null;
  split_strategy: string | null;
  source_vendor: string | null;
  frequency: string | null;
  freshness_status: string | null;
  quality_status: string | null;
  readiness_status: "not_ready" | "warning" | "ready" | string;
  readiness_reason: string | null;
};

export type TrainingDatasetListResponse = {
  items: TrainingDatasetSummaryView[];
  total?: number;
};

export type DatasetFusionBuildResponse = {
  dataset_id: string;
  status: string;
  message: string;
  detail_href: string;
  training_href: string;
  feature_view_uri?: string | null;
  dataset_manifest_uri?: string | null;
  training_summary?: TrainingDatasetSummaryView | null;
  readiness?: DatasetReadinessSummaryView | null;
};

export type OhlcvBarView = {
  event_time: string;
  available_time: string | null;
  symbol: string;
  venue: string | null;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type OhlcvBarsResponse = {
  dataset_id: string;
  asset_id: string | null;
  symbol: string | null;
  frequency: string | null;
  total: number;
  page: number;
  per_page: number;
  start_time: string | null;
  end_time: string | null;
  items: OhlcvBarView[];
};

export type RecommendedActionView = {
  key: string;
  title: string;
  description: string;
  target_path: string;
};

export type WorkbenchOverviewView = {
  recent_runs: ExperimentListItem[];
  recent_backtests: BacktestListItemView[];
  recent_benchmarks: BenchmarkListItemView[];
  recent_jobs: JobOverviewItem[];
  data_freshness: DataFreshnessView | null;
  recommended_actions: RecommendedActionView[];
};

export type ArtifactPreviewResponse = {
  uri: string;
  kind: string;
  is_json: boolean;
  content: unknown;
};
