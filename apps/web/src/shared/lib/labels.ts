import { I18N } from "./i18n";

const STATUS_LABELS: Record<string, string> = {
  queued: I18N.status.queued,
  running: I18N.status.running,
  success: I18N.status.success,
  failed: I18N.status.failed,
  error: I18N.status.failed,
  partial: I18N.status.partial,
  ready: "\u53ef\u8bad\u7ec3",
  warning: "\u9700\u7559\u610f",
  not_ready: "\u6682\u4e0d\u53ef\u8bad\u7ec3",
  benchmark: I18N.status.benchmark,
  unknown: I18N.status.unknown,
};

const JOB_TYPE_LABELS: Record<string, string> = {
  train: "\u8bad\u7ec3\u4efb\u52a1",
  backtest: "\u56de\u6d4b\u4efb\u52a1",
  dataset_request: "\u6570\u636e\u7533\u8bf7",
  dataset_pipeline: "\u95ed\u73af\u7f16\u6392",
  prepare: "\u6570\u636e\u51c6\u5907",
  build: "\u6570\u636e\u6784\u5efa",
  acquisition: "\u6570\u636e\u91c7\u96c6",
};

const SOURCE_TYPE_LABELS: Record<string, string> = {
  run: I18N.sourceType.run,
  benchmark: I18N.sourceType.benchmark,
};

const STAGE_NAME_LABELS: Record<string, string> = {
  acquire_base: "\u7533\u8bf7\u57fa\u7840\u6570\u636e",
  prepare_base: "\u6784\u5efa\u57fa\u7840\u6570\u636e",
  readiness_base: "\u57fa\u7840\u5c31\u7eea\u6821\u9a8c",
  build_fusion: "\u6784\u5efa\u878d\u5408\u6570\u636e",
  readiness_fusion: "\u878d\u5408\u5c31\u7eea\u6821\u9a8c",
  prepare: I18N.stage.prepare,
  train: I18N.stage.train,
  predict: I18N.stage.predict,
  backtest: I18N.stage.backtest,
  acquire: "\u7533\u8bf7\u6570\u636e",
  readiness: "\u5c31\u7eea\u6821\u9a8c",
};

const ARTIFACT_LABELS: Record<string, string> = {
  tracking_summary: "\u8ddf\u8e2a\u6458\u8981",
  train_manifest: "\u8bad\u7ec3\u6e05\u5355",
  legacy_manifest: "\u65e7\u7248\u6e05\u5355",
  model_state: "\u6a21\u578b\u72b6\u6001",
  model_metadata: "\u6a21\u578b\u5143\u6570\u636e",
  feature_importance: "\u7279\u5f81\u91cd\u8981\u6027",
  evaluation_summary: "\u8bc4\u4f30\u6458\u8981",
  prediction_frame: "\u9884\u6d4b\u7ed3\u679c",
  benchmark_json: "\u57fa\u51c6 JSON",
  benchmark_markdown: "\u57fa\u51c6 Markdown",
  benchmark_csv: "\u57fa\u51c6 CSV",
  research_result: "\u7814\u7a76\u5f15\u64ce\u7ed3\u679c",
  simulation_result: "\u4eff\u771f\u5f15\u64ce\u7ed3\u679c",
  research_backtest_result: "\u7814\u7a76\u5f15\u64ce\u7ed3\u679c",
  simulation_backtest_result: "\u6a21\u62df\u5f15\u64ce\u7ed3\u679c",
  backtest_report: "\u56de\u6d4b\u62a5\u544a",
  simulation_backtest_report: "\u6a21\u62df\u62a5\u544a",
  report: "\u62a5\u544a",
  diagnostics: "\u8bca\u65ad",
  backtest_diagnostics: "\u56de\u6d4b\u8bca\u65ad",
  leakage_audit: "\u6cc4\u6f0f\u5ba1\u8ba1",
  backtest_leakage: "\u6cc4\u6f0f\u5ba1\u8ba1",
  pnl: "\u6536\u76ca\u5206\u89e3",
  backtest_pnl: "\u56de\u6d4b\u6536\u76ca\u5206\u89e3",
  positions: "\u6301\u4ed3",
  backtest_positions: "\u56de\u6d4b\u6301\u4ed3\u8def\u5f84",
  orders: "\u59d4\u6258",
  fills: "\u6210\u4ea4",
  scenario_summary: "\u538b\u529b\u573a\u666f",
  scenarios: "\u538b\u529b\u573a\u666f",
  backtest_scenarios: "\u56de\u6d4b\u538b\u529b\u573a\u666f",
};

const FRESHNESS_LABELS: Record<string, string> = {
  fresh: "\u65b0\u9c9c",
  stale: "\u504f\u65e7",
  delayed: "\u5ef6\u8fdf",
  unknown: "\u672a\u77e5",
};

export function formatStatusLabel(status: string | null | undefined): string {
  const normalized = (status ?? "unknown").toLowerCase();
  return STATUS_LABELS[normalized] ?? normalized;
}

export function formatJobTypeLabel(jobType: string | null | undefined): string {
  const normalized = (jobType ?? "").toLowerCase();
  return JOB_TYPE_LABELS[normalized] ?? (jobType || "--");
}

export function formatSourceTypeLabel(sourceType: string | null | undefined): string {
  const normalized = (sourceType ?? "").toLowerCase();
  return SOURCE_TYPE_LABELS[normalized] ?? (sourceType || "--");
}

export function formatStageNameLabel(stageName: string | null | undefined): string {
  const normalized = (stageName ?? "").toLowerCase();
  return STAGE_NAME_LABELS[normalized] ?? (stageName || "--");
}

export function formatArtifactLabel(kind: string | null | undefined, fallback?: string): string {
  const normalized = (kind ?? "").toLowerCase();
  return ARTIFACT_LABELS[normalized] ?? fallback ?? (normalized || "--");
}

export function formatFreshnessLabel(freshness: string | null | undefined): string {
  const normalized = (freshness ?? "unknown").toLowerCase();
  return FRESHNESS_LABELS[normalized] ?? (freshness || "--");
}
