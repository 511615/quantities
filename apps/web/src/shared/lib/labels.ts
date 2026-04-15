import { I18N, translateText } from "./i18n";

const STATUS_LABELS: Record<string, () => string> = {
  queued: () => I18N.status.queued,
  running: () => I18N.status.running,
  success: () => I18N.status.success,
  failed: () => I18N.status.failed,
  error: () => I18N.status.failed,
  partial: () => I18N.status.partial,
  ready: () => translateText("可训练"),
  warning: () => translateText("需留意"),
  not_ready: () => translateText("暂不可训练"),
  benchmark: () => I18N.status.benchmark,
  unknown: () => I18N.status.unknown,
};

const JOB_TYPE_LABELS: Record<string, () => string> = {
  train: () => translateText("训练任务"),
  backtest: () => translateText("回测任务"),
  dataset_request: () => translateText("数据申请"),
  dataset_pipeline: () => translateText("数据闭环编排"),
  prepare: () => translateText("数据准备"),
  build: () => translateText("数据构建"),
  acquisition: () => translateText("数据采集"),
};

const SOURCE_TYPE_LABELS: Record<string, () => string> = {
  run: () => I18N.sourceType.run,
  benchmark: () => I18N.sourceType.benchmark,
};

const STAGE_NAME_LABELS: Record<string, () => string> = {
  acquire_base: () => translateText("申请基础数据"),
  prepare_base: () => translateText("构建基础数据"),
  readiness_base: () => translateText("基础就绪校验"),
  build_fusion: () => translateText("构建融合数据"),
  readiness_fusion: () => translateText("融合就绪校验"),
  prepare: () => I18N.stage.prepare,
  train: () => I18N.stage.train,
  predict: () => I18N.stage.predict,
  backtest: () => I18N.stage.backtest,
  acquire: () => translateText("申请数据"),
  readiness: () => translateText("就绪校验"),
};

const ARTIFACT_LABELS: Record<string, () => string> = {
  tracking_summary: () => translateText("跟踪摘要"),
  train_manifest: () => translateText("训练清单"),
  legacy_manifest: () => translateText("旧版清单"),
  model_state: () => translateText("模型状态"),
  model_metadata: () => translateText("模型元数据"),
  feature_importance: () => translateText("特征重要性"),
  evaluation_summary: () => translateText("评估摘要"),
  prediction_frame: () => translateText("预测结果"),
  benchmark_json: () => translateText("基准 JSON"),
  benchmark_markdown: () => translateText("基准 Markdown"),
  benchmark_csv: () => translateText("基准 CSV"),
  research_result: () => translateText("研究引擎结果"),
  simulation_result: () => translateText("仿真引擎结果"),
  research_backtest_result: () => translateText("研究引擎结果"),
  simulation_backtest_result: () => translateText("模拟引擎结果"),
  backtest_report: () => translateText("回测报告"),
  simulation_backtest_report: () => translateText("模拟报告"),
  report: () => translateText("报告"),
  diagnostics: () => translateText("诊断"),
  backtest_diagnostics: () => translateText("回测诊断"),
  leakage_audit: () => translateText("泄漏审计"),
  backtest_leakage: () => translateText("泄漏审计"),
  pnl: () => translateText("收益分解"),
  backtest_pnl: () => translateText("回测收益分解"),
  positions: () => translateText("持仓"),
  backtest_positions: () => translateText("回测持仓路径"),
  orders: () => translateText("委托"),
  fills: () => translateText("成交"),
  scenario_summary: () => translateText("压力场景"),
  scenarios: () => translateText("压力场景"),
  backtest_scenarios: () => translateText("回测压力场景"),
};

const FRESHNESS_LABELS: Record<string, () => string> = {
  fresh: () => translateText("新鲜"),
  stale: () => translateText("偏旧"),
  delayed: () => translateText("延迟"),
  unknown: () => I18N.status.unknown,
};

const MODALITY_LABELS: Record<string, () => string> = {
  market: () => translateText("市场"),
  macro: () => translateText("宏观"),
  on_chain: () => translateText("链上"),
  derivatives: () => translateText("衍生品"),
  multimodal_bundle: () => translateText("多模态特征集"),
  sentiment_events: () => "NLP",
  nlp: () => "NLP",
};

export function formatStatusLabel(status: string | null | undefined): string {
  const normalized = (status ?? "unknown").toLowerCase();
  return STATUS_LABELS[normalized]?.() ?? normalized;
}

export function formatJobTypeLabel(jobType: string | null | undefined): string {
  const normalized = (jobType ?? "").toLowerCase();
  return JOB_TYPE_LABELS[normalized]?.() ?? (jobType || "--");
}

export function formatSourceTypeLabel(sourceType: string | null | undefined): string {
  const normalized = (sourceType ?? "").toLowerCase();
  return SOURCE_TYPE_LABELS[normalized]?.() ?? (sourceType || "--");
}

export function formatStageNameLabel(stageName: string | null | undefined): string {
  const normalized = (stageName ?? "").toLowerCase();
  return STAGE_NAME_LABELS[normalized]?.() ?? (stageName || "--");
}

export function formatArtifactLabel(kind: string | null | undefined, fallback?: string): string {
  const normalized = (kind ?? "").toLowerCase();
  return ARTIFACT_LABELS[normalized]?.() ?? fallback ?? (normalized || "--");
}

export function formatFreshnessLabel(freshness: string | null | undefined): string {
  const normalized = (freshness ?? "unknown").toLowerCase();
  return FRESHNESS_LABELS[normalized]?.() ?? (freshness || "--");
}

export function formatModalityLabel(modality: string | null | undefined): string {
  const normalized = (modality ?? "").toLowerCase();
  return MODALITY_LABELS[normalized]?.() ?? (modality || "--");
}
