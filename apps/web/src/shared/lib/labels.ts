import { I18N } from "./i18n";

const STATUS_LABELS: Record<string, () => string> = {
  queued: () => I18N.status.queued,
  running: () => I18N.status.running,
  success: () => I18N.status.success,
  failed: () => I18N.status.failed,
  error: () => I18N.status.failed,
  partial: () => I18N.status.partial,
  ready: () => "可训练",
  warning: () => "需留意",
  not_ready: () => "暂不可训练",
  benchmark: () => I18N.status.benchmark,
  unknown: () => I18N.status.unknown,
};

const JOB_TYPE_LABELS: Record<string, () => string> = {
  train: () => "训练任务",
  backtest: () => "回测任务",
  dataset_request: () => "数据申请",
  dataset_pipeline: () => "数据闭环编排",
  dataset_multimodal_train: () => "多模态训练闭环",
  prepare: () => "数据准备",
  build: () => "数据构建",
  acquisition: () => "数据采集",
};

const SOURCE_TYPE_LABELS: Record<string, () => string> = {
  run: () => I18N.sourceType.run,
  benchmark: () => I18N.sourceType.benchmark,
};

const STAGE_NAME_LABELS: Record<string, () => string> = {
  acquire_base: () => "申请基础数据",
  prepare_base: () => "构建基础数据",
  readiness_base: () => "基础就绪校验",
  build_fusion: () => "构建融合数据",
  readiness_fusion: () => "融合就绪校验",
  acquire: () => "申请数据",
  prepare_dataset: () => "检查数据集",
  prepare: () => I18N.stage.prepare,
  train: () => I18N.stage.train,
  train_market: () => "训练市场模态",
  train_macro: () => "训练宏观模态",
  train_on_chain: () => "训练链上模态",
  train_derivatives: () => "训练衍生品模态",
  train_nlp: () => "训练 NLP 模态",
  inspect: () => "校验融合输入",
  compose: () => "生成融合模型",
  predict: () => I18N.stage.predict,
  backtest: () => I18N.stage.backtest,
  readiness: () => "就绪校验",
  review: () => "结果复核",
};

const ARTIFACT_LABELS: Record<string, () => string> = {
  tracking_summary: () => "跟踪摘要",
  train_manifest: () => "训练清单",
  legacy_manifest: () => "旧版清单",
  model_state: () => "模型状态",
  model_metadata: () => "模型元数据",
  feature_importance: () => "特征重要性",
  evaluation_summary: () => "评估摘要",
  prediction_frame: () => "预测结果",
  benchmark_json: () => "基准 JSON",
  benchmark_markdown: () => "基准 Markdown",
  benchmark_csv: () => "基准 CSV",
  research_result: () => "研究引擎结果",
  simulation_result: () => "仿真引擎结果",
  research_backtest_result: () => "研究引擎结果",
  simulation_backtest_result: () => "模拟引擎结果",
  backtest_report: () => "回测报告",
  simulation_backtest_report: () => "模拟报告",
  report: () => "报告",
  diagnostics: () => "诊断",
  backtest_diagnostics: () => "回测诊断",
  leakage_audit: () => "泄漏审计",
  backtest_leakage: () => "回测泄漏审计",
  pnl: () => "收益拆解",
  backtest_pnl: () => "回测收益拆解",
  positions: () => "持仓",
  backtest_positions: () => "回测持仓路径",
  orders: () => "委托",
  fills: () => "成交",
  scenario_summary: () => "压力场景",
  scenarios: () => "压力场景",
  backtest_scenarios: () => "回测压力场景",
};

const FRESHNESS_LABELS: Record<string, () => string> = {
  fresh: () => "新鲜",
  stale: () => "偏旧",
  delayed: () => "延迟",
  unknown: () => I18N.status.unknown,
};

const MODALITY_LABELS: Record<string, () => string> = {
  market: () => "市场",
  macro: () => "宏观",
  on_chain: () => "链上",
  derivatives: () => "衍生品",
  multimodal_bundle: () => "多模态特征集",
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
