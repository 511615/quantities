import type {
  DatasetDetailView,
  DatasetFieldGroupView,
  DatasetQualitySummaryView,
  DatasetReadinessSummaryView,
  DatasetSummaryView,
  JobStatusView,
  TrainingDatasetSummaryView,
} from "../../shared/api/types";
import { formatFreshnessLabel } from "../../shared/lib/labels";

export type DatasetDomain =
  | "market"
  | "derivatives"
  | "on_chain"
  | "macro"
  | "sentiment_events";

export type DatasetType =
  | "display_slice"
  | "training_panel"
  | "feature_snapshot"
  | "fusion_training_panel";

export type DatasetEntityScope =
  | "single_asset"
  | "multi_asset"
  | "macro_series"
  | "event_stream"
  | "unknown";

export type DatasetBrowserFilters = {
  data_domain: string;
  dataset_type: string;
  source: string;
  exchange: string;
  symbol: string;
  frequency: string;
  version: string;
  time_from: string;
  time_to: string;
};

export type DatasetFacetSet = {
  domains: string[];
  types: string[];
  sources: string[];
  exchanges: string[];
  symbols: string[];
  frequencies: string[];
  versions: string[];
};

export type DatasetKvEntry = {
  key: string;
  label: string;
  value: string;
};

export type DatasetReadinessChecklistItem = {
  key: string;
  label: string;
  value: string;
  tone: "success" | "warning" | "failed" | "unknown";
};

export type DatasetWorkbenchSummary = {
  raw: DatasetSummaryView;
  datasetId: string;
  title: string;
  subtitle: string;
  dataDomain: DatasetDomain;
  dataDomainLabel: string;
  datasetType: DatasetType;
  datasetTypeLabel: string;
  sourceVendor: string;
  exchange: string | null;
  exchangeLabel: string;
  symbol: string | null;
  symbolLabel: string;
  symbolsPreview: string[];
  entityScope: DatasetEntityScope;
  entityScopeLabel: string;
  entityCount: number | null;
  frequency: string | null;
  frequencyLabel: string;
  snapshotVersion: string;
  coverageLabel: string;
  timeRangeLabel: string;
  freshnessStatus: string;
  freshnessLabel: string;
  freshnessSummary: string;
  qualityStatus: string;
  qualityLabel: string;
  healthStatus: string;
  healthLabel: string;
  readinessStatus: string;
  readinessLabel: string;
  buildStatus: string;
  buildStatusLabel: string;
  requestOriginLabel: string;
  rowCount: number | null;
  featureCount: number | null;
  labelCount: number | null;
  labelHorizon: number | null;
  rowCountLabel: string;
  featureCountLabel: string;
  labelCountLabel: string;
  labelHorizonLabel: string;
  summaryText: string;
  intendedUseText: string;
  riskNoteText: string;
  technicalId: string;
  asOfTime: string | null;
  isSmoke: boolean;
};

export type DatasetReadinessViewModel = {
  rawStatus: string;
  statusLabel: string;
  summary: string;
  source: "backend" | "fallback";
  blockingIssues: string[];
  warnings: string[];
  recommendedNextActions: string[];
  checklist: DatasetReadinessChecklistItem[];
};

export type DatasetWorkbenchDetail = {
  raw: DatasetDetailView;
  summary: DatasetWorkbenchSummary;
  heroSummary: string;
  intendedUse: string;
  riskNote: string;
  featureGroups: DatasetFieldGroupView[];
  labelColumns: string[];
  featureColumnsPreview: string[];
  qualitySummary: DatasetQualitySummaryView | null;
  samplePolicyEntries: DatasetKvEntry[];
  splitManifestEntries: DatasetKvEntry[];
  qualityEntries: DatasetKvEntry[];
  labelSpecEntries: DatasetKvEntry[];
  acquisitionEntries: DatasetKvEntry[];
  buildEntries: DatasetKvEntry[];
  schemaEntries: DatasetKvEntry[];
  trainingEntries: DatasetKvEntry[];
  sourceLineage: string[];
  readiness: DatasetReadinessViewModel;
};

export type TrainingDatasetWorkbenchItem = {
  datasetId: string;
  title: string;
  subtitle: string;
  technicalId: string;
  datasetType: DatasetType;
  datasetTypeLabel: string;
  dataDomain: DatasetDomain;
  dataDomainLabel: string;
  sourceVendor: string;
  entityScope: string;
  entityScopeLabel: string;
  universeSummary: string;
  sampleCountLabel: string;
  featureCountLabel: string;
  labelCountLabel: string;
  labelHorizonLabel: string;
  splitStrategyLabel: string;
  frequencyLabel: string;
  freshnessLabel: string;
  qualityLabel: string;
  readinessStatus: string;
  readinessLabel: string;
  readinessReason: string;
  snapshotVersion: string;
};

const DOMAIN_LABELS: Record<DatasetDomain, string> = {
  market: "市场数据",
  derivatives: "衍生品数据",
  on_chain: "链上数据",
  macro: "宏观数据",
  sentiment_events: "情绪 / 事件数据",
};

const TYPE_LABELS: Record<DatasetType, string> = {
  display_slice: "展示切片",
  training_panel: "训练面板",
  feature_snapshot: "特征快照",
  fusion_training_panel: "融合训练面板",
};

const ENTITY_SCOPE_LABELS: Record<DatasetEntityScope, string> = {
  single_asset: "单资产",
  multi_asset: "多资产",
  macro_series: "宏观序列",
  event_stream: "事件流",
  unknown: "未知",
};

const DOMAIN_SUMMARIES: Record<DatasetDomain, string> = {
  market: "单资产价格、成交量与波动率序列。",
  derivatives: "来自衍生品市场的利率、资金费率与持仓量指标。",
  on_chain: "链上转账、余额、钱包活跃度等原生区块链指标。",
  macro: "从公开来源与指数采集的宏观经济序列。",
  sentiment_events: "承载新闻、社媒讨论与情绪信号的事件流。",
};

const TYPE_SUMMARIES: Record<DatasetType, string> = {
  display_slice: "用于监控数值或快速审计数据的参考切片。",
  training_panel: "为训练与基准评测准备好的完整带标签表。",
  feature_snapshot: "无需回到原始数据重建、可直接做下游融合的聚合特征集合。",
  fusion_training_panel: "组合多种互补信号的多域训练面板。",
};

const PRETTY_LABELS: Record<string, string> = {
  asset_mode: "资产模式",
  asset_id: "资产 ID",
  available_time: "可用时间",
  as_of_time: "截至时间",
  build_status: "构建状态",
  calendar_mode: "日历模式",
  data_domain: "数据域",
  data_source: "数据源",
  dataset_type: "数据集类型",
  drop_unaligned_rows: "丢弃未对齐行",
  end_time: "结束时间",
  entity_count: "实体数量",
  entity_scope: "实体范围",
  exchange: "交易所",
  feature_count: "特征数量",
  feature_schema_hash: "特征模式哈希",
  frequency: "频率",
  join_key: "连接键",
  label_count: "标签数量",
  label_horizon: "标签窗口",
  label_kind: "标签类型",
  max_missing_ratio: "最大缺失比例",
  min_entity_coverage_ratio: "最小实体覆盖比例",
  missing_feature_policy: "缺失特征策略",
  quality_status: "质量状态",
  readiness_status: "就绪状态",
  request_name: "申请名称",
  request_origin: "请求来源",
  sample_count: "样本数量",
  sample_policy: "样本策略",
  selection_mode: "选择方式",
  source_vendor: "主来源",
  split_strategy: "切分策略",
  start_time: "开始时间",
  symbol_count: "标的数量",
  symbol_type: "标的类型",
  symbols: "标的列表",
  symbols_preview: "标的预览",
  temporal_safety_status: "时间安全",
  time_window: "时间窗口",
  usable_sample_count: "可用样本数量",
};

function compactText(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function firstText(...values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    const normalized = compactText(value);
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

function normalizeSource(source: string | null | undefined): string {
  const value = (source ?? "").trim().toLowerCase();
  if (!value) {
    return "内部数据";
  }
  if (value.includes("binance")) {
    return "Binance";
  }
  if (value.includes("okx")) {
    return "OKX";
  }
  if (value.includes("fred")) {
    return "FRED";
  }
  if (value.includes("stooq")) {
    return "Stooq";
  }
  if (value.includes("chain")) {
    return "链上源";
  }
  if (value.includes("sentiment") || value.includes("news") || value.includes("event")) {
    return "事件源";
  }
  return source ?? "内部数据";
}

function inferExchange(source: string | null | undefined): string | null {
  const value = (source ?? "").toLowerCase();
  if (value.includes("binance")) {
    return "Binance";
  }
  if (value.includes("okx")) {
    return "OKX";
  }
  if (value.includes("bybit")) {
    return "Bybit";
  }
  return null;
}

function inferDomain(summary: DatasetSummaryView, detail?: DatasetDetailView | null): DatasetDomain {
  const explicit = firstText(
    summary.data_domain,
    detail?.acquisition_profile?.data_domain as string | undefined,
  )?.toLowerCase();
  if (explicit === "market" || explicit === "derivatives" || explicit === "on_chain") {
    return explicit;
  }
  if (explicit === "macro" || explicit === "sentiment_events") {
    return explicit;
  }

  const joined = [
    summary.dataset_id,
    summary.data_source,
    summary.source_vendor,
    summary.dataset_category,
    summary.asset_id,
    detail?.summary,
    detail?.subtitle,
    detail?.acquisition_profile?.data_domain as string | undefined,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (
    joined.includes("funding") ||
    joined.includes("open_interest") ||
    joined.includes("perp") ||
    joined.includes("derivative")
  ) {
    return "derivatives";
  }
  if (joined.includes("macro") || joined.includes("fred") || joined.includes("stooq")) {
    return "macro";
  }
  if (joined.includes("chain") || joined.includes("on_chain") || joined.includes("wallet")) {
    return "on_chain";
  }
  if (
    joined.includes("sentiment") ||
    joined.includes("event") ||
    joined.includes("news") ||
    joined.includes("social")
  ) {
    return "sentiment_events";
  }
  return "market";
}

function inferType(summary: DatasetSummaryView, detail?: DatasetDetailView | null): DatasetType {
  const explicit = firstText(
    summary.dataset_type,
    detail?.acquisition_profile?.dataset_type as string | undefined,
    summary.dataset_category,
  )?.toLowerCase();

  if (explicit === "fusion_training_panel" || explicit?.includes("fusion")) {
    return "fusion_training_panel";
  }
  if (explicit?.includes("feature") || explicit?.includes("snapshot")) {
    return "feature_snapshot";
  }
  if (explicit?.includes("training") || explicit?.includes("panel")) {
    return "training_panel";
  }
  if (explicit?.includes("display") || explicit?.includes("slice")) {
    return "display_slice";
  }

  const joined = [
    summary.dataset_id,
    summary.dataset_category,
    detail?.summary,
    detail?.subtitle,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (joined.includes("snapshot") || joined.includes("feature")) {
    return "feature_snapshot";
  }

  if (
    (summary.feature_count ?? 0) > 0 ||
    (summary.label_count ?? 0) > 0 ||
    summary.label_horizon !== null ||
    Boolean(summary.split_strategy) ||
    (detail?.label_columns.length ?? 0) > 0
  ) {
    return "training_panel";
  }

  return "display_slice";
}

function inferEntityScope(summary: DatasetSummaryView, detail?: DatasetDetailView | null): DatasetEntityScope {
  const explicit = firstText(
    summary.entity_scope,
    detail?.acquisition_profile?.entity_scope as string | undefined,
    detail?.training_profile?.entity_scope as string | undefined,
  )?.toLowerCase();

  if (explicit === "single_asset") {
    return "single_asset";
  }
  if (explicit === "multi_asset") {
    return "multi_asset";
  }
  if (explicit === "macro_series") {
    return "macro_series";
  }
  if (explicit === "event_stream") {
    return "event_stream";
  }

  const joined = [
    summary.dataset_id,
    summary.asset_id,
    detail?.summary,
    detail?.subtitle,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (joined.includes("macro") || joined.includes("fred") || joined.includes("stooq")) {
    return "macro_series";
  }
  if (joined.includes("event") || joined.includes("sentiment")) {
    return "event_stream";
  }
  if (
    joined.includes("multi") ||
    joined.includes("panel") ||
    joined.includes("universe") ||
    joined.includes("cross_section")
  ) {
    return "multi_asset";
  }
  if (summary.asset_id) {
    return "single_asset";
  }
  return "unknown";
}

function formatFrequency(value: string | null | undefined): string {
  const raw = (value ?? "").trim();
  if (!raw) {
    return "未标注";
  }
  if (raw.endsWith("m")) {
    return `${raw.slice(0, -1)} 分钟`;
  }
  if (raw.endsWith("h")) {
    return `${raw.slice(0, -1)} 小时`;
  }
  if (raw.endsWith("d")) {
    return `${raw.slice(0, -1)} 天`;
  }
  if (raw.endsWith("w")) {
    return `${raw.slice(0, -1)} 周`;
  }
  return raw;
}

function formatVersion(value: string | null | undefined): string {
  if (!value) {
    return "当前版本";
  }
  return value.slice(0, 10);
}

function formatSplitStrategy(value: string | null | undefined): string {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized) {
    return "待补充";
  }
  if (normalized === "time_walk_forward" || normalized === "walk_forward") {
    return "时间滚动切分";
  }
  if (normalized === "time_series" || normalized === "chronological_holdout") {
    return "时间顺序切分";
  }
  if (normalized === "random_split") {
    return "随机切分";
  }
  if (normalized === "stratified_split") {
    return "分层切分";
  }
  return value ?? "待补充";
}

function formatCoverage(summary: DatasetSummaryView): string {
  if (summary.time_range_label) {
    return summary.time_range_label;
  }
  if (summary.freshness.data_start_time || summary.freshness.data_end_time) {
    const start = summary.freshness.data_start_time?.slice(0, 10) ?? "--";
    const end = summary.freshness.data_end_time?.slice(0, 10) ?? "--";
    return `${start} - ${end}`;
  }
  return "覆盖范围待补充";
}

function formatQualityStatus(status: string | null | undefined, isSmoke = false): string {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "healthy" || normalized === "ok" || normalized === "pass") {
    return "健康";
  }
  if (normalized === "warning" || normalized === "partial") {
    return "需留意";
  }
  if (normalized === "error" || normalized === "failed") {
    return "异常";
  }
  if (isSmoke) {
    return "演示样本";
  }
  return "待确认";
}

function formatReadinessStatus(status: string | null | undefined): string {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "ready" || normalized === "success") {
    return "可训练";
  }
  if (normalized === "warning" || normalized === "partial") {
    return "可训练但需留意";
  }
  if (normalized === "not_ready" || normalized === "failed" || normalized === "blocked") {
    return "暂不可训练";
  }
  return "待确认";
}

function formatBuildStatus(status: string | null | undefined): string {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "success" || normalized === "completed" || normalized === "ready") {
    return "已完成";
  }
  if (normalized === "running" || normalized === "queued") {
    return "构建中";
  }
  if (normalized === "failed" || normalized === "error") {
    return "构建失败";
  }
  return "待确认";
}

function formatRequestOrigin(value: string | null | undefined): string {
  const normalized = (value ?? "").trim();
  if (!normalized) {
    return "目录发现";
  }
  if (normalized === "dataset_request") {
    return "申请生成";
  }
  if (normalized === "preset") {
    return "预置数据集";
  }
  return normalized;
}

function formatCount(value: number | null | undefined, fallback = "待补充"): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return fallback;
  }
  return value.toLocaleString("zh-CN");
}

function humanizeKey(value: string): string {
  return PRETTY_LABELS[value] ?? value.replace(/_/g, " ").replace(/\b\w/g, (item) => item.toUpperCase());
}

function stringifyValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  if (typeof value === "number") {
    return value.toLocaleString("zh-CN");
  }
  if (typeof value === "boolean") {
    return value ? "是" : "否";
  }
  if (Array.isArray(value)) {
    return value.length > 0 ? value.map((item) => stringifyValue(item)).join(" / ") : "--";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function formatUniverseSummary(
  value: string | Record<string, unknown> | null | undefined,
  entityScope?: string | null,
): string {
  if (!value) {
    return "范围待补充";
  }
  if (typeof value === "string") {
    return value;
  }

  const entityCount =
    typeof value.entity_count === "number" && !Number.isNaN(value.entity_count)
      ? value.entity_count
      : null;
  const symbolsPreview = Array.isArray(value.symbols_preview)
    ? value.symbols_preview.filter((item): item is string => typeof item === "string" && Boolean(item))
    : [];
  const explicitScope =
    typeof value.entity_scope === "string" && value.entity_scope
      ? value.entity_scope
      : entityScope ?? null;

  if (explicitScope === "multi_asset") {
    const previewText = symbolsPreview.length > 0 ? ` · ${symbolsPreview.slice(0, 3).join(" / ")}` : "";
    return `${entityCount ? `${entityCount} 个实体` : "多资产"}${previewText}`;
  }
  if (explicitScope === "single_asset") {
    return symbolsPreview[0] ?? "单资产";
  }
  if (explicitScope === "macro_series") {
    return `${entityCount ? `${entityCount} 个序列` : "宏观序列"}${symbolsPreview.length > 0 ? ` · ${symbolsPreview.slice(0, 3).join(" / ")}` : ""}`;
  }
  if (symbolsPreview.length > 0) {
    return symbolsPreview.slice(0, 3).join(" / ");
  }
  if (entityCount) {
    return `${entityCount} 个实体`;
  }
  return stringifyValue(value);
}

function toEntries(record: Record<string, unknown> | null | undefined, keys?: string[]): DatasetKvEntry[] {
  if (!record) {
    return [];
  }

  const source = keys
    ? keys
        .filter((key) => key in record)
        .map((key) => [key, record[key]] as const)
    : Object.entries(record);

  return source
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([key, value]) => ({
      key,
      label: humanizeKey(key),
      value: stringifyValue(value),
    }));
}

function createFallbackFeatureGroups(columns: string[]): DatasetFieldGroupView[] {
  if (columns.length === 0) {
    return [];
  }

  return [
    {
      key: "feature_preview",
      label: "字段预览",
      description: "后端暂未提供完整字段分组，这里先把已知特征列作为一组展示。",
      count: columns.length,
      columns,
    },
  ];
}

function createReadableTitle(
  summary: DatasetSummaryView,
  domain: DatasetDomain,
  type: DatasetType,
  sourceVendor: string,
  frequencyLabel: string,
): string {
  if (compactText(summary.display_name)) {
    return summary.display_name as string;
  }

  const assetPart = summary.asset_id ? ` ${summary.asset_id}` : "";
  const freqPart = summary.frequency ? ` ${frequencyLabel}` : "";
  const englishType =
    type === "training_panel"
      ? "训练面板"
      : type === "feature_snapshot"
        ? "特征快照"
        : "展示切片";

  if (domain === "macro") {
    return `${sourceVendor}${freqPart}宏观因子数据集 / ${englishType}`;
  }
  if (domain === "sentiment_events") {
    return `${sourceVendor}${assetPart}${freqPart}事件数据集 / ${englishType}`;
  }
  if (domain === "on_chain") {
    return `${sourceVendor}${assetPart}${freqPart}链上数据集 / ${englishType}`;
  }
  if (domain === "derivatives") {
    return `${sourceVendor}${assetPart}${freqPart}衍生品数据集 / ${englishType}`;
  }
  return `${sourceVendor}${assetPart}${freqPart}${TYPE_LABELS[type]} / ${englishType}`;
}

function createSubtitle(
  summary: DatasetSummaryView,
  domain: DatasetDomain,
  type: DatasetType,
  coverageLabel: string,
  entityScope: DatasetEntityScope,
): string {
  if (compactText(summary.subtitle)) {
    return summary.subtitle as string;
  }
  return `${DOMAIN_LABELS[domain]} · ${TYPE_LABELS[type]} · ${ENTITY_SCOPE_LABELS[entityScope]} · ${coverageLabel}`;
}

function createSummaryText(
  summary: DatasetSummaryView,
  domain: DatasetDomain,
  type: DatasetType,
  sourceVendor: string,
  entityScope: DatasetEntityScope,
): string {
  const assetPart = summary.asset_id ? `主要围绕 ${summary.asset_id}` : "覆盖一组研究对象";
  const frequencyPart = summary.frequency ? `，频率为 ${formatFrequency(summary.frequency)}` : "";
  const scopePart =
    entityScope === "multi_asset"
      ? "，按多资产统一面板组织"
      : entityScope === "single_asset"
        ? "，以单资产切片组织"
        : "";

  return `这是一个来自 ${sourceVendor} 的${DOMAIN_LABELS[domain]}${TYPE_LABELS[type]}，${assetPart}${frequencyPart}${scopePart}。${DOMAIN_SUMMARIES[domain]}`;
}

function createIntendedUse(summary: DatasetSummaryView, type: DatasetType, domain: DatasetDomain): string {
  if (summary.is_smoke) {
    return "更适合联调、烟雾验证和工作流排查，不建议直接拿来做长期结论。";
  }
  if (type === "fusion_training_panel") {
    return "适合做跨域训练、可用时间对齐检查和融合特征覆盖评估，也适合和单域训练面板并排比较。";
  }
  if (type === "training_panel") {
    return "适合直接进入训练、验证和回测前的数据检查，也适合比较不同训练面板的覆盖和标签设计。";
  }
  if (type === "feature_snapshot") {
    return "适合作为上游特征资产复用，先检查可用字段和质量，再决定是否纳入训练链路。";
  }
  if (domain === "macro") {
    return "适合做宏观背景判断、风险偏好分析和跨市场解释变量浏览。";
  }
  return "适合先看图、看样本和看覆盖范围，帮助快速理解这份数据是否值得继续深挖。";
}

function createRiskNote(summary: DatasetSummaryView, qualityLabel: string, freshnessLabel: string): string {
  if (summary.is_smoke) {
    return "这是演示性质的数据样本，窗口和样本量都偏小，容易高估可泛化性。";
  }
  if (freshnessLabel !== "新鲜") {
    return `当前数据新鲜度为“${freshnessLabel}”，在做接近实时的研究判断前需要先确认更新时间。`;
  }
  if (qualityLabel !== "健康") {
    return `当前质量状态为“${qualityLabel}”，建议先检查质量摘要、缺失率和时间安全说明。`;
  }
  return "正式使用前仍建议核对覆盖窗口、标签定义和切分方式，避免把结构差异当成模型能力。";
}

function normalizeDetailRecord(detail: DatasetDetailView): DatasetDetailView {
  return {
    ...detail,
    label_columns: Array.isArray(detail.label_columns) ? detail.label_columns : [],
    feature_columns_preview: Array.isArray(detail.feature_columns_preview)
      ? detail.feature_columns_preview
      : [],
    feature_groups: Array.isArray(detail.feature_groups) ? detail.feature_groups : [],
    glossary_hints: Array.isArray(detail.glossary_hints) ? detail.glossary_hints : [],
    label_spec: detail.label_spec ?? {},
    split_manifest: detail.split_manifest ?? {},
    sample_policy: detail.sample_policy ?? {},
    quality: detail.quality ?? {},
    links: Array.isArray(detail.links) ? detail.links : [],
    quality_summary: detail.quality_summary ?? null,
    acquisition_profile: detail.acquisition_profile ?? null,
    build_profile: detail.build_profile ?? null,
    schema_profile: detail.schema_profile ?? null,
    readiness_profile: detail.readiness_profile ?? null,
    training_profile: detail.training_profile ?? null,
  };
}

export function adaptDatasetSummary(summary: DatasetSummaryView): DatasetWorkbenchSummary {
  const dataDomain = inferDomain(summary, null);
  const datasetType = inferType(summary, null);
  const sourceVendor = normalizeSource(firstText(summary.source_vendor, summary.data_source));
  const frequencyLabel = formatFrequency(summary.frequency);
  const entityScope = inferEntityScope(summary, null);
  const coverageLabel = formatCoverage(summary);
  const qualityLabel = formatQualityStatus(summary.quality_status, summary.is_smoke);
  const title = createReadableTitle(summary, dataDomain, datasetType, sourceVendor, frequencyLabel);
  const subtitle = createSubtitle(summary, dataDomain, datasetType, coverageLabel, entityScope);

  return {
    raw: summary,
    datasetId: summary.dataset_id,
    title,
    subtitle,
    dataDomain,
    dataDomainLabel: DOMAIN_LABELS[dataDomain],
    datasetType,
    datasetTypeLabel: TYPE_LABELS[datasetType],
    sourceVendor,
    exchange: firstText(summary.exchange, inferExchange(summary.data_source)),
    exchangeLabel: firstText(summary.exchange, inferExchange(summary.data_source)) ?? "非交易所 / 未标注",
    symbol: summary.asset_id,
    symbolLabel: summary.asset_id ?? "多资产 / 非 symbol 数据",
    symbolsPreview: Array.isArray(summary.symbols_preview) ? summary.symbols_preview : [],
    entityScope,
    entityScopeLabel: ENTITY_SCOPE_LABELS[entityScope],
    entityCount: summary.entity_count ?? null,
    frequency: summary.frequency,
    frequencyLabel,
    snapshotVersion: formatVersion(summary.snapshot_version ?? summary.as_of_time),
    coverageLabel,
    timeRangeLabel: coverageLabel,
    freshnessStatus: summary.freshness.status,
    freshnessLabel: formatFreshnessLabel(summary.freshness.status),
    freshnessSummary: summary.freshness.summary || "新鲜度说明待补充",
    qualityStatus: summary.quality_status ?? "unknown",
    qualityLabel,
    healthStatus: summary.quality_status ?? "unknown",
    healthLabel: qualityLabel,
    readinessStatus: summary.readiness_status ?? "unknown",
    readinessLabel: formatReadinessStatus(summary.readiness_status),
    buildStatus: summary.build_status ?? "unknown",
    buildStatusLabel: formatBuildStatus(summary.build_status),
    requestOriginLabel: formatRequestOrigin(summary.request_origin),
    rowCount: summary.row_count ?? summary.sample_count,
    featureCount: summary.feature_count,
    labelCount: summary.label_count,
    labelHorizon: summary.label_horizon,
    rowCountLabel: formatCount(summary.row_count ?? summary.sample_count),
    featureCountLabel: formatCount(summary.feature_count),
    labelCountLabel: formatCount(summary.label_count),
    labelHorizonLabel:
      summary.label_horizon === null || summary.label_horizon === undefined
        ? "不适用"
        : `${summary.label_horizon} 个 bar`,
    summaryText: createSummaryText(summary, dataDomain, datasetType, sourceVendor, entityScope),
    intendedUseText: createIntendedUse(summary, datasetType, dataDomain),
    riskNoteText: createRiskNote(summary, qualityLabel, formatFreshnessLabel(summary.freshness.status)),
    technicalId: summary.dataset_id,
    asOfTime: summary.as_of_time,
    isSmoke: summary.is_smoke,
  };
}

function createReadinessChecklist(
  readiness: DatasetReadinessSummaryView,
): DatasetReadinessChecklistItem[] {
  const normalizeTone = (value: string | boolean | null | undefined) => {
    if (typeof value === "boolean") {
      return value ? "success" : "failed";
    }
    const normalized = (value ?? "").toString().toLowerCase();
    if (
      normalized === "pass" ||
      normalized === "passed" ||
      normalized === "healthy" ||
      normalized === "aligned" ||
      normalized === "ready" ||
      normalized === "success"
    ) {
      return "success";
    }
    if (
      normalized === "warning" ||
      normalized === "partial" ||
      normalized === "stale" ||
      normalized === "delayed"
    ) {
      return "warning";
    }
    if (
      normalized === "failed" ||
      normalized === "error" ||
      normalized === "not_ready" ||
      normalized === "broken"
    ) {
      return "failed";
    }
    return "unknown";
  };

  return [
    {
      key: "alignment",
      label: "对齐状态",
      value: stringifyValue(readiness.alignment_status),
      tone: normalizeTone(readiness.alignment_status),
    },
    {
      key: "missing_feature",
      label: "缺失特征控制",
      value: stringifyValue(readiness.missing_feature_status),
      tone: normalizeTone(readiness.missing_feature_status),
    },
    {
      key: "label_alignment",
      label: "标签对齐",
      value: stringifyValue(readiness.label_alignment_status),
      tone: normalizeTone(readiness.label_alignment_status),
    },
    {
      key: "split_integrity",
      label: "切分完整性",
      value: stringifyValue(readiness.split_integrity_status),
      tone: normalizeTone(readiness.split_integrity_status),
    },
    {
      key: "temporal_safety",
      label: "时间安全",
      value: stringifyValue(readiness.temporal_safety_status),
      tone: normalizeTone(readiness.temporal_safety_status),
    },
    {
      key: "freshness",
      label: "新鲜度",
      value: stringifyValue(readiness.freshness_status),
      tone: normalizeTone(readiness.freshness_status),
    },
  ];
}

function createFallbackReadiness(detail: DatasetDetailView): DatasetReadinessViewModel {
  const featureCount = detail.feature_count ?? detail.dataset.feature_count ?? 0;
  const labelCount = detail.label_count ?? detail.dataset.label_count ?? 0;
  const hasSplit =
    Boolean(detail.dataset.split_strategy) ||
    Boolean((detail.split_manifest.strategy as string | undefined) ?? null);
  const freshnessLabel = formatFreshnessLabel(detail.dataset.freshness.status);
  const qualityLabel = formatQualityStatus(detail.quality_summary?.status, detail.dataset.is_smoke);
  const blockingIssues: string[] = [];
  const warnings: string[] = [];

  if (featureCount <= 0) {
    blockingIssues.push("缺少可用特征维度。");
  }
  if (labelCount <= 0) {
    blockingIssues.push("缺少明确标签列。");
  }
  if (!hasSplit) {
    blockingIssues.push("缺少可验证的切分方式。");
  }
  if (!compactText(detail.dataset.temporal_safety_summary)) {
    warnings.push("时间安全说明暂未补齐。");
  }
  if (freshnessLabel !== "新鲜") {
    warnings.push(`数据新鲜度为“${freshnessLabel}”。`);
  }
  if (qualityLabel !== "健康") {
    warnings.push(`质量状态为“${qualityLabel}”。`);
  }

  const rawStatus =
    blockingIssues.length > 0
      ? "not_ready"
      : warnings.length > 0 || detail.dataset.is_smoke
        ? "warning"
        : "ready";

  return {
    rawStatus,
    statusLabel: formatReadinessStatus(rawStatus),
    source: "fallback",
    summary:
      rawStatus === "ready"
        ? "后端还没有显式训练就绪摘要，前端按现有详情字段判断这份数据可以进入训练前检查。"
        : rawStatus === "warning"
          ? "当前是按现有详情字段做兼容判断，建议把新鲜度、质量和时间安全说明一起核对。"
          : "当前详情字段显示这份数据还不满足训练最小条件，建议先补齐标签、切分或特征元数据。",
    blockingIssues,
    warnings,
    recommendedNextActions:
      rawStatus === "not_ready"
        ? ["先补齐标签定义、切分方式或特征字段后再尝试训练。"]
        : rawStatus === "warning"
          ? ["先核对新鲜度、质量摘要和时间安全说明，再决定是否进入训练。"]
          : ["可以继续进入训练前检查或直接在训练页比较同类面板。"],
    checklist: [
      {
        key: "feature_dimensions",
        label: "特征维度",
        value: featureCount > 0 ? `${featureCount} 维` : "缺失",
        tone: featureCount > 0 ? "success" : "failed",
      },
      {
        key: "label_columns",
        label: "标签列",
        value: labelCount > 0 ? `${labelCount} 列` : "缺失",
        tone: labelCount > 0 ? "success" : "failed",
      },
      {
        key: "split_strategy",
        label: "切分方式",
        value: hasSplit ? stringifyValue(detail.dataset.split_strategy ?? detail.split_manifest.strategy) : "缺失",
        tone: hasSplit ? "success" : "failed",
      },
      {
        key: "temporal_safety",
        label: "时间安全",
        value: compactText(detail.dataset.temporal_safety_summary) ?? "待补充",
        tone: compactText(detail.dataset.temporal_safety_summary) ? "success" : "warning",
      },
      {
        key: "freshness",
        label: "新鲜度",
        value: freshnessLabel,
        tone: freshnessLabel === "新鲜" ? "success" : "warning",
      },
      {
        key: "quality",
        label: "质量状态",
        value: qualityLabel,
        tone: qualityLabel === "健康" ? "success" : qualityLabel === "需留意" ? "warning" : "failed",
      },
    ],
  };
}

export function adaptDatasetReadiness(
  detail: DatasetDetailView,
  readiness?: DatasetReadinessSummaryView | null,
): DatasetReadinessViewModel {
  if (readiness) {
    return {
      rawStatus: readiness.readiness_status,
      statusLabel: formatReadinessStatus(readiness.readiness_status),
      source: "backend",
      summary:
        readiness.readiness_status === "ready"
          ? "后端已确认这份数据满足训练就绪的最小条件。"
          : readiness.readiness_status === "warning"
            ? "后端认为这份数据可以训练，但存在需要先留意的问题。"
            : "后端认为这份数据当前还不适合直接进入训练。",
      blockingIssues: readiness.blocking_issues ?? [],
      warnings: readiness.warnings ?? [],
      recommendedNextActions: readiness.recommended_next_actions ?? [],
      checklist: createReadinessChecklist(readiness),
    };
  }

  return createFallbackReadiness(detail);
}

export function adaptDatasetDetail(
  input: DatasetDetailView,
  readiness?: DatasetReadinessSummaryView | null,
): DatasetWorkbenchDetail {
  const detail = normalizeDetailRecord(input);
  const summary = adaptDatasetSummary(detail.dataset);
  const detailQualityLabel = formatQualityStatus(detail.quality_summary?.status, detail.dataset.is_smoke);
  const enrichedSummary: DatasetWorkbenchSummary = {
    ...summary,
    qualityStatus: detail.quality_summary?.status ?? summary.qualityStatus,
    qualityLabel: detailQualityLabel,
    healthStatus: detail.quality_summary?.status ?? summary.healthStatus,
    healthLabel: detailQualityLabel,
    readinessStatus: readiness?.readiness_status ?? summary.readinessStatus,
    readinessLabel: formatReadinessStatus(readiness?.readiness_status ?? summary.readinessStatus),
  };
  const featureGroups =
    detail.feature_groups.length > 0
      ? detail.feature_groups
      : createFallbackFeatureGroups(detail.feature_columns_preview);

  const acquisitionEntries = toEntries(detail.acquisition_profile, [
    "request_name",
    "data_domain",
    "source_vendor",
    "exchange",
    "time_window",
    "asset_mode",
    "symbol_type",
    "selection_mode",
    "symbols",
    "symbol_count",
  ]);
  if (acquisitionEntries.length === 0) {
    acquisitionEntries.push(
      { key: "source_vendor", label: "来源", value: enrichedSummary.sourceVendor },
      { key: "exchange", label: "交易所", value: enrichedSummary.exchangeLabel },
      { key: "coverage", label: "覆盖范围", value: enrichedSummary.coverageLabel },
      { key: "request_origin", label: "请求来源", value: enrichedSummary.requestOriginLabel },
    );
  }

  const buildEntries = toEntries(detail.build_profile, [
    "build_status",
    "snapshot_version",
    "usable_sample_count",
    "dropped_rows",
    "entity_count",
  ]);
  if (buildEntries.length === 0) {
    buildEntries.push(
      { key: "build_status", label: "构建状态", value: enrichedSummary.buildStatusLabel },
      { key: "snapshot_version", label: "版本", value: enrichedSummary.snapshotVersion },
      { key: "row_count", label: "样本条数", value: enrichedSummary.rowCountLabel },
    );
  }

  const schemaEntries = toEntries(detail.schema_profile, [
    "feature_count",
    "label_count",
    "feature_schema_hash",
    "missing_feature_policy",
    "feature_dimension_consistent",
  ]);
  if (schemaEntries.length === 0) {
    schemaEntries.push(
      { key: "feature_count", label: "特征维度", value: enrichedSummary.featureCountLabel },
      { key: "label_count", label: "标签数", value: enrichedSummary.labelCountLabel },
      { key: "label_columns", label: "标签列", value: detail.label_columns.join(" / ") || "--" },
    );
  }

  const trainingEntries = toEntries(detail.training_profile, [
    "sample_policy",
    "split_strategy",
    "entity_scope",
    "label_horizon",
    "universe_summary",
    "recommended_training_use",
  ]);
  if (trainingEntries.length === 0) {
    trainingEntries.push(
      { key: "sample_policy", label: "样本策略", value: detail.sample_policy ? stringifyValue(detail.sample_policy) : "--" },
      { key: "split_strategy", label: "切分方式", value: stringifyValue(detail.dataset.split_strategy ?? detail.split_manifest.strategy) },
      { key: "entity_scope", label: "实体范围", value: enrichedSummary.entityScopeLabel },
      { key: "label_horizon", label: "标签窗口", value: enrichedSummary.labelHorizonLabel },
    );
  }

  const readinessView = adaptDatasetReadiness(detail, readiness);

  return {
    raw: detail,
    summary: enrichedSummary,
    heroSummary: detail.summary ?? enrichedSummary.summaryText,
    intendedUse: detail.intended_use ?? enrichedSummary.intendedUseText,
    riskNote: detail.risk_note ?? createRiskNote(detail.dataset, detailQualityLabel, enrichedSummary.freshnessLabel),
    featureGroups,
    labelColumns: detail.label_columns,
    featureColumnsPreview: detail.feature_columns_preview,
    qualitySummary: detail.quality_summary,
    samplePolicyEntries: toEntries(detail.sample_policy),
    splitManifestEntries: toEntries(detail.split_manifest),
    qualityEntries: toEntries(detail.quality),
    labelSpecEntries: toEntries(detail.label_spec),
    acquisitionEntries,
    buildEntries,
    schemaEntries,
    trainingEntries,
    sourceLineage: [
      `${enrichedSummary.dataDomainLabel} / ${enrichedSummary.datasetTypeLabel}`,
      `${enrichedSummary.sourceVendor} / ${enrichedSummary.exchangeLabel}`,
      `版本 ${enrichedSummary.snapshotVersion}`,
    ],
    readiness: readinessView,
  };
}

export function adaptTrainingDatasetSummary(
  item: TrainingDatasetSummaryView,
): TrainingDatasetWorkbenchItem {
  const dataDomain =
    item.data_domain === "derivatives" ||
    item.data_domain === "on_chain" ||
    item.data_domain === "macro" ||
    item.data_domain === "sentiment_events"
      ? item.data_domain
      : "market";
  const datasetType =
    item.dataset_type === "feature_snapshot" ||
    item.dataset_type === "display_slice" ||
    item.dataset_type === "fusion_training_panel"
      ? item.dataset_type
      : "training_panel";
  const entityScope = item.entity_scope ?? "unknown";
  const entityScopeLabel =
    ENTITY_SCOPE_LABELS[(entityScope as DatasetEntityScope) ?? "unknown"] ?? entityScope;
  const universeSummary = formatUniverseSummary(item.universe_summary, entityScope);
  return {
    datasetId: item.dataset_id,
    title: compactText(item.display_name) ?? item.dataset_id,
    subtitle: [
      TYPE_LABELS[datasetType],
      DOMAIN_LABELS[dataDomain],
      universeSummary,
      normalizeSource(item.source_vendor),
      formatFrequency(item.frequency),
    ]
      .filter(Boolean)
      .join(" · "),
    technicalId: item.dataset_id,
    datasetType,
    datasetTypeLabel: TYPE_LABELS[datasetType],
    dataDomain: dataDomain as DatasetDomain,
    dataDomainLabel: DOMAIN_LABELS[dataDomain as DatasetDomain],
    sourceVendor: normalizeSource(item.source_vendor),
    entityScope,
    entityScopeLabel,
    universeSummary,
    sampleCountLabel: formatCount(item.sample_count),
    featureCountLabel: formatCount(item.feature_count),
    labelCountLabel: formatCount(item.label_count),
    labelHorizonLabel:
      item.label_horizon === null || item.label_horizon === undefined
        ? "不适用"
        : `${item.label_horizon} 个周期`,
    splitStrategyLabel: formatSplitStrategy(item.split_strategy),
    frequencyLabel: formatFrequency(item.frequency),
    freshnessLabel: formatFreshnessLabel(item.freshness_status),
    qualityLabel: formatQualityStatus(item.quality_status),
    readinessStatus: item.readiness_status ?? "unknown",
    readinessLabel: formatReadinessStatus(item.readiness_status),
    readinessReason: item.readiness_reason ?? "后端未返回额外说明。",
    snapshotVersion: formatVersion(item.snapshot_version),
  };
}

export function createFallbackTrainingDatasetItems(
  items: DatasetWorkbenchSummary[],
): TrainingDatasetWorkbenchItem[] {
  return items
    .filter((item) => item.datasetType === "training_panel" || item.datasetType === "fusion_training_panel")
    .map((item) => ({
      datasetId: item.datasetId,
      title: item.title,
      subtitle: item.subtitle,
      technicalId: item.technicalId,
      datasetType: item.datasetType,
      datasetTypeLabel: item.datasetTypeLabel,
      dataDomain: item.dataDomain,
      dataDomainLabel: item.dataDomainLabel,
      sourceVendor: item.sourceVendor,
      entityScope: item.entityScope,
      entityScopeLabel: item.entityScopeLabel,
      universeSummary:
        item.entityScope === "multi_asset"
          ? `${item.entityScopeLabel} · ${formatCount(item.entityCount, "范围待补充")}`
          : item.symbolLabel,
      sampleCountLabel: item.rowCountLabel,
      featureCountLabel: item.featureCountLabel,
      labelCountLabel: item.labelCountLabel,
      labelHorizonLabel: item.labelHorizonLabel,
      splitStrategyLabel: formatSplitStrategy(item.raw.split_strategy),
      frequencyLabel: item.frequencyLabel,
      freshnessLabel: item.freshnessLabel,
      qualityLabel: item.qualityLabel,
      readinessStatus: item.readinessStatus,
      readinessLabel: item.readinessLabel,
      readinessReason: "当前按现有数据集元数据做兼容判断，待后端训练摘要接口接管。",
      snapshotVersion: item.snapshotVersion,
    }));
}

export function createDatasetFacets(items: DatasetSummaryView[]): DatasetFacetSet {
  const adapted = items.map(adaptDatasetSummary);
  const uniq = (values: Array<string | null | undefined>) =>
    Array.from(new Set(values.filter((value): value is string => Boolean(value)))).sort((a, b) =>
      a.localeCompare(b, "zh-CN"),
    );

  return {
    domains: uniq(adapted.map((item) => item.dataDomain)),
    types: uniq(adapted.map((item) => item.datasetType)),
    sources: uniq(adapted.map((item) => item.sourceVendor)),
    exchanges: uniq(adapted.map((item) => item.exchange)),
    symbols: uniq(
      adapted.flatMap((item) => (item.symbolsPreview.length > 0 ? item.symbolsPreview : [item.symbol])),
    ),
    frequencies: uniq(adapted.map((item) => item.frequency)),
    versions: uniq(adapted.map((item) => item.snapshotVersion)),
  };
}

export function filterDatasetSummaries(
  items: DatasetWorkbenchSummary[],
  filters: DatasetBrowserFilters,
): DatasetWorkbenchSummary[] {
  return items.filter((item) => {
    const symbolPool = item.symbolsPreview.length > 0 ? item.symbolsPreview : [item.symbol].filter(Boolean);

    if (filters.data_domain && item.dataDomain !== filters.data_domain) {
      return false;
    }
    if (filters.dataset_type && item.datasetType !== filters.dataset_type) {
      return false;
    }
    if (filters.source && item.sourceVendor !== filters.source) {
      return false;
    }
    if (filters.exchange && item.exchange !== filters.exchange) {
      return false;
    }
    if (filters.symbol && !symbolPool.includes(filters.symbol)) {
      return false;
    }
    if (filters.frequency && item.frequency !== filters.frequency) {
      return false;
    }
    if (filters.version && item.snapshotVersion !== filters.version) {
      return false;
    }
    if (filters.time_from) {
      const end = item.raw.freshness.data_end_time ?? item.raw.as_of_time;
      if (end && new Date(end) < new Date(filters.time_from)) {
        return false;
      }
    }
    if (filters.time_to) {
      const start = item.raw.freshness.data_start_time;
      if (start && new Date(start) > new Date(filters.time_to)) {
        return false;
      }
    }
    return true;
  });
}

export function groupDatasetDomains(items: DatasetWorkbenchSummary[]) {
  return (Object.keys(DOMAIN_LABELS) as DatasetDomain[]).map((domain) => {
    const domainItems = items.filter((item) => item.dataDomain === domain);
    const trainingCount = domainItems.filter((item) => item.datasetType === "training_panel").length;
    const freshCount = domainItems.filter((item) => item.freshnessLabel === "新鲜").length;
    return {
      key: domain,
      label: DOMAIN_LABELS[domain],
      summary: DOMAIN_SUMMARIES[domain],
      total: domainItems.length,
      trainingCount,
      freshCount,
      items: domainItems,
    };
  });
}

export function groupTrainingDatasets(items: DatasetWorkbenchSummary[]) {
  return items.filter(
    (item) => item.datasetType === "training_panel" || item.datasetType === "fusion_training_panel",
  );
}

export function describeDatasetType(type: DatasetType): string {
  return TYPE_SUMMARIES[type];
}

export function isApiNotReadyError(error: unknown): boolean {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  return (
    message.includes("404") ||
    message.includes("405") ||
    message.includes("501") ||
    message.includes("not found") ||
    message.includes("未就绪") ||
    message.includes("not implemented")
  );
}

export function createApiNotReadyMessage(featureLabel: string): string {
  return `${featureLabel}接口暂未就绪，当前环境还没有提供这块真实能力。`;
}

export function filterDatasetRequestJobs(items: JobStatusView[]): JobStatusView[] {
  return items.filter((job) => {
    const type = (job.job_type ?? "").toLowerCase();
    const result = job.result;
    return (
      type.includes("dataset") ||
      type.includes("prepare") ||
      type.includes("acquisition") ||
      type.includes("ingestion") ||
      type.includes("build") ||
      (Boolean(result?.dataset_id) &&
        (result?.run_ids?.length ?? 0) === 0 &&
        (result?.backtest_ids?.length ?? 0) === 0)
    );
  });
}

export function datasetJobDetailPath(job: JobStatusView): string | null {
  const deeplinks = job.result?.deeplinks;
  if (deeplinks?.dataset_detail) {
    return deeplinks.dataset_detail;
  }
  if (job.result?.dataset_id) {
    return `/datasets/${encodeURIComponent(job.result.dataset_id)}`;
  }
  return null;
}

export function normalizeDatasetDetail(detail: DatasetDetailView | undefined): DatasetDetailView | null {
  if (!detail) {
    return null;
  }
  return normalizeDetailRecord(detail);
}
