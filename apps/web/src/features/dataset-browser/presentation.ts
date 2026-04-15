import type {
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

export type DatasetCardView = {
  datasetId: string;
  title: string;
  subtitle: string;
  domain: DatasetDomain;
  domainLabel: string;
  domainSummary: string;
  datasetType: DatasetType;
  datasetTypeLabel: string;
  sourceLabel: string;
  exchangeLabel: string;
  symbolLabel: string;
  frequencyLabel: string;
  coverageLabel: string;
  snapshotVersion: string;
  freshnessLabel: string;
  qualityLabel: string;
  readinessLabel: string;
  rowCountLabel: string;
  featureCountLabel: string;
  labelCountLabel: string;
  labelHorizonLabel: string;
  entityScopeLabel: string;
  entityScope: string;
  symbolsPreview: string[];
  asOfTime: string | null;
  raw: DatasetSummaryView;
};

export type DatasetDomainGroupView = {
  key: DatasetDomain;
  label: string;
  summary: string;
  total: number;
  trainingCount: number;
  freshCount: number;
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

export type TrainingDatasetCardView = {
  datasetId: string;
  title: string;
  subtitle: string;
  datasetTypeLabel: string;
  domainLabel: string;
  sourceLabel: string;
  universeSummary: string;
  sampleCountLabel: string;
  featureCountLabel: string;
  labelCountLabel: string;
  labelHorizonLabel: string;
  splitStrategyLabel: string;
  frequencyLabel: string;
  freshnessLabel: string;
  qualityLabel: string;
  readinessLabel: string;
  readinessStatus: string;
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

const DOMAIN_SUMMARIES: Record<DatasetDomain, string> = {
  market: "单资产价格、成交量与波动率序列，适合研究基础市场行为。",
  derivatives: "利率、资金费率、持仓量等衍生品指标，适合补充交易结构信息。",
  on_chain: "链上转账、余额、活跃度等原生指标，适合观察资产链上行为。",
  macro: "宏观因子与公开经济序列，适合做跨市场背景判断。",
  sentiment_events: "新闻、社媒与事件信号的结构化流，适合构建文本相关特征。",
};

const TYPE_LABELS: Record<DatasetType, string> = {
  display_slice: "展示切片",
  training_panel: "训练面板",
  feature_snapshot: "特征快照",
  fusion_training_panel: "融合训练面板",
};

const ENTITY_SCOPE_LABELS: Record<string, string> = {
  single_asset: "单资产",
  multi_asset: "多资产",
  macro_series: "宏观序列",
  event_stream: "事件流",
  unknown: "未标注",
};

function compactText(value: string | null | undefined) {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function normalizeSourceLabel(value: string | null | undefined) {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized) {
    return "内部数据";
  }
  if (normalized.includes("binance")) {
    return "Binance";
  }
  if (normalized.includes("okx")) {
    return "OKX";
  }
  if (normalized.includes("bybit")) {
    return "Bybit";
  }
  if (normalized.includes("fred")) {
    return "FRED";
  }
  if (normalized.includes("stooq")) {
    return "Stooq";
  }
  if (normalized.includes("defillama")) {
    return "DeFiLlama";
  }
  if (normalized.includes("news")) {
    return "News Archive";
  }
  if (normalized.includes("reddit")) {
    return "Reddit Archive";
  }
  return value ?? "内部数据";
}

function inferDomain(summary: DatasetSummaryView): DatasetDomain {
  const explicit = compactText(summary.data_domain)?.toLowerCase();
  if (
    explicit === "market" ||
    explicit === "derivatives" ||
    explicit === "on_chain" ||
    explicit === "macro" ||
    explicit === "sentiment_events"
  ) {
    return explicit;
  }

  const joined = [
    summary.dataset_id,
    summary.display_name,
    summary.subtitle,
    summary.dataset_category,
    summary.data_source,
    summary.source_vendor,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (joined.includes("macro") || joined.includes("fred") || joined.includes("stooq")) {
    return "macro";
  }
  if (joined.includes("chain") || joined.includes("on_chain")) {
    return "on_chain";
  }
  if (joined.includes("funding") || joined.includes("perp") || joined.includes("derivative")) {
    return "derivatives";
  }
  if (joined.includes("sentiment") || joined.includes("event") || joined.includes("news")) {
    return "sentiment_events";
  }
  return "market";
}

function inferDatasetType(summary: DatasetSummaryView): DatasetType {
  const explicit = compactText(summary.dataset_type ?? summary.dataset_category)?.toLowerCase();
  if (explicit === "display_slice" || explicit?.includes("display")) {
    return "display_slice";
  }
  if (explicit === "feature_snapshot" || explicit?.includes("snapshot") || explicit?.includes("feature")) {
    return "feature_snapshot";
  }
  if (explicit === "fusion_training_panel" || explicit?.includes("fusion")) {
    return "fusion_training_panel";
  }
  if (explicit === "training_panel" || explicit?.includes("training") || explicit?.includes("panel")) {
    return "training_panel";
  }
  if ((summary.label_count ?? 0) > 0 || Boolean(summary.split_strategy)) {
    return "training_panel";
  }
  return "display_slice";
}

function formatFrequency(value: string | null | undefined) {
  const raw = compactText(value);
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

function formatSnapshotVersion(value: string | null | undefined) {
  const normalized = compactText(value);
  if (!normalized) {
    return "当前版本";
  }
  return normalized.slice(0, 10);
}

function formatCoverage(summary: DatasetSummaryView) {
  if (compactText(summary.time_range_label)) {
    return summary.time_range_label as string;
  }
  const start = summary.freshness.data_start_time?.slice(0, 10);
  const end = summary.freshness.data_end_time?.slice(0, 10);
  if (start || end) {
    return `${start ?? "--"} - ${end ?? "--"}`;
  }
  return "覆盖范围待补充";
}

function formatQualityLabel(status: string | null | undefined, isSmoke: boolean) {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "healthy" || normalized === "ok" || normalized === "pass") {
    return "健康";
  }
  if (normalized === "warning" || normalized === "partial") {
    return "需留意";
  }
  if (normalized === "failed" || normalized === "error") {
    return "异常";
  }
  return isSmoke ? "演示样本" : "待确认";
}

function formatReadinessLabel(status: string | null | undefined) {
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

function formatEntityScopeLabel(value: string | null | undefined) {
  return ENTITY_SCOPE_LABELS[value ?? "unknown"] ?? "未标注";
}

function formatCount(value: number | null | undefined, fallback = "--") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return fallback;
  }
  return value.toLocaleString("zh-CN");
}

function formatLabelHorizon(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "不适用";
  }
  return `${value} 个 bar`;
}

function summarizeSymbols(summary: DatasetSummaryView) {
  const preview = Array.isArray(summary.symbols_preview) ? summary.symbols_preview.filter(Boolean) : [];
  if (preview.length > 0) {
    return preview;
  }
  if (summary.asset_id) {
    return [summary.asset_id];
  }
  return [];
}

function buildTitle(summary: DatasetSummaryView, domain: DatasetDomain, type: DatasetType, sourceLabel: string, frequencyLabel: string) {
  if (compactText(summary.display_name)) {
    return summary.display_name as string;
  }
  const assetPart = summary.asset_id ? ` ${summary.asset_id}` : "";
  const frequencyPart = summary.frequency ? ` ${frequencyLabel}` : "";
  return `${sourceLabel}${assetPart}${frequencyPart} ${TYPE_LABELS[type]} / ${DOMAIN_LABELS[domain]}`;
}

function buildSubtitle(summary: DatasetSummaryView, domain: DatasetDomain, type: DatasetType, frequencyLabel: string, exchangeLabel: string) {
  if (compactText(summary.subtitle)) {
    return summary.subtitle as string;
  }
  return [DOMAIN_LABELS[domain], TYPE_LABELS[type], frequencyLabel, exchangeLabel].filter(Boolean).join(" · ");
}

function buildUniverseSummary(summary: DatasetSummaryView) {
  const symbols = summarizeSymbols(summary);
  if (symbols.length > 0) {
    return symbols.slice(0, 3).join(" / ");
  }
  if (summary.entity_count) {
    return `${summary.entity_count} 个研究对象`;
  }
  return "范围待补充";
}

export function buildDatasetCard(summary: DatasetSummaryView): DatasetCardView {
  const domain = inferDomain(summary);
  const datasetType = inferDatasetType(summary);
  const sourceLabel = normalizeSourceLabel(summary.source_vendor ?? summary.data_source);
  const exchangeLabel = compactText(summary.exchange) ?? (summary.asset_id ? "交易场景" : "非交易所");
  const frequencyLabel = formatFrequency(summary.frequency);
  return {
    datasetId: summary.dataset_id,
    title: buildTitle(summary, domain, datasetType, sourceLabel, frequencyLabel),
    subtitle: buildSubtitle(summary, domain, datasetType, frequencyLabel, exchangeLabel),
    domain,
    domainLabel: DOMAIN_LABELS[domain],
    domainSummary: DOMAIN_SUMMARIES[domain],
    datasetType,
    datasetTypeLabel: TYPE_LABELS[datasetType],
    sourceLabel,
    exchangeLabel,
    symbolLabel: summary.asset_id ?? "多资产 / 非 symbol 数据",
    frequencyLabel,
    coverageLabel: formatCoverage(summary),
    snapshotVersion: formatSnapshotVersion(summary.snapshot_version ?? summary.as_of_time),
    freshnessLabel: formatFreshnessLabel(summary.freshness.status),
    qualityLabel: formatQualityLabel(summary.quality_status, summary.is_smoke),
    readinessLabel: formatReadinessLabel(summary.readiness_status),
    rowCountLabel: formatCount(summary.row_count ?? summary.sample_count),
    featureCountLabel: formatCount(summary.feature_count),
    labelCountLabel: formatCount(summary.label_count),
    labelHorizonLabel: formatLabelHorizon(summary.label_horizon),
    entityScopeLabel: formatEntityScopeLabel(summary.entity_scope),
    entityScope: summary.entity_scope ?? "unknown",
    symbolsPreview: summarizeSymbols(summary),
    asOfTime: summary.as_of_time,
    raw: summary,
  };
}

export function groupDatasetDomains(items: DatasetCardView[]): DatasetDomainGroupView[] {
  return (Object.keys(DOMAIN_LABELS) as DatasetDomain[]).map((key) => {
    const rows = items.filter((item) => item.domain === key);
    return {
      key,
      label: DOMAIN_LABELS[key],
      summary: DOMAIN_SUMMARIES[key],
      total: rows.length,
      trainingCount: rows.filter((item) => item.datasetType === "training_panel" || item.datasetType === "fusion_training_panel").length,
      freshCount: rows.filter((item) => item.freshnessLabel === "新鲜").length,
    };
  });
}

export function createDatasetFacets(items: DatasetCardView[]): DatasetFacetSet {
  const uniq = (values: Array<string | null | undefined>) =>
    Array.from(new Set(values.filter((value): value is string => Boolean(value)))).sort((a, b) => a.localeCompare(b, "zh-CN"));

  return {
    domains: uniq(items.map((item) => item.domain)),
    types: uniq(items.map((item) => item.datasetType)),
    sources: uniq(items.map((item) => item.sourceLabel)),
    exchanges: uniq(items.map((item) => item.exchangeLabel)),
    symbols: uniq(items.flatMap((item) => item.symbolsPreview.length > 0 ? item.symbolsPreview : [item.raw.asset_id])),
    frequencies: uniq(items.map((item) => item.raw.frequency)),
    versions: uniq(items.map((item) => item.snapshotVersion)),
  };
}

export function filterDatasetCards(items: DatasetCardView[], filters: DatasetBrowserFilters) {
  return items.filter((item) => {
    const symbolPool = item.symbolsPreview.length > 0 ? item.symbolsPreview : [item.raw.asset_id].filter(Boolean) as string[];
    if (filters.data_domain && item.domain !== filters.data_domain) return false;
    if (filters.dataset_type && item.datasetType !== filters.dataset_type) return false;
    if (filters.source && item.sourceLabel !== filters.source) return false;
    if (filters.exchange && item.exchangeLabel !== filters.exchange) return false;
    if (filters.symbol && !symbolPool.includes(filters.symbol)) return false;
    if (filters.frequency && item.raw.frequency !== filters.frequency) return false;
    if (filters.version && item.snapshotVersion !== filters.version) return false;
    if (filters.time_from && item.raw.freshness.data_start_time && item.raw.freshness.data_start_time < filters.time_from) return false;
    if (filters.time_to && item.raw.freshness.data_end_time && item.raw.freshness.data_end_time > filters.time_to) return false;
    return true;
  });
}

export function describeDatasetType(type: DatasetType) {
  const descriptions: Record<DatasetType, string> = {
    display_slice: "适合先看图、看覆盖和看样本，不建议直接当训练面板使用。",
    training_panel: "可直接进入训练、验证和回测前的数据检查。",
    feature_snapshot: "适合作为上游信号资产复用，供后续融合或下游模型消费。",
    fusion_training_panel: "适合跨域训练与多信号联合研究。",
  };
  return descriptions[type];
}

export function buildTrainingCardsFromDatasets(items: DatasetCardView[]): TrainingDatasetCardView[] {
  return items
    .filter((item) => item.datasetType === "training_panel" || item.datasetType === "fusion_training_panel")
    .map((item) => ({
      datasetId: item.datasetId,
      title: item.title,
      subtitle: item.subtitle,
      datasetTypeLabel: item.datasetTypeLabel,
      domainLabel: item.domainLabel,
      sourceLabel: item.sourceLabel,
      universeSummary: buildUniverseSummary(item.raw),
      sampleCountLabel: item.rowCountLabel,
      featureCountLabel: item.featureCountLabel,
      labelCountLabel: item.labelCountLabel,
      labelHorizonLabel: item.labelHorizonLabel,
      splitStrategyLabel: compactText(item.raw.split_strategy) ?? "待补充",
      frequencyLabel: item.frequencyLabel,
      freshnessLabel: item.freshnessLabel,
      qualityLabel: item.qualityLabel,
      readinessLabel: item.readinessLabel,
      readinessStatus: item.raw.readiness_status ?? "unknown",
      readinessReason: item.readinessLabel === "可训练" ? "基础元数据满足训练前检查的最低要求。" : "建议先补齐标签、切分或质量说明。",
      snapshotVersion: item.snapshotVersion,
    }));
}

export function buildTrainingCardsFromApi(items: TrainingDatasetSummaryView[]): TrainingDatasetCardView[] {
  return items.map((item) => ({
    datasetId: item.dataset_id,
    title: compactText(item.display_name) ?? item.dataset_id,
    subtitle: [item.dataset_type, item.data_domain, item.source_vendor, item.frequency].filter(Boolean).join(" · "),
    datasetTypeLabel: TYPE_LABELS[(item.dataset_type as DatasetType) ?? "training_panel"] ?? "训练面板",
    domainLabel: DOMAIN_LABELS[(item.data_domain as DatasetDomain) ?? "market"] ?? "市场数据",
    sourceLabel: normalizeSourceLabel(item.source_vendor),
    universeSummary: typeof item.universe_summary === "string" ? item.universe_summary : "范围待补充",
    sampleCountLabel: formatCount(item.sample_count),
    featureCountLabel: formatCount(item.feature_count),
    labelCountLabel: formatCount(item.label_count),
    labelHorizonLabel: item.label_horizon === null || item.label_horizon === undefined ? "不适用" : `${item.label_horizon}`,
    splitStrategyLabel: compactText(item.split_strategy) ?? "待补充",
    frequencyLabel: formatFrequency(item.frequency),
    freshnessLabel: formatFreshnessLabel(item.freshness_status),
    qualityLabel: formatQualityLabel(item.quality_status, false),
    readinessLabel: formatReadinessLabel(item.readiness_status),
    readinessStatus: item.readiness_status,
    readinessReason: item.readiness_reason ?? "后端未返回额外说明。",
    snapshotVersion: formatSnapshotVersion(item.snapshot_version),
  }));
}

export function filterDatasetRequestJobs(jobs: JobStatusView[]) {
  return jobs.filter((job) => job.job_type.includes("dataset") || job.job_type.includes("prepare") || job.job_type.includes("build"));
}

export function datasetJobDetailPath(job: JobStatusView) {
  return job.result?.deeplinks?.dataset_detail ?? null;
}
