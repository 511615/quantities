import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import type { EChartsOption } from "echarts";

import { DatasetCandlestickChart } from "../features/dataset-browser/DatasetCandlestickChart";
import type { CandlePoint } from "../features/dataset-browser/DatasetCandlestickChart";
import { DatasetDeleteDialog } from "../features/dataset-browser/DatasetDeleteDialog";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import {
  adaptDatasetDetail,
  normalizeDatasetDetail,
} from "../features/dataset-browser/workbench";
import {
  useDatasetDependencies,
  useDatasetDetail,
  useDatasetFeatureSeries,
  useDatasetNlpInspection,
  useDatasetOhlcvAll,
  useDatasetReadiness,
} from "../shared/api/hooks";
import type { DatasetDependencyView, DatasetFeatureSeriesView } from "../shared/api/types";
import { api } from "../shared/api/client";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import { translateText } from "../shared/lib/i18n";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { ModalityQualitySummary } from "../shared/ui/ModalityQualitySummary";
import { StatusPill } from "../shared/ui/StatusPill";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import { LaunchDatasetMultimodalTrainDrawer } from "../features/launch-training/LaunchDatasetMultimodalTrainDrawer";

const NLP_FEATURE_REGEX = /^(sentiment|text|news)_/i;
const FEATURE_DOMAIN_LABELS: Record<string, string> = {
  macro: "宏观数据",
  on_chain: "链上数据",
  derivatives: "衍生品",
  sentiment_events: "NLP / 情绪",
  other: "其他信号",
};

function dependencyKindLabel(kind: string) {
  const normalized = kind.trim().toLowerCase();
  if (normalized === "run") {
    return "训练实例";
  }
  if (normalized === "backtest") {
    return "回测";
  }
  if (normalized === "dataset") {
    return "派生数据集";
  }
  if (normalized === "data_asset") {
    return "上游资产";
  }
  return kind || "依赖项";
}

function dependencyDirectionLabel(direction?: string) {
  if (direction === "depends_on") {
    return "上游";
  }
  if (direction === "referenced_by") {
    return "下游";
  }
  return "关联";
}

function dependencyText(item: DatasetDependencyView) {
  return item.dependency_label || item.dependency_id;
}

function toCandles(
  items: Array<{ event_time: string; open: number; high: number; low: number; close: number; volume: number }>,
): CandlePoint[] {
  return items.map((item) => ({
    time: formatDate(item.event_time),
    open: item.open,
    high: item.high,
    low: item.low,
    close: item.close,
    volume: item.volume,
  }));
}

function trailingAverage(
  items: Array<{ close: number }>,
  windowSize: number,
) {
  if (items.length < windowSize) {
    return null;
  }
  const slice = items.slice(items.length - windowSize);
  const mean = slice.reduce((sum, item) => sum + item.close, 0) / slice.length;
  return Number(mean.toFixed(2));
}

function formatWindow(startTime?: string | null, endTime?: string | null) {
  if (startTime && endTime) {
    return `${formatDate(startTime)} - ${formatDate(endTime)}`;
  }
  if (startTime) {
    return `${formatDate(startTime)} - --`;
  }
  if (endTime) {
    return `-- - ${formatDate(endTime)}`;
  }
  return "--";
}

function gateStatusLabel(status?: string | null) {
  if (status === "passed") {
    return "通过";
  }
  if (status === "failed") {
    return "未通过";
  }
  if (status === "warning") {
    return "需复核";
  }
  return "--";
}

function booleanLabel(value?: boolean | null) {
  if (value === null || value === undefined) {
    return "--";
  }
  return value ? "是" : "否";
}

function resolveDatasetDownloadHref(datasetId: string, links: Array<{ kind?: string; label?: string; href?: string; api_path?: string | null }> = []) {
  const explicitLink = links.find((link) => {
    const marker = `${link.kind ?? ""} ${link.label ?? ""}`.toLowerCase();
    return marker.includes("download") || marker.includes("export") || marker.includes("获取");
  });
  return explicitLink?.href ?? explicitLink?.api_path ?? api.datasetDownloadUrl(datasetId);
}

function safeExternalUrl(value?: string | null) {
  if (!value) {
    return null;
  }
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:" ? url.toString() : null;
  } catch {
    return null;
  }
}

function normalizeSeriesValues(values: Array<number | null>) {
  const numeric = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (numeric.length === 0) {
    return values.map(() => null);
  }
  const min = Math.min(...numeric);
  const max = Math.max(...numeric);
  const span = max - min;
  if (span === 0) {
    return values.map((value) => (typeof value === "number" && Number.isFinite(value) ? 0 : null));
  }
  return values.map((value) =>
    typeof value === "number" && Number.isFinite(value)
      ? Number((((value - min) / span) * 2 - 1).toFixed(4))
      : null,
  );
}

function featureSeriesChartOption(seriesItems: DatasetFeatureSeriesView[]): EChartsOption {
  const firstSeries = seriesItems[0];
  const xAxisData = (firstSeries?.points ?? []).map((point) => formatDate(point.timestamp));
  const rawValuesBySeries = Object.fromEntries(
    seriesItems.map((series) => [
      series.label,
      series.points.map((point) => point.value),
    ]),
  );
  return {
    tooltip: {
      trigger: "axis",
      formatter: (params: unknown) => {
        const items = Array.isArray(params) ? params : [params];
        const axisLabel = String((items[0] as { axisValueLabel?: string } | undefined)?.axisValueLabel ?? "");
        const rows = items
          .map((item) => {
            const entry = item as { seriesName?: string; dataIndex?: number; marker?: string };
            const seriesName = entry.seriesName ?? "";
            const rawValue = rawValuesBySeries[seriesName]?.[entry.dataIndex ?? -1];
            return `${entry.marker ?? ""}${seriesName}: ${formatNumber(rawValue, 6)}`;
          })
          .join("<br/>");
        return `${axisLabel}<br/>${rows}`;
      },
    },
    legend: {
      data: seriesItems.map((series) => series.label),
      textStyle: { color: "#6f6a60" },
    },
    grid: { left: 56, right: 28, top: 48, bottom: 72 },
    xAxis: {
      type: "category",
      data: xAxisData,
      axisLabel: { color: "#6f6a60" },
      axisLine: { lineStyle: { color: "rgba(55, 53, 46, 0.22)" } },
    },
    yAxis: {
      type: "value",
      scale: true,
      name: "标准化走势",
      axisLabel: { color: "#6f6a60" },
      splitLine: { lineStyle: { color: "rgba(55, 53, 46, 0.1)" } },
    },
    dataZoom: [
      { type: "inside", start: 70, end: 100 },
      { type: "slider", start: 70, end: 100, height: 24, bottom: 24 },
    ],
    series: seriesItems.map((series) => ({
      name: series.label,
      type: "line",
      showSymbol: false,
      smooth: false,
      data: normalizeSeriesValues(series.points.map((point) => point.value)),
    })),
  };
}

function normalizedDatasetModalities(dataDomains: string[] = []) {
  return Array.from(
    new Set(
      dataDomains.map((domain) => {
        const normalized = domain.trim().toLowerCase();
        if (normalized === "sentiment_events") {
          return "nlp";
        }
        return normalized;
      }),
    ),
  ).filter(Boolean);
}

export function DatasetDetailPage() {
  const navigate = useNavigate();
  const { datasetId = "" } = useParams();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [isDeleted, setIsDeleted] = useState(false);
  const activeDatasetId = isDeleted ? null : datasetId;

  const detailQuery = useDatasetDetail(activeDatasetId);
  const readinessQuery = useDatasetReadiness(activeDatasetId);
  const dependenciesQuery = useDatasetDependencies(activeDatasetId);

  const normalized = normalizeDatasetDetail(detailQuery.data);
  const detail = normalized ? adaptDatasetDetail(normalized, readinessQuery.data ?? null) : null;

  const datasetRoleHighlights = [
    {
      title: "原始事件库",
      body: "上游事件表会保留每条 NLP 记录的 event_time、available_time、source 和元数据，便于审计与时序安全核查。",
    },
    {
      title: "训练面板",
      body: "特征已经与标签和切分方式对齐，因此训练流程可以直接基于 dataset_id 运行，而不用回到原始文本。",
    },
    {
      title: "特征快照",
      body: "聚合后的情绪与关键词指标已经打包完成，方便融合模型或下游模型稳定消费这些信号资产。",
    },
  ];

  const hasNlpSignal = useMemo(() => {
    if (!detail) {
      return false;
    }
    const domains = Array.isArray(detail.summary.raw.data_domains) ? detail.summary.raw.data_domains : [];
    return (
      detail.summary.dataDomain === "sentiment_events" ||
      domains.includes("sentiment_events") ||
      detail.featureColumnsPreview.some((column) => NLP_FEATURE_REGEX.test(column))
    );
  }, [detail]);

  const nlpInspectionQuery = useDatasetNlpInspection(activeDatasetId, hasNlpSignal);

  const datasetLinks = [
    ...(detailQuery.data?.links ?? []),
    ...(detailQuery.data?.dataset.links ?? []),
  ];
  const detailDataDomains = Array.isArray(detail?.summary.raw.data_domains)
    ? detail.summary.raw.data_domains
    : [];
  const hasNonMarketSignals = detailDataDomains.some(
    (domain) => domain !== "market" && domain !== "unknown",
  );
  const hasMarketDomain =
    detail?.summary.dataDomain === "market" || detailDataDomains.includes("market");
  const hasOhlcvLink = datasetLinks.some((link) => {
    const marker = `${link.kind ?? ""} ${link.label ?? ""} ${link.href ?? ""} ${link.api_path ?? ""}`.toLowerCase();
    return marker.includes("ohlcv") || marker.includes("kline") || marker.includes("candlestick");
  });
  const canRenderMarketSlice = Boolean(
    activeDatasetId &&
      detail &&
      hasMarketDomain &&
      (hasOhlcvLink || Boolean(detail.summary.symbol)),
  );
  const barsQuery = useDatasetOhlcvAll(canRenderMarketSlice ? activeDatasetId : null, {
    per_page: 5000,
    start_time: detail?.summary.raw.freshness.data_start_time ?? null,
    end_time: detail?.summary.raw.freshness.data_end_time ?? null,
  });
  const bars = barsQuery.data?.items ?? [];
  const candles = useMemo(() => toCandles(bars), [bars]);
  const latestMa5 = useMemo(() => trailingAverage(bars, 5), [bars]);
  const latestMa10 = useMemo(() => trailingAverage(bars, 10), [bars]);
  const featureSeriesQuery = useDatasetFeatureSeries(
    activeDatasetId,
    { max_points: 900 },
    Boolean(activeDatasetId && detail && hasNonMarketSignals),
  );
  const featureSeriesByDomain = useMemo(() => {
    const grouped: Record<string, DatasetFeatureSeriesView[]> = {};
    for (const item of featureSeriesQuery.data?.items ?? []) {
      if (item.data_domain === "market") {
        continue;
      }
      grouped[item.data_domain] = [...(grouped[item.data_domain] ?? []), item];
    }
    return grouped;
  }, [featureSeriesQuery.data?.items]);
  const visibleFeatureDomains = ["macro", "on_chain", "derivatives", "sentiment_events", "other"].filter(
    (domain) => (featureSeriesByDomain[domain] ?? []).length > 0,
  );
  const marketPreviewSymbol =
    barsQuery.data?.symbol ??
    bars.find((row) => row.symbol)?.symbol ??
    detail?.summary.symbol ??
    detail?.summary.symbolsPreview[0] ??
    detail?.summary.symbolLabel ??
    "--";

  const nlpTimelineOption = useMemo(() => {
    const timeline = nlpInspectionQuery.data?.event_timeline ?? [];
    if (timeline.length === 0) {
      return null;
    }
    return {
      tooltip: { trigger: "axis" as const },
        legend: {
          data: ["事件数", "平均情绪"],
          textStyle: { color: "#b8b09e" },
        },
        xAxis: {
          type: "category" as const,
          data: timeline.map((point) => point.label),
          axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
          axisLabel: { color: "#b8b09e" },
        },
        yAxis: [
          {
            type: "value" as const,
            name: "事件数",
            axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
            axisLabel: { color: "#b8b09e" },
            splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
          },
          {
            type: "value" as const,
            name: "情绪值",
            min: -1,
            max: 1,
            axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
            axisLabel: { color: "#b8b09e" },
          splitLine: { show: false },
        },
      ],
        series: [
          {
            name: "事件数",
            type: "line" as const,
            smooth: true,
            data: timeline.map((point) => point.event_count),
            lineStyle: { width: 2, color: "#c7ff73" },
          },
          {
            name: "平均情绪",
            type: "line" as const,
            yAxisIndex: 1,
            smooth: true,
            data: timeline.map((point) => point.avg_sentiment ?? 0),
            lineStyle: { width: 2, type: "dashed", color: "#7fe3ff" },
        },
      ],
    } as EChartsOption;
  }, [nlpInspectionQuery.data?.event_timeline]);

  if (detailQuery.isLoading) {
    return <LoadingState label="正在加载数据集详情..." />;
  }

  if (detailQuery.isError) {
    return <ErrorState message={(detailQuery.error as Error).message} />;
  }

  if (!detail) {
    return <EmptyState title="未找到数据集" body="请求的数据集详情未能成功加载。" />;
  }

  const dependencyItems = dependenciesQuery.data?.items ?? [];
  const blockingDependencies = dependenciesQuery.data?.blocking_items ?? [];
  const nlpInspection = nlpInspectionQuery.data;
  const downloadHref = resolveDatasetDownloadHref(datasetId, datasetLinks);
  const requestedRangeLabel =
    nlpInspection?.requested_start_time || readinessQuery.data?.nlp_requested_start_time
      ? formatWindow(
          nlpInspection?.requested_start_time ?? readinessQuery.data?.nlp_requested_start_time,
          nlpInspection?.requested_end_time ?? readinessQuery.data?.nlp_requested_end_time,
        )
      : "--";
  const actualRangeLabel =
    nlpInspection?.actual_start_time || readinessQuery.data?.nlp_actual_start_time
      ? formatWindow(
          nlpInspection?.actual_start_time ?? readinessQuery.data?.nlp_actual_start_time,
          nlpInspection?.actual_end_time ?? readinessQuery.data?.nlp_actual_end_time,
        )
      : "--";
  const sourceVendorLabel =
    nlpInspection?.source_vendors && nlpInspection.source_vendors.length > 0
      ? nlpInspection.source_vendors.join(", ")
      : detail.summary.sourceVendor || "--";
  const officialGateStatus =
    nlpInspection?.official_template_gate_status ?? readinessQuery.data?.official_nlp_gate_status;
  const officialGateReasons =
    nlpInspection?.official_template_gate_reasons?.length
      ? nlpInspection.official_template_gate_reasons
      : (readinessQuery.data?.official_nlp_gate_reasons ?? []);
  const marketWindowLabel = formatWindow(
    nlpInspection?.market_window_start_time ?? readinessQuery.data?.market_window_start_time,
    nlpInspection?.market_window_end_time ?? readinessQuery.data?.market_window_end_time,
  );
  const officialTestWindowLabel = formatWindow(
    nlpInspection?.official_backtest_start_time ?? readinessQuery.data?.official_backtest_start_time,
    nlpInspection?.official_backtest_end_time ?? readinessQuery.data?.official_backtest_end_time,
  );
  const officialEligible =
    nlpInspection?.official_template_eligible ?? readinessQuery.data?.official_template_eligible;
  const archivalSourceOnly =
    nlpInspection?.archival_source_only ?? readinessQuery.data?.archival_nlp_source_only;
  const coverageRatio =
    nlpInspection?.coverage_ratio ?? readinessQuery.data?.nlp_coverage_ratio ?? null;
  const testCoverageRatio =
    nlpInspection?.test_coverage_ratio ?? readinessQuery.data?.nlp_test_coverage_ratio ?? null;
  const maxEmptyBars =
    nlpInspection?.max_consecutive_empty_bars ??
    readinessQuery.data?.nlp_max_consecutive_empty_bars ??
    null;
  const duplicateRatio =
    nlpInspection?.duplicate_ratio ?? readinessQuery.data?.nlp_duplicate_ratio ?? null;
  const entityLinkCoverageRatio =
    nlpInspection?.entity_link_coverage_ratio ??
    readinessQuery.data?.nlp_entity_link_coverage_ratio ??
    null;
  const datasetModalities = normalizedDatasetModalities(detail.summary.raw.data_domains);
  const insufficientObservationModalities = ["macro", "on_chain"].filter((modality) => {
    const item = readinessQuery.data?.modality_quality_summary?.[modality];
    return (
      item?.status === "failed" &&
      (item.freshness_lag_days ?? null) === 0 &&
      item.blocking_reasons.some((reason) => reason.includes("below 300"))
    );
  });

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">{translateText("数据集详情")}</div>
          <h1>{detail.summary.title}</h1>
          <p>{detail.heroSummary}</p>
        </div>
        <div className="hero-actions">
          {detail.readiness.rawStatus !== "not_ready" ? (
            <>
              <LaunchTrainDrawer
                datasetId={datasetId}
                datasetLabel={detail.summary.title}
                triggerLabel={translateText("基于此数据集训练")}
                title={translateText("发起训练")}
                description={translateText("训练流程保持以数据集 ID 为主入口，并会直接使用当前数据集。")}
              />
              {datasetModalities.length >= 2 ? (
                <LaunchDatasetMultimodalTrainDrawer
                  datasetId={datasetId}
                  datasetLabel={detail.summary.title}
                  datasetModalities={datasetModalities as Array<"market" | "macro" | "on_chain" | "derivatives" | "nlp">}
                />
              ) : null}
            </>
          ) : null}
          <a className="link-button" href={downloadHref}>
            {translateText("获取该数据集")}
          </a>
          <button className="link-button danger-link" onClick={() => setDeleteOpen(true)} type="button">
            {translateText("删除数据集")}
          </button>
          <Link className="comparison-link" to="/datasets/browser">
            {translateText("返回浏览器")}
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            {translateText("训练数据集")}
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav detailLabel="详情" />

      <div className="metric-grid">
        <div className="metric-tile">
          <span>{translateText("数据域")}</span>
          <strong>{detail.summary.dataDomainLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>{translateText("数据集类型")}</span>
          <strong>{detail.summary.datasetTypeLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>{translateText("覆盖范围")}</span>
          <strong>{detail.summary.coverageLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>{translateText("快照版本")}</span>
          <strong>{detail.summary.snapshotVersion}</strong>
        </div>
      </div>

      <section className="panel dataset-roles-panel">
        <PanelHeader
          eyebrow={translateText("数据故事")}
          title={translateText("信号链路")}
          description={translateText("原始 NLP 事件如何转成带标签的训练面板和结构化特征快照，这里会解释当前数据集在整条链路中的位置。")}
        />
        <div className="dataset-roles-grid">
          {datasetRoleHighlights.map((item) => (
            <article className="details-panel dataset-role-card" key={item.title}>
              <strong>{item.title}</strong>
              <p>{item.body}</p>
            </article>
          ))}
        </div>
        <div className="dataset-callout">
          <strong>{translateText("结构化 NLP 信号资产")}</strong>
          <span>
            {translateText("该数据集暴露的是由文本聚合而来的情绪、关键词和关注度指标。训练流程只消费数值特征，不直接消费原始文本，因此既保留可追溯性，也兼容 market-first 的训练 / 回测流程。")}
          </span>
        </div>
      </section>

      {hasNlpSignal ? (
        <section className="panel">
          <PanelHeader
            eyebrow={translateText("NLP 检查")}
            title={translateText("文本与事件巡检")}
            description={translateText("预览文本事件、关键词集中度、来源构成，以及可直接用于训练的情绪特征。")}
          />
          {nlpInspectionQuery.isLoading ? <LoadingState label={translateText("正在加载 NLP 巡检...")} /> : null}
          {nlpInspectionQuery.isError ? <ErrorState message={(nlpInspectionQuery.error as Error).message} /> : null}
          {!nlpInspectionQuery.isLoading && !nlpInspectionQuery.isError ? (
            nlpInspection?.contains_nlp ? (
              <div className="dataset-nlp-layout">
                <section className="details-panel nlp-summary-panel">
                  <PanelHeader
                    eyebrow={translateText("官方门禁")}
                    title={translateText("NLP 质量门禁")}
                    description={translateText("这是官方 / 系统模板专用门禁，要求比通用训练 readiness 更严格。")}
                  />
                  <div className="nlp-stat-grid">
                    <div className="metric-tile compact">
                      <span>{translateText("门禁状态")}</span>
                      <strong>{gateStatusLabel(officialGateStatus)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("官方模板可用")}</span>
                      <strong>{booleanLabel(officialEligible)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("仅归档型 NLP 数据源")}</span>
                      <strong>{booleanLabel(archivalSourceOnly)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("覆盖率")}</span>
                      <strong>{formatPercent(coverageRatio, 1)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("官方测试覆盖率")}</span>
                      <strong>{formatPercent(testCoverageRatio, 1)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("最大连续空档条数")}</span>
                      <strong>{maxEmptyBars ?? "--"}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("重复率")}</span>
                      <strong>{formatPercent(duplicateRatio, 1)}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("实体关联覆盖率")}</span>
                      <strong>{formatPercent(entityLinkCoverageRatio, 1)}</strong>
                    </div>
                  </div>
                  <div className="stack-list">
                    <div className="stack-item align-start">
                      <strong>{translateText("实际市场窗口")}</strong>
                      <span>{marketWindowLabel}</span>
                    </div>
                    <div className="stack-item align-start">
                      <strong>{translateText("官方测试窗口")}</strong>
                      <span>{officialTestWindowLabel}</span>
                    </div>
                    <div className="stack-item align-start">
                      <strong>{translateText("实际文本信号窗口")}</strong>
                      <span>{actualRangeLabel}</span>
                    </div>
                    <div className="stack-item align-start">
                      <strong>{translateText("请求的文本信号窗口")}</strong>
                      <span>{requestedRangeLabel}</span>
                    </div>
                  </div>
                  <div className="dataset-callout">
                    <strong>{translateText("官方模板规则")}</strong>
                    <span>{translateText("时间窗对齐必须和 market 模板窗口一致。")}</span>
                    <span>{translateText("只有归档型 NLP 数据源才允许参加官方对比。")}</span>
                    <span>{translateText("NLP 门禁一旦失败，官方回测发起会被硬阻断。")}</span>
                  </div>
                  {officialGateReasons.length > 0 ? (
                    <div className="stack-list">
                      {officialGateReasons.map((reason) => (
                        <div className="stack-item align-start" key={reason}>
                          <strong>{translateText("门禁说明")}</strong>
                          <span>{reason}</span>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </section>

                <section className="details-panel nlp-summary-panel">
                  <PanelHeader eyebrow={translateText("覆盖情况")} title={translateText("覆盖范围与来源")} />
                  <div className="nlp-stat-grid">
                    <div className="metric-tile compact">
                      <span>{translateText("请求窗口")}</span>
                      <strong>{requestedRangeLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("实际 NLP 覆盖")}</span>
                      <strong>{actualRangeLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("来源供应商")}</span>
                      <strong>{sourceVendorLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>{translateText("预览事件数")}</span>
                      <strong>{nlpInspection.recent_event_previews?.length ?? 0}</strong>
                    </div>
                  </div>
                  <div className="dataset-callout">
                    <strong>{nlpInspection.coverage_summary ?? translateText("NLP 巡检结果已就绪。")}</strong>
                    <span>
                      {translateText("这里展示的是从事件元数据派生出的结构化 NLP 信号，比如情绪、计数和关注度，而不是原始文本本身。")}
                    </span>
                  </div>
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow={translateText("时间线")} title={translateText("事件时间线")} />
                  {nlpTimelineOption ? (
                    <WorkbenchChart option={nlpTimelineOption} />
                  ) : (
                    <EmptyState title={translateText("暂无时间线")} body={translateText("当前数据集没有可展示的事件时间线数据。")} />
                  )}
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow={translateText("预览")} title={translateText("近期文本样本")} />
                  {nlpInspection.recent_event_previews && nlpInspection.recent_event_previews.length > 0 ? (
                    <div className="nlp-preview-list">
                      {nlpInspection.recent_event_previews.slice(0, 4).map((preview, index) => {
                        const sourceUrl = safeExternalUrl(preview.url);
                        return (
                        <div className="nlp-preview-row" key={`${preview.event_id}-${index}`}>
                          <div>
                            <strong>{preview.title}</strong>
                            <span>
                              {(preview.symbol || translateText("混合"))} · {preview.source}
                            </span>
                            <span>{formatDate(preview.event_time)}</span>
                          </div>
                          <p>{preview.snippet}</p>
                          {sourceUrl ? (
                            <a
                              href={sourceUrl}
                              rel="noopener noreferrer"
                              target="_blank"
                            >
                              {translateText("打开来源")}
                            </a>
                          ) : (
                            <span className="muted">{translateText("归档样本无外部链接")}</span>
                          )}
                        </div>
                        );
                      })}
                    </div>
                  ) : (
                    <EmptyState title={translateText("暂无文本预览")} body={translateText("当前数据集没有可展示的事件预览。")} />
                  )}
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow={translateText("来源")} title={translateText("来源构成")} />
                  <div className="nlp-source-breakdown">
                    {(nlpInspection.source_breakdown ?? []).length > 0 ? (
                      nlpInspection.source_breakdown?.slice(0, 8).map((source, index) => (
                        <div className="nlp-source-row" key={`${source.source}-${index}`}>
                          <strong>{source.source}</strong>
                          <span>
                            {source.count} 条 · {formatPercent(source.share ?? 0, 1)}
                          </span>
                        </div>
                      ))
                    ) : (
                      <span className="muted">{translateText("暂无来源构成。")}</span>
                    )}
                  </div>
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow={translateText("关键词")} title={translateText("关键词与高频词")} />
                  {nlpInspection.keyword_summary && nlpInspection.keyword_summary.length > 0 ? (
                    <div className="nlp-keyword-cloud">
                      {nlpInspection.keyword_summary.slice(0, 12).map((keyword, index) => (
                        <span className="nlp-keyword-pill" key={`${keyword.term}-${index}`}>
                          {keyword.term}
                          {keyword.count ? <small>{keyword.count}</small> : null}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <EmptyState title={translateText("暂无关键词")} body={translateText("当前数据集的关键词聚合结果为空。")} />
                  )}
                  {nlpInspection.word_cloud_terms && nlpInspection.word_cloud_terms.length > 0 ? (
                    <div className="nlp-word-cloud compact-cloud">
                      {nlpInspection.word_cloud_terms.slice(0, 16).map((term, index) => {
                        const fontSize = 12 + Math.min(12, Math.max(0, term.weight ?? 0) * 0.35);
                        return (
                          <span key={`${term.term}-${index}`} style={{ fontSize: `${fontSize}px` }}>
                            {term.term}
                          </span>
                        );
                      })}
                    </div>
                  ) : null}
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow={translateText("情绪")} title={translateText("情绪分布与特征")} />
                  <div className="nlp-sentiment-grid">
                    {(nlpInspection.sentiment_distribution ?? []).length > 0 ? (
                      nlpInspection.sentiment_distribution?.map((bucket, index) => (
                        <div className="nlp-sentiment-row" key={`${bucket.label}-${index}`}>
                          <span>{bucket.label}</span>
                          <div className="nlp-sentiment-bar">
                            <div style={{ width: `${Math.min(100, Math.max(0, (bucket.value ?? 0) * 100))}%` }} />
                          </div>
                          <strong>{formatPercent(bucket.value ?? 0, 1)}</strong>
                        </div>
                      ))
                    ) : (
                      <span className="muted">{translateText("暂无情绪直方图。")}</span>
                    )}
                  </div>
                  {nlpInspection.sample_feature_preview && Object.keys(nlpInspection.sample_feature_preview).length > 0 ? (
                    <div className="kv-list compact">
                      {Object.entries(nlpInspection.sample_feature_preview).map(([key, value]) => (
                        <div className="kv-row" key={key}>
                          <span>{key}</span>
                          <strong>{formatNumber(value ?? null, 4)}</strong>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyState title={translateText("暂无 NLP 特征")} body={translateText("当前还没有可展示的训练级 NLP 样例特征。")} />
                  )}
                </section>
              </div>
            ) : (
              <EmptyState title={translateText("暂无 NLP 载荷")} body={translateText("该数据集看起来包含 NLP 信号，但后端没有返回对应巡检载荷。")} />
            )
          ) : null}
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader eyebrow={translateText("概览")} title={translateText("这个数据集是做什么的")} description={translateText("高层用途、可用性状态和数据形态说明。")} />
        <div className="dataset-hero-grid">
          <section className="details-panel">
            <PanelHeader eyebrow={translateText("使用场景")} title={translateText("预期用途")} description={detail.intendedUse} />
            <div className="story-list">
              <div className="story-item">
                <p>{detail.heroSummary}</p>
              </div>
              <div className="story-item">
                <p>{detail.riskNote}</p>
              </div>
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow={translateText("规模")} title={translateText("数据形态与覆盖范围")} />
            <div className="definition-grid">
              <div className="definition-item">
                <span>{translateText("行数")}</span>
                <strong>{detail.summary.rowCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>{translateText("特征数")}</span>
                <strong>{detail.summary.featureCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>{translateText("标签数")}</span>
                <strong>{detail.summary.labelCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>{translateText("跨度")}</span>
                <strong>{detail.summary.labelHorizonLabel}</strong>
              </div>
              <div className="definition-item">
                <span>{translateText("实体范围")}</span>
                <strong>{detail.summary.entityScopeLabel}</strong>
              </div>
              <div className="definition-item">
                <span>{translateText("新鲜度")}</span>
                <strong>{detail.summary.freshnessLabel}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow={translateText("就绪度")} title={translateText("训练就绪状态")} description={translateText("这里直接展示后端返回的 readiness 合同结果。")} />
        <div className="dataset-lifecycle-grid">
          <section className="details-panel readiness-contract-panel">
            <div className="split-line">
              <strong>{detail.readiness.statusLabel}</strong>
              <StatusPill status={detail.readiness.rawStatus} />
            </div>
            <div className="dataset-callout">
              <strong>{detail.readiness.summary}</strong>
              <span>
                {readinessQuery.isError
                  ? (readinessQuery.error as Error).message
                  : translateText("通用 readiness 负责 schema 和训练安全校验；上方单独展示的官方 NLP 质量门禁会更严格。")}
              </span>
            </div>
            <div className="kv-list compact">
              {detail.readiness.checklist.map((item, index) => (
                <div className="kv-row" key={`${item.key}-${index}`}>
                  <span>{item.label}</span>
                  <strong>{item.value}</strong>
                </div>
              ))}
            </div>
            {readinessQuery.data?.modality_quality_summary ? (
              <div className="page-stack compact-gap">
                <strong>{translateText("模态质量摘要")}</strong>
                <div className="dataset-callout">
                  <strong>{translateText("多频率域按训练主时钟对齐")}</strong>
                  <span>
                    {translateText("市场维度保留 1h 主时钟；宏观、链上、衍生品、NLP 会保留原始频率，并通过 as-of / available_time 对齐后再进入训练样本，不会伪造成真实高频原始观测。")}
                  </span>
                </div>
                <ModalityQualitySummary
                  summary={readinessQuery.data.modality_quality_summary}
                  title={translateText("数据集模态质量摘要")}
                />
              </div>
            ) : null}
            {insufficientObservationModalities.length > 0 ? (
              <div className="dataset-callout">
                <strong>{translateText("宏观 / 链上当前是样本量不足，不是 freshness 不足")}</strong>
                <span>
                  {insufficientObservationModalities
                    .map((modality) => `${modality === "macro" ? translateText("宏观") : translateText("链上")} freshness lag 为 0 天，但有效观测点数还没达到 300`)
                    .join("；")}
                  {translateText("。这通常是因为当前时间窗只有半年左右，而这两类信号按较低频 canonical 采样后，可用点数不足。")}
                </span>
                <span>{translateText("如果想让它们过门槛，建议优先拉长时间窗，而不是把问题理解成“数据太旧”。")}</span>
              </div>
            ) : null}
            {readinessQuery.data?.aligned_multimodal_quality ? (
              <div className="page-stack compact-gap">
                <strong>{translateText("对齐后的多模态窗口")}</strong>
                <ModalityQualitySummary
                  summary={{
                    aligned_multimodal: {
                      ...readinessQuery.data.aligned_multimodal_quality,
                      modality:
                        readinessQuery.data.aligned_multimodal_quality.modality ??
                        "aligned_multimodal",
                    },
                  }}
                  title={translateText("对齐后的多模态质量")}
                />
              </div>
            ) : null}
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow={translateText("采集")} title={translateText("请求画像")} />
            <div className="kv-list compact">
              {detail.acquisitionEntries.map((row, index) => (
                <div className="kv-row" key={`${row.key}-${index}`}>
                  <span>{row.label}</span>
                  <strong>{row.value}</strong>
                </div>
              ))}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow={translateText("构建")} title={translateText("构建与 Schema")} />
            <div className="kv-list compact">
              {detail.buildEntries.concat(detail.schemaEntries).map((row, index) => (
                <div className="kv-row" key={`${row.key}-${row.value}-${index}`}>
                  <span>{row.label}</span>
                  <strong>{row.value}</strong>
                </div>
              ))}
            </div>
          </section>
        </div>
      </section>

      {hasNonMarketSignals ? (
        <section className="panel">
          <PanelHeader
            eyebrow="融合维度"
            title="多模态信号预览"
            description="展示训练面板中模型实际消费的 as-of 对齐特征；低频域会保留 freshness / ffill 语义，不伪装成原始高频观测。"
          />
          {featureSeriesQuery.isLoading ? <LoadingState label="正在加载多模态信号..." /> : null}
          {featureSeriesQuery.isError ? <ErrorState message={(featureSeriesQuery.error as Error).message} /> : null}
          {!featureSeriesQuery.isLoading && !featureSeriesQuery.isError && visibleFeatureDomains.length === 0 ? (
            <EmptyState title="暂无信号预览" body="当前训练面板没有可展示的非市场特征序列。" />
          ) : null}
          {visibleFeatureDomains.length > 0 ? (
            <div className="dataset-domain-grid">
              {visibleFeatureDomains.map((domain) => {
                const seriesItems = featureSeriesByDomain[domain] ?? [];
                const latestPoints = seriesItems
                  .map((series) => ({
                    label: series.label,
                    value: series.points[series.points.length - 1]?.value ?? null,
                  }))
                  .filter((item) => item.value !== null);
                return (
                  <article className="details-panel" key={domain}>
                    <PanelHeader
                      eyebrow={FEATURE_DOMAIN_LABELS[domain] ?? domain}
                      title={`${FEATURE_DOMAIN_LABELS[domain] ?? domain}时序`}
                      description={`${seriesItems.length} 个训练特征，来自融合面板的 point-in-time 对齐结果；图中按特征各自区间标准化，下面保留最新原始值。`}
                    />
                    <WorkbenchChart
                      option={featureSeriesChartOption(seriesItems)}
                      style={{ height: 300 }}
                    />
                    <div className="kv-list compact">
                      {latestPoints.slice(0, 4).map((item) => (
                        <div className="kv-row" key={item.label}>
                          <span>{item.label}</span>
                          <strong>{formatNumber(item.value, 4)}</strong>
                        </div>
                      ))}
                    </div>
                  </article>
                );
              })}
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader
          eyebrow="依赖"
          title="上下游关系图"
          description="当前删除模式为硬删除。这里展示依赖关系，仅供核查，不做拦截。"
        />
        <div className="metric-grid">
          <div className="metric-tile">
            <span>依赖总数</span>
            <strong>{dependencyItems.length}</strong>
          </div>
          <div className="metric-tile">
            <span>信息型下游引用</span>
            <strong>{blockingDependencies.length}</strong>
          </div>
          <div className="metric-tile">
            <span>删除模式</span>
            <strong>硬删除</strong>
          </div>
        </div>
        <div className="dataset-callout">
          <strong>删除不会被血缘关系阻止。</strong>
          <span>
            训练实例、回测和派生数据集仍可能保留已删除的数据集 ID，之后会显示为缺失引用。
          </span>
        </div>
        {dependenciesQuery.isLoading ? <LoadingState label="正在加载依赖关系图..." /> : null}
        {dependenciesQuery.isError ? <ErrorState message={(dependenciesQuery.error as Error).message} /> : null}
        {!dependenciesQuery.isLoading && !dependenciesQuery.isError ? (
          dependencyItems.length > 0 ? (
            <div className="dataset-domain-grid">
              {dependencyItems.map((item, index) =>
                item.href ? (
                  <Link className="dataset-card" key={`${item.dependency_kind}-${item.dependency_id}-${index}`} to={item.href}>
                    <div className="dataset-domain-top">
                      <div>
                        <strong>{dependencyText(item)}</strong>
                        <span>{dependencyKindLabel(item.dependency_kind)}</span>
                      </div>
                      <span className="dataset-card-tag">
                        {item.blocking ? "信息型引用" : dependencyDirectionLabel(item.direction)}
                      </span>
                    </div>
                    <div className="dataset-domain-stats">
                      <span>ID: {item.dependency_id}</span>
                    </div>
                  </Link>
                ) : (
                  <div className="dataset-card" key={`${item.dependency_kind}-${item.dependency_id}-${index}`}>
                    <div className="dataset-domain-top">
                      <div>
                        <strong>{dependencyText(item)}</strong>
                        <span>{dependencyKindLabel(item.dependency_kind)}</span>
                      </div>
                      <span className="dataset-card-tag">
                        {item.blocking ? "信息型引用" : dependencyDirectionLabel(item.direction)}
                      </span>
                    </div>
                    <div className="dataset-domain-stats">
                      <span>ID: {item.dependency_id}</span>
                    </div>
                  </div>
                ),
              )}
            </div>
          ) : (
            <EmptyState title="暂无依赖" body="没有找到相关的上游或下游项目。" />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="特征" title="字段分组与标签" description="以紧凑形式查看特征分组与标签定义。" />
        <div className="dataset-field-layout">
          <section className="details-panel">
            <PanelHeader eyebrow="分组" title="特征分组" />
            <div className="feature-group-list">
              {detail.featureGroups.length > 0 ? (
                detail.featureGroups.map((group) => (
                  <div className="feature-group-card" key={group.key}>
                    <strong>{group.label}</strong>
                    <p>{group.description}</p>
                    <span>{group.columns.join(" / ") || "暂无示例列"}</span>
                  </div>
                ))
              ) : (
                <EmptyState title="暂无特征分组" body="当前没有可展示的分组特征预览。" />
              )}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="标签" title="标签与质量摘要" />
            <div className="kv-list compact">
              <div className="kv-row">
                <span>标签列</span>
                <strong>{detail.labelColumns.join(" / ") || "--"}</strong>
              </div>
              <div className="kv-row">
                <span>切分策略</span>
                <strong>{detail.summary.raw.split_strategy ?? "--"}</strong>
              </div>
              <div className="kv-row">
                <span>时序安全</span>
                <strong>{detail.summary.raw.temporal_safety_summary ?? "--"}</strong>
              </div>
              <div className="kv-row">
                <span>缺失率</span>
                <strong>{formatPercent(detail.qualitySummary?.missing_ratio, 2)}</strong>
              </div>
              <div className="kv-row">
                <span>重复率</span>
                <strong>{formatPercent(detail.qualitySummary?.duplicate_ratio, 2)}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader
          eyebrow="市场切片"
          title="行情预览"
          description="包含真实市场锚点的训练面板会渲染 K 线预览；纯非市场数据域不会使用伪造回退图。"
        />
        {canRenderMarketSlice ? (
          <div className="page-stack">
            {barsQuery.isLoading ? <LoadingState label="正在加载行情条目..." /> : null}
            {barsQuery.isError ? <ErrorState message={(barsQuery.error as Error).message} /> : null}
            {!barsQuery.isLoading && !barsQuery.isError && bars.length === 0 ? (
              <EmptyState title="暂无 K 线" body="当前数据集窗口下没有可用的市场条目。" />
            ) : null}
            {bars.length > 0 ? (
              <>
                <div className="metric-grid">
                  <div className="metric-tile">
                    <span>已加载条数</span>
                    <strong>{bars.length} / {barsQuery.data?.total ?? bars.length}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>最新收盘价</span>
                    <strong>{formatNumber(bars[bars.length - 1]?.close, 2)}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>标的</span>
                    <strong>{marketPreviewSymbol}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>频率</span>
                    <strong>{detail.summary.frequencyLabel}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>最新 5 bar 均线</span>
                    <strong>{formatNumber(latestMa5, 2)}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>最新 10 bar 均线</span>
                    <strong>{formatNumber(latestMa10, 2)}</strong>
                  </div>
                </div>
                <div className="dataset-callout">
                  <strong>实际展示时间</strong>
                  <span>{formatWindow(bars[0]?.event_time, bars[bars.length - 1]?.event_time)}</span>
                </div>
                <section className="panel">
                  <DatasetCandlestickChart candles={candles} showMA10={true} showMA5={true} showVolume={true} />
                </section>
                <section className="panel">
                  <PanelHeader eyebrow="数据行" title="最新行情记录" />
                  <div className="dataset-browser-table-shell">
                    <table className="data-table compact-table">
                      <thead>
                        <tr>
                          <th>事件时间</th>
                          <th>可用时间</th>
                          <th>标的</th>
                          <th>开盘</th>
                          <th>最高</th>
                          <th>最低</th>
                          <th>收盘</th>
                          <th>成交量</th>
                        </tr>
                      </thead>
                      <tbody>
                        {bars.slice(-12).reverse().map((row, index) => (
                          <tr key={`${row.event_time}-${row.symbol}-${index}`}>
                            <td>{formatDate(row.event_time)}</td>
                            <td>{formatDate(row.available_time)}</td>
                            <td>{row.symbol}</td>
                            <td>{formatNumber(row.open, 2)}</td>
                            <td>{formatNumber(row.high, 2)}</td>
                            <td>{formatNumber(row.low, 2)}</td>
                            <td>{formatNumber(row.close, 2)}</td>
                            <td>{formatNumber(row.volume, 0)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>
              </>
            ) : null}
          </div>
        ) : (
          <EmptyState title="暂无市场预览" body="这个数据集没有声明可查询的真实市场 OHLCV 锚点。" />
        )}
      </section>

      <DatasetDeleteDialog
        datasetId={datasetId}
        datasetLabel={detail.summary.title}
        onClose={() => setDeleteOpen(false)}
        onDeleted={() => {
          setIsDeleted(true);
          navigate("/datasets/browser");
        }}
        open={deleteOpen}
      />
    </div>
  );
}
