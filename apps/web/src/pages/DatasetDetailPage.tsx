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
  useDatasetNlpInspection,
  useDatasetOhlcv,
  useDatasetReadiness,
} from "../shared/api/hooks";
import type { DatasetDependencyView } from "../shared/api/types";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";

const NLP_FEATURE_REGEX = /^(sentiment|text|news)_/i;

function dependencyKindLabel(kind: string) {
  const normalized = kind.trim().toLowerCase();
  if (normalized === "run") {
    return "Training run";
  }
  if (normalized === "backtest") {
    return "Backtest";
  }
  if (normalized === "dataset") {
    return "Derived dataset";
  }
  if (normalized === "data_asset") {
    return "Upstream asset";
  }
  return kind || "Dependency";
}

function dependencyDirectionLabel(direction?: string) {
  if (direction === "depends_on") {
    return "Upstream";
  }
  if (direction === "referenced_by") {
    return "Downstream";
  }
  return "Related";
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
      title: "Raw event library",
      body: "The upstream event table keeps each NLP record with event_time, available_time, source, and metadata for auditing and temporal safety.",
    },
    {
      title: "Training panel",
      body: "Features are aligned with labels and splits so the dataset_id-first training workflow can run without touching raw text.",
    },
    {
      title: "Feature snapshot",
      body: "Aggregated sentiment and keyword metrics are packaged so fusion or downstream models can consume stable signal assets.",
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

  const canRenderMarketSlice =
    activeDatasetId && detail?.summary.dataDomain === "market" && Boolean(detail.summary.symbol);
  const barsQuery = useDatasetOhlcv(canRenderMarketSlice ? activeDatasetId : null, {
    page: 1,
    per_page: 120,
  });
  const bars = barsQuery.data?.items ?? [];
  const candles = useMemo(() => toCandles(bars), [bars]);

  const nlpTimelineOption = useMemo(() => {
    const timeline = nlpInspectionQuery.data?.event_timeline ?? [];
    if (timeline.length === 0) {
      return null;
    }
    return {
      tooltip: { trigger: "axis" as const },
        legend: {
          data: ["Event count", "Avg sentiment"],
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
            name: "Events",
            axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
            axisLabel: { color: "#b8b09e" },
            splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
          },
          {
            type: "value" as const,
            name: "Sentiment",
            min: -1,
            max: 1,
            axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
            axisLabel: { color: "#b8b09e" },
          splitLine: { show: false },
        },
      ],
        series: [
          {
            name: "Event count",
            type: "line" as const,
            smooth: true,
            data: timeline.map((point) => point.event_count),
            lineStyle: { width: 2, color: "#c7ff73" },
          },
          {
            name: "Avg sentiment",
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
    return <LoadingState label="Loading dataset detail..." />;
  }

  if (detailQuery.isError) {
    return <ErrorState message={(detailQuery.error as Error).message} />;
  }

  if (!detail) {
    return <EmptyState title="Dataset not found" body="The requested dataset detail could not be loaded." />;
  }

  const dependencyItems = dependenciesQuery.data?.items ?? [];
  const blockingDependencies = dependenciesQuery.data?.blocking_items ?? [];
  const nlpInspection = nlpInspectionQuery.data;
  const requestedRangeLabel =
    nlpInspection?.requested_start_time && nlpInspection?.requested_end_time
      ? `${formatDate(nlpInspection.requested_start_time)} - ${formatDate(nlpInspection.requested_end_time)}`
      : "--";
  const actualRangeLabel =
    nlpInspection?.actual_start_time && nlpInspection?.actual_end_time
      ? `${formatDate(nlpInspection.actual_start_time)} - ${formatDate(nlpInspection.actual_end_time)}`
      : "--";
  const sourceVendorLabel =
    nlpInspection?.source_vendors && nlpInspection.source_vendors.length > 0
      ? nlpInspection.source_vendors.join(", ")
      : detail.summary.sourceVendor || "--";

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">Dataset detail</div>
          <h1>{detail.summary.title}</h1>
          <p>{detail.heroSummary}</p>
        </div>
        <div className="hero-actions">
          {detail.readiness.rawStatus !== "not_ready" ? (
            <LaunchTrainDrawer
              datasetId={datasetId}
              datasetLabel={detail.summary.title}
              triggerLabel="Train on this dataset"
              title="Launch training"
              description="Training stays dataset_id-first and will use this dataset directly."
            />
          ) : null}
          <button className="link-button danger-link" onClick={() => setDeleteOpen(true)} type="button">
            Delete dataset
          </button>
          <Link className="comparison-link" to="/datasets/browser">
            Back to browser
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            Training datasets
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav detailLabel="Detail" />

      <div className="metric-grid">
        <div className="metric-tile">
          <span>Data domain</span>
          <strong>{detail.summary.dataDomainLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>Dataset type</span>
          <strong>{detail.summary.datasetTypeLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>Coverage</span>
          <strong>{detail.summary.coverageLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>Snapshot</span>
          <strong>{detail.summary.snapshotVersion}</strong>
        </div>
      </div>

      <section className="panel dataset-roles-panel">
        <PanelHeader
          eyebrow="Dataset story"
          title="Signal path"
          description="Raw NLP events turn into labeled training panels and structured feature snapshots. This summary explains how the current dataset fits into that path."
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
          <strong>Structured NLP signal asset</strong>
          <span>
            This dataset exposes aggregated sentiment, keyword, and attention metrics derived from text. The training pipeline consumes numbers only—not the raw text—so the asset stays traceable and compatible with the market-first training/backtest flow.
          </span>
        </div>
      </section>

      {hasNlpSignal ? (
        <section className="panel">
          <PanelHeader
            eyebrow="NLP inspection"
            title="Text and event inspection"
            description="Preview text events, keyword concentration, source mix, and training-ready sentiment features."
          />
          {nlpInspectionQuery.isLoading ? <LoadingState label="Loading NLP inspection..." /> : null}
          {nlpInspectionQuery.isError ? <ErrorState message={(nlpInspectionQuery.error as Error).message} /> : null}
          {!nlpInspectionQuery.isLoading && !nlpInspectionQuery.isError ? (
            nlpInspection?.contains_nlp ? (
              <div className="dataset-nlp-layout">
                <section className="details-panel nlp-summary-panel">
                  <PanelHeader eyebrow="Coverage" title="Coverage and source" />
                  <div className="nlp-stat-grid">
                    <div className="metric-tile compact">
                      <span>Requested window</span>
                      <strong>{requestedRangeLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>Actual NLP coverage</span>
                      <strong>{actualRangeLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>Vendor</span>
                      <strong>{sourceVendorLabel}</strong>
                    </div>
                    <div className="metric-tile compact">
                      <span>Preview events</span>
                      <strong>{nlpInspection.recent_event_previews?.length ?? 0}</strong>
                    </div>
                  </div>
                  <div className="dataset-callout">
                    <strong>{nlpInspection.coverage_summary ?? "NLP inspection is available."}</strong>
                    <span>
                      This panel surfaces structured NLP signals (sentiment, counts, attention) derived from the event metadata, not raw text.
                    </span>
                  </div>
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow="Timeline" title="Event timeline" />
                  {nlpTimelineOption ? (
                    <WorkbenchChart option={nlpTimelineOption} />
                  ) : (
                    <EmptyState title="No timeline" body="Event timeline data is not available for this dataset." />
                  )}
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow="Preview" title="Recent text samples" />
                  {nlpInspection.recent_event_previews && nlpInspection.recent_event_previews.length > 0 ? (
                    <div className="nlp-preview-list">
                      {nlpInspection.recent_event_previews.slice(0, 4).map((preview, index) => (
                        <div className="nlp-preview-row" key={`${preview.event_id}-${index}`}>
                          <div>
                            <strong>{preview.title}</strong>
                            <span>
                              {(preview.symbol || "mixed")} · {preview.source}
                            </span>
                            <span>{formatDate(preview.event_time)}</span>
                          </div>
                          <p>{preview.snippet}</p>
                          {preview.url ? (
                            <a href={preview.url} rel="noreferrer" target="_blank">
                              Open source
                            </a>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyState title="No text preview" body="No event previews are available for this dataset." />
                  )}
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow="Sources" title="Source breakdown" />
                  <div className="nlp-source-breakdown">
                    {(nlpInspection.source_breakdown ?? []).length > 0 ? (
                      nlpInspection.source_breakdown?.slice(0, 8).map((source, index) => (
                        <div className="nlp-source-row" key={`${source.source}-${index}`}>
                          <strong>{source.source}</strong>
                          <span>
                            {source.count} items · {formatPercent(source.share ?? 0, 1)}
                          </span>
                        </div>
                      ))
                    ) : (
                      <span className="muted">No source mix available.</span>
                    )}
                  </div>
                </section>

                <section className="details-panel">
                  <PanelHeader eyebrow="Keywords" title="Keywords and terms" />
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
                    <EmptyState title="No keywords" body="Keyword aggregation is empty for the current dataset." />
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
                  <PanelHeader eyebrow="Sentiment" title="Sentiment and features" />
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
                      <span className="muted">No sentiment histogram available.</span>
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
                    <EmptyState title="No NLP features" body="Training-ready NLP sample features are not available yet." />
                  )}
                </section>
              </div>
            ) : (
              <EmptyState title="No NLP payload" body="The dataset suggests NLP usage, but no inspection payload was returned." />
            )
          ) : null}
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader eyebrow="Overview" title="What this dataset is for" description="High-level use case, readiness, and data shape." />
        <div className="dataset-hero-grid">
          <section className="details-panel">
            <PanelHeader eyebrow="Use case" title="Intended use" description={detail.intendedUse} />
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
            <PanelHeader eyebrow="Scale" title="Shape and coverage" />
            <div className="definition-grid">
              <div className="definition-item">
                <span>Rows</span>
                <strong>{detail.summary.rowCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>Features</span>
                <strong>{detail.summary.featureCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>Labels</span>
                <strong>{detail.summary.labelCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>Horizon</span>
                <strong>{detail.summary.labelHorizonLabel}</strong>
              </div>
              <div className="definition-item">
                <span>Entity scope</span>
                <strong>{detail.summary.entityScopeLabel}</strong>
              </div>
              <div className="definition-item">
                <span>Freshness</span>
                <strong>{detail.summary.freshnessLabel}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow="Readiness" title="Training readiness" description="The backend readiness contract is shown directly here." />
        <div className="dataset-lifecycle-grid">
          <section className="details-panel">
            <div className="split-line">
              <strong>{detail.readiness.statusLabel}</strong>
              <StatusPill status={detail.readiness.rawStatus} />
            </div>
            <div className="dataset-callout">
              <strong>{detail.readiness.summary}</strong>
              <span>
                {readinessQuery.isError
                  ? (readinessQuery.error as Error).message
                  : "Training and backtest gates use the same readiness state and schema checks shown here."}
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
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="Acquisition" title="Request profile" />
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
            <PanelHeader eyebrow="Build" title="Build and schema" />
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

      <section className="panel">
        <PanelHeader
          eyebrow="Dependencies"
          title="Upstream and downstream graph"
          description="Deletion is hard delete now. Dependencies are surfaced for review only."
        />
        <div className="metric-grid">
          <div className="metric-tile">
            <span>Total dependencies</span>
            <strong>{dependencyItems.length}</strong>
          </div>
          <div className="metric-tile">
            <span>Informational downstream refs</span>
            <strong>{blockingDependencies.length}</strong>
          </div>
          <div className="metric-tile">
            <span>Delete mode</span>
            <strong>Hard delete</strong>
          </div>
        </div>
        <div className="dataset-callout">
          <strong>Deletion will not be blocked by lineage.</strong>
          <span>
            Runs, backtests, and derived datasets may still carry the deleted dataset id and will surface a missing reference afterward.
          </span>
        </div>
        {dependenciesQuery.isLoading ? <LoadingState label="Loading dependency graph..." /> : null}
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
                        {item.blocking ? "Informational" : dependencyDirectionLabel(item.direction)}
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
                        {item.blocking ? "Informational" : dependencyDirectionLabel(item.direction)}
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
            <EmptyState title="No dependencies" body="No related upstream or downstream items were found." />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="Features" title="Field groups and labels" description="A compact view of feature groupings and label definitions." />
        <div className="dataset-field-layout">
          <section className="details-panel">
            <PanelHeader eyebrow="Groups" title="Feature groups" />
            <div className="feature-group-list">
              {detail.featureGroups.length > 0 ? (
                detail.featureGroups.map((group) => (
                  <div className="feature-group-card" key={group.key}>
                    <strong>{group.label}</strong>
                    <p>{group.description}</p>
                    <span>{group.columns.join(" / ") || "No example columns"}</span>
                  </div>
                ))
              ) : (
                <EmptyState title="No feature groups" body="No grouped feature preview is available." />
              )}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="Labels" title="Label and quality summary" />
            <div className="kv-list compact">
              <div className="kv-row">
                <span>Label columns</span>
                <strong>{detail.labelColumns.join(" / ") || "--"}</strong>
              </div>
              <div className="kv-row">
                <span>Split strategy</span>
                <strong>{detail.summary.raw.split_strategy ?? "--"}</strong>
              </div>
              <div className="kv-row">
                <span>Temporal safety</span>
                <strong>{detail.summary.raw.temporal_safety_summary ?? "--"}</strong>
              </div>
              <div className="kv-row">
                <span>Missing ratio</span>
                <strong>{formatPercent(detail.qualitySummary?.missing_ratio, 2)}</strong>
              </div>
              <div className="kv-row">
                <span>Duplicate ratio</span>
                <strong>{formatPercent(detail.qualitySummary?.duplicate_ratio, 2)}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow="Market slice" title="OHLCV preview" description="Only market datasets render a bar preview. Other domains avoid synthetic fallback visuals." />
        {canRenderMarketSlice ? (
          <div className="page-stack">
            {barsQuery.isLoading ? <LoadingState label="Loading OHLCV bars..." /> : null}
            {barsQuery.isError ? <ErrorState message={(barsQuery.error as Error).message} /> : null}
            {!barsQuery.isLoading && !barsQuery.isError && bars.length === 0 ? (
              <EmptyState title="No bars" body="No market bars are available for the current dataset window." />
            ) : null}
            {bars.length > 0 ? (
              <>
                <div className="metric-grid">
                  <div className="metric-tile">
                    <span>Loaded bars</span>
                    <strong>{bars.length}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>Latest close</span>
                    <strong>{formatNumber(bars[bars.length - 1]?.close, 2)}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>Symbol</span>
                    <strong>{detail.summary.symbolLabel}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>Frequency</span>
                    <strong>{detail.summary.frequencyLabel}</strong>
                  </div>
                </div>
                <section className="panel">
                  <DatasetCandlestickChart candles={candles} showMA10={true} showMA5={true} showVolume={true} />
                </section>
                <section className="panel">
                  <PanelHeader eyebrow="Rows" title="Latest OHLCV rows" />
                  <div className="dataset-browser-table-shell">
                    <table className="data-table compact-table">
                      <thead>
                        <tr>
                          <th>Event time</th>
                          <th>Available time</th>
                          <th>Symbol</th>
                          <th>Open</th>
                          <th>High</th>
                          <th>Low</th>
                          <th>Close</th>
                          <th>Volume</th>
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
          <EmptyState title="No market preview" body="This dataset is not a single market slice with real OHLCV bars." />
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
