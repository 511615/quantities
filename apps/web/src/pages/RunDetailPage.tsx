import { type ReactNode, useState } from "react";
import type { EChartsOption } from "echarts";
import { Link, useParams } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { useArtifactPreview, useRunDetail } from "../shared/api/hooks";
import type { ArtifactView, RunDetailView } from "../shared/api/types";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatArtifactLabel } from "../shared/lib/labels";
import { mapRunDetail } from "../shared/view-model/mappers";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { MetricGrid } from "../shared/ui/MetricGrid";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";

type NumericMap = Record<string, number>;
type GenericRecord = Record<string, unknown>;
type TimeSeriesPoint = { timestamp?: string; prediction?: number; target?: number; residual?: number };
type ScatterPoint = { prediction?: number; target?: number; timestamp?: string };
type HistogramPoint = { label?: string; count?: number; center?: number };

const PERCENT_METRIC_KEYS = new Set(["sign_hit_rate", "mape", "smape"]);

function asRecord(value: unknown): GenericRecord {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as GenericRecord)
    : {};
}

function asNumericMap(value: unknown): NumericMap {
  const record = asRecord(value);
  return Object.fromEntries(
    Object.entries(record).filter(([, item]) => typeof item === "number"),
  ) as NumericMap;
}

function asTimeSeries(value: unknown): TimeSeriesPoint[] {
  return Array.isArray(value) ? (value as TimeSeriesPoint[]) : [];
}

function asScatterSeries(value: unknown): ScatterPoint[] {
  return Array.isArray(value) ? (value as ScatterPoint[]) : [];
}

function asHistogram(value: unknown): HistogramPoint[] {
  return Array.isArray(value) ? (value as HistogramPoint[]) : [];
}

function asArtifacts(value: unknown): ArtifactView[] {
  return Array.isArray(value) ? (value as ArtifactView[]) : [];
}

function metricLabel(key: string): string {
  const labels: Record<string, string> = {
    mae: "MAE",
    rmse: "RMSE",
    r2: "R2",
    bias: "偏差",
    sign_hit_rate: "方向命中率",
    valid_mae: "验证集 MAE",
    mean_prediction: "预测均值",
    mean_target: "真实均值",
    sample_count: "样本数",
    mape: "MAPE",
    smape: "SMAPE",
  };
  return labels[key] ?? key;
}

function formatMetricValue(key: string, value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "--";
  }
  if (PERCENT_METRIC_KEYS.has(key)) {
    return formatPercent(value);
  }
  if (key === "sample_count") {
    return Math.round(value).toString();
  }
  return formatNumber(value);
}

function stringValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "--";
  }
  if (Array.isArray(value)) {
    return value.join(", ") || "--";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function combineArtifacts(detail: RunDetailView): ArtifactView[] {
  const seen = new Set<string>();
  const merged: ArtifactView[] = [];
  for (const artifact of [...detail.artifacts, ...asArtifacts(detail.evaluation_artifacts)]) {
    if (!seen.has(artifact.uri)) {
      seen.add(artifact.uri);
      merged.push(artifact);
    }
  }
  return merged;
}

function deriveDatasetIds(detail: RunDetailView) {
  const rawIds = detail.dataset_ids?.length ? detail.dataset_ids : detail.dataset_id ? [detail.dataset_id] : [];
  return Array.from(new Set(rawIds.map((item) => item.trim()).filter(Boolean)));
}

function lineChartOption(
  points: TimeSeriesPoint[],
  seriesKey: "prediction" | "target" | "residual",
  title: string,
): EChartsOption {
  return {
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#d5cfc1" } },
    grid: { left: 42, right: 20, top: 36, bottom: 36 },
    xAxis: {
      type: "category",
      data: points.map((point) => formatDate(point.timestamp ?? null)),
      axisLabel: { color: "#b8b09e", hideOverlap: true },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b8b09e" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        name: title,
        type: "line",
        smooth: true,
        showSymbol: false,
        areaStyle: seriesKey === "residual" ? undefined : { opacity: 0.12 },
        lineStyle: { color: seriesKey === "residual" ? "#ffb479" : "#c7ff73" },
        data: points.map((point) => {
          if (seriesKey === "residual") {
            return point.residual ?? 0;
          }
          return point[seriesKey] ?? 0;
        }),
      },
    ],
  } as EChartsOption;
}

function compareChartOption(points: TimeSeriesPoint[]): EChartsOption {
  return {
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#d5cfc1" } },
    grid: { left: 42, right: 20, top: 36, bottom: 36 },
    xAxis: {
      type: "category",
      data: points.map((point) => formatDate(point.timestamp ?? null)),
      axisLabel: { color: "#b8b09e", hideOverlap: true },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b8b09e" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        name: "预测值",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { color: "#c7ff73" },
        areaStyle: { opacity: 0.12 },
        data: points.map((point) => point.prediction ?? 0),
      },
      {
        name: "真实值",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { color: "#8ad4ff" },
        data: points.map((point) => point.target ?? 0),
      },
    ],
  } as EChartsOption;
}

function scatterChartOption(points: ScatterPoint[]): EChartsOption {
  return {
    tooltip: { trigger: "item" },
    grid: { left: 42, right: 20, top: 24, bottom: 36 },
    xAxis: {
      type: "value",
      name: "预测值",
      nameTextStyle: { color: "#b8b09e" },
      axisLabel: { color: "#b8b09e" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    yAxis: {
      type: "value",
      name: "真实值",
      nameTextStyle: { color: "#b8b09e" },
      axisLabel: { color: "#b8b09e" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        type: "scatter",
        symbolSize: 9,
        itemStyle: { color: "#ffb479" },
        data: points.map((point) => [point.prediction ?? 0, point.target ?? 0]),
      },
    ],
  } as EChartsOption;
}

function histogramChartOption(points: HistogramPoint[]): EChartsOption {
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 42, right: 20, top: 24, bottom: 56 },
    xAxis: {
      type: "category",
      data: points.map((point) => point.label ?? ""),
      axisLabel: { color: "#b8b09e", interval: "auto", rotate: 24 },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b8b09e" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        type: "bar",
        itemStyle: { color: "#8ad4ff" },
        data: points.map((point) => point.count ?? 0),
      },
    ],
  } as EChartsOption;
}

function SummaryList({
  items,
}: {
  items: Array<{ label: string; value: ReactNode }>;
}) {
  return (
    <div className="stack-list">
      {items.map((item) => (
        <div className="stack-item align-start" key={item.label}>
          <strong>{item.label}</strong>
          <span>{item.value}</span>
        </div>
      ))}
    </div>
  );
}

export function RunDetailPage() {
  const { runId = "" } = useParams();
  const [previewUri, setPreviewUri] = useState<string | null>(null);
  const runQuery = useRunDetail(runId);
  const previewQuery = useArtifactPreview(previewUri);

  if (runQuery.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (runQuery.isError) {
    return <ErrorState message={(runQuery.error as Error).message} />;
  }
  if (!runQuery.data) {
    return <EmptyState body="没有找到对应的训练运行。" title={I18N.state.empty} />;
  }

  const detail = mapRunDetail(runQuery.data);
  const evaluation = asRecord(detail.evaluation_summary);
  const regressionMetrics = asNumericMap(evaluation.regression_metrics);
  const datasetSummary = asRecord(detail.dataset_summary);
  const timeRange = asRecord(detail.time_range);
  const series = asRecord(evaluation.series);
  const predictionVsTarget = asTimeSeries(series.prediction_vs_target_timeseries);
  const residualSeries = asTimeSeries(series.residual_timeseries);
  const scatterSeries = asScatterSeries(series.prediction_vs_target_scatter);
  const histogramSeries = asHistogram(series.residual_histogram);
  const evaluationArtifacts = asArtifacts(detail.evaluation_artifacts);
  const artifacts = combineArtifacts(detail);
  const datasetIds = deriveDatasetIds(detail);
  const compositionSources = detail.composition?.source_runs ?? [];
  const displayContextItems = [
    { label: "任务类型", value: stringValue(detail.task_type) },
    { label: "数据集类型", value: stringValue(datasetSummary.dataset_type) },
    { label: "数据域", value: stringValue(datasetSummary.data_domains ?? datasetSummary.data_domain) },
    { label: "实体范围", value: stringValue(datasetSummary.entity_scope) },
    { label: "特征模式", value: stringValue(datasetSummary.feature_schema_hash) },
    { label: "快照版本", value: stringValue(datasetSummary.snapshot_version) },
    { label: "训练参数", value: stringValue(detail.tracking_params) },
    { label: "复现实验上下文", value: stringValue(detail.repro_context) },
  ];
  const displaySummaryMetrics = [
    { label: "模型", value: detail.model_name },
    { label: "模型家族", value: detail.family ?? "--" },
    { label: "数据集数量", value: datasetIds.length > 0 ? String(datasetIds.length) : "--" },
    {
      label: "时间范围",
      value: `${stringValue(timeRange.start_time)} 至 ${stringValue(timeRange.end_time)}`,
    },
    { label: "创建时间", value: formatDate(detail.created_at) },
    { label: "训练后端", value: detail.backend ?? "--" },
    { label: "产物格式", value: detail.artifact_format_status ?? "--" },
  ];
  const displayCoreMetrics = [
    { label: "MAE", value: formatMetricValue("mae", regressionMetrics.mae) },
    { label: "RMSE", value: formatMetricValue("rmse", regressionMetrics.rmse) },
    { label: "R2", value: formatMetricValue("r2", regressionMetrics.r2) },
    { label: "偏差", value: formatMetricValue("bias", regressionMetrics.bias) },
    {
      label: "方向命中率",
      value: formatMetricValue("sign_hit_rate", regressionMetrics.sign_hit_rate),
    },
    { label: "验证集 MAE", value: formatMetricValue("valid_mae", regressionMetrics.valid_mae) },
  ];
  const contextItems = displayContextItems;
  const summaryMetrics = displaySummaryMetrics;
  const coreMetrics = displayCoreMetrics;

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.trainedModels}
          title={detail.run_id}
          description="这里汇总训练后的模型摘要、组合来源、评估产物和回测入口，方便快速确认这个 run 能否按预期工作。"
          action={
            <div className="table-actions">
              <Link className="link-button" to="/models?tab=trained">
                {I18N.nav.trainedModels}
              </Link>
              <StatusPill status={detail.status} />
              <LaunchBacktestDrawer
                initialRunId={detail.run_id}
                initialDatasetId={detail.dataset_id}
                initialDatasetIds={datasetIds}
              />
            </div>
          }
        />
        <MetricGrid items={displaySummaryMetrics} />
      </section>

      <section className="panel">
        <PanelHeader
          eyebrow="组合来源"
            title="多模态来源运行"
            description="这里列出参与融合的源运行、模态、权重和各自数据集，方便确认组合是否符合预期。"
          />
        {datasetIds.length > 0 ? (
          <SummaryList
            items={[
              {
                label: "数据集 IDs",
                value: (
                  <div className="table-title-cell">
                    {datasetIds.map((datasetId) => (
                      <Link key={datasetId} to={`/datasets/${encodeURIComponent(datasetId)}`}>
                        {datasetId}
                      </Link>
                    ))}
                  </div>
                ),
              },
            ]}
          />
        ) : (
          <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
        )}
      </section>

      <section className="panel">
        <PanelHeader
          eyebrow="核心评估"
          title="回归指标总览"
          description={`主展示范围：${stringValue(evaluation.selected_scope)}。如果这是组合 run，训练阶段本身可能不产出完整评估快照。`}
        />
        <MetricGrid items={displayCoreMetrics} />
        {compositionSources.length > 0 && Object.keys(regressionMetrics).length === 0 ? (
          <EmptyState
            title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
        ) : null}
      </section>

      {compositionSources.length > 0 ? (
        <section className="panel">
          <PanelHeader
            eyebrow="组合来源"
            title="多模态来源运行"
            description="这里列出参与融合的源运行、模态、权重和各自数据集，方便确认组合是否符合预期。"
          />
          <div className="stack-list">
            {compositionSources.map((source) => (
              <div className="stack-item align-start" key={`${source.run_id}-${source.modality}`}>
                <strong>{source.run_id}</strong>
                <span>{`模态：${source.modality || "--"} / 权重：${source.weight ?? "--"} / 模型：${source.model_name || "--"}`}</span>
                <span className="table-title-cell">
                  {(source.dataset_ids ?? []).length > 0
                    ? (source.dataset_ids ?? []).map((datasetId) => (
                        <Link key={datasetId} to={`/datasets/${encodeURIComponent(datasetId)}`}>
                          {datasetId}
                        </Link>
                      ))
                    : "--"}
                </span>
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="回归曲线" title="预测与真实对比" />
          {predictionVsTarget.length > 0 ? (
            <WorkbenchChart option={compareChartOption(predictionVsTarget)} style={{ height: 320 }} />
          ) : (
            <EmptyState
              title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow="误差分布" title="残差直方图" />
          {histogramSeries.length > 0 ? (
            <WorkbenchChart option={histogramChartOption(histogramSeries)} style={{ height: 320 }} />
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="回归诊断" title="预测散点图" />
          {scatterSeries.length > 0 ? (
            <WorkbenchChart option={scatterChartOption(scatterSeries)} style={{ height: 320 }} />
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow="误差序列" title="残差时间线" />
          {residualSeries.length > 0 ? (
            <WorkbenchChart
              option={lineChartOption(residualSeries, "residual", "残差")}
              style={{ height: 320 }}
            />
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="训练上下文" title="数据与配置线索" />
          <SummaryList items={displayContextItems} />
        </section>

        <section className="panel">
          <PanelHeader eyebrow="特征解释" title="特征重要性" />
          {Object.keys(detail.feature_importance).length > 0 ? (
            <div className="stack-list">
              {Object.entries(detail.feature_importance)
                .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
                .map(([name, value]) => (
                  <div className="stack-item" key={name}>
                    <strong>{name}</strong>
                    <span>{formatNumber(value)}</span>
                  </div>
                ))}
            </div>
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
          {evaluationArtifacts.length > 0 ? (
            <p className="drawer-copy">评估摘要和特征解释工件已经落盘，可在下方工件区继续预览。</p>
          ) : null}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="预测产物" title="可用预测产物" />
          {detail.predictions.length > 0 ? (
            <div className="stack-list">
              {detail.predictions.map((prediction) => (
                <div className="stack-item align-start" key={prediction.uri}>
                  <div>
                    <strong>{prediction.scope}</strong>
                    <div>{`${prediction.sample_count} 条样本`}</div>
                  </div>
                  <button className="link-button" onClick={() => setPreviewUri(prediction.uri)} type="button">
                    {I18N.action.preview}
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow={I18N.nav.backtests} title="关联回测" />
          {detail.related_backtests.length > 0 ? (
            <div className="stack-list">
              {detail.related_backtests.map((backtest) => (
                <div className="stack-item align-start" key={backtest.backtest_id}>
                  <div>
                    <strong>
                      <Link to={`/backtests/${encodeURIComponent(backtest.backtest_id)}`}>{backtest.backtest_id}</Link>
                    </strong>
                    <div>{`年化收益 ${formatPercent(backtest.annual_return)}`}</div>
                    <div>
                      <GlossaryHint hintKey="max_drawdown" /> {formatPercent(backtest.max_drawdown)}
                    </div>
                  </div>
                  <StatusPill status={backtest.passed_consistency_checks === false ? "failed" : "success"} />
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
          )}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow="详细指标" title="评估指标明细" />
        {Object.keys(regressionMetrics).length > 0 ? (
          <table className="data-table compact-table">
            <thead>
              <tr>
                <th>指标</th>
                <th>值</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(regressionMetrics).map(([key, value]) => (
                <tr key={key}>
                  <td>{key === "mae" ? <GlossaryHint hintKey="mae" /> : metricLabel(key)}</td>
                  <td>{formatMetricValue(key, value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <EmptyState title="组合模型暂无独立训练评估"
            body="这是由多个单模态 run 组合出来的模型实例，创建阶段不会重新训练，因此这里不会像单模型那样生成完整训练评估。请优先查看来源运行和关联回测。"
          />
        )}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="工件浏览" title="训练与评估工件" />
        <div className="artifact-grid">
          <div className="artifact-list">
            {artifacts.map((artifact) => (
              <button
                className="artifact-row"
                key={artifact.uri}
                onClick={() => setPreviewUri(artifact.uri)}
                type="button"
              >
                <strong>{formatArtifactLabel(artifact.kind, artifact.label)}</strong>
                <span>{artifact.uri}</span>
              </button>
            ))}
          </div>
          <div className="artifact-preview">
            {previewQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {previewQuery.isError ? <ErrorState message={(previewQuery.error as Error).message} /> : null}
            {!previewQuery.isLoading && !previewQuery.isError ? (
              previewQuery.data ? (
                <pre>{JSON.stringify(previewQuery.data.content, null, 2)}</pre>
              ) : (
                <EmptyState body="点击左侧工件即可预览内容。" title={I18N.state.selectArtifact} />
              )
            ) : null}
          </div>
        </div>
      </section>

      {detail.notes.length > 0 || (detail.missing_artifacts?.length ?? 0) > 0 ? (
        <section className="panel">
          <PanelHeader eyebrow="说明" title="运行备注" />
          <div className="stack-list">
            {detail.notes.map((note) => (
              <div className="stack-item align-start" key={note}>
                <strong>{note}</strong>
              </div>
            ))}
            {(detail.missing_artifacts ?? []).map((item) => (
              <div className="stack-item align-start" key={item}>
                <strong>{`缺失工件：${item}`}</strong>
              </div>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}

