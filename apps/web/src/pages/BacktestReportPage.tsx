import type { EChartsOption } from "echarts";
import { Link, useParams } from "react-router-dom";

import { useBacktestDetail } from "../shared/api/hooks";
import type {
  ArtifactView,
  BacktestEngineView,
  BacktestReportView,
  ScenarioDeltaView,
  TimeValuePoint,
} from "../shared/api/types";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatArtifactLabel, formatModalityLabel } from "../shared/lib/labels";
import {
  localizeBacktestGateDetail,
  localizeBacktestGateReason,
  localizeBacktestGateLabel,
  localizeBacktestMetadata,
  localizeBacktestNote,
  localizeBacktestRequirement,
  localizeBacktestTemplateName,
  localizeBacktestWarning,
  localizeProtocolValue,
} from "../shared/lib/protocolI18n";
import { mapBacktestDetail } from "../shared/view-model/mappers";
import { ModalityQualitySummary } from "../shared/ui/ModalityQualitySummary";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";

const PERCENT_KEYS = new Set([
  "annual_return",
  "cumulative_return",
  "max_drawdown",
  "alpha",
  "beta",
  "var_95",
  "cvar_95",
  "fill_rate",
  "partial_fill_rate",
  "rejection_rate",
  "drawdown",
  "gross_leverage",
  "net_leverage",
  "long_exposure",
  "short_exposure",
  "signal_autocorrelation",
  "average_signal",
  "worst_scenario_return_delta",
]);

function formatMetric(key: string, value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "--";
  }
  if (PERCENT_KEYS.has(key)) {
    return formatPercent(value);
  }
  if (Math.abs(value) >= 1000) {
    return formatNumber(value, 0);
  }
  return formatNumber(value);
}

function deriveDatasetIds(detail: BacktestReportView) {
  const rawIds = detail.dataset_ids?.length ? detail.dataset_ids : detail.dataset_id ? [detail.dataset_id] : [];
  return Array.from(new Set(rawIds.map((item) => item.trim()).filter(Boolean)));
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

function metricLabel(key: string) {
  const labels: Record<string, string> = {
    annual_return: "年化收益",
    cumulative_return: "累计收益",
    max_drawdown: "最大回撤",
    turnover_total: "换手规模",
    implementation_shortfall: "实现短缺",
    sharpe: "夏普比率",
    information_ratio: "信息比率",
    worst_scenario_return_delta: "最差场景收益偏移",
    simulation_minus_research_cumulative_return: "研究 / 仿真偏差",
  };
  return labels[key] ?? key;
}

function gateStatusLabel(status: string | null | undefined) {
  const labels: Record<string, string> = {
    passed: "通过",
    warning: "需复核",
    failed: "未通过",
  };
  return labels[status ?? ""] ?? (status ?? "--");
}

function protocolLabel(key: string) {
  const labels: Record<string, string> = {
    train_dataset_window: "训练数据集窗口",
    lookback_window: "回看窗口",
    label_horizon: "标签跨度",
    modalities_and_fusion_summary: "模态 / 融合摘要",
    random_seed: "随机种子",
    tuning_trial_count: "调参轮数",
    external_pretraining_flag: "外部预训练",
    synthetic_data_flag: "合成数据",
    actual_market_dataset_window: "实际市场窗口",
    actual_official_backtest_window: "官方测试窗口",
    actual_nlp_coverage_window: "实际 NLP 覆盖 / 门禁",
    required_modalities_resolved: "要求模态",
    official_rolling_benchmark_version: "官方 benchmark 版本",
    official_rolling_window_size: "官方窗口档位",
    official_market_benchmark_dataset_id: "官方市场基准数据集",
    official_multimodal_benchmark_dataset_id: "官方多模态基准数据集",
  };
  return labels[key] ?? localizeBacktestMetadata(key);
}

function researchBackendLabel(value: string | null | undefined) {
  const labels: Record<string, string> = {
    native: "Native（仓内研究引擎）",
    vectorbt: "vectorbt（旁路矩阵研究）",
  };
  return labels[value ?? ""] ?? (value ?? "--");
}

function portfolioMethodLabel(value: string | null | undefined) {
  const labels: Record<string, string> = {
    proportional: "Proportional（现有信号归一）",
    skfolio_mean_risk: "skfolio Mean-Risk（可选优化）",
  };
  return labels[value ?? ""] ?? (value ?? "--");
}

function formatProtocolValue(key: string, value: string | null | undefined) {
  if (!value) {
    return "--";
  }
  if (key.endsWith("_time")) {
    return formatDate(value);
  }
  return localizeProtocolValue(value);
}

function templateRequirements(protocol: NonNullable<BacktestReportView["protocol"]>) {
  const items = [
    "时间窗对齐：所有非市场模态都必须绑定到同一官方滚动窗口。",
    "非市场模态统一挂到官方多模态 benchmark，不允许自由替换成其他窗口。",
    "如果包含 NLP 信号，则只允许归档型 NLP 数据源：news_archive、reddit_archive、gdelt。",
    "如果 NLP 质量门禁失败，官方 / 系统模板会被硬阻断。",
    protocol.template?.output_contract_version
      ? `模型输出必须遵守 ${protocol.template.output_contract_version}。`
      : null,
    protocol.template?.fixed_prediction_scope
      ? `预测范围固定为 ${localizeProtocolValue(protocol.template.fixed_prediction_scope)}。`
      : null,
    ...((protocol.template?.eligibility_rules ?? []).map(
      (item, index) =>
        `准入要求：${localizeBacktestRequirement(item, protocol.template?.eligibility_rule_keys?.[index])}`,
    )),
    ...((protocol.template?.required_metadata ?? []).map(
      (item, index) =>
        `必填披露：${localizeBacktestMetadata(item, protocol.template?.required_metadata_keys?.[index])}`,
    )),
    ...((protocol.template?.notes ?? []).map(
      (item, index) => `说明：${localizeBacktestNote(item, protocol.template?.note_keys?.[index])}`,
    )),
  ];
  return items.filter((item): item is string => Boolean(item));
}

function summarizeArtifacts(detailArtifacts: ArtifactView[], engineArtifacts: ArtifactView[] = []) {
  const seen = new Set<string>();
  return [...detailArtifacts, ...engineArtifacts].filter((artifact) => {
    if (seen.has(artifact.uri)) {
      return false;
    }
    seen.add(artifact.uri);
    return true;
  });
}

function summarizeWarnings(warnings: string[]) {
  const counter = new Map<string, number>();
  warnings.forEach((warning) => {
    const localized = localizeBacktestWarning(warning);
    counter.set(localized, (counter.get(localized) ?? 0) + 1);
  });
  return Array.from(counter.entries()).map(([warning, count]) => ({ warning, count }));
}

function summarizeProtocolFailures(protocol: BacktestReportView["protocol"] | null | undefined) {
  return (protocol?.protocol_gate_failures ?? []).map((item) => ({
    title: localizeBacktestGateLabel(item.label, item.label_key),
    reasons:
      item.reasons.length > 0
        ? item.reasons.map((reason, index) =>
            localizeBacktestGateDetail(reason, item.reason_keys?.[index] ?? null),
          )
        : [localizeBacktestGateDetail(null, null)],
  }));
}

function metricTiles(title: string, metrics: Record<string, number>) {
  const keys = [
    "annual_return",
    "cumulative_return",
    "max_drawdown",
    "turnover_total",
    "implementation_shortfall",
    "sharpe",
  ];
  return (
    <section className="panel">
      <PanelHeader eyebrow="引擎概览" title={title} />
      <div className="metric-grid detail-metric-grid">
        {keys.map((key) => (
          <div className="metric-tile" key={key}>
            <span>{metricLabel(key)}</span>
            <strong>{formatMetric(key, metrics[key])}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}

function comparisonOption(
  researchMetrics: Record<string, number>,
  simulationMetrics: Record<string, number>,
): EChartsOption {
  const keys = ["annual_return", "max_drawdown", "turnover_total", "implementation_shortfall"];
  return {
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#d6d2c4" } },
    grid: { left: 42, right: 20, top: 36, bottom: 36 },
    xAxis: {
      type: "category",
      data: keys.map((key) => metricLabel(key)),
      axisLabel: { color: "#b9b0a0" },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b9b0a0" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        name: "研究引擎",
        type: "bar",
        itemStyle: { color: "#8ad4ff" },
        data: keys.map((key) => researchMetrics[key] ?? 0),
      },
      {
        name: "仿真引擎",
        type: "bar",
        itemStyle: { color: "#c7ff73" },
        data: keys.map((key) => simulationMetrics[key] ?? 0),
      },
    ],
  };
}

function curveOption(points: TimeValuePoint[], title: string, color: string): EChartsOption {
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 42, right: 20, top: 30, bottom: 36 },
    xAxis: {
      type: "category",
      data: points.map((point) => formatDate(point.label)),
      axisLabel: { color: "#b9b0a0", hideOverlap: true },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b9b0a0" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        name: title,
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { color },
        areaStyle: { opacity: 0.12 },
        data: points.map((point) => point.value),
      },
    ],
  };
}

function scenarioOption(scenarios: ScenarioDeltaView[]): EChartsOption {
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 42, right: 20, top: 30, bottom: 56 },
    xAxis: {
      type: "category",
      data: scenarios.map((item) => item.scenario_name),
      axisLabel: { rotate: 24, color: "#b9b0a0" },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b9b0a0" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        type: "bar",
        itemStyle: { color: "#ffb479" },
        data: scenarios.map((item) => item.cumulative_return_delta),
      },
    ],
  };
}

function pnlOption(pnlSnapshot: Record<string, number>): EChartsOption {
  const entries = Object.entries(pnlSnapshot);
  return {
    tooltip: { trigger: "axis" },
    grid: { left: 42, right: 20, top: 30, bottom: 56 },
    xAxis: {
      type: "category",
      data: entries.map(([key]) => key),
      axisLabel: { rotate: 24, color: "#b9b0a0" },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#b9b0a0" },
      splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
    },
    series: [
      {
        type: "bar",
        itemStyle: { color: "#8ad4ff" },
        data: entries.map(([, value]) => value),
      },
    ],
  };
}

function flattenedDiagnostics(engine: BacktestEngineView | null) {
  if (!engine?.diagnostics || typeof engine.diagnostics !== "object") {
    return [];
  }
  const entries: Array<{ key: string; value: number }> = [];
  for (const [groupKey, groupValue] of Object.entries(engine.diagnostics)) {
    if (!groupValue || typeof groupValue !== "object") {
      continue;
    }
    for (const [metricKey, metricValue] of Object.entries(groupValue as Record<string, unknown>)) {
      if (typeof metricValue === "number") {
        entries.push({ key: `${groupKey}.${metricKey}`, value: metricValue });
      }
    }
  }
  return entries.slice(0, 12);
}

export function BacktestReportPage() {
  const { backtestId = "" } = useParams();
  const query = useBacktestDetail(backtestId);

  if (query.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (query.isError) {
    return <ErrorState message={(query.error as Error).message} />;
  }
  if (!query.data) {
    return <EmptyState title={I18N.state.empty} body="未找到对应的回测报告。" />;
  }

  const detail = mapBacktestDetail(query.data);
  const researchMetrics = detail.research?.metrics ?? {};
  const simulationMetrics = detail.simulation?.metrics ?? {};
  const protocol = detail.protocol;
  const protocolFailureMessages = summarizeProtocolFailures(protocol);
  const executionDiagnostics = detail.execution_diagnostics_summary;
  const signalCount = executionDiagnostics?.signal_count ?? null;
  const orderCount = executionDiagnostics?.order_count ?? null;
  const fillCount = executionDiagnostics?.fill_count ?? null;
  const noTradeOutcome =
    (signalCount ?? 0) > 0 && ((orderCount ?? 0) === 0 || (fillCount ?? 0) === 0);
  const protocolFailureText = protocolFailureMessages.flatMap((item) =>
    item.reasons.map((reason) => `${item.title}: ${reason}`),
  );
  const surfacedIssues = [
    ...protocolFailureText,
    ...(noTradeOutcome
      ? executionDiagnostics?.block_reasons?.length
        ? executionDiagnostics.block_reasons
        : ["这条回测产生了信号，但没有形成订单或成交，因此顶部指标不具备可解释性。"]
      : []),
  ];
  const simulationCurve = detail.simulation?.positions ?? [];
  const scenarioSeries = detail.simulation?.scenarios ?? [];
  const simulationArtifacts = detail.simulation?.artifacts ?? [];
  const allArtifacts = summarizeArtifacts(detail.artifacts, simulationArtifacts);
  const warningItems = [
    ...detail.comparison_warnings,
    ...(detail.simulation?.warnings ?? []),
    ...(detail.research?.warnings ?? []),
    ...surfacedIssues,
  ];
  const warningSummary = summarizeWarnings(warningItems);
  const protocolMetadata = Object.entries(protocol?.metadata_summary ?? {}).filter(
    ([, value]) => Boolean(value),
  );
  const requirementItems = protocol ? templateRequirements(protocol) : [];
  const diagnostics = flattenedDiagnostics(detail.simulation);
  const datasetIds = deriveDatasetIds(detail);
  const actualMarketWindow = formatWindow(
    protocol?.actual_market_start_time,
    protocol?.actual_market_end_time,
  );
  const officialRollingWindow = formatWindow(
    protocol?.official_window_start_time,
    protocol?.official_window_end_time,
  );
  const officialTestWindow = formatWindow(
    protocol?.actual_backtest_start_time,
    protocol?.actual_backtest_end_time,
  );
  const actualNlpWindow = formatWindow(
    protocol?.actual_nlp_start_time,
    protocol?.actual_nlp_end_time,
  );
  const requiredModalities = protocol?.required_modalities ?? [];
  const modalityQualitySummary = detail.modality_quality_summary ?? protocol?.modality_quality_summary ?? null;
  const qualityBlockingReasons =
    detail.quality_blocking_reasons?.length
      ? detail.quality_blocking_reasons
      : protocol?.quality_blocking_reasons ?? [];
  const dataQualityReady =
    modalityQualitySummary !== null &&
    Object.values(modalityQualitySummary).every((item) => item.status === "ready") &&
    qualityBlockingReasons.length === 0;
  const officialBenchmarkDatasetIds =
    protocol?.official_dataset_ids?.length
      ? protocol.official_dataset_ids
      : [protocol?.official_market_dataset_id, protocol?.official_multimodal_dataset_id].filter(
          (item): item is string => Boolean(item),
        );
  const isOfficialTemplate = Boolean(protocol?.template?.official);
  const isOfficialResultInvalid = isOfficialTemplate && protocol?.gate_status === "failed";
  const headlineStatusLabel = isOfficialTemplate
    ? isOfficialResultInvalid
      ? "不可用于官方比较"
      : protocol?.gate_status === "warning"
        ? "需复核"
        : "可用于官方比较"
    : detail.passed_consistency_checks === null
      ? "--"
      : detail.passed_consistency_checks
        ? "通过"
        : "未通过";

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.backtests}
          title={detail.backtest_id}
          description="这里汇总展示研究结果与仿真结果，方便在同一处核对系统模板、实际市场窗口、官方测试窗口和多模态对齐情况。"
        />
        {isOfficialTemplate && (isOfficialResultInvalid || noTradeOutcome) ? (
          <div
            className={`backtest-callout ${isOfficialResultInvalid ? "backtest-callout-danger" : "backtest-callout-warning"}`}
          >
            <strong>
              {isOfficialResultInvalid
                ? "该结果不可用于官方比较"
                : "该结果需要复核"}
            </strong>
            <span>
              {isOfficialResultInvalid
                ? dataQualityReady
                  ? "数据质量已达标，但协议资格没有通过。这条历史回测记录只能作为排错样本，不能当作有效官方结果使用。"
                  : "这条历史回测记录同时存在协议资格或数据质量问题，不能当作有效官方结果使用。"
                : "这条回测没有形成实际成交，页面中的收益和风险指标不具备解释价值。"}
            </span>
            {surfacedIssues.length > 0 ? (
              <div className="stack-list">
                {surfacedIssues.map((item) => (
                  <div className="stack-item align-start" key={item}>
                    <strong>复核原因</strong>
                    <span>{item}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
          <div className="metric-grid detail-metric-grid">
            <div className="metric-tile">
              <span>训练实例</span>
            <strong>
              {detail.run_id ? (
                <Link to={`/runs/${encodeURIComponent(detail.run_id)}`}>{detail.run_id}</Link>
              ) : (
                "--"
              )}
            </strong>
          </div>
          <div className="metric-tile">
            <span>模型</span>
            <strong>{detail.model_name ?? "--"}</strong>
            </div>
            <div className="metric-tile">
              <span>{isOfficialTemplate ? "结果有效性" : "一致性检查"}</span>
              <strong>{headlineStatusLabel}</strong>
            </div>
            <div className="metric-tile">
              <span>数据质量</span>
              <strong>{dataQualityReady ? "合格" : "需复核"}</strong>
            </div>
            <div className="metric-tile">
              <span>{isOfficialTemplate ? "复核项" : "告警数"}</span>
              <strong>{isOfficialTemplate ? surfacedIssues.length : warningItems.length}</strong>
            </div>
          </div>
        <div className="stack-list">
          <div className="stack-item align-start">
            <strong>关联数据集</strong>
            <span className="table-title-cell">
              {datasetIds.length > 0
                ? datasetIds.map((datasetId) => (
                    <Link key={datasetId} to={`/datasets/${encodeURIComponent(datasetId)}`}>
                      {datasetId}
                    </Link>
                  ))
                : "--"}
            </span>
          </div>
          <div className="stack-item align-start">
            <strong>研究后端</strong>
            <span>{researchBackendLabel(detail.research_backend)}</span>
          </div>
          <div className="stack-item align-start">
            <strong>组合构建</strong>
            <span>{portfolioMethodLabel(detail.portfolio_method)}</span>
          </div>
        </div>
      </section>

      {metricTiles("研究引擎", researchMetrics)}
      {metricTiles("仿真引擎", simulationMetrics)}

      {protocol ? (
        <section className="panel">
          <PanelHeader
            eyebrow={protocol.template?.official ? "官方协议" : "回测协议"}
            title={localizeBacktestTemplateName(protocol.template?.name, protocol.template?.template_id)}
            description="官方 / 系统模板要求按同一滚动窗口对齐多模态信号；若包含 NLP，还必须通过归档源与质量门禁。"
            action={
              protocol.template?.template_id ? (
                <Link
                  className="link-button"
                  to={`/comparison?official_only=${protocol.template.official ? "1" : "0"}&template_id=${encodeURIComponent(protocol.template.template_id)}${detail.run_id ? `&runs=${encodeURIComponent(detail.run_id)}` : ""}`}
                >
                  查看同模板对比
                </Link>
              ) : undefined
            }
          />
          <div className="metric-grid detail-metric-grid">
            <div className="metric-tile">
              <span>模板 ID</span>
              <strong>{protocol.template?.template_id ?? "--"}</strong>
            </div>
            <div className="metric-tile">
              <span>协议版本</span>
              <strong>{protocol.template?.protocol_version ?? detail.protocol_version ?? "--"}</strong>
            </div>
            <div className="metric-tile">
              <span>协议门禁</span>
              <strong>{gateStatusLabel(protocol.gate_status)}</strong>
            </div>
            <div className="metric-tile">
              <span>NLP 质量门禁</span>
              <strong>{gateStatusLabel(protocol.nlp_gate_status)}</strong>
            </div>
          </div>

          {protocol.template?.official ? (
            <section className="panel">
              <PanelHeader
                eyebrow="官方基准"
                title="官方滚动 benchmark"
                description="这里明确展示当前回测实际绑定的官方 benchmark 版本、窗口档位与官方数据集。官方排名只在同版本、同窗口内比较。"
              />
              <div className="stack-list">
                <div className="stack-item align-start">
                  <strong>官方 benchmark 版本</strong>
                  <span>{protocol.official_benchmark_version ?? "--"}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方窗口档位</strong>
                  <span>
                    {protocol.official_window_days ? `${protocol.official_window_days} 天` : "--"}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方滚动窗口</strong>
                  <span>{officialRollingWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方市场数据集 ID</strong>
                  <span className="table-title-cell">
                    {protocol.official_market_dataset_id ? (
                      <Link
                        to={`/datasets/${encodeURIComponent(protocol.official_market_dataset_id)}`}
                      >
                        {protocol.official_market_dataset_id}
                      </Link>
                    ) : (
                      "--"
                    )}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>要求模态</strong>
                  <span>
                    {requiredModalities.length > 0
                      ? requiredModalities.map((modality) => formatModalityLabel(modality)).join(" / ")
                      : "--"}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方 benchmark 数据集</strong>
                  <span className="table-title-cell">
                    {officialBenchmarkDatasetIds.length > 0
                      ? officialBenchmarkDatasetIds.map((datasetId) => (
                          <Link key={datasetId} to={`/datasets/${encodeURIComponent(datasetId)}`}>
                            {datasetId}
                          </Link>
                        ))
                      : "--"}
                  </span>
                </div>
                {qualityBlockingReasons.length > 0 ? (
                  <div className="stack-item align-start">
                    <strong>质量阻断原因</strong>
                    <span>{qualityBlockingReasons.join(" / ")}</span>
                  </div>
                ) : null}
              </div>
            </section>
          ) : null}

          {modalityQualitySummary ? (
            <section className="panel">
              <PanelHeader
                eyebrow="模态质量"
                title={dataQualityReady ? "官方模态质量摘要（已通过）" : "官方模态质量摘要"}
                description="这里展示进入官方回测前的五模态质量结论，便于确认哪些模态通过了正式门禁。"
              />
              <ModalityQualitySummary
                modalities={requiredModalities.length > 0 ? requiredModalities : undefined}
                summary={modalityQualitySummary}
                title="Backtest modality quality summary"
              />
            </section>
          ) : null}

          {detail.alignment ? (
            <section className="panel">
              <PanelHeader
                eyebrow="模态对齐"
                title="实际回测使用的数据与对齐方式"
                description="这里展示本次回测实际绑定的数据集、模态和对齐策略，方便确认是否真的走到了五模态路径。"
              />
              <div className="stack-list">
                <div className="stack-item align-start">
                  <strong>对齐状态</strong>
                  <span>{detail.alignment.alignment_status ?? "--"}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>融合策略</strong>
                  <span>{detail.alignment.fusion_strategy ?? "--"}</span>
                </div>
                {(detail.alignment.datasets ?? []).map((dataset) => (
                  <div className="stack-item align-start" key={dataset.dataset_id}>
                    <strong>{dataset.role ?? "dataset"}</strong>
                    <span className="table-title-cell">
                      <Link to={`/datasets/${encodeURIComponent(dataset.dataset_id)}`}>
                        {dataset.dataset_id}
                      </Link>
                    </span>
                    <span>{`模态：${formatModalityLabel(dataset.modality)}`}</span>
                  </div>
                ))}
                {(detail.alignment.notes ?? []).map((note) => (
                  <div className="stack-item align-start" key={note}>
                    <strong>对齐说明</strong>
                    <span>{note}</span>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          <div className="detail-grid wide-secondary">
            <section className="panel">
              <PanelHeader
                eyebrow="实际时间窗"
                title="官方模板实际时间范围"
                description="这里展示的是官方 / 系统模板真实使用的时间窗，不再从训练开始或结束时间推导。"
              />
              <div className="stack-list">
                <div className="stack-item align-start">
                  <strong>实际市场窗口</strong>
                  <span>{actualMarketWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方测试窗口</strong>
                  <span>{officialTestWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>实际 NLP 覆盖窗口</strong>
                  <span>{actualNlpWindow}</span>
                </div>
              </div>
            </section>

            <section className="panel">
              <PanelHeader
                eyebrow="模板规则"
                title="使用此模板必须满足的要求"
                description="只有满足这些条件，结果才可参与官方同模板对比。"
              />
              <div className="stack-list">
                {requirementItems.map((item) => (
                  <div className="stack-item align-start" key={item}>
                    <strong>规则</strong>
                    <span>{item}</span>
                  </div>
                ))}
                {protocol.nlp_gate_reasons?.map((reason) => (
                  <div className="stack-item align-start" key={reason}>
                    <strong>NLP 门禁说明</strong>
                    <span>{localizeBacktestGateReason(reason, detail.protocol?.nlp_gate_reason_keys?.[detail.protocol?.nlp_gate_reasons?.indexOf(reason) ?? -1] ?? null)}</span>
                  </div>
                ))}
              </div>
            </section>
          </div>

          <div className="detail-grid wide-secondary">
            <section className="panel">
              <PanelHeader eyebrow="门禁结果" title="协议检查项" />
              <div className="stack-list">
                {protocol.gate_results.map((item) => (
                  <div className="stack-item align-start" key={item.key}>
                    <strong>
                      {`${localizeBacktestGateLabel(item.label, item.label_key)} / ${gateStatusLabel(
                        item.passed === null ? "warning" : item.passed ? "passed" : "failed",
                      )}`}
                    </strong>
                    <span>{localizeBacktestGateDetail(item.detail, item.detail_key)}</span>
                  </div>
                ))}
              </div>
            </section>

            <section className="panel">
              <PanelHeader eyebrow="协议失败" title="协议失败原因" />
              {protocolFailureMessages.length > 0 ? (
                <div className="stack-list">
                  {protocolFailureMessages.map((item) => (
                    <div className="stack-item align-start" key={item.title}>
                      <strong>{item.title}</strong>
                      <span>{item.reasons.join(" / ")}</span>
                    </div>
                  ))}
                  {protocol.missing_required_metadata_labels?.length ? (
                    <div className="stack-item align-start">
                      <strong>缺失披露字段</strong>
                      <span>{protocol.missing_required_metadata_labels.join(" / ")}</span>
                    </div>
                  ) : null}
                  {protocol.missing_stress_scenarios?.length ? (
                    <div className="stack-item align-start">
                      <strong>缺失压力场景</strong>
                      <span>{protocol.missing_stress_scenarios.join(" / ")}</span>
                    </div>
                  ) : null}
                </div>
              ) : (
                <EmptyState title="协议门禁已通过" body="当前没有协议资格失败项。" />
              )}
            </section>
          </div>

          <div className="detail-grid wide-secondary">
            <section className="panel">
              <PanelHeader eyebrow="元数据" title="协议元数据" />
              {protocolMetadata.length > 0 ? (
                <div className="stack-list">
                  {protocolMetadata.map(([key, value]) => (
                    <div className="stack-item" key={`metadata-${key}`}>
                      <strong>{protocolLabel(key)}</strong>
                      <span>{formatProtocolValue(key, value)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState title="暂无元数据" body="当前没有可展示的协议元数据摘要。" />
              )}
            </section>
            <section className="panel">
              <PanelHeader eyebrow="执行诊断" title="信号到订单链路" />
              {executionDiagnostics ? (
                <div className="stack-list">
                  <div className="stack-item">
                    <strong>signal_count</strong>
                    <span>{formatMetric("signal_count", executionDiagnostics.signal_count)}</span>
                  </div>
                  <div className="stack-item">
                    <strong>eligible_order_count</strong>
                    <span>{formatMetric("eligible_order_count", executionDiagnostics.eligible_order_count)}</span>
                  </div>
                  <div className="stack-item">
                    <strong>blocked_order_count</strong>
                    <span>{formatMetric("blocked_order_count", executionDiagnostics.blocked_order_count)}</span>
                  </div>
                  <div className="stack-item">
                    <strong>order_count</strong>
                    <span>{formatMetric("order_count", executionDiagnostics.order_count)}</span>
                  </div>
                  <div className="stack-item">
                    <strong>fill_count</strong>
                    <span>{formatMetric("fill_count", executionDiagnostics.fill_count)}</span>
                  </div>
                  <div className="stack-item">
                    <strong>position_open_count</strong>
                    <span>{formatMetric("position_open_count", executionDiagnostics.position_open_count)}</span>
                  </div>
                  {executionDiagnostics.block_reasons.map((reason) => (
                    <div className="stack-item align-start" key={reason}>
                      <strong>阻断原因</strong>
                      <span>{reason}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState title="暂无执行诊断" body="当前没有额外的信号到订单诊断摘要。" />
              )}
            </section>
          </div>

          <section className="panel">
            <PanelHeader eyebrow="排序输入" title="排序组件与切片范围" />
            <div className="stack-list">
              {protocol.rank_components.map((item) => (
                <div className="stack-item" key={item.key}>
                  <strong>{metricLabel(item.key)}</strong>
                  <span>{formatMetric(item.key, item.value)}</span>
                </div>
              ))}
              {protocol.slice_coverage.length > 0 ? (
                <div className="stack-item align-start">
                  <strong>切片覆盖范围</strong>
                  <span>{protocol.slice_coverage.join(" / ")}</span>
                </div>
              ) : null}
            </div>
          </section>
        </section>
      ) : null}

      <div className="detail-grid wide-secondary">
        <section className="panel">
              <PanelHeader eyebrow="对比" title="研究引擎与仿真引擎" />
          <WorkbenchChart option={comparisonOption(researchMetrics, simulationMetrics)} />
        </section>

        <section className="panel">
          <PanelHeader eyebrow="权益曲线" title="仿真权益曲线" />
          {simulationCurve.length > 0 ? (
            <WorkbenchChart option={curveOption(simulationCurve, "权益曲线", "#c7ff73")} />
          ) : (
            <EmptyState
              title="暂无权益曲线"
              body="仿真引擎没有返回可持久化的权益快照。"
            />
          )}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="盈亏拆解" title="仿真盈亏分解" />
          {Object.keys(detail.simulation?.pnl_snapshot ?? {}).length > 0 ? (
            <WorkbenchChart option={pnlOption(detail.simulation?.pnl_snapshot ?? {})} />
          ) : (
            <EmptyState
              title="暂无盈亏拆解"
              body="当前回测没有可展示的盈亏归因数据。"
            />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow="压力场景" title="场景收益偏移" />
          {scenarioSeries.length > 0 ? (
            <WorkbenchChart option={scenarioOption(scenarioSeries)} />
          ) : (
            <EmptyState
              title="暂无压力场景"
              body="当前回测没有可用的场景偏移结果。"
            />
          )}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow="诊断" title="仿真诊断指标" />
        {diagnostics.length > 0 ? (
          <div className="stack-list">
            {diagnostics.map((item) => (
              <div className="stack-item" key={item.key}>
                <strong>{item.key}</strong>
                <span>{formatMetric(item.key, item.value)}</span>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState
            title="暂无诊断数据"
            body="仿真引擎没有返回分组诊断指标。"
          />
        )}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="告警" title="告警与审计信号" />
        {warningSummary.length > 0 ? (
          <div className="stack-list">
            {warningSummary.map((item) => (
              <div className="stack-item align-start" key={item.warning}>
                <strong>{item.warning}</strong>
                <span>{`出现 ${item.count} 次`}</span>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState
            title="暂无告警"
            body="当前回测没有对比告警或审计异常。"
          />
        )}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="工件" title="回测工件" />
        {allArtifacts.length > 0 ? (
          <div className="stack-list">
            {allArtifacts.map((artifact) => (
              <div className="stack-item" key={artifact.uri}>
                <strong>{formatArtifactLabel(artifact.kind, artifact.label)}</strong>
                <span>{artifact.uri}</span>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState title="暂无工件" body="当前回测没有暴露可持久化工件。" />
        )}
      </section>
    </div>
  );
}

