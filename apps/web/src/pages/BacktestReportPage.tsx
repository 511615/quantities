import type { EChartsOption } from "echarts";
import { useParams } from "react-router-dom";

import { useBacktestDetail } from "../shared/api/hooks";
import type { ArtifactView, BacktestEngineView, ScenarioDeltaView, TimeValuePoint } from "../shared/api/types";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatArtifactLabel } from "../shared/lib/labels";
import { mapBacktestDetail } from "../shared/view-model/mappers";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";

const PERCENT_KEYS = new Set([
  "annual_return",
  "max_drawdown",
  "cumulative_return",
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

function labelMetric(key: string) {
  const labels: Record<string, string> = {
    annual_return: "年化收益",
    cumulative_return: "累计收益",
    max_drawdown: "最大回撤",
    turnover_total: "总换手",
    implementation_shortfall: "实施短缺",
    sharpe: "夏普比率",
    alpha: "Alpha",
    beta: "Beta",
    annual_volatility: "年化波动",
    average_drawdown: "平均回撤",
    calmar: "Calmar",
    information_ratio: "信息比率",
    sortino: "Sortino",
    up_capture: "上涨捕获",
    down_capture: "下跌捕获",
    var_95: "VaR 95",
    cvar_95: "CVaR 95",
    average_fee_bps: "平均手续费 bps",
    average_slippage_bps: "平均滑点 bps",
    fill_count: "成交次数",
    fill_rate: "成交率",
    order_count: "订单数",
    partial_fill_rate: "部分成交率",
    rejection_rate: "拒单率",
    concentration_hhi: "集中度 HHI",
    drawdown: "当前回撤",
    gross_exposure: "总敞口",
    gross_leverage: "总杠杆",
    long_exposure: "多头敞口",
    short_exposure: "空头敞口",
    maintenance_margin: "维持保证金",
    margin_used: "已用保证金",
    max_drawdown_seen: "历史最大回撤",
    max_gross_leverage_seen: "历史最大杠杆",
    net_exposure: "净敞口",
    net_leverage: "净杠杆",
    position_count: "持仓数量",
    risk_trigger_count: "风控触发次数",
    average_signal: "平均信号",
    hit_rate: "命中率",
    profit_factor: "盈亏比",
    signal_autocorrelation: "信号自相关",
    signal_count: "信号数",
  };
  return labels[key] ?? key;
}

function labelGroup(key: string) {
  const labels: Record<string, string> = {
    performance_metrics: "表现指标",
    execution_metrics: "执行指标",
    risk_metrics: "风险指标",
    signal_metrics: "信号指标",
  };
  return labels[key] ?? key;
}

function labelScenario(key: string) {
  const labels: Record<string, string> = {
    cost_x2: "成本翻倍",
    cost_x5: "成本五倍",
    latency_shock: "延迟冲击",
    liquidity_drought: "流动性枯竭",
  };
  return labels[key] ?? key;
}

function labelPnlKey(key: string) {
  const labels: Record<string, string> = {
    alpha_pnl: "Alpha PnL",
    beta_or_benchmark_pnl: "Beta/基准 PnL",
    borrow_cost: "借贷成本",
    cash_pnl: "现金 PnL",
    funding_pnl: "资金费率",
    trading_cost: "交易成本",
  };
  return labels[key] ?? key;
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
    counter.set(warning, (counter.get(warning) ?? 0) + 1);
  });
  return Array.from(counter.entries()).map(([warning, count]) => ({ warning, count }));
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
      <PanelHeader eyebrow="引擎摘要" title={title} />
      <div className="metric-grid detail-metric-grid">
        {keys.map((key) => (
          <div className="metric-tile" key={key}>
            <span>{labelMetric(key)}</span>
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
      data: keys.map((key) => labelMetric(key)),
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
      data: scenarios.map((item) => labelScenario(item.scenario_name)),
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
      data: entries.map(([key]) => labelPnlKey(key)),
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

function diagnosticSections(engine: BacktestEngineView | null) {
  if (!engine) {
    return <EmptyState title="暂无诊断信息" body="当前回测没有返回诊断指标。" />;
  }
  const diagnostics = engine.diagnostics ?? {};
  const groups = ["performance_metrics", "execution_metrics", "risk_metrics", "signal_metrics"];
  return (
    <div className="detail-grid wide-secondary">
      {groups.map((group) => {
        const record = diagnostics[group];
        const items = record && typeof record === "object" ? Object.entries(record as Record<string, number>) : [];
        return (
          <section className="panel" key={group}>
            <PanelHeader eyebrow="诊断信息" title={labelGroup(group)} />
            {items.length > 0 ? (
              <div className="stack-list">
                {items.slice(0, 8).map(([key, value]) => (
                  <div className="stack-item" key={key}>
                    <strong>{labelMetric(key)}</strong>
                    <span>{formatMetric(key, typeof value === "number" ? value : Number(value))}</span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState title="暂无数据" body="当前诊断分组为空。" />
            )}
          </section>
        );
      })}
    </div>
  );
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
    return <EmptyState body="没有找到对应的回测报告。" title={I18N.state.empty} />;
  }

  const detail = mapBacktestDetail(query.data);
  const researchMetrics = detail.research?.metrics ?? {};
  const simulationMetrics = detail.simulation?.metrics ?? {};
  const simulationCurve = detail.simulation?.positions ?? [];
  const scenarioSeries = detail.simulation?.scenarios ?? [];
  const simulationArtifacts = detail.simulation?.artifacts ?? [];
  const allArtifacts = summarizeArtifacts(detail.artifacts, simulationArtifacts);
  const warningItems = [
    ...detail.comparison_warnings,
    ...(detail.simulation?.warnings ?? []),
    ...(detail.research?.warnings ?? []),
  ];
  const warningSummary = summarizeWarnings(warningItems);

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.backtests}
          title={detail.backtest_id}
          description="这里把研究引擎和仿真引擎的结果汇总到同一页，便于对照收益、风险、执行拖累、权益曲线、压力场景与泄漏审计。"
        />
        <div className="metric-grid detail-metric-grid">
          <div className="metric-tile">
            <span>训练运行</span>
            <strong>{detail.run_id ?? "--"}</strong>
          </div>
          <div className="metric-tile">
            <span>模型</span>
            <strong>{detail.model_name ?? "--"}</strong>
          </div>
          <div className="metric-tile">
            <span>一致性检查</span>
            <strong>
              {detail.passed_consistency_checks === null
                ? "--"
                : detail.passed_consistency_checks
                  ? "通过"
                  : "失败"}
            </strong>
          </div>
          <div className="metric-tile">
            <span>告警数</span>
            <strong>{warningItems.length}</strong>
          </div>
        </div>
      </section>

      {metricTiles("研究引擎", researchMetrics)}
      {metricTiles("仿真引擎", simulationMetrics)}

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="对比" title="研究 vs 仿真" />
          <WorkbenchChart option={comparisonOption(researchMetrics, simulationMetrics)} />
        </section>

        <section className="panel">
          <PanelHeader eyebrow="资金路径" title="仿真权益曲线" />
          {simulationCurve.length > 0 ? (
            <WorkbenchChart option={curveOption(simulationCurve, "权益", "#c7ff73")} />
          ) : (
            <EmptyState title="暂无权益曲线" body="仿真引擎没有持久化组合快照。" />
          )}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow="PnL" title="仿真收益分解" />
          {Object.keys(detail.simulation?.pnl_snapshot ?? {}).length > 0 ? (
            <WorkbenchChart option={pnlOption(detail.simulation?.pnl_snapshot ?? {})} />
          ) : (
            <EmptyState title="暂无 PnL 分解" body="当前回测没有可展示的收益拆分。" />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow="压力测试" title="情景收益变化" />
          {scenarioSeries.length > 0 ? (
            <WorkbenchChart option={scenarioOption(scenarioSeries)} />
          ) : (
            <EmptyState title="暂无情景测试" body="当前回测没有可用的压力场景结果。" />
          )}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow="告警与审计" title="回测警告和泄漏检查" />
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
          <EmptyState title="没有警告" body="当前回测没有对比告警或泄漏审计告警。" />
        )}
      </section>

      {diagnosticSections(detail.simulation)}

      <section className="panel">
        <PanelHeader eyebrow="工件" title="回测产物" />
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
          <EmptyState title="暂无工件" body="当前回测没有持久化产物。" />
        )}
      </section>
    </div>
  );
}
