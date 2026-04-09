import { Suspense, lazy } from "react";
import { useParams } from "react-router-dom";

import { useBacktestDetail } from "../shared/api/hooks";
import { formatNumber, formatPercent } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatArtifactLabel } from "../shared/lib/labels";
import { mapBacktestDetail } from "../shared/view-model/mappers";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { WorkbenchChart } from "../shared/ui/WorkbenchChart";

const BacktestPerformanceChart = lazy(
  async () =>
    ({
      default: (await import("../features/backtest-report/BacktestPerformanceChart"))
        .BacktestPerformanceChart,
    }),
);
const BacktestScenarioChart = lazy(
  async () =>
    ({
      default: (await import("../features/backtest-report/BacktestScenarioChart"))
        .BacktestScenarioChart,
    }),
);

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
    return <EmptyState body={"\u5f53\u524d backtest_id \u6ca1\u6709\u5bf9\u5e94\u7684\u62a5\u544a\u3002"} title={I18N.state.empty} />;
  }

  const detail = mapBacktestDetail(query.data);
  const researchMetrics = detail.research?.metrics ?? {};
  const simulationMetrics = detail.simulation?.metrics ?? {};

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.backtests}
          title={detail.backtest_id}
          description={"\u66f4\u6e05\u6670\u5730\u6309\u6536\u76ca\u3001\u98ce\u9669\u3001\u6267\u884c\u4e0e\u4e00\u81f4\u6027\u5206\u7ec4\u67e5\u770b\u56de\u6d4b\u7ed3\u679c\u3002"}
        />
        <div className="metric-grid detail-metric-grid">
          <div className="metric-tile">
            <span>{"\u5173\u8054\u8bad\u7ec3\u5b9e\u4f8b"}</span>
            <strong>{detail.run_id ?? "--"}</strong>
          </div>
          <div className="metric-tile">
            <span>{"\u6a21\u578b"}</span>
            <strong>{detail.model_name ?? "--"}</strong>
          </div>
          <div className="metric-tile">
            <span>
              <GlossaryHint hintKey="consistency_check" />
            </span>
            <strong>
              {detail.passed_consistency_checks === null
                ? "--"
                : detail.passed_consistency_checks
                  ? "\u901a\u8fc7"
                  : "\u672a\u901a\u8fc7"}
            </strong>
          </div>
        </div>
      </section>

      <div className="three-column-grid">
        <section className="panel">
          <PanelHeader eyebrow={"\u6536\u76ca"} title={"\u6536\u76ca\u6307\u6807"} />
          <div className="stack-list">
            <div className="stack-item">
              <strong>{"\u7814\u7a76\u5f15\u64ce\u5e74\u5316\u6536\u76ca"}</strong>
              <span>{formatPercent(researchMetrics.annual_return)}</span>
            </div>
            <div className="stack-item">
              <strong>{"\u6a21\u62df\u5f15\u64ce\u5e74\u5316\u6536\u76ca"}</strong>
              <span>{formatPercent(simulationMetrics.annual_return)}</span>
            </div>
            <div className="stack-item">
              <strong>{"PnL"}</strong>
              <span>{formatNumber(simulationMetrics.cumulative_return)}</span>
            </div>
          </div>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u98ce\u9669"} title={"\u98ce\u9669\u6307\u6807"} />
          <div className="stack-list">
            <div className="stack-item">
              <strong>
                <GlossaryHint hintKey="max_drawdown" />
              </strong>
              <span>{formatPercent(simulationMetrics.max_drawdown)}</span>
            </div>
            <div className="stack-item">
              <strong>
                <GlossaryHint hintKey="turnover" />
              </strong>
              <span>{formatNumber(simulationMetrics.turnover_total)}</span>
            </div>
          </div>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u6267\u884c"} title={"\u6267\u884c\u6307\u6807"} />
          <div className="stack-list">
            <div className="stack-item">
              <strong>
                <GlossaryHint hintKey="implementation_shortfall" />
              </strong>
              <span>{formatNumber(simulationMetrics.implementation_shortfall)}</span>
            </div>
            <div className="stack-item">
              <strong>{"\u544a\u8b66\u6570"}</strong>
              <span>{detail.comparison_warnings.length}</span>
            </div>
          </div>
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow={"\u53cc\u5f15\u64ce\u5bf9\u7167"} title={"\u5173\u952e\u6307\u6807\u5bf9\u6bd4"} />
          <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
            <BacktestPerformanceChart detail={detail} />
          </Suspense>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u6301\u4ed3\u8f68\u8ff9"} title={"\u6301\u4ed3\u8f68\u8ff9"} />
          {(detail.simulation?.positions ?? []).length > 0 ? (
            <WorkbenchChart
              loadingLabel={I18N.state.loading}
              option={{
                tooltip: { trigger: "axis" },
                xAxis: {
                  type: "category",
                  data: (detail.simulation?.positions ?? []).map((point) => point.label),
                  axisLabel: { color: "#b8b09e" },
                },
                yAxis: {
                  type: "value",
                  axisLabel: { color: "#b8b09e" },
                  splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
                },
                series: [
                  {
                    type: "line",
                    smooth: true,
                    data: (detail.simulation?.positions ?? []).map((point) => point.value),
                    areaStyle: { opacity: 0.14 },
                    lineStyle: { color: "#c7ff73" },
                  },
                ],
              }}
              style={{ height: 320 }}
            />
          ) : (
            <EmptyState body={"\u6a21\u62df\u5f15\u64ce\u6682\u65e0\u6301\u4ed3\u8f68\u8ff9\u6570\u636e\u3002"} title={I18N.state.empty} />
          )}
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow={"\u544a\u8b66\u4e0e\u4e00\u81f4\u6027"} title={"\u8bca\u65ad\u4e0e\u544a\u8b66"} />
          {detail.comparison_warnings.length > 0 ? (
            <div className="stack-list">
              {detail.comparison_warnings.map((warning) => (
                <div className="stack-item align-start" key={warning}>
                  <strong>{warning}</strong>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState body={"\u5f53\u524d\u56de\u6d4b\u6ca1\u6709 warning\u3002"} title={"\u65e0\u544a\u8b66"} />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u538b\u529b\u573a\u666f"} title={"\u573a\u666f\u51b2\u51fb"} />
          {Object.keys(detail.scenario_metrics).length > 0 ? (
            <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
              <BacktestScenarioChart detail={detail} />
            </Suspense>
          ) : (
            <EmptyState body={"\u5f53\u524d\u56de\u6d4b\u6ca1\u6709\u573a\u666f\u6c47\u603b\u6570\u636e\u3002"} title={I18N.state.empty} />
          )}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={"\u56de\u6d4b\u4ea7\u7269"} title={"\u56de\u6d4b\u4ea7\u7269"} />
        <div className="stack-list">
          {detail.artifacts.map((artifact) => (
            <div className="stack-item" key={artifact.uri}>
              <strong>{formatArtifactLabel(artifact.kind, artifact.label)}</strong>
              <span>{artifact.uri}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
