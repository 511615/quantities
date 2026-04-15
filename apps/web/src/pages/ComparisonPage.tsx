import { Suspense, lazy, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useComparison } from "../shared/api/hooks";
import { formatNumber } from "../shared/lib/format";
import { I18N, translateText } from "../shared/lib/i18n";
import { formatSourceTypeLabel } from "../shared/lib/labels";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

const ComparisonScatterChart = lazy(
  async () => ({ default: (await import("../features/model-comparison/ComparisonScatterChart")).ComparisonScatterChart }),
);
const ComparisonRiskBarChart = lazy(
  async () => ({ default: (await import("../features/model-comparison/ComparisonRiskBarChart")).ComparisonRiskBarChart }),
);

export function ComparisonPage() {
  const [searchParams] = useSearchParams();
  const runIds = useMemo(() => searchParams.get("runs")?.split(",").filter(Boolean) ?? [], [searchParams]);
  const templateId = searchParams.get("template_id") ?? undefined;
  const officialOnly = searchParams.get("official_only") === "1";
  const benchmarkSelections = useMemo(() => {
    const benchmarkName = searchParams.get("benchmark");
    return benchmarkName ? [{ benchmark_name: benchmarkName, model_names: [] }] : [];
  }, [searchParams]);
  const query = useComparison({
    runIds,
    benchmarkSelections,
    templateId,
    officialOnly,
  });

  if (query.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (query.isError) {
    return <ErrorState message={(query.error as Error).message} />;
  }
  if (!query.data || query.data.rows.length === 0) {
    return (
      <EmptyState
        title={I18N.state.empty}
        body={
          officialOnly
            ? translateText("当前已切换到官方模板过滤视图。请从回测详情页的“查看同模板对比”进入，或在 URL 中附带运行参数。")
            : translateText("请先从运行页勾选多个训练实例进入对比，或传入基准参数。")
        }
      />
    );
  }

  const rows = query.data.rows;

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.comparison}
          title={translateText("模型性能对比")}
          description={
            officialOnly
              ? translateText("当前仅展示官方模板下的回测结果，便于在相同标准下比较。")
              : translateText("统一展示训练指标、基准表现和回测结果。")
          }
        />
      </section>

      <div className="detail-grid">
        <section className="panel">
          <PanelHeader eyebrow={translateText("散点图")} title={translateText("测试集 MAE 与年化收益")} />
          <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
            <ComparisonScatterChart rows={rows} />
          </Suspense>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={translateText("条形图")} title={translateText("回撤与换手")} />
          <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
            <ComparisonRiskBarChart rows={rows} />
          </Suspense>
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={translateText("表格")} title={translateText("对比结果")} />
        <table className="data-table">
          <thead>
            <tr>
              <th>{translateText("标签")}</th>
              <th>{translateText("来源")}</th>
              <th>{I18N.nav.runs}</th>
              <th>{translateText("模板")}</th>
              <th>{translateText("门禁")}</th>
              <th>{translateText("训练 MAE")}</th>
              <th>{translateText("验证 MAE")}</th>
              <th>{translateText("测试 MAE")}</th>
              <th>{translateText("年化收益")}</th>
              <th><GlossaryHint hintKey="max_drawdown" /></th>
              <th><GlossaryHint hintKey="turnover" /></th>
              <th><GlossaryHint hintKey="implementation_shortfall" /></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.row_id}>
                <td>{row.label}</td>
                <td>{formatSourceTypeLabel(row.source_type)}</td>
                <td>{row.model_name}</td>
                <td>{row.template_id ?? (row.official ? translateText("官方模板") : "--")}</td>
                <td>{row.gate_status ?? "--"}</td>
                <td>{formatNumber(row.train_mae)}</td>
                <td>{formatNumber(row.mean_valid_mae)}</td>
                <td>{formatNumber(row.mean_test_mae)}</td>
                <td>{formatNumber(row.annual_return)}</td>
                <td>{formatNumber(row.max_drawdown)}</td>
                <td>{formatNumber(row.turnover_total)}</td>
                <td>{formatNumber(row.implementation_shortfall)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
