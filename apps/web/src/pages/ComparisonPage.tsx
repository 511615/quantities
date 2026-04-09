import { Suspense, lazy, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useComparison } from "../shared/api/hooks";
import { formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatSourceTypeLabel } from "../shared/lib/labels";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

const ComparisonScatterChart = lazy(
  async () =>
    ({ default: (await import("../features/model-comparison/ComparisonScatterChart")).ComparisonScatterChart }),
);
const ComparisonRiskBarChart = lazy(
  async () =>
    ({ default: (await import("../features/model-comparison/ComparisonRiskBarChart")).ComparisonRiskBarChart }),
);

export function ComparisonPage() {
  const [searchParams] = useSearchParams();
  const runIds = useMemo(
    () => searchParams.get("runs")?.split(",").filter(Boolean) ?? [],
    [searchParams],
  );
  const benchmarkSelections = useMemo(() => {
    const benchmarkName = searchParams.get("benchmark");
    return benchmarkName ? [{ benchmark_name: benchmarkName, model_names: [] }] : [];
  }, [searchParams]);
  const query = useComparison(runIds, benchmarkSelections);

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
          "\u8bf7\u5148\u4ece\u8fd0\u884c\u9875\u52fe\u9009\u591a\u4e2a run \u8fdb\u5165\u5bf9\u6bd4\uff0c\u6216\u4f20\u5165 benchmark \u53c2\u6570\u3002"
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
          title={"\u6a21\u578b\u6027\u80fd\u5bf9\u6bd4"}
          description={"\u7edf\u4e00\u5c55\u793a\u8bad\u7ec3\u6307\u6807\u3001benchmark \u8868\u73b0\u548c\u56de\u6d4b\u7ed3\u679c\u3002"}
        />
      </section>

      <div className="detail-grid">
        <section className="panel">
          <PanelHeader eyebrow={"\u6563\u70b9\u56fe"} title={"Test MAE vs \u5e74\u5316\u6536\u76ca"} />
          <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
            <ComparisonScatterChart rows={rows} />
          </Suspense>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u6761\u5f62\u56fe"} title={"\u56de\u64a4\u4e0e\u6362\u624b"} />
          <Suspense fallback={<LoadingState label={I18N.state.loading} />}>
            <ComparisonRiskBarChart rows={rows} />
          </Suspense>
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={"\u8868\u683c"} title={"\u5bf9\u6bd4\u7ed3\u679c"} />
        <table className="data-table">
          <thead>
            <tr>
              <th>{"\u6807\u7b7e"}</th>
              <th>{"\u6765\u6e90"}</th>
              <th>{I18N.nav.runs}</th>
              <th>{"\u8bad\u7ec3 MAE"}</th>
              <th>{"\u9a8c\u8bc1 MAE"}</th>
              <th>{"\u6d4b\u8bd5 MAE"}</th>
              <th>{"\u5e74\u5316\u6536\u76ca"}</th>
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
