import { Link } from "react-router-dom";

import { useBacktestOptions, useBenchmarks } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

export function BenchmarksPage() {
  const query = useBenchmarks();
  const optionsQuery = useBacktestOptions();
  const officialTemplate = optionsQuery.data?.template_options?.find(
    (item) => item.template_id === optionsQuery.data?.official_template_id,
  );

  return (
    <div className="page-stack">
      {officialTemplate ? (
        <section className="panel">
          <PanelHeader
            eyebrow={"Official Protocol"}
            title={officialTemplate.name}
            description={officialTemplate.description ?? "\u5e73\u53f0\u7edf\u4e00\u56de\u6d4b\u6a21\u677f\u3002"}
            action={
              <Link
                className="link-button"
                to={`/comparison?official_only=1&template_id=${encodeURIComponent(officialTemplate.template_id)}`}
              >
                {"\u67e5\u770b\u5bf9\u6bd4"}
              </Link>
            }
          />
          <div className="metric-grid detail-metric-grid">
            <div className="metric-tile">
              <span>{"\u6a21\u677f ID"}</span>
              <strong>{officialTemplate.template_id}</strong>
            </div>
            <div className="metric-tile">
              <span>{"Protocol"}</span>
              <strong>{officialTemplate.protocol_version ?? "--"}</strong>
            </div>
            <div className="metric-tile">
              <span>{"\u72b6\u6001"}</span>
              <strong>{"\u4e0d\u53ef\u5220\u9664"}</strong>
            </div>
          </div>
          <div className="stack-list">
            {officialTemplate.scenario_bundle.slice(0, 4).map((item) => (
              <div className="stack-item" key={item}>
                <strong>{item}</strong>
                <span>{"\u5b98\u65b9 stress bundle \u56fa\u5b9a\u7ec4\u6210\u90e8\u5206"}</span>
              </div>
            ))}
          </div>
        </section>
      ) : null}
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.benchmarks}
          title={I18N.nav.benchmarks}
          description={"\u5c06\u57fa\u51c6\u699c\u5355\u4e0e\u6a21\u578b\u5bf9\u6bd4\u5165\u53e3\u6536\u655b\u5230\u540c\u4e00\u4e2a\u5de5\u4f5c\u9762\u3002"}
          action={
            <Link className="link-button" to="/comparison">
              {I18N.nav.comparison}
            </Link>
          }
        />
        {query.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
        {query.isError ? <ErrorState message={(query.error as Error).message} /> : null}
        {!query.isLoading && !query.isError ? (
          query.data && query.data.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>{"\u57fa\u51c6\u540d\u79f0"}</th>
                  <th>{"\u6570\u636e\u96c6"}</th>
                  <th>{"\u5f53\u524d\u9886\u5148"}</th>
                  <th><GlossaryHint hintKey="benchmark" termOverride={"\u5f97\u5206"} /></th>
                  <th>{"\u66f4\u65b0\u65f6\u95f4"}</th>
                </tr>
              </thead>
              <tbody>
                {query.data.map((item) => (
                  <tr key={item.benchmark_name}>
                    <td>
                      <Link to={`/benchmarks/${item.benchmark_name}`}>{item.benchmark_name}</Link>
                    </td>
                    <td>{item.dataset_id}</td>
                    <td>{item.top_model_name ?? "--"}</td>
                    <td>{formatNumber(item.top_model_score)}</td>
                    <td>{formatDate(item.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState
              title={I18N.state.empty}
              body={"\u5f53\u524d\u6ca1\u6709\u53ef\u5c55\u793a\u7684\u57fa\u51c6\u7ed3\u679c\u3002"}
            />
          )
        ) : null}
      </section>
    </div>
  );
}
