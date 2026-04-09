import { Link, useParams } from "react-router-dom";

import { useBenchmarkDetail } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { mapBenchmarkDetail } from "../shared/view-model/mappers";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { MetricGrid } from "../shared/ui/MetricGrid";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

export function BenchmarkDetailPage() {
  const { benchmarkName = "" } = useParams();
  const query = useBenchmarkDetail(benchmarkName);

  if (query.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (query.isError) {
    return <ErrorState message={(query.error as Error).message} />;
  }
  if (!query.data) {
    return (
      <EmptyState
        title={I18N.state.empty}
        body={"\u6ca1\u6709\u627e\u5230\u5bf9\u5e94\u7684 benchmark \u8be6\u60c5\u3002"}
      />
    );
  }

  const detail = mapBenchmarkDetail(query.data);

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.benchmarks}
          title={detail.benchmark_name}
          description={"\u5305\u542b leaderboard\u3001\u9a8c\u8bc1\u6458\u8981\u4e0e\u53ef\u8ffd\u6eaf\u5de5\u4ef6\u5165\u53e3\u3002"}
        />
        <MetricGrid
          items={[
            { label: "\u6570\u636e\u96c6", value: detail.dataset_id },
            { label: "\u7a97\u53e3\u6570", value: String(detail.window_count) },
            { label: "\u66f4\u65b0\u65f6\u95f4", value: formatDate(detail.updated_at) },
          ]}
        />
      </section>

      <div className="detail-grid">
        <section className="panel">
          <PanelHeader
            eyebrow={"Leaderboard"}
            title={"\u6a21\u578b\u6392\u540d"}
            description={"\u57fa\u4e8e MAE \u7684\u6a2a\u5411\u5bf9\u6bd4\uff0c\u5173\u952e\u672f\u8bed\u5168\u90e8\u6536\u655b\u4e3a hover \u95ee\u53f7\u89e3\u91ca\u3002"}
          />
          {detail.results.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>{I18N.nav.runs}</th>
                  <th>{"\u7b97\u6cd5\u65cf"}</th>
                  <th><GlossaryHint hintKey="mae" /></th>
                  <th><GlossaryHint hintKey="mae" termOverride={"Test MAE"} /></th>
                </tr>
              </thead>
              <tbody>
                {detail.results.map((row) => (
                  <tr key={`${row.rank}-${row.model_name}`}>
                    <td>{row.rank}</td>
                    <td>{row.model_name}</td>
                    <td>{row.family}</td>
                    <td>{formatNumber(row.mean_valid_mae)}</td>
                    <td>{formatNumber(row.mean_test_mae)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState
              title={I18N.state.empty}
              body={"\u57fa\u51c6\u7ed3\u679c\u4e2d\u6ca1\u6709 leaderboard \u6570\u636e\u3002"}
            />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u544a\u8b66\u6458\u8981"} title={"\u98ce\u9669\u63d0\u793a"} />
          {detail.warning_summary && detail.warning_summary.count > 0 ? (
            <div className="stack-list">
              {detail.warning_summary.items.map((warning) => (
                <div className="stack-item" key={warning}>
                  <strong>{warning}</strong>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState
              title={"\u65e0\u544a\u8b66"}
              body={"\u5f53\u524d benchmark \u6ca1\u6709\u989d\u5916\u544a\u8b66\u3002"}
            />
          )}
          {detail.review_summary ? (
            <div className="stack-list">
              <div className="stack-item">
                <strong>{detail.review_summary.title}</strong>
                <span>{detail.review_summary.summary}</span>
              </div>
            </div>
          ) : null}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={"\u5de5\u4ef6"} title={"\u57fa\u51c6\u4ea7\u7269"} />
        <div className="stack-list">
          {detail.artifacts.map((artifact) => (
            <div className="stack-item" key={artifact.uri}>
              <strong>{artifact.label}</strong>
              <span>{artifact.uri}</span>
            </div>
          ))}
          <Link className="link-button" to="/benchmarks">
            {"\u8fd4\u56de\u57fa\u51c6\u5217\u8868"}
          </Link>
        </div>
      </section>
    </div>
  );
}
