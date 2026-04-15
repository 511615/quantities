import { Link, useParams } from "react-router-dom";

import { useBenchmarkDetail } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N, translateText } from "../shared/lib/i18n";
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
    return <EmptyState title={I18N.state.empty} body={translateText("没有找到对应的基准详情。")} />;
  }

  const detail = mapBenchmarkDetail(query.data);
  const rows = detail.results.length > 0 ? detail.results : detail.leaderboard;

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.benchmarks}
          title={detail.benchmark_name}
          description={translateText("包含排行榜、验证摘要与可追溯工件入口。")}
        />
        <MetricGrid
          items={[
            { label: translateText("数据集"), value: detail.dataset_id },
            { label: translateText("窗口数"), value: String(detail.window_count) },
            { label: translateText("更新时间"), value: formatDate(detail.updated_at) },
          ]}
        />
      </section>

      <div className="detail-grid">
        <section className="panel">
          <PanelHeader
            eyebrow={translateText("排行榜")}
            title={translateText("模型排名")}
            description={translateText("基于 MAE 的横向对比，关键术语全部收敛为 hover 问号解释。")}
          />
          {rows.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>{translateText("排名")}</th>
                  <th>{I18N.nav.runs}</th>
                  <th>{translateText("算法族")}</th>
                  <th><GlossaryHint hintKey="mae" /></th>
                  <th><GlossaryHint hintKey="mae" termOverride={translateText("测试集 MAE")} /></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
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
            <EmptyState title={I18N.state.empty} body={translateText("基准结果中没有排行榜数据。")} />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow={translateText("告警摘要")} title={translateText("风险提示")} />
          {detail.warning_summary && detail.warning_summary.count > 0 ? (
            <div className="stack-list">
              {detail.warning_summary.items.map((warning) => (
                <div className="stack-item" key={warning}>
                  <strong>{warning}</strong>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title={translateText("无告警")} body={translateText("当前基准没有额外告警。")} />
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
        <PanelHeader eyebrow={translateText("工件")} title={translateText("基准产物")} />
        <div className="stack-list">
          {detail.artifacts.map((artifact) => (
            <div className="stack-item" key={artifact.uri}>
              <strong>{artifact.label}</strong>
              <span>{artifact.uri}</span>
            </div>
          ))}
          <Link className="link-button" to="/benchmarks">
            {translateText("返回基准列表")}
          </Link>
        </div>
      </section>
    </div>
  );
}
