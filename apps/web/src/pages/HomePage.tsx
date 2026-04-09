import { Link } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import { useWorkbenchOverview } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatFreshnessLabel, formatJobTypeLabel, formatStatusLabel } from "../shared/lib/labels";
import { mapOverviewView } from "../shared/view-model/mappers";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

export function HomePage() {
  const overviewQuery = useWorkbenchOverview();

  if (overviewQuery.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }

  if (overviewQuery.isError) {
    return <ErrorState message={(overviewQuery.error as Error).message} />;
  }

  if (!overviewQuery.data) {
    return (
      <EmptyState
        body={"\u5f53\u524d\u6ca1\u6709\u53ef\u5c55\u793a\u7684\u5de5\u4f5c\u53f0\u6982\u89c8\u6570\u636e\u3002"}
        title={I18N.state.empty}
      />
    );
  }

  const overview = mapOverviewView(overviewQuery.data);
  const riskItems = [
    ...overview.recentJobs.filter((job) => job.status === "failed").map((job) => ({
      id: job.job_id,
      title: `${formatJobTypeLabel(job.job_type)} / ${job.job_id}`,
      body: "\u4efb\u52a1\u72b6\u6001\u4e3a failed\uff0c\u5efa\u8bae\u5148\u56de\u5230\u4efb\u52a1\u4e2d\u5fc3\u67e5\u770b stage \u548c error \u8bf4\u660e\u3002",
    })),
    ...overview.recentBacktests
      .filter((item) => item.warning_count > 0 || item.status === "failed")
      .map((item) => ({
        id: item.backtest_id,
        title: `${item.backtest_id}`,
        body: `\u544a\u8b66 ${item.warning_count} \u6761 / \u72b6\u6001 ${formatStatusLabel(item.status)}`,
      })),
  ].slice(0, 4);

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">{I18N.nav.workbench}</div>
          <h1>{I18N.app.brand}</h1>
          <p>
            {
              "\u9996\u9875\u53ea\u4fdd\u7559\u6700\u8fd1\u6d3b\u52a8\u3001\u5feb\u901f\u5165\u53e3\u548c\u98ce\u9669\u63d0\u793a\uff0c\u964d\u4f4e\u9996\u5c4f\u566a\u58f0\uff0c\u8ba9\u7814\u7a76\u64cd\u4f5c\u66f4\u76f4\u63a5\u3002"
            }
          </p>
        </div>
        <div className="hero-actions">
          <LaunchTrainDrawer />
          <LaunchBacktestDrawer />
        </div>
      </section>

      <div className="workspace-grid">
        <section className="workspace-primary page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow={"\u6700\u8fd1\u6d3b\u52a8"}
              title={"\u6700\u8fd1\u6d3b\u52a8"}
              description={"\u4ece run\u3001backtest \u548c job \u4e09\u6761\u4e3b\u7ebf\u53cd\u6620\u5f53\u524d\u7814\u7a76\u6d41\u8f6c\u72b6\u6001\u3002"}
            />
            <div className="activity-list">
              {overview.recentRuns.slice(0, 3).map((item) => (
                <div className="activity-row" key={item.run_id}>
                  <div className="activity-badge">{I18N.nav.trainedModels}</div>
                  <div className="activity-copy">
                    <Link to={`/models/trained/${item.run_id}`}>{item.run_id}</Link>
                    <span>
                      {item.model_name} / {item.dataset_id ?? "--"} / <GlossaryHint hintKey="mae" />
                      {" "}
                      {formatNumber(item.primary_metric_value)}
                    </span>
                  </div>
                  <StatusPill status={item.status} />
                </div>
              ))}
              {overview.recentBacktests.slice(0, 2).map((item) => (
                <div className="activity-row" key={item.backtest_id}>
                  <div className="activity-badge">{I18N.nav.backtests}</div>
                  <div className="activity-copy">
                    <Link to={`/backtests/${item.backtest_id}`}>{item.backtest_id}</Link>
                    <span>
                      <GlossaryHint hintKey="max_drawdown" /> {formatNumber(item.max_drawdown)} / {"\u544a\u8b66 "}
                      {item.warning_count}
                      {" \u6761"}
                    </span>
                  </div>
                  <StatusPill status={item.status} />
                </div>
              ))}
              {overview.recentJobs.slice(0, 3).map((job) => (
                <div className="activity-row" key={job.job_id}>
                  <div className="activity-badge">{I18N.nav.jobs}</div>
                  <div className="activity-copy">
                    <Link to="/jobs">{job.job_id}</Link>
                    <span>
                      {formatJobTypeLabel(job.job_type)} / {formatDate(job.updated_at)}
                    </span>
                  </div>
                  <StatusPill status={job.status} />
                </div>
              ))}
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={"\u5feb\u901f\u5165\u53e3"}
              title={"\u5feb\u901f\u5165\u53e3"}
              description={"\u6309\u7814\u7a76\u64cd\u4f5c\u6d41\u89c6\u89d2\u8fdb\u5165\u6a21\u578b\u3001\u6570\u636e\u96c6\u3001\u56de\u6d4b\u548c\u57fa\u51c6\u5bf9\u6bd4\u3002"}
            />
            <div className="quick-link-grid">
              <Link className="nav-panel-link" to="/models">
                <strong>{I18N.nav.models}</strong>
                <span>{I18N.model.templateSection}</span>
              </Link>
              <Link className="nav-panel-link" to="/datasets">
                <strong>{I18N.nav.datasets}</strong>
                <span>
                  {overview.freshness?.dataset_id ?? "--"} /{" "}
                  {formatFreshnessLabel(overview.freshness?.freshness)}
                </span>
              </Link>
              <Link className="nav-panel-link" to="/backtests">
                <strong>{I18N.nav.backtests}</strong>
                <span>{`\u6700\u8fd1 ${overview.recentBacktests.length} \u6761\u56de\u6d4b\u7ed3\u679c`}</span>
              </Link>
              <Link className="nav-panel-link" to="/benchmarks">
                <strong>{I18N.nav.benchmarks}</strong>
                <span>{overview.recentBenchmarks[0]?.benchmark_name ?? "\u6682\u65e0\u57fa\u51c6"}</span>
              </Link>
            </div>
          </section>
        </section>

        <aside className="workspace-sidebar page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow={"\u98ce\u9669\u63d0\u793a"}
              title={"\u98ce\u9669\u63d0\u793a"}
              description={"\u5c06\u5931\u8d25\u4efb\u52a1\u3001\u9ad8\u544a\u8b66\u56de\u6d4b\u548c\u6570\u636e\u65b0\u9c9c\u5ea6\u6536\u5728\u540c\u4e00\u4fa7\u8fb9\u4e0a\u3002"}
            />
            {riskItems.length > 0 ? (
              <div className="stack-list">
                {riskItems.map((risk) => (
                  <div className="stack-item align-start" key={risk.id}>
                    <strong>{risk.title}</strong>
                    <span>{risk.body}</span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                body={"\u5f53\u524d\u6ca1\u6709\u65b0\u7684\u9ad8\u98ce\u9669\u9879\uff0c\u53ef\u7ee7\u7eed\u6d4f\u89c8\u6700\u8fd1\u8bad\u7ec3\u5b9e\u4f8b\u6216\u57fa\u51c6\u5bf9\u6bd4\u3002"}
                title={"\u98ce\u9669\u53ef\u63a7"}
              />
            )}
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={"\u57fa\u51c6\u5feb\u7167"}
              title={"\u57fa\u51c6\u5feb\u7167"}
              description={"\u9996\u9875\u53ea\u4fdd\u7559\u53ef\u626b\u63cf\u7684\u57fa\u51c6\u6982\u89c8\uff0c\u8be6\u7ec6\u5bf9\u6bd4\u5728\u57fa\u51c6\u9875\u5b8c\u6210\u3002"}
            />
            {overview.recentBenchmarks.length > 0 ? (
              <div className="stack-list">
                {overview.recentBenchmarks.map((item) => (
                  <div className="stack-item align-start" key={item.benchmark_name}>
                    <Link to={`/benchmarks/${item.benchmark_name}`}>{item.benchmark_name}</Link>
                    <span>{item.dataset_id}</span>
                    <span>
                      {`\u5f53\u524d\u9886\u5148 ${item.top_model_name ?? "--"} / \u5f97\u5206 ${formatNumber(item.top_model_score)}`}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState body={"\u6682\u65e0\u57fa\u51c6\u6458\u8981\u3002"} title={I18N.state.empty} />
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
