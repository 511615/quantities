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
    return <EmptyState body="当前没有可展示的工作台概览数据。" title={I18N.state.empty} />;
  }

  const overview = mapOverviewView(overviewQuery.data);
  const riskItems = [
    ...overview.recentJobs.filter((job) => job.status === "failed").map((job) => ({
      id: job.job_id,
      title: `${formatJobTypeLabel(job.job_type)} / ${job.job_id}`,
      body: "任务状态为失败，建议先回到任务中心查看阶段和错误说明。",
    })),
    ...overview.recentBacktests
      .filter((item) => item.warning_count > 0 || item.status === "failed")
      .map((item) => ({
        id: item.backtest_id,
        title: item.backtest_id,
        body: `告警 ${item.warning_count} 条 / 状态 ${formatStatusLabel(item.status)}`,
      })),
  ].slice(0, 4);

  return (
    <div className="page-stack">
      <section className="page-header-shell">
        <div className="page-header-main">
          <div className="eyebrow">{I18N.nav.workbench}</div>
          <h1>{I18N.app.brand}</h1>
          <p>把最近活动、关键入口和风险提示压缩到同一屏里，让你先看清现在发生了什么，再决定下一步。</p>
        </div>
        <div className="page-header-actions">
          <LaunchTrainDrawer />
          <LaunchBacktestDrawer />
        </div>
      </section>

      <div className="summary-grid">
        <div className="summary-card">
          <span>最近训练</span>
          <strong>{overview.recentRuns.length}</strong>
        </div>
        <div className="summary-card">
          <span>最近回测</span>
          <strong>{overview.recentBacktests.length}</strong>
        </div>
        <div className="summary-card">
          <span>最近任务</span>
          <strong>{overview.recentJobs.length}</strong>
        </div>
        <div className="summary-card">
          <span>数据新鲜度</span>
          <strong>{formatFreshnessLabel(overview.freshness?.freshness)}</strong>
        </div>
      </div>

      <div className="workspace-grid workspace-grid-balanced">
        <section className="workspace-primary page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow="当前流转"
              title="最近活动"
              description="用同一视角跟踪训练、回测和任务状态，不再把它们分散在不同页面才能看全。"
            />
            <div className="activity-list">
              {overview.recentRuns.slice(0, 3).map((item) => (
                <div className="activity-row" key={item.run_id}>
                  <div className="activity-badge">{I18N.nav.trainedModels}</div>
                  <div className="activity-copy">
                    <Link to={`/models/trained/${encodeURIComponent(item.run_id)}`}>{item.run_id}</Link>
                    <span>
                      {item.model_name} / {item.dataset_id ?? "--"} / <GlossaryHint hintKey="mae" />{" "}
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
                    <Link to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>{item.backtest_id}</Link>
                    <span>
                      <GlossaryHint hintKey="max_drawdown" /> {formatNumber(item.max_drawdown)} / 告警 {item.warning_count} 条
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
                    <span>{formatJobTypeLabel(job.job_type)} / {formatDate(job.updated_at)}</span>
                  </div>
                  <StatusPill status={job.status} />
                </div>
              ))}
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="关键入口"
              title="下一步最常用的操作"
              description="把模型、数据集、回测和基准入口做成清晰的工作流跳板，而不是四块同质化卡片。"
            />
            <div className="quick-link-grid">
              <Link className="nav-panel-link" to="/models">
                <strong>{I18N.nav.models}</strong>
                <span>{I18N.model.templateSection}</span>
              </Link>
              <Link className="nav-panel-link" to="/datasets">
                <strong>{I18N.nav.datasets}</strong>
                <span>{overview.freshness?.dataset_id ?? "--"} / {formatFreshnessLabel(overview.freshness?.freshness)}</span>
              </Link>
              <Link className="nav-panel-link" to="/backtests">
                <strong>{I18N.nav.backtests}</strong>
                <span>最近 {overview.recentBacktests.length} 条回测结果</span>
              </Link>
              <Link className="nav-panel-link" to="/benchmarks">
                <strong>{I18N.nav.benchmarks}</strong>
                <span>{overview.recentBenchmarks[0]?.benchmark_name ?? "暂无基准"}</span>
              </Link>
            </div>
          </section>
        </section>

        <aside className="workspace-sidebar page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow="待处理项"
              title="风险提示"
              description="失败任务、高告警回测和可疑结果优先出现在右侧，帮助你先处理风险。"
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
              <EmptyState body="当前没有新的高风险项，可继续浏览最近训练实例或基准对比。" title="风险可控" />
            )}
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="基准快照"
              title="当前 benchmark 状态"
              description="首页只保留适合扫读的 benchmark 摘要，详细分析统一收敛到 benchmark 页。"
            />
            {overview.recentBenchmarks.length > 0 ? (
              <div className="stack-list">
                {overview.recentBenchmarks.map((item) => (
                  <div className="stack-item align-start" key={item.benchmark_name}>
                    <Link to={`/benchmarks/${encodeURIComponent(item.benchmark_name)}`}>{item.benchmark_name}</Link>
                    <span>{item.dataset_id}</span>
                    <span>当前领先 {item.top_model_name ?? "--"} / 得分 {formatNumber(item.top_model_score)}</span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState body="暂无基准摘要。" title={I18N.state.empty} />
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
