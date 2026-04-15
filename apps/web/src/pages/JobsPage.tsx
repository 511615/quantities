import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import { api } from "../shared/api/client";
import { useBacktestOptions, useJobStatus, useJobs } from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { formatJobTypeLabel, formatStageNameLabel } from "../shared/lib/labels";
import { I18N, translateText } from "../shared/lib/i18n";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

const COPY = {
  pageTitle: () => translateText("任务中心"),
  pageDescription: () => translateText("同一页内统一完成训练、回测发起，job 跟踪，失败原因浏览和结果落点跳转。"),
  trainTitle: () => translateText("发起训练"),
  backtestTitle: () => translateText("发起回测"),
  futureTitle: () => translateText("数据层任务"),
  futureDescription: () => translateText("后端还没有开放数据同步和数据集构建接口，这里先保留受控占位按钮和统一错误语义。"),
  trackedTitle: () => translateText("当前跟踪任务"),
  resultTitle: () => translateText("结果落点"),
  recentTitle: () => translateText("最近任务"),
  failedTitle: () => translateText("失败任务"),
  noJobs: () => translateText("暂无任务，请从左侧发起训练或回测。"),
  unsupported: () => translateText("该接口未就绪。"),
  datasetPreset: () => translateText("数据集预置"),
  predictionScope: () => translateText("预测范围"),
  benchmarkSymbol: () => translateText("基准标的"),
  runId: () => translateText("运行 ID"),
  runIdHint: () => translateText("请输入运行 ID（run_id）。"),
  benchmarkHint: () => translateText("请输入基准标的。"),
  submitSync: () => translateText("提交行情同步"),
  submitBuild: () => translateText("提交数据集构建"),
  openRun: () => translateText("跳转运行详情"),
  openBacktest: () => translateText("跳转回测详情"),
  type: () => translateText("类型"),
  updatedAt: () => translateText("更新时间"),
  stage: () => translateText("阶段"),
  status: () => translateText("状态"),
  deeplinks: () => translateText("落点"),
} as const;

function localizeBacktestOptionLabel(value: string, label?: string | null) {
  const normalized = (label ?? value).trim().toLowerCase();
  if (normalized === "smoke") {
    return translateText("联调样本");
  }
  if (normalized === "real_benchmark") {
    return translateText("真实基准");
  }
  if (normalized === "full") {
    return translateText("全量");
  }
  if (normalized === "test") {
    return translateText("测试集");
  }
  return label ?? value;
}

export function JobsPage() {
  const queryClient = useQueryClient();
  const jobsQuery = useJobs();
  const backtestOptionsQuery = useBacktestOptions();
  const jobs = jobsQuery.data?.items ?? [];
  const [trackedJobId, setTrackedJobId] = useState<string | null>(null);
  const [runId, setRunId] = useState("");
  const [backtestDatasetPreset, setBacktestDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [predictionScope, setPredictionScope] = useState<"full" | "test">("full");
  const [benchmarkSymbol, setBenchmarkSymbol] = useState("BTCUSDT");
  const [backtestFormError, setBacktestFormError] = useState<string | null>(null);
  const [unsupportedMessage, setUnsupportedMessage] = useState<string | null>(null);

  const trackedJobQuery = useJobStatus(trackedJobId);
  const trackedJob = trackedJobQuery.data;
  const trackedDeeplinks = trackedJob?.result?.deeplinks;

  useEffect(() => {
    if (!trackedJobId && jobs[0]?.job_id) {
      setTrackedJobId(jobs[0].job_id);
    }
  }, [jobs, trackedJobId]);

  useEffect(() => {
    const defaultSymbol = backtestOptionsQuery.data?.default_benchmark_symbol;
    if (defaultSymbol) {
      setBenchmarkSymbol(defaultSymbol);
    }
  }, [backtestOptionsQuery.data?.default_benchmark_symbol]);

  const backtestMutation = useMutation({
    mutationFn: () =>
      api.launchBacktest({
        run_id: runId,
        dataset_preset: backtestDatasetPreset,
        prediction_scope: predictionScope,
        strategy_preset: "sign",
        portfolio_preset: "research_default",
        cost_preset: "standard",
        benchmark_symbol: benchmarkSymbol,
      }),
    onSuccess: (result) => {
      setTrackedJobId(result.job_id);
      setBacktestFormError(null);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["backtests"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  const failedJobs = useMemo(() => jobs.filter((job) => job.error_message), [jobs]);

  function handleBacktestSubmit() {
    if (!runId.trim()) {
      setBacktestFormError(COPY.runIdHint());
      return;
    }
    if (!benchmarkSymbol.trim()) {
      setBacktestFormError(COPY.benchmarkHint());
      return;
    }
    setUnsupportedMessage(null);
    setBacktestFormError(null);
    backtestMutation.mutate();
  }

  if (jobsQuery.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (jobsQuery.isError) {
    return <ErrorState message={(jobsQuery.error as Error).message} />;
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader eyebrow={I18N.nav.jobs} title={COPY.pageTitle()} description={COPY.pageDescription()} />
      </section>

      <div className="detail-grid wide-secondary">
        <div className="stack-list">
          <section className="panel">
            <PanelHeader
              eyebrow={COPY.trainTitle()}
              title={COPY.trainTitle()}
              description={translateText("统一复用模板驱动训练入口，避免任务中心与模型页出现不同的训练参数语义。")}
            />
            <LaunchTrainDrawer
              defaultOpen
              showTrigger={false}
              title={COPY.trainTitle()}
              description={I18N.model.templateSection}
              onJobCreated={setTrackedJobId}
            />
          </section>

          <section className="panel">
            <PanelHeader eyebrow={COPY.backtestTitle()} title={COPY.backtestTitle()} description={I18N.model.trainedSection} />
            <div className="form-section-grid">
              <label>
                <span>{COPY.runId()}</span>
                <input className="field" onChange={(event) => setRunId(event.target.value)} value={runId} />
              </label>
              <label>
                <span>{COPY.datasetPreset()}</span>
                <select
                  className="field"
                  onChange={(event) => setBacktestDatasetPreset(event.target.value as "smoke" | "real_benchmark")}
                  value={backtestDatasetPreset}
                >
                  {(backtestOptionsQuery.data?.dataset_presets ?? []).map((option) => (
                    <option key={option.value} value={option.value}>
                      {localizeBacktestOptionLabel(option.value, option.label)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>{COPY.predictionScope()}</span>
                <select
                  className="field"
                  onChange={(event) => setPredictionScope(event.target.value as "full" | "test")}
                  value={predictionScope}
                >
                  {(backtestOptionsQuery.data?.prediction_scopes ?? []).map((option) => (
                    <option key={option.value} value={option.value}>
                      {localizeBacktestOptionLabel(option.value, option.label)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>{COPY.benchmarkSymbol()}</span>
                <input className="field" onChange={(event) => setBenchmarkSymbol(event.target.value)} value={benchmarkSymbol} />
              </label>
            </div>
            {backtestFormError ? <p className="form-error">{backtestFormError}</p> : null}
            {backtestMutation.isError ? <p className="form-error">{(backtestMutation.error as Error).message}</p> : null}
            <div className="toolbar">
              <button
                className="action-button secondary"
                disabled={backtestMutation.isPending || backtestOptionsQuery.isLoading}
                onClick={handleBacktestSubmit}
                type="button"
              >
                {backtestMutation.isPending ? translateText("提交中...") : I18N.action.launchBacktest}
              </button>
            </div>
          </section>

          <section className="panel">
            <PanelHeader eyebrow={COPY.futureTitle()} title={COPY.futureTitle()} description={COPY.futureDescription()} />
            <div className="toolbar">
              <button className="action-button secondary" onClick={() => setUnsupportedMessage(COPY.unsupported())} type="button">
                {COPY.submitSync()}
              </button>
              <button className="action-button secondary" onClick={() => setUnsupportedMessage(COPY.unsupported())} type="button">
                {COPY.submitBuild()}
              </button>
            </div>
            {unsupportedMessage ? <p className="form-error">{unsupportedMessage}</p> : null}
          </section>
        </div>

        <div className="stack-list">
          <section className="panel">
            <PanelHeader eyebrow={COPY.trackedTitle()} title={COPY.trackedTitle()} description={trackedJobId ?? "--"} />
            {trackedJobQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {trackedJobQuery.isError ? <ErrorState message={(trackedJobQuery.error as Error).message} /> : null}
            {trackedJob ? (
              <div className="job-box">
                <div className="split-line">
                  <strong>{trackedJob.job_id}</strong>
                  <StatusPill status={trackedJob.status} />
                </div>
                {trackedJob.stages.map((stage) => (
                  <div className="job-stage" key={stage.name}>
                    <span>{formatStageNameLabel(stage.name)}</span>
                    <span>{stage.summary || "--"}</span>
                  </div>
                ))}
                {trackedJob.error_message ? <p className="form-error">{trackedJob.error_message}</p> : null}
              </div>
            ) : (
              <EmptyState title={I18N.state.empty} body={COPY.noJobs()} />
            )}
          </section>

          {trackedDeeplinks ? (
            <section className="panel">
              <PanelHeader eyebrow={COPY.resultTitle()} title={COPY.resultTitle()} />
              <div className="toolbar">
                {trackedDeeplinks.run_detail ? <Link className="link-button" to={trackedDeeplinks.run_detail}>{COPY.openRun()}</Link> : null}
                {trackedDeeplinks.backtest_detail ? <Link className="link-button" to={trackedDeeplinks.backtest_detail}>{COPY.openBacktest()}</Link> : null}
              </div>
            </section>
          ) : null}
        </div>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={COPY.recentTitle()} title={COPY.recentTitle()} />
        <table className="data-table">
          <thead>
            <tr>
              <th>{translateText("任务 ID")}</th>
              <th>{COPY.type()}</th>
              <th>{COPY.updatedAt()}</th>
              <th>{COPY.stage()}</th>
              <th>{COPY.status()}</th>
              <th>{COPY.deeplinks()}</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.job_id} onClick={() => setTrackedJobId(job.job_id)}>
                <td>{job.job_id}</td>
                <td>{formatJobTypeLabel(job.job_type)}</td>
                <td>{formatDate(job.updated_at)}</td>
                <td>{job.stages.length > 0 ? formatStageNameLabel(job.stages[job.stages.length - 1].name) : "--"}</td>
                <td><StatusPill status={job.status} /></td>
                <td>
                  <div className="inline-link-row">
                    {job.result?.deeplinks?.run_detail ? <Link to={job.result.deeplinks.run_detail}>{I18N.nav.runs}</Link> : null}
                    {job.result?.deeplinks?.backtest_detail ? <Link to={job.result.deeplinks.backtest_detail}>{I18N.nav.backtests}</Link> : null}
                    {job.result?.deeplinks?.review_detail ? <Link to={job.result.deeplinks.review_detail}>{translateText("审阅")}</Link> : null}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {jobs.length === 0 ? <EmptyState title={I18N.state.empty} body={COPY.noJobs()} /> : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow={COPY.failedTitle()} title={COPY.failedTitle()} />
        {failedJobs.length === 0 ? (
          <EmptyState title={I18N.state.empty} body={translateText("当前没有需要处理的失败任务。")} />
        ) : (
          <div className="stack-list">
            {failedJobs.map((job) => (
              <div className="stack-item align-start" key={`${job.job_id}-error`}>
                <strong>{job.job_id}</strong>
                <span>{job.error_message}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
