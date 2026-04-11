import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import { api } from "../shared/api/client";
import {
  useBacktestOptions,
  useJobStatus,
  useJobs,
} from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { formatJobTypeLabel, formatStageNameLabel } from "../shared/lib/labels";
import { I18N } from "../shared/lib/i18n";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

const COPY = {
  pageTitle: "\u4efb\u52a1\u4e2d\u5fc3",
  pageDescription:
    "\u540c\u4e00\u9875\u5185\u7edf\u4e00\u5b8c\u6210\u8bad\u7ec3\u3001\u56de\u6d4b\u53d1\u8d77\uff0cjob \u8ddf\u8e2a\uff0c\u5931\u8d25\u539f\u56e0\u6d4f\u89c8\u548c\u7ed3\u679c\u843d\u70b9\u8df3\u8f6c\u3002",
  trainTitle: "\u53d1\u8d77\u8bad\u7ec3",
  backtestTitle: "\u53d1\u8d77\u56de\u6d4b",
  futureTitle: "\u6570\u636e\u5c42\u4efb\u52a1",
  futureDescription:
    "\u540e\u7aef\u8fd8\u6ca1\u6709\u5f00\u653e ingestion sync \u548c dataset build \u63a5\u53e3\uff0c\u8fd9\u91cc\u5148\u4fdd\u7559\u53d7\u63a7\u5360\u4f4d\u6309\u94ae\u548c\u7edf\u4e00\u9519\u8bef\u8bed\u4e49\u3002",
  trackedTitle: "\u5f53\u524d\u8ddf\u8e2a\u4efb\u52a1",
  resultTitle: "\u7ed3\u679c\u843d\u70b9",
  recentTitle: "\u6700\u8fd1\u4efb\u52a1",
  failedTitle: "\u5931\u8d25\u4efb\u52a1",
  noJobs: "\u6682\u65e0\u4efb\u52a1\uff0c\u8bf7\u4ece\u5de6\u4fa7\u53d1\u8d77\u8bad\u7ec3\u6216\u56de\u6d4b\u3002",
  unsupported: "\u8be5\u63a5\u53e3\u672a\u5c31\u7eea\u3002",
  datasetPreset: "\u6570\u636e\u96c6\u9884\u7f6e",
  trainerPreset: "\u8bad\u7ec3\u9884\u7f6e",
  seed: "Seed",
  experimentName: "\u5b9e\u9a8c\u540d\u79f0",
  modelNames: "\u6a21\u578b\u540d\u79f0\uff0c\u9017\u53f7\u5206\u9694",
  modelNamesHint: "\u8bf7\u81f3\u5c11\u8f93\u5165\u4e00\u4e2a\u6a21\u578b\u540d\u79f0\u3002",
  runId: "Run ID",
  predictionScope: "\u9884\u6d4b\u8303\u56f4",
  benchmarkSymbol: "\u57fa\u51c6 Symbol",
  runIdHint: "\u8bf7\u8f93\u5165 run_id\u3002",
  benchmarkHint: "\u8bf7\u8f93\u5165 benchmark symbol\u3002",
  submitSync: "\u63d0\u4ea4\u884c\u60c5\u540c\u6b65",
  submitBuild: "\u63d0\u4ea4\u6570\u636e\u96c6\u6784\u5efa",
  openRun: "\u8df3\u8f6c\u8fd0\u884c\u8be6\u60c5",
  openBacktest: "\u8df3\u8f6c\u56de\u6d4b\u8be6\u60c5",
  type: "\u7c7b\u578b",
  updatedAt: "\u66f4\u65b0\u65f6\u95f4",
  stage: "\u9636\u6bb5",
  status: "\u72b6\u6001",
  deeplinks: "\u843d\u70b9",
} as const;

export function JobsPage() {
  const queryClient = useQueryClient();
  const jobsQuery = useJobs();
  const backtestOptionsQuery = useBacktestOptions();
  const [trackedJobId, setTrackedJobId] = useState<string | null>(null);
  const [runId, setRunId] = useState("");
  const [backtestDatasetPreset, setBacktestDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [predictionScope, setPredictionScope] = useState<"full" | "test">("full");
  const [benchmarkSymbol, setBenchmarkSymbol] = useState("BTCUSDT");
  const [backtestFormError, setBacktestFormError] = useState<string | null>(null);
  const [unsupportedMessage, setUnsupportedMessage] = useState<string | null>(null);

  const trackedJobQuery = useJobStatus(trackedJobId);
  const trackedJob = trackedJobQuery.data;

  useEffect(() => {
    if (!trackedJobId && jobsQuery.data?.items[0]?.job_id) {
      setTrackedJobId(jobsQuery.data.items[0].job_id);
    }
  }, [jobsQuery.data, trackedJobId]);

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

  const failedJobs = useMemo(
    () => jobsQuery.data?.items.filter((job) => job.error_message) ?? [],
    [jobsQuery.data?.items],
  );

  function handleBacktestSubmit() {
    if (!runId.trim()) {
      setBacktestFormError(COPY.runIdHint);
      return;
    }
    if (!benchmarkSymbol.trim()) {
      setBacktestFormError(COPY.benchmarkHint);
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
  if (!jobsQuery.data || jobsQuery.data.items.length === 0) {
    return (
      <div className="page-stack">
        <section className="panel">
          <PanelHeader
            eyebrow={I18N.nav.jobs}
            title={COPY.pageTitle}
            description={COPY.pageDescription}
          />
        </section>
        <EmptyState title={I18N.state.empty} body={COPY.noJobs} />
      </div>
    );
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.jobs}
          title={COPY.pageTitle}
          description={COPY.pageDescription}
        />
      </section>

      <div className="detail-grid wide-secondary">
        <div className="stack-list">
          <section className="panel">
            <PanelHeader
              eyebrow={COPY.trainTitle}
              title={COPY.trainTitle}
              description={"统一复用模板驱动训练入口，避免任务中心与模型页出现不同的训练参数语义。"}
            />
            <LaunchTrainDrawer
              defaultOpen
              showTrigger={false}
              title={COPY.trainTitle}
              description={I18N.model.templateSection}
              onJobCreated={setTrackedJobId}
            />
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={COPY.backtestTitle}
              title={COPY.backtestTitle}
              description={I18N.model.trainedSection}
            />
            <div className="form-section-grid">
              <label>
                <span>{COPY.runId}</span>
                <input
                  className="field"
                  onChange={(event) => setRunId(event.target.value)}
                  value={runId}
                />
              </label>
              <label>
                <span>{COPY.datasetPreset}</span>
                <select
                  className="field"
                  onChange={(event) =>
                    setBacktestDatasetPreset(event.target.value as "smoke" | "real_benchmark")
                  }
                  value={backtestDatasetPreset}
                >
                  {(backtestOptionsQuery.data?.dataset_presets ?? []).map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>{COPY.predictionScope}</span>
                <select
                  className="field"
                  onChange={(event) => setPredictionScope(event.target.value as "full" | "test")}
                  value={predictionScope}
                >
                  {(backtestOptionsQuery.data?.prediction_scopes ?? []).map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>{COPY.benchmarkSymbol}</span>
                <input
                  className="field"
                  onChange={(event) => setBenchmarkSymbol(event.target.value)}
                  value={benchmarkSymbol}
                />
              </label>
            </div>
            {backtestFormError ? <p className="form-error">{backtestFormError}</p> : null}
            {backtestMutation.isError ? (
              <p className="form-error">{(backtestMutation.error as Error).message}</p>
            ) : null}
            <div className="toolbar">
              <button
                className="action-button secondary"
                disabled={backtestMutation.isPending || backtestOptionsQuery.isLoading}
                onClick={handleBacktestSubmit}
                type="button"
              >
                {backtestMutation.isPending ? "\u63d0\u4ea4\u4e2d..." : I18N.action.launchBacktest}
              </button>
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={COPY.futureTitle}
              title={COPY.futureTitle}
              description={COPY.futureDescription}
            />
            <div className="toolbar">
              <button
                className="action-button secondary"
                onClick={() => setUnsupportedMessage(COPY.unsupported)}
                type="button"
              >
                {COPY.submitSync}
              </button>
              <button
                className="action-button secondary"
                onClick={() => setUnsupportedMessage(COPY.unsupported)}
                type="button"
              >
                {COPY.submitBuild}
              </button>
            </div>
            {unsupportedMessage ? <p className="form-error">{unsupportedMessage}</p> : null}
          </section>
        </div>

        <div className="stack-list">
          <section className="panel">
            <PanelHeader
              eyebrow={COPY.trackedTitle}
              title={COPY.trackedTitle}
              description={trackedJobId ?? "--"}
            />
            {trackedJobQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {trackedJobQuery.isError ? (
              <ErrorState message={(trackedJobQuery.error as Error).message} />
            ) : null}
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
                {trackedJob.error_message ? (
                  <p className="form-error">{trackedJob.error_message}</p>
                ) : null}
              </div>
            ) : null}
          </section>

          {trackedJob?.result?.deeplinks ? (
            <section className="panel">
              <PanelHeader eyebrow={COPY.resultTitle} title={COPY.resultTitle} />
              <div className="toolbar">
                {trackedJob.result.deeplinks.run_detail ? (
                  <Link className="link-button" to={trackedJob.result.deeplinks.run_detail}>
                    {COPY.openRun}
                  </Link>
                ) : null}
                {trackedJob.result.deeplinks.backtest_detail ? (
                  <Link className="link-button" to={trackedJob.result.deeplinks.backtest_detail}>
                    {COPY.openBacktest}
                  </Link>
                ) : null}
              </div>
            </section>
          ) : null}
        </div>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={COPY.recentTitle} title={COPY.recentTitle} />
        <table className="data-table">
          <thead>
            <tr>
              <th>Job ID</th>
              <th>{COPY.type}</th>
              <th>{COPY.updatedAt}</th>
              <th>{COPY.stage}</th>
              <th>{COPY.status}</th>
              <th>{COPY.deeplinks}</th>
            </tr>
          </thead>
          <tbody>
            {jobsQuery.data.items.map((job) => (
              <tr key={job.job_id} onClick={() => setTrackedJobId(job.job_id)}>
                <td>{job.job_id}</td>
                <td>{formatJobTypeLabel(job.job_type)}</td>
                <td>{formatDate(job.updated_at)}</td>
                <td>
                  {job.stages.length > 0
                    ? formatStageNameLabel(job.stages[job.stages.length - 1].name)
                    : "--"}
                </td>
                <td>
                  <StatusPill status={job.status} />
                </td>
                <td>
                  <div className="inline-link-row">
                    {job.result.deeplinks.run_detail ? (
                      <Link to={job.result.deeplinks.run_detail}>{I18N.nav.runs}</Link>
                    ) : null}
                    {job.result.deeplinks.backtest_detail ? (
                      <Link to={job.result.deeplinks.backtest_detail}>{I18N.nav.backtests}</Link>
                    ) : null}
                    {job.result.deeplinks.review_detail ? (
                      <Link to={job.result.deeplinks.review_detail}>{"\u5ba1\u9605"}</Link>
                    ) : null}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <PanelHeader eyebrow={COPY.failedTitle} title={COPY.failedTitle} />
        {failedJobs.length === 0 ? (
          <EmptyState
            title={I18N.state.empty}
            body={"\u5f53\u524d\u6ca1\u6709\u9700\u8981\u5904\u7406\u7684\u5931\u8d25\u4efb\u52a1\u3002"}
          />
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
