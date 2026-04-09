import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../../shared/api/client";
import { useBacktestOptions, useJobStatus } from "../../shared/api/hooks";
import { formatStageNameLabel } from "../../shared/lib/labels";
import { I18N } from "../../shared/lib/i18n";
import { StatusPill } from "../../shared/ui/StatusPill";

type LaunchBacktestDrawerProps = {
  initialRunId?: string | null;
  initialDatasetId?: string | null;
};

export function LaunchBacktestDrawer({ initialRunId = null, initialDatasetId = null }: LaunchBacktestDrawerProps) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const optionsQuery = useBacktestOptions();
  const [open, setOpen] = useState(false);
  const [runId, setRunId] = useState(initialRunId ?? "");
  const [jobId, setJobId] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [predictionScope, setPredictionScope] = useState<"full" | "test">("full");
  const [datasetPreset, setDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [benchmarkSymbol, setBenchmarkSymbol] = useState("BTCUSDT");
  const [datasetId, setDatasetId] = useState(initialDatasetId ?? "");
  const jobQuery = useJobStatus(jobId);

  useEffect(() => {
    if (initialRunId) {
      setRunId(initialRunId);
      setOpen(true);
    }
    if (initialDatasetId) {
      setDatasetId(initialDatasetId);
    }
  }, [initialRunId]);

  useEffect(() => {
    if (initialDatasetId) {
      setDatasetId(initialDatasetId);
    }
  }, [initialDatasetId]);

  useEffect(() => {
    if (optionsQuery.data?.default_benchmark_symbol) {
      setBenchmarkSymbol(optionsQuery.data.default_benchmark_symbol);
    }
  }, [optionsQuery.data?.default_benchmark_symbol]);

  const mutation = useMutation({
    mutationFn: () =>
      api.launchBacktest({
        run_id: runId,
        dataset_id: datasetId.trim() ? datasetId.trim() : undefined,
        dataset_preset: datasetId.trim() ? undefined : datasetPreset,
        prediction_scope: predictionScope,
        strategy_preset: "sign",
        portfolio_preset: "research_default",
        cost_preset: "standard",
        benchmark_symbol: benchmarkSymbol,
      }),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["backtests"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  const backtestLink = jobQuery.data?.result.deeplinks.backtest_detail ?? null;

  function handleSubmit() {
    if (!runId.trim()) {
      setFormError("\u8bf7\u8f93\u5165 run_id\u3002");
      return;
    }
    if (!benchmarkSymbol.trim()) {
      setFormError("\u8bf7\u8f93\u5165 benchmark \u4ee3\u7801\u3002");
      return;
    }
    if (!datasetId.trim() && !datasetPreset) {
      setFormError("\u8bf7\u5148\u63d0\u4f9b dataset_id \u6216\u9009\u62e9 dataset preset\u3002");
      return;
    }
    setFormError(null);
    mutation.mutate();
  }

  return (
    <div className="drawer-wrap">
      <button
        className="action-button secondary"
        onClick={() => setOpen((value) => !value)}
        type="button"
      >
        {I18N.action.launchBacktest}
      </button>
      {open ? (
        <div className="drawer-panel">
          <h3>{I18N.action.launchBacktest}</h3>
          <label>
            <span>{"\u8bad\u7ec3\u5b9e\u4f8b ID"}</span>
            <input onChange={(event) => setRunId(event.target.value)} value={runId} />
          </label>
          <label>
            <span>{"Dataset ID"}</span>
            <input onChange={(event) => setDatasetId(event.target.value)} value={datasetId} />
          </label>
          {datasetId.trim() ? (
            <div className="dataset-callout">
              <strong>{"\u56de\u6d4b\u5c06\u4f18\u5148\u4f7f\u7528\u8bad\u7ec3\u6570\u636e\u96c6"}</strong>
              <span>
                {
                  "\u5de5\u4f5c\u53f0\u4e3b\u94fe\u5728 dataset-aware \u56de\u6d4b\u4e2d\u4e0d\u4f1a\u9759\u9ed8\u56de\u9000\u5230 smoke / real_benchmark preset\u3002"
                }
              </span>
            </div>
          ) : (
            <label>
              <span>{"\u6570\u636e\u96c6\u9884\u7f6e"}</span>
              <select
                onChange={(event) =>
                  setDatasetPreset(event.target.value as "smoke" | "real_benchmark")
                }
                value={datasetPreset}
              >
                {(optionsQuery.data?.dataset_presets ?? []).map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          )}
          <label>
            <span>{"\u9884\u6d4b\u8303\u56f4"}</span>
            <select
              onChange={(event) => setPredictionScope(event.target.value as "full" | "test")}
              value={predictionScope}
            >
              {(optionsQuery.data?.prediction_scopes ?? []).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>{"\u57fa\u51c6\u4ee3\u7801"}</span>
            <input
              onChange={(event) => setBenchmarkSymbol(event.target.value)}
              value={benchmarkSymbol}
            />
          </label>
          {formError ? <p className="form-error">{formError}</p> : null}
          {mutation.isError ? (
            <p className="form-error">{(mutation.error as Error).message}</p>
          ) : null}
          <button
            className="action-button secondary"
            disabled={mutation.isPending || optionsQuery.isLoading}
            onClick={handleSubmit}
            type="button"
          >
            {mutation.isPending ? "\u63d0\u4ea4\u4e2d..." : I18N.action.submit}
          </button>
          {jobQuery.data ? (
            <div className="job-box">
              <div className="split-line">
                <strong>{jobQuery.data.job_id}</strong>
                <StatusPill status={jobQuery.data.status} />
              </div>
              {jobQuery.data.stages.map((stage) => (
                <div className="job-stage" key={stage.name}>
                  <span>{formatStageNameLabel(stage.name)}</span>
                  <span>{stage.summary}</span>
                </div>
              ))}
              {jobQuery.data.error_message ? (
                <p className="form-error">{jobQuery.data.error_message}</p>
              ) : null}
              {jobQuery.data.status === "success" && backtestLink ? (
                <button
                  className="link-button"
                  onClick={() => navigate(backtestLink)}
                  type="button"
                >
                  {"\u8df3\u8f6c\u5230\u56de\u6d4b\u8be6\u60c5"}
                </button>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
