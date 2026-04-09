import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { api } from "../../shared/api/client";
import { useDatasetReadiness, useJobStatus, useTrainOptions } from "../../shared/api/hooks";
import { formatStageNameLabel } from "../../shared/lib/labels";
import { I18N } from "../../shared/lib/i18n";
import { StatusPill } from "../../shared/ui/StatusPill";

type LaunchTrainDrawerProps = {
  defaultOpen?: boolean;
  datasetId?: string;
  datasetLabel?: string;
  triggerLabel?: string;
  title?: string;
  description?: string;
};

export function LaunchTrainDrawer({
  defaultOpen = false,
  datasetId,
  datasetLabel,
  triggerLabel,
  title,
  description,
}: LaunchTrainDrawerProps = {}) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const optionsQuery = useTrainOptions();
  const readinessQuery = useDatasetReadiness(datasetId ?? null, Boolean(datasetId));
  const [open, setOpen] = useState(defaultOpen);
  const [jobId, setJobId] = useState<string | null>(null);
  const [datasetPreset, setDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [experimentName, setExperimentName] = useState("workbench-train");
  const [modelName, setModelName] = useState("elastic_net");
  const [seed, setSeed] = useState("7");
  const [formError, setFormError] = useState<string | null>(null);

  const jobQuery = useJobStatus(jobId);
  const modelOptions = useMemo(
    () => optionsQuery.data?.model_options ?? [],
    [optionsQuery.data?.model_options],
  );
  const runDetailLink = jobQuery.data?.result.deeplinks.run_detail ?? null;
  const isDatasetAware = Boolean(datasetId);
  const readinessStatus = readinessQuery.data?.readiness_status ?? null;
  const datasetDetailPath = datasetId ? `/datasets/${encodeURIComponent(datasetId)}` : null;

  useEffect(() => {
    if (defaultOpen) {
      setOpen(true);
    }
  }, [defaultOpen, datasetId]);

  const mutation = useMutation({
    mutationFn: () =>
      api.launchTrain({
        dataset_preset: datasetPreset,
        dataset_id: datasetId,
        model_names: [modelName],
        trainer_preset: "fast",
        seed: Number(seed),
        experiment_name: experimentName,
      }),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["runs"] });
      void queryClient.invalidateQueries({ queryKey: ["experiments"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  function handleSubmit() {
    if (!modelName) {
      setFormError("\u8bf7\u9009\u62e9\u6a21\u578b\u3002");
      return;
    }
    if (!experimentName.trim()) {
      setFormError("\u8bf7\u8f93\u5165\u5b9e\u9a8c\u540d\u79f0\u3002");
      return;
    }
    if (!seed.trim() || Number.isNaN(Number(seed))) {
      setFormError("\u8bf7\u8f93\u5165\u6709\u6548\u7684 seed\u3002");
      return;
    }
    if (isDatasetAware && readinessQuery.isLoading) {
      setFormError(
        "\u6b63\u5728\u8bfb\u53d6\u8fd9\u4efd\u6570\u636e\u96c6\u7684\u8bad\u7ec3\u5c31\u7eea\u5ea6\uff0c\u8bf7\u7a0d\u540e\u518d\u8bd5\u3002",
      );
      return;
    }
    if (isDatasetAware && readinessQuery.isError) {
      setFormError(
        "\u65e0\u6cd5\u8bfb\u53d6\u6570\u636e\u96c6\u7684\u8bad\u7ec3\u5c31\u7eea\u5ea6\uff0c\u8bf7\u5148\u56de\u5230\u6570\u636e\u96c6\u8be6\u60c5\u9875\u786e\u8ba4\u72b6\u6001\u3002",
      );
      return;
    }
    if (isDatasetAware && readinessStatus === "not_ready") {
      setFormError(
        "\u5f53\u524d\u6570\u636e\u96c6\u8fd8\u672a\u901a\u8fc7\u8bad\u7ec3\u5c31\u7eea\u6821\u9a8c\uff0c\u8bf7\u5148\u68c0\u67e5\u6570\u636e\u96c6\u8be6\u60c5\u6216\u56de\u5230\u8bad\u7ec3\u6570\u636e\u96c6\u9875\u91cd\u65b0\u9009\u62e9\u3002",
      );
      return;
    }

    setFormError(null);
    mutation.mutate();
  }

  return (
    <div className="drawer-wrap">
      <button className="action-button" onClick={() => setOpen((value) => !value)} type="button">
        {triggerLabel ?? I18N.action.launchTrain}
      </button>
      {open ? (
        <div className="drawer-panel">
          <h3>{title ?? triggerLabel ?? I18N.action.launchTrain}</h3>
          {description ? <p className="drawer-copy">{description}</p> : null}

          {isDatasetAware ? (
            <div className="page-stack compact-gap">
              <div className="dataset-callout">
                <strong>{"\u5f53\u524d\u8bad\u7ec3\u6570\u636e\u96c6"}</strong>
                <span>{datasetLabel ?? datasetId}</span>
              </div>
              <div className="dataset-callout">
                <strong>
                  {readinessQuery.isLoading
                    ? "\u6b63\u5728\u8bfb\u53d6\u8bad\u7ec3\u5c31\u7eea\u5ea6"
                    : readinessQuery.isError
                      ? "\u5c31\u7eea\u5ea6\u8bfb\u53d6\u5931\u8d25"
                      : readinessStatus === "not_ready"
                        ? "\u8fd9\u4efd\u6570\u636e\u96c6\u6682\u4e0d\u53ef\u8bad\u7ec3"
                        : readinessStatus === "warning"
                          ? "\u8fd9\u4efd\u6570\u636e\u96c6\u53ef\u4ee5\u8bad\u7ec3\uff0c\u4f46\u9700\u8981\u5148\u7559\u610f"
                          : "\u8fd9\u4efd\u6570\u636e\u96c6\u53ef\u4ee5\u76f4\u63a5\u53d1\u8d77\u8bad\u7ec3"}
                </strong>
                <span>
                  {readinessQuery.isLoading
                    ? "\u62bd\u5c49\u4f1a\u5148\u7b49\u540e\u7aef\u7684 readiness \u7ed3\u679c\uff0c\u907f\u514d\u672a\u6821\u9a8c\u7684 dataset_id \u76f4\u63a5\u8fdb\u5165\u8bad\u7ec3\u3002"
                    : readinessQuery.isError
                      ? "\u8bf7\u5148\u53bb\u6570\u636e\u96c6\u8be6\u60c5\u9875\u6216\u8bad\u7ec3\u6570\u636e\u96c6\u9875\u786e\u8ba4 readiness \u72b6\u6001\u3002"
                      : readinessStatus === "not_ready"
                        ? "\u8fd9\u4e2a\u5165\u53e3\u4e0d\u4f1a\u9759\u9ed8\u56de\u9000\u5230 preset \u6a21\u5f0f\uff0c\u4f60\u9700\u8981\u5148\u5904\u7406\u963b\u585e\u539f\u56e0\u3002"
                        : readinessStatus === "warning"
                          ? "\u540e\u7aef\u5141\u8bb8\u7ee7\u7eed\u8bad\u7ec3\uff0c\u4f46\u8fd9\u4efd\u6570\u636e\u96c6\u8fd8\u5e26\u6709\u544a\u8b66\uff0c\u5efa\u8bae\u5148\u68c0\u67e5\u8be6\u60c5\u9875\u91cc\u7684 readiness \u8bf4\u660e\u3002"
                          : "\u672c\u6b21\u8bad\u7ec3\u4f1a\u4ee5 dataset_id \u4f5c\u4e3a\u552f\u4e00\u6570\u636e\u96c6\u6765\u6e90\uff0c\u4e0d\u4f1a\u56de\u9000\u5230 preset \u9009\u62e9\u3002"}
                </span>
              </div>
              {!readinessQuery.isLoading && (readinessQuery.isError || readinessStatus === "not_ready") ? (
                <div className="table-actions">
                  {datasetDetailPath ? (
                    <Link className="link-button" to={datasetDetailPath}>
                      {"\u6253\u5f00\u6570\u636e\u96c6\u8be6\u60c5"}
                    </Link>
                  ) : null}
                  <Link className="link-button" to="/datasets/training">
                    {"\u56de\u5230\u8bad\u7ec3\u6570\u636e\u96c6\u9875"}
                  </Link>
                </div>
              ) : null}
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
            <span>{"\u6a21\u578b"}</span>
            <select onChange={(event) => setModelName(event.target.value)} value={modelName}>
              {modelOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>{"\u5b9e\u9a8c\u540d\u79f0"}</span>
            <input onChange={(event) => setExperimentName(event.target.value)} value={experimentName} />
          </label>
          <label>
            <span>Seed</span>
            <input inputMode="numeric" onChange={(event) => setSeed(event.target.value)} value={seed} />
          </label>
          {formError ? <p className="form-error">{formError}</p> : null}
          {mutation.isError ? (
            <p className="form-error">{(mutation.error as Error).message}</p>
          ) : null}
          <button
            className="action-button"
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
              {jobQuery.data.status === "success" && runDetailLink ? (
                <button
                  className="link-button"
                  onClick={() => navigate(runDetailLink)}
                  type="button"
                >
                  {"\u8df3\u8f6c\u5230\u8fd0\u884c\u8be6\u60c5"}
                </button>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
