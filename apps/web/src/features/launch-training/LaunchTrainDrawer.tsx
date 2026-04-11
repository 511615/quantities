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
  showTrigger?: boolean;
  datasetId?: string;
  datasetLabel?: string;
  triggerLabel?: string;
  title?: string;
  description?: string;
  initialTemplateId?: string | null;
  onJobCreated?: (jobId: string) => void;
};

export function LaunchTrainDrawer({
  defaultOpen = false,
  showTrigger = true,
  datasetId,
  datasetLabel,
  triggerLabel,
  title,
  description,
  initialTemplateId = null,
  onJobCreated,
}: LaunchTrainDrawerProps = {}) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const optionsQuery = useTrainOptions();
  const readinessQuery = useDatasetReadiness(datasetId ?? null, Boolean(datasetId));
  const [open, setOpen] = useState(defaultOpen || !showTrigger);
  const [jobId, setJobId] = useState<string | null>(null);
  const [datasetPreset, setDatasetPreset] = useState<"" | "smoke" | "real_benchmark">("");
  const [experimentName, setExperimentName] = useState("workbench-train");
  const [templateId, setTemplateId] = useState(initialTemplateId ?? "");
  const [seed, setSeed] = useState("7");
  const [formError, setFormError] = useState<string | null>(null);

  const jobQuery = useJobStatus(jobId);
  const templateOptions = useMemo(
    () => optionsQuery.data?.template_options ?? [],
    [optionsQuery.data?.template_options],
  );
  const runDetailLink = jobQuery.data?.result.deeplinks.run_detail ?? null;
  const isDatasetAware = Boolean(datasetId);
  const readinessStatus = readinessQuery.data?.readiness_status ?? null;
  const datasetDetailPath = datasetId ? `/datasets/${encodeURIComponent(datasetId)}` : null;
  const selectedTemplate = useMemo(
    () => templateOptions.find((option) => option.value === templateId) ?? null,
    [templateId, templateOptions],
  );

  useEffect(() => {
    if (defaultOpen || !showTrigger) {
      setOpen(true);
    }
  }, [defaultOpen, showTrigger]);

  useEffect(() => {
    if (initialTemplateId) {
      setTemplateId(initialTemplateId);
      return;
    }
    if (templateOptions.length === 0) {
      return;
    }
    setTemplateId((current) => {
      if (current && templateOptions.some((option) => option.value === current)) {
        return current;
      }
      return (
        templateOptions.find((option) => option.recommended)?.value ?? templateOptions[0]?.value ?? ""
      );
    });
  }, [initialTemplateId, templateOptions]);

  useEffect(() => {
    const defaultSeed = optionsQuery.data?.default_seed;
    if (defaultSeed === undefined) {
      return;
    }
    setSeed((current) => (current === "" || current === "7" ? String(defaultSeed) : current));
  }, [optionsQuery.data?.default_seed]);

  const mutation = useMutation({
    mutationFn: () =>
      api.launchTrain({
        ...(datasetPreset ? { dataset_preset: datasetPreset } : {}),
        ...(datasetId ? { dataset_id: datasetId } : {}),
        template_id: templateId,
        seed: Number(seed),
        experiment_name: experimentName,
      }),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      onJobCreated?.(result.job_id);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["runs"] });
      void queryClient.invalidateQueries({ queryKey: ["experiments"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  function handleSubmit() {
    if (!templateId) {
      setFormError("请选择模型模板。");
      return;
    }
    if (!experimentName.trim()) {
      setFormError("请输入实验名称。");
      return;
    }
    if (!seed.trim() || Number.isNaN(Number(seed))) {
      setFormError("请输入有效的 seed。");
      return;
    }
    if (isDatasetAware && readinessQuery.isLoading) {
      setFormError("正在读取这份数据集的训练就绪度，请稍后再试。");
      return;
    }
    if (isDatasetAware && readinessQuery.isError) {
      setFormError("无法读取数据集的训练就绪度，请先到数据集详情页确认状态。");
      return;
    }
    if (isDatasetAware && readinessStatus === "not_ready") {
      setFormError("当前数据集还未通过训练就绪校验，请先处理阻塞问题。");
      return;
    }

    setFormError(null);
    mutation.mutate();
  }

  return (
    <div className="drawer-wrap">
      {showTrigger ? (
        <button className="action-button" onClick={() => setOpen((value) => !value)} type="button">
          {triggerLabel ?? I18N.action.launchTrain}
        </button>
      ) : null}
      {open ? (
        <div className="drawer-panel">
          <h3>{title ?? triggerLabel ?? I18N.action.launchTrain}</h3>
          {description ? <p className="drawer-copy">{description}</p> : null}

          {isDatasetAware ? (
            <div className="page-stack compact-gap">
              <div className="dataset-callout">
                <strong>{"当前训练数据集"}</strong>
                <span>{datasetLabel ?? datasetId}</span>
              </div>
              <div className="dataset-callout">
                <strong>
                  {readinessQuery.isLoading
                    ? "正在读取训练就绪度"
                    : readinessQuery.isError
                      ? "就绪度读取失败"
                      : readinessStatus === "not_ready"
                        ? "这份数据集暂不可训练"
                        : readinessStatus === "warning"
                          ? "这份数据集可以训练，但需要先留意"
                          : "这份数据集可以直接发起训练"}
                </strong>
                <span>
                  {readinessQuery.isLoading
                    ? "抽屉会先等待后端 readiness 结果，避免未校验的数据集直接进入训练。"
                    : readinessQuery.isError
                      ? "请先到数据集详情页或训练数据集页确认 readiness 状态。"
                      : readinessStatus === "not_ready"
                        ? "这个入口不会静默回退到 preset 模式，你需要先处理阻塞原因。"
                        : readinessStatus === "warning"
                          ? "后端允许继续训练，但这份数据集还带有告警，建议先检查详情页里的说明。"
                          : "本次训练会直接以 dataset_id 作为唯一数据集来源，不会回退到 preset 选择。"}
                </span>
              </div>
              {!readinessQuery.isLoading && (readinessQuery.isError || readinessStatus === "not_ready") ? (
                <div className="table-actions">
                  {datasetDetailPath ? (
                    <Link className="link-button" to={datasetDetailPath}>
                      {"打开数据集详情"}
                    </Link>
                  ) : null}
                  <Link className="link-button" to="/datasets/training">
                    {"回到训练数据集页"}
                  </Link>
                </div>
              ) : null}
            </div>
          ) : (
            <label>
              <span>{"数据集预置"}</span>
              <select
                onChange={(event) =>
                  setDatasetPreset(event.target.value as "" | "smoke" | "real_benchmark")
                }
                value={datasetPreset}
              >
                <option value="">使用模板默认数据集</option>
                {(optionsQuery.data?.dataset_presets ?? []).map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          )}

          <label>
            <span>{"模型模板"}</span>
            <select onChange={(event) => setTemplateId(event.target.value)} value={templateId}>
              {templateOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          {selectedTemplate?.description ? (
            <p className="drawer-copy">{selectedTemplate.description}</p>
          ) : null}
          <label>
            <span>{"实验名称"}</span>
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
            disabled={mutation.isPending || optionsQuery.isLoading || templateOptions.length === 0}
            onClick={handleSubmit}
            type="button"
          >
            {mutation.isPending ? "提交中..." : I18N.action.submit}
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
                  {"跳转到运行详情"}
                </button>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
