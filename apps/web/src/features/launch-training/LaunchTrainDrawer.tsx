import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../../shared/api/client";
import { useDatasetReadiness, useJobStatus, useTrainOptions } from "../../shared/api/hooks";
import type { ModalityQualityView } from "../../shared/api/types";
import { I18N, translateText } from "../../shared/lib/i18n";
import { formatModalityLabel, formatStageNameLabel, formatStatusLabel } from "../../shared/lib/labels";
import { ModalityQualitySummary } from "../../shared/ui/ModalityQualitySummary";
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

type FeatureScopeModality = "market" | "macro" | "on_chain" | "derivatives" | "nlp";

const MODALITIES: FeatureScopeModality[] = ["market", "macro", "on_chain", "derivatives", "nlp"];

function localizeTrainOptionLabel(value: string, label?: string | null) {
  const normalized = (label ?? value).trim().toLowerCase();
  if (normalized === "smoke") {
    return translateText("联调样本");
  }
  if (normalized === "real_benchmark") {
    return translateText("真实基准");
  }
  if (normalized === "elastic net default") {
    return translateText("Elastic Net Default");
  }
  return label ?? value;
}

function localizeTrainOptionDescription(description?: string | null) {
  if (!description) {
    return null;
  }
  if (description.trim().toLowerCase() === "template sourced from model registry.") {
    return translateText("模板直接来自后端注册信息与持久化存储。");
  }
  return description;
}

function modalityBlockingSummary(item: ModalityQualityView | null) {
  if (!item) {
    return null;
  }
  if (item.blocking_reasons.length > 0) {
    return item.blocking_reasons[0];
  }
  if (item.status === "ready") {
    return translateText("可训练");
  }
  if (item.status === "warning") {
    return translateText("需留意");
  }
  if (item.status === "failed") {
    return translateText("暂不可训练");
  }
  return formatStatusLabel(item.status);
}

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
  const optionsQuery = useTrainOptions();
  const readinessQuery = useDatasetReadiness(datasetId ?? null, Boolean(datasetId));
  const [open, setOpen] = useState(defaultOpen || !showTrigger);
  const [jobId, setJobId] = useState<string | null>(null);
  const [datasetPreset, setDatasetPreset] = useState<"" | "smoke" | "real_benchmark">("");
  const [experimentName, setExperimentName] = useState("workbench-train");
  const [templateId, setTemplateId] = useState(initialTemplateId ?? "");
  const [featureScopeModality, setFeatureScopeModality] = useState<FeatureScopeModality | "">("");
  const [seed, setSeed] = useState("7");
  const [formError, setFormError] = useState<string | null>(null);

  const jobQuery = useJobStatus(jobId);
  const templateOptions = useMemo(
    () => optionsQuery.data?.template_options ?? [],
    [optionsQuery.data?.template_options],
  );
  const modalityOptions = useMemo(
    () =>
      (optionsQuery.data?.feature_scope_modalities ?? []).filter((option) =>
        MODALITIES.includes(option.value as FeatureScopeModality),
      ),
    [optionsQuery.data?.feature_scope_modalities],
  );
  const selectedTemplate = useMemo(
    () => templateOptions.find((option) => option.value === templateId) ?? null,
    [templateId, templateOptions],
  );
  const readinessStatus = readinessQuery.data?.readiness_status ?? null;
  const selectedModalityQuality =
    featureScopeModality && datasetId
      ? readinessQuery.data?.modality_quality_summary?.[featureScopeModality] ?? null
      : null;
  const datasetDetailPath = datasetId ? `/datasets/${encodeURIComponent(datasetId)}` : null;
  const runDetailLink = jobQuery.data?.result?.deeplinks?.run_detail ?? null;

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
        feature_scope_modality: featureScopeModality || undefined,
        seed: Number(seed),
        experiment_name: experimentName.trim(),
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
      setFormError(translateText("请选择模型类型。"));
      return;
    }
    if (!featureScopeModality) {
      setFormError(translateText("请先为这次训练选择一个模态。"));
      return;
    }
    if (!experimentName.trim()) {
      setFormError(translateText("请输入实验名称。"));
      return;
    }
    if (!seed.trim() || Number.isNaN(Number(seed))) {
      setFormError(translateText("请输入有效的随机种子。"));
      return;
    }
    if (datasetId && readinessQuery.isLoading) {
      setFormError(translateText("数据集就绪状态仍在加载，请稍后再试。"));
      return;
    }
    if (datasetId && readinessQuery.isError) {
      setFormError(translateText("无法加载数据集就绪状态，请先检查数据集详情页。"));
      return;
    }
    if (datasetId && readinessStatus === "not_ready") {
      setFormError(translateText("当前数据集还不能训练，因为 readiness 检查尚未通过。"));
      return;
    }
    if (datasetId && selectedModalityQuality && selectedModalityQuality.status !== "ready") {
      setFormError(
        selectedModalityQuality.blocking_reasons[0] ??
          translateText("所选模态 {modality} 还没有达到可训练状态。").replace("{modality}", formatModalityLabel(featureScopeModality)),
      );
      return;
    }

    setFormError(null);
    mutation.mutate();
  }

  return (
    <div className="drawer-wrap">
      {showTrigger ? (
        <button
          className="action-button"
          data-testid="launch-train-trigger"
          onClick={() => setOpen((value) => !value)}
          type="button"
        >
          {triggerLabel ?? I18N.action.launchTrain}
        </button>
      ) : null}
      {open ? (
        <div className="drawer-panel">
          <h3>{title ?? triggerLabel ?? I18N.action.launchTrain}</h3>
          {description ? <p className="drawer-copy">{description}</p> : null}

          {datasetId ? (
            <div className="page-stack compact-gap">
              <div className="dataset-callout">
                <strong>{translateText("当前训练数据集")}</strong>
                <span>{datasetLabel ?? datasetId}</span>
              </div>
              <div className="dataset-callout">
                <strong>
                  {readinessQuery.isLoading
                    ? translateText("正在加载就绪状态")
                    : readinessQuery.isError
                      ? translateText("就绪状态不可用")
                      : `${translateText("数据集状态")}：${formatStatusLabel(readinessStatus)}`}
                </strong>
                <span>
                  {readinessQuery.isLoading
                    ? translateText("发起绑定数据集的训练前，需要先等就绪状态加载完成。")
                    : readinessQuery.isError
                      ? translateText("请打开数据集详情页检查 readiness 返回结果。")
                      : readinessStatus === "not_ready"
                        ? translateText("数据集本身仍会保留，但在 readiness 变为可训练前，这里不会允许继续训练。")
                        : translateText("这次训练会直接使用当前数据集，而不是回退到预置数据集。")}
                </span>
              </div>

              {readinessQuery.data?.modality_quality_summary ? (
                <section className="details-panel">
                  <strong>{translateText("数据集模态质量")}</strong>
                  <ModalityQualitySummary
                    emptyText={translateText("当前数据集还没有可展示的模态质量摘要。")}
                    summary={readinessQuery.data.modality_quality_summary}
                    title={translateText("数据集模态质量")}
                  />
                </section>
              ) : null}

              {!readinessQuery.isLoading && (readinessQuery.isError || readinessStatus === "not_ready") ? (
                <div className="table-actions">
                  {datasetDetailPath ? (
                    <Link className="link-button" to={datasetDetailPath}>
                      {translateText("打开数据集详情")}
                    </Link>
                  ) : null}
                  <Link className="link-button" to="/datasets/training">
                    {translateText("返回训练数据集")}
                  </Link>
                </div>
              ) : null}
            </div>
          ) : (
            <label>
              <span>{translateText("数据集预置")}</span>
              <select
                onChange={(event) =>
                  setDatasetPreset(event.target.value as "" | "smoke" | "real_benchmark")
                }
                value={datasetPreset}
              >
                <option value="">{translateText("使用模板默认数据集预置")}</option>
                {(optionsQuery.data?.dataset_presets ?? []).map((option) => (
                  <option key={option.value} value={option.value}>
                    {localizeTrainOptionLabel(option.value, option.label)}
                  </option>
                ))}
              </select>
            </label>
          )}

          <label>
            <span>{translateText("模板")}</span>
            <select onChange={(event) => setTemplateId(event.target.value)} value={templateId}>
              {templateOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {localizeTrainOptionLabel(option.value, option.label)}
                </option>
              ))}
            </select>
          </label>
          {localizeTrainOptionDescription(selectedTemplate?.description) ? (
            <p className="drawer-copy">{localizeTrainOptionDescription(selectedTemplate?.description)}</p>
          ) : null}

          <label>
            <span>{translateText("特征模态")}</span>
            <select
              aria-label={translateText("特征模态")}
              data-testid="feature-modality-select"
              onChange={(event) =>
                setFeatureScopeModality(event.target.value as FeatureScopeModality | "")
              }
              value={featureScopeModality}
            >
              <option value="">{translateText("请选择模态")}</option>
              {modalityOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label ?? formatModalityLabel(option.value)}
                </option>
              ))}
            </select>
          </label>

          {featureScopeModality ? (
            <div className="dataset-callout">
              <strong>{`${translateText("已选模态")}：${formatModalityLabel(featureScopeModality)}`}</strong>
              <span>
                {selectedModalityQuality
                  ? modalityBlockingSummary(selectedModalityQuality)
                  : translateText("这次训练会只使用当前选中的模态特征范围。")}
              </span>
            </div>
          ) : null}

          {selectedModalityQuality ? (
            <section className="details-panel">
              <strong>{translateText("已选模态质量")}</strong>
              <ModalityQualitySummary
                modalities={[featureScopeModality]}
                summary={{ [featureScopeModality]: selectedModalityQuality }}
                title={translateText("已选模态质量")}
              />
            </section>
          ) : null}

          <label>
            <span>{translateText("名称")}</span>
            <input onChange={(event) => setExperimentName(event.target.value)} value={experimentName} />
          </label>
          <label>
            <span>{translateText("随机种子")}</span>
            <input inputMode="numeric" onChange={(event) => setSeed(event.target.value)} value={seed} />
          </label>

          {formError ? <p className="form-error">{formError}</p> : null}
          {mutation.isError ? <p className="form-error">{(mutation.error as Error).message}</p> : null}

          <button
            className="action-button"
            data-testid="submit-train-launch"
            disabled={mutation.isPending || optionsQuery.isLoading || templateOptions.length === 0}
            onClick={handleSubmit}
            type="button"
          >
            {mutation.isPending ? translateText("提交中...") : I18N.action.submit}
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
                <Link className="link-button" to={runDetailLink}>
                  {translateText("跳转运行详情")}
                </Link>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
