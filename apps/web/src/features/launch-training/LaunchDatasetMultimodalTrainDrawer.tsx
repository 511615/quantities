import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../../shared/api/client";
import {
  useBacktestOptions,
  useDatasetReadiness,
  useJobStatus,
  useTrainOptions,
} from "../../shared/api/hooks";
import type { ModalityQualityView } from "../../shared/api/types";
import { formatModalityLabel, formatStageNameLabel } from "../../shared/lib/labels";
import { ModalityQualitySummary } from "../../shared/ui/ModalityQualitySummary";
import { StatusPill } from "../../shared/ui/StatusPill";

type SupportedModality = "market" | "macro" | "on_chain" | "derivatives" | "nlp";
type OfficialWindowDays = 30 | 90 | 180 | 365;
type FusionStrategy = "attention_late_fusion" | "late_score_blend";

type LaunchDatasetMultimodalTrainDrawerProps = {
  datasetId: string;
  datasetLabel?: string;
  datasetModalities: SupportedModality[];
  triggerLabel?: string;
};

const SUPPORTED_MODALITIES: SupportedModality[] = ["market", "macro", "on_chain", "derivatives", "nlp"];
const OFFICIAL_WINDOW_OPTIONS: OfficialWindowDays[] = [30, 90, 180, 365];

function blockingReasonForModality(item: ModalityQualityView | null | undefined) {
  if (!item) {
    return "该模态暂时没有质量摘要。";
  }
  if (item.status === "ready") {
    return null;
  }
  return item.blocking_reasons[0] ?? `${formatModalityLabel(item.modality)} 当前不允许训练。`;
}

function parseOfficialWindowDays(value: string): OfficialWindowDays {
  const parsed = Number.parseInt(value, 10);
  if (OFFICIAL_WINDOW_OPTIONS.includes(parsed as OfficialWindowDays)) {
    return parsed as OfficialWindowDays;
  }
  return 30;
}

export function LaunchDatasetMultimodalTrainDrawer({
  datasetId,
  datasetLabel,
  datasetModalities,
  triggerLabel = "按模态训练并融合",
}: LaunchDatasetMultimodalTrainDrawerProps) {
  const queryClient = useQueryClient();
  const trainOptionsQuery = useTrainOptions();
  const backtestOptionsQuery = useBacktestOptions();
  const readinessQuery = useDatasetReadiness(datasetId, Boolean(datasetId));
  const [open, setOpen] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [selectedModalities, setSelectedModalities] = useState<SupportedModality[]>([]);
  const [templateByModality, setTemplateByModality] = useState<Partial<Record<SupportedModality, string>>>({});
  const [experimentNamePrefix, setExperimentNamePrefix] = useState("workbench-multimodal");
  const [compositionName, setCompositionName] = useState("Multimodal Composite");
  const [seed, setSeed] = useState("7");
  const [fusionStrategy, setFusionStrategy] = useState<FusionStrategy>("attention_late_fusion");
  const [autoLaunchOfficialBacktest, setAutoLaunchOfficialBacktest] = useState(true);
  const [officialWindowDays, setOfficialWindowDays] = useState<OfficialWindowDays>(30);
  const [formError, setFormError] = useState<string | null>(null);
  const jobQuery = useJobStatus(jobId);

  const templateOptions = useMemo(
    () => trainOptionsQuery.data?.template_options ?? [],
    [trainOptionsQuery.data?.template_options],
  );
  const officialWindowOptions = useMemo(() => {
    const options = backtestOptionsQuery.data?.official_window_options ?? [];
    if (options.length === 0) {
      return OFFICIAL_WINDOW_OPTIONS.map((value) => ({ value: String(value), label: `${value}d` }));
    }
    return options.map((item) => ({ value: item.value, label: item.label ?? item.value }));
  }, [backtestOptionsQuery.data?.official_window_options]);
  const availableModalities = useMemo(
    () =>
      Array.from(
        new Set(
          datasetModalities
            .map((item) => item.trim())
            .filter((item): item is SupportedModality => SUPPORTED_MODALITIES.includes(item as SupportedModality)),
        ),
      ),
    [datasetModalities],
  );

  useEffect(() => {
    if (templateOptions.length === 0) {
      return;
    }
    const defaultTemplateId =
      templateOptions.find((option) => option.recommended)?.value ?? templateOptions[0]?.value ?? "";
    setTemplateByModality((current) => {
      const next = { ...current };
      let changed = false;
      for (const modality of availableModalities) {
        if (!next[modality]) {
          next[modality] = defaultTemplateId;
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [availableModalities, templateOptions]);

  useEffect(() => {
    const defaultSeed = trainOptionsQuery.data?.default_seed;
    if (defaultSeed === undefined) {
      return;
    }
    setSeed((current) => (current === "" || current === "7" ? String(defaultSeed) : current));
  }, [trainOptionsQuery.data?.default_seed]);

  useEffect(() => {
    const recommendedWindow =
      backtestOptionsQuery.data?.official_window_options?.find((item) => item.recommended)?.value ?? "30";
    setOfficialWindowDays((current) => {
      if (current !== 30) {
        return current;
      }
      return parseOfficialWindowDays(recommendedWindow);
    });
  }, [backtestOptionsQuery.data?.official_window_options]);

  const mutation = useMutation({
    mutationFn: () =>
      api.launchDatasetMultimodalTrain({
        dataset_id: datasetId,
        selected_modalities: selectedModalities,
        template_by_modality: templateByModality,
        trainer_preset: "fast",
        experiment_name_prefix: experimentNamePrefix.trim(),
        seed: Number(seed),
        fusion_strategy: fusionStrategy,
        composition_name: compositionName.trim(),
        auto_launch_official_backtest: autoLaunchOfficialBacktest,
        official_window_days: autoLaunchOfficialBacktest ? officialWindowDays : null,
      }),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      void Promise.all([
        queryClient.invalidateQueries({ queryKey: ["jobs"] }),
        queryClient.invalidateQueries({ queryKey: ["runs"] }),
        queryClient.invalidateQueries({ queryKey: ["backtests"] }),
        queryClient.invalidateQueries({ queryKey: ["experiments"] }),
        queryClient.invalidateQueries({ queryKey: ["workbench-overview"] }),
      ]);
    },
  });

  function toggleModality(modality: SupportedModality, checked: boolean) {
    const modalityQuality = readinessQuery.data?.modality_quality_summary?.[modality] ?? null;
    const blockingReason = blockingReasonForModality(modalityQuality);
    if (checked && blockingReason) {
      setFormError(blockingReason);
      return;
    }
    setFormError(null);
    setSelectedModalities((current) =>
      checked ? Array.from(new Set([...current, modality])) : current.filter((item) => item !== modality),
    );
  }

  function submit() {
    if (selectedModalities.length < 2) {
      setFormError("请至少选择两个质量达标的模态。");
      return;
    }
    if (!experimentNamePrefix.trim()) {
      setFormError("请输入实验名前缀。");
      return;
    }
    if (!compositionName.trim()) {
      setFormError("请输入融合模型名称。");
      return;
    }
    if (!seed.trim() || Number.isNaN(Number(seed))) {
      setFormError("请输入有效的随机种子。");
      return;
    }
    for (const modality of selectedModalities) {
      if (!templateByModality[modality]) {
        setFormError(`请为 ${formatModalityLabel(modality)} 选择模板。`);
        return;
      }
    }
    setFormError(null);
    mutation.mutate();
  }

  return (
    <div className="drawer-wrap">
      <button
        className="action-button secondary"
        data-testid="launch-dataset-multimodal-train-trigger"
        onClick={() => setOpen((value) => !value)}
        type="button"
      >
        {triggerLabel}
      </button>
      {open ? (
        <div className="drawer-panel">
          <h3>按模态训练并自动编排</h3>
          <p className="drawer-copy">
            这条流程会先分别训练单模态 run，再做晚期融合。默认使用带注意力分配的
            `attention_late_fusion`，也可以手动切回 `late_score_blend` 做对照。你也可以让系统在融合完成后自动发起
            official backtest，形成完整闭环。
          </p>

          <div className="dataset-callout">
            <strong>当前数据集</strong>
            <span>{datasetLabel ?? datasetId}</span>
          </div>

          {readinessQuery.data?.modality_quality_summary ? (
            <section className="details-panel">
              <strong>数据质量摘要</strong>
              <ModalityQualitySummary
                modalities={availableModalities}
                summary={readinessQuery.data.modality_quality_summary}
                title="Dataset modality quality"
              />
            </section>
          ) : null}

          <section className="details-panel">
            <strong>模态选择与模板</strong>
            <div className="stack-list">
              {availableModalities.map((modality) => {
                const modalityQuality = readinessQuery.data?.modality_quality_summary?.[modality] ?? null;
                const blockingReason = blockingReasonForModality(modalityQuality);
                const isReady = !blockingReason;
                const checked = selectedModalities.includes(modality);
                return (
                  <div className="stack-item align-start" key={modality}>
                    <div className="split-line">
                      <label className="table-actions">
                        <input
                          checked={checked}
                          data-testid={`multimodal-modality-${modality}`}
                          disabled={!isReady}
                          onChange={(event) => toggleModality(modality, event.target.checked)}
                          type="checkbox"
                        />
                        <strong>{formatModalityLabel(modality)}</strong>
                      </label>
                      <span>{isReady ? "可训练" : "不可选择"}</span>
                    </div>
                    <select
                      className="field"
                      data-testid={`multimodal-template-${modality}`}
                      disabled={!checked}
                      onChange={(event) =>
                        setTemplateByModality((current) => ({
                          ...current,
                          [modality]: event.target.value,
                        }))
                      }
                      value={templateByModality[modality] ?? ""}
                    >
                      <option value="">选择模板</option>
                      {templateOptions.map((option) => (
                        <option key={`${modality}-${option.value}`} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                    {blockingReason ? <span>{blockingReason}</span> : null}
                  </div>
                );
              })}
            </div>
          </section>

          <label>
            <span>实验名前缀</span>
            <input
              className="field"
              data-testid="dataset-multimodal-experiment-prefix"
              onChange={(event) => setExperimentNamePrefix(event.target.value)}
              value={experimentNamePrefix}
            />
          </label>
          <label>
            <span>融合模型名称</span>
            <input
              className="field"
              data-testid="dataset-multimodal-composition-name"
              onChange={(event) => setCompositionName(event.target.value)}
              value={compositionName}
            />
          </label>
          <label>
            <span>融合策略</span>
            <select
              className="field"
              data-testid="dataset-multimodal-fusion-strategy"
              onChange={(event) => setFusionStrategy(event.target.value as FusionStrategy)}
              value={fusionStrategy}
            >
              <option value="attention_late_fusion">attention_late_fusion (默认)</option>
              <option value="late_score_blend">late_score_blend</option>
            </select>
          </label>
          <label>
            <span>Seed</span>
            <input
              className="field"
              data-testid="dataset-multimodal-seed"
              inputMode="numeric"
              onChange={(event) => setSeed(event.target.value)}
              value={seed}
            />
          </label>

          <section className="details-panel">
            <div className="split-line">
              <strong>自动 official backtest</strong>
              <label className="table-actions">
                <input
                  checked={autoLaunchOfficialBacktest}
                  data-testid="dataset-multimodal-auto-backtest-toggle"
                  onChange={(event) => setAutoLaunchOfficialBacktest(event.target.checked)}
                  type="checkbox"
                />
                <span>{autoLaunchOfficialBacktest ? "已开启" : "关闭"}</span>
              </label>
            </div>
            <p className="drawer-copy">
              开启后会在融合完成后自动用 official template 发起回测。如果当前组合不满足官方协议，训练和融合产物仍会保留，但作业会在回测阶段给出精确失败原因。
            </p>
            <label>
              <span>官方回测窗口</span>
              <select
                className="field"
                data-testid="dataset-multimodal-official-window"
                disabled={!autoLaunchOfficialBacktest}
                onChange={(event) => setOfficialWindowDays(parseOfficialWindowDays(event.target.value))}
                value={String(officialWindowDays)}
              >
                {officialWindowOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          </section>

          {formError ? <p className="form-error">{formError}</p> : null}
          {mutation.isError ? <p className="form-error">{(mutation.error as Error).message}</p> : null}

          <button
            className="action-button"
            data-testid="dataset-multimodal-submit"
            disabled={mutation.isPending}
            onClick={submit}
            type="button"
          >
            {mutation.isPending
              ? "提交中..."
              : autoLaunchOfficialBacktest
                ? "开始训练、融合并官方回测"
                : "开始训练并融合"}
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
              {jobQuery.data.error_message ? <p className="form-error">{jobQuery.data.error_message}</p> : null}
              <div className="table-actions">
                {jobQuery.data.result.deeplinks.run_detail ? (
                  <Link className="link-button" to={jobQuery.data.result.deeplinks.run_detail}>
                    打开融合模型
                  </Link>
                ) : null}
                {jobQuery.data.result.deeplinks.backtest_detail ? (
                  <Link className="link-button" to={jobQuery.data.result.deeplinks.backtest_detail}>
                    打开官方回测
                  </Link>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
