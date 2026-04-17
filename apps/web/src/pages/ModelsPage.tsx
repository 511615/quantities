import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import {
  useCreateModelTemplateMutation,
  useDatasetDetail,
  useDeleteModelTemplateMutation,
  useJobStatus,
  useLaunchModelCompositionMutation,
  useModelTemplates,
  useRuns,
  useTrainOptions,
  useUpdateModelTemplateMutation,
} from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { I18N, translateText } from "../shared/lib/i18n";
import {
  type TemplateDraft,
  buildTemplateDraft,
  modelCategory,
  modelLabel,
  modelSuitableData,
  summarizeTemplateParameters,
  templateDraftFromRun,
  templateDraftFromView,
} from "../shared/lib/modelRegistry";
import { ConfirmDialog } from "../shared/ui/ConfirmDialog";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

type TemplateEditorMode = "create" | "edit";

type TrainedModelMeta = {
  runId: string;
  displayName: string;
  note: string;
  hidden?: boolean;
};

const TRAINED_MODELS_STORAGE_KEY = "quant-workbench:trained-models";

function loadRunMeta(): Record<string, TrainedModelMeta> {
  const stored = window.localStorage.getItem(TRAINED_MODELS_STORAGE_KEY);
  if (!stored) {
    return {};
  }
  try {
    return JSON.parse(stored) as Record<string, TrainedModelMeta>;
  } catch {
    return {};
  }
}

function buildHyperparamsText(hyperparams: Record<string, unknown>): string {
  return JSON.stringify(hyperparams, null, 2);
}

function duplicateDraft(template: TemplateDraft): TemplateDraft {
  return {
    ...template,
    template_id: undefined,
    name: `${template.name} ${translateText("副本")}`,
    read_only: false,
  };
}

function applyModelDefaults(current: TemplateDraft, modelName: string): TemplateDraft {
  const next = buildTemplateDraft(modelName);
  return {
    ...current,
    model_name: modelName,
    hyperparams: next.hyperparams,
  };
}

function localizeTemplateDisplayText(value?: string | null) {
  const normalized = (value ?? "").trim().toLowerCase();
  if (normalized === "elastic net default") {
    return "Elastic Net Default";
  }
  if (normalized === "smoke") {
    return "Smoke";
  }
  if (normalized === "real_benchmark") {
    return "Real Benchmark";
  }
  if (normalized === "fast") {
    return "Fast";
  }
  return value ?? "--";
}

function isInternalHelperDatasetId(datasetId: string) {
  return datasetId.endsWith("_market_anchor");
}

function datasetIdsForRun(run: {
  dataset_id: string | null;
  dataset_ids?: string[];
  datasets?: Array<{ dataset_id: string }>;
  primary_dataset_id?: string | null;
  composition?: Record<string, unknown> | null;
  tags?: Record<string, string>;
}) {
  const rawIds = run.datasets?.length
    ? run.datasets.map((item) => item.dataset_id)
    : run.dataset_ids?.length
      ? run.dataset_ids
      : run.tags?.dataset_ids
        ? run.tags.dataset_ids.split(",")
        : run.dataset_id
          ? [run.dataset_id]
          : [];
  const deduped = Array.from(new Set(rawIds.map((item) => item.trim()).filter(Boolean)));
  if (run.composition) {
    return deduped;
  }
  const visibleIds = deduped.filter((item) => !isInternalHelperDatasetId(item));
  return visibleIds.length > 0 ? visibleIds : deduped;
}

function singleDatasetIdForRun(run: {
  dataset_id: string | null;
  dataset_ids?: string[];
  tags?: Record<string, string>;
}) {
  const ids = datasetIdsForRun(run);
  return ids.length === 1 ? ids[0] : null;
}

export function ModelsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [runMeta, setRunMeta] = useState<Record<string, TrainedModelMeta>>(() => loadRunMeta());
  const [templateMode, setTemplateMode] = useState<TemplateEditorMode>("create");
  const [templateDraft, setTemplateDraft] = useState<TemplateDraft | null>(null);
  const [hyperparamsText, setHyperparamsText] = useState("{}");
  const [templateError, setTemplateError] = useState<string | null>(null);
  const [templateDeleteId, setTemplateDeleteId] = useState<string | null>(null);
  const [runDeleteId, setRunDeleteId] = useState<string | null>(null);
  const [editingRunId, setEditingRunId] = useState<string | null>(null);
  const [runNoteDraft, setRunNoteDraft] = useState({ displayName: "", note: "" });
  const [runSearch, setRunSearch] = useState("");
  const deferredRunSearch = useDeferredValue(runSearch);
  const [selectedRunIdForBacktest, setSelectedRunIdForBacktest] = useState<string | null>(null);
  const [selectedRunIdsForComposition, setSelectedRunIdsForComposition] = useState<string[]>([]);
  const [compositionName, setCompositionName] = useState("Multimodal Composite");
  const [compositionError, setCompositionError] = useState<string | null>(null);
  const [compositionJobId, setCompositionJobId] = useState<string | null>(null);

  const activeTab = searchParams.get("tab") === "trained" ? "trained" : "templates";
  const launchTrainRequested = searchParams.get("launchTrain") === "1";
  const requestedDatasetId = searchParams.get("datasetId");
  const requestedDatasetQuery = useDatasetDetail(
    launchTrainRequested && requestedDatasetId ? requestedDatasetId : null,
  );

  const params = useMemo(() => {
    const next = new URLSearchParams({
      page: "1",
      per_page: "100",
      sort_by: "created_at",
      sort_order: "desc",
    });
    if (deferredRunSearch) {
      next.set("search", deferredRunSearch);
    }
    return next;
  }, [deferredRunSearch]);

  const templatesQuery = useModelTemplates();
  const trainOptionsQuery = useTrainOptions();
  const runsQuery = useRuns(params);
  const createTemplateMutation = useCreateModelTemplateMutation();
  const updateTemplateMutation = useUpdateModelTemplateMutation();
  const deleteTemplateMutation = useDeleteModelTemplateMutation();
  const compositionMutation = useLaunchModelCompositionMutation();
  const compositionJobQuery = useJobStatus(compositionJobId);

  useEffect(() => {
    window.localStorage.setItem(TRAINED_MODELS_STORAGE_KEY, JSON.stringify(runMeta));
  }, [runMeta]);

  const modelOptions = trainOptionsQuery.data?.model_options ?? [];
  const trainerPresets = trainOptionsQuery.data?.trainer_presets ?? [];
  const datasetPresets = trainOptionsQuery.data?.dataset_presets ?? [];
  const templates = templatesQuery.data?.items ?? [];

  function switchTab(tab: "templates" | "trained") {
    setSearchParams((current) => {
      const next = new URLSearchParams(current);
      next.set("tab", tab);
      return next;
    });
  }

  function openCreateTemplate() {
    const defaultModel = modelOptions[0]?.value ?? "elastic_net";
    const draft = buildTemplateDraft(defaultModel);
    setTemplateMode("create");
    setTemplateDraft(draft);
    setHyperparamsText(buildHyperparamsText(draft.hyperparams));
    setTemplateError(null);
  }

  function openEditTemplate(templateId: string) {
    const template = templates.find((item) => item.template_id === templateId);
    if (!template || template.read_only) {
      return;
    }
    const draft = templateDraftFromView(template);
    setTemplateMode("edit");
    setTemplateDraft(draft);
    setHyperparamsText(buildHyperparamsText(draft.hyperparams));
    setTemplateError(null);
  }

  function openDuplicateTemplate(templateId: string) {
    const template = templates.find((item) => item.template_id === templateId);
    if (!template) {
      return;
    }
    const draft = duplicateDraft(templateDraftFromView(template));
    setTemplateMode("create");
    setTemplateDraft(draft);
    setHyperparamsText(buildHyperparamsText(draft.hyperparams));
    setTemplateError(null);
  }

  async function handleTemplateSave() {
    if (!templateDraft) {
      return;
    }
    if (!templateDraft.name.trim()) {
      setTemplateError(translateText("请输入模板名称。"));
      return;
    }
    if (!templateDraft.model_name.trim()) {
      setTemplateError(translateText("请选择模型类型。"));
      return;
    }

    let parsedHyperparams: Record<string, unknown> = {};
    try {
      const parsed = JSON.parse(hyperparamsText || "{}") as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("invalid");
      }
      parsedHyperparams = parsed as Record<string, unknown>;
    } catch {
      setTemplateError(translateText("超参数必须是有效 JSON。"));
      return;
    }

    const payload = {
      name: templateDraft.name.trim(),
      model_name: templateDraft.model_name,
      description: templateDraft.description.trim() || null,
      hyperparams: parsedHyperparams,
      trainer_preset: templateDraft.trainer_preset,
      dataset_preset: templateDraft.dataset_preset,
    };

    try {
      if (templateMode === "edit" && templateDraft.template_id) {
        await updateTemplateMutation.mutateAsync({
          templateId: templateDraft.template_id,
          body: payload,
        });
      } else {
        await createTemplateMutation.mutateAsync(payload);
      }
      setTemplateDraft(null);
      setTemplateError(null);
    } catch (error) {
      setTemplateError((error as Error).message);
    }
  }

  async function handleTemplateDeleteConfirm() {
    if (!templateDeleteId) {
      return;
    }
    try {
      await deleteTemplateMutation.mutateAsync(templateDeleteId);
      setTemplateDeleteId(null);
    } catch (error) {
      setTemplateError((error as Error).message);
    }
  }

  function openRunMetaEditor(runId: string) {
    const currentMeta = runMeta[runId];
    setEditingRunId(runId);
    setRunNoteDraft({
      displayName: currentMeta?.displayName ?? "",
      note: currentMeta?.note ?? "",
    });
  }

  function saveRunMeta() {
    if (!editingRunId) {
      return;
    }
    setRunMeta((current) => ({
      ...current,
      [editingRunId]: {
        runId: editingRunId,
        displayName: runNoteDraft.displayName,
        note: runNoteDraft.note,
        hidden: current[editingRunId]?.hidden ?? false,
      },
    }));
    setEditingRunId(null);
  }

  function hideRunFromWorkbench() {
    if (!runDeleteId) {
      return;
    }
    setRunMeta((current) => ({
      ...current,
      [runDeleteId]: {
        runId: runDeleteId,
        displayName: current[runDeleteId]?.displayName ?? "",
        note: current[runDeleteId]?.note ?? "",
        hidden: true,
      },
    }));
    setRunDeleteId(null);
  }

  const visibleRuns = (runsQuery.data?.items ?? []).filter((item) => !runMeta[item.run_id]?.hidden);
  const templateSavePending = createTemplateMutation.isPending || updateTemplateMutation.isPending;
  const selectedRuns = visibleRuns.filter((run) => selectedRunIdsForComposition.includes(run.run_id));
  const selectedRunDatasetIds = selectedRuns.flatMap((run) => datasetIdsForRun(run));
  const uniqueSelectedDatasetIds = Array.from(new Set(selectedRunDatasetIds));
  const selectedRunsHaveOnlySingleDataset = selectedRuns.every(
    (run) => datasetIdsForRun(run).length === 1,
  );
  const compositionReady =
    selectedRuns.length >= 2 &&
    selectedRunsHaveOnlySingleDataset &&
    uniqueSelectedDatasetIds.length >= 2;

  function toggleRunSelection(runId: string, checked: boolean) {
    setSelectedRunIdsForComposition((current) =>
      checked ? Array.from(new Set([...current, runId])) : current.filter((value) => value !== runId),
    );
  }

  async function handleLaunchComposition() {
    if (selectedRuns.length < 2) {
      setCompositionError(translateText("请至少选择两个单模态训练实例。"));
      return;
    }
    if (!selectedRunsHaveOnlySingleDataset) {
      setCompositionError(translateText("当前只支持组合单数据集训练实例。"));
      return;
    }
    if (uniqueSelectedDatasetIds.length < 2) {
      setCompositionError(translateText("请选择至少来自两个不同数据集的训练实例。"));
      return;
    }
    if (!compositionName.trim()) {
      setCompositionError(translateText("请输入组合名称。"));
      return;
    }
    try {
      const result = await compositionMutation.mutateAsync({
        source_run_ids: selectedRuns.map((run) => run.run_id),
        composition_name: compositionName.trim(),
        dataset_ids: uniqueSelectedDatasetIds,
      });
      setCompositionError(null);
      setCompositionJobId(result.job_id);
    } catch (error) {
      setCompositionError((error as Error).message);
    }
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.models}
          title={I18N.nav.models}
          description={translateText("在同一页面统一管理模型模板、已训练模型和回测发起。")}
          action={
            <div className="table-actions">
              <LaunchTrainDrawer
                defaultOpen={launchTrainRequested && Boolean(requestedDatasetId)}
                datasetId={launchTrainRequested ? requestedDatasetId ?? undefined : undefined}
                datasetLabel={
                  launchTrainRequested
                    ? requestedDatasetQuery.data?.display_name ??
                      requestedDatasetQuery.data?.dataset.display_name ??
                      requestedDatasetId ??
                      undefined
                    : undefined
                }
                description={
                  launchTrainRequested && requestedDatasetId
                    ? translateText("这个训练入口来自数据集页面，将直接使用当前数据集。")
                    : undefined
                }
                title={
                  launchTrainRequested && requestedDatasetId
                    ? translateText("基于该数据集发起训练")
                    : undefined
                }
                triggerLabel={
                  launchTrainRequested && requestedDatasetId
                    ? translateText("继续训练")
                    : undefined
                }
              />
              <LaunchBacktestDrawer
                initialRunId={selectedRunIdForBacktest}
                initialMode={selectedRunIdForBacktest ? "custom" : undefined}
              />
            </div>
          }
        />
        <div className="segmented-tabs">
          <button
            className={activeTab === "templates" ? "active" : ""}
            onClick={() => switchTab("templates")}
            type="button"
          >
            {I18N.nav.modelTemplates}
          </button>
          <button
            className={activeTab === "trained" ? "active" : ""}
            onClick={() => switchTab("trained")}
            type="button"
          >
            {I18N.nav.trainedModels}
          </button>
        </div>
      </section>

      {activeTab === "templates" ? (
        <section className="page-stack">
          <section className="panel">
              <PanelHeader
                eyebrow={I18N.nav.modelTemplates}
                title={I18N.nav.modelTemplates}
                description={translateText("模板直接来自后端注册信息与持久化存储。")}
              action={
                <button
                  className="action-button"
                  onClick={openCreateTemplate}
                  type="button"
                  disabled={trainOptionsQuery.isLoading || modelOptions.length === 0}
                >
                  {I18N.action.createTemplate}
                </button>
              }
            />
            {templatesQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {templatesQuery.isError ? (
              <ErrorState message={(templatesQuery.error as Error).message} />
            ) : null}
            {!templatesQuery.isLoading && !templatesQuery.isError ? (
              templates.length > 0 ? (
                <table className="data-table trained-models-table">
                  <thead>
                    <tr>
                      <th>{translateText("模板")}</th>
                      <th>{translateText("模型")}</th>
                      <th>{translateText("默认项")}</th>
                      <th>{translateText("训练入口")}</th>
                      <th>{translateText("启用状态")}</th>
                      <th>{translateText("操作")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {templates.map((template) => (
                      <tr key={template.template_id}>
                        <td>{localizeTemplateDisplayText(template.name)}</td>
                        <td>{modelLabel(template.model_name)}</td>
                        <td>{summarizeTemplateParameters(template)}</td>
                        <td>{`${localizeTemplateDisplayText(template.dataset_preset)} / ${localizeTemplateDisplayText(template.trainer_preset)}`}</td>
                        <td>
                          <StatusPill status={template.model_registered ? "success" : "partial"} />
                        </td>
                        <td>
                          <div className="table-actions template-actions">
                            <LaunchTrainDrawer
                              triggerLabel={I18N.action.trainWithTemplate}
                              title={`${translateText("基于模板发起训练")} ${localizeTemplateDisplayText(template.name)}`}
                              description={translateText("这个入口会把当前模板中的模型和超参数直接提交给训练后端。")}
                              initialTemplateId={template.template_id}
                            />
                            {!template.read_only ? (
                              <button
                                className="link-button"
                                onClick={() => openEditTemplate(template.template_id)}
                                type="button"
                              >
                                {I18N.action.editTemplate}
                              </button>
                            ) : null}
                            <button
                              className="link-button"
                              onClick={() => openDuplicateTemplate(template.template_id)}
                              type="button"
                            >
                              {I18N.action.duplicateTemplate}
                            </button>
                            {!template.read_only ? (
                              <button
                                className="link-button danger-link"
                                onClick={() => setTemplateDeleteId(template.template_id)}
                                type="button"
                              >
                                {I18N.action.delete}
                              </button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <EmptyState
                  title={I18N.state.empty}
                  body={translateText("当前还没有可用的模型模板。")}
                />
              )
            ) : null}
          </section>

          {templateDraft ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                title={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                description={translateText("模板用于让模型、超参数和默认项始终与后端契约保持一致。")}
                action={
                  <button className="link-button" onClick={() => setTemplateDraft(null)} type="button">
                    {I18N.action.close}
                  </button>
                }
              />
              <div className="form-section-grid">
                <label>
                  <span>{translateText("名称")}</span>
                  <input
                    className="field"
                    onChange={(event) =>
                      setTemplateDraft((current) =>
                        current ? { ...current, name: event.target.value } : current,
                      )
                    }
                    value={templateDraft.name}
                  />
                </label>
                <label>
                  <span>{translateText("模型类型")}</span>
                  <select
                    className="field"
                    onChange={(event) => {
                      const nextDraft = templateDraft ? applyModelDefaults(templateDraft, event.target.value) : null;
                      setTemplateDraft(nextDraft);
                      if (nextDraft) {
                        setHyperparamsText(buildHyperparamsText(nextDraft.hyperparams));
                      }
                    }}
                    value={templateDraft.model_name}
                  >
                    {modelOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>{translateText("默认训练预置")}</span>
                  <select
                    className="field"
                    onChange={(event) =>
                      setTemplateDraft((current) =>
                        current ? { ...current, dataset_preset: event.target.value } : current,
                      )
                    }
                    value={templateDraft.dataset_preset}
                  >
                    {datasetPresets.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>{translateText("默认训练预置")}</span>
                  <select
                    className="field"
                    onChange={(event) =>
                      setTemplateDraft((current) =>
                        current ? { ...current, trainer_preset: event.target.value } : current,
                      )
                    }
                    value={templateDraft.trainer_preset}
                  >
                    {trainerPresets.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <label>
                <span>{translateText("说明")}</span>
                <textarea
                  className="field area-field"
                  onChange={(event) =>
                    setTemplateDraft((current) =>
                      current ? { ...current, description: event.target.value } : current,
                    )
                  }
                  value={templateDraft.description}
                />
              </label>

              <section className="form-section">
                <h3>{translateText("超参数")}</h3>
                <p className="drawer-copy">{`${modelCategory(templateDraft.model_name)} | ${modelSuitableData(templateDraft.model_name)}`}</p>
                <textarea
                  className="field area-field"
                  onChange={(event) => setHyperparamsText(event.target.value)}
                  spellCheck={false}
                  value={hyperparamsText}
                />
              </section>

              {templateError ? <p className="form-error">{templateError}</p> : null}
              <div className="dialog-actions inline-actions">
                <button className="link-button" onClick={() => setTemplateDraft(null)} type="button">
                  {I18N.action.cancel}
                </button>
                <button
                  className="action-button"
                  onClick={() => void handleTemplateSave()}
                  type="button"
                  disabled={templateSavePending}
                >
                  {templateSavePending ? translateText("保存中...") : I18N.action.save}
                </button>
              </div>
            </section>
          ) : null}
        </section>
      ) : (
        <section className="page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow={I18N.nav.trainedModels}
              title={I18N.nav.trainedModels}
              description={I18N.model.trainedSection}
              action={
                <input
                  className="field search-field"
                  onChange={(event) => setRunSearch(event.target.value)}
                  placeholder={translateText("搜索训练实例 ID / 模型 / 数据集")}
                  value={runSearch}
                />
              }
            />
            {runsQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {runsQuery.isError ? <ErrorState message={(runsQuery.error as Error).message} /> : null}
            {!runsQuery.isLoading && !runsQuery.isError ? (
              visibleRuns.length > 0 ? (
                <div className="trained-model-table-shell">
                  <table className="data-table trained-models-table">
                    <thead>
                      <tr>
                        <th>{translateText("选择")}</th>
                        <th>{translateText("训练实例")}</th>
                        <th>{translateText("模型")}</th>
                        <th>{translateText("数据集")}</th>
                        <th>{translateText("创建时间")}</th>
                        <th>{translateText("指标")}</th>
                        <th>{translateText("回测数")}</th>
                        <th>{translateText("状态")}</th>
                        <th>{translateText("操作")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visibleRuns.map((run) => {
                        return (
                          <tr key={run.run_id}>
                            <td>
                              <input
                                aria-label={`${translateText("选择")} ${run.run_id} ${translateText("用于多模态组合")}`}
                                checked={selectedRunIdsForComposition.includes(run.run_id)}
                                onChange={(event) =>
                                  toggleRunSelection(run.run_id, event.target.checked)
                                }
                                type="checkbox"
                              />
                            </td>
                            <td>
                              <div className="table-title-cell">
                                <strong>{runMeta[run.run_id]?.displayName || run.run_id}</strong>
                                <span>{runMeta[run.run_id]?.note || run.model_name}</span>
                              </div>
                            </td>
                            <td>
                              {modelLabel(run.model_name)}
                              <div className="table-subcopy">{modelCategory(run.model_name)}</div>
                            </td>
                            <td>
                              <div className="table-title-cell">
                                {datasetIdsForRun(run).length > 0 ? (
                                  datasetIdsForRun(run).map((datasetId) => (
                                    <Link
                                      key={datasetId}
                                      to={`/datasets/${encodeURIComponent(datasetId)}`}
                                    >
                                      {datasetId}
                                    </Link>
                                  ))
                                ) : (
                                  <span>--</span>
                                )}
                              </div>
                            </td>
                            <td>{formatDate(run.created_at)}</td>
                            <td>
                              {`${run.primary_metric_name?.toUpperCase() ?? "MAE"}=${run.primary_metric_value?.toFixed(4) ?? "--"} / backtests=${run.backtest_count}`}
                            </td>
                            <td>{run.backtest_count}</td>
                            <td>
                              <StatusPill status={run.status} />
                            </td>
                            <td>
                              <div className="table-actions trained-model-actions">
                                <Link
                                  className="link-button"
                                  to={`/models/trained/${encodeURIComponent(run.run_id)}`}
                                >
                                  {I18N.action.openDetail}
                                </Link>
                                <button
                                  className="link-button"
                                  onClick={() => openRunMetaEditor(run.run_id)}
                                  type="button"
                                >
                                  {I18N.action.rename}
                                </button>
                                <button
                                  className="link-button"
                                  onClick={() => {
                                    setTemplateMode("create");
                                    const draft = templateDraftFromRun(run);
                                    setTemplateDraft(draft);
                                    setHyperparamsText(buildHyperparamsText(draft.hyperparams));
                                    setTemplateError(null);
                                    switchTab("templates");
                                  }}
                                  type="button"
                                >
                                  {I18N.action.copyToTemplate}
                                </button>
                                <button
                                  className="link-button"
                                  onClick={() => setSelectedRunIdForBacktest(run.run_id)}
                                  type="button"
                                >
                                  {I18N.action.launchBacktest}
                                </button>
                                <button
                                  className="link-button danger-link"
                                  onClick={() => setRunDeleteId(run.run_id)}
                                  type="button"
                                >
                                  {I18N.action.delete}
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <EmptyState
                  title={I18N.state.empty}
                  body={translateText("当前还没有可用的已训练模型。")}
                />
              )
            ) : null}
          </section>

          <section className="panel form-shell">
            <PanelHeader
              eyebrow={translateText("模型组合")}
              title={translateText("组合多模态模型")}
              description={translateText("选择至少两个单模态训练实例，发起组合模型。")}
            />
            <div className="form-section-grid">
              <label>
                <span>{translateText("组合名称")}</span>
                <input
                  className="field"
                  onChange={(event) => setCompositionName(event.target.value)}
                  value={compositionName}
                />
              </label>
              <div className="metric-tile">
                <span>{translateText("已选训练实例")}</span>
                <strong>{selectedRuns.length}</strong>
              </div>
              <div className="metric-tile">
                <span>{translateText("数据集数")}</span>
                <strong>{uniqueSelectedDatasetIds.length}</strong>
              </div>
            </div>
            {selectedRuns.length > 0 ? (
              <div className="stack-list">
                {selectedRuns.map((run) => (
                  <div className="stack-item align-start" key={run.run_id}>
                    <strong>{run.run_id}</strong>
                    <span>
                      {datasetIdsForRun(run).join(", ") || "--"} | {modelLabel(run.model_name)}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                title={translateText("尚未选择训练实例")}
                body={translateText("请先在上方选择至少两个单模态训练实例，再发起多模态组合。")}
              />
            )}
            {!selectedRunsHaveOnlySingleDataset && selectedRuns.length > 0 ? (
              <p className="form-error">
                {translateText("当前选择中包含多数据集训练实例，暂时只支持组合单数据集训练实例。")}
              </p>
            ) : null}
            {selectedRunsHaveOnlySingleDataset &&
            selectedRuns.length >= 2 &&
            uniqueSelectedDatasetIds.length < 2 ? (
              <p className="form-error">
                {translateText("当前选择未覆盖两个不同数据集，因此无法创建多模态组合。")}
              </p>
            ) : null}
            {compositionError ? <p className="form-error">{compositionError}</p> : null}
            {compositionMutation.isError ? (
              <p className="form-error">{(compositionMutation.error as Error).message}</p>
            ) : null}
            <div className="dialog-actions inline-actions">
              <button
                className="action-button"
                disabled={compositionMutation.isPending || !compositionReady}
                onClick={() => void handleLaunchComposition()}
                type="button"
              >
                {compositionMutation.isPending ? translateText("提交中...") : translateText("发起组合")}
              </button>
            </div>
            {compositionJobQuery.data ? (
              <div className="job-box">
                <div className="split-line">
                  <strong>{compositionJobQuery.data.job_id}</strong>
                  <StatusPill status={compositionJobQuery.data.status} />
                </div>
                {compositionJobQuery.data.stages.map((stage) => (
                  <div className="job-stage" key={stage.name}>
                    <span>{stage.name}</span>
                    <span>{stage.summary}</span>
                  </div>
                ))}
                {compositionJobQuery.data.error_message ? (
                  <p className="form-error">{compositionJobQuery.data.error_message}</p>
                ) : null}
                {compositionJobQuery.data.status === "success" &&
                compositionJobQuery.data.result.deeplinks.run_detail ? (
                  <Link
                    className="link-button"
                    to={compositionJobQuery.data.result.deeplinks.run_detail}
                  >
                    {translateText("打开组合模型")}
                  </Link>
                ) : null}
              </div>
            ) : null}
          </section>

          {editingRunId ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={I18N.action.rename}
                title={I18N.action.rename}
                description={translateText("为已训练模型补充显示名称和研究备注。")}
                action={
                  <button className="link-button" onClick={() => setEditingRunId(null)} type="button">
                    {I18N.action.close}
                  </button>
                }
              />
              <div className="form-section-grid">
                <label>
                  <span>{translateText("显示名称")}</span>
                  <input
                    className="field"
                    onChange={(event) =>
                      setRunNoteDraft((current) => ({ ...current, displayName: event.target.value }))
                    }
                    value={runNoteDraft.displayName}
                  />
                </label>
                <label>
                  <span>{translateText("研究备注")}</span>
                  <textarea
                    className="field area-field"
                    onChange={(event) =>
                      setRunNoteDraft((current) => ({ ...current, note: event.target.value }))
                    }
                    value={runNoteDraft.note}
                  />
                </label>
              </div>
              <div className="dialog-actions inline-actions">
                <button className="link-button" onClick={() => setEditingRunId(null)} type="button">
                  {I18N.action.cancel}
                </button>
                <button className="action-button" onClick={saveRunMeta} type="button">
                  {I18N.action.save}
                </button>
              </div>
            </section>
          ) : null}
        </section>
      )}

      <ConfirmDialog
        cancelLabel={I18N.action.cancel}
        confirmLabel={I18N.action.confirmDelete}
        message={I18N.model.deleteTemplateMessage}
        onCancel={() => setTemplateDeleteId(null)}
        onConfirm={() => void handleTemplateDeleteConfirm()}
        open={Boolean(templateDeleteId)}
        title={I18N.action.delete}
        tone="danger"
      />
      <ConfirmDialog
        cancelLabel={I18N.action.cancel}
        confirmLabel={I18N.action.confirmDelete}
        message={I18N.model.deleteRunMessage}
        onCancel={() => setRunDeleteId(null)}
        onConfirm={hideRunFromWorkbench}
        open={Boolean(runDeleteId)}
        title={I18N.action.delete}
        tone="danger"
      />
    </div>
  );
}
