import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import {
  useCreateModelTemplateMutation,
  useDatasetDetail,
  useDeleteModelTemplateMutation,
  useModelTemplates,
  useRuns,
  useTrainOptions,
  useUpdateModelTemplateMutation,
} from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
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
    name: `${template.name} 副本`,
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
      setTemplateError("请输入模板名称。");
      return;
    }
    if (!templateDraft.model_name.trim()) {
      setTemplateError("请选择模型类型。");
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
      setTemplateError("超参数 JSON 解析失败，请输入合法对象。");
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

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.models}
          title={I18N.nav.models}
          description={"把模板设计、已训练产物管理和回测发起收在同一工作面里。"}
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
                    ? "当前是从数据集页跳过来的 dataset-aware 训练模式，会直接以 dataset_id 作为训练入口。"
                    : undefined
                }
                title={
                  launchTrainRequested && requestedDatasetId
                    ? "基于当前数据集发起训练"
                    : undefined
                }
                triggerLabel={
                  launchTrainRequested && requestedDatasetId
                    ? "继续这份数据集训练"
                    : undefined
                }
              />
              <LaunchBacktestDrawer initialRunId={selectedRunIdForBacktest} />
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
              description={"模板直接来自后端注册与存储，可直接作为训练入口使用。"}
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
                      <th>{"模板名称"}</th>
                      <th>{"算法类型"}</th>
                      <th>{"默认参数"}</th>
                      <th>{"默认训练入口"}</th>
                      <th>{"启用状态"}</th>
                      <th>{"操作"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {templates.map((template) => (
                      <tr key={template.template_id}>
                        <td>{template.name}</td>
                        <td>{modelLabel(template.model_name)}</td>
                        <td>{summarizeTemplateParameters(template)}</td>
                        <td>{`${template.dataset_preset} / ${template.trainer_preset}`}</td>
                        <td>
                          <StatusPill status={template.model_registered ? "success" : "partial"} />
                        </td>
                        <td>
                          <div className="table-actions template-actions">
                            <LaunchTrainDrawer
                              triggerLabel={I18N.action.trainWithTemplate}
                              title={`基于 ${template.name} 发起训练`}
                              description={"这个入口会直接把当前模板的 model_name 与 hyperparams 交给训练后端。"}
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
                  body={"当前还没有模型模板，可从已注册模型创建一个新的可训练模板。"}
                />
              )
            ) : null}
          </section>

          {templateDraft ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                title={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                description={"模板字段与真实训练契约保持一致：模型、超参数、默认 trainer preset、默认 dataset preset。"}
                action={
                  <button className="link-button" onClick={() => setTemplateDraft(null)} type="button">
                    {I18N.action.close}
                  </button>
                }
              />
              <div className="form-section-grid">
                <label>
                  <span>{"名称"}</span>
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
                  <span>{"模型类型"}</span>
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
                  <span>{"默认数据集预置"}</span>
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
                  <span>{"默认训练预置"}</span>
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
                <span>{"说明"}</span>
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
                <h3>{"超参数"}</h3>
                <p className="drawer-copy">{`${modelCategory(templateDraft.model_name)} · ${modelSuitableData(templateDraft.model_name)}`}</p>
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
                  {templateSavePending ? "保存中..." : I18N.action.save}
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
                  placeholder={"搜索 run_id / 模型 / 数据集"}
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
                      <th>{"实例名称"}</th>
                      <th>{"来源模板"}</th>
                      <th>{"创建时间"}</th>
                      <th>{"指标摘要"}</th>
                      <th>{"关联回测"}</th>
                      <th>{"状态"}</th>
                      <th>{"操作"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRuns.map((run) => (
                      <tr key={run.run_id}>
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
                        <td>{formatDate(run.created_at)}</td>
                        <td>{`${run.primary_metric_name?.toUpperCase() ?? "MAE"}=${run.primary_metric_value?.toFixed(4) ?? "--"} / backtests=${run.backtest_count}`}</td>
                        <td>{run.backtest_count}</td>
                        <td>
                          <StatusPill status={run.status} />
                        </td>
                        <td>
                          <div className="table-actions trained-model-actions">
                            <Link className="link-button" to={`/models/trained/${run.run_id}`}>
                              {I18N.action.openDetail}
                            </Link>
                            <button className="link-button" onClick={() => openRunMetaEditor(run.run_id)} type="button">
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
                    ))}
                  </tbody>
                  </table>
                </div>
              ) : (
                <EmptyState
                  title={I18N.state.empty}
                  body={"当前没有可展示的已训练模型实例。"}
                />
              )
            ) : null}
          </section>

          {editingRunId ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={I18N.action.rename}
                title={I18N.action.rename}
                description={"可为已训练模型添加展示名和研究备注。"}
                action={
                  <button className="link-button" onClick={() => setEditingRunId(null)} type="button">
                    {I18N.action.close}
                  </button>
                }
              />
              <div className="form-section-grid">
                <label>
                  <span>{"展示名称"}</span>
                  <input
                    className="field"
                    onChange={(event) =>
                      setRunNoteDraft((current) => ({ ...current, displayName: event.target.value }))
                    }
                    value={runNoteDraft.displayName}
                  />
                </label>
                <label>
                  <span>{"研究备注"}</span>
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
