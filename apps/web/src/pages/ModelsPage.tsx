import { useDeferredValue, useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import { useDatasetDetail, useRuns } from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { GlossaryKey, I18N } from "../shared/lib/i18n";
import {
  ALGORITHM_DEFINITIONS,
  AlgorithmKey,
  MODEL_TEMPLATES_STORAGE_KEY,
  ModelTemplate,
  TRAINED_MODELS_STORAGE_KEY,
  TrainedModelMeta,
  algorithmCategory,
  algorithmLabel,
  buildTemplate,
  defaultTemplates,
  deriveTemplateFromRun,
  getAlgorithmDefinition,
  summarizeRunMetrics,
  summarizeTemplateParameters,
} from "../shared/lib/modelRegistry";
import { ConfirmDialog } from "../shared/ui/ConfirmDialog";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

type TemplateDraft = ModelTemplate;

type TemplateEditorMode = "create" | "edit";

function loadTemplates(): ModelTemplate[] {
  const stored = window.localStorage.getItem(MODEL_TEMPLATES_STORAGE_KEY);
  if (!stored) {
    return defaultTemplates();
  }
  try {
    return JSON.parse(stored) as ModelTemplate[];
  } catch {
    return defaultTemplates();
  }
}

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

function buildDraft(algorithm: AlgorithmKey): TemplateDraft {
  return buildTemplate(algorithm);
}

function templateCopy(template: ModelTemplate): ModelTemplate {
  const now = new Date().toISOString();
  return {
    ...template,
    id: `${template.algorithm}-${now}`,
    name: `${template.name} \u526f\u672c`,
    createdAt: now,
    updatedAt: now,
  };
}

function applyAlgorithmToDraft(draft: TemplateDraft, algorithm: AlgorithmKey): TemplateDraft {
  const next = buildTemplate(algorithm);
  return {
    ...draft,
    algorithm,
    datasetId: draft.datasetId || next.datasetId,
    targetColumn: draft.targetColumn || next.targetColumn,
    commonParams: next.commonParams,
    algorithmParams: next.algorithmParams,
    updatedAt: new Date().toISOString(),
  };
}

export function ModelsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [templates, setTemplates] = useState<ModelTemplate[]>(() => loadTemplates());
  const [runMeta, setRunMeta] = useState<Record<string, TrainedModelMeta>>(() => loadRunMeta());
  const [templateMode, setTemplateMode] = useState<TemplateEditorMode>("create");
  const [templateDraft, setTemplateDraft] = useState<TemplateDraft | null>(null);
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

  const runsQuery = useRuns(params);

  useEffect(() => {
    window.localStorage.setItem(MODEL_TEMPLATES_STORAGE_KEY, JSON.stringify(templates));
  }, [templates]);

  useEffect(() => {
    window.localStorage.setItem(TRAINED_MODELS_STORAGE_KEY, JSON.stringify(runMeta));
  }, [runMeta]);

  function switchTab(tab: "templates" | "trained") {
    setSearchParams((current) => {
      const next = new URLSearchParams(current);
      next.set("tab", tab);
      return next;
    });
  }

  function openCreateTemplate() {
    setTemplateMode("create");
    setTemplateDraft(buildDraft("elastic_net"));
    setTemplateError(null);
  }

  function openEditTemplate(template: ModelTemplate) {
    setTemplateMode("edit");
    setTemplateDraft({ ...template });
    setTemplateError(null);
  }

  function handleTemplateSave() {
    if (!templateDraft) {
      return;
    }
    if (!templateDraft.name.trim()) {
      setTemplateError("\u8bf7\u8f93\u5165\u6a21\u677f\u540d\u79f0\u3002");
      return;
    }
    if (!templateDraft.datasetId.trim()) {
      setTemplateError("\u8bf7\u8f93\u5165\u6570\u636e\u96c6\u6807\u8bc6\u3002");
      return;
    }
    if (!templateDraft.targetColumn.trim()) {
      setTemplateError("\u8bf7\u8f93\u5165\u76ee\u6807\u5217\u3002");
      return;
    }

    const nextTemplate = {
      ...templateDraft,
      updatedAt: new Date().toISOString(),
    };

    setTemplates((current) => {
      if (templateMode === "edit") {
        return current.map((item) => (item.id === nextTemplate.id ? nextTemplate : item));
      }
      return [nextTemplate, ...current];
    });
    setTemplateDraft(null);
    setTemplateError(null);
  }

  function applyRecommendedDefaults() {
    if (!templateDraft) {
      return;
    }
    const definition = getAlgorithmDefinition(templateDraft.algorithm);
    setTemplateDraft((current) =>
      current
        ? {
            ...current,
            datasetId: definition.defaultDataset,
            targetColumn: definition.defaultTargetColumn,
            commonParams: definition.commonDefaults,
            algorithmParams: Object.fromEntries(
              definition.parameterFields.map((field) => [field.key, field.defaultValue]),
            ),
          }
        : current,
    );
  }

  function handleTemplateDeleteConfirm() {
    if (!templateDeleteId) {
      return;
    }
    setTemplates((current) => current.filter((item) => item.id !== templateDeleteId));
    setTemplateDeleteId(null);
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

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.models}
          title={I18N.nav.models}
          description={
            "\u628a\u6a21\u677f\u8bbe\u8ba1\u3001\u5df2\u8bad\u7ec3\u4ea7\u7269\u7ba1\u7406\u548c\u56de\u6d4b\u53d1\u8d77\u6536\u5728\u540c\u4e00\u5de5\u4f5c\u9762\u91cc\u3002"
          }
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
                    ? "\u5f53\u524d\u662f\u4ece\u6570\u636e\u96c6\u9875\u8df3\u8fc7\u6765\u7684 dataset-aware \u8bad\u7ec3\u6a21\u5f0f\uff0c\u4f1a\u76f4\u63a5\u4ee5 dataset_id \u4f5c\u4e3a\u8bad\u7ec3\u5165\u53e3\u3002"
                    : undefined
                }
                title={
                  launchTrainRequested && requestedDatasetId
                    ? "\u57fa\u4e8e\u5f53\u524d\u6570\u636e\u96c6\u53d1\u8d77\u8bad\u7ec3"
                    : undefined
                }
                triggerLabel={
                  launchTrainRequested && requestedDatasetId
                    ? "\u7ee7\u7eed\u8fd9\u4efd\u6570\u636e\u96c6\u8bad\u7ec3"
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
              description={I18N.model.templateSection}
              action={
                <button className="action-button" onClick={openCreateTemplate} type="button">
                  {I18N.action.createTemplate}
                </button>
              }
            />
            {templates.length > 0 ? (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{"\u6a21\u677f\u540d\u79f0"}</th>
                    <th>{"\u7b97\u6cd5\u7c7b\u578b"}</th>
                    <th>{"\u9ed8\u8ba4\u53c2\u6570"}</th>
                    <th>{"\u9002\u7528\u6570\u636e"}</th>
                    <th>{"\u542f\u7528\u72b6\u6001"}</th>
                    <th>{"\u64cd\u4f5c"}</th>
                  </tr>
                </thead>
                <tbody>
                  {templates.map((template) => (
                    <tr key={template.id}>
                      <td>{template.name}</td>
                      <td>{getAlgorithmDefinition(template.algorithm).label}</td>
                      <td>{summarizeTemplateParameters(template)}</td>
                      <td>{getAlgorithmDefinition(template.algorithm).suitableData}</td>
                      <td>
                        <StatusPill status={template.enabled ? "success" : "partial"} />
                      </td>
                      <td>
                        <div className="table-actions">
                          <button className="link-button" onClick={() => openEditTemplate(template)} type="button">
                            {I18N.action.editTemplate}
                          </button>
                          <button
                            className="link-button"
                            onClick={() => {
                              setTemplateMode("create");
                              setTemplateDraft(templateCopy(template));
                              setTemplateError(null);
                            }}
                            type="button"
                          >
                            {I18N.action.duplicateTemplate}
                          </button>
                          <button
                            className="link-button danger-link"
                            onClick={() => setTemplateDeleteId(template.id)}
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
            ) : (
              <EmptyState
                title={I18N.state.empty}
                body={"\u5f53\u524d\u8fd8\u6ca1\u6709\u6a21\u578b\u6a21\u677f\uff0c\u53ef\u4ece\u63a8\u8350\u9ed8\u8ba4\u503c\u5f00\u59cb\u3002"}
              />
            )}
          </section>

          {templateDraft ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                title={templateMode === "edit" ? I18N.action.editTemplate : I18N.action.createTemplate}
                description={"\u57fa\u4e8e\u5206\u533a\u914d\u7f6e\u5668\u5904\u7406\u57fa\u7840\u4fe1\u606f\u3001\u901a\u7528\u53c2\u6570\u548c\u7b97\u6cd5\u4e13\u5c5e\u53c2\u6570\u3002"}
                action={
                  <div className="table-actions">
                    <button className="link-button" onClick={applyRecommendedDefaults} type="button">
                      {I18N.action.applyDefaults}
                    </button>
                    <button className="link-button" onClick={() => setTemplateDraft(null)} type="button">
                      {I18N.action.close}
                    </button>
                  </div>
                }
              />
              <div className="form-section-grid">
                <section className="form-section">
                  <h3>{I18N.model.basicInfo}</h3>
                  <label>
                    <span>{"\u540d\u79f0"}</span>
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
                    <span>{"\u7b97\u6cd5\u7c7b\u578b"}</span>
                    <select
                      className="field"
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current
                            ? applyAlgorithmToDraft(current, event.target.value as AlgorithmKey)
                            : current,
                        )
                      }
                      value={templateDraft.algorithm}
                    >
                      {ALGORITHM_DEFINITIONS.map((definition) => (
                        <option key={definition.key} value={definition.key}>
                          {definition.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    <span>{"\u6570\u636e\u96c6"}</span>
                    <input
                      className="field"
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current ? { ...current, datasetId: event.target.value } : current,
                        )
                      }
                      value={templateDraft.datasetId}
                    />
                  </label>
                  <label>
                    <span>{"\u76ee\u6807\u5217"}</span>
                    <input
                      className="field"
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current ? { ...current, targetColumn: event.target.value } : current,
                        )
                      }
                      value={templateDraft.targetColumn}
                    />
                  </label>
                  <label>
                    <span>{"\u8bad\u7ec3\u8bf4\u660e"}</span>
                    <textarea
                      className="field area-field"
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current ? { ...current, trainingNote: event.target.value } : current,
                        )
                      }
                      value={templateDraft.trainingNote}
                    />
                  </label>
                  <label className="toggle-row">
                    <input
                      checked={templateDraft.enabled}
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current ? { ...current, enabled: event.target.checked } : current,
                        )
                      }
                      type="checkbox"
                    />
                    <span>{templateDraft.enabled ? I18N.status.enabled : I18N.status.disabled}</span>
                  </label>
                </section>

                <section className="form-section">
                  <h3>{I18N.model.trainingParams}</h3>
                  <FormField
                    glossaryKey="batch_size"
                    label={"\u6279\u5927\u5c0f"}
                    value={String(templateDraft.commonParams.batchSize)}
                    onChange={(value) =>
                      setTemplateDraft((current) =>
                        current
                          ? {
                              ...current,
                              commonParams: {
                                ...current.commonParams,
                                batchSize: Number(value),
                              },
                            }
                          : current,
                      )
                    }
                  />
                  <FormField
                    glossaryKey="epochs"
                    label={"\u8bad\u7ec3\u8f6e\u6b21"}
                    value={String(templateDraft.commonParams.epochs)}
                    onChange={(value) =>
                      setTemplateDraft((current) =>
                        current
                          ? {
                              ...current,
                              commonParams: {
                                ...current.commonParams,
                                epochs: Number(value),
                              },
                            }
                          : current,
                      )
                    }
                  />
                  <FormField
                    label={"\u968f\u673a\u79cd\u5b50"}
                    value={String(templateDraft.commonParams.seed)}
                    onChange={(value) =>
                      setTemplateDraft((current) =>
                        current
                          ? {
                              ...current,
                              commonParams: {
                                ...current.commonParams,
                                seed: Number(value),
                              },
                            }
                          : current,
                      )
                    }
                  />
                  <label>
                    <span>{"\u9a8c\u8bc1\u7b56\u7565"}</span>
                    <select
                      className="field"
                      onChange={(event) =>
                        setTemplateDraft((current) =>
                          current
                            ? {
                                ...current,
                                commonParams: {
                                  ...current.commonParams,
                                  validationStrategy: event.target.value,
                                },
                              }
                            : current,
                        )
                      }
                      value={templateDraft.commonParams.validationStrategy}
                    >
                      <option value="\u65f6\u95f4\u5207\u5206">{"\u65f6\u95f4\u5207\u5206"}</option>
                      <option value="\u6eda\u52a8\u65f6\u95f4\u7a97">{"\u6eda\u52a8\u65f6\u95f4\u7a97"}</option>
                      <option value="\u6b65\u8fdb\u5f0f validation">{"\u6b65\u8fdb\u5f0f validation"}</option>
                    </select>
                  </label>
                </section>
              </div>

              <section className="form-section">
                <h3>{I18N.model.algorithmParams}</h3>
                <div className="parameter-grid">
                  {getAlgorithmDefinition(templateDraft.algorithm).parameterFields
                    .filter((field) => !field.advanced)
                    .map((field) => (
                      <FormField
                        glossaryKey={field.glossaryKey}
                        key={field.key}
                        label={field.label}
                        value={String(templateDraft.algorithmParams[field.key] ?? field.defaultValue)}
                        onChange={(value) =>
                          setTemplateDraft((current) =>
                            current
                              ? {
                                  ...current,
                                  algorithmParams: {
                                    ...current.algorithmParams,
                                    [field.key]: value.includes(".") ? Number(value) : Number(value) || value,
                                  },
                                }
                              : current,
                          )
                        }
                        step={field.step}
                      />
                    ))}
                </div>
              </section>

              <details className="details-panel">
                <summary>{I18N.model.advancedParams}</summary>
                <div className="parameter-grid">
                  {getAlgorithmDefinition(templateDraft.algorithm).parameterFields
                    .filter((field) => field.advanced)
                    .map((field) => (
                      <FormField
                        glossaryKey={field.glossaryKey}
                        key={field.key}
                        label={field.label}
                        value={String(templateDraft.algorithmParams[field.key] ?? field.defaultValue)}
                        onChange={(value) =>
                          setTemplateDraft((current) =>
                            current
                              ? {
                                  ...current,
                                  algorithmParams: {
                                    ...current.algorithmParams,
                                    [field.key]: value.includes(".") ? Number(value) : Number(value) || value,
                                  },
                                }
                              : current,
                          )
                        }
                        step={field.step}
                      />
                    ))}
                </div>
              </details>

              {templateError ? <p className="form-error">{templateError}</p> : null}
              <div className="dialog-actions inline-actions">
                <button className="link-button" onClick={() => setTemplateDraft(null)} type="button">
                  {I18N.action.cancel}
                </button>
                <button className="action-button" onClick={handleTemplateSave} type="button">
                  {I18N.action.save}
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
                  placeholder="\u641c\u7d22 run_id / \u6a21\u578b / \u6570\u636e\u96c6"
                  value={runSearch}
                />
              }
            />
            {runsQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {runsQuery.isError ? <ErrorState message={(runsQuery.error as Error).message} /> : null}
            {!runsQuery.isLoading && !runsQuery.isError ? (
              visibleRuns.length > 0 ? (
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{"\u5b9e\u4f8b\u540d\u79f0"}</th>
                      <th>{"\u6765\u6e90\u6a21\u677f"}</th>
                      <th>{"\u521b\u5efa\u65f6\u95f4"}</th>
                      <th>{"\u6307\u6807\u6458\u8981"}</th>
                      <th>{"\u5173\u8054\u56de\u6d4b"}</th>
                      <th>{"\u72b6\u6001"}</th>
                      <th>{"\u64cd\u4f5c"}</th>
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
                          {algorithmLabel(run.model_name)}
                          <div className="table-subcopy">{algorithmCategory(run.model_name)}</div>
                        </td>
                        <td>{formatDate(run.created_at)}</td>
                        <td>{summarizeRunMetrics(run)}</td>
                        <td>{run.backtest_count}</td>
                        <td>
                          <StatusPill status={run.status} />
                        </td>
                        <td>
                          <div className="table-actions">
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
                                setTemplateDraft(templateCopy(deriveTemplateFromRun(run)));
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
              ) : (
                <EmptyState
                  title={I18N.state.empty}
                  body={"\u5f53\u524d\u6ca1\u6709\u53ef\u5c55\u793a\u7684\u5df2\u8bad\u7ec3\u6a21\u578b\u5b9e\u4f8b\u3002"}
                />
              )
            ) : null}
          </section>

          {editingRunId ? (
            <section className="panel form-shell">
              <PanelHeader
                eyebrow={I18N.action.rename}
                title={I18N.action.rename}
                description={"\u53ef\u4e3a\u5df2\u8bad\u7ec3\u6a21\u578b\u6dfb\u52a0\u5c55\u793a\u540d\u548c\u7814\u7a76\u5907\u6ce8\u3002"}
                action={
                  <button className="link-button" onClick={() => setEditingRunId(null)} type="button">
                    {I18N.action.close}
                  </button>
                }
              />
              <div className="form-section-grid">
                <label>
                  <span>{"\u5c55\u793a\u540d\u79f0"}</span>
                  <input
                    className="field"
                    onChange={(event) =>
                      setRunNoteDraft((current) => ({ ...current, displayName: event.target.value }))
                    }
                    value={runNoteDraft.displayName}
                  />
                </label>
                <label>
                  <span>{"\u7814\u7a76\u5907\u6ce8"}</span>
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
        onConfirm={handleTemplateDeleteConfirm}
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

function FormField({
  label,
  value,
  onChange,
  step,
  glossaryKey,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  step?: string;
  glossaryKey?: GlossaryKey;
}) {
  return (
    <label>
      <span className="form-label-row">
        <span>{label}</span>
        {glossaryKey ? <GlossaryHint hintKey={glossaryKey} iconOnly /> : null}
      </span>
      <input
        className="field"
        onChange={(event) => onChange(event.target.value)}
        step={step}
        type="number"
        value={value}
      />
    </label>
  );
}
