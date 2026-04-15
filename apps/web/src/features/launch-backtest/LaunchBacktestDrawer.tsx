import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../../shared/api/client";
import {
  useBacktestOptions,
  useBacktestPreflight,
  useDatasetReadiness,
  useJobStatus,
  useRunDetail,
} from "../../shared/api/hooks";
import type {
  BacktestTemplateView,
  DatasetReferenceView,
  RunDetailView,
} from "../../shared/api/types";
import { formatDate } from "../../shared/lib/format";
import { I18N } from "../../shared/lib/i18n";
import { formatModalityLabel, formatStageNameLabel } from "../../shared/lib/labels";
import {
  localizeBacktestGateReason,
  localizeBacktestMetadata,
  localizeBacktestRequirement,
  localizeBacktestTemplateName,
} from "../../shared/lib/protocolI18n";
import { StatusPill } from "../../shared/ui/StatusPill";

type LaunchBacktestDrawerProps = {
  initialRunId?: string | null;
  initialDatasetId?: string | null;
  initialDatasetIds?: string[] | null;
};

type RunOfficialEligibility = {
  composition?: Record<string, unknown> | null;
  official_template_eligible?: boolean | null;
  official_blocking_reasons?: string[] | null;
};

type OfficialWindowDays = 30 | 90 | 180 | 365;
type ResearchBackend = "native" | "vectorbt";
type PortfolioMethod = "proportional" | "skfolio_mean_risk";

const OFFICIAL_MARKET_DATASET_ID = "baseline_real_benchmark_dataset";
const OFFICIAL_MULTIMODAL_DATASET_ID = "official_reddit_pullpush_multimodal_v2_fusion";
const OFFICIAL_WINDOW_OPTIONS: OfficialWindowDays[] = [30, 90, 180, 365];

function normalizeDatasetIds(value: string) {
  return Array.from(
    new Set(
      value
        .split(/[\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  );
}

function formatWindow(startTime?: string | null, endTime?: string | null) {
  if (startTime && endTime) {
    return `${formatDate(startTime)} - ${formatDate(endTime)}`;
  }
  if (startTime) {
    return `${formatDate(startTime)} - --`;
  }
  if (endTime) {
    return `-- - ${formatDate(endTime)}`;
  }
  return "--";
}

function computeOfficialRollingWindow(endTime?: string | null, windowDays = 180) {
  if (!endTime) {
    return { start: null, end: null };
  }
  const end = new Date(endTime);
  if (Number.isNaN(end.getTime())) {
    return { start: null, end: endTime };
  }
  const start = new Date(end.getTime() - windowDays * 24 * 60 * 60 * 1000);
  return {
    start: start.toISOString(),
    end: end.toISOString(),
  };
}

function gateStatusLabel(status?: string | null) {
  if (status === "passed") {
    return "通过";
  }
  if (status === "failed") {
    return "未通过";
  }
  if (status === "warning") {
    return "需复核";
  }
  return "--";
}

function localizeBacktestOptionLabel(value: string, label?: string | null) {
  const normalized = (label ?? value).trim().toLowerCase();
  if (normalized === "smoke") {
    return "Smoke";
  }
  if (normalized === "real_benchmark") {
    return "Real Benchmark";
  }
  if (normalized === "full") {
    return "全部";
  }
  if (normalized === "test") {
    return "测试集";
  }
  if (normalized === "sign") {
    return "Sign";
  }
  if (normalized === "research_default" || normalized === "default") {
    return "Research Default";
  }
  if (normalized === "standard") {
    return "Standard";
  }
  if (normalized === "native" || normalized === "native research") {
    return "Native";
  }
  if (normalized === "vectorbt") {
    return "vectorbt";
  }
  if (normalized === "proportional") {
    return "Proportional";
  }
  if (normalized === "skfolio_mean_risk" || normalized === "skfolio mean-risk") {
    return "skfolio Mean-Risk";
  }
  return label ?? value;
}

function localizeOfficialWindowOptionLabel(value: string, label?: string | null) {
  const days = Number.parseInt(value, 10);
  if (OFFICIAL_WINDOW_OPTIONS.includes(days as OfficialWindowDays)) {
    return `最近 ${days} 天`;
  }
  return label ?? value;
}

function parseOfficialWindowDays(value: string): OfficialWindowDays {
  const parsed = Number.parseInt(value, 10);
  if (OFFICIAL_WINDOW_OPTIONS.includes(parsed as OfficialWindowDays)) {
    return parsed as OfficialWindowDays;
  }
  return 180;
}

function normalizeModality(value?: string | null) {
  return (value ?? "").trim().toLowerCase();
}

function resolveRunOfficialEligibility(run?: RunDetailView | null) {
  const candidate = (run ?? null) as (RunDetailView & RunOfficialEligibility) | null;
  return {
    isComposed: Boolean(candidate?.composition),
    eligible: candidate?.official_template_eligible,
    blockingReasons: Array.isArray(candidate?.official_blocking_reasons)
      ? candidate.official_blocking_reasons.filter((item): item is string => Boolean(item))
      : [],
  };
}

function isNlpModality(value?: string | null) {
  const modality = normalizeModality(value);
  return Boolean(modality) && modality !== "market";
}

function localizeRequirementItemText(item: string) {
  let localized = item;
  const replacements = [
    "Model output must follow the prediction_frame_v1 contract.",
    "Training-time disclosure fields must be populated before official comparison is trusted.",
    "Official mode binds to the newest official rolling benchmark and ignores dataset overrides.",
    "Official mode allows only fixed window presets: 30, 90, 180, and 365 days.",
    "Official ranking compares runs only when the official benchmark version and window size match.",
    "If NLP is used, the requested NLP collection window must match the market template window.",
    "If NLP is used, only archival NLP sources are eligible for official same-template comparison.",
    "If NLP is used, the official gate requires test-window coverage >= 60%, max empty gap <= 168 bars, duplicate ratio <= 5%, and entity link coverage >= 95%.",
    "Any compatible run can be launched in custom mode.",
    "The official template is read-only and cannot be deleted.",
    "The official template locks prediction scope to test and defaults the benchmark to BTCUSDT.",
    "The official template always uses the newest available market environment instead of the training dataset window.",
    "Window size is user-selectable, but official rankings only compare results that use the same window preset.",
    "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible.",
    "Custom mode stays visible for inspection but is excluded from official ranking.",
    "Training dataset start/end time",
    "Lookback window / context length",
    "Label horizon",
    "Modalities and fusion summary",
    "Random seed",
    "Tuning trial count",
    "External pretraining flag",
    "Synthetic data flag",
    "Actual market dataset window",
    "Actual official backtest test window",
    "Actual NLP coverage window and official NLP gate result when NLP is present",
    "Official rolling benchmark version",
    "Official rolling window size and actual window start/end time",
    "Official market benchmark dataset id",
    "Official multimodal benchmark dataset id when non-market signals are used",
  ] as const;

  replacements.forEach((source) => {
    const target =
      localizeBacktestRequirement(source) !== source
        ? localizeBacktestRequirement(source)
        : localizeBacktestMetadata(source);
    localized = localized.replace(source, target);
  });
  return localized;
}

function templateRequirementItems(template: BacktestTemplateView | undefined) {
  if (!template) {
    return [];
  }
  const items = [
    "如果使用 NLP，申请的 NLP 采集时间窗必须与市场模板时间窗一致。",
    "如果使用 NLP，只有归档型 NLP 数据源才允许参加官方同模板对比。",
    "如果 NLP 质量门禁失败，官方模板会被阻断。",
    template.output_contract_version
      ? `模型输出必须遵守 ${template.output_contract_version}。`
      : null,
    template.fixed_prediction_scope
      ? `官方模式会将预测范围固定为 ${localizeBacktestOptionLabel(template.fixed_prediction_scope)}。`
      : null,
    ...template.eligibility_rules.map((item) => `准入要求：${item}`),
    ...template.required_metadata.map((item) => `必填披露：${item}`),
    ...template.notes.map((item) => `说明：${item}`),
  ];
  return items.filter((item): item is string => Boolean(item)).map(localizeRequirementItemText);
}

export function LaunchBacktestDrawer({
  initialRunId = null,
  initialDatasetId = null,
  initialDatasetIds = null,
}: LaunchBacktestDrawerProps) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const optionsQuery = useBacktestOptions();
  const [open, setOpen] = useState(false);
  const [runId, setRunId] = useState(initialRunId ?? "");
  const [jobId, setJobId] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [mode, setMode] = useState<"official" | "custom">("official");
  const [predictionScope, setPredictionScope] = useState<"full" | "test">("full");
  const [datasetPreset, setDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [benchmarkSymbol, setBenchmarkSymbol] = useState("BTCUSDT");
  const [officialWindowDays, setOfficialWindowDays] = useState<OfficialWindowDays>(180);
  const [researchBackend, setResearchBackend] = useState<ResearchBackend>("native");
  const [portfolioMethod, setPortfolioMethod] = useState<PortfolioMethod>("proportional");
  const [datasetId, setDatasetId] = useState(initialDatasetId ?? "");
  const [datasetIdsText, setDatasetIdsText] = useState(
    initialDatasetIds?.length
      ? initialDatasetIds.join("\n")
      : initialDatasetId
        ? initialDatasetId
        : "",
  );

  const runQuery = useRunDetail(runId.trim());
  const runOfficialEligibility = resolveRunOfficialEligibility(runQuery.data);
  const officialRunBlockingReasons = runOfficialEligibility.blockingReasons;
  const fallbackBoundDatasetId = initialDatasetId ?? runQuery.data?.dataset_id ?? null;
  const boundDatasetIds = initialDatasetIds?.length
    ? initialDatasetIds
    : runQuery.data?.dataset_ids?.length
      ? runQuery.data.dataset_ids
      : fallbackBoundDatasetId
        ? [fallbackBoundDatasetId]
        : [];
  const runDatasets = runQuery.data?.datasets ?? [];
  const marketDatasetId =
    runDatasets.find((item: DatasetReferenceView) => normalizeModality(item.modality) === "market")
      ?.dataset_id ??
    boundDatasetIds[0] ??
    null;
  const nlpDatasetId =
    runDatasets.find((item: DatasetReferenceView) => isNlpModality(item.modality))?.dataset_id ??
    boundDatasetIds.find((item) => item !== marketDatasetId) ??
    null;
  const customDatasetIds = normalizeDatasetIds(datasetIdsText);
  const marketReadinessQuery = useDatasetReadiness(
    mode === "official" ? marketDatasetId : null,
    mode === "official" && Boolean(marketDatasetId),
  );
  const nlpReadinessQuery = useDatasetReadiness(
    mode === "official" ? nlpDatasetId : null,
    mode === "official" && Boolean(nlpDatasetId) && nlpDatasetId !== marketDatasetId,
  );
  const officialPreflightQuery = useBacktestPreflight(
    {
      run_id: runId.trim(),
      mode: "official",
      template_id: optionsQuery.data?.official_template_id ?? undefined,
      official_window_days: officialWindowDays,
    },
    mode === "official" &&
      Boolean(runId.trim()) &&
      Boolean(optionsQuery.data?.official_template_id),
  );
  const jobQuery = useJobStatus(jobId);
  const officialReadiness = nlpReadinessQuery.data ?? marketReadinessQuery.data;
  const officialPreflight = officialPreflightQuery.data;

  const officialTemplate = optionsQuery.data?.template_options?.find(
    (item) => item.template_id === optionsQuery.data?.official_template_id,
  );
  const officialRequirements = templateRequirementItems(officialTemplate);
  const officialSchemaVersion =
    optionsQuery.data?.official_multimodal_schema_version ?? "official_multimodal_standard_v1";
  const officialSchemaFeatureNames = optionsQuery.data?.official_multimodal_feature_names ?? [];
  const officialNlpGateFailed =
    officialPreflight?.nlp_gate_status === "failed" ||
    officialReadiness?.official_nlp_gate_status === "failed";
  const officialGateReasons =
    officialPreflight?.nlp_gate_reasons?.length
      ? officialPreflight.nlp_gate_reasons
      : officialReadiness?.official_nlp_gate_reasons ?? [];
  const officialBlockingReasons = officialPreflight?.blocking_reasons ?? [];
  const officialSchemaMissingFeatures = officialPreflight?.missing_official_feature_names ?? [];
  const officialPreflightPending =
    !officialPreflight && (officialPreflightQuery.isLoading || officialPreflightQuery.isFetching);
  const officialPreflightError = !officialPreflight && officialPreflightQuery.isError
    ? (officialPreflightQuery.error as Error).message
    : null;
  const officialCompatibilityBlocked =
    Boolean(officialPreflightError) ||
    Boolean(officialSchemaMissingFeatures.length) ||
    officialNlpGateFailed ||
    (officialPreflight ? officialPreflight.compatible === false : false);
  const officialSubmitBlocked =
    mode === "official" && officialCompatibilityBlocked;
  const officialBlockingSummary = officialPreflightPending
    ? "正在检查官方兼容性，请稍候。"
    : officialPreflightError ??
      (!officialPreflight && mode === "official" && runId.trim()
        ? "官方兼容性检查尚未完成。"
        : null) ??
      officialBlockingReasons[0] ??
      officialRunBlockingReasons[0] ??
      officialGateReasons[0] ??
      (officialNlpGateFailed ? "官方 NLP 门禁未通过。" : null);
  const officialCompatibilityLabel = officialPreflightPending
    ? "检查中"
    : officialPreflightError
      ? "错误"
      : officialPreflight
        ? officialPreflight.compatible
          ? "兼容"
          : "不兼容"
        : "--";
  const actualMarketWindow = formatWindow(
    officialReadiness?.market_window_start_time,
    officialReadiness?.market_window_end_time,
  );
  const officialTestWindow = formatWindow(
    officialReadiness?.official_backtest_start_time,
    officialReadiness?.official_backtest_end_time,
  );
  const actualNlpWindow = formatWindow(
    officialReadiness?.nlp_actual_start_time,
    officialReadiness?.nlp_actual_end_time,
  );
  const rollingWindowEnd =
    officialReadiness?.nlp_actual_end_time ?? officialReadiness?.market_window_end_time ?? null;
  const rollingWindow = computeOfficialRollingWindow(rollingWindowEnd, officialWindowDays);
  const officialRollingWindow = officialPreflight
    ? formatWindow(
        officialPreflight.official_window_start_time,
        officialPreflight.official_window_end_time,
      )
    : formatWindow(rollingWindow.start, rollingWindow.end);
  const officialWindowOptions =
    optionsQuery.data?.official_window_options?.length
      ? optionsQuery.data.official_window_options
      : OFFICIAL_WINDOW_OPTIONS.map((days) => ({
          value: String(days),
          label: `Recent ${days}d`,
          description: null,
          recommended: days === 180,
        }));
  const researchBackendOptions =
    optionsQuery.data?.research_backends?.length
      ? optionsQuery.data.research_backends
      : [
          {
            value: "native",
            label: "Native",
            description: null,
            recommended: true,
          },
          {
            value: "vectorbt",
            label: "vectorbt",
            description: null,
            recommended: false,
          },
        ];
  const portfolioMethodOptions =
    optionsQuery.data?.portfolio_methods?.length
      ? optionsQuery.data.portfolio_methods
      : [
          {
            value: "proportional",
            label: "Proportional",
            description: null,
            recommended: true,
          },
          {
            value: "skfolio_mean_risk",
            label: "skfolio Mean-Risk",
            description: null,
            recommended: false,
          },
        ];

  useEffect(() => {
    if (initialRunId) {
      setRunId(initialRunId);
      setOpen(true);
    }
  }, [initialRunId]);

  useEffect(() => {
    if (initialDatasetId) {
      setDatasetId(initialDatasetId);
    }
  }, [initialDatasetId]);

  useEffect(() => {
    if (initialDatasetIds?.length) {
      setDatasetIdsText(initialDatasetIds.join("\n"));
      return;
    }
    if (initialDatasetId) {
      setDatasetIdsText(initialDatasetId);
    }
  }, [initialDatasetId, initialDatasetIds]);

  useEffect(() => {
    if (optionsQuery.data?.default_benchmark_symbol) {
      setBenchmarkSymbol(optionsQuery.data.default_benchmark_symbol);
    }
  }, [optionsQuery.data?.default_benchmark_symbol]);

  useEffect(() => {
    const nextValue = optionsQuery.data?.default_official_window_days;
    if (nextValue === 30 || nextValue === 90 || nextValue === 180 || nextValue === 365) {
      setOfficialWindowDays(nextValue);
    }
  }, [optionsQuery.data?.default_official_window_days]);

  useEffect(() => {
    if (optionsQuery.data?.default_mode) {
      setMode(optionsQuery.data.default_mode);
    }
  }, [optionsQuery.data?.default_mode]);

  useEffect(() => {
    const value = optionsQuery.data?.constraints?.research_backend;
    const defaultValue =
      value && typeof value === "object" && "default" in value
        ? value.default
        : null;
    if (defaultValue === "native" || defaultValue === "vectorbt") {
      setResearchBackend(defaultValue);
    }
  }, [optionsQuery.data?.constraints]);

  useEffect(() => {
    const value = optionsQuery.data?.constraints?.portfolio_method;
    const defaultValue =
      value && typeof value === "object" && "default" in value
        ? value.default
        : null;
    if (defaultValue === "proportional" || defaultValue === "skfolio_mean_risk") {
      setPortfolioMethod(defaultValue);
    }
  }, [optionsQuery.data?.constraints]);

  useEffect(() => {
    if (jobQuery.data?.status !== "success") {
      return;
    }
    void queryClient.invalidateQueries({ queryKey: ["run", runId] });
    void queryClient.invalidateQueries({ queryKey: ["runs"] });
    void queryClient.invalidateQueries({ queryKey: ["experiments"] });
    void queryClient.invalidateQueries({ queryKey: ["backtests"] });
    void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
  }, [jobQuery.data?.status, queryClient, runId]);

  const mutation = useMutation({
    mutationFn: () =>
      api.launchBacktest({
        run_id: runId,
        mode,
        template_id:
          mode === "official" ? optionsQuery.data?.official_template_id ?? undefined : undefined,
        official_window_days: mode === "official" ? officialWindowDays : undefined,
        dataset_id:
          mode === "custom" && customDatasetIds.length === 1
            ? customDatasetIds[0]
            : mode === "custom" && datasetId.trim() && customDatasetIds.length === 0
              ? datasetId.trim()
              : undefined,
        dataset_ids:
          mode === "custom" && customDatasetIds.length > 1 ? customDatasetIds : undefined,
        dataset_preset:
          mode === "custom" && !datasetId.trim() && customDatasetIds.length === 0
            ? datasetPreset
            : undefined,
        prediction_scope: mode === "official" ? "test" : predictionScope,
        strategy_preset: "sign",
        portfolio_preset: "research_default",
        cost_preset: "standard",
        research_backend: researchBackend,
        portfolio_method: portfolioMethod,
        benchmark_symbol:
          mode === "official" ? benchmarkSymbol.trim() || "BTCUSDT" : benchmarkSymbol,
      }),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["backtests"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  const backtestLink = jobQuery.data?.result?.deeplinks?.backtest_detail ?? null;

  async function handleSubmit() {
    if (!runId.trim()) {
          setFormError("请输入 run_id。");
      return;
    }
    if (!benchmarkSymbol.trim()) {
      setFormError("请输入基准符号。");
      return;
    }
    if (
      mode === "custom" &&
      !datasetId.trim() &&
      customDatasetIds.length === 0 &&
      !datasetPreset
    ) {
      setFormError("请提供 dataset_id / dataset_ids，或选择一个数据集预设。");
      return;
    }
    if (mode === "official" && !officialPreflight && !officialPreflightError) {
      const preflightResult = await officialPreflightQuery.refetch();
      const nextPreflight = preflightResult.data;
      if (!nextPreflight) {
        setFormError("官方兼容性检查尚未完成。");
        return;
      }
      if (
        nextPreflight.compatible === false ||
        nextPreflight.missing_official_feature_names.length > 0 ||
        nextPreflight.nlp_gate_status === "failed"
      ) {
        setFormError(
          nextPreflight.blocking_reasons[0] ??
            nextPreflight.nlp_gate_reasons[0] ??
            "Official compatibility checks failed.",
        );
        return;
      }
    }
    if (mode === "official" && officialSubmitBlocked) {
      setFormError(officialBlockingSummary ?? "官方兼容性检查失败。");
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
          <div className="segmented-tabs" role="tablist" aria-label="回测模式">
            <button
              className={`tab-chip ${mode === "official" ? "active" : ""}`}
              onClick={() => setMode("official")}
              type="button"
            >
              官方模板
            </button>
            <button
              className={`tab-chip ${mode === "custom" ? "active" : ""}`}
              onClick={() => setMode("custom")}
              type="button"
            >
              自定义回测
            </button>
          </div>

          <label>
            <span>训练实例 ID</span>
            <input onChange={(event) => setRunId(event.target.value)} value={runId} />
          </label>
          {mode === "official" ? (
            <div className="dataset-callout">
              <strong>
                {localizeBacktestTemplateName(officialTemplate?.name, officialTemplate?.template_id)}
              </strong>
              <span>
                官方模式会绑定最新的官方基准数据集，并忽略自定义数据集覆盖。
              </span>
              <span>
                官方排名只比较使用同一官方基准版本和同一窗口档位的结果。
              </span>
              {boundDatasetIds.length > 0 ? (
                <span>{`训练参考数据集：${boundDatasetIds.join(", ")}`}</span>
              ) : runQuery.isLoading ? (
                <span>正在解析训练参考数据集...</span>
              ) : null}
              {officialTemplate?.protocol_version ? (
                <span>{`协议版本：${officialTemplate.protocol_version}`}</span>
              ) : null}
              {officialTemplate?.scenario_bundle?.length ? (
                <span>{`场景包：${officialTemplate.scenario_bundle.join(", ")}`}</span>
              ) : null}

              <label>
                  <span>官方窗口</span>
                <select
                  onChange={(event) =>
                    setOfficialWindowDays(parseOfficialWindowDays(event.target.value))
                  }
                  value={String(officialWindowDays)}
                >
                  {officialWindowOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {localizeOfficialWindowOptionLabel(option.value, option.label)}
                    </option>
                  ))}
                </select>
              </label>

              <div className="stack-list">
                <div className="stack-item align-start">
                  <strong>官方市场数据集 ID</strong>
                  <span>{OFFICIAL_MARKET_DATASET_ID}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方多模态数据集 ID</strong>
                  <span>{OFFICIAL_MULTIMODAL_DATASET_ID}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>最新官方窗口</strong>
                  <span>{officialRollingWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>实际市场窗口</strong>
                  <span>{actualMarketWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方测试窗口</strong>
                  <span>{officialTestWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>实际 NLP 窗口</strong>
                  <span>{actualNlpWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>NLP Gate</strong>
                  <span>{gateStatusLabel(officialReadiness?.official_nlp_gate_status)}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>仅归档型 NLP</strong>
                  <span>
                    {officialReadiness?.archival_nlp_source_only === null ||
                    officialReadiness?.archival_nlp_source_only === undefined
                      ? "--"
                      : officialReadiness.archival_nlp_source_only
                        ? "是"
                        : "否"}
                  </span>
                </div>
              </div>

              <span>
                官方滚动窗口会取官方市场与 NLP 数据共同支持到的最新时间戳作为结束点。
              </span>
              <span>
                官方多模态比较要求模型实际特征集能够映射到官方 NLP schema。
              </span>

              {officialRequirements.length > 0 ? (
                <div className="stack-list">
                  {officialRequirements.map((item) => (
                    <div className="stack-item align-start" key={item}>
                      <strong>模板规则</strong>
                      <span>{item}</span>
                    </div>
                  ))}
                </div>
              ) : null}

              {officialGateReasons.length > 0 ? (
                <div className="stack-list">
                  {officialGateReasons.map((reason) => (
                    <div className="stack-item align-start" key={reason}>
                      <strong>NLP 门禁说明</strong>
                      <span>{localizeBacktestGateReason(reason)}</span>
                    </div>
                  ))}
                </div>
              ) : null}

              <div className="stack-list">
                <div className="stack-item align-start">
                  <strong>官方兼容性</strong>
                  <span>{officialCompatibilityLabel}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方基准版本</strong>
                  <span>{officialPreflight?.official_benchmark_version ?? "--"}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>官方 Schema 版本</strong>
                  <span>{officialSchemaVersion}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>需要 NLP 模态</strong>
                  <span>
                    {officialPreflight?.requires_text_features === undefined
                      ? "--"
                      : officialPreflight.requires_text_features
                        ? "是"
                        : "否"}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>需要辅助模态</strong>
                  <span>
                    {officialPreflight?.requires_auxiliary_features === undefined
                      ? "--"
                      : officialPreflight.requires_auxiliary_features
                        ? "是"
                        : "否"}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>要求模态</strong>
                  <span>
                    {officialPreflight?.required_modalities?.length
                      ? officialPreflight.required_modalities
                          .map((modality) => formatModalityLabel(modality))
                          .join(" / ")
                      : "--"}
                  </span>
                </div>
                <div className="stack-item align-start">
                  <strong>解析后的官方窗口</strong>
                  <span>{officialRollingWindow}</span>
                </div>
                <div className="stack-item align-start">
                  <strong>阻断摘要</strong>
                  <span>{officialBlockingSummary ?? "当前可发起官方回测。"}</span>
                </div>
              </div>

              {officialSchemaMissingFeatures.length > 0 ? (
                <div className="stack-list">
                  {officialSchemaMissingFeatures.map((featureName) => (
                    <div className="stack-item align-start" key={featureName}>
                      <strong>缺失官方特征</strong>
                      <span>{featureName}</span>
                    </div>
                  ))}
                </div>
              ) : null}

              {officialPreflight?.required_feature_names?.length ? (
                <div className="stack-list">
                  {officialPreflight.required_feature_names.map((featureName) => (
                    <div className="stack-item align-start" key={featureName}>
                      <strong>模型实际所需特征</strong>
                      <span>{featureName}</span>
                    </div>
                  ))}
                </div>
              ) : null}

              {officialSchemaFeatureNames.length > 0 ? (
                <div className="stack-list">
                  {officialSchemaFeatureNames.map((featureName) => (
                    <div className="stack-item align-start" key={featureName}>
                      <strong>官方标准特征</strong>
                      <span>{featureName}</span>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ) : (
            <>
              <label>
                <span>Dataset ID</span>
                <input onChange={(event) => setDatasetId(event.target.value)} value={datasetId} />
              </label>
              <label>
                <span>Dataset IDs</span>
                <textarea
                  className="field area-field"
                  onChange={(event) => setDatasetIdsText(event.target.value)}
                  placeholder="One dataset_id per line, or separate with commas"
                  value={datasetIdsText}
                />
              </label>
              {customDatasetIds.length > 0 || datasetId.trim() ? (
                <div className="dataset-callout">
                  <strong>
                    {customDatasetIds.length > 1
                      ? "Backtest from multiple datasets"
                      : "Backtest from a dataset"}
                  </strong>
                  <span>
                    Custom mode uses the dataset IDs you provide directly instead of any preset benchmark dataset.
                  </span>
                  {customDatasetIds.length > 1 ? (
                    <span>{`This launch will submit ${customDatasetIds.length} dataset IDs.`}</span>
                  ) : null}
                </div>
              ) : (
                <label>
                  <span>Dataset Preset</span>
                  <select
                    onChange={(event) =>
                      setDatasetPreset(event.target.value as "smoke" | "real_benchmark")
                    }
                    value={datasetPreset}
                  >
                    {(optionsQuery.data?.dataset_presets ?? []).map((option) => (
                      <option key={option.value} value={option.value}>
                        {localizeBacktestOptionLabel(option.value, option.label)}
                      </option>
                    ))}
                  </select>
                </label>
              )}
            </>
          )}

          {mode === "custom" ? (
            <label>
              <span>Prediction Scope</span>
              <select
                onChange={(event) => setPredictionScope(event.target.value as "full" | "test")}
                value={predictionScope}
              >
                {(optionsQuery.data?.prediction_scopes ?? []).map((option) => (
                  <option key={option.value} value={option.value}>
                    {localizeBacktestOptionLabel(option.value, option.label)}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            <div className="dataset-callout">
              <strong>固定预测范围</strong>
              <span>测试集</span>
            </div>
          )}

          <label>
            <span>基准符号</span>
            <input
              onChange={(event) => setBenchmarkSymbol(event.target.value)}
              value={benchmarkSymbol}
            />
          </label>

          <label>
            <span>Research Backend</span>
            <select
              onChange={(event) => setResearchBackend(event.target.value as ResearchBackend)}
              value={researchBackend}
            >
              {researchBackendOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {localizeBacktestOptionLabel(option.value, option.label)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Portfolio Method</span>
            <select
              onChange={(event) => setPortfolioMethod(event.target.value as PortfolioMethod)}
              value={portfolioMethod}
            >
              {portfolioMethodOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {localizeBacktestOptionLabel(option.value, option.label)}
                </option>
              ))}
            </select>
          </label>

          {(researchBackend !== "native" || portfolioMethod !== "proportional") ? (
            <div className="dataset-callout">
              <strong>Advanced research override</strong>
              <span>
                These optional overrides are explicit research choices. The default path remains
                Native + Proportional.
              </span>
            </div>
          ) : null}

          {formError ? <p className="form-error">{formError}</p> : null}
          {mutation.isError ? <p className="form-error">{(mutation.error as Error).message}</p> : null}

          <button
            className="action-button secondary"
            disabled={
              mutation.isPending ||
              optionsQuery.isLoading ||
              officialSubmitBlocked
            }
            onClick={handleSubmit}
            type="button"
          >
            {mutation.isPending ? "鎻愪氦涓?.." : I18N.action.submit}
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
                  鎵撳紑鍥炴祴璇︽儏
                </button>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
