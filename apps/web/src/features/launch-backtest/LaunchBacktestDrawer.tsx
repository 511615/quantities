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
  localizeBlockingReason,
  localizeBacktestGateReason,
  localizeBacktestMetadata,
  localizeBacktestNote,
  localizeBacktestRequirement,
  localizeBacktestTemplateName,
} from "../../shared/lib/protocolI18n";
import { ModalityQualitySummary } from "../../shared/ui/ModalityQualitySummary";
import { StatusPill } from "../../shared/ui/StatusPill";

type LaunchBacktestDrawerProps = {
  initialRunId?: string | null;
  initialDatasetId?: string | null;
  initialDatasetIds?: string[] | null;
  initialMode?: "official" | "custom" | null;
};

type RunOfficialEligibility = {
  composition?: Record<string, unknown> | null;
  official_template_eligible?: boolean | null;
  official_blocking_reasons?: string[] | null;
};

type OfficialWindowDays = 30 | 90 | 180 | 365;
type ResearchBackend = "native" | "vectorbt";
type PortfolioMethod = "proportional" | "skfolio_mean_risk";

type SummaryItem = {
  label: string;
  value: string;
  tone?: "default" | "danger";
};

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
  if (status === "not_required") {
    return "不需要";
  }
  return "--";
}

function boolLabel(value?: boolean | null) {
  if (value === undefined || value === null) {
    return "--";
  }
  return value ? "是" : "否";
}

function compatibilityTone(
  label: string,
): "ready" | "blocked" | "pending" | "error" | "default" {
  if (label === "兼容") {
    return "ready";
  }
  if (label === "不兼容") {
    return "blocked";
  }
  if (label === "检查中") {
    return "pending";
  }
  if (label === "错误") {
    return "error";
  }
  return "default";
}

function localizeBacktestOptionLabel(value: string, label?: string | null) {
  const normalized = (label ?? value).trim().toLowerCase();
  if (normalized === "smoke") {
    return "联调样本";
  }
  if (normalized === "real_benchmark") {
    return "真实基准";
  }
  if (normalized === "full") {
    return "全部";
  }
  if (normalized === "test") {
    return "测试集";
  }
  if (normalized === "sign") {
    return "方向信号";
  }
  if (normalized === "research_default" || normalized === "default") {
    return "默认组合";
  }
  if (normalized === "standard") {
    return "标准成本";
  }
  if (normalized === "native" || normalized === "native research") {
    return "原生引擎";
  }
  if (normalized === "vectorbt") {
    return "vectorbt";
  }
  if (normalized === "proportional") {
    return "比例分配";
  }
  if (normalized === "skfolio_mean_risk" || normalized === "skfolio mean-risk") {
    return "skfolio 均值-风险";
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

function isAuxiliaryModality(value?: string | null) {
  const modality = normalizeModality(value);
  return Boolean(modality) && modality !== "market";
}

function isTextModality(value?: string | null) {
  return normalizeModality(value) === "nlp";
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
    ...template.eligibility_rules.map(
      (item, index) =>
        `准入要求：${localizeBacktestRequirement(item, template.eligibility_rule_keys?.[index])}`,
    ),
    ...template.required_metadata.map(
      (item, index) =>
        `必填披露：${localizeBacktestMetadata(item, template.required_metadata_keys?.[index])}`,
    ),
    ...template.notes.map(
      (item, index) => `说明：${localizeBacktestNote(item, template.note_keys?.[index])}`,
    ),
  ];
  return items.filter((item): item is string => Boolean(item)).map(localizeRequirementItemText);
}

function listSummary(value: string[]) {
  return value.length > 0 ? value.join(" / ") : "--";
}

function SummaryGrid({ items }: { items: SummaryItem[] }) {
  return (
    <div className="backtest-launch-kv-grid">
      {items.map((item) => (
        <div
          className={`backtest-launch-kv${item.tone === "danger" ? " is-danger" : ""}`}
          key={`${item.label}-${item.value}`}
        >
          <span>{item.label}</span>
          <strong>{item.value}</strong>
        </div>
      ))}
    </div>
  );
}

function DisclosureList({
  items,
  emptyText,
}: {
  items: string[];
  emptyText: string;
}) {
  if (items.length === 0) {
    return <p className="backtest-launch-disclosure-empty">{emptyText}</p>;
  }
  return (
    <div className="backtest-launch-list">
      {items.map((item) => (
        <div className="backtest-launch-list-item" key={item}>
          {item}
        </div>
      ))}
    </div>
  );
}

export function LaunchBacktestDrawer({
  initialRunId = null,
  initialDatasetId = null,
  initialDatasetIds = null,
  initialMode = null,
}: LaunchBacktestDrawerProps) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const optionsQuery = useBacktestOptions();
  const [open, setOpen] = useState(false);
  const [runId, setRunId] = useState(initialRunId ?? "");
  const [jobId, setJobId] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [mode, setMode] = useState<"official" | "custom">(initialMode ?? "official");
  const [predictionScope, setPredictionScope] = useState<"full" | "test">("full");
  const [datasetPreset, setDatasetPreset] = useState<"smoke" | "real_benchmark">("smoke");
  const [benchmarkSymbol, setBenchmarkSymbol] = useState("BTCUSDT");
  const [officialWindowDays, setOfficialWindowDays] = useState<OfficialWindowDays>(180);
  const [researchBackend, setResearchBackend] = useState<ResearchBackend>("native");
  const [portfolioMethod, setPortfolioMethod] = useState<PortfolioMethod>("proportional");
  const [datasetId, setDatasetId] = useState(initialDatasetId ?? "");
  const [datasetIdsText, setDatasetIdsText] = useState(
    initialDatasetIds?.length ? initialDatasetIds.join("\n") : initialDatasetId ?? "",
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
  const explicitMarketDatasetId =
    runDatasets.find((item: DatasetReferenceView) => normalizeModality(item.modality) === "market")
      ?.dataset_id ?? null;
  const explicitAuxiliaryDatasetId =
    runDatasets.find((item: DatasetReferenceView) => isAuxiliaryModality(item.modality))?.dataset_id ??
    null;
  const marketDatasetId =
    explicitMarketDatasetId ?? (!explicitAuxiliaryDatasetId ? boundDatasetIds[0] ?? null : null);
  const auxiliaryDatasetId =
    explicitAuxiliaryDatasetId ??
    boundDatasetIds.find((item) => item !== marketDatasetId) ??
    (explicitMarketDatasetId ? null : boundDatasetIds[0] ?? null);
  const customDatasetIds = normalizeDatasetIds(datasetIdsText);
  const hasBoundAuxiliaryContext =
    Boolean(auxiliaryDatasetId) ||
    runDatasets.some((item: DatasetReferenceView) => isAuxiliaryModality(item.modality));
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
  const officialMarketDatasetId =
    mode === "official"
      ? officialPreflightQuery.data?.official_market_dataset_id ?? OFFICIAL_MARKET_DATASET_ID
      : marketDatasetId;
  const officialMultimodalDatasetId =
    mode === "official" &&
    (officialPreflightQuery.data?.requires_multimodal_benchmark || hasBoundAuxiliaryContext)
      ? officialPreflightQuery.data?.official_multimodal_dataset_id ?? OFFICIAL_MULTIMODAL_DATASET_ID
      : null;
  const marketReadinessQuery = useDatasetReadiness(
    mode === "official" ? officialMarketDatasetId : null,
    mode === "official" && Boolean(officialMarketDatasetId),
  );
  const multimodalReadinessQuery = useDatasetReadiness(
    mode === "official" ? officialMultimodalDatasetId : null,
    mode === "official" && Boolean(officialMultimodalDatasetId),
  );
  const jobQuery = useJobStatus(jobId);
  const officialPreflight = officialPreflightQuery.data;
  const marketReadiness = marketReadinessQuery.data;
  const multimodalReadiness = multimodalReadinessQuery.data;
  const officialTemplate = optionsQuery.data?.template_options?.find(
    (item) => item.template_id === optionsQuery.data?.official_template_id,
  );
  const officialRequirements = templateRequirementItems(officialTemplate);
  const officialSchemaVersion =
    optionsQuery.data?.official_multimodal_schema_version ?? "official_multimodal_standard_v1";
  const officialSchemaFeatureNames = optionsQuery.data?.official_multimodal_feature_names ?? [];
  const officialNlpGateStatus =
    officialPreflight?.nlp_gate_status ?? multimodalReadiness?.official_nlp_gate_status ?? null;
  const officialNlpGateFailed = officialNlpGateStatus === "failed";
  const officialGateReasons =
    officialPreflight?.nlp_gate_reasons?.length
      ? officialPreflight.nlp_gate_reasons
      : multimodalReadiness?.official_nlp_gate_reasons ?? [];
  const officialBlockingReasons = officialPreflight?.blocking_reasons ?? [];
  const officialSchemaMissingFeatures = officialPreflight?.missing_official_feature_names ?? [];
  const officialPreflightPending =
    !officialPreflight && (officialPreflightQuery.isLoading || officialPreflightQuery.isFetching);
  const officialPreflightError =
    !officialPreflight && officialPreflightQuery.isError
      ? (officialPreflightQuery.error as Error).message
      : null;
  const officialCompatibilityBlocked =
    Boolean(officialPreflightError) ||
    Boolean(officialSchemaMissingFeatures.length) ||
    officialNlpGateFailed ||
    (officialPreflight ? officialPreflight.compatible === false : false);
  const officialSubmitBlocked = mode === "official" && officialCompatibilityBlocked;
  const officialBlockingSummary = officialPreflightPending
    ? "正在检查官方兼容性，请稍候。"
    : officialPreflightError ??
      (!officialPreflight && mode === "official" && runId.trim()
        ? "官方兼容性检查尚未完成。"
        : null) ??
      officialBlockingReasons[0] ??
      officialRunBlockingReasons[0] ??
      officialGateReasons[0] ??
      (officialNlpGateFailed ? "官方 NLP 文本门禁未通过。" : null);
  const officialBlockingSummaryText = officialBlockingSummary
    ? localizeBlockingReason(
        officialBlockingSummary,
        officialPreflight?.blocking_reason_codes?.[0] ?? null,
      )
    : "当前可发起官方回测。";
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
    marketReadiness?.market_window_start_time,
    marketReadiness?.market_window_end_time,
  );
  const officialTestWindow = formatWindow(
    marketReadiness?.official_backtest_start_time,
    marketReadiness?.official_backtest_end_time,
  );
  const actualNlpWindow = formatWindow(
    multimodalReadiness?.nlp_actual_start_time,
    multimodalReadiness?.nlp_actual_end_time,
  );
  const rollingWindowEnd =
    multimodalReadiness?.nlp_actual_end_time ?? marketReadiness?.market_window_end_time ?? null;
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
          { value: "native", label: "Native", description: null, recommended: true },
          { value: "vectorbt", label: "vectorbt", description: null, recommended: false },
        ];
  const portfolioMethodOptions =
    optionsQuery.data?.portfolio_methods?.length
      ? optionsQuery.data.portfolio_methods
      : [
          { value: "proportional", label: "Proportional", description: null, recommended: true },
          {
            value: "skfolio_mean_risk",
            label: "skfolio Mean-Risk",
            description: null,
            recommended: false,
          },
        ];
  const hasRunId = Boolean(runId.trim());
  const hasOfficialAuxiliaryContext =
    Boolean(officialMultimodalDatasetId) ||
    Boolean(officialPreflight?.requires_auxiliary_features) ||
    Boolean(officialPreflight?.requires_multimodal_benchmark) ||
    Boolean(multimodalReadiness);
  const hasOfficialNlpContext =
    runDatasets.some((item: DatasetReferenceView) => isTextModality(item.modality)) ||
    Boolean(officialPreflight?.requires_text_features) ||
    Boolean(multimodalReadiness?.nlp_actual_end_time) ||
    Boolean(multimodalReadiness?.nlp_actual_start_time) ||
    Boolean(officialNlpGateStatus);
  const officialRequiredModalities = officialPreflight?.required_modalities ?? [];
  const officialDatasetIds =
    officialPreflight?.official_dataset_ids?.length
      ? officialPreflight.official_dataset_ids
      : [officialMarketDatasetId, officialMultimodalDatasetId].filter(
          (item): item is string => Boolean(item),
        );
  const mergedModalityQualitySummary = {
    ...(marketReadiness?.modality_quality_summary ?? {}),
    ...(multimodalReadiness?.modality_quality_summary ?? {}),
    ...(officialPreflight?.modality_quality_summary ?? {}),
  };
  const officialModalityQualitySummary =
    Object.keys(mergedModalityQualitySummary).length > 0 ? mergedModalityQualitySummary : null;
  const officialQualityBlockingReasons =
    officialPreflight?.quality_blocking_reasons?.length
      ? officialPreflight.quality_blocking_reasons
      : [
          ...(marketReadiness?.blocking_issues ?? []),
          ...(multimodalReadiness?.blocking_issues ?? []),
        ];
  const officialSummaryItems: SummaryItem[] = hasRunId
    ? [
        {
          label: "兼容性结论",
          value: officialCompatibilityLabel,
          tone: officialCompatibilityBlocked ? "danger" : "default",
        },
        {
          label: "阻断摘要",
          value: officialBlockingSummaryText,
          tone: officialCompatibilityBlocked ? "danger" : "default",
        },
        ...(officialTemplate?.protocol_version
          ? [{ label: "协议版本", value: officialTemplate.protocol_version }]
          : []),
        ...(officialPreflight?.official_benchmark_version
          ? [{ label: "官方基准版本", value: officialPreflight.official_benchmark_version }]
          : []),
        ...(officialPreflight?.requires_text_features !== undefined
          ? [{ label: "需要文本模态", value: boolLabel(officialPreflight.requires_text_features) }]
          : []),
        ...(officialPreflight?.requires_auxiliary_features !== undefined
          ? [{ label: "需要辅助模态", value: boolLabel(officialPreflight.requires_auxiliary_features) }]
          : []),
        ...(officialPreflight?.required_modalities?.length
          ? [
              {
                label: "要求模态",
                value: listSummary(
                  officialPreflight.required_modalities.map((modality) =>
                    formatModalityLabel(modality),
                  ),
                ),
              },
            ]
          : []),
        ...(officialSchemaMissingFeatures.length > 0
          ? [{ label: "缺失官方特征", value: `${officialSchemaMissingFeatures.length} 项`, tone: "danger" as const }]
          : []),
      ]
    : [];
  const officialWindowItems: SummaryItem[] = [
    { label: "最新官方窗口", value: officialRollingWindow },
    ...(marketReadiness?.market_window_end_time
      ? [{ label: "实际市场窗口", value: actualMarketWindow }]
      : []),
    ...(marketReadiness?.official_backtest_end_time
      ? [{ label: "官方测试窗口", value: officialTestWindow }]
      : []),
    ...(hasOfficialNlpContext && actualNlpWindow !== "--"
      ? [{ label: "实际 NLP 窗口", value: actualNlpWindow }]
      : []),
  ];
  const bindingItems = [
    boundDatasetIds.length > 0 ? `训练参考数据集：${boundDatasetIds.join(", ")}` : null,
    `官方市场数据集 ID：${officialPreflight?.official_market_dataset_id ?? OFFICIAL_MARKET_DATASET_ID}`,
    officialPreflight?.official_multimodal_dataset_id || hasOfficialAuxiliaryContext
      ? `官方多模态数据集 ID：${officialPreflight?.official_multimodal_dataset_id ?? OFFICIAL_MULTIMODAL_DATASET_ID}`
      : null,
    officialTemplate?.scenario_bundle?.length
      ? `场景包：${officialTemplate.scenario_bundle.join(", ")}`
      : null,
  ].filter((item): item is string => Boolean(item));
  const localizedGateReasons = officialGateReasons.map((reason, index) =>
    localizeBacktestGateReason(
      reason,
      officialPreflight?.nlp_gate_reason_codes?.[index] ?? null,
    ),
  );
  const featureItems = [
    `官方 Schema 版本：${officialSchemaVersion}`,
    ...(officialPreflight?.required_feature_names?.length
      ? officialPreflight.required_feature_names.map((featureName) => `模型实际所需特征：${featureName}`)
      : []),
    ...officialSchemaMissingFeatures.map((featureName) => `缺失官方特征：${featureName}`),
    ...officialSchemaFeatureNames.map((featureName) => `官方标准特征：${featureName}`),
  ];
  const customDatasetSummary =
    customDatasetIds.length > 1
      ? `将提交 ${customDatasetIds.length} 个数据集 ID。`
      : customDatasetIds.length === 1
        ? `将直接使用数据集 ${customDatasetIds[0]}。`
        : datasetId.trim()
          ? `将直接使用数据集 ${datasetId.trim()}。`
          : `当前会使用数据集预置：${localizeBacktestOptionLabel(datasetPreset)}。`;

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
    if (initialMode) {
      return;
    }
    if (optionsQuery.data?.default_mode) {
      setMode(optionsQuery.data.default_mode);
    }
  }, [initialMode, optionsQuery.data?.default_mode]);

  useEffect(() => {
    if (initialMode) {
      setMode(initialMode);
    }
  }, [initialMode]);

  useEffect(() => {
    const value = optionsQuery.data?.constraints?.research_backend;
    const defaultValue =
      value && typeof value === "object" && "default" in value ? value.default : null;
    if (defaultValue === "native" || defaultValue === "vectorbt") {
      setResearchBackend(defaultValue);
    }
  }, [optionsQuery.data?.constraints]);

  useEffect(() => {
    const value = optionsQuery.data?.constraints?.portfolio_method;
    const defaultValue =
      value && typeof value === "object" && "default" in value ? value.default : null;
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

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const handleKeydown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", handleKeydown);
    return () => window.removeEventListener("keydown", handleKeydown);
  }, [open]);

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
        dataset_ids: mode === "custom" && customDatasetIds.length > 1 ? customDatasetIds : undefined,
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
  const submitButtonLabel = mutation.isPending
    ? "提交中..."
    : mode === "official" && officialPreflightPending
      ? "兼容性检查中..."
      : I18N.action.submit;
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
    if (mode === "custom" && !datasetId.trim() && customDatasetIds.length === 0 && !datasetPreset) {
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
        data-testid="launch-backtest-trigger"
        onClick={() => setOpen(true)}
        type="button"
      >
        {I18N.action.launchBacktest}
      </button>
      {open ? (
        <div
          className="dialog-backdrop backtest-launch-backdrop"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              setOpen(false);
            }
          }}
          role="presentation"
        >
          <div
            aria-label="发起回测"
            aria-modal="true"
            className="dialog-shell backtest-launch-modal"
            role="dialog"
          >
            <div className="backtest-launch-header">
              <div className="backtest-launch-header-copy">
                <strong>{I18N.action.launchBacktest}</strong>
                <p>
                  {mode === "official"
                    ? "先确认兼容性摘要，再决定是否展开协议细节。"
                    : "先填写必要参数，再按需要展开高级选项。"}
                </p>
              </div>
              <button className="link-button" onClick={() => setOpen(false)} type="button">
                {I18N.action.close}
              </button>
            </div>

            <div className="backtest-launch-scroll">
              <div className="segmented-tabs" role="tablist" aria-label="回测模式">
                <button
                  className={`tab-chip ${mode === "official" ? "active" : ""}`}
                  data-testid="backtest-mode-official"
                  onClick={() => setMode("official")}
                  type="button"
                >
                  官方模板
                </button>
                <button
                  className={`tab-chip ${mode === "custom" ? "active" : ""}`}
                  data-testid="backtest-mode-custom"
                  onClick={() => setMode("custom")}
                  type="button"
                >
                  自定义回测
                </button>
              </div>

              <section className="backtest-launch-section">
                <div className="backtest-launch-topbar">
                  <div className="backtest-launch-primary-grid">
                    <label className="backtest-launch-field-wide">
                      <span>训练实例 ID</span>
                      <input
                        data-testid="backtest-run-id"
                        onChange={(event) => setRunId(event.target.value)}
                        value={runId}
                      />
                    </label>

                    {mode === "official" ? (
                      <label>
                        <span>官方窗口</span>
                        <select
                          data-testid="official-window-days-select"
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
                    ) : (
                      <label>
                        <span>预测范围</span>
                        <select
                          onChange={(event) =>
                            setPredictionScope(event.target.value as "full" | "test")
                          }
                          value={predictionScope}
                        >
                          {(optionsQuery.data?.prediction_scopes ?? []).map((option) => (
                            <option key={option.value} value={option.value}>
                              {localizeBacktestOptionLabel(option.value, option.label)}
                            </option>
                          ))}
                        </select>
                      </label>
                    )}

                    <label>
                      <span>基准符号</span>
                      <input
                        onChange={(event) => setBenchmarkSymbol(event.target.value)}
                        value={benchmarkSymbol}
                      />
                    </label>
                  </div>

                  <div className="backtest-launch-submit">
                    <button
                      className="action-button secondary"
                      data-testid="submit-backtest-launch"
                      disabled={
                        mutation.isPending ||
                        optionsQuery.isLoading ||
                        officialPreflightPending ||
                        officialSubmitBlocked
                      }
                      onClick={handleSubmit}
                      type="button"
                    >
                      {submitButtonLabel}
                    </button>
                    <span>
                      {mode === "official"
                        ? "官方模板会固定使用测试集，并按官方窗口发起。"
                        : "自定义模式会优先采用你填写的数据集 ID。"}
                    </span>
                  </div>
                </div>
              </section>

              {mode === "official" ? (
                <>
                  {!hasRunId ? (
                    <section className="backtest-launch-summary">
                      <div className="backtest-launch-summary-head">
                        <div>
                          <strong>
                            {localizeBacktestTemplateName(
                              officialTemplate?.name,
                              officialTemplate?.template_id,
                            )}
                          </strong>
                          <p>请先输入训练实例 ID，再查看兼容性摘要和官方协议细节。</p>
                        </div>
                      </div>
                    </section>
                  ) : (
                    <>
                      <section className="backtest-launch-summary">
                        <div className="backtest-launch-summary-head">
                          <div>
                            <strong>
                              {localizeBacktestTemplateName(
                                officialTemplate?.name,
                                officialTemplate?.template_id,
                              )}
                            </strong>
                            <p>
                              {officialPreflightPending
                                ? "正在校验官方窗口、市场锚点和多模态约束。"
                                : "首屏只保留发起所需摘要，详细协议默认收起。"}
                            </p>
                          </div>
                          <span
                            className={`backtest-launch-status is-${compatibilityTone(
                              officialCompatibilityLabel,
                            )}`}
                          >
                            {officialCompatibilityLabel}
                          </span>
                        </div>

                        <SummaryGrid items={officialSummaryItems} />
                      </section>

                      <section className="backtest-launch-section">
                        <div className="backtest-launch-section-head">
                          <strong>关键窗口</strong>
                          <span>只展示最影响发起判断的时间窗。</span>
                        </div>
                        <SummaryGrid items={officialWindowItems} />
                      </section>

                      <details className="backtest-launch-disclosure">
                        <summary>基准与数据绑定</summary>
                        <DisclosureList
                          emptyText="当前没有可展示的数据绑定信息。"
                          items={bindingItems}
                        />
                      </details>

                      <details className="backtest-launch-disclosure">
                        <summary>模板规则</summary>
                        <DisclosureList
                          emptyText="当前模板没有额外规则说明。"
                          items={officialRequirements}
                        />
                      </details>

                      {(officialDatasetIds.length > 0 || officialModalityQualitySummary) ? (
                        <details className="backtest-launch-disclosure">
                          <summary>五模态质量与官方数据绑定</summary>
                          <div className="backtest-launch-disclosure-body">
                            <SummaryGrid
                              items={[
                                {
                                  label: "Required Modalities",
                                  value:
                                    officialRequiredModalities.length > 0
                                      ? officialRequiredModalities
                                          .map((modality) => formatModalityLabel(modality))
                                          .join(" / ")
                                      : "--",
                                },
                                {
                                  label: "Official Dataset IDs",
                                  value:
                                    officialDatasetIds.length > 0
                                      ? officialDatasetIds.join(" / ")
                                      : "--",
                                },
                              ]}
                            />
                            {officialQualityBlockingReasons.length > 0 ? (
                              <DisclosureList
                                emptyText="No quality blocking reasons."
                                items={officialQualityBlockingReasons}
                              />
                            ) : null}
                            <ModalityQualitySummary
                              emptyText="No modality quality summary returned for official preflight."
                              modalities={
                                officialRequiredModalities.length > 0
                                  ? officialRequiredModalities
                                  : undefined
                              }
                              summary={officialModalityQualitySummary}
                              title="Official modality quality summary"
                            />
                          </div>
                        </details>
                      ) : null}

                      {(localizedGateReasons.length > 0 || hasOfficialNlpContext) ? (
                        <details className="backtest-launch-disclosure">
                          <summary>NLP 门禁说明</summary>
                          <div className="backtest-launch-disclosure-body">
                            <SummaryGrid
                              items={[
                                {
                                  label: "NLP Gate",
                                  value: gateStatusLabel(
                                    officialNlpGateStatus,
                                  ),
                                  tone: officialNlpGateFailed ? "danger" : "default",
                                },
                                {
                                  label: "仅归档型 NLP",
                                  value: boolLabel(multimodalReadiness?.archival_nlp_source_only),
                                },
                              ]}
                            />
                            <DisclosureList
                              emptyText="当前没有额外的门禁说明。"
                              items={localizedGateReasons}
                            />
                          </div>
                        </details>
                      ) : null}

                      {officialPreflight && !officialPreflightPending ? (
                        <details className="backtest-launch-disclosure">
                          <summary>特征契约</summary>
                          <DisclosureList
                            emptyText="当前没有需要展示的特征契约信息。"
                            items={featureItems}
                          />
                        </details>
                      ) : null}
                    </>
                  )}
                </>
              ) : (
                <section className="backtest-launch-section">
                  <div className="backtest-launch-section-head">
                    <strong>数据来源</strong>
                    <span>可填单个数据集、多个数据集，或回退到数据集预置。</span>
                  </div>
                  <div className="backtest-launch-primary-grid">
                    <label>
                      <span>数据集 ID</span>
                      <input onChange={(event) => setDatasetId(event.target.value)} value={datasetId} />
                    </label>
                    <label>
                      <span>数据集预置</span>
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
                    <label className="backtest-launch-field-wide">
                      <span>数据集 ID 列表</span>
                      <textarea
                        className="field area-field"
                        onChange={(event) => setDatasetIdsText(event.target.value)}
                        placeholder="每行一个 dataset_id，或用逗号分隔"
                        value={datasetIdsText}
                      />
                    </label>
                  </div>
                  <div className="backtest-launch-inline-note">{customDatasetSummary}</div>
                </section>
              )}

              <details className="backtest-launch-disclosure backtest-launch-advanced">
                <summary>高级选项</summary>
                <div className="backtest-launch-disclosure-body">
                  <div className="backtest-launch-primary-grid">
                    <label>
                      <span>研究引擎</span>
                      <select
                        onChange={(event) =>
                          setResearchBackend(event.target.value as ResearchBackend)
                        }
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
                      <span>组合方法</span>
                      <select
                        onChange={(event) =>
                          setPortfolioMethod(event.target.value as PortfolioMethod)
                        }
                        value={portfolioMethod}
                      >
                        {portfolioMethodOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {localizeBacktestOptionLabel(option.value, option.label)}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                  <p className="backtest-launch-disclosure-empty">
                    默认路径保持为原生引擎 + 比例分配，只有研究对比时才建议改这里。
                  </p>
                </div>
              </details>

              {formError ? <p className="form-error">{formError}</p> : null}
              {mutation.isError ? (
                <p className="form-error">{(mutation.error as Error).message}</p>
              ) : null}

              {jobQuery.data ? (
                <section className="backtest-launch-section">
                  <div className="backtest-launch-section-head">
                    <strong>提交结果</strong>
                    <span>任务创建后会在这里更新状态和跳转入口。</span>
                  </div>
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
                        打开回测详情
                      </button>
                    ) : null}
                  </div>
                </section>
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
