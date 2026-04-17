import { getCurrentLocale, type Locale } from "./i18n";

type LocalizedRecord = Record<Locale, string>;

function pick(value: LocalizedRecord): string {
  return value[getCurrentLocale()];
}

const TEMPLATE_NAME_MAP: Record<string, LocalizedRecord> = {
  "system::official_backtest_protocol_v1": {
    "zh-CN": "官方回测协议 v1",
    "en-US": "Official Backtest Protocol v1",
  },
  "custom::interactive": {
    "zh-CN": "自定义交互式回测",
    "en-US": "Custom Interactive Backtest",
  },
};

const REQUIREMENT_KEY_MAP: Record<string, LocalizedRecord> = {
  prediction_frame_contract: {
    "zh-CN": "模型输出必须遵守 prediction_frame_v1 契约。",
    "en-US": "Model output must follow the prediction_frame_v1 contract.",
  },
  training_disclosure_required: {
    "zh-CN": "参与官方对比前，必须补齐训练阶段的披露字段。",
    "en-US": "Training-time disclosure fields must be populated before official comparison is trusted.",
  },
  official_latest_benchmark_binding: {
    "zh-CN": "官方模式会绑定最新官方滚动基准，并忽略数据集覆盖。",
    "en-US": "Official mode binds to the newest official rolling benchmark and ignores dataset overrides.",
  },
  official_fixed_window_options: {
    "zh-CN": "官方模式只允许固定窗口：30、90、180、365 天。",
    "en-US": "Official mode allows only fixed window presets: 30, 90, 180, and 365 days.",
  },
  official_same_slice_ranking: {
    "zh-CN": "官方排名只比较官方基准版本和窗口大小都一致的结果。",
    "en-US": "Official ranking compares runs only when the official benchmark version and window size match.",
  },
  official_multimodal_bundle_binding: {
    "zh-CN": "如果使用非市场模态，官方模式会将评估绑定到官方多模态基准包。",
    "en-US": "If non-market modalities are used, official mode binds evaluation to the official multimodal benchmark bundle.",
  },
  official_nlp_window_alignment: {
    "zh-CN": "如果使用 NLP，申请的 NLP 采集窗口必须与市场模板窗口一致。",
    "en-US": "If NLP is used, the requested NLP collection window must match the market template window.",
  },
  official_source_asset_alignment: {
    "zh-CN": "官方来源校验关注基准资产对齐和平台兼容 schema，而不是训练 vendor 标签。",
    "en-US": "Official source checks focus on benchmark asset alignment and platform-compatible schema, not the training vendor label.",
  },
  official_nlp_quality_thresholds: {
    "zh-CN": "如果使用 NLP，官方门禁要求测试窗口覆盖率 >= 60%、最大空窗 <= 168 bars、重复率 <= 5%、实体链接覆盖率 >= 95%。",
    "en-US": "If NLP is used, the official gate requires test-window coverage >= 60%, max empty gap <= 168 bars, duplicate ratio <= 5%, and entity link coverage >= 95%.",
  },
  custom_any_compatible_run: {
    "zh-CN": "任何兼容的训练实例都可以在自定义模式下发起回测。",
    "en-US": "Any compatible run can be launched in custom mode.",
  },
};

const METADATA_KEY_MAP: Record<string, LocalizedRecord> = {
  train_dataset_window: { "zh-CN": "训练数据集起止时间", "en-US": "Training dataset start/end time" },
  lookback_window: { "zh-CN": "回看窗口 / 上下文长度", "en-US": "Lookback window / context length" },
  label_horizon: { "zh-CN": "标签跨度", "en-US": "Label horizon" },
  modalities_and_fusion_summary: { "zh-CN": "模态与融合摘要", "en-US": "Modalities and fusion summary" },
  random_seed: { "zh-CN": "随机种子", "en-US": "Random seed" },
  tuning_trial_count: { "zh-CN": "调参轮数", "en-US": "Tuning trial count" },
  external_pretraining_flag: { "zh-CN": "是否使用外部预训练", "en-US": "External pretraining flag" },
  synthetic_data_flag: { "zh-CN": "是否使用合成数据", "en-US": "Synthetic data flag" },
  actual_market_dataset_window: { "zh-CN": "实际市场数据窗口", "en-US": "Actual market dataset window" },
  actual_official_backtest_window: { "zh-CN": "实际官方回测测试窗口", "en-US": "Actual official backtest test window" },
  actual_nlp_coverage_window: {
    "zh-CN": "存在 NLP 时的实际覆盖窗口与官方 NLP 门禁结果",
    "en-US": "Actual NLP coverage window and official NLP gate result when NLP is present",
  },
  required_modalities_resolved: { "zh-CN": "官方运行解析后的必需模态", "en-US": "Required modalities resolved for the official run" },
  official_rolling_benchmark_version: { "zh-CN": "官方滚动基准版本", "en-US": "Official rolling benchmark version" },
  official_rolling_window_size: { "zh-CN": "官方滚动窗口大小与实际起止时间", "en-US": "Official rolling window size and actual window start/end time" },
  official_market_benchmark_dataset_id: { "zh-CN": "官方市场基准数据集 ID", "en-US": "Official market benchmark dataset id" },
  official_multimodal_benchmark_dataset_id: {
    "zh-CN": "使用非市场信号时的官方多模态基准数据集 ID",
    "en-US": "Official multimodal benchmark dataset id when non-market signals are used",
  },
};

const NOTE_KEY_MAP: Record<string, LocalizedRecord> = {
  official_template_read_only: { "zh-CN": "官方模板为只读，不能删除。", "en-US": "The official template is read-only and cannot be deleted." },
  official_template_fixed_prediction_scope: { "zh-CN": "官方模板会将预测范围固定为测试集，并默认使用 BTCUSDT 作为基准。", "en-US": "The official template locks prediction scope to test and defaults the benchmark to BTCUSDT." },
  official_template_latest_market_environment: { "zh-CN": "官方模板始终使用最新可用市场环境，而不是沿用训练数据窗口。", "en-US": "The official template always uses the newest available market environment instead of the training dataset window." },
  official_window_comparison_scope: { "zh-CN": "窗口大小可以选择，但官方排名只比较同一窗口档位的结果。", "en-US": "Window size is user-selectable, but official rankings only compare results that use the same window preset." },
  custom_mode_flexible_controls: { "zh-CN": "自定义模式保留数据集预置、范围、策略、组合和成本的灵活配置。", "en-US": "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible." },
  custom_visible_but_unranked: { "zh-CN": "自定义模式保留可见性供检查使用，但不参与官方排名。", "en-US": "Custom mode stays visible for inspection but is excluded from official ranking." },
};

const GATE_LABEL_KEY_MAP: Record<string, LocalizedRecord> = {
  metadata_complete: { "zh-CN": "元数据完整", "en-US": "Metadata Complete" },
  research_simulation_consistency: { "zh-CN": "研究 / 仿真一致性", "en-US": "Research / Simulation Consistency" },
  stress_bundle_complete: { "zh-CN": "压力场景包完整", "en-US": "Stress Bundle Complete" },
  risk_limits: { "zh-CN": "风险约束", "en-US": "Risk Limits" },
  official_nlp_quality_gate: { "zh-CN": "官方 NLP 质量门禁", "en-US": "Official NLP Quality Gate" },
};

const GATE_DETAIL_KEY_MAP: Record<string, LocalizedRecord> = {
  training_and_backtest_disclosure_required: {
    "zh-CN": "官方对比要求训练与回测披露字段都已补齐。",
    "en-US": "Official comparison requires the training and backtest disclosure fields to be populated.",
  },
  research_and_simulation_no_abnormal_inversion: {
    "zh-CN": "研究与仿真输出不能出现异常反向关系。",
    "en-US": "Research and simulation outputs must not show an abnormal inversion relationship.",
  },
  fixed_stress_bundle_required: {
    "zh-CN": "官方对比要求固定压力场景包完整存在。",
    "en-US": "Official comparison requires the fixed stress bundle to be present.",
  },
  official_risk_limit_thresholds: {
    "zh-CN": "要求 max_drawdown <= 0.35、turnover_total <= 24.00 且 stress_fail_count <= 0。",
    "en-US": "Requires max_drawdown <= 0.35, turnover_total <= 24.00, stress_fail_count <= 0.",
  },
  official_nlp_gate_passed: {
    "zh-CN": "官方 NLP 门禁已通过：来源为归档型、时间窗口对齐且质量阈值达标。",
    "en-US": "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied.",
  },
  official_nlp_quality_gate_failed: {
    "zh-CN": "官方回测模板被 NLP 质量门禁阻断。",
    "en-US": "Official backtest template is blocked by the NLP quality gate.",
  },
  official_nlp_gate_missing_detail: {
    "zh-CN": "NLP 门禁未返回详细信息。",
    "en-US": "NLP gate did not report details.",
  },
};

const BLOCKING_REASON_CODE_MAP: Record<string, LocalizedRecord> = {
  official_missing_features: {
    "zh-CN": "官方基准数据集缺少模型需要的特征。",
    "en-US": "Official benchmark dataset is missing required model features.",
  },
  official_nlp_quality_gate_failed: {
    "zh-CN": "官方回测被 NLP 质量门禁阻断。",
    "en-US": "Official backtest is blocked by the NLP quality gate.",
  },
  source_market_anchor_vendor_mismatch: {
    "zh-CN": "训练运行使用的市场锚点 vendor 与官方环境不一致。",
    "en-US": "The source run uses a market anchor vendor different from the official environment.",
  },
  source_market_anchor_asset_mismatch: {
    "zh-CN": "训练运行使用的市场锚点资产与官方基准资产不一致。",
    "en-US": "The source run uses market anchor assets that do not match the official benchmark asset.",
  },
  source_market_vendor_mismatch: {
    "zh-CN": "训练运行使用的市场 vendor 与官方环境不一致。",
    "en-US": "The source run uses a market vendor different from the official environment.",
  },
  source_market_asset_mismatch: {
    "zh-CN": "训练运行使用的市场资产与官方基准资产不一致。",
    "en-US": "The source run uses market assets that do not match the official benchmark asset.",
  },
  source_label_vendor_mismatch: {
    "zh-CN": "训练运行使用的标签来源 vendor 与官方环境不一致。",
    "en-US": "The source run uses a label-source vendor different from the official environment.",
  },
  source_nlp_vendor_mismatch: {
    "zh-CN": "训练运行使用的 NLP vendor 与官方环境不一致。",
    "en-US": "The source run uses an NLP vendor different from the official environment.",
  },
  source_nlp_symbol_mismatch: {
    "zh-CN": "训练运行绑定的 NLP 资产与官方基准资产不一致。",
    "en-US": "The source run uses NLP-linked symbols that do not match the official benchmark asset.",
  },
  official_market_benchmark_unavailable: {
    "zh-CN": "官方市场基准数据集当前不可用。",
    "en-US": "The official market benchmark dataset is not available.",
  },
  official_multimodal_benchmark_unavailable: {
    "zh-CN": "官方多模态基准数据集当前不可用。",
    "en-US": "The official multimodal benchmark dataset is not available.",
  },
};

const WARNING_KEY_MAP: Record<string, LocalizedRecord> = {
  simulation_shortfall_better_than_research_shortfall: {
    "zh-CN": "仿真实现短缺优于研究实现短缺，建议人工复核。",
    "en-US": "Simulation shortfall is better than research shortfall and should be reviewed manually.",
  },
};

const PROTOCOL_VALUE_MAP: Record<string, LocalizedRecord> = {
  market: { "zh-CN": "市场", "en-US": "Market" },
  sentiment_events: { "zh-CN": "情绪事件", "en-US": "Sentiment Events" },
  late_score_blend: { "zh-CN": "后期得分融合", "en-US": "Late Score Blend" },
  test: { "zh-CN": "测试集", "en-US": "Test" },
};

const FALLBACK_REQUIREMENT_MAP: Record<string, LocalizedRecord> = Object.fromEntries(
  Object.entries(REQUIREMENT_KEY_MAP).map(([key, value]) => [value["en-US"], value]),
);
const FALLBACK_METADATA_MAP: Record<string, LocalizedRecord> = Object.fromEntries(
  Object.entries(METADATA_KEY_MAP).map(([key, value]) => [value["en-US"], value]),
);
const FALLBACK_NOTE_MAP: Record<string, LocalizedRecord> = Object.fromEntries(
  Object.entries(NOTE_KEY_MAP).map(([key, value]) => [value["en-US"], value]),
);
const FALLBACK_GATE_LABEL_MAP: Record<string, LocalizedRecord> = Object.fromEntries(
  Object.entries(GATE_LABEL_KEY_MAP).map(([key, value]) => [value["en-US"], value]),
);
const FALLBACK_GATE_DETAIL_MAP: Record<string, LocalizedRecord> = Object.fromEntries(
  Object.entries(GATE_DETAIL_KEY_MAP).map(([key, value]) => [value["en-US"], value]),
);

export function localizeBacktestTemplateName(name?: string | null, templateId?: string | null) {
  return templateId
    ? pick(TEMPLATE_NAME_MAP[templateId] ?? { "zh-CN": name ?? "回测协议", "en-US": name ?? "Backtest Protocol" })
    : name ?? pick({ "zh-CN": "回测协议", "en-US": "Backtest Protocol" });
}

export function localizeBacktestRequirement(value: string, key?: string | null) {
  return key && REQUIREMENT_KEY_MAP[key]
    ? pick(REQUIREMENT_KEY_MAP[key])
    : FALLBACK_REQUIREMENT_MAP[value]
      ? pick(FALLBACK_REQUIREMENT_MAP[value])
      : value;
}

export function localizeBacktestMetadata(value: string, key?: string | null) {
  return key && METADATA_KEY_MAP[key]
    ? pick(METADATA_KEY_MAP[key])
    : FALLBACK_METADATA_MAP[value]
      ? pick(FALLBACK_METADATA_MAP[value])
      : value;
}

export function localizeBacktestNote(value: string, key?: string | null) {
  return key && NOTE_KEY_MAP[key]
    ? pick(NOTE_KEY_MAP[key])
    : FALLBACK_NOTE_MAP[value]
      ? pick(FALLBACK_NOTE_MAP[value])
      : value;
}

export function localizeBacktestGateLabel(value: string, key?: string | null) {
  return key && GATE_LABEL_KEY_MAP[key]
    ? pick(GATE_LABEL_KEY_MAP[key])
    : FALLBACK_GATE_LABEL_MAP[value]
      ? pick(FALLBACK_GATE_LABEL_MAP[value])
      : value;
}

export function localizeBacktestGateDetail(value?: string | null, key?: string | null) {
  if (!value && !key) {
    return "--";
  }
  if (key && GATE_DETAIL_KEY_MAP[key]) {
    return pick(GATE_DETAIL_KEY_MAP[key]);
  }
  if (value && FALLBACK_GATE_DETAIL_MAP[value]) {
    return pick(FALLBACK_GATE_DETAIL_MAP[value]);
  }
  return value ?? "--";
}

export function localizeBacktestGateReason(value: string, key?: string | null) {
  return localizeBacktestGateDetail(value, key);
}

export function localizeBlockingReason(value: string, code?: string | null) {
  return code && BLOCKING_REASON_CODE_MAP[code] ? pick(BLOCKING_REASON_CODE_MAP[code]) : value;
}

export function localizeBacktestWarning(value: string, key?: string | null) {
  return key && WARNING_KEY_MAP[key] ? pick(WARNING_KEY_MAP[key]) : value;
}

export function localizeProtocolValue(value?: string | null) {
  if (!value) {
    return "--";
  }
  return PROTOCOL_VALUE_MAP[value] ? pick(PROTOCOL_VALUE_MAP[value]) : value;
}
