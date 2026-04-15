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

const REQUIREMENT_MAP: Record<string, LocalizedRecord> = {
  "Model output must follow the prediction_frame_v1 contract.": {
    "zh-CN": "模型输出必须遵守 prediction_frame_v1 契约。",
    "en-US": "Model output must follow the prediction_frame_v1 contract.",
  },
  "Training-time disclosure fields must be populated before official comparison is trusted.": {
    "zh-CN": "参与官方对比前，必须补齐训练阶段的披露字段。",
    "en-US": "Training-time disclosure fields must be filled before official comparison is trusted.",
  },
  "Official mode binds to the newest official rolling benchmark and ignores dataset overrides.": {
    "zh-CN": "官方模式会绑定最新官方滚动 benchmark，并忽略 dataset 覆盖。",
    "en-US": "Official mode binds to the latest official rolling benchmark and ignores dataset overrides.",
  },
  "Official mode allows only fixed window presets: 30, 90, 180, and 365 days.": {
    "zh-CN": "官方模式只允许固定窗口档位：30、90、180、365 天。",
    "en-US": "Official mode allows only fixed window presets: 30, 90, 180, and 365 days.",
  },
  "Official ranking compares runs only when the official benchmark version and window size match.": {
    "zh-CN": "官方排名只比较 benchmark 版本和窗口大小都一致的结果。",
    "en-US": "Official ranking compares runs only when official benchmark version and window size match.",
  },
  "If NLP is used, the requested NLP collection window must match the market template window.": {
    "zh-CN": "如果使用 NLP，申请的 NLP 采集窗口必须与市场模板窗口一致。",
    "en-US": "If NLP is used, the requested NLP collection window must match the market template window.",
  },
  "If NLP is used, only archival NLP sources are eligible for official same-template comparison.": {
    "zh-CN": "如果使用 NLP，只有归档型 NLP 数据源允许参与官方同模板对比。",
    "en-US": "If NLP is used, only archival NLP sources are eligible for official same-template comparison.",
  },
  "If NLP is used, the official gate requires test-window coverage >= 60%, max empty gap <= 168 bars, duplicate ratio <= 5%, and entity link coverage >= 95%.": {
    "zh-CN": "如果使用 NLP，官方门禁要求测试窗口覆盖率 >= 60%，最大空档 <= 168 bars，重复率 <= 5%，实体链接覆盖率 >= 95%。",
    "en-US": "If NLP is used, the official gate requires >=60% test-window coverage, <=168 bars max empty gap, <=5% duplicate ratio, and >=95% entity link coverage.",
  },
  "Any compatible run can be launched in custom mode.": {
    "zh-CN": "任何兼容的训练实例都可以在自定义模式下发起回测。",
    "en-US": "Any compatible run can be launched in custom mode.",
  },
  "The official template is read-only and cannot be deleted.": {
    "zh-CN": "官方模板为只读，不允许删除。",
    "en-US": "The official template is read-only and cannot be deleted.",
  },
  "The official template locks prediction scope to test and defaults the benchmark to BTCUSDT.": {
    "zh-CN": "官方模板会把预测范围锁定为测试集，并默认使用 BTCUSDT 作为基准。",
    "en-US": "The official template locks prediction scope to test and defaults the benchmark to BTCUSDT.",
  },
  "The official template always uses the newest available market environment instead of the training dataset window.": {
    "zh-CN": "官方模板始终使用最新可用市场环境，而不是沿用训练数据窗口。",
    "en-US": "The official template always uses the newest available market environment instead of the training dataset window.",
  },
  "Window size is user-selectable, but official rankings only compare results that use the same window preset.": {
    "zh-CN": "窗口大小可以由用户选择，但官方排名只比较同一窗口档位的结果。",
    "en-US": "Window size is user-selectable, but official rankings only compare results that use the same window preset.",
  },
  "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible.": {
    "zh-CN": "自定义模式下，数据集预置、预测范围、策略、组合和成本参数都可灵活调整。",
    "en-US": "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible.",
  },
  "Custom mode stays visible for inspection but is excluded from official ranking.": {
    "zh-CN": "自定义模式保留给研究检查使用，但不会进入官方排名。",
    "en-US": "Custom mode stays visible for inspection but is excluded from official ranking.",
  },
};

const METADATA_MAP: Record<string, LocalizedRecord> = {
  "Training dataset start/end time": {
    "zh-CN": "训练数据集起止时间",
    "en-US": "Training dataset start/end time",
  },
  "Lookback window / context length": {
    "zh-CN": "回看窗口 / 上下文长度",
    "en-US": "Lookback window / context length",
  },
  "Label horizon": {
    "zh-CN": "标签跨度",
    "en-US": "Label horizon",
  },
  "Modalities and fusion summary": {
    "zh-CN": "模态与融合摘要",
    "en-US": "Modalities and fusion summary",
  },
  "Random seed": {
    "zh-CN": "随机种子",
    "en-US": "Random seed",
  },
  "Tuning trial count": {
    "zh-CN": "调参轮数",
    "en-US": "Tuning trial count",
  },
  "External pretraining flag": {
    "zh-CN": "是否使用外部预训练",
    "en-US": "External pretraining flag",
  },
  "Synthetic data flag": {
    "zh-CN": "是否使用合成数据",
    "en-US": "Synthetic data flag",
  },
  "Actual market dataset window": {
    "zh-CN": "实际市场数据窗口",
    "en-US": "Actual market dataset window",
  },
  "Actual official backtest test window": {
    "zh-CN": "实际官方回测测试窗口",
    "en-US": "Actual official backtest test window",
  },
  "Actual NLP coverage window and official NLP gate result when NLP is present": {
    "zh-CN": "存在 NLP 时的实际覆盖窗口与官方门禁结果",
    "en-US": "Actual NLP coverage window and official NLP gate result when NLP is present",
  },
  "Official rolling benchmark version": {
    "zh-CN": "官方滚动 benchmark 版本",
    "en-US": "Official rolling benchmark version",
  },
  "Official rolling window size and actual window start/end time": {
    "zh-CN": "官方滚动窗口大小与实际起止时间",
    "en-US": "Official rolling window size and actual window start/end time",
  },
  "Official market benchmark dataset id": {
    "zh-CN": "官方市场 benchmark 数据集 ID",
    "en-US": "Official market benchmark dataset id",
  },
  "Official multimodal benchmark dataset id when text signals are used": {
    "zh-CN": "使用文本信号时的官方多模态 benchmark 数据集 ID",
    "en-US": "Official multimodal benchmark dataset id when text signals are used",
  },
};

const GATE_LABEL_MAP: Record<string, LocalizedRecord> = {
  "Metadata Complete": { "zh-CN": "元数据完整性", "en-US": "Metadata Complete" },
  "Research / Simulation Consistency": {
    "zh-CN": "研究 / 仿真一致性",
    "en-US": "Research / Simulation Consistency",
  },
  "Stress Bundle Complete": { "zh-CN": "压力场景包完整性", "en-US": "Stress Bundle Complete" },
  "Risk Limits": { "zh-CN": "风险约束", "en-US": "Risk Limits" },
};

const GATE_DETAIL_MAP: Record<string, LocalizedRecord> = {
  "Official comparison requires the training and backtest disclosure fields to be populated.": {
    "zh-CN": "官方对比要求训练与回测阶段的披露字段都已补齐。",
    "en-US": "Official comparison requires the training and backtest disclosure fields to be populated.",
  },
  "Research and simulation outputs must not show an abnormal inversion relationship.": {
    "zh-CN": "研究引擎与仿真引擎的输出不能出现异常反向关系。",
    "en-US": "Research and simulation outputs must not show an abnormal inversion relationship.",
  },
  "Official comparison requires the fixed stress bundle to be present.": {
    "zh-CN": "官方对比要求固定压力场景包完整存在。",
    "en-US": "Official comparison requires the fixed stress bundle to be present.",
  },
  "Requires max_drawdown <= 0.35, turnover_total <= 24.00, stress_fail_count <= 0.": {
    "zh-CN": "要求最大回撤 <= 0.35、总换手 <= 24.00，且压力场景失败次数必须为 0。",
    "en-US": "Requires max_drawdown <= 0.35, turnover_total <= 24.00, stress_fail_count <= 0.",
  },
  "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied.": {
    "zh-CN": "官方 NLP 门禁已通过：数据源为归档型、时间窗口已对齐且质量阈值达标。",
    "en-US": "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied.",
  },
};

const WARNING_MAP: Record<string, LocalizedRecord> = {
  "simulation shortfall is better than research shortfall": {
    "zh-CN": "仿真实现短缺优于研究实现短缺，需要人工复核。",
    "en-US": "Simulation shortfall is better than research shortfall and should be reviewed manually.",
  },
};

const PROTOCOL_VALUE_MAP: Record<string, LocalizedRecord> = {
  market: { "zh-CN": "市场", "en-US": "Market" },
  sentiment_events: { "zh-CN": "情绪事件", "en-US": "Sentiment Events" },
  late_score_blend: { "zh-CN": "晚期得分融合", "en-US": "Late Score Blend" },
};

export function localizeBacktestTemplateName(name?: string | null, templateId?: string | null) {
  return templateId ? pick(TEMPLATE_NAME_MAP[templateId] ?? { "zh-CN": name ?? "回测协议", "en-US": name ?? "Backtest Protocol" }) : name ?? pick({ "zh-CN": "回测协议", "en-US": "Backtest Protocol" });
}

export function localizeBacktestRequirement(value: string) {
  return REQUIREMENT_MAP[value] ? pick(REQUIREMENT_MAP[value]) : value;
}

export function localizeBacktestMetadata(value: string) {
  return METADATA_MAP[value] ? pick(METADATA_MAP[value]) : value;
}

export function localizeBacktestGateLabel(value: string) {
  return GATE_LABEL_MAP[value] ? pick(GATE_LABEL_MAP[value]) : value;
}

export function localizeBacktestGateDetail(value?: string | null) {
  if (!value) {
    return "--";
  }
  return GATE_DETAIL_MAP[value] ? pick(GATE_DETAIL_MAP[value]) : value;
}

export function localizeBacktestGateReason(value: string) {
  return GATE_DETAIL_MAP[value] ? pick(GATE_DETAIL_MAP[value]) : value;
}

export function localizeBacktestWarning(value: string) {
  return WARNING_MAP[value] ? pick(WARNING_MAP[value]) : value;
}

export function localizeProtocolValue(value?: string | null) {
  if (!value) {
    return "--";
  }
  return PROTOCOL_VALUE_MAP[value] ? pick(PROTOCOL_VALUE_MAP[value]) : value;
}
