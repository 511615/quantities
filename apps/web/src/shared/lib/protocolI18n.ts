const TEMPLATE_NAME_MAP: Record<string, string> = {
  "system::official_backtest_protocol_v1": "官方回测协议 v1",
  "custom::interactive": "自定义交互式回测",
};

const REQUIREMENT_MAP: Record<string, string> = {
  "Model output must follow the prediction_frame_v1 contract.": "模型输出必须遵守 prediction_frame_v1 契约。",
  "Training-time disclosure fields must be populated before official comparison is trusted.": "参与官方同模板对比前，必须补齐训练阶段的披露字段。",
  "Official mode binds to the newest official rolling benchmark and ignores dataset overrides.": "官方模式会绑定最新滚动官方 benchmark，并忽略 dataset_id / dataset_ids 覆盖。",
  "Official mode allows only fixed window presets: 30, 90, 180, and 365 days.": "官方模式只允许固定窗口档位：30、90、180、365 天。",
  "Official ranking compares runs only when the official benchmark version and window size match.": "官方排名只会比较官方 benchmark 版本和窗口大小都一致的结果。",
  "If NLP is used, the requested NLP collection window must match the market template window.": "如果使用 NLP，申请的 NLP 采集时间窗必须与市场模板时间窗一致。",
  "If NLP is used, only archival NLP sources are eligible for official same-template comparison.": "如果使用 NLP，只有归档型 NLP 数据源才允许参加官方同模板对比。",
  "If NLP is used, the official gate requires test-window coverage >= 60%, max empty gap <= 168 bars, duplicate ratio <= 5%, and entity link coverage >= 95%.": "如果使用 NLP，官方门禁要求测试窗口覆盖率不低于 60%，最大连续空档不超过 168 根 bar，重复率不高于 5%，实体关联覆盖率不低于 95%。",
  "Any compatible run can be launched in custom mode.": "任何兼容的训练实例都可以在自定义模式下发起回测。",
  "The official template is read-only and cannot be deleted.": "官方模板为只读，不允许删除。",
  "The official template locks prediction scope to test and defaults the benchmark to BTCUSDT.": "官方模板会把预测范围锁定为测试集，并默认使用 BTCUSDT 作为基准。",
  "The official template always uses the newest available market environment instead of the training dataset window.": "官方模板始终使用最新可用市场环境，而不是沿用训练数据时间窗。",
  "Window size is user-selectable, but official rankings only compare results that use the same window preset.": "窗口大小可以由用户选择，但官方排名只比较使用同一窗口档位的结果。",
  "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible.": "自定义模式下，数据集预设、预测范围、策略、组合和成本参数都可以灵活调整。",
  "Custom mode stays visible for inspection but is excluded from official ranking.": "自定义模式会保留给研究检查使用，但不会进入官方排名。",
};

const METADATA_MAP: Record<string, string> = {
  "Training dataset start/end time": "训练数据集起止时间",
  "Lookback window / context length": "回看窗口 / 上下文长度",
  "Label horizon": "标签跨度",
  "Modalities and fusion summary": "模态与融合摘要",
  "Random seed": "随机种子",
  "Tuning trial count": "调参轮数",
  "External pretraining flag": "是否使用外部预训练",
  "Synthetic data flag": "是否使用合成数据",
  "Actual market dataset window": "实际市场数据窗口",
  "Actual official backtest test window": "官方回测测试窗口",
  "Actual NLP coverage window and official NLP gate result when NLP is present": "存在 NLP 时的实际覆盖窗口与官方 NLP 门禁结果",
  "Official rolling benchmark version": "官方滚动 benchmark 版本",
  "Official rolling window size and actual window start/end time": "官方滚动窗口大小与实际起止时间",
  "Official market benchmark dataset id": "官方市场 benchmark 数据集 ID",
  "Official multimodal benchmark dataset id when text signals are used": "使用文本信号时的官方多模态 benchmark 数据集 ID",
};

const GATE_LABEL_MAP: Record<string, string> = {
  "Metadata Complete": "元数据完整性",
  "Research / Simulation Consistency": "研究与仿真一致性",
  "Stress Bundle Complete": "压力场景包完整性",
  "Risk Limits": "风险约束",
};

const GATE_DETAIL_MAP: Record<string, string> = {
  "Official comparison requires the training and backtest disclosure fields to be populated.": "官方对比要求训练和回测阶段的披露字段都已补齐。",
  "Research and simulation outputs must not show an abnormal inversion relationship.": "研究引擎与仿真引擎的输出不能出现异常反向关系。",
  "Official comparison requires the fixed stress bundle to be present.": "官方对比要求固定压力场景包完整存在。",
  "Requires max_drawdown <= 0.35, turnover_total <= 24.00, stress_fail_count <= 0.": "要求最大回撤不高于 0.35、总换手不高于 24.00，且压力场景失败次数必须为 0。",
  "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied.": "官方 NLP 门禁已通过：数据源为归档型，时间窗已对齐，且质量阈值满足要求。",
};

const WARNING_MAP: Record<string, string> = {
  "simulation shortfall is better than research shortfall": "仿真实现短缺优于研究实现短缺，需要人工复核。",
};

const PROTOCOL_VALUE_MAP: Record<string, string> = {
  market: "市场",
  sentiment_events: "情绪事件",
  late_score_blend: "晚期得分融合",
};

export function localizeBacktestTemplateName(name?: string | null, templateId?: string | null) {
  if (templateId && TEMPLATE_NAME_MAP[templateId]) {
    return TEMPLATE_NAME_MAP[templateId];
  }
  return name ?? "回测协议";
}

export function localizeBacktestRequirement(value: string) {
  return REQUIREMENT_MAP[value] ?? value;
}

export function localizeBacktestMetadata(value: string) {
  return METADATA_MAP[value] ?? value;
}

export function localizeBacktestGateLabel(value: string) {
  return GATE_LABEL_MAP[value] ?? value;
}

export function localizeBacktestGateDetail(value?: string | null) {
  if (!value) {
    return "--";
  }
  return GATE_DETAIL_MAP[value] ?? value;
}

export function localizeBacktestGateReason(value: string) {
  return GATE_DETAIL_MAP[value] ?? value;
}

export function localizeBacktestWarning(value: string) {
  return WARNING_MAP[value] ?? value;
}

export function localizeProtocolValue(value?: string | null) {
  if (!value) {
    return "--";
  }
  return PROTOCOL_VALUE_MAP[value] ?? value;
}
