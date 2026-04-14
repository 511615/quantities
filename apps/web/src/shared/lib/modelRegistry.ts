import type { ExperimentListItem, ModelTemplateView } from "../api/types";

export type ParameterField = {
  key: string;
  label: string;
  glossaryKey?: "learning_rate" | "tree_depth" | "regularization";
  defaultValue: number | string;
  step?: string;
  advanced?: boolean;
};

type ModelDefinition = {
  label: string;
  category: string;
  suitableData: string;
  parameterFields: ParameterField[];
  defaultHyperparams: Record<string, number | string>;
};

export type TemplateDraft = {
  template_id?: string;
  name: string;
  description: string;
  model_name: string;
  hyperparams: Record<string, number | string>;
  trainer_preset: string;
  dataset_preset: string;
  read_only: boolean;
};

const MODEL_DEFINITIONS: Record<string, ModelDefinition> = {
  elastic_net: {
    label: "弹性网络",
    category: "线性模型",
    suitableData: "中低维度连续型特征",
    defaultHyperparams: { alpha: 0.001, l1_ratio: 0.5 },
    parameterFields: [
      { key: "alpha", label: "正则强度", defaultValue: 0.001, step: "0.0001", glossaryKey: "regularization" },
      { key: "l1_ratio", label: "L1 占比", defaultValue: 0.5, step: "0.01", glossaryKey: "regularization" },
    ],
  },
  lightgbm: {
    label: "LightGBM",
    category: "梯度提升树",
    suitableData: "截面 + 时序混合特征",
    defaultHyperparams: { learning_rate: 0.05, max_depth: 6, n_estimators: 300 },
    parameterFields: [
      { key: "learning_rate", label: "学习率", defaultValue: 0.05, step: "0.005", glossaryKey: "learning_rate" },
      { key: "max_depth", label: "树深度", defaultValue: 6, step: "1", glossaryKey: "tree_depth" },
      { key: "n_estimators", label: "树数", defaultValue: 300, step: "10" },
    ],
  },
  gru: {
    label: "GRU",
    category: "时序神经网络",
    suitableData: "需要 lookback 的时序特征",
    defaultHyperparams: { lookback: 2 },
    parameterFields: [{ key: "lookback", label: "回看窗口", defaultValue: 2, step: "1" }],
  },
  lstm: {
    label: "LSTM",
    category: "时序神经网络",
    suitableData: "需要更长上下文的时序特征",
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "回看窗口", defaultValue: 3, step: "1" }],
  },
  mean_baseline: {
    label: "均值基线",
    category: "基线模型",
    suitableData: "快速 smoke / 基线对照",
    defaultHyperparams: {},
    parameterFields: [],
  },
  mlp: {
    label: "MLP",
    category: "神经网络",
    suitableData: "中等维度稠密特征",
    defaultHyperparams: {},
    parameterFields: [],
  },
  multimodal_reference: {
    label: "多模态参考模型",
    category: "多模态参考模型",
    suitableData: "市场 + 文本特征的参考组合",
    defaultHyperparams: {
      lookback: 3,
      text_feature_prefixes: "text_,sentiment_,news_",
      text_weight: 0.5,
    },
    parameterFields: [
      { key: "lookback", label: "回看窗口", defaultValue: 3, step: "1" },
      { key: "text_weight", label: "文本权重", defaultValue: 0.5, step: "0.1" },
      { key: "text_feature_prefixes", label: "文本前缀", defaultValue: "text_,sentiment_,news_", advanced: true },
    ],
  },
  patch_mixer_reference: {
    label: "补丁混合参考模型",
    category: "时序参考模型",
    suitableData: "序列 patch 化后的参考建模",
    defaultHyperparams: { lookback: 4, patch_size: 2 },
    parameterFields: [
      { key: "lookback", label: "回看窗口", defaultValue: 4, step: "1" },
      { key: "patch_size", label: "切片大小", defaultValue: 2, step: "1" },
    ],
  },
  temporal_fusion_reference: {
    label: "时序融合参考模型",
    category: "时序参考模型",
    suitableData: "多变量时序融合参考",
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "回看窗口", defaultValue: 3, step: "1" }],
  },
  transformer_reference: {
    label: "Transformer 参考模型",
    category: "时序参考模型",
    suitableData: "长上下文时序参考",
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "回看窗口", defaultValue: 3, step: "1" }],
  },
};

function normalizeModelName(modelName: string): string {
  return modelName.trim().toLowerCase();
}

export function getModelDefinition(modelName: string): ModelDefinition | null {
  return MODEL_DEFINITIONS[normalizeModelName(modelName)] ?? null;
}

export function modelLabel(modelName: string): string {
  return getModelDefinition(modelName)?.label ?? modelName;
}

export function modelCategory(modelName: string): string {
  return getModelDefinition(modelName)?.category ?? "已注册模型";
}

export function modelSuitableData(modelName: string): string {
  return getModelDefinition(modelName)?.suitableData ?? "以后端模型注册能力为准";
}

export function summarizeTemplateParameters(
  template: Pick<ModelTemplateView, "model_name" | "hyperparams"> | Pick<TemplateDraft, "model_name" | "hyperparams">,
): string {
  const definition = getModelDefinition(template.model_name);
  if (definition && definition.parameterFields.length > 0) {
    const summary = definition.parameterFields
      .slice(0, 3)
      .map((field) => `${field.label}=${template.hyperparams[field.key] ?? field.defaultValue}`);
    return summary.join(" / ");
  }
  const pairs = Object.entries(template.hyperparams).slice(0, 3);
  if (pairs.length === 0) {
    return "使用模型默认超参数";
  }
  return pairs.map(([key, value]) => `${key}=${String(value)}`).join(" / ");
}

export function buildTemplateDraft(modelName: string): TemplateDraft {
  const definition = getModelDefinition(modelName);
  return {
    name: `${modelLabel(modelName)} 模板`,
    description: "面向研究工作台的训练模板。",
    model_name: modelName,
    hyperparams: definition ? { ...definition.defaultHyperparams } : {},
    trainer_preset: "fast",
    dataset_preset: "smoke",
    read_only: false,
  };
}

export function templateDraftFromView(template: ModelTemplateView): TemplateDraft {
  const hyperparams = Object.fromEntries(
    Object.entries(template.hyperparams).filter(
      ([, value]) => typeof value === "number" || typeof value === "string",
    ),
  ) as Record<string, number | string>;
  return {
    template_id: template.template_id,
    name: template.name,
    description: template.description ?? "",
    model_name: template.model_name,
    hyperparams,
    trainer_preset: template.trainer_preset,
    dataset_preset: template.dataset_preset,
    read_only: template.read_only,
  };
}

export function templateDraftFromRun(run: ExperimentListItem): TemplateDraft {
  const draft = buildTemplateDraft(run.model_name);
  return {
    ...draft,
    name: `${modelLabel(run.model_name)} 复用模板`,
    description: `基于 ${run.run_id} 创建的训练模板。`,
    dataset_preset: run.dataset_id?.toLowerCase().includes("smoke") ? "smoke" : "real_benchmark",
  };
}
