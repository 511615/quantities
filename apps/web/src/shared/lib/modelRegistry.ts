import type { ExperimentListItem, ModelTemplateView } from "../api/types";

import { getCurrentLocale, type Locale } from "./i18n";

export type ParameterField = {
  key: string;
  label: string;
  glossaryKey?: "learning_rate" | "tree_depth" | "regularization";
  defaultValue: number | string;
  step?: string;
  advanced?: boolean;
};

type LocalizedText = Record<Locale, string>;

type ModelDefinition = {
  label: LocalizedText;
  category: LocalizedText;
  suitableData: LocalizedText;
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

function pick(text: LocalizedText): string {
  return text[getCurrentLocale()];
}

const MODEL_DEFINITIONS: Record<string, ModelDefinition> = {
  elastic_net: {
    label: { "zh-CN": "弹性网络", "en-US": "Elastic Net" },
    category: { "zh-CN": "线性模型", "en-US": "Linear Model" },
    suitableData: { "zh-CN": "中低维连续特征", "en-US": "Low-to-mid dimensional continuous features" },
    defaultHyperparams: { alpha: 0.001, l1_ratio: 0.5 },
    parameterFields: [
      { key: "alpha", label: "alpha", defaultValue: 0.001, step: "0.0001", glossaryKey: "regularization" },
      { key: "l1_ratio", label: "L1 ratio", defaultValue: 0.5, step: "0.01", glossaryKey: "regularization" },
    ],
  },
  lightgbm: {
    label: { "zh-CN": "LightGBM", "en-US": "LightGBM" },
    category: { "zh-CN": "梯度提升树", "en-US": "Gradient Boosting Tree" },
    suitableData: { "zh-CN": "截面 + 时序混合特征", "en-US": "Cross-sectional and sequential mixed features" },
    defaultHyperparams: { learning_rate: 0.05, max_depth: 6, n_estimators: 300 },
    parameterFields: [
      { key: "learning_rate", label: "learning_rate", defaultValue: 0.05, step: "0.005", glossaryKey: "learning_rate" },
      { key: "max_depth", label: "max_depth", defaultValue: 6, step: "1", glossaryKey: "tree_depth" },
      { key: "n_estimators", label: "n_estimators", defaultValue: 300, step: "10" },
    ],
  },
  gru: {
    label: { "zh-CN": "GRU", "en-US": "GRU" },
    category: { "zh-CN": "时序神经网络", "en-US": "Sequence Neural Network" },
    suitableData: { "zh-CN": "需要 lookback 的时序特征", "en-US": "Sequential features with lookback context" },
    defaultHyperparams: { lookback: 2 },
    parameterFields: [{ key: "lookback", label: "lookback", defaultValue: 2, step: "1" }],
  },
  lstm: {
    label: { "zh-CN": "LSTM", "en-US": "LSTM" },
    category: { "zh-CN": "时序神经网络", "en-US": "Sequence Neural Network" },
    suitableData: { "zh-CN": "需要更长上下文的时序特征", "en-US": "Sequential features that need longer context" },
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "lookback", defaultValue: 3, step: "1" }],
  },
  mean_baseline: {
    label: { "zh-CN": "均值基线", "en-US": "Mean Baseline" },
    category: { "zh-CN": "基线模型", "en-US": "Baseline Model" },
    suitableData: { "zh-CN": "快速 smoke / 基线对照", "en-US": "Fast smoke checks and baseline comparisons" },
    defaultHyperparams: {},
    parameterFields: [],
  },
  mlp: {
    label: { "zh-CN": "MLP", "en-US": "MLP" },
    category: { "zh-CN": "神经网络", "en-US": "Neural Network" },
    suitableData: { "zh-CN": "中等维度稠密特征", "en-US": "Medium-dimensional dense features" },
    defaultHyperparams: {},
    parameterFields: [],
  },
  multimodal_reference: {
    label: { "zh-CN": "多模态参考模型", "en-US": "Multimodal Reference Model" },
    category: { "zh-CN": "多模态参考模型", "en-US": "Multimodal Reference Model" },
    suitableData: { "zh-CN": "市场 + 文本特征的参考组合", "en-US": "Reference setup for market plus text features" },
    defaultHyperparams: {
      lookback: 3,
      text_feature_prefixes: "text_,sentiment_,news_",
      text_weight: 0.5,
    },
    parameterFields: [
      { key: "lookback", label: "lookback", defaultValue: 3, step: "1" },
      { key: "text_weight", label: "text_weight", defaultValue: 0.5, step: "0.1" },
      { key: "text_feature_prefixes", label: "text_feature_prefixes", defaultValue: "text_,sentiment_,news_", advanced: true },
    ],
  },
  patch_mixer_reference: {
    label: { "zh-CN": "Patch Mixer 参考模型", "en-US": "Patch Mixer Reference Model" },
    category: { "zh-CN": "时序参考模型", "en-US": "Sequence Reference Model" },
    suitableData: { "zh-CN": "序列 patch 化后的参考建模", "en-US": "Reference modeling for patchified sequences" },
    defaultHyperparams: { lookback: 4, patch_size: 2 },
    parameterFields: [
      { key: "lookback", label: "lookback", defaultValue: 4, step: "1" },
      { key: "patch_size", label: "patch_size", defaultValue: 2, step: "1" },
    ],
  },
  temporal_fusion_reference: {
    label: { "zh-CN": "时序融合参考模型", "en-US": "Temporal Fusion Reference Model" },
    category: { "zh-CN": "时序参考模型", "en-US": "Sequence Reference Model" },
    suitableData: { "zh-CN": "多变量时序融合参考", "en-US": "Reference setup for multivariate temporal fusion" },
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "lookback", defaultValue: 3, step: "1" }],
  },
  transformer_reference: {
    label: { "zh-CN": "Transformer 参考模型", "en-US": "Transformer Reference Model" },
    category: { "zh-CN": "时序参考模型", "en-US": "Sequence Reference Model" },
    suitableData: { "zh-CN": "长上下文时序参考", "en-US": "Long-context sequence reference" },
    defaultHyperparams: { lookback: 3 },
    parameterFields: [{ key: "lookback", label: "lookback", defaultValue: 3, step: "1" }],
  },
};

function normalizeModelName(modelName: string): string {
  return modelName.trim().toLowerCase();
}

export function getModelDefinition(modelName: string): ModelDefinition | null {
  return MODEL_DEFINITIONS[normalizeModelName(modelName)] ?? null;
}

export function modelLabel(modelName: string): string {
  return getModelDefinition(modelName) ? pick(getModelDefinition(modelName)!.label) : modelName;
}

export function modelCategory(modelName: string): string {
  const definition = getModelDefinition(modelName);
  return definition ? pick(definition.category) : getCurrentLocale() === "zh-CN" ? "已注册模型" : "Registered Model";
}

export function modelSuitableData(modelName: string): string {
  const definition = getModelDefinition(modelName);
  return definition ? pick(definition.suitableData) : getCurrentLocale() === "zh-CN" ? "以后端模型注册能力为准" : "See backend model registration";
}

export function summarizeTemplateParameters(
  template: Pick<ModelTemplateView, "model_name" | "hyperparams"> | Pick<TemplateDraft, "model_name" | "hyperparams">,
): string {
  const definition = getModelDefinition(template.model_name);
  if (definition && definition.parameterFields.length > 0) {
    return definition.parameterFields
      .slice(0, 3)
      .map((field) => `${field.label}=${template.hyperparams[field.key] ?? field.defaultValue}`)
      .join(" / ");
  }
  const pairs = Object.entries(template.hyperparams).slice(0, 3);
  if (pairs.length === 0) {
    return getCurrentLocale() === "zh-CN" ? "使用模型默认超参数" : "Using model defaults";
  }
  return pairs.map(([key, value]) => `${key}=${String(value)}`).join(" / ");
}

export function buildTemplateDraft(modelName: string): TemplateDraft {
  const definition = getModelDefinition(modelName);
  return {
    name: `${modelLabel(modelName)} ${getCurrentLocale() === "zh-CN" ? "模板" : "Template"}`,
    description:
      getCurrentLocale() === "zh-CN"
        ? "面向研究工作台的训练模板。"
        : "Training template for the research workbench.",
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
    name: `${modelLabel(run.model_name)} ${getCurrentLocale() === "zh-CN" ? "复用模板" : "Reuse Template"}`,
    description:
      getCurrentLocale() === "zh-CN"
        ? `基于 ${run.run_id} 创建的训练模板。`
        : `Training template created from ${run.run_id}.`,
    dataset_preset: run.dataset_id?.toLowerCase().includes("smoke") ? "smoke" : "real_benchmark",
  };
}
