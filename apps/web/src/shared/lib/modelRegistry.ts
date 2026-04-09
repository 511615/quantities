import type { ExperimentListItem } from "../api/types";

export type AlgorithmKey =
  | "elastic_net"
  | "lightgbm"
  | "xgboost"
  | "decision_tree"
  | "random_forest"
  | "mlp"
  | "gru"
  | "transformer";

export type ParameterField = {
  key: string;
  label: string;
  glossaryKey?: "epochs" | "learning_rate" | "tree_depth" | "regularization" | "batch_size";
  defaultValue: number | string;
  step?: string;
  advanced?: boolean;
};

export type AlgorithmDefinition = {
  key: AlgorithmKey;
  label: string;
  category: string;
  suitableData: string;
  defaultDataset: string;
  defaultTargetColumn: string;
  commonDefaults: {
    seed: number;
    batchSize: number;
    epochs: number;
    validationStrategy: string;
  };
  parameterFields: ParameterField[];
};

export type ModelTemplate = {
  id: string;
  name: string;
  algorithm: AlgorithmKey;
  datasetId: string;
  targetColumn: string;
  trainingNote: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
  commonParams: {
    seed: number;
    batchSize: number;
    epochs: number;
    validationStrategy: string;
  };
  algorithmParams: Record<string, number | string>;
};

export type TrainedModelMeta = {
  runId: string;
  displayName: string;
  note: string;
  hidden?: boolean;
};

export const MODEL_TEMPLATES_STORAGE_KEY = "quant-workbench:model-templates";
export const TRAINED_MODELS_STORAGE_KEY = "quant-workbench:trained-models";

export const ALGORITHM_DEFINITIONS: AlgorithmDefinition[] = [
  {
    key: "elastic_net",
    label: "Elastic Net",
    category: "\u7ebf\u6027\u6a21\u578b",
    suitableData: "\u4e2d\u4f4e\u7ef4\u5ea6\u8fde\u7eed\u578b\u7279\u5f81",
    defaultDataset: "smoke_dataset",
    defaultTargetColumn: "forward_return_1h",
    commonDefaults: { seed: 7, batchSize: 64, epochs: 40, validationStrategy: "\u65f6\u95f4\u5207\u5206" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.03, step: "0.001", advanced: true, glossaryKey: "learning_rate" },
      { key: "l1", label: "L1", defaultValue: 0.15, step: "0.01", glossaryKey: "regularization" },
      { key: "l2", label: "L2", defaultValue: 0.2, step: "0.01", glossaryKey: "regularization" },
    ],
  },
  {
    key: "lightgbm",
    label: "LightGBM",
    category: "\u68af\u5ea6\u63d0\u5347\u6811",
    suitableData: "\u622a\u9762 + \u65f6\u5e8f\u6df7\u5408\u7279\u5f81",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_4h",
    commonDefaults: { seed: 7, batchSize: 256, epochs: 160, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.05, step: "0.005", glossaryKey: "learning_rate" },
      { key: "max_depth", label: "\u6811\u6df1\u5ea6", defaultValue: 6, step: "1", glossaryKey: "tree_depth" },
      { key: "n_estimators", label: "\u6811\u6570", defaultValue: 300, step: "10" },
      { key: "subsample", label: "Subsample", defaultValue: 0.85, step: "0.01", advanced: true },
      { key: "l2", label: "L2", defaultValue: 0.15, step: "0.01", advanced: true, glossaryKey: "regularization" },
    ],
  },
  {
    key: "xgboost",
    label: "XGBoost",
    category: "\u68af\u5ea6\u63d0\u5347\u6811",
    suitableData: "\u5f3a\u975e\u7ebf\u6027\u7279\u5f81\u7ec4\u5408",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_4h",
    commonDefaults: { seed: 11, batchSize: 256, epochs: 180, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.04, step: "0.005", glossaryKey: "learning_rate" },
      { key: "max_depth", label: "\u6811\u6df1\u5ea6", defaultValue: 5, step: "1", glossaryKey: "tree_depth" },
      { key: "n_estimators", label: "\u6811\u6570", defaultValue: 260, step: "10" },
      { key: "subsample", label: "Subsample", defaultValue: 0.82, step: "0.01", advanced: true },
      { key: "reg_lambda", label: "Lambda", defaultValue: 0.3, step: "0.01", advanced: true, glossaryKey: "regularization" },
    ],
  },
  {
    key: "decision_tree",
    label: "Decision Tree",
    category: "\u5355\u6811\u6a21\u578b",
    suitableData: "\u7279\u5f81\u89c4\u5219\u8f83\u6e05\u6670\u7684\u5feb\u901f\u57fa\u51c6",
    defaultDataset: "smoke_dataset",
    defaultTargetColumn: "forward_return_1h",
    commonDefaults: { seed: 5, batchSize: 512, epochs: 1, validationStrategy: "\u65f6\u95f4\u5207\u5206" },
    parameterFields: [
      { key: "max_depth", label: "\u6811\u6df1\u5ea6", defaultValue: 4, step: "1", glossaryKey: "tree_depth" },
      { key: "min_samples_leaf", label: "\u53f6\u5b50\u8282\u70b9\u6700\u5c0f\u6837\u672c", defaultValue: 16, step: "1", advanced: true },
    ],
  },
  {
    key: "random_forest",
    label: "Random Forest",
    category: "\u96c6\u6210\u6811\u6a21\u578b",
    suitableData: "\u5bf9\u566a\u58f0\u76f8\u5bf9\u7a33\u5065\u7684\u622a\u9762\u7279\u5f81",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_4h",
    commonDefaults: { seed: 13, batchSize: 256, epochs: 1, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "max_depth", label: "\u6811\u6df1\u5ea6", defaultValue: 8, step: "1", glossaryKey: "tree_depth" },
      { key: "n_estimators", label: "\u6811\u6570", defaultValue: 240, step: "10" },
      { key: "max_features", label: "\u7279\u5f81\u91c7\u6837\u6bd4\u4f8b", defaultValue: 0.7, step: "0.01", advanced: true },
    ],
  },
  {
    key: "mlp",
    label: "MLP",
    category: "\u795e\u7ecf\u7f51\u7edc",
    suitableData: "\u4e2d\u7b49\u7ef4\u5ea6\u7a20\u5bc6\u7279\u5f81",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_4h",
    commonDefaults: { seed: 19, batchSize: 128, epochs: 60, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.001, step: "0.0001", glossaryKey: "learning_rate" },
      { key: "hidden_size", label: "\u9690\u85cf\u7ef4\u5ea6", defaultValue: 128, step: "8" },
      { key: "dropout", label: "Dropout", defaultValue: 0.2, step: "0.01", advanced: true },
      { key: "l2", label: "L2", defaultValue: 0.0001, step: "0.0001", advanced: true, glossaryKey: "regularization" },
    ],
  },
  {
    key: "gru",
    label: "GRU",
    category: "\u65f6\u5e8f\u795e\u7ecf\u7f51\u7edc",
    suitableData: "\u9700\u8981\u65f6\u5e8f\u4f9d\u8d56\u7684\u5e8f\u5217\u7279\u5f81",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_4h",
    commonDefaults: { seed: 23, batchSize: 96, epochs: 70, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.0008, step: "0.0001", glossaryKey: "learning_rate" },
      { key: "hidden_size", label: "\u9690\u85cf\u7ef4\u5ea6", defaultValue: 96, step: "8" },
      { key: "dropout", label: "Dropout", defaultValue: 0.15, step: "0.01", advanced: true },
      { key: "num_layers", label: "\u5c42\u6570", defaultValue: 2, step: "1", advanced: true },
    ],
  },
  {
    key: "transformer",
    label: "Transformer",
    category: "\u65f6\u5e8f\u53d8\u6362\u5668",
    suitableData: "\u591a\u56e0\u5b50\u3001\u957f\u65f6\u95f4\u8303\u56f4\u7279\u5f81",
    defaultDataset: "real_benchmark",
    defaultTargetColumn: "forward_return_12h",
    commonDefaults: { seed: 29, batchSize: 64, epochs: 90, validationStrategy: "\u6eda\u52a8\u65f6\u95f4\u7a97" },
    parameterFields: [
      { key: "learning_rate", label: "\u5b66\u4e60\u7387", defaultValue: 0.0005, step: "0.0001", glossaryKey: "learning_rate" },
      { key: "hidden_size", label: "\u9690\u85cf\u7ef4\u5ea6", defaultValue: 160, step: "8" },
      { key: "dropout", label: "Dropout", defaultValue: 0.1, step: "0.01", advanced: true },
      { key: "num_heads", label: "\u6ce8\u610f\u529b\u5934\u6570", defaultValue: 4, step: "1", advanced: true },
    ],
  },
];

export function getAlgorithmDefinition(algorithm: AlgorithmKey): AlgorithmDefinition {
  return (
    ALGORITHM_DEFINITIONS.find((item) => item.key === algorithm) ?? ALGORITHM_DEFINITIONS[0]
  );
}

export function buildTemplate(algorithm: AlgorithmKey, seedOffset = 0): ModelTemplate {
  const definition = getAlgorithmDefinition(algorithm);
  const now = new Date().toISOString();

  return {
    id: `${algorithm}-${now}`,
    name: `${definition.label} \u6a21\u677f`,
    algorithm,
    datasetId: definition.defaultDataset,
    targetColumn: definition.defaultTargetColumn,
    trainingNote: "\u9762\u5411\u7814\u7a76\u5de5\u4f5c\u53f0\u7684\u63a7\u5236\u542f\u52a8\u914d\u7f6e\u3002",
    enabled: true,
    createdAt: now,
    updatedAt: now,
    commonParams: {
      seed: definition.commonDefaults.seed + seedOffset,
      batchSize: definition.commonDefaults.batchSize,
      epochs: definition.commonDefaults.epochs,
      validationStrategy: definition.commonDefaults.validationStrategy,
    },
    algorithmParams: Object.fromEntries(
      definition.parameterFields.map((field) => [field.key, field.defaultValue]),
    ),
  };
}

export function defaultTemplates(): ModelTemplate[] {
  return ["elastic_net", "lightgbm", "xgboost", "mlp"].map((key, index) =>
    buildTemplate(key as AlgorithmKey, index),
  );
}

export function summarizeTemplateParameters(template: ModelTemplate): string {
  const definition = getAlgorithmDefinition(template.algorithm);
  const firstFields = definition.parameterFields
    .slice(0, 3)
    .map((field) => `${field.label}=${template.algorithmParams[field.key] ?? field.defaultValue}`);
  return firstFields.join(" / ");
}

export function summarizeRunMetrics(run: ExperimentListItem): string {
  const primary = run.primary_metric_name && run.primary_metric_value !== null
    ? `${run.primary_metric_name.toUpperCase()}=${run.primary_metric_value.toFixed(4)}`
    : "MAE=--";
  return `${primary} / backtests=${run.backtest_count}`;
}

export function deriveTemplateFromRun(run: ExperimentListItem): ModelTemplate {
  const normalized = normalizeAlgorithm(run.model_name);
  const template = buildTemplate(normalized);
  return {
    ...template,
    id: `${run.run_id}-template`,
    name: `${algorithmLabel(run.model_name)} \u590d\u7528\u6a21\u677f`,
    datasetId: run.dataset_id ?? template.datasetId,
    targetColumn: template.targetColumn,
    trainingNote: `\u57fa\u4e8e ${run.run_id} \u590d\u5236\u751f\u6210\u3002`,
  };
}

export function algorithmLabel(modelName: string): string {
  return getAlgorithmDefinition(normalizeAlgorithm(modelName)).label;
}

export function algorithmCategory(modelName: string): string {
  return getAlgorithmDefinition(normalizeAlgorithm(modelName)).category;
}

export function algorithmSuitableData(modelName: string): string {
  return getAlgorithmDefinition(normalizeAlgorithm(modelName)).suitableData;
}

export function normalizeAlgorithm(modelName: string): AlgorithmKey {
  const normalized = modelName.toLowerCase();
  if (normalized.includes("lightgbm")) {
    return "lightgbm";
  }
  if (normalized.includes("xgboost")) {
    return "xgboost";
  }
  if (normalized.includes("decision")) {
    return "decision_tree";
  }
  if (normalized.includes("random")) {
    return "random_forest";
  }
  if (normalized.includes("mlp")) {
    return "mlp";
  }
  if (normalized.includes("gru")) {
    return "gru";
  }
  if (normalized.includes("transformer")) {
    return "transformer";
  }
  return "elastic_net";
}
