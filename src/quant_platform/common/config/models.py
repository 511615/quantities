from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from quant_platform.common.enums.core import ModelFamily
from quant_platform.models.contracts.registration import AdvancedModelKind


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ProjectConfig(ConfigModel):
    name: str
    version: str
    artifact_root: str
    manifest_root: str


class EnvironmentConfig(ConfigModel):
    name: str
    tracking_backend: str
    tracking_uri: str
    artifact_root: str
    strict_mode: bool = True


class DataSchemaConfig(ConfigModel):
    required_columns: list[str]
    timestamp_timezone: str


class DataFeatureStoreConfig(ConfigModel):
    default_feature_set: str
    enforce_available_time: bool
    lineage_required: bool


class DataPoliciesConfig(ConfigModel):
    allowed_splitters: list[str]
    forbid_random_split: bool
    default_label_horizon: int


class DataConfig(ConfigModel):
    sources: dict[str, Any]
    schema_: DataSchemaConfig = Field(alias="schema")
    feature_store: DataFeatureStoreConfig
    dataset_policies: DataPoliciesConfig


class ModelRegistryEntryConfig(ConfigModel):
    family: ModelFamily
    advanced_kind: AdvancedModelKind = AdvancedModelKind.BASELINE
    entrypoint: str
    input_adapter_key: str = "tabular"
    prediction_adapter_key: str = "prediction_frame"
    artifact_adapter_key: str = "native_pickle"
    capabilities: list[str] = Field(default_factory=list)
    default_hyperparams: dict[str, Any] = Field(default_factory=dict)
    config_schema_version: str = "1"
    aliases: list[str] = Field(default_factory=list)
    benchmark_eligible: bool = True
    default_eligible: bool = True
    enabled: bool = True


class ModelConfig(ConfigModel):
    default_model: str
    models: dict[str, ModelRegistryEntryConfig] = Field(alias="families")


class TrainConfig(ConfigModel):
    seed: int
    epochs: int
    batch_size: int
    runner: str
    deterministic: bool
    metrics: dict[str, list[str]]
    callbacks: dict[str, bool]
    hpo: dict[str, Any]


class BacktestConfig(ConfigModel):
    engine: str
    position_mode: str
    allow_short: bool
    initial_cash: float
    event_driven: dict[str, Any]
    costs: dict[str, float]
    portfolio_rules: dict[str, float]


class ExperimentConfig(ConfigModel):
    tracking: dict[str, Any]
    lineage: dict[str, Any]
    reproducibility: dict[str, Any]


class AgentConfig(ConfigModel):
    enabled: bool
    task_types: list[str]
    guardrails: dict[str, Any]
    tools: dict[str, list[str]]


class AppConfig(ConfigModel):
    project: ProjectConfig
    env: EnvironmentConfig
    data: DataConfig
    model: ModelConfig
    train: TrainConfig
    backtest: BacktestConfig
    experiment: ExperimentConfig
    agent: AgentConfig
