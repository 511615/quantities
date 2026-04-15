from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from quant_platform.webapi.schemas.views import BacktestTemplateView


class LaunchApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LaunchTrainRequest(LaunchApiModel):
    dataset_preset: Literal["smoke", "real_benchmark"] | None = None
    dataset_id: str | None = None
    template_id: str | None = None
    template_overrides: dict[str, Any] = Field(default_factory=dict)
    model_names: list[str] = Field(default_factory=list)
    trainer_preset: Literal["fast"] | None = None
    seed: int = 7
    experiment_name: str = "workbench-train"
    run_id_prefix: str | None = None


class LaunchBacktestRequest(LaunchApiModel):
    run_id: str
    mode: Literal["official", "custom"] = "custom"
    template_id: str | None = None
    official_window_days: Literal[30, 90, 180, 365] | None = None
    dataset_preset: Literal["smoke", "real_benchmark"] = "smoke"
    dataset_id: str | None = None
    dataset_ids: list[str] = Field(default_factory=list)
    prediction_scope: Literal["full", "test"] = "full"
    strategy_preset: Literal["sign"] = "sign"
    portfolio_preset: Literal["research_default"] = "research_default"
    cost_preset: Literal["standard"] = "standard"
    research_backend: Literal["native", "vectorbt"] = "native"
    portfolio_method: Literal["proportional", "skfolio_mean_risk"] = "proportional"
    benchmark_symbol: str = "BTCUSDT"


class LaunchBacktestPreflightRequest(LaunchApiModel):
    run_id: str
    mode: Literal["official", "custom"] = "official"
    template_id: str | None = None
    official_window_days: Literal[30, 90, 180, 365] | None = None


class LaunchModelCompositionRequest(LaunchApiModel):
    source_run_ids: list[str] = Field(default_factory=list, min_length=2)
    composition_name: str | None = None
    fusion_strategy: Literal["late_score_blend"] = "late_score_blend"
    dataset_ids: list[str] = Field(default_factory=list)
    weights: dict[str, float] = Field(default_factory=dict)


class LaunchJobResponse(LaunchApiModel):
    job_id: str
    status: Literal["queued", "running", "success", "failed"]
    job_api_path: str | None = None
    tracking_token: str | None = None
    submitted_at: datetime | None = None


class PresetOptionView(LaunchApiModel):
    value: str
    label: str
    description: str | None = None
    recommended: bool = False


class TrainLaunchOptionsView(LaunchApiModel):
    dataset_presets: list[PresetOptionView] = Field(default_factory=list)
    model_options: list[PresetOptionView] = Field(default_factory=list)
    template_options: list[PresetOptionView] = Field(default_factory=list)
    trainer_presets: list[PresetOptionView] = Field(default_factory=list)
    default_seed: int = 7
    constraints: dict[str, Any] = Field(default_factory=dict)


class BacktestLaunchOptionsView(LaunchApiModel):
    default_mode: Literal["official", "custom"] = "official"
    official_template_id: str | None = None
    official_multimodal_schema_version: str | None = None
    official_multimodal_feature_names: list[str] = Field(default_factory=list)
    template_options: list[BacktestTemplateView] = Field(default_factory=list)
    official_window_options: list[PresetOptionView] = Field(default_factory=list)
    dataset_presets: list[PresetOptionView] = Field(default_factory=list)
    prediction_scopes: list[PresetOptionView] = Field(default_factory=list)
    strategy_presets: list[PresetOptionView] = Field(default_factory=list)
    portfolio_presets: list[PresetOptionView] = Field(default_factory=list)
    cost_presets: list[PresetOptionView] = Field(default_factory=list)
    research_backends: list[PresetOptionView] = Field(default_factory=list)
    portfolio_methods: list[PresetOptionView] = Field(default_factory=list)
    default_benchmark_symbol: str = "BTCUSDT"
    default_official_window_days: int = 180
    constraints: dict[str, Any] = Field(default_factory=dict)


class BacktestLaunchPreflightView(LaunchApiModel):
    compatible: bool
    mode: Literal["official", "custom"]
    template_id: str | None = None
    official_window_days: int | None = None
    official_benchmark_version: str | None = None
    official_market_dataset_id: str | None = None
    official_multimodal_dataset_id: str | None = None
    official_dataset_ids: list[str] = Field(default_factory=list)
    required_modalities: list[str] = Field(default_factory=list)
    official_window_start_time: datetime | None = None
    official_window_end_time: datetime | None = None
    requires_text_features: bool = False
    requires_nlp_features: bool = False
    requires_auxiliary_features: bool = False
    requires_multimodal_benchmark: bool = False
    required_feature_names: list[str] = Field(default_factory=list)
    available_official_feature_names: list[str] = Field(default_factory=list)
    missing_official_feature_names: list[str] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
    nlp_gate_status: str | None = None
    nlp_gate_reasons: list[str] = Field(default_factory=list)
