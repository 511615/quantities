from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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
    dataset_preset: Literal["smoke", "real_benchmark"] = "smoke"
    dataset_id: str | None = None
    prediction_scope: Literal["full", "test"] = "full"
    strategy_preset: Literal["sign"] = "sign"
    portfolio_preset: Literal["research_default"] = "research_default"
    cost_preset: Literal["standard"] = "standard"
    benchmark_symbol: str = "BTCUSDT"


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
    dataset_presets: list[PresetOptionView] = Field(default_factory=list)
    prediction_scopes: list[PresetOptionView] = Field(default_factory=list)
    strategy_presets: list[PresetOptionView] = Field(default_factory=list)
    portfolio_presets: list[PresetOptionView] = Field(default_factory=list)
    cost_presets: list[PresetOptionView] = Field(default_factory=list)
    default_benchmark_symbol: str = "BTCUSDT"
    constraints: dict[str, Any] = Field(default_factory=dict)
