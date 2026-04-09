from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from quant_platform.datasets.contracts.dataset import DatasetRef
from quant_platform.features.contracts.feature_view import FeatureViewRef
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.common.types.core import FrozenModel


class TrackingContext(FrozenModel):
    backend: str
    experiment_name: str
    tracking_uri: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class TrainerConfig(FrozenModel):
    runner: str
    epochs: int = Field(ge=1)
    batch_size: int = Field(ge=1)
    deterministic: bool = True


class FitRequest(FrozenModel):
    run_id: str
    dataset_ref: DatasetRef
    model_spec: ModelSpec
    trainer_config: TrainerConfig
    seed: int
    tracking_context: TrackingContext


class FitResult(FrozenModel):
    run_id: str
    model_artifact_uri: str
    model_name: str
    metrics: dict[str, float]
    feature_importance_uri: str | None = None
    train_manifest_uri: str
    repro_digest: str


class PredictionScope(FrozenModel):
    scope_name: Literal["train", "valid", "test", "full"]
    as_of_time: datetime


class PredictRequest(FrozenModel):
    model_artifact_uri: str
    dataset_ref: DatasetRef | None = None
    feature_view_ref: FeatureViewRef | None = None
    prediction_scope: PredictionScope

    @model_validator(mode="after")
    def validate_input_source(self) -> "PredictRequest":
        if not self.dataset_ref and not self.feature_view_ref:
            raise ValueError("either dataset_ref or feature_view_ref must be provided")
        return self


class PredictionRow(FrozenModel):
    entity_keys: dict[str, str]
    timestamp: datetime
    prediction: float
    confidence: float = Field(ge=0.0, le=1.0)
    model_run_id: str
    feature_available_time: datetime | None = None

    @model_validator(mode="after")
    def validate_temporal_order(self) -> "PredictionRow":
        if self.feature_available_time and self.timestamp < self.feature_available_time:
            raise ValueError("prediction timestamp cannot be earlier than feature_available_time")
        return self


class PredictionMetadata(FrozenModel):
    feature_view_ref: FeatureViewRef | None = None
    prediction_time: datetime | None = None
    inference_latency_ms: int = Field(default=0, ge=0)
    target_horizon: int | None = Field(default=None, gt=0)


class PredictionFrame(FrozenModel):
    rows: list[PredictionRow]
    metadata: PredictionMetadata | None = None

    @property
    def sample_count(self) -> int:
        return len(self.rows)
