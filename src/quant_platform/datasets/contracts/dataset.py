from __future__ import annotations

from datetime import datetime

from pydantic import Field, model_validator

from quant_platform.common.enums.core import LabelKind, SplitStrategy
from quant_platform.common.types.core import FrozenModel, TimeRange
from quant_platform.features.contracts.feature_view import FeatureViewRef


class LabelSpec(FrozenModel):
    target_column: str
    horizon: int = Field(gt=0)
    kind: LabelKind
    generated_in_layer: str = "datasets"

    @model_validator(mode="after")
    def validate_layer(self) -> "LabelSpec":
        if self.generated_in_layer != "datasets":
            raise ValueError("labels must be generated in datasets layer")
        return self


class SplitManifest(FrozenModel):
    strategy: SplitStrategy
    train_range: TimeRange
    valid_range: TimeRange
    test_range: TimeRange
    purge_gap_bars: int = 0

    @model_validator(mode="after")
    def validate_order(self) -> "SplitManifest":
        if self.train_range.end > self.valid_range.start:
            raise ValueError("train range must end before validation range starts")
        if self.valid_range.end > self.test_range.start:
            raise ValueError("validation range must end before test range starts")
        return self


class SamplePolicy(FrozenModel):
    min_history_bars: int = Field(default=1, ge=1)
    drop_missing_targets: bool = True
    universe: str = "single_asset"
    recommended_training_use: str | None = None


class DatasetRef(FrozenModel):
    dataset_id: str
    feature_view_ref: FeatureViewRef
    label_spec: LabelSpec
    split_manifest: SplitManifest
    sample_policy: SamplePolicy
    dataset_hash: str
    dataset_manifest_uri: str | None = None
    dataset_samples_uri: str | None = None
    feature_schema_hash: str | None = None
    label_schema_hash: str | None = None
    entity_scope: str = "single_asset"
    entity_count: int = 1
    readiness_status: str = "unknown"


class DatasetSample(FrozenModel):
    entity_key: str
    timestamp: datetime
    available_time: datetime
    features: dict[str, float]
    target: float

    @model_validator(mode="after")
    def validate_availability(self) -> "DatasetSample":
        if self.available_time < self.timestamp:
            raise ValueError("available_time cannot be earlier than timestamp")
        return self
