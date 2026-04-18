from __future__ import annotations

from pathlib import Path

from pydantic import Field

from quant_platform.common.types.core import FrozenModel
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.contracts.registration import AdvancedModelKind, ModelRegistration


class ModelArtifactMeta(FrozenModel):
    run_id: str
    model_name: str
    model_family: str
    advanced_kind: AdvancedModelKind
    model_spec: ModelSpec
    registration: ModelRegistration
    artifact_uri: str
    artifact_dir: str
    state_uri: str
    backend: str
    training_sample_count: int
    feature_names: list[str]
    training_config: dict[str, object] = Field(default_factory=dict)
    training_metrics: dict[str, float] = Field(default_factory=dict)
    best_epoch: int | None = None
    trained_steps: int | None = None
    checkpoint_tag: str | None = None
    input_metadata: dict[str, object] = Field(default_factory=dict)
    prediction_metadata: dict[str, object] = Field(default_factory=dict)
    feature_scope_modality: str | None = None
    feature_scope_feature_names: list[str] = Field(default_factory=list)
    source_dataset_quality_status: str | None = None

    @property
    def artifact_path(self) -> Path:
        return Path(self.artifact_uri)

    @property
    def artifact_directory(self) -> Path:
        return Path(self.artifact_dir)
