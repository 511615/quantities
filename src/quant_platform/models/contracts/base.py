from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.models.contracts.io import ModelArtifactMeta
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.contracts.runtime import (
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)

if TYPE_CHECKING:
    from quant_platform.training.contracts.training import PredictionFrame


class BaseModelPlugin(ABC):
    def __init__(self, spec: ModelSpec) -> None:
        self.spec = spec

    @abstractmethod
    def fit(
        self,
        train_input: TrainInputBundle | list[DatasetSample],
        valid_input: PredictInputBundle | None = None,
    ) -> dict[str, float]:
        """Train a model on either raw samples or an adapted input bundle."""

    @abstractmethod
    def predict(
        self,
        predict_input: PredictInputBundle | list[DatasetSample],
        model_run_id: str | None = None,
    ) -> ModelPredictionOutputs | PredictionFrame:
        """Return raw outputs for adapted bundles or a prediction frame for compatibility."""

    @abstractmethod
    def save(self, artifact_dir: Path) -> ModelArtifactMeta:
        """Persist model state and metadata under artifact_dir."""

    @classmethod
    @abstractmethod
    def load(cls, spec: ModelSpec, artifact_dir: Path) -> BaseModelPlugin:
        """Restore a model instance from artifact_dir."""

    def feature_importance(self) -> dict[str, float] | None:
        return None

    def explainability_payload(self) -> dict[str, object] | None:
        return None
