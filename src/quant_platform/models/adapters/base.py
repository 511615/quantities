from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.contracts import (
    BaseModelPlugin,
    ModelArtifactMeta,
    ModelPredictionOutputs,
    ModelRegistration,
    ModelSpec,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.training.contracts.training import PredictionFrame, PredictionScope

if TYPE_CHECKING:
    from quant_platform.common.io.files import LocalArtifactStore


class ModelInputAdapter(ABC):
    @abstractmethod
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        """Adapt dataset samples into a model-ready training bundle."""

    @abstractmethod
    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        """Adapt dataset samples into a model-ready prediction bundle."""


class PredictionAdapter(ABC):
    @abstractmethod
    def build_prediction_frame(
        self,
        raw_output: ModelPredictionOutputs | PredictionFrame,
        predict_input: PredictInputBundle,
        *,
        model_run_id: str,
        prediction_scope: PredictionScope | None = None,
    ) -> PredictionFrame:
        """Normalize model outputs into a backtest-compatible prediction frame."""


class ArtifactAdapter(ABC):
    @abstractmethod
    def save_model(
        self,
        plugin: BaseModelPlugin,
        *,
        run_id: str,
        artifact_root: Path,
        registration: ModelRegistration,
        train_input: TrainInputBundle,
    ) -> ModelArtifactMeta:
        """Persist model artifacts under artifact_root for a single run."""

    @abstractmethod
    def load_model(
        self,
        model_cls: type[BaseModelPlugin],
        *,
        spec: ModelSpec,
        artifact_meta: ModelArtifactMeta,
        artifact_store: LocalArtifactStore,
    ) -> BaseModelPlugin:
        """Restore a plugin from an artifact manifest."""


class CapabilityValidator(ABC):
    @abstractmethod
    def validate(
        self,
        registration: ModelRegistration,
        dataset_ref: DatasetRef,
        bundle: TrainInputBundle | PredictInputBundle,
    ) -> None:
        """Fail fast when a registration is incompatible with its input contract."""
