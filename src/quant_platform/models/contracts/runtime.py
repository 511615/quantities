from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.contracts.model_spec import ModelSpec


@dataclass(frozen=True)
class TrainInputBundle:
    dataset_ref: DatasetRef
    model_spec: ModelSpec
    source_samples: list[DatasetSample]
    feature_names: list[str]
    targets: list[float]
    blocks: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PredictInputBundle:
    dataset_ref: DatasetRef
    model_spec: ModelSpec
    source_samples: list[DatasetSample]
    feature_names: list[str]
    blocks: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelPredictionOutputs:
    predictions: list[float]
    confidences: list[float] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
