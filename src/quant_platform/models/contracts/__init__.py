"""Model contracts."""

from quant_platform.models.contracts.base import BaseModelPlugin
from quant_platform.models.contracts.io import ModelArtifactMeta
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.contracts.registration import AdvancedModelKind, ModelRegistration
from quant_platform.models.contracts.runtime import (
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)

__all__ = [
    "AdvancedModelKind",
    "BaseModelPlugin",
    "ModelArtifactMeta",
    "ModelPredictionOutputs",
    "ModelRegistration",
    "ModelSpec",
    "PredictInputBundle",
    "TrainInputBundle",
]
