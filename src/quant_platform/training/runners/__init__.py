"""Training runners."""

from quant_platform.training.runners.local import LocalTrainingRunner
from quant_platform.training.runners.prediction import PredictionRunner

__all__ = ["LocalTrainingRunner", "PredictionRunner"]
