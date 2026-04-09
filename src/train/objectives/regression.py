"""Regression objective implementations."""

from __future__ import annotations

from typing import Any

from .base import BaseObjective

try:
    import torch.nn.functional as F
except ImportError:  # pragma: no cover - optional dependency
    F = None


class RegressionObjective(BaseObjective):
    name = "regression"
    objective_type = "regression"

    def compute_loss(self, predictions: Any, targets: Any, **kwargs: Any) -> Any:
        if F is None:
            raise RuntimeError("torch is required for RegressionObjective")
        return F.mse_loss(predictions, targets.float())

    def predict(self, raw_outputs: Any) -> Any:
        return raw_outputs
