"""Classification objective implementations."""

from __future__ import annotations

from typing import Any

from .base import BaseObjective

try:
    import torch
    import torch.nn.functional as F
except ImportError:  # pragma: no cover - optional dependency
    torch = None
    F = None


class BinaryClassificationObjective(BaseObjective):
    name = "binary_classification"
    objective_type = "classification"

    def compute_loss(self, predictions: Any, targets: Any, **kwargs: Any) -> Any:
        if F is None:
            raise RuntimeError("torch is required for BinaryClassificationObjective")
        return F.binary_cross_entropy_with_logits(predictions, targets.float())

    def predict(self, raw_outputs: Any) -> Any:
        if torch is None:
            raise RuntimeError("torch is required for BinaryClassificationObjective")
        return torch.sigmoid(raw_outputs)
