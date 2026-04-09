"""Ranking objective implementations."""

from __future__ import annotations

from typing import Any

from .base import BaseObjective

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None


class RankingObjective(BaseObjective):
    name = "ranking"
    objective_type = "ranking"

    def compute_loss(self, predictions: Any, targets: Any, **kwargs: Any) -> Any:
        if torch is None:
            raise RuntimeError("torch is required for RankingObjective")
        if "group_sizes" not in kwargs:
            raise ValueError("RankingObjective requires group_sizes for listwise training")
        reshaped_targets = targets.float().reshape_as(predictions)
        return torch.mean((predictions - reshaped_targets) ** 2)

    def predict(self, raw_outputs: Any) -> Any:
        return raw_outputs
