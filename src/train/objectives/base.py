"""Objective protocol and common utilities."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseObjective(ABC):
    name: str
    objective_type: str

    @abstractmethod
    def compute_loss(self, predictions: Any, targets: Any, **kwargs: Any) -> Any:
        """Return the training loss tensor or scalar."""

    @abstractmethod
    def predict(self, raw_outputs: Any) -> Any:
        """Transform model outputs into inference values."""
