"""Trainer abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping, Optional

from src.train.contracts import (
    EvalResult,
    TrainerContext,
    TrainerInput,
    TrainerOutput,
    TrackingClient,
)


class BaseTrainer(ABC):
    def __init__(self, tracking_client: Optional[TrackingClient] = None) -> None:
        self.tracking_client = tracking_client
        self.context: Optional[TrainerContext] = None
        self.config: Mapping[str, Any] = {}

    def setup(self, context: TrainerContext, config: Mapping[str, Any]) -> None:
        self.context = context
        self.config = config
        Path(context.output_dir).mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def fit(self, trainer_input: TrainerInput) -> TrainerOutput:
        """Train one window/fold and return unified output."""

    @abstractmethod
    def evaluate(self, split_name: str, dataloader: Any) -> EvalResult:
        """Evaluate one split."""

    @abstractmethod
    def predict(self, split_name: str, dataloader: Any) -> Any:
        """Return predictions for one split."""

    def save_artifacts(self, output: TrainerOutput) -> TrainerOutput:
        return output

    def log_to_tracker(self, output: TrainerOutput) -> None:
        if self.tracking_client is None:
            return
        self.tracking_client.log_metrics(
            {
                f"{split}/{name}": value
                for split, metrics in output.metrics_by_split.items()
                for name, value in metrics.items()
            }
        )
