"""Minimal native PyTorch trainer scaffold."""

from __future__ import annotations

from typing import Any

from src.train.contracts import EvalResult, TrainerInput, TrainerOutput

from .base import BaseTrainer

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None


class TorchTrainer(BaseTrainer):
    def fit(self, trainer_input: TrainerInput) -> TrainerOutput:
        if self.context is None:
            raise RuntimeError("TorchTrainer.setup must be called before fit")
        if torch is None:
            raise RuntimeError("torch is required to use TorchTrainer")
        if trainer_input.objective is None:
            raise ValueError("trainer_input.objective is required")

        train_metrics = {"loss": 0.0}
        valid_metrics = {"loss": 0.0}
        if trainer_input.metrics:
            for metric_fn in trainer_input.metrics:
                valid_metrics.update(metric_fn([], []))

        output = TrainerOutput(
            run_id=self.context.run_id,
            primary_metric_name=self.config.get("primary_metric", "loss"),
            primary_metric_value=valid_metrics.get(self.config.get("primary_metric", "loss"), 0.0),
            best_step=0,
            best_epoch=0,
            metrics_by_split={"train": train_metrics, "valid": valid_metrics},
            metadata={
                "objective_name": getattr(trainer_input.objective, "name", "unknown"),
                "device": self.context.device,
            },
        )
        return self.save_artifacts(output)

    def evaluate(self, split_name: str, dataloader: Any) -> EvalResult:
        return EvalResult(split_name=split_name, metrics={"loss": 0.0})

    def predict(self, split_name: str, dataloader: Any) -> Any:
        return {"split": split_name, "predictions": []}
