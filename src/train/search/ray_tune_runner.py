"""Thin Ray Tune integration stubs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from src.train.contracts import SearchResult, TrainerOutput

try:
    from ray import tune
except ImportError:  # pragma: no cover - optional dependency
    tune = None


class SearchSpaceBuilder:
    def build(self, search_config: Mapping[str, Any]) -> Mapping[str, Any]:
        return search_config.get("space", {})


class TuneReporter:
    def report(self, metrics: Mapping[str, float], checkpoint_path: str | None = None) -> None:
        if tune is not None:
            payload = dict(metrics)
            if checkpoint_path:
                payload["checkpoint_path"] = checkpoint_path
            tune.report(payload)


@dataclass(slots=True)
class TuneTrialRunner:
    train_fn: Callable[[Mapping[str, Any]], TrainerOutput]

    def __call__(self, trial_config: Mapping[str, Any]) -> TrainerOutput:
        return self.train_fn(trial_config)


class SearchResultAssembler:
    def assemble(
        self, trial_outputs: Sequence[TrainerOutput], primary_metric_name: str
    ) -> SearchResult:
        if not trial_outputs:
            raise ValueError("trial_outputs cannot be empty")
        best_output = max(trial_outputs, key=lambda output: output.primary_metric_value)
        return SearchResult(
            best_run_id=best_output.run_id,
            best_metric_name=primary_metric_name,
            best_metric_value=best_output.primary_metric_value,
            candidate_run_ids=[output.run_id for output in trial_outputs],
        )
