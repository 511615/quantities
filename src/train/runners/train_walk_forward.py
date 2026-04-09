"""Walk-forward training entrypoint."""

from __future__ import annotations

from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

from src.train.contracts import TrainerOutput
from src.train.runners.train_one import train_one
from src.train.splits import RollingWindowPlan


def train_walk_forward(
    trainer_factory: Any,
    config: Mapping[str, Any],
    trainer_input_factory: Any,
    windows: Sequence[RollingWindowPlan],
) -> dict[str, Any]:
    outputs: list[TrainerOutput] = []
    for window in windows:
        trainer = trainer_factory()
        trainer_input = trainer_input_factory(window)
        outputs.append(
            train_one(
                trainer=trainer,
                config=config,
                trainer_input=trainer_input,
                run_id=f"{config['experiment']['name']}::{window.window_id}",
                window_id=window.window_id,
            )
        )

    return {
        "windows": [window.to_dict() for window in windows],
        "outputs": outputs,
        "aggregate_metrics": aggregate_walk_forward_metrics(outputs),
    }


def aggregate_walk_forward_metrics(outputs: Sequence[TrainerOutput]) -> dict[str, float]:
    if not outputs:
        raise ValueError("outputs cannot be empty")
    values = [float(output.primary_metric_value) for output in outputs]
    metric_name = outputs[0].primary_metric_name
    return {
        f"wf/mean_{metric_name}": mean(values),
        f"wf/std_{metric_name}": pstdev(values) if len(values) > 1 else 0.0,
    }
