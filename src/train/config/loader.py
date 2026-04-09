"""Config composition helpers for Hydra-friendly YAML layouts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Mapping

from .schema import ExperimentConfig


def compose_experiment_config(*parts: Mapping[str, Any]) -> ExperimentConfig:
    merged: Dict[str, Any] = {}
    for part in parts:
        merged = deep_merge(merged, dict(part))

    required = [
        "experiment",
        "data",
        "dataset",
        "model",
        "objective",
        "trainer",
        "eval",
        "tracking",
        "search",
        "runtime",
    ]
    missing = [key for key in required if key not in merged]
    if missing:
        raise ValueError(f"Missing required config sections: {', '.join(missing)}")

    extras = {key: value for key, value in merged.items() if key not in required}
    return ExperimentConfig(**{key: merged[key] for key in required}, extras=extras)


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(dict(base))
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged
