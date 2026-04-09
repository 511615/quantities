"""Lightweight metric helpers."""

from __future__ import annotations

from statistics import mean
from typing import Iterable, Mapping


def prefix_metrics(metrics: Mapping[str, float], prefix: str) -> dict[str, float]:
    return {f"{prefix}{name}": value for name, value in metrics.items()}


def metric_mean(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        raise ValueError("metric_mean requires at least one value")
    return float(mean(values))
