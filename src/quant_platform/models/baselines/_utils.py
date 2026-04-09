from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from math import exp

import numpy as np

from quant_platform.datasets.contracts.dataset import DatasetSample


def resolve_feature_names(
    samples: Sequence[DatasetSample],
    requested_names: Sequence[str],
) -> list[str]:
    if requested_names:
        return list(requested_names)
    if not samples:
        return []
    return sorted(samples[0].features.keys())


def tabular_xy(
    samples: Sequence[DatasetSample],
    feature_names: Sequence[str],
) -> tuple[np.ndarray, np.ndarray]:
    x = np.array(
        [[float(sample.features.get(name, 0.0)) for name in feature_names] for sample in samples],
        dtype=float,
    )
    y = np.array([float(sample.target) for sample in samples], dtype=float)
    return x, y


def tabular_x(samples: Sequence[DatasetSample], feature_names: Sequence[str]) -> np.ndarray:
    x, _ = tabular_xy(samples, feature_names)
    return x


def sequence_windows(
    samples: Sequence[DatasetSample],
    feature_names: Sequence[str],
    lookback: int,
) -> tuple[np.ndarray, np.ndarray, list[DatasetSample]]:
    grouped: dict[str, list[DatasetSample]] = defaultdict(list)
    for sample in samples:
        grouped[sample.entity_key].append(sample)
    windows: list[np.ndarray] = []
    targets: list[float] = []
    aligned_samples: list[DatasetSample] = []
    for entity_samples in grouped.values():
        ordered = sorted(entity_samples, key=lambda item: item.timestamp)
        for end_idx in range(lookback - 1, len(ordered)):
            window = ordered[end_idx - lookback + 1 : end_idx + 1]
            windows.append(
                np.array(
                    [
                        [float(sample.features.get(name, 0.0)) for name in feature_names]
                        for sample in window
                    ],
                    dtype=float,
                )
            )
            targets.append(float(ordered[end_idx].target))
            aligned_samples.append(ordered[end_idx])
    if not windows:
        return np.zeros((0, lookback, len(feature_names))), np.zeros((0,)), []
    return np.stack(windows), np.array(targets, dtype=float), aligned_samples


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    return {"mae": mae, "rmse": rmse, "sample_count": float(len(y_true))}


def confidence_from_predictions(predictions: Sequence[float], residual_scale: float) -> list[float]:
    scale = max(float(residual_scale), 1e-6)
    return [float(1.0 - exp(-abs(float(prediction)) / scale)) for prediction in predictions]
