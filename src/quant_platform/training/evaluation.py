from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from math import sqrt
from statistics import mean
from typing import Any

from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.training.contracts.training import PredictionFrame

MAX_TIMESERIES_POINTS = 240
MAX_SCATTER_POINTS = 220
HISTOGRAM_BINS = 18


def build_regression_evaluation_summary(
    *,
    run_id: str,
    dataset_ref: DatasetRef,
    scope_payloads: Mapping[str, tuple[list[DatasetSample], PredictionFrame]],
    feature_importance: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    sample_count = len(scope_payloads.get("full", ([], PredictionFrame(rows=[])))[0]) or sum(
        len(samples) for samples, _frame in scope_payloads.values()
    )
    split_sample_counts = {
        scope: len(samples) for scope, (samples, _frame) in scope_payloads.items()
    }
    split_metrics = {
        scope: _regression_metrics(samples, frame)
        for scope, (samples, frame) in scope_payloads.items()
        if samples
    }
    selected_scope = _preferred_scope(split_metrics)
    selected_samples, selected_frame = scope_payloads.get(selected_scope, ([], PredictionFrame(rows=[])))
    selected_metrics = dict(split_metrics.get(selected_scope, {}))
    valid_metrics = split_metrics.get("valid", {})
    if "mae" in valid_metrics and "valid_mae" not in selected_metrics:
        selected_metrics["valid_mae"] = float(valid_metrics["mae"])
    if "sample_count" not in selected_metrics:
        selected_metrics["sample_count"] = float(len(selected_samples))
    coverage = _coverage(scope_payloads)
    return {
        "run_id": run_id,
        "dataset_id": dataset_ref.dataset_id,
        "task_type": str(dataset_ref.label_spec.kind.value).lower(),
        "selected_scope": selected_scope,
        "sample_count": sample_count,
        "split_sample_counts": split_sample_counts,
        "split_metrics": split_metrics,
        "regression_metrics": selected_metrics,
        "coverage": coverage,
        "feature_importance_summary": _feature_importance_summary(feature_importance or {}),
        "series": _series_payload(selected_samples, selected_frame),
    }


def _preferred_scope(split_metrics: Mapping[str, dict[str, float]]) -> str:
    for scope in ("test", "valid", "full", "train"):
        if scope in split_metrics:
            return scope
    return "full"


def _regression_metrics(
    samples: Sequence[DatasetSample],
    frame: PredictionFrame,
) -> dict[str, float]:
    targets = [float(sample.target) for sample in samples]
    predictions = [float(row.prediction) for row in frame.rows]
    if not targets or not predictions:
        return {}
    paired = list(zip(predictions, targets, strict=False))
    if not paired:
        return {}
    residuals = [prediction - target for prediction, target in paired]
    abs_errors = [abs(value) for value in residuals]
    squared_errors = [value * value for value in residuals]
    target_mean = mean(targets)
    prediction_mean = mean(predictions)
    total_variance = sum((target - target_mean) ** 2 for target in targets)
    stable_targets = [target for target in targets if abs(target) > 1e-9]
    sign_hits = [
        1.0 if (prediction >= 0 and target >= 0) or (prediction < 0 and target < 0) else 0.0
        for prediction, target in paired
    ]
    metrics = {
        "mae": mean(abs_errors),
        "rmse": sqrt(mean(squared_errors)),
        "r2": 0.0 if total_variance <= 1e-12 else 1.0 - (sum(squared_errors) / total_variance),
        "mean_prediction": prediction_mean,
        "mean_target": target_mean,
        "bias": mean(residuals),
        "sign_hit_rate": mean(sign_hits),
        "sample_count": float(len(paired)),
    }
    if stable_targets:
        mape_terms = [
            abs(prediction - target) / abs(target)
            for prediction, target in paired
            if abs(target) > 1e-9
        ]
        if mape_terms:
            metrics["mape"] = mean(mape_terms)
        smape_terms = [
            (2.0 * abs(prediction - target)) / (abs(prediction) + abs(target))
            for prediction, target in paired
            if abs(prediction) + abs(target) > 1e-9
        ]
        if smape_terms:
            metrics["smape"] = mean(smape_terms)
    return {key: float(value) for key, value in metrics.items()}


def _coverage(
    scope_payloads: Mapping[str, tuple[list[DatasetSample], PredictionFrame]],
) -> dict[str, Any]:
    split_time_ranges: dict[str, dict[str, str | None]] = {}
    all_samples: list[DatasetSample] = []
    available_scopes: list[str] = []
    for scope, (samples, _frame) in scope_payloads.items():
        if not samples:
            continue
        available_scopes.append(scope)
        all_samples.extend(samples)
        split_time_ranges[scope] = {
            "start_time": _isoformat(samples[0].timestamp),
            "end_time": _isoformat(samples[-1].timestamp),
        }
    if not all_samples:
        return {
            "start_time": None,
            "end_time": None,
            "missing_samples": 0,
            "available_scopes": [],
            "split_time_ranges": {},
        }
    ordered = sorted(all_samples, key=lambda sample: sample.timestamp)
    return {
        "start_time": _isoformat(ordered[0].timestamp),
        "end_time": _isoformat(ordered[-1].timestamp),
        "missing_samples": 0,
        "available_scopes": available_scopes,
        "split_time_ranges": split_time_ranges,
    }


def _feature_importance_summary(
    values: Mapping[str, float],
) -> list[dict[str, float | str]]:
    ranked = sorted(
        ((name, float(score)) for name, score in values.items()),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    return [
        {"name": name, "value": score}
        for name, score in ranked[:12]
    ]


def _series_payload(
    samples: Sequence[DatasetSample],
    frame: PredictionFrame,
) -> dict[str, list[dict[str, Any]]]:
    paired = list(zip(samples, frame.rows, strict=False))
    if not paired:
        return {
            "prediction_vs_target_timeseries": [],
            "residual_timeseries": [],
            "prediction_vs_target_scatter": [],
            "residual_histogram": [],
        }
    ordered = sorted(paired, key=lambda item: item[0].timestamp)
    sampled_timeseries = _downsample(ordered, MAX_TIMESERIES_POINTS)
    residuals = [float(row.prediction) - float(sample.target) for sample, row in ordered]
    sampled_scatter = _downsample(ordered, MAX_SCATTER_POINTS)
    return {
        "prediction_vs_target_timeseries": [
            {
                "timestamp": _isoformat(sample.timestamp),
                "prediction": float(row.prediction),
                "target": float(sample.target),
            }
            for sample, row in sampled_timeseries
        ],
        "residual_timeseries": [
            {
                "timestamp": _isoformat(sample.timestamp),
                "residual": float(row.prediction) - float(sample.target),
            }
            for sample, row in sampled_timeseries
        ],
        "prediction_vs_target_scatter": [
            {
                "prediction": float(row.prediction),
                "target": float(sample.target),
                "timestamp": _isoformat(sample.timestamp),
            }
            for sample, row in sampled_scatter
        ],
        "residual_histogram": _histogram(residuals, HISTOGRAM_BINS),
    }


def _downsample(values: Sequence[Any], limit: int) -> list[Any]:
    if len(values) <= limit:
        return list(values)
    step = max(1, len(values) // limit)
    sampled = list(values[::step])
    if sampled[-1] != values[-1]:
        sampled[-1] = values[-1]
    return sampled[:limit]


def _histogram(values: Sequence[float], bins: int) -> list[dict[str, float | int | str]]:
    if not values:
        return []
    minimum = min(values)
    maximum = max(values)
    if abs(maximum - minimum) <= 1e-12:
        return [{"label": f"{minimum:.4f}", "count": len(values), "center": float(minimum)}]
    width = (maximum - minimum) / max(1, bins)
    counts = [0 for _ in range(bins)]
    for value in values:
        index = min(int((value - minimum) / width), bins - 1)
        counts[index] += 1
    rows: list[dict[str, float | int | str]] = []
    for index, count in enumerate(counts):
        start = minimum + (index * width)
        end = start + width
        rows.append(
            {
                "label": f"{start:.4f} to {end:.4f}",
                "count": count,
                "center": float((start + end) / 2.0),
            }
        )
    return rows


def _isoformat(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
