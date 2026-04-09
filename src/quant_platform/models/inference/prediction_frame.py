from __future__ import annotations

from collections.abc import Sequence

from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.training.contracts.training import (
    PredictionFrame,
    PredictionMetadata,
    PredictionRow,
)


def build_prediction_frame(
    samples: Sequence[DatasetSample],
    predictions: Sequence[float],
    model_run_id: str,
    confidences: Sequence[float] | None = None,
) -> PredictionFrame:
    if len(samples) != len(predictions):
        raise ValueError("samples and predictions must have the same length")
    confidence_values = confidences if confidences is not None else [0.5] * len(predictions)
    if len(confidence_values) != len(predictions):
        raise ValueError("confidences and predictions must have the same length")
    rows = [
        PredictionRow(
            entity_keys={"instrument": sample.entity_key},
            timestamp=_prediction_timestamp(sample),
            prediction=float(prediction),
            confidence=max(0.0, min(1.0, float(confidence))),
            model_run_id=model_run_id,
            feature_available_time=sample.available_time,
        )
        for sample, prediction, confidence in zip(
            samples,
            predictions,
            confidence_values,
            strict=True,
        )
    ]
    return PredictionFrame(
        rows=rows,
        metadata=PredictionMetadata(
            prediction_time=_prediction_timestamp(samples[-1]) if samples else None,
            inference_latency_ms=0,
            target_horizon=1,
        ),
    )


def _prediction_timestamp(sample: DatasetSample) -> datetime:
    return max(sample.timestamp, sample.available_time)
