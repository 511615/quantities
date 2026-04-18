from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from quant_platform.backtest.adapters.prediction_adapter import PredictionToSignalAdapter
from quant_platform.backtest.contracts.backtest import LatencyConfig, StrategyConfig
from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.models.inference.prediction_frame import build_prediction_frame
from quant_platform.training.contracts.training import (
    PredictionFrame,
    PredictionMetadata,
    PredictionRow,
)


def test_prediction_frame_adapts_to_signal_frame_with_latency() -> None:
    prediction_frame = PredictionFrame(
        rows=[
            PredictionRow(
                entity_keys={"instrument": "BTCUSDT", "venue": "binance"},
                timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                prediction=0.12,
                confidence=0.8,
                model_run_id="run-1",
                feature_available_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            )
        ],
        metadata=PredictionMetadata(inference_latency_ms=1500, target_horizon=1),
    )
    signal_frame = PredictionToSignalAdapter().adapt(
        prediction_frame=prediction_frame,
        strategy_config=StrategyConfig(name="rank", direction_mode="long_short"),
        latency_config=LatencyConfig(signal_delay_seconds=2),
    )
    row = signal_frame.rows[0]
    assert row.signal_time == prediction_frame.rows[0].timestamp
    assert row.available_time == prediction_frame.rows[0].feature_available_time
    assert row.tradable_from == prediction_frame.rows[0].timestamp + timedelta(
        milliseconds=1500, seconds=2
    )
    assert row.direction_mode == "long_short"


def test_prediction_row_rejects_future_feature_visibility() -> None:
    with pytest.raises(ValueError, match="prediction timestamp cannot be earlier"):
        PredictionRow(
            entity_keys={"instrument": "BTCUSDT"},
            timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            prediction=0.1,
            confidence=0.5,
            model_run_id="run-1",
            feature_available_time=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
            + timedelta(seconds=1),
        )


def test_build_prediction_frame_uses_feature_available_time_when_later() -> None:
    sample = DatasetSample(
        entity_key="BTCUSDT",
        timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        available_time=datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        features={"feature": 1.0},
        target=0.1,
    )

    frame = build_prediction_frame([sample], [0.2], model_run_id="run-1")

    assert frame.rows[0].timestamp == sample.available_time
    assert frame.rows[0].feature_available_time == sample.available_time
    assert frame.metadata is not None
    assert frame.metadata.prediction_time == sample.available_time


def test_prediction_adapter_calibrates_extreme_prediction_scales() -> None:
    prediction_frame = PredictionFrame(
        rows=[
            PredictionRow(
                entity_keys={"instrument": "BTCUSDT", "venue": "binance"},
                timestamp=datetime(2024, 1, 1, hour, 0, tzinfo=timezone.utc),
                prediction=value,
                confidence=1.0,
                model_run_id="run-scale",
                feature_available_time=datetime(2024, 1, 1, hour, 0, tzinfo=timezone.utc),
            )
            for hour, value in enumerate([2.5e9, 0.0, -2.5e9], start=0)
        ],
        metadata=PredictionMetadata(inference_latency_ms=0, target_horizon=1),
    )

    signal_frame = PredictionToSignalAdapter().adapt(
        prediction_frame=prediction_frame,
        strategy_config=StrategyConfig(name="rank", direction_mode="long_short"),
        latency_config=LatencyConfig(signal_delay_seconds=0),
    )

    normalized_values = [row.normalized_value for row in signal_frame.rows]
    assert normalized_values[0] is not None and normalized_values[0] > 0.0
    assert normalized_values[1] is not None and abs(normalized_values[1]) < 1e-9
    assert normalized_values[2] is not None and normalized_values[2] < 0.0
    assert all(value is not None and abs(value) <= 1.0 for value in normalized_values)
