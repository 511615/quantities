from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from quant_platform.common.enums.core import LabelKind, ModelFamily, SplitStrategy
from quant_platform.common.types.core import FeatureField, SchemaField, TimeRange
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.datasets.contracts.dataset import (
    DatasetRef,
    DatasetSample,
    LabelSpec,
    SamplePolicy,
    SplitManifest,
)
from quant_platform.features.contracts.feature_view import FeatureViewRef
from quant_platform.models.baselines.lstm import (
    _build_supervised_windows,
    _build_dense_windows,
    _hierarchical_windows,
)
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.contracts.training import FitRequest, TrackingContext, TrainerConfig
from quant_platform.training.runners.local import LocalTrainingRunner


def test_lstm_training_stride_reduces_supervised_examples() -> None:
    samples = _build_samples()
    feature_names = ["lag_return_1", "volume_zscore"]
    dense_windows, aligned_samples = _build_dense_windows(samples, feature_names, lookback=4)

    selected_windows, selected_samples = _build_supervised_windows(
        dense_windows,
        aligned_samples,
        forecast_horizon=1,
        stride=2,
    )

    assert len(dense_windows) == len(samples)
    assert len(selected_windows) == 5
    assert len(selected_samples) == 5
    assert selected_samples[0].timestamp == samples[0].timestamp
    assert selected_samples[1].timestamp == samples[2].timestamp


def test_lstm_subsequence_builder_splits_window_into_multiple_subsequences() -> None:
    samples = _build_samples()
    feature_names = ["lag_return_1", "volume_zscore"]
    dense_windows, _ = _build_dense_windows(samples, feature_names, lookback=6)
    hierarchical, subsequence_count = _hierarchical_windows(
        dense_windows[-1:],
        subsequence_length=3,
        subsequence_stride=3,
    )

    assert subsequence_count == 2
    assert len(hierarchical) == 1
    assert len(hierarchical[0]) == 2
    assert len(hierarchical[0][0]) == 3
    assert len(hierarchical[0][1]) == 3


def test_lstm_forecast_horizon_delays_supervised_sample_start() -> None:
    samples = _build_samples()
    feature_names = ["lag_return_1", "volume_zscore"]
    dense_windows, aligned_samples = _build_dense_windows(samples, feature_names, lookback=2)

    selected_windows, selected_samples = _build_supervised_windows(
        dense_windows,
        aligned_samples,
        forecast_horizon=4,
        stride=1,
    )

    assert selected_windows[0][-1] == [0.01, 0.0]
    assert selected_samples[0].timestamp == samples[3].timestamp


def test_lstm_window_builder_uses_explicit_feature_order() -> None:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    samples = [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base,
            available_time=base,
            features={"volume_zscore": 0.2, "lag_return_1": 0.1},
            target=0.0,
        ),
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base + timedelta(hours=1),
            available_time=base + timedelta(hours=1),
            features={"volume_zscore": 0.4, "lag_return_1": 0.3},
            target=0.0,
        ),
    ]
    windows, _ = _build_dense_windows(samples, ["lag_return_1", "volume_zscore"], lookback=2)
    assert windows[-1][-1] == [0.3, 0.4]


def test_lstm_runner_rejects_samples_with_future_available_time(tmp_path) -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    start = datetime(2024, 1, 1, tzinfo=UTC)
    dataset_ref = _build_dataset_ref()
    dataset_ref = dataset_ref.model_copy(
        update={"feature_view_ref": dataset_ref.feature_view_ref.model_copy(update={"as_of_time": start})}
    )
    samples = [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=start,
            available_time=start + timedelta(hours=1),
            features={"lag_return_1": 0.1, "volume_zscore": 0.2},
            target=0.0,
        )
    ]
    runner = LocalTrainingRunner(
        model_registry=registry,
        dataset_store={dataset_ref.dataset_id: samples},
        artifact_root=tmp_path / "artifacts",
    )
    with pytest.raises(ValueError, match="available_time exceeds feature view as_of_time"):
        runner.fit(
            FitRequest(
                run_id="lstm-future-availability",
                dataset_ref=dataset_ref,
                model_spec=ModelSpec(
                    model_name="lstm",
                    family=ModelFamily.SEQUENCE,
                    version="0.1.0",
                    input_schema=[
                        SchemaField(name="lag_return_1", dtype="float"),
                        SchemaField(name="volume_zscore", dtype="float"),
                    ],
                    output_schema=[SchemaField(name="prediction", dtype="float")],
                    hyperparams={"force_backend": "fallback"},
                ),
                trainer_config=TrainerConfig(runner="local", epochs=1, batch_size=4, deterministic=True),
                seed=7,
                tracking_context=TrackingContext(backend="file", experiment_name="lstm-compliance"),
            )
        )


def test_lstm_metadata_records_window_subsequence_and_rolling_specs(tmp_path) -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    dataset_ref = _build_dataset_ref()
    samples = _build_samples()
    runner = LocalTrainingRunner(
        model_registry=registry,
        dataset_store={dataset_ref.dataset_id: samples},
        artifact_root=tmp_path / "artifacts",
    )
    model_spec = ModelSpec(
        model_name="lstm",
        family=ModelFamily.SEQUENCE,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        hyperparams={
            "lookback": 6,
            "forecast_horizon": 1,
            "stride": 2,
            "subsequence_length": 3,
            "subsequence_stride": 3,
                "rolling_window_spec": {
                "train_size": 6,
                "valid_size": 2,
                "test_size": 2,
                "step_size": 2,
                "min_train_size": 6,
                "embargo": 0,
                "purge_gap": 0,
                "expanding_train": True,
            },
            "force_backend": "fallback",
        },
    )

    fit_result = runner.fit(
        FitRequest(
            run_id="lstm-compliance",
            dataset_ref=dataset_ref,
            model_spec=model_spec,
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=4,
                deterministic=True,
            ),
            seed=7,
            tracking_context=TrackingContext(backend="file", experiment_name="lstm-compliance"),
        )
    )

    metadata_path = tmp_path / "artifacts" / "models" / "lstm-compliance" / "metadata.json"
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    input_metadata = payload["input_metadata"]
    prediction_metadata = payload["prediction_metadata"]

    assert input_metadata["window_spec"]["lookback"] == 6
    assert input_metadata["window_spec"]["forecast_horizon"] == 1
    assert input_metadata["window_spec"]["stride"] == 2
    assert input_metadata["subsequence_spec"]["subsequence_length"] == 3
    assert input_metadata["subsequence_spec"]["subsequence_stride"] == 3
    assert input_metadata["effective_subsequence_layout"]["subsequence_count"] >= 1
    assert prediction_metadata["rolling_window_spec"]["train_size"] == 6
    assert prediction_metadata["rolling_window_spec"]["valid_size"] == 2
    assert fit_result.metrics["rolling_window_count"] >= 0.0


def _build_dataset_ref() -> DatasetRef:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    feature_view_ref = FeatureViewRef(
        feature_set_id="lstm-compliance",
        input_data_refs=[
            DataAssetRef(
                asset_id="lstm-compliance",
                schema_version=1,
                source="internal",
                symbol="BTCUSDT",
                venue="binance",
                frequency="1h",
                time_range=TimeRange(start=start, end=start + timedelta(days=1)),
                storage_uri="memory://lstm-compliance",
                content_hash="lstm-compliance",
            )
        ],
        as_of_time=start + timedelta(days=1),
        feature_schema=[
            FeatureField(name="lag_return_1", dtype="float", lineage_source="tests"),
            FeatureField(name="volume_zscore", dtype="float", lineage_source="tests"),
        ],
        build_config_hash="lstm-compliance",
        storage_uri="memory://lstm-compliance",
    )
    return DatasetRef(
        dataset_id="lstm-compliance",
        feature_view_ref=feature_view_ref,
        label_spec=LabelSpec(target_column="target", horizon=1, kind=LabelKind.REGRESSION),
        split_manifest=SplitManifest(
            strategy=SplitStrategy.TIME_SERIES,
            train_range=TimeRange(start=start, end=start + timedelta(hours=6)),
            valid_range=TimeRange(start=start + timedelta(hours=6), end=start + timedelta(hours=8)),
            test_range=TimeRange(start=start + timedelta(hours=8), end=start + timedelta(hours=12)),
        ),
        sample_policy=SamplePolicy(),
        dataset_hash="lstm-compliance",
    )


def _build_samples() -> list[DatasetSample]:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base + timedelta(hours=index),
            available_time=base + timedelta(hours=index),
            features={
                "lag_return_1": 0.01 * (index + 1),
                "volume_zscore": float(index) / 10.0,
            },
            target=0.005 * (index + 1),
        )
        for index in range(10)
    ]
