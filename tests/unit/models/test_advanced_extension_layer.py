from __future__ import annotations

import json
import pickle
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
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.contracts.registration import AdvancedModelKind, ModelRegistration
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.contracts.training import (
    FitRequest,
    PredictionScope,
    PredictRequest,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.training.runners import LocalTrainingRunner, PredictionRunner
from tests.fixtures.dataset_samples import build_dataset_samples


def test_registry_rejects_config_entrypoint_missing_from_code() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    broken = ModelRegistration(
        model_name="broken",
        family=ModelFamily.DEEP,
        advanced_kind=AdvancedModelKind.TRANSFORMER,
        entrypoint="missing.Model",
        input_adapter_key="sequence_market",
        prediction_adapter_key="standard_prediction",
        artifact_adapter_key="json_manifest",
    )
    with pytest.raises(ValueError, match="entrypoint"):
        registry.register_model(broken)


def test_registry_rejects_alias_conflicts() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    registry.register_model(
        ModelRegistration(
            model_name="alpha",
            family=ModelFamily.DEEP,
            advanced_kind=AdvancedModelKind.TRANSFORMER,
            entrypoint=(
                "quant_platform.models.advanced.transformer_sequence."
                "TransformerSequenceModel"
            ),
            input_adapter_key="sequence_market",
            prediction_adapter_key="standard_prediction",
            artifact_adapter_key="json_manifest",
            aliases=["shared"],
        )
    )
    with pytest.raises(ValueError, match="alias"):
        registry.register_model(
            ModelRegistration(
                model_name="beta",
                family=ModelFamily.DEEP,
                advanced_kind=AdvancedModelKind.PATCH_MIXER,
                entrypoint=(
                    "quant_platform.models.advanced.patch_mixer.PatchMixerModel"
                ),
                input_adapter_key="patch_sequence",
                prediction_adapter_key="standard_prediction",
                artifact_adapter_key="json_manifest",
                aliases=["shared"],
            )
        )


def test_multimodal_model_supports_missing_text_modality(tmp_path) -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    samples = build_dataset_samples()
    dataset_ref = _build_dataset_ref()
    runner = LocalTrainingRunner(
        model_registry=registry,
        dataset_store={dataset_ref.dataset_id: samples},
        artifact_root=tmp_path / "artifacts",
    )
    model_spec = ModelSpec(
        model_name="multimodal_reference",
        family=ModelFamily.DEEP,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        hyperparams={"lookback": 3},
    )
    fit_result = runner.fit(
        FitRequest(
            run_id="multimodal-missing-text",
            dataset_ref=dataset_ref,
            model_spec=model_spec,
            trainer_config=TrainerConfig(
                runner="local", epochs=1, batch_size=8, deterministic=True
            ),
            seed=7,
            tracking_context=TrackingContext(backend="file", experiment_name="advanced-tests"),
        )
    )
    prediction_runner = PredictionRunner(
        model_registry=registry,
        dataset_store={dataset_ref.dataset_id: samples},
        artifact_root=tmp_path / "artifacts",
    )
    frame = prediction_runner.predict(
        PredictRequest(
            model_artifact_uri=fit_result.model_artifact_uri,
            dataset_ref=dataset_ref,
            prediction_scope=PredictionScope(
                scope_name="full", as_of_time=dataset_ref.feature_view_ref.as_of_time
            ),
        )
    )
    assert frame.sample_count == len(samples)
    metadata_payload = json.loads(
        (tmp_path / "artifacts" / "models" / "multimodal-missing-text" / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata_payload["prediction_metadata"]["fusion_strategy"] == "late_score_blend"
    state_payload = (
        tmp_path / "artifacts" / "models" / "multimodal-missing-text" / "state.pkl"
    ).read_bytes()
    estimator = pickle.loads(state_payload)["estimator"]
    assert estimator["market_branch"] is not None
    assert estimator["blender_branch"] is not None


def test_multimodal_aligned_text_allows_market_available_time_when_text_features_exist() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    runtime = registry.resolve_runtime("multimodal_reference")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    samples = [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base,
            available_time=base + timedelta(hours=1),
            features={"lag_return_1": 0.1, "sentiment_score": 0.2},
            target=0.0,
        )
    ]
    dataset_ref = _build_dataset_ref(
        as_of_time=base + timedelta(hours=2),
        feature_names=["lag_return_1", "sentiment_score"],
    )
    bundle = runtime.input_adapter.build_train_input(
        samples,
        dataset_ref,
        ModelSpec(
            model_name="multimodal_reference",
            family=ModelFamily.DEEP,
            version="0.1.0",
            input_schema=[
                SchemaField(name="lag_return_1", dtype="float"),
                SchemaField(name="sentiment_score", dtype="float"),
            ],
            output_schema=[SchemaField(name="prediction", dtype="float")],
            hyperparams={"lookback": 2, "text_feature_prefixes": ["sentiment_"]},
        ),
        runtime.registration,
    )
    registry.capability_validator.validate(runtime.registration, dataset_ref, bundle)


def test_multimodal_aligned_text_treats_zero_value_text_features_as_present() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    runtime = registry.resolve_runtime("multimodal_reference")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    samples = [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base,
            available_time=base,
            features={"lag_return_1": 0.1, "sentiment_score": 0.0},
            target=0.0,
        )
    ]
    dataset_ref = _build_dataset_ref(
        as_of_time=base + timedelta(hours=1),
        feature_names=["lag_return_1", "sentiment_score"],
    )
    bundle = runtime.input_adapter.build_predict_input(
        samples,
        dataset_ref,
        ModelSpec(
            model_name="multimodal_reference",
            family=ModelFamily.DEEP,
            version="0.1.0",
            input_schema=[
                SchemaField(name="lag_return_1", dtype="float"),
                SchemaField(name="sentiment_score", dtype="float"),
            ],
            output_schema=[SchemaField(name="prediction", dtype="float")],
            hyperparams={"lookback": 2, "text_feature_prefixes": ["sentiment_"]},
        ),
        runtime.registration,
    )
    assert bundle.blocks["text_feature_names"] == ["sentiment_score"]
    assert bundle.blocks["text_mask"] == [True]


def test_multimodal_aligned_text_uses_dataset_schema_when_first_sample_is_sparse() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    runtime = registry.resolve_runtime("multimodal_reference")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    samples = [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base,
            available_time=base,
            features={"lag_return_1": 0.1},
            target=0.0,
        ),
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base + timedelta(hours=1),
            available_time=base + timedelta(hours=1),
            features={"lag_return_1": 0.2, "sentiment_score": 0.3},
            target=0.0,
        ),
    ]
    dataset_ref = _build_dataset_ref(
        as_of_time=base + timedelta(hours=2),
        feature_names=["lag_return_1", "sentiment_score"],
    )
    bundle = runtime.input_adapter.build_predict_input(
        samples,
        dataset_ref,
        ModelSpec(
            model_name="multimodal_reference",
            family=ModelFamily.DEEP,
            version="0.1.0",
            input_schema=[
                SchemaField(name="lag_return_1", dtype="float"),
                SchemaField(name="sentiment_score", dtype="float"),
            ],
            output_schema=[SchemaField(name="prediction", dtype="float")],
            hyperparams={"lookback": 2, "text_feature_prefixes": ["sentiment_"]},
        ),
        runtime.registration,
    )
    assert bundle.feature_names == ["lag_return_1", "sentiment_score"]
    assert bundle.blocks["text_feature_names"] == ["sentiment_score"]
    assert bundle.blocks["text_mask"] == [False, True]


def _build_dataset_ref(
    as_of_time: datetime | None = None,
    feature_names: list[str] | None = None,
) -> DatasetRef:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    as_of = as_of_time or (start + timedelta(days=5))
    schema_names = feature_names or ["lag_return_1"]
    feature_view_ref = FeatureViewRef(
        feature_set_id="advanced-tests",
        input_data_refs=[
            DataAssetRef(
                asset_id="advanced-tests",
                schema_version=1,
                source="internal",
                symbol="BTCUSDT",
                venue="binance",
                frequency="1h",
                time_range=TimeRange(start=start, end=start + timedelta(days=1)),
                storage_uri="memory://advanced-tests",
                content_hash="advanced-tests",
            )
        ],
        as_of_time=as_of,
        feature_schema=[
            FeatureField(name=name, dtype="float", lineage_source="tests")
            for name in schema_names
        ],
        build_config_hash="advanced-tests",
        storage_uri="memory://advanced-tests",
    )
    return DatasetRef(
        dataset_id="advanced-tests",
        feature_view_ref=feature_view_ref,
        label_spec=LabelSpec(target_column="target", horizon=1, kind=LabelKind.REGRESSION),
        split_manifest=SplitManifest(
            strategy=SplitStrategy.TIME_SERIES,
            train_range=TimeRange(start=start, end=start + timedelta(hours=1)),
            valid_range=TimeRange(start=start + timedelta(hours=1), end=start + timedelta(hours=2)),
            test_range=TimeRange(start=start + timedelta(hours=2), end=start + timedelta(days=1)),
        ),
        sample_policy=SamplePolicy(),
        dataset_hash="advanced-tests",
    )
