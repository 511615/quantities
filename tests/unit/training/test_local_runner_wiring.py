from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.enums.core import ModelFamily
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.experiment.manifests.run_manifest import RunManifest
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.adapters.defaults import StandardPredictionAdapter
from quant_platform.models.contracts.registration import AdvancedModelKind, ModelRegistration
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.contracts.training import FitRequest, TrackingContext, TrainerConfig
from quant_platform.training.runners.local import LocalTrainingRunner
from tests.fixtures.model_specs import build_model_spec


def test_local_runner_executes_expected_wiring_order(
    artifact_root,
    built_dataset,
    monkeypatch,
) -> None:
    dataset_ref, samples = built_dataset
    dataset_store = {dataset_ref.dataset_id: samples}
    model_registry = MagicMock()
    plugin = MagicMock()
    plugin.fit.return_value = {"mae": 0.1, "sample_count": float(len(samples))}
    plugin.feature_importance.return_value = {}
    plugin.predict.side_effect = lambda bundle: SimpleNamespace(
        predictions=[0.1 for _ in bundle.source_samples],
        confidences=[0.5 for _ in bundle.source_samples],
    )
    artifact_path = artifact_root / "models" / "runner-order" / "metadata.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("{}", encoding="utf-8")
    model_cls = MagicMock(return_value=plugin)
    runtime = SimpleNamespace(
        registration=ModelRegistration(
            model_name="mean_baseline",
            family=ModelFamily.BASELINE,
            advanced_kind=AdvancedModelKind.BASELINE,
            entrypoint="quant_platform.models.baselines.mean_baseline.MeanBaselineModel",
            input_adapter_key="tabular_passthrough",
            prediction_adapter_key="standard_prediction",
            artifact_adapter_key="json_manifest",
        ),
        model_cls=model_cls,
        input_adapter=SimpleNamespace(
            build_train_input=MagicMock(return_value=samples),
            build_predict_input=MagicMock(
                side_effect=lambda scoped_samples, dataset_ref, spec, registration: SimpleNamespace(
                    source_samples=scoped_samples,
                    dataset_ref=dataset_ref,
                    model_spec=spec,
                )
            ),
        ),
        prediction_adapter=StandardPredictionAdapter(),
        artifact_adapter=SimpleNamespace(
            save_model=MagicMock(return_value=SimpleNamespace(artifact_uri=str(artifact_path)))
        ),
    )
    model_registry.resolve_runtime.return_value = runtime
    model_registry.capability_validator = SimpleNamespace(validate=MagicMock())
    runner = LocalTrainingRunner(
        model_registry=model_registry,
        dataset_store=dataset_store,
        artifact_root=artifact_root,
    )
    event_log: list[str] = []

    def track_seed(seed: int) -> None:
        event_log.append(f"seed:{seed}")

    def track_validate(loaded_samples, as_of_time) -> None:
        assert loaded_samples == samples
        event_log.append("validate")

    monkeypatch.setattr("quant_platform.training.runners.local.apply_seed", track_seed)
    monkeypatch.setattr(
        "quant_platform.training.runners.local.DatasetBuilder.validate_samples",
        track_validate,
    )

    request = FitRequest(
        run_id="runner-order",
        dataset_ref=dataset_ref,
        model_spec=build_model_spec(),
        trainer_config=TrainerConfig(runner="local", epochs=1, batch_size=8, deterministic=True),
        seed=11,
        tracking_context=TrackingContext(backend="file", experiment_name="tests"),
    )

    result = runner.fit(request)

    assert event_log == ["seed:11", "validate"]
    model_registry.resolve_runtime.assert_called_once_with(request.model_spec.model_name)
    model_cls.assert_called_once()
    plugin.fit.assert_called_once()
    runtime.artifact_adapter.save_model.assert_called_once()
    assert Path(result.model_artifact_uri).exists()
    assert Path(result.train_manifest_uri).exists()
    assert (artifact_root / "models" / "runner-order" / "evaluation_summary.json").exists()
    assert (artifact_root / "predictions" / "runner-order" / "full.json").exists()
    assert (artifact_root / "tracking" / "runner-order.json").exists()


def test_local_runner_rejects_missing_dataset(artifact_root) -> None:
    runner = LocalTrainingRunner(
        model_registry=MagicMock(),
        dataset_store={},
        artifact_root=artifact_root,
    )
    with pytest.raises(KeyError, match="is not available"):
        runner._load_dataset(MagicMock(dataset_id="missing"))


def test_local_runner_persists_dataset_readiness_context_in_manifest(
    artifact_root,
    built_dataset,
) -> None:
    dataset_ref, samples = built_dataset
    manifest_artifact = LocalArtifactStore(artifact_root).write_model(
        "datasets/dataset_fixture_dataset_manifest.json",
        DatasetBuildManifest(
            dataset_id=dataset_ref.dataset_id,
            asset_id="market_ohlcv_btcusdt_1h",
            feature_set_id=dataset_ref.feature_view_ref.feature_set_id,
            label_horizon=dataset_ref.label_spec.horizon,
            sample_count=len(samples),
            dropped_rows=0,
            split_strategy=dataset_ref.split_manifest.strategy.value,
            snapshot_version="snapshot-v1",
            entity_scope="single_asset",
            entity_count=1,
            usable_sample_count=len(samples),
            raw_row_count=len(samples),
            feature_schema_hash="feature-hash-v1",
            readiness_status="warning",
            freshness_status="warning",
            quality_status="warning",
            acquisition_profile={
                "dataset_type": "fusion_training_panel",
                "data_domain": "market",
                "source_dataset_ids": ["smoke_dataset"],
                "fusion_domains": ["market", "macro", "on_chain"],
            },
        ),
    )
    dataset_ref = dataset_ref.model_copy(
        update={
            "dataset_manifest_uri": manifest_artifact.uri,
            "readiness_status": "warning",
            "feature_schema_hash": "feature-hash-v1",
        }
    )
    dataset_store = {dataset_ref.dataset_id: samples}
    model_registry = MagicMock()
    plugin = MagicMock()
    plugin.fit.return_value = {"mae": 0.1}
    plugin.feature_importance.return_value = {}
    plugin.predict.side_effect = lambda bundle: SimpleNamespace(
        predictions=[0.1 for _ in bundle.source_samples],
        confidences=[0.5 for _ in bundle.source_samples],
    )
    artifact_path = artifact_root / "models" / "runner-warning" / "metadata.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("{}", encoding="utf-8")
    model_registry.resolve_runtime.return_value = SimpleNamespace(
        registration=ModelRegistration(
            model_name="mean_baseline",
            family=ModelFamily.BASELINE,
            advanced_kind=AdvancedModelKind.BASELINE,
            entrypoint="quant_platform.models.baselines.mean_baseline.MeanBaselineModel",
            input_adapter_key="tabular_passthrough",
            prediction_adapter_key="standard_prediction",
            artifact_adapter_key="json_manifest",
        ),
        model_cls=MagicMock(return_value=plugin),
        input_adapter=SimpleNamespace(
            build_train_input=MagicMock(return_value=samples),
            build_predict_input=MagicMock(
                side_effect=lambda scoped_samples, dataset_ref, spec, registration: SimpleNamespace(
                    source_samples=scoped_samples,
                    dataset_ref=dataset_ref,
                    model_spec=spec,
                )
            ),
        ),
        prediction_adapter=StandardPredictionAdapter(),
        artifact_adapter=SimpleNamespace(
            save_model=MagicMock(return_value=SimpleNamespace(artifact_uri=str(artifact_path)))
        ),
    )
    model_registry.capability_validator = SimpleNamespace(validate=MagicMock())
    runner = LocalTrainingRunner(
        model_registry=model_registry,
        dataset_store=dataset_store,
        artifact_root=artifact_root,
    )

    result = runner.fit(
        FitRequest(
            run_id="runner-warning",
            dataset_ref=dataset_ref,
            model_spec=build_model_spec(),
            trainer_config=TrainerConfig(runner="local", epochs=1, batch_size=8, deterministic=True),
            seed=11,
            tracking_context=TrackingContext(backend="file", experiment_name="tests"),
        )
    )

    train_manifest = LocalArtifactStore(artifact_root).read_model(
        result.train_manifest_uri,
        RunManifest,
    )
    evaluation_summary = LocalArtifactStore(artifact_root).read_json(
        str(artifact_root / "models" / "runner-warning" / "evaluation_summary.json")
    )
    assert train_manifest.dataset_manifest_uri == manifest_artifact.uri
    assert train_manifest.snapshot_version == "snapshot-v1"
    assert train_manifest.dataset_type == "fusion_training_panel"
    assert train_manifest.data_domain == "market"
    assert train_manifest.feature_schema_hash == "feature-hash-v1"
    assert train_manifest.dataset_readiness_status == "warning"
    assert train_manifest.source_dataset_ids == ["smoke_dataset"]
    assert train_manifest.fusion_domains == ["market", "macro", "on_chain"]
    assert "dataset_readiness_warning" in train_manifest.dataset_readiness_warnings
    assert "dataset_freshness:warning" in train_manifest.dataset_readiness_warnings
    assert evaluation_summary["task_type"] == "regression"
    assert evaluation_summary["selected_scope"] in {"test", "valid", "full", "train"}
    assert "regression_metrics" in evaluation_summary


def test_local_runner_records_lstm_rolling_window_evaluation(
    artifact_root,
    built_dataset,
) -> None:
    dataset_ref, samples = built_dataset
    registry = ModelRegistry()
    register_default_models(registry)
    runner = LocalTrainingRunner(
        model_registry=registry,
        dataset_store={dataset_ref.dataset_id: samples},
        artifact_root=artifact_root,
    )

    result = runner.fit(
        FitRequest(
            run_id="lstm-rolling-runner",
            dataset_ref=dataset_ref,
            model_spec=ModelSpec(
                model_name="lstm",
                family=ModelFamily.SEQUENCE,
                version="0.1.0",
                input_schema=build_model_spec().input_schema,
                output_schema=build_model_spec().output_schema,
                hyperparams={
                    "lookback": 3,
                    "forecast_horizon": 1,
                    "stride": 1,
                    "subsequence_length": 2,
                    "subsequence_stride": 1,
                    "force_backend": "fallback",
                        "rolling_window_spec": {
                            "train_size": 3,
                            "valid_size": 1,
                            "test_size": 0,
                            "step_size": 1,
                            "min_train_size": 3,
                            "embargo": 0,
                        "purge_gap": 0,
                        "expanding_train": True,
                    },
                },
            ),
            trainer_config=TrainerConfig(runner="local", epochs=1, batch_size=8, deterministic=True),
            seed=7,
            tracking_context=TrackingContext(backend="file", experiment_name="tests"),
        )
    )

    evaluation_summary = LocalArtifactStore(artifact_root).read_json(
        str(artifact_root / "models" / "lstm-rolling-runner" / "evaluation_summary.json")
    )
    assert result.metrics["rolling_window_count"] >= 1.0
    assert "rolling_window_evaluation" in evaluation_summary
    assert evaluation_summary["rolling_window_evaluation"]["window_count"] >= 1
