from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path

from quant_platform.common.clock.seed import apply_seed
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import ArtifactRef
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.experiment.manifests.run_manifest import ReproContext, RunManifest
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.models.support import merge_training_hyperparams
from quant_platform.training.contracts.training import (
    FitRequest,
    FitResult,
    PredictionFrame,
    PredictionScope,
)
from quant_platform.training.evaluation import build_regression_evaluation_summary
from quant_platform.training.tracking.file_tracking import FileTrackingClient
from train.splits.rolling import RollingWindowSpec, generate_rolling_windows


@dataclass
class LocalTrainingRunner:
    model_registry: ModelRegistry
    dataset_store: dict[str, list[DatasetSample]]
    artifact_root: Path

    def fit(self, request: FitRequest) -> FitResult:
        apply_seed(request.seed)
        artifact_store = LocalArtifactStore(self.artifact_root)
        samples = self._load_dataset(request.dataset_ref)
        dataset_manifest = self._load_dataset_manifest(request.dataset_ref)
        DatasetBuilder.validate_samples(samples, request.dataset_ref.feature_view_ref.as_of_time)
        runtime = self.model_registry.resolve_runtime(request.model_spec.model_name)
        effective_spec = request.model_spec.model_copy(
            update={
                "hyperparams": {
                    **runtime.registration.default_hyperparams,
                    **request.model_spec.hyperparams,
                }
            }
        )
        effective_spec = merge_training_hyperparams(
            effective_spec,
            request.trainer_config,
            seed=request.seed,
        )
        plugin = runtime.model_cls(effective_spec)
        train_input = runtime.input_adapter.build_train_input(
            samples, request.dataset_ref, effective_spec, runtime.registration
        )
        self.model_registry.capability_validator.validate(
            runtime.registration, request.dataset_ref, train_input
        )
        valid_samples = self._samples_in_range(samples, request.dataset_ref, "valid")
        valid_input = None
        if valid_samples:
            valid_input = runtime.input_adapter.build_predict_input(
                valid_samples,
                request.dataset_ref,
                effective_spec,
                runtime.registration,
            )
            self.model_registry.capability_validator.validate(
                runtime.registration, request.dataset_ref, valid_input
            )
        rolling_evaluation = None
        if self._should_run_lstm_rolling(effective_spec):
            plugin, metrics, rolling_evaluation = self._fit_lstm_with_rolling_windows(
                runtime=runtime,
                dataset_ref=request.dataset_ref,
                effective_spec=effective_spec,
                samples=samples,
            )
        else:
            metrics = plugin.fit(train_input, valid_input)
        model_artifact = runtime.artifact_adapter.save_model(
            plugin,
            run_id=request.run_id,
            artifact_root=self.artifact_root,
            registration=runtime.registration,
            train_input=train_input,
        )
        feature_importance_uri: str | None = None
        feature_importance = plugin.feature_importance() or {}
        if feature_importance:
            importance_path = (
                self.artifact_root / "models" / request.run_id / "feature_importance.json"
            )
            importance_path.parent.mkdir(parents=True, exist_ok=True)
            importance_path.write_text(
                json.dumps(
                    {
                        "run_id": request.run_id,
                        "model_name": request.model_spec.model_name,
                        "feature_importance": feature_importance,
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            feature_importance_uri = str(importance_path)
        scope_payloads = self._collect_prediction_scopes(
            plugin=plugin,
            runtime=runtime,
            dataset_ref=request.dataset_ref,
            effective_spec=effective_spec,
            run_id=request.run_id,
            samples=samples,
            artifact_store=artifact_store,
        )
        evaluation_summary = build_regression_evaluation_summary(
            run_id=request.run_id,
            dataset_ref=request.dataset_ref,
            scope_payloads=scope_payloads,
            feature_importance=feature_importance,
        )
        if rolling_evaluation is not None:
            evaluation_summary["rolling_window_evaluation"] = rolling_evaluation
        evaluation_path = self.artifact_root / "models" / request.run_id / "evaluation_summary.json"
        evaluation_path.parent.mkdir(parents=True, exist_ok=True)
        evaluation_path.write_text(
            json.dumps(evaluation_summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        repro_digest = stable_digest(
            {
                "dataset_hash": request.dataset_ref.dataset_hash,
                "model_spec": effective_spec,
                "seed": request.seed,
                "metrics": metrics,
                "evaluation_summary": evaluation_summary,
            }
        )
        manifest = RunManifest(
            run_id=request.run_id,
            repro_context=ReproContext(
                config_hash=stable_digest(request.trainer_config),
                data_hash=request.dataset_ref.dataset_hash,
                code_version=effective_spec.version,
                seed=request.seed,
            ),
            dataset_ref_uri=f"dataset://{request.dataset_ref.dataset_id}",
            dataset_id=request.dataset_ref.dataset_id,
            dataset_manifest_uri=request.dataset_ref.dataset_manifest_uri,
            dataset_type=self._dataset_type(dataset_manifest),
            data_domain=self._data_domain(dataset_manifest),
            data_domains=self._data_domains(dataset_manifest),
            snapshot_version=dataset_manifest.snapshot_version,
            entity_scope=request.dataset_ref.entity_scope,
            entity_count=request.dataset_ref.entity_count,
            feature_schema_hash=(
                request.dataset_ref.feature_schema_hash or dataset_manifest.feature_schema_hash
            ),
            dataset_readiness_status=request.dataset_ref.readiness_status,
            dataset_readiness_warnings=self._dataset_readiness_warnings(
                request.dataset_ref,
                dataset_manifest,
            ),
            source_dataset_ids=self._source_dataset_ids(dataset_manifest),
            fusion_domains=self._fusion_domains(dataset_manifest),
            model_artifact=ArtifactRef(
                kind="model_artifact_meta",
                uri=model_artifact.artifact_uri,
                metadata={"model_name": request.model_spec.model_name},
            ),
            metrics=metrics,
        )
        manifest_path = self.artifact_root / "models" / request.run_id / "train_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        FileTrackingClient(self.artifact_root).log_run_summary(
            request.run_id,
            metrics,
            {
                "model_name": request.model_spec.model_name,
                "dataset_id": request.dataset_ref.dataset_id,
            },
        )
        self._validate_artifact_bundle(request.run_id)
        return FitResult(
            run_id=request.run_id,
            model_artifact_uri=model_artifact.artifact_uri,
            model_name=request.model_spec.model_name,
            metrics=metrics,
            feature_importance_uri=feature_importance_uri,
            train_manifest_uri=str(manifest_path),
            repro_digest=repro_digest,
        )

    def _fit_lstm_with_rolling_windows(
        self,
        *,
        runtime,
        dataset_ref: DatasetRef,
        effective_spec,
        samples: list[DatasetSample],
    ):
        rolling_spec = self._resolve_lstm_rolling_spec(effective_spec, len(samples))
        windows = generate_rolling_windows(len(samples), rolling_spec)
        selected_plugin = None
        best_valid = float("inf")
        per_window: list[dict[str, object]] = []
        valid_mae_values: list[float] = []
        test_mae_values: list[float] = []

        for window in windows:
            train_samples = samples[window.train_start : window.train_end]
            valid_context = samples[window.train_start : window.valid_end]
            test_context = samples[window.train_start : (window.test_end or window.valid_end)]
            valid_samples = samples[window.valid_start : window.valid_end]
            test_samples = (
                samples[window.test_start : window.test_end]
                if window.test_start is not None and window.test_end is not None
                else []
            )

            window_plugin = runtime.model_cls(effective_spec)
            window_train_input = runtime.input_adapter.build_train_input(
                train_samples,
                dataset_ref,
                effective_spec,
                runtime.registration,
            )
            self.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                window_train_input,
            )
            window_valid_input = runtime.input_adapter.build_predict_input(
                valid_context,
                dataset_ref,
                effective_spec,
                runtime.registration,
            )
            self.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                window_valid_input,
            )
            window_metrics = window_plugin.fit(window_train_input, window_valid_input)
            aligned_valid_samples, valid_frame = self._predict_frame(
                plugin=window_plugin,
                runtime=runtime,
                dataset_ref=dataset_ref,
                effective_spec=effective_spec,
                scoped_samples=valid_context,
                run_id="rolling-valid",
                scope_name="valid",
                target_start_index=len(valid_context) - len(valid_samples),
            )
            valid_mae = self._window_frame_mae(valid_frame, aligned_valid_samples)
            test_mae = None
            if test_samples:
                aligned_test_samples, test_frame = self._predict_frame(
                    plugin=window_plugin,
                    runtime=runtime,
                    dataset_ref=dataset_ref,
                    effective_spec=effective_spec,
                    scoped_samples=test_context,
                    run_id="rolling-test",
                    scope_name="test",
                    target_start_index=len(test_context) - len(test_samples),
                )
                test_mae = self._window_frame_mae(test_frame, aligned_test_samples)
                test_mae_values.append(test_mae)
            valid_mae_values.append(valid_mae)
            per_window.append(
                {
                    "window_id": window.window_id,
                    "train_range": {"start": window.train_start, "end": window.train_end},
                    "valid_range": {"start": window.valid_start, "end": window.valid_end},
                    "test_range": {"start": window.test_start, "end": window.test_end},
                    "train_time_range": self._time_range_payload(train_samples),
                    "valid_time_range": self._time_range_payload(valid_samples),
                    "test_time_range": self._time_range_payload(test_samples),
                    "train_sample_count": len(train_samples),
                    "valid_sample_count": len(aligned_valid_samples),
                    "test_sample_count": len(aligned_test_samples) if test_samples else 0,
                    "valid_mae": valid_mae,
                    "test_mae": test_mae,
                    "fit_metrics": window_metrics,
                }
            )
            if valid_mae < best_valid:
                best_valid = valid_mae
                selected_plugin = window_plugin

        if selected_plugin is None:
            raise ValueError("rolling windows did not produce a valid LSTM checkpoint")

        metrics = {
            "mae": float(sum(valid_mae_values) / max(1, len(valid_mae_values))),
            "valid_mae": float(sum(valid_mae_values) / max(1, len(valid_mae_values))),
            "sample_count": float(len(samples)),
            "rolling_window_count": float(len(windows)),
        }
        if test_mae_values:
            metrics["test_mae"] = float(sum(test_mae_values) / max(1, len(test_mae_values)))

        rolling_evaluation = {
            "window_spec": {
                "train_size": rolling_spec.train_size,
                "valid_size": rolling_spec.valid_size,
                "test_size": rolling_spec.test_size,
                "step_size": rolling_spec.step_size,
                "min_train_size": rolling_spec.min_train_size,
                "embargo": rolling_spec.embargo,
                "purge_gap": rolling_spec.purge_gap,
                "expanding_train": rolling_spec.expanding_train,
            },
            "window_count": len(windows),
            "windows": per_window,
            "mean_valid_mae": metrics["valid_mae"],
            "mean_test_mae": metrics.get("test_mae"),
            "best_valid_window_id": per_window[min(range(len(per_window)), key=lambda index: float(per_window[index]["valid_mae"]))]["window_id"]
            if per_window
            else None,
        }
        return selected_plugin, metrics, rolling_evaluation

    @staticmethod
    def _window_mae(predictions: list[float], samples: list[DatasetSample]) -> float:
        if not predictions or not samples:
            return 0.0
        paired = list(zip(predictions, samples, strict=False))
        return sum(abs(float(prediction) - float(sample.target)) for prediction, sample in paired) / max(1, len(paired))

    @staticmethod
    def _window_frame_mae(frame: PredictionFrame, samples: list[DatasetSample]) -> float:
        if not frame.rows or not samples:
            return 0.0
        paired = list(zip(frame.rows, samples, strict=False))
        return sum(abs(float(row.prediction) - float(sample.target)) for row, sample in paired) / max(1, len(paired))

    @staticmethod
    def _should_run_lstm_rolling(effective_spec) -> bool:
        if effective_spec.model_name != "lstm":
            return False
        return isinstance(effective_spec.hyperparams.get("rolling_window_spec"), dict)

    @staticmethod
    def _resolve_lstm_rolling_spec(effective_spec, sample_count: int) -> RollingWindowSpec:
        raw = effective_spec.hyperparams.get("rolling_window_spec")
        if not isinstance(raw, dict):
            raw = {}
        train_size = int(raw.get("train_size", max(8, sample_count // 2)) or max(8, sample_count // 2))
        valid_size = int(raw.get("valid_size", max(2, sample_count // 6)) or max(2, sample_count // 6))
        raw_test_size = raw.get("test_size")
        if raw_test_size is None:
            test_size = max(2, sample_count // 6)
        else:
            test_size = max(0, int(raw_test_size))
        step_size = int(raw.get("step_size", max(1, test_size)) or max(1, test_size))
        min_train_size = int(raw.get("min_train_size", max(1, min(train_size, sample_count))) or max(1, min(train_size, sample_count)))
        embargo = int(raw.get("embargo", 0) or 0)
        purge_gap = int(raw.get("purge_gap", 0) or 0)
        expanding_train = bool(raw.get("expanding_train", True))
        return RollingWindowSpec(
            train_size=train_size,
            valid_size=valid_size,
            test_size=test_size,
            step_size=step_size,
            min_train_size=min_train_size,
            embargo=embargo,
            purge_gap=purge_gap,
            expanding_train=expanding_train,
        )

    def _collect_prediction_scopes(
        self,
        *,
        plugin,
        runtime,
        dataset_ref: DatasetRef,
        effective_spec,
        run_id: str,
        samples: list[DatasetSample],
        artifact_store: LocalArtifactStore,
    ) -> dict[str, tuple[list[DatasetSample], object]]:
        scope_payloads: dict[str, tuple[list[DatasetSample], PredictionFrame]] = {}
        for scope_name in ("train", "valid", "test", "full"):
            scoped_samples = (
                samples if scope_name == "full" else self._samples_in_range(samples, dataset_ref, scope_name)
            )
            if not scoped_samples:
                continue
            aligned_samples, frame = self._predict_frame(
                plugin=plugin,
                runtime=runtime,
                dataset_ref=dataset_ref,
                effective_spec=effective_spec,
                scoped_samples=scoped_samples,
                run_id=run_id,
                scope_name=scope_name,
            )
            artifact_store.write_model(f"predictions/{run_id}/{scope_name}.json", frame)
            explainability_payload = plugin.explainability_payload()
            if explainability_payload:
                artifact_store.write_json(
                    f"predictions/{run_id}/{scope_name}_explainability.json",
                    explainability_payload,
                )
            scope_payloads[scope_name] = (aligned_samples, frame)
        return scope_payloads

    def _predict_frame(
        self,
        *,
        plugin,
        runtime,
        dataset_ref: DatasetRef,
        effective_spec,
        scoped_samples: list[DatasetSample],
        run_id: str,
        scope_name: str,
        target_start_index: int | None = None,
    ) -> tuple[list[DatasetSample], PredictionFrame]:
        predict_input = runtime.input_adapter.build_predict_input(
            scoped_samples,
            dataset_ref,
            effective_spec,
            runtime.registration,
        )
        if target_start_index is not None:
            predict_input = replace(
                predict_input,
                metadata={**predict_input.metadata, "target_start_index": target_start_index},
            )
        self.model_registry.capability_validator.validate(
            runtime.registration, dataset_ref, predict_input
        )
        raw_output = plugin.predict(predict_input)
        frame = runtime.prediction_adapter.build_prediction_frame(
            raw_output,
            predict_input,
            model_run_id=run_id,
            prediction_scope=PredictionScope(
                scope_name=scope_name,
                as_of_time=dataset_ref.feature_view_ref.as_of_time,
            ),
        )
        return self._align_samples_to_frame(scoped_samples, frame), frame

    @staticmethod
    def _align_samples_to_frame(
        scoped_samples: list[DatasetSample],
        frame: PredictionFrame,
    ) -> list[DatasetSample]:
        if len(frame.rows) == len(scoped_samples):
            return scoped_samples
        sample_index = {
            (sample.entity_key, max(sample.timestamp, sample.available_time), sample.available_time): sample
            for sample in scoped_samples
        }
        aligned: list[DatasetSample] = []
        for row in frame.rows:
            key = (
                str(row.entity_keys.get("instrument", "")),
                row.timestamp,
                row.feature_available_time or row.timestamp,
            )
            sample = sample_index.get(key)
            if sample is None:
                fallback = next(
                    (
                        candidate
                        for candidate in scoped_samples
                        if candidate.entity_key == key[0] and candidate.available_time == key[2]
                    ),
                    None,
                )
                if fallback is not None:
                    aligned.append(fallback)
                continue
            aligned.append(sample)
        return aligned

    @staticmethod
    def _time_range_payload(samples: list[DatasetSample]) -> dict[str, str | None]:
        if not samples:
            return {"start_time": None, "end_time": None}
        return {
            "start_time": samples[0].timestamp.isoformat(),
            "end_time": samples[-1].timestamp.isoformat(),
        }

    def _load_dataset(self, dataset_ref: DatasetRef) -> list[DatasetSample]:
        if dataset_ref.dataset_id not in self.dataset_store:
            raise KeyError(f"dataset '{dataset_ref.dataset_id}' is not available in local store")
        return self.dataset_store[dataset_ref.dataset_id]

    def _load_dataset_manifest(self, dataset_ref: DatasetRef) -> DatasetBuildManifest:
        if dataset_ref.dataset_manifest_uri:
            return LocalArtifactStore(self.artifact_root).read_model(
                dataset_ref.dataset_manifest_uri,
                DatasetBuildManifest,
            )

        fallback_path = self.artifact_root / "datasets" / f"{dataset_ref.dataset_id}_dataset_manifest.json"
        if fallback_path.exists():
            return LocalArtifactStore(self.artifact_root).read_model(
                str(fallback_path),
                DatasetBuildManifest,
            )

        return DatasetBuildManifest(
            dataset_id=dataset_ref.dataset_id,
            asset_id=dataset_ref.dataset_id,
            feature_set_id=dataset_ref.feature_view_ref.feature_set_id,
            label_horizon=dataset_ref.label_spec.horizon,
            sample_count=len(self.dataset_store.get(dataset_ref.dataset_id, [])),
            dropped_rows=0,
            split_strategy=dataset_ref.split_manifest.strategy.value,
            entity_scope=dataset_ref.entity_scope,
            entity_count=dataset_ref.entity_count,
            feature_schema_hash=dataset_ref.feature_schema_hash,
            label_schema_hash=dataset_ref.label_schema_hash,
            readiness_status=dataset_ref.readiness_status,
        )

    def _validate_artifact_bundle(self, run_id: str) -> None:
        required_paths = {
            "metadata.json": self.artifact_root / "models" / run_id / "metadata.json",
            "train_manifest.json": self.artifact_root / "models" / run_id / "train_manifest.json",
            "evaluation_summary.json": self.artifact_root / "models" / run_id / "evaluation_summary.json",
            "predictions/full.json": self.artifact_root / "predictions" / run_id / "full.json",
            "tracking.json": self.artifact_root / "tracking" / f"{run_id}.json",
        }
        missing = [label for label, path in required_paths.items() if not path.exists()]
        if missing:
            raise ValueError(
                f"training artifact bundle is incomplete for run '{run_id}': {', '.join(missing)}"
            )

    def _dataset_readiness_warnings(
        self,
        dataset_ref: DatasetRef,
        dataset_manifest: DatasetBuildManifest,
    ) -> list[str]:
        warnings: list[str] = []
        if dataset_ref.readiness_status == "warning":
            warnings.append("dataset_readiness_warning")
        if dataset_manifest.freshness_status in {"warning", "stale", "outdated"}:
            warnings.append(f"dataset_freshness:{dataset_manifest.freshness_status}")
        if dataset_manifest.quality_status in {"warning", "risk"}:
            warnings.append(f"dataset_quality:{dataset_manifest.quality_status}")
        return warnings

    @staticmethod
    def _dataset_type(dataset_manifest: DatasetBuildManifest) -> str | None:
        acquisition_profile = dataset_manifest.acquisition_profile or {}
        dataset_type = acquisition_profile.get("dataset_type")
        return str(dataset_type) if isinstance(dataset_type, str) and dataset_type else None

    @staticmethod
    def _data_domain(dataset_manifest: DatasetBuildManifest) -> str | None:
        acquisition_profile = dataset_manifest.acquisition_profile or {}
        data_domain = acquisition_profile.get("data_domain")
        return str(data_domain) if isinstance(data_domain, str) and data_domain else None

    @staticmethod
    def _data_domains(dataset_manifest: DatasetBuildManifest) -> list[str]:
        acquisition_profile = dataset_manifest.acquisition_profile or {}
        value = acquisition_profile.get("data_domains")
        if isinstance(value, list):
            resolved = [str(item) for item in value if isinstance(item, str) and item]
            if resolved:
                return resolved
        data_domain = acquisition_profile.get("data_domain")
        return [str(data_domain)] if isinstance(data_domain, str) and data_domain else []

    @staticmethod
    def _source_dataset_ids(dataset_manifest: DatasetBuildManifest) -> list[str]:
        acquisition_profile = dataset_manifest.acquisition_profile or {}
        value = acquisition_profile.get("source_dataset_ids")
        return [str(item) for item in value if isinstance(item, str) and item] if isinstance(value, list) else []

    @staticmethod
    def _fusion_domains(dataset_manifest: DatasetBuildManifest) -> list[str]:
        acquisition_profile = dataset_manifest.acquisition_profile or {}
        value = acquisition_profile.get("fusion_domains")
        return [str(item) for item in value if isinstance(item, str) and item] if isinstance(value, list) else []

    def _samples_in_range(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        scope_name: str,
    ) -> list[DatasetSample]:
        split_manifest = dataset_ref.split_manifest
        time_range = getattr(split_manifest, f"{scope_name}_range")
        return [
            sample for sample in samples if time_range.start <= sample.timestamp < time_range.end
        ]
