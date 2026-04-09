from __future__ import annotations

import json
from dataclasses import dataclass
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
from quant_platform.training.contracts.training import FitRequest, FitResult
from quant_platform.training.tracking.file_tracking import FileTrackingClient


@dataclass
class LocalTrainingRunner:
    model_registry: ModelRegistry
    dataset_store: dict[str, list[DatasetSample]]
    artifact_root: Path

    def fit(self, request: FitRequest) -> FitResult:
        apply_seed(request.seed)
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
        metrics = plugin.fit(train_input, valid_input)
        model_artifact = runtime.artifact_adapter.save_model(
            plugin,
            run_id=request.run_id,
            artifact_root=self.artifact_root,
            registration=runtime.registration,
            train_input=train_input,
        )
        feature_importance_uri: str | None = None
        feature_importance = plugin.feature_importance()
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
        repro_digest = stable_digest(
            {
                "dataset_hash": request.dataset_ref.dataset_hash,
                "model_spec": effective_spec,
                "seed": request.seed,
                "metrics": metrics,
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
        return FitResult(
            run_id=request.run_id,
            model_artifact_uri=model_artifact.artifact_uri,
            model_name=request.model_spec.model_name,
            metrics=metrics,
            feature_importance_uri=feature_importance_uri,
            train_manifest_uri=str(manifest_path),
            repro_digest=repro_digest,
        )

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
