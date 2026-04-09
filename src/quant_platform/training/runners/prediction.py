from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.contracts.training import PredictionFrame, PredictRequest


@dataclass
class PredictionRunner:
    model_registry: ModelRegistry
    dataset_store: dict[str, list[DatasetSample]]
    artifact_root: Path

    def predict(self, request: PredictRequest) -> PredictionFrame:
        if request.dataset_ref is None:
            raise ValueError("phase 1 prediction runner requires dataset_ref input")
        model, meta = self.model_registry.load_from_artifact(request.model_artifact_uri)
        runtime = self.model_registry.resolve_runtime(meta.model_name)
        samples = self._load_dataset(request.dataset_ref)
        scoped_samples = self._samples_for_scope(
            samples, request.dataset_ref, request.prediction_scope.scope_name
        )
        predict_input = runtime.input_adapter.build_predict_input(
            scoped_samples,
            request.dataset_ref,
            meta.model_spec,
            runtime.registration,
        )
        self.model_registry.capability_validator.validate(
            runtime.registration, request.dataset_ref, predict_input
        )
        raw_output = model.predict(predict_input)
        return runtime.prediction_adapter.build_prediction_frame(
            raw_output,
            predict_input,
            model_run_id=meta.run_id,
            prediction_scope=request.prediction_scope,
        )

    def _load_dataset(self, dataset_ref: DatasetRef) -> list[DatasetSample]:
        if dataset_ref.dataset_id not in self.dataset_store:
            raise KeyError(f"dataset '{dataset_ref.dataset_id}' is not available in local store")
        return self.dataset_store[dataset_ref.dataset_id]

    def _samples_for_scope(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        scope_name: str,
    ) -> list[DatasetSample]:
        if scope_name == "full":
            return samples
        split_manifest = dataset_ref.split_manifest
        time_range = getattr(split_manifest, f"{scope_name}_range")
        return [
            sample for sample in samples if time_range.start <= sample.timestamp < time_range.end
        ]
