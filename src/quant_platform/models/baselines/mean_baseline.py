from __future__ import annotations

from pathlib import Path

from quant_platform.common.enums.core import ModelFamily
from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.models.contracts import (
    AdvancedModelKind,
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.registry.base import BaseModelPlugin
from quant_platform.models.support import (
    build_tabular_train_bundle,
    bundle_to_prediction_frame,
    load_saved_state,
    mae,
    save_model_artifact,
)


class MeanBaselineModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.BASELINE
    default_input_adapter_key = "tabular_passthrough"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._mean_target = 0.0
        self._training_sample_count = 0
        self._feature_names = [field.name for field in spec.input_schema]

    def fit(
        self,
        train_input: TrainInputBundle | list[DatasetSample],
        valid_input: PredictInputBundle | None = None,
    ) -> dict[str, float]:
        _ = valid_input
        bundle = (
            build_tabular_train_bundle(self.spec, train_input)
            if isinstance(train_input, list)
            else train_input
        )
        if not bundle.targets:
            raise ValueError("training samples cannot be empty")
        self._mean_target = sum(bundle.targets) / len(bundle.targets)
        self._training_sample_count = len(bundle.targets)
        self._feature_names = bundle.feature_names
        predictions = [self._mean_target for _ in bundle.targets]
        return {"mae": mae(predictions, bundle.targets), "sample_count": float(len(bundle.targets))}

    def predict(
        self,
        predict_input: PredictInputBundle | list[DatasetSample],
        model_run_id: str | None = None,
    ):
        if isinstance(predict_input, list):
            if model_run_id is None:
                raise ValueError("model_run_id is required when predicting from raw samples")
            bundle = build_tabular_train_bundle(self.spec, predict_input)
            return bundle_to_prediction_frame(
                PredictInputBundle(
                    dataset_ref=bundle.dataset_ref,
                    model_spec=bundle.model_spec,
                    source_samples=bundle.source_samples,
                    feature_names=bundle.feature_names,
                    blocks=bundle.blocks,
                    metadata=bundle.metadata,
                ),
                ModelPredictionOutputs(
                    predictions=[self._mean_target for _ in predict_input],
                    confidences=[0.5] * len(predict_input),
                ),
                model_run_id=model_run_id,
            )
        return ModelPredictionOutputs(
            predictions=[self._mean_target for _ in predict_input.source_samples],
            confidences=[0.5] * len(predict_input.source_samples),
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={"mean_target": self._mean_target},
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)
        model = cls(spec)
        model._mean_target = float(state["mean_target"])
        return model


def build_mean_baseline(spec):
    return MeanBaselineModel(spec)
