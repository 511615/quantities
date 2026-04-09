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
from quant_platform.models.inference.prediction_frame import build_prediction_frame
from quant_platform.models.registry.base import BaseModelPlugin
from quant_platform.models.support import (
    average,
    build_tabular_train_bundle,
    load_saved_state,
    mae,
    save_model_artifact,
)


class ElasticNetModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.LINEAR
    default_input_adapter_key = "tabular_passthrough"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._bias = 0.0
        self._weights: list[float] = []
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
        matrix = bundle.blocks["feature_matrix"]
        targets = bundle.targets
        if not targets:
            raise ValueError("training samples cannot be empty")
        self._feature_names = bundle.feature_names
        self._training_sample_count = len(targets)
        feature_means = [
            average(row[index] for row in matrix) for index in range(len(bundle.feature_names))
        ]
        target_mean = average(targets)
        l1_ratio = float(self.spec.hyperparams.get("l1_ratio", 0.5))
        alpha = float(self.spec.hyperparams.get("alpha", 0.001))
        self._weights = [
            target_mean / (feature_mean if feature_mean else 1.0) * (1.0 - alpha)
            for feature_mean in feature_means
        ]
        self._bias = target_mean * l1_ratio
        predictions = [self._predict_row(row) for row in matrix]
        return {"mae": mae(predictions, targets), "sample_count": float(len(targets))}

    def predict(
        self,
        predict_input: PredictInputBundle | list[DatasetSample],
        model_run_id: str | None = None,
    ):
        if isinstance(predict_input, list):
            if model_run_id is None:
                raise ValueError("model_run_id is required when predicting from raw samples")
            bundle = build_tabular_train_bundle(self.spec, predict_input)
            predictions = [self._predict_row(row) for row in bundle.blocks["feature_matrix"]]
            return build_prediction_frame(predict_input, predictions, model_run_id=model_run_id)
        predictions = [self._predict_row(row) for row in predict_input.blocks["feature_matrix"]]
        return ModelPredictionOutputs(
            predictions=predictions, confidences=[0.55] * len(predictions)
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={"estimator": {"weights": self._weights, "bias": self._bias}},
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._weights = [float(value) for value in state["weights"]]
        model._bias = float(state["bias"])
        return model

    def _predict_row(self, row: list[float]) -> float:
        if not self._weights:
            return self._bias
        score = sum(weight * value for weight, value in zip(self._weights, row, strict=False))
        return (score / max(1, len(row))) + self._bias

    def feature_importance(self) -> dict[str, float] | None:
        if not self._weights:
            return None
        return {
            feature_name: abs(float(weight))
            for feature_name, weight in zip(self._feature_names, self._weights, strict=False)
        }
