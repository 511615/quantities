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


class LightGBMModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.TREE
    default_input_adapter_key = "tabular_passthrough"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._tree_thresholds: list[float] = []
        self._leaf_values: list[float] = []
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
        self._tree_thresholds = [
            average(row[index] for row in matrix) for index in range(len(bundle.feature_names))
        ]
        global_mean = average(targets)
        self._leaf_values = [global_mean + threshold * 0.1 for threshold in self._tree_thresholds]
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
            return build_prediction_frame(
                predict_input,
                predictions,
                model_run_id=model_run_id,
                confidences=[0.58] * len(predictions),
            )
        predictions = [self._predict_row(row) for row in predict_input.blocks["feature_matrix"]]
        return ModelPredictionOutputs(
            predictions=predictions, confidences=[0.58] * len(predictions)
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "tree_thresholds": self._tree_thresholds,
                    "leaf_values": self._leaf_values,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._tree_thresholds = [float(value) for value in state["tree_thresholds"]]
        model._leaf_values = [float(value) for value in state["leaf_values"]]
        return model

    def _predict_row(self, row: list[float]) -> float:
        if not self._tree_thresholds:
            return 0.0
        score = 0.0
        for index, threshold in enumerate(self._tree_thresholds):
            leaf_value = self._leaf_values[index]
            score += leaf_value if row[index] >= threshold else leaf_value * 0.5
        return score / len(self._tree_thresholds)

    def feature_importance(self) -> dict[str, float] | None:
        if not self._tree_thresholds:
            return None
        return {
            feature_name: abs(float(leaf_value - threshold))
            for feature_name, threshold, leaf_value in zip(
                self._feature_names,
                self._tree_thresholds,
                self._leaf_values,
                strict=False,
            )
        }
