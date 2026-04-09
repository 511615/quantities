from __future__ import annotations

from pathlib import Path

from quant_platform.models.advanced._trainable import (
    AdvancedTrainingArtifacts,
    build_valid_targets,
    fit_linear_regressor,
    predict_linear_regressor,
    slice_valid_features,
)
from quant_platform.models.contracts import (
    AdvancedModelKind,
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.registry.base import BaseModelPlugin
from quant_platform.models.support import (
    average,
    flatten_numeric,
    load_saved_state,
    save_model_artifact,
)


class MultimodalFusionModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.MULTIMODAL

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._training_sample_count = 0
        self._feature_names = [field.name for field in spec.input_schema]
        self._weights: list[float] = []
        self._bias = 0.0
        self._feature_means: list[float] = []
        self._feature_stds: list[float] = []
        self._train_loss = 0.0
        self._valid_loss = 0.0
        self._best_epoch = 0
        self._trained_steps = 0
        self._residual_scale = 1.0
        self._confidence_source = "fallback_residual"
        self._backend = "linear_multimodal_fusion"

    def fit(
        self, train_input: TrainInputBundle, valid_input: PredictInputBundle | None = None
    ) -> dict[str, float]:
        targets = train_input.targets
        if not targets:
            raise ValueError("training samples cannot be empty")
        self._feature_names = train_input.feature_names
        self._training_sample_count = len(targets)
        train_features = self._build_feature_matrix(train_input.blocks)
        valid_features = (
            self._build_feature_matrix(valid_input.blocks) if valid_input is not None else None
        )
        valid_features = slice_valid_features(valid_input, valid_features)
        training = fit_linear_regressor(
            train_features,
            targets,
            valid_features=valid_features,
            valid_targets=build_valid_targets(valid_input),
            hyperparams=self.spec.hyperparams,
        )
        self._apply_training(training)
        return {
            "mae": training.train_loss,
            "train_loss": training.train_loss,
            "valid_mae": training.valid_loss,
            "valid_loss": training.valid_loss,
            "best_epoch": float(training.best_epoch),
            "trained_steps": float(training.trained_steps),
            "sample_count": float(len(targets)),
        }

    def predict(
        self,
        predict_input: PredictInputBundle,
        model_run_id: str | None = None,
    ) -> ModelPredictionOutputs:
        _ = model_run_id
        features = self._build_feature_matrix(predict_input.blocks)
        return predict_linear_regressor(
            features,
            weights=self._weights,
            bias=self._bias,
            feature_means=self._feature_means,
            feature_stds=self._feature_stds,
            residual_scale=self._residual_scale,
            confidence_source=self._confidence_source,
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "weights": self._weights,
                    "bias": self._bias,
                    "feature_means": self._feature_means,
                    "feature_stds": self._feature_stds,
                    "residual_scale": self._residual_scale,
                    "confidence_source": self._confidence_source,
                    "feature_names": self._feature_names,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
            input_adapter_key="market_text_aligned",
            training_config=dict(self.spec.hyperparams),
            training_metrics={
                "train_loss": self._train_loss,
                "valid_loss": self._valid_loss,
            },
            best_epoch=self._best_epoch,
            trained_steps=self._trained_steps,
            checkpoint_tag="best_validation",
            input_metadata={"lookback": int(self.spec.hyperparams.get("lookback", 1))},
            prediction_metadata={"confidence_source": self._confidence_source},
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._weights = [float(value) for value in state.get("weights", [])]
        model._bias = float(state.get("bias", 0.0))
        model._feature_means = [float(value) for value in state.get("feature_means", [])]
        model._feature_stds = [float(value) for value in state.get("feature_stds", [])]
        model._residual_scale = float(state.get("residual_scale", 1.0))
        model._confidence_source = str(state.get("confidence_source", model._confidence_source))
        model._feature_names = list(state.get("feature_names", model._feature_names))
        return model

    def _build_feature_matrix(self, blocks: dict[str, object]) -> list[list[float]]:
        feature_rows: list[list[float]] = []
        market_block = blocks["market_block"]
        text_block = blocks["text_block"]
        text_mask = blocks["text_mask"]
        for index in range(len(market_block)):
            market_values = flatten_numeric(market_block[index])
            raw_text_values = flatten_numeric(text_block[index])
            text_values = (
                raw_text_values
                if text_mask[index]
                else [0.0 for _ in raw_text_values]
            )
            feature_rows.append(
                market_values
                + text_values
                + [
                    average(market_values),
                    average(text_values),
                    1.0 if text_mask[index] else 0.0,
                    float(len(text_values)),
                ]
            )
        return feature_rows

    def _apply_training(self, training: AdvancedTrainingArtifacts) -> None:
        self._weights = list(training.weights)
        self._bias = float(training.bias)
        self._feature_means = list(training.feature_means)
        self._feature_stds = list(training.feature_stds)
        self._train_loss = float(training.train_loss)
        self._valid_loss = float(training.valid_loss)
        self._best_epoch = int(training.best_epoch)
        self._trained_steps = int(training.trained_steps)
        self._residual_scale = float(training.residual_scale)
        self._confidence_source = training.confidence_source
