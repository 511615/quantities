from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_platform.models.advanced._trainable import (
    AdvancedTrainingArtifacts,
    build_valid_targets,
    dot_product,
    fit_linear_regressor,
    normalize_features,
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
        self._modality_order: list[str] = []
        self._modality_feature_names: dict[str, list[str]] = {}
        self._branch_states: dict[str, dict[str, Any] | None] = {}
        self._market_feature_names: list[str] = []
        self._text_feature_names: list[str] = []
        self._market_branch: dict[str, Any] | None = None
        self._text_branch: dict[str, Any] | None = None
        self._blender_branch: dict[str, Any] | None = None
        self._legacy_estimator: dict[str, Any] | None = None
        self._train_loss = 0.0
        self._valid_loss = 0.0
        self._best_epoch = 0
        self._trained_steps = 0
        self._residual_scale = 1.0
        self._confidence_source = "fallback_residual"
        self._fusion_strategy = "late_score_blend"
        self._backend = "late_score_blend"

    def fit(
        self, train_input: TrainInputBundle, valid_input: PredictInputBundle | None = None
    ) -> dict[str, float]:
        targets = train_input.targets
        if not targets:
            raise ValueError("training samples cannot be empty")
        self._feature_names = train_input.feature_names
        self._modality_order = [
            str(item)
            for item in train_input.blocks.get("modality_order", [])
            if isinstance(item, str)
        ]
        self._modality_feature_names = {
            str(key): [str(item) for item in value if isinstance(item, str)]
            for key, value in dict(train_input.blocks.get("modality_feature_names", {})).items()
            if isinstance(key, str) and isinstance(value, list)
        }
        self._market_feature_names = list(train_input.blocks.get("market_feature_names", []))
        self._text_feature_names = list(train_input.blocks.get("text_feature_names", []))
        self._training_sample_count = len(targets)
        train_modality_features = self._build_modality_feature_matrices(train_input.blocks)
        valid_modality_features = (
            self._build_modality_feature_matrices(valid_input.blocks) if valid_input is not None else {}
        )
        valid_modality_features = {
            modality: slice_valid_features(valid_input, features)
            for modality, features in valid_modality_features.items()
        }
        valid_targets = build_valid_targets(valid_input)
        modality_presence_train = self._modality_presence(train_input.blocks)
        modality_presence_valid = (
            {
                key: (self._slice_valid_list(valid_input, value) or [])
                for key, value in self._modality_presence(valid_input.blocks).items()
            }
            if valid_input is not None
            else {}
        )

        self._branch_states = {}
        branch_predictions_train: dict[str, list[float]] = {}
        branch_predictions_valid: dict[str, list[float]] = {}
        branch_metrics: dict[str, float] = {}
        branch_steps = 0
        for modality in self._modality_order:
            train_features = train_modality_features.get(modality, [])
            if not train_features or not train_features[0]:
                self._branch_states[modality] = None
                branch_predictions_train[modality] = [0.0 for _ in targets]
                branch_predictions_valid[modality] = (
                    [0.0 for _ in valid_targets] if valid_targets is not None else []
                )
                branch_metrics[f"{modality}_branch_train_loss"] = 0.0
                branch_metrics[f"{modality}_branch_valid_loss"] = 0.0
                continue
            training = fit_linear_regressor(
                train_features,
                targets,
                valid_features=valid_modality_features.get(modality),
                valid_targets=valid_targets,
                hyperparams=self.spec.hyperparams,
            )
            state = self._serialize_training(training)
            self._branch_states[modality] = state
            branch_predictions_train[modality] = self._predict_with_state(train_features, state)
            branch_predictions_valid[modality] = self._predict_with_state(
                valid_modality_features.get(modality),
                state,
            )
            branch_metrics[f"{modality}_branch_train_loss"] = float(training.train_loss)
            branch_metrics[f"{modality}_branch_valid_loss"] = float(training.valid_loss)
            branch_steps += int(training.trained_steps)

        self._market_branch = self._branch_states.get("market")
        self._text_branch = self._branch_states.get("nlp")
        blender_train_features = self._build_blender_feature_matrix(
            branch_predictions_train,
            modality_presence_train,
        )
        blender_valid_features = (
            self._build_blender_feature_matrix(
                branch_predictions_valid,
                modality_presence_valid,
            )
            if valid_targets is not None
            else None
        )
        blender_training = fit_linear_regressor(
            blender_train_features,
            targets,
            valid_features=blender_valid_features,
            valid_targets=valid_targets,
            hyperparams=self.spec.hyperparams,
        )
        self._blender_branch = self._serialize_training(blender_training)
        self._legacy_estimator = None

        self._train_loss = float(blender_training.train_loss)
        self._valid_loss = float(blender_training.valid_loss)
        self._best_epoch = int(blender_training.best_epoch)
        self._trained_steps = int(branch_steps + blender_training.trained_steps)
        self._residual_scale = float(blender_training.residual_scale)
        self._confidence_source = blender_training.confidence_source
        metrics = {
            "mae": blender_training.train_loss,
            "train_loss": blender_training.train_loss,
            "valid_mae": blender_training.valid_loss,
            "valid_loss": blender_training.valid_loss,
            "best_epoch": float(blender_training.best_epoch),
            "trained_steps": float(self._trained_steps),
            "sample_count": float(len(targets)),
        }
        metrics.update(branch_metrics)
        return metrics

    def predict(
        self,
        predict_input: PredictInputBundle,
        model_run_id: str | None = None,
    ) -> ModelPredictionOutputs:
        _ = model_run_id
        if self._blender_branch is not None and self._market_branch is not None:
            modality_features = self._build_modality_feature_matrices(predict_input.blocks)
            modality_presence = self._modality_presence(predict_input.blocks)
            branch_predictions = {
                modality: self._predict_with_state(
                    modality_features.get(modality),
                    self._branch_states.get(modality),
                )
                for modality in self._modality_order
            }
            blender_features = self._build_blender_feature_matrix(
                branch_predictions,
                modality_presence,
            )
            outputs = predict_linear_regressor(
                blender_features,
                weights=list(self._blender_branch.get("weights", [])),
                bias=float(self._blender_branch.get("bias", 0.0)),
                feature_means=list(self._blender_branch.get("feature_means", [])),
                feature_stds=list(self._blender_branch.get("feature_stds", [])),
                residual_scale=self._residual_scale,
                confidence_source=self._confidence_source,
            )
            metadata = dict(outputs.metadata or {})
            metadata["fusion_strategy"] = self._fusion_strategy
            metadata["branch_modalities"] = list(self._modality_order)
            metadata["market_branch_present"] = self._market_branch is not None
            metadata["text_branch_present"] = self._text_branch is not None
            return ModelPredictionOutputs(
                predictions=list(outputs.predictions),
                confidences=list(outputs.confidences),
                metadata=metadata,
            )

        legacy = self._legacy_estimator
        if legacy is None:
            raise ValueError("multimodal model has not been trained")
        features = self._build_legacy_feature_matrix(predict_input.blocks)
        return predict_linear_regressor(
            features,
            weights=list(legacy.get("weights", [])),
            bias=float(legacy.get("bias", 0.0)),
            feature_means=list(legacy.get("feature_means", [])),
            feature_stds=list(legacy.get("feature_stds", [])),
            residual_scale=float(legacy.get("residual_scale", self._residual_scale)),
            confidence_source=str(legacy.get("confidence_source", self._confidence_source)),
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "fusion_strategy": self._fusion_strategy,
                    "modality_order": self._modality_order,
                    "modality_feature_names": self._modality_feature_names,
                    "branch_states": self._branch_states,
                    "market_feature_names": self._market_feature_names,
                    "text_feature_names": self._text_feature_names,
                    "market_branch": self._market_branch,
                    "text_branch": self._text_branch,
                    "blender_branch": self._blender_branch,
                    "legacy_estimator": self._legacy_estimator,
                    "feature_names": self._feature_names,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
            input_adapter_key="multimodal_aligned_v2",
            training_config=dict(self.spec.hyperparams),
            training_metrics={
                "train_loss": self._train_loss,
                "valid_loss": self._valid_loss,
            },
            best_epoch=self._best_epoch,
            trained_steps=self._trained_steps,
            checkpoint_tag="best_validation",
            input_metadata={
                "lookback": int(self.spec.hyperparams.get("lookback", 1)),
                "modality_order": list(self._modality_order),
                "modality_feature_names": dict(self._modality_feature_names),
                "market_feature_names": list(self._market_feature_names),
                "text_feature_names": list(self._text_feature_names),
            },
            prediction_metadata={
                "confidence_source": self._confidence_source,
                "fusion_strategy": self._fusion_strategy,
                "branch_modalities": list(self._modality_order),
            },
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._fusion_strategy = str(state.get("fusion_strategy", model._fusion_strategy))
        model._modality_order = [
            str(item) for item in state.get("modality_order", []) if isinstance(item, str)
        ]
        model._modality_feature_names = {
            str(key): [str(item) for item in value if isinstance(item, str)]
            for key, value in dict(state.get("modality_feature_names", {})).items()
            if isinstance(key, str) and isinstance(value, list)
        }
        model._market_feature_names = list(state.get("market_feature_names", []))
        model._text_feature_names = list(state.get("text_feature_names", []))
        model._feature_names = list(state.get("feature_names", model._feature_names))
        if "market_branch" in state or "blender_branch" in state:
            branch_states = state.get("branch_states")
            if isinstance(branch_states, dict):
                model._branch_states = {
                    str(key): cls._deserialize_branch(value)
                    for key, value in branch_states.items()
                    if isinstance(key, str)
                }
            model._market_branch = cls._deserialize_branch(state.get("market_branch"))
            model._text_branch = cls._deserialize_branch(state.get("text_branch"))
            model._blender_branch = cls._deserialize_branch(state.get("blender_branch"))
            model._legacy_estimator = cls._deserialize_branch(state.get("legacy_estimator"))
            if not model._branch_states:
                model._branch_states = {
                    "market": model._market_branch,
                    "nlp": model._text_branch,
                }
            if model._blender_branch is not None:
                model._residual_scale = float(model._blender_branch.get("residual_scale", 1.0))
                model._confidence_source = str(
                    model._blender_branch.get("confidence_source", model._confidence_source)
                )
            return model
        model._legacy_estimator = {
            "weights": [float(value) for value in state.get("weights", [])],
            "bias": float(state.get("bias", 0.0)),
            "feature_means": [float(value) for value in state.get("feature_means", [])],
            "feature_stds": [float(value) for value in state.get("feature_stds", [])],
            "residual_scale": float(state.get("residual_scale", 1.0)),
            "confidence_source": str(state.get("confidence_source", model._confidence_source)),
        }
        model._residual_scale = float(model._legacy_estimator["residual_scale"])
        model._confidence_source = str(model._legacy_estimator["confidence_source"])
        return model

    def _build_modality_feature_matrices(self, blocks: dict[str, object]) -> dict[str, list[list[float]]]:
        modality_blocks = blocks.get("modality_blocks")
        modality_presence = self._modality_presence(blocks)
        if isinstance(modality_blocks, dict):
            feature_rows: dict[str, list[list[float]]] = {}
            for modality, values in modality_blocks.items():
                if not isinstance(modality, str) or not isinstance(values, list):
                    continue
                mask = [bool(item) for item in modality_presence.get(modality, [])]
                rows: list[list[float]] = []
                for index, row in enumerate(values):
                    raw_values = flatten_numeric(row)
                    if index < len(mask) and mask[index]:
                        rows.append(raw_values)
                    else:
                        rows.append([0.0 for _ in raw_values])
                feature_rows[modality] = rows
            if feature_rows:
                return feature_rows
        return {
            "market": self._build_market_feature_matrix(blocks),
            "nlp": self._build_text_feature_matrix(blocks),
        }

    def _modality_presence(self, blocks: dict[str, object]) -> dict[str, list[bool]]:
        values = blocks.get("modality_presence_mask")
        if isinstance(values, dict):
            return {
                str(key): [bool(item) for item in value]
                for key, value in values.items()
                if isinstance(key, str) and isinstance(value, list)
            }
        return {"nlp": [bool(value) for value in blocks.get("text_mask", [])]}

    def _build_market_feature_matrix(self, blocks: dict[str, object]) -> list[list[float]]:
        market_block = blocks.get("market_block", [])
        return [flatten_numeric(row) for row in market_block]

    def _build_text_feature_matrix(self, blocks: dict[str, object]) -> list[list[float]]:
        text_block = blocks.get("text_block", [])
        text_mask = [bool(value) for value in blocks.get("text_mask", [])]
        feature_rows: list[list[float]] = []
        for index in range(len(text_block)):
            raw_text_values = flatten_numeric(text_block[index])
            feature_rows.append(
                raw_text_values if index < len(text_mask) and text_mask[index] else [0.0 for _ in raw_text_values]
            )
        return feature_rows

    def _build_legacy_feature_matrix(self, blocks: dict[str, object]) -> list[list[float]]:
        market_block = blocks.get("market_block", [])
        text_block = blocks.get("text_block", [])
        text_mask = [bool(value) for value in blocks.get("text_mask", [])]
        feature_rows: list[list[float]] = []
        for index in range(len(market_block)):
            market_values = flatten_numeric(market_block[index])
            raw_text_values = flatten_numeric(text_block[index]) if index < len(text_block) else []
            text_values = (
                raw_text_values
                if index < len(text_mask) and text_mask[index]
                else [0.0 for _ in raw_text_values]
            )
            feature_rows.append(
                market_values
                + text_values
                + [
                    average(market_values),
                    average(text_values),
                    1.0 if index < len(text_mask) and text_mask[index] else 0.0,
                    float(len(text_values)),
                ]
            )
        return feature_rows

    def _build_blender_feature_matrix(
        self,
        branch_predictions: dict[str, list[float]],
        modality_presence: dict[str, list[bool]],
    ) -> list[list[float]]:
        branch_order = list(self._modality_order or branch_predictions.keys())
        primary_predictions = branch_predictions.get("market")
        if primary_predictions is None:
            primary_predictions = next(iter(branch_predictions.values()), [])
        feature_rows: list[list[float]] = []
        for index in range(len(primary_predictions)):
            base_prediction = float(primary_predictions[index])
            row: list[float] = []
            observed_predictions: list[float] = []
            for modality in branch_order:
                predictions = branch_predictions.get(modality, [])
                prediction = float(predictions[index]) if index < len(predictions) else 0.0
                present_mask = modality_presence.get(modality, [])
                present = bool(present_mask[index]) if index < len(present_mask) else False
                row.extend([prediction, 1.0 if present else 0.0, prediction - base_prediction])
                if present:
                    observed_predictions.append(prediction)
            row.extend(
                [
                    float(len(observed_predictions)),
                    average(observed_predictions),
                    max(observed_predictions, default=base_prediction) - min(observed_predictions, default=base_prediction),
                ]
            )
            feature_rows.append(row)
        return feature_rows

    def _predict_with_state(
        self,
        features: list[list[float]] | None,
        state: dict[str, Any] | None,
    ) -> list[float]:
        if features is None:
            return []
        if state is None:
            return [0.0 for _ in features]
        weights = list(state.get("weights", []))
        feature_means = list(state.get("feature_means", []))
        feature_stds = list(state.get("feature_stds", []))
        if not features:
            return []
        normalized = normalize_features(features, feature_means, feature_stds)
        bias = float(state.get("bias", 0.0))
        return [dot_product(weights, row) + bias for row in normalized]

    def _slice_valid_list(
        self,
        valid_input: PredictInputBundle | None,
        values: object,
    ) -> list[object] | None:
        if valid_input is None or not isinstance(values, list):
            return None
        start_index = int(valid_input.metadata.get("target_start_index", 0))
        return values[start_index:]

    @staticmethod
    def _serialize_training(training: AdvancedTrainingArtifacts) -> dict[str, Any]:
        return {
            "weights": list(training.weights),
            "bias": float(training.bias),
            "feature_means": list(training.feature_means),
            "feature_stds": list(training.feature_stds),
            "train_loss": float(training.train_loss),
            "valid_loss": float(training.valid_loss),
            "best_epoch": int(training.best_epoch),
            "trained_steps": int(training.trained_steps),
            "residual_scale": float(training.residual_scale),
            "confidence_source": training.confidence_source,
        }

    @staticmethod
    def _deserialize_branch(value: object) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        return {
            "weights": [float(item) for item in value.get("weights", [])],
            "bias": float(value.get("bias", 0.0)),
            "feature_means": [float(item) for item in value.get("feature_means", [])],
            "feature_stds": [float(item) for item in value.get("feature_stds", [])],
            "train_loss": float(value.get("train_loss", 0.0)),
            "valid_loss": float(value.get("valid_loss", 0.0)),
            "best_epoch": int(value.get("best_epoch", 0)),
            "trained_steps": int(value.get("trained_steps", 0)),
            "residual_scale": float(value.get("residual_scale", 1.0)),
            "confidence_source": str(value.get("confidence_source", "fallback_residual")),
        }
