from __future__ import annotations

import math
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
    BaseModelPlugin,
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.support import (
    average,
    flatten_numeric,
    load_saved_state,
    save_model_artifact,
)

try:  # pragma: no cover - depends on local environment
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    torch = None
    nn = None


if nn is not None:  # pragma: no cover - depends on local environment

    class _ModalityEncoder(nn.Module):
        def __init__(self, input_dim: int, hidden_dim: int, dropout: float) -> None:
            super().__init__()
            self.norm = nn.LayerNorm(input_dim)
            self.encoder = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(hidden_dim, hidden_dim),
            )
            self.score_head = nn.Linear(hidden_dim, 1)

        def forward(self, inputs: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
            hidden = self.encoder(self.norm(inputs))
            score = self.score_head(hidden).squeeze(-1)
            return hidden, score


    class _AttentionGate(nn.Module):
        def __init__(self, hidden_dim: int, attention_hidden_dim: int) -> None:
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(hidden_dim + 5, attention_hidden_dim),
                nn.GELU(),
                nn.Linear(attention_hidden_dim, 1),
            )

        def forward(self, inputs: torch.Tensor) -> torch.Tensor:
            return self.network(inputs).squeeze(-1)


    class _FusionRegressor(nn.Module):
        def __init__(self, hidden_dim: int) -> None:
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(hidden_dim + 3, 32),
                nn.GELU(),
                nn.Linear(32, 1),
            )

        def forward(self, inputs: torch.Tensor) -> torch.Tensor:
            return self.network(inputs).squeeze(-1)

else:
    _ModalityEncoder = None
    _AttentionGate = None
    _FusionRegressor = None


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
        self._fusion_strategy = str(self.spec.hyperparams.get("fusion_strategy", "attention_late_fusion"))
        self._fallback_fusion_strategy = str(
            self.spec.hyperparams.get("fusion_fallback_strategy", "late_score_blend")
        )
        self._backend = self._fusion_strategy
        self._attention_state: dict[str, Any] | None = None
        self._attention_summary: dict[str, Any] = {}
        self._last_explainability_payload: dict[str, Any] | None = None
        self._available_fusion_strategies = ["attention_late_fusion", "late_score_blend"]
        self._torch_available = torch is not None and _ModalityEncoder is not None
        self._explainability_filename = "attention_explainability.json"

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
        if self._should_use_attention():
            if not self._torch_available:
                self._fusion_strategy = self._fallback_fusion_strategy
                self._backend = f"attention_unavailable_fallback:{self._fallback_fusion_strategy}"
            else:
                return self._fit_attention(train_input, valid_input)
        return self._fit_late_score_blend(train_input, valid_input)

    def predict(
        self,
        predict_input: PredictInputBundle,
        model_run_id: str | None = None,
    ) -> ModelPredictionOutputs:
        _ = model_run_id
        if self._fusion_strategy == "attention_late_fusion" and self._attention_state is not None:
            return self._predict_attention(predict_input)
        return self._predict_late_score_blend(predict_input)

    def save(self, artifact_dir: Path):
        attention_summary = dict(self._attention_summary)
        explainability_uri = str(artifact_dir / self._explainability_filename)
        if self._last_explainability_payload is not None:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            (artifact_dir / self._explainability_filename).write_text(
                __import__("json").dumps(self._last_explainability_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        prediction_metadata = {
            "confidence_source": self._confidence_source,
            "fusion_strategy": self._fusion_strategy,
            "requested_fusion_strategy": str(self.spec.hyperparams.get("fusion_strategy", self._fusion_strategy)),
            "branch_modalities": list(self._modality_order),
            "available_fusion_strategies": list(self._available_fusion_strategies),
        }
        if attention_summary:
            prediction_metadata["attention_summary"] = attention_summary
        if self._attention_state is not None or self._last_explainability_payload is not None:
            prediction_metadata["explainability_uri"] = explainability_uri
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "fusion_strategy": self._fusion_strategy,
                    "fusion_fallback_strategy": self._fallback_fusion_strategy,
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
                    "attention_state": self._attention_state,
                    "attention_summary": attention_summary,
                    "available_fusion_strategies": list(self._available_fusion_strategies),
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
            prediction_metadata=prediction_metadata,
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._fusion_strategy = str(state.get("fusion_strategy", model._fusion_strategy))
        model._fallback_fusion_strategy = str(
            state.get("fusion_fallback_strategy", model._fallback_fusion_strategy)
        )
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
        model._attention_state = state.get("attention_state")
        model._attention_summary = (
            dict(state.get("attention_summary", {}))
            if isinstance(state.get("attention_summary"), dict)
            else {}
        )
        available = state.get("available_fusion_strategies")
        if isinstance(available, list):
            model._available_fusion_strategies = [
                str(item) for item in available if isinstance(item, str)
            ] or model._available_fusion_strategies
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
        if model._attention_state is None and state.get("legacy_estimator") and model._blender_branch is None:
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

    def _fit_late_score_blend(
        self,
        train_input: TrainInputBundle,
        valid_input: PredictInputBundle | None,
    ) -> dict[str, float]:
        targets = train_input.targets
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
        self._attention_state = None
        self._attention_summary = {}
        self._last_explainability_payload = None
        self._fusion_strategy = "late_score_blend"
        if self._backend.startswith("attention_unavailable_fallback:"):
            pass
        else:
            self._backend = "late_score_blend"

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

    def _predict_late_score_blend(self, predict_input: PredictInputBundle) -> ModelPredictionOutputs:
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
            metadata["fusion_strategy"] = "late_score_blend"
            metadata["requested_fusion_strategy"] = str(
                self.spec.hyperparams.get("fusion_strategy", "late_score_blend")
            )
            metadata["branch_modalities"] = list(self._modality_order)
            metadata["market_branch_present"] = self._market_branch is not None
            metadata["text_branch_present"] = self._text_branch is not None
            metadata["available_fusion_strategies"] = list(self._available_fusion_strategies)
            return ModelPredictionOutputs(
                predictions=list(outputs.predictions),
                confidences=list(outputs.confidences),
                metadata=metadata,
            )

        legacy = self._legacy_estimator
        if legacy is None:
            raise ValueError("multimodal model has not been trained")
        features = self._build_legacy_feature_matrix(predict_input.blocks)
        outputs = predict_linear_regressor(
            features,
            weights=list(legacy.get("weights", [])),
            bias=float(legacy.get("bias", 0.0)),
            feature_means=list(legacy.get("feature_means", [])),
            feature_stds=list(legacy.get("feature_stds", [])),
            residual_scale=float(legacy.get("residual_scale", self._residual_scale)),
            confidence_source=str(legacy.get("confidence_source", self._confidence_source)),
        )
        metadata = dict(outputs.metadata or {})
        metadata["fusion_strategy"] = "late_score_blend"
        metadata["requested_fusion_strategy"] = str(
            self.spec.hyperparams.get("fusion_strategy", "late_score_blend")
        )
        metadata["available_fusion_strategies"] = list(self._available_fusion_strategies)
        return ModelPredictionOutputs(
            predictions=list(outputs.predictions),
            confidences=list(outputs.confidences),
            metadata=metadata,
        )

    def _fit_attention(
        self,
        train_input: TrainInputBundle,
        valid_input: PredictInputBundle | None,
    ) -> dict[str, float]:
        if not self._torch_available:
            raise ValueError(
                "attention_late_fusion requires torch; use fusion_strategy=late_score_blend to fall back"
            )
        assert torch is not None
        assert _ModalityEncoder is not None
        assert _AttentionGate is not None
        assert _FusionRegressor is not None

        encoder_hidden_dim = int(self.spec.hyperparams.get("encoder_hidden_dim", 64))
        attention_hidden_dim = int(self.spec.hyperparams.get("attention_hidden_dim", 32))
        dropout = float(self.spec.hyperparams.get("dropout", 0.1))
        learning_rate = float(self.spec.hyperparams.get("learning_rate", 1e-3))
        weight_decay = float(self.spec.hyperparams.get("weight_decay", 1e-4))
        epochs = max(1, int(self.spec.hyperparams.get("epochs", 24)))
        patience = max(1, int(self.spec.hyperparams.get("patience", 6)))
        min_delta = float(self.spec.hyperparams.get("min_delta", 1e-4))
        temperature = max(float(self.spec.hyperparams.get("temperature", 1.0)), 1e-6)
        seed = int(self.spec.hyperparams.get("random_state", 7))
        torch.manual_seed(seed)

        train_payload = self._prepare_attention_payload(train_input)
        valid_payload = self._prepare_attention_payload(valid_input) if valid_input is not None else None

        encoders: dict[str, _ModalityEncoder] = {
            modality: _ModalityEncoder(payload["input_dim"], encoder_hidden_dim, dropout)
            for modality, payload in train_payload["modalities"].items()
        }
        gate = _AttentionGate(encoder_hidden_dim, attention_hidden_dim)
        regressor = _FusionRegressor(encoder_hidden_dim)
        modules: list[nn.Module] = [*encoders.values(), gate, regressor]
        optimizer = torch.optim.AdamW(
            [parameter for module in modules for parameter in module.parameters()],
            lr=learning_rate,
            weight_decay=weight_decay,
        )
        loss_fn = nn.L1Loss()
        best_state: dict[str, Any] | None = None
        best_score = float("inf")
        stale_epochs = 0
        trained_steps = 0
        best_train_loss = float("inf")
        best_valid_loss = float("inf")
        best_epoch = 0

        for _epoch in range(epochs):
            for module in modules:
                module.train()
            optimizer.zero_grad()
            train_outputs = self._forward_attention(train_payload, encoders, gate, regressor, temperature)
            loss = loss_fn(train_outputs["predictions"], train_payload["targets"])
            loss.backward()
            optimizer.step()
            trained_steps += 1

            for module in modules:
                module.eval()
            with torch.no_grad():
                train_predictions = train_outputs["predictions"].detach().cpu().tolist()
                train_loss = float(loss_fn(train_outputs["predictions"], train_payload["targets"]).item())
                if valid_payload is not None:
                    valid_outputs = self._forward_attention(valid_payload, encoders, gate, regressor, temperature)
                    valid_loss = float(loss_fn(valid_outputs["predictions"], valid_payload["targets"]).item())
                else:
                    valid_outputs = None
                    valid_loss = train_loss
                if valid_loss + min_delta < best_score:
                    best_score = valid_loss
                    stale_epochs = 0
                    best_state = {
                        "encoders": {
                            modality: {
                                key: value.detach().cpu()
                                for key, value in encoder.state_dict().items()
                            }
                            for modality, encoder in encoders.items()
                        },
                        "gate": {key: value.detach().cpu() for key, value in gate.state_dict().items()},
                        "regressor": {
                            key: value.detach().cpu() for key, value in regressor.state_dict().items()
                        },
                        "temperature": temperature,
                        "encoder_hidden_dim": encoder_hidden_dim,
                        "attention_hidden_dim": attention_hidden_dim,
                        "dropout": dropout,
                    }
                    best_train_loss = train_loss
                    best_valid_loss = valid_loss
                    best_epoch = _epoch + 1
                else:
                    stale_epochs += 1
                    if stale_epochs >= patience:
                        break

        if best_state is None:
            raise ValueError("attention_late_fusion failed to produce a valid checkpoint")

        for modality, encoder in encoders.items():
            encoder.load_state_dict(best_state["encoders"][modality])
        gate.load_state_dict(best_state["gate"])
        regressor.load_state_dict(best_state["regressor"])
        for module in modules:
            module.eval()

        with torch.no_grad():
            final_train_outputs = self._forward_attention(
                train_payload, encoders, gate, regressor, temperature
            )
            train_predictions = [float(value) for value in final_train_outputs["predictions"].detach().cpu().tolist()]
            final_valid_outputs = (
                self._forward_attention(valid_payload, encoders, gate, regressor, temperature)
                if valid_payload is not None
                else None
            )
            valid_predictions = (
                [float(value) for value in final_valid_outputs["predictions"].detach().cpu().tolist()]
                if final_valid_outputs is not None
                else []
            )

        self._attention_state = {
            "encoders": best_state["encoders"],
            "gate": best_state["gate"],
            "regressor": best_state["regressor"],
            "encoder_hidden_dim": encoder_hidden_dim,
            "attention_hidden_dim": attention_hidden_dim,
            "dropout": dropout,
            "temperature": temperature,
            "input_dims": {
                modality: payload["input_dim"] for modality, payload in train_payload["modalities"].items()
            },
            "lookback": int(self.spec.hyperparams.get("lookback", 1)),
        }
        self._blender_branch = None
        self._legacy_estimator = None
        self._branch_states = {}
        self._market_branch = None
        self._text_branch = None
        self._fusion_strategy = "attention_late_fusion"
        self._backend = "attention_late_fusion"
        self._train_loss = float(best_train_loss)
        self._valid_loss = float(best_valid_loss)
        self._best_epoch = int(best_epoch)
        self._trained_steps = int(trained_steps)
        residual_predictions = valid_predictions or train_predictions
        residual_targets = (
            valid_payload["targets"].detach().cpu().tolist()
            if valid_payload is not None
            else train_payload["targets"].detach().cpu().tolist()
        )
        self._residual_scale = max(
            average(abs(pred - target) for pred, target in zip(residual_predictions, residual_targets, strict=False)),
            1e-6,
        )
        self._confidence_source = "validation_residual" if valid_payload is not None else "fallback_residual"
        self._attention_summary = self._build_attention_summary(
            final_train_outputs,
            train_payload,
            encoder_hidden_dim=encoder_hidden_dim,
            attention_hidden_dim=attention_hidden_dim,
            temperature=temperature,
        )
        self._last_explainability_payload = self._build_attention_explainability_payload(
            train_input,
            final_train_outputs,
        )
        metrics = {
            "mae": float(best_train_loss),
            "train_loss": float(best_train_loss),
            "valid_mae": float(best_valid_loss),
            "valid_loss": float(best_valid_loss),
            "best_epoch": float(best_epoch),
            "trained_steps": float(trained_steps),
            "sample_count": float(len(train_input.targets)),
        }
        avg_attention = self._attention_summary.get("average_attention")
        if isinstance(avg_attention, dict):
            for modality, value in avg_attention.items():
                metrics[f"attention_avg_{modality}"] = float(value)
        return metrics

    def _predict_attention(self, predict_input: PredictInputBundle) -> ModelPredictionOutputs:
        if self._attention_state is None:
            raise ValueError("attention_late_fusion model has not been trained")
        assert torch is not None
        assert _ModalityEncoder is not None
        assert _AttentionGate is not None
        assert _FusionRegressor is not None
        payload = self._prepare_attention_payload(predict_input)
        encoders: dict[str, _ModalityEncoder] = {}
        for modality, modality_payload in payload["modalities"].items():
            input_dim = int(self._attention_state["input_dims"][modality])
            encoder = _ModalityEncoder(
                input_dim,
                int(self._attention_state["encoder_hidden_dim"]),
                float(self._attention_state.get("dropout", 0.1)),
            )
            encoder.load_state_dict(self._attention_state["encoders"][modality])
            encoder.eval()
            encoders[modality] = encoder
        gate = _AttentionGate(
            int(self._attention_state["encoder_hidden_dim"]),
            int(self._attention_state["attention_hidden_dim"]),
        )
        gate.load_state_dict(self._attention_state["gate"])
        gate.eval()
        regressor = _FusionRegressor(int(self._attention_state["encoder_hidden_dim"]))
        regressor.load_state_dict(self._attention_state["regressor"])
        regressor.eval()
        with torch.no_grad():
            outputs = self._forward_attention(
                payload,
                encoders,
                gate,
                regressor,
                float(self._attention_state.get("temperature", 1.0)),
            )
        predictions = [float(value) for value in outputs["predictions"].detach().cpu().tolist()]
        confidences = self._build_confidences(predictions)
        attention_summary = self._build_attention_summary(
            outputs,
            payload,
            encoder_hidden_dim=int(self._attention_state["encoder_hidden_dim"]),
            attention_hidden_dim=int(self._attention_state["attention_hidden_dim"]),
            temperature=float(self._attention_state.get("temperature", 1.0)),
        )
        self._last_explainability_payload = self._build_attention_explainability_payload(
            predict_input,
            outputs,
        )
        self._attention_summary = attention_summary
        metadata = {
            "confidence_source": self._confidence_source,
            "fusion_strategy": "attention_late_fusion",
            "branch_modalities": list(self._modality_order),
            "available_fusion_strategies": list(self._available_fusion_strategies),
            "attention_summary": attention_summary,
            "explainability_uri": self._explainability_filename,
        }
        return ModelPredictionOutputs(
            predictions=predictions,
            confidences=confidences,
            metadata=metadata,
        )

    def _prepare_attention_payload(
        self,
        bundle: TrainInputBundle | PredictInputBundle | None,
    ) -> dict[str, Any] | None:
        if bundle is None:
            return None
        assert torch is not None
        modality_blocks = self._build_modality_feature_matrices(bundle.blocks)
        modality_presence_mask = self._modality_presence(bundle.blocks)
        modality_coverage_ratio = bundle.blocks.get("modality_coverage_ratio", {})
        payload_modalities: dict[str, dict[str, Any]] = {}
        branch_order = list(self._modality_order or modality_blocks.keys())
        for modality in branch_order:
            rows = modality_blocks.get(modality)
            if rows is None:
                continue
            normalized_rows = [flatten_numeric(row) for row in rows]
            if not normalized_rows:
                continue
            input_dim = len(normalized_rows[0]) if normalized_rows[0] else 1
            features = []
            for row in normalized_rows:
                if not row:
                    features.append([0.0] * input_dim)
                elif len(row) < input_dim:
                    features.append(row + ([0.0] * (input_dim - len(row))))
                else:
                    features.append(row[:input_dim])
            payload_modalities[modality] = {
                "features": torch.tensor(features, dtype=torch.float32),
                "present": torch.tensor(
                    [1.0 if item else 0.0 for item in modality_presence_mask.get(modality, [False] * len(features))],
                    dtype=torch.float32,
                ),
                "coverage_ratio": torch.tensor(
                    [float(item) for item in modality_coverage_ratio.get(modality, [0.0] * len(features))],
                    dtype=torch.float32,
                ),
                "feature_count_norm": torch.tensor(
                    [min(1.0, len(self._modality_feature_names.get(modality, [])) / max(1.0, len(self._feature_names)))]
                    * len(features),
                    dtype=torch.float32,
                ),
                "input_dim": input_dim,
            }
        targets = None
        if isinstance(bundle, TrainInputBundle):
            targets = torch.tensor([float(value) for value in bundle.targets], dtype=torch.float32)
        elif bundle.source_samples:
            targets = torch.tensor([float(sample.target) for sample in bundle.source_samples], dtype=torch.float32)
        return {
            "modalities": payload_modalities,
            "targets": targets,
            "sample_count": len(bundle.source_samples),
            "source_samples": bundle.source_samples,
        }

    def _forward_attention(
        self,
        payload: dict[str, Any],
        encoders: dict[str, Any],
        gate,
        regressor,
        temperature: float,
    ) -> dict[str, Any]:
        assert torch is not None
        hidden_by_modality: dict[str, torch.Tensor] = {}
        score_by_modality: dict[str, torch.Tensor] = {}
        modalities = list(payload["modalities"].keys())
        sample_count = payload["sample_count"]
        for modality in modalities:
            hidden, score = encoders[modality](payload["modalities"][modality]["features"])
            hidden_by_modality[modality] = hidden
            score_by_modality[modality] = score
        market_scores = score_by_modality.get("market")
        if market_scores is None:
            market_scores = next(iter(score_by_modality.values()))
        logits: list[torch.Tensor] = []
        presence_stack: list[torch.Tensor] = []
        for modality in modalities:
            present = payload["modalities"][modality]["present"]
            coverage_ratio = payload["modalities"][modality]["coverage_ratio"]
            feature_count_norm = payload["modalities"][modality]["feature_count_norm"]
            score = score_by_modality[modality]
            score_delta_to_market = score - market_scores
            gate_inputs = torch.cat(
                [
                    hidden_by_modality[modality],
                    score.unsqueeze(-1),
                    present.unsqueeze(-1),
                    coverage_ratio.unsqueeze(-1),
                    feature_count_norm.unsqueeze(-1),
                    score_delta_to_market.unsqueeze(-1),
                ],
                dim=1,
            )
            modality_logits = gate(gate_inputs)
            masked_logits = torch.where(
                present > 0.0,
                modality_logits / temperature,
                torch.full_like(modality_logits, -1e9),
            )
            logits.append(masked_logits)
            presence_stack.append(present)
        logits_tensor = torch.stack(logits, dim=1)
        present_tensor = torch.stack(presence_stack, dim=1)
        attention = torch.softmax(logits_tensor, dim=1)
        attention = attention * present_tensor
        attention = attention / attention.sum(dim=1, keepdim=True).clamp_min(1e-6)

        hidden_stack = torch.stack([hidden_by_modality[modality] for modality in modalities], dim=1)
        score_stack = torch.stack([score_by_modality[modality] for modality in modalities], dim=1)
        fused_hidden = (attention.unsqueeze(-1) * hidden_stack).sum(dim=1)
        weighted_score = (attention * score_stack).sum(dim=1)
        attention_entropy = -(
            attention * torch.log(attention.clamp_min(1e-6))
        ).sum(dim=1)
        observed_modality_count = present_tensor.sum(dim=1)
        final_inputs = torch.cat(
            [
                fused_hidden,
                weighted_score.unsqueeze(-1),
                attention_entropy.unsqueeze(-1),
                observed_modality_count.unsqueeze(-1),
            ],
            dim=1,
        )
        predictions = regressor(final_inputs)
        return {
            "predictions": predictions,
            "attention": attention,
            "weighted_score": weighted_score,
            "attention_entropy": attention_entropy,
            "observed_modality_count": observed_modality_count,
            "modalities": modalities,
            "present_tensor": present_tensor,
            "score_stack": score_stack,
        }

    def _build_attention_summary(
        self,
        outputs: dict[str, Any],
        payload: dict[str, Any],
        *,
        encoder_hidden_dim: int,
        attention_hidden_dim: int,
        temperature: float,
    ) -> dict[str, Any]:
        attention = outputs["attention"].detach().cpu()
        present_tensor = outputs["present_tensor"].detach().cpu()
        average_attention: dict[str, float] = {}
        for index, modality in enumerate(outputs["modalities"]):
            average_attention[modality] = float(attention[:, index].mean().item())
        return {
            "average_attention": average_attention,
            "average_entropy": float(outputs["attention_entropy"].detach().cpu().mean().item()),
            "average_observed_modality_count": float(
                outputs["observed_modality_count"].detach().cpu().mean().item()
            ),
            "sample_count": payload["sample_count"],
            "encoder_hidden_dim": encoder_hidden_dim,
            "attention_hidden_dim": attention_hidden_dim,
            "temperature": temperature,
            "missing_modality_rate": {
                modality: float(1.0 - present_tensor[:, index].mean().item())
                for index, modality in enumerate(outputs["modalities"])
            },
        }

    def _build_attention_explainability_payload(
        self,
        bundle: TrainInputBundle | PredictInputBundle,
        outputs: dict[str, Any],
    ) -> dict[str, Any]:
        attention = outputs["attention"].detach().cpu().tolist()
        weighted_score = outputs["weighted_score"].detach().cpu().tolist()
        entropy = outputs["attention_entropy"].detach().cpu().tolist()
        observed_count = outputs["observed_modality_count"].detach().cpu().tolist()
        predictions = outputs["predictions"].detach().cpu().tolist()
        rows: list[dict[str, Any]] = []
        for index, sample in enumerate(bundle.source_samples):
            rows.append(
                {
                    "entity_key": sample.entity_key,
                    "timestamp": sample.timestamp.isoformat(),
                    "available_time": sample.available_time.isoformat(),
                    "prediction": float(predictions[index]),
                    "weighted_score": float(weighted_score[index]),
                    "attention_entropy": float(entropy[index]),
                    "observed_modality_count": float(observed_count[index]),
                    "attention_weights": {
                        modality: float(attention[index][modality_index])
                        for modality_index, modality in enumerate(outputs["modalities"])
                    },
                }
            )
        return {
            "fusion_strategy": "attention_late_fusion",
            "modalities": list(outputs["modalities"]),
            "rows": rows,
        }

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
                        rows.append([0.0 for _ in raw_values] if raw_values else [0.0])
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
            if raw_text_values:
                feature_rows.append(
                    raw_text_values
                    if index < len(text_mask) and text_mask[index]
                    else [0.0 for _ in raw_text_values]
                )
            else:
                feature_rows.append([0.0])
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
                    max(observed_predictions, default=base_prediction)
                    - min(observed_predictions, default=base_prediction),
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

    def _should_use_attention(self) -> bool:
        return self._fusion_strategy == "attention_late_fusion"

    def _build_confidences(self, predictions: list[float]) -> list[float]:
        scale = max(self._residual_scale, 1e-6)
        return [float(1.0 - math.exp(-abs(prediction) / scale)) for prediction in predictions]

    def explainability_payload(self) -> dict[str, object] | None:
        if self._last_explainability_payload is None:
            return None
        return dict(self._last_explainability_payload)
