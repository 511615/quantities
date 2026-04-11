from __future__ import annotations

from math import exp
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
from quant_platform.models.support import average, load_saved_state, mae, save_model_artifact

try:  # pragma: no cover - depends on local environment
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    torch = None
    nn = None


if nn is not None:  # pragma: no cover - depends on local environment

    class _TorchLSTMRegressor(nn.Module):
        def __init__(
            self,
            input_dim: int,
            hidden_dim: int,
            num_layers: int,
            dropout: float,
            bidirectional: bool,
            layer_norm: bool,
        ) -> None:
            super().__init__()
            self.lstm = nn.LSTM(
                input_size=input_dim,
                hidden_size=hidden_dim,
                num_layers=num_layers,
                dropout=dropout if num_layers > 1 else 0.0,
                bidirectional=bidirectional,
                batch_first=True,
            )
            output_dim = hidden_dim * (2 if bidirectional else 1)
            self.layer_norm = nn.LayerNorm(output_dim) if layer_norm else None
            self.projection = nn.Linear(output_dim, 1)

        def forward(self, sequence: torch.Tensor) -> torch.Tensor:
            outputs, _state = self.lstm(sequence)
            last_state = outputs[:, -1, :]
            if self.layer_norm is not None:
                last_state = self.layer_norm(last_state)
            return self.projection(last_state).squeeze(-1)
else:
    _TorchLSTMRegressor = None


def _strict_sequence_windows(
    samples: list[DatasetSample],
    lookback: int,
) -> tuple[list[list[list[float]]], list[DatasetSample]]:
    if lookback < 1:
        raise ValueError("lookback must be >= 1")
    if len(samples) < lookback:
        return [], []
    feature_names = list(samples[0].features.keys())
    windows: list[list[list[float]]] = []
    aligned_samples: list[DatasetSample] = []
    for end_index in range(lookback - 1, len(samples)):
        window = []
        for sample in samples[end_index - lookback + 1 : end_index + 1]:
            window.append(
                [float(sample.features.get(feature_name, 0.0)) for feature_name in feature_names]
            )
        windows.append(window)
        aligned_samples.append(samples[end_index])
    return windows, aligned_samples


class LSTMSequenceModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.SEQUENCE
    default_input_adapter_key = "sequence_market"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._sequence_scale = 0.0
        self._lookback = spec.lookback or int(spec.hyperparams.get("lookback", 2) or 2)
        self._training_sample_count = 0
        self._feature_names = [field.name for field in spec.input_schema]
        self._backend = "fallback"
        self._residual_scale = 1.0
        self._model_state: dict[str, object] | None = None
        self._backend_mode = str(self.spec.hyperparams.get("force_backend", "auto"))

    def fit(
        self,
        train_input: TrainInputBundle | list[DatasetSample],
        valid_input: PredictInputBundle | None = None,
    ) -> dict[str, float]:
        if isinstance(train_input, list):
            windows, aligned_samples = _strict_sequence_windows(train_input, self._lookback)
            targets = [sample.target for sample in aligned_samples]
        else:
            windows = train_input.blocks["market_block"]
            aligned_samples = train_input.source_samples
            targets = train_input.targets
            self._feature_names = train_input.feature_names
        if not targets:
            raise ValueError("training samples cannot be empty")
        self._training_sample_count = len(targets)
        if self._should_use_torch():
            predictions = self._fit_torch(windows, targets, valid_input)
        else:
            predictions = self._fit_fallback(windows, targets)
        metrics = {"mae": mae(predictions, targets), "sample_count": float(len(targets))}
        self._residual_scale = mae(predictions, targets) or 1.0
        if valid_input is not None:
            valid_outputs = self.predict(valid_input)
            metrics["valid_mae"] = mae(
                valid_outputs.predictions,
                [sample.target for sample in valid_input.source_samples],
            )
        return metrics

    def predict(
        self,
        predict_input: PredictInputBundle | list[DatasetSample],
        model_run_id: str | None = None,
    ):
        if isinstance(predict_input, list):
            if model_run_id is None:
                raise ValueError("model_run_id is required when predicting from raw samples")
            windows, aligned_samples = _strict_sequence_windows(predict_input, self._lookback)
            predictions = self._predict_windows(windows)
            return build_prediction_frame(
                aligned_samples,
                predictions,
                model_run_id=model_run_id,
                confidences=self._build_confidences(predictions),
            )
        predictions = self._predict_windows(predict_input.blocks["market_block"])
        return ModelPredictionOutputs(
            predictions=predictions,
            confidences=self._build_confidences(predictions),
        )

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "backend": self._backend,
                    "sequence_scale": self._sequence_scale,
                    "lookback": self._lookback,
                    "residual_scale": self._residual_scale,
                    "model_state": self._model_state,
                    "feature_names": self._feature_names,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
            input_adapter_key="sequence_market",
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._backend = str(state.get("backend", "fallback"))
        model._sequence_scale = float(state.get("sequence_scale", 0.0))
        model._lookback = int(state.get("lookback", model._lookback))
        model._residual_scale = float(state.get("residual_scale", 1.0))
        model._model_state = state.get("model_state")
        model._feature_names = list(state.get("feature_names", model._feature_names))
        return model

    def _fit_fallback(
        self,
        windows: list[list[list[float]]],
        targets: list[float],
    ) -> list[float]:
        self._backend = "fallback"
        horizon = int(self.spec.hyperparams.get("forecast_horizon", self.spec.target_horizon or 1))
        self._sequence_scale = average(targets) * max(1, horizon)
        self._model_state = None
        return [self._predict_window_fallback(window) for window in windows]

    def _fit_torch(
        self,
        windows: list[list[list[float]]],
        targets: list[float],
        valid_input: PredictInputBundle | None,
    ) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchLSTMRegressor is not None
        hidden_dim = int(self.spec.hyperparams.get("hidden_size", 16))
        num_layers = int(self.spec.hyperparams.get("num_layers", 1))
        dropout = float(self.spec.hyperparams.get("dropout", 0.0))
        bidirectional = bool(self.spec.hyperparams.get("bidirectional", False))
        layer_norm = bool(self.spec.hyperparams.get("layer_norm", False))
        epochs = int(self.spec.hyperparams.get("epochs", 90))
        learning_rate = float(self.spec.hyperparams.get("learning_rate", 0.01))
        patience = int(self.spec.hyperparams.get("patience", 8))
        min_delta = float(self.spec.hyperparams.get("min_delta", 1e-5))
        seed = int(self.spec.hyperparams.get("random_state", 7))
        torch.manual_seed(seed)
        sequence_tensor = torch.tensor(windows, dtype=torch.float32)
        target_tensor = torch.tensor(targets, dtype=torch.float32)
        network = _TorchLSTMRegressor(
            input_dim=len(self._feature_names),
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            dropout=dropout,
            bidirectional=bidirectional,
            layer_norm=layer_norm,
        )
        optimizer = torch.optim.Adam(network.parameters(), lr=learning_rate)
        loss_fn = nn.MSELoss()
        best_state = {
            key: value.detach().cpu().clone() for key, value in network.state_dict().items()
        }
        best_score = float("inf")
        stale_epochs = 0
        valid_sequence_tensor = None
        valid_target_tensor = None
        if valid_input is not None:
            valid_sequence_tensor = torch.tensor(
                valid_input.blocks["market_block"], dtype=torch.float32
            )
            valid_target_tensor = torch.tensor(
                [sample.target for sample in valid_input.source_samples],
                dtype=torch.float32,
            )
        network.train()
        for _ in range(epochs):
            optimizer.zero_grad()
            predictions = network(sequence_tensor)
            loss = loss_fn(predictions, target_tensor)
            loss.backward()
            optimizer.step()
            network.eval()
            with torch.no_grad():
                score = float(loss_fn(network(sequence_tensor), target_tensor).item())
                if valid_sequence_tensor is not None and valid_target_tensor is not None:
                    score = float(
                        loss_fn(network(valid_sequence_tensor), valid_target_tensor).item()
                    )
            if score + min_delta < best_score:
                best_score = score
                stale_epochs = 0
                best_state = {
                    key: value.detach().cpu().clone() for key, value in network.state_dict().items()
                }
            else:
                stale_epochs += 1
                if stale_epochs >= patience:
                    break
            network.train()
        network.load_state_dict(best_state)
        network.eval()
        with torch.no_grad():
            fitted = network(sequence_tensor).detach().cpu().tolist()
        self._backend = "torch"
        self._model_state = {
            key: value.detach().cpu().tolist() for key, value in best_state.items()
        }
        return [float(item) for item in fitted]

    def _should_use_torch(self) -> bool:
        if self._backend_mode == "fallback":
            return False
        if self._backend_mode == "torch":
            return torch is not None and _TorchLSTMRegressor is not None
        return torch is not None and _TorchLSTMRegressor is not None

    def _predict_windows(self, windows: list[list[list[float]]]) -> list[float]:
        if self._backend == "torch" and self._model_state and torch is not None and _TorchLSTMRegressor is not None:
            return self._predict_torch(windows)
        return [self._predict_window_fallback(window) for window in windows]

    def _predict_window_fallback(self, window: list[list[float]]) -> float:
        if not window:
            return 0.0
        terminal = window[-1]
        if not terminal:
            return self._sequence_scale
        signal = average(terminal)
        return self._sequence_scale + signal * 0.05

    def _predict_torch(self, windows: list[list[list[float]]]) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchLSTMRegressor is not None
        hidden_dim = int(self.spec.hyperparams.get("hidden_size", 16))
        num_layers = int(self.spec.hyperparams.get("num_layers", 1))
        dropout = float(self.spec.hyperparams.get("dropout", 0.0))
        bidirectional = bool(self.spec.hyperparams.get("bidirectional", False))
        layer_norm = bool(self.spec.hyperparams.get("layer_norm", False))
        network = _TorchLSTMRegressor(
            input_dim=len(self._feature_names),
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            dropout=dropout,
            bidirectional=bidirectional,
            layer_norm=layer_norm,
        )
        restored_state = {
            key: torch.tensor(value)
            for key, value in (self._model_state or {}).items()
        }
        network.load_state_dict(restored_state)
        network.eval()
        with torch.no_grad():
            sequence_tensor = torch.tensor(windows, dtype=torch.float32)
            outputs = network(sequence_tensor).detach().cpu().tolist()
        return [float(item) for item in outputs]

    def _build_confidences(self, predictions: list[float]) -> list[float]:
        scale = max(self._residual_scale, 1e-6)
        return [
            max(0.05, min(0.99, 1.0 / (1.0 + exp(-abs(prediction) / scale))))
            for prediction in predictions
        ]
