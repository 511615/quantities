from __future__ import annotations

from math import exp, tanh
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

try:  # pragma: no cover - depends on local environment
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    torch = None
    nn = None


if nn is not None:  # pragma: no cover - depends on local environment

    class _TorchMLPRegressor(nn.Module):
        def __init__(
            self,
            input_dim: int,
            hidden_layers: list[int],
            dropout: float,
        ) -> None:
            super().__init__()
            layers: list[nn.Module] = []
            current_dim = input_dim
            for hidden_dim in hidden_layers:
                layers.append(nn.Linear(current_dim, hidden_dim))
                layers.append(nn.ReLU())
                if dropout > 0.0:
                    layers.append(nn.Dropout(dropout))
                current_dim = hidden_dim
            layers.append(nn.Linear(current_dim, 1))
            self.network = nn.Sequential(*layers)

        def forward(self, inputs: torch.Tensor) -> torch.Tensor:
            return self.network(inputs).squeeze(-1)
else:
    _TorchMLPRegressor = None


class MLPModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.DEEP
    default_input_adapter_key = "tabular_passthrough"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._hidden_weights: list[float] = []
        self._output_scale = 1.0
        self._training_sample_count = 0
        self._feature_names = [field.name for field in spec.input_schema]
        self._backend = "fallback"
        self._feature_means: list[float] = []
        self._feature_stds: list[float] = []
        self._residual_scale = 1.0
        self._model_state: dict[str, object] | None = None
        self._backend_mode = str(self.spec.hyperparams.get("force_backend", "auto"))

    def fit(
        self,
        train_input: TrainInputBundle | list[DatasetSample],
        valid_input: PredictInputBundle | None = None,
    ) -> dict[str, float]:
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
        if self._should_use_torch():
            train_predictions = self._fit_torch(matrix, targets, valid_input)
        else:
            train_predictions = self._fit_fallback(matrix, targets)
        metrics = {
            "mae": mae(train_predictions, targets),
            "sample_count": float(len(targets)),
        }
        self._residual_scale = mae(train_predictions, targets) or 1.0
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
            bundle = build_tabular_train_bundle(self.spec, predict_input)
            predictions = self._predict_matrix(bundle.blocks["feature_matrix"])
            confidences = self._build_confidences(predictions)
            return build_prediction_frame(
                predict_input,
                predictions,
                model_run_id=model_run_id,
                confidences=confidences,
            )
        predictions = self._predict_matrix(predict_input.blocks["feature_matrix"])
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
                    "hidden_weights": self._hidden_weights,
                    "output_scale": self._output_scale,
                    "feature_means": self._feature_means,
                    "feature_stds": self._feature_stds,
                    "residual_scale": self._residual_scale,
                    "model_state": self._model_state,
                    "feature_names": self._feature_names,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._backend = str(state.get("backend", "fallback"))
        model._hidden_weights = [float(value) for value in state.get("hidden_weights", [])]
        model._output_scale = float(state.get("output_scale", 1.0))
        model._feature_means = [float(value) for value in state.get("feature_means", [])]
        model._feature_stds = [float(value) for value in state.get("feature_stds", [])]
        model._residual_scale = float(state.get("residual_scale", 1.0))
        model._model_state = state.get("model_state")
        model._feature_names = list(state.get("feature_names", model._feature_names))
        return model

    def _fit_fallback(self, matrix: list[list[float]], targets: list[float]) -> list[float]:
        self._backend = "fallback"
        self._hidden_weights = [
            average(row[index] for row in matrix) for index in range(len(self._feature_names))
        ]
        self._output_scale = average(targets) or 1.0
        self._feature_means = [
            average(row[index] for row in matrix) for index in range(len(self._feature_names))
        ]
        self._feature_stds = [1.0 for _ in self._feature_names]
        self._model_state = None
        return [self._predict_row_fallback(row) for row in matrix]

    def _fit_torch(
        self,
        matrix: list[list[float]],
        targets: list[float],
        valid_input: PredictInputBundle | None,
    ) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchMLPRegressor is not None
        input_dim = len(self._feature_names)
        hidden_layers = [
            int(value) for value in self.spec.hyperparams.get("hidden_layers", [32, 16])
        ]
        if not hidden_layers:
            hidden_layers = [32, 16]
        epochs = int(self.spec.hyperparams.get("epochs", 80))
        learning_rate = float(self.spec.hyperparams.get("learning_rate", 0.01))
        weight_decay = float(self.spec.hyperparams.get("weight_decay", 1e-4))
        dropout = float(self.spec.hyperparams.get("dropout", 0.1))
        patience = int(self.spec.hyperparams.get("patience", 10))
        min_delta = float(self.spec.hyperparams.get("min_delta", 1e-5))
        seed = int(self.spec.hyperparams.get("random_state", 7))
        torch.manual_seed(seed)
        self._feature_means = [average(row[index] for row in matrix) for index in range(input_dim)]
        self._feature_stds = []
        normalized_rows: list[list[float]] = []
        for feature_index in range(input_dim):
            column = [row[feature_index] for row in matrix]
            column_mean = self._feature_means[feature_index]
            variance = average((value - column_mean) ** 2 for value in column)
            self._feature_stds.append(max(variance**0.5, 1e-6))
        for row in matrix:
            normalized_rows.append(
                [
                    (row[index] - self._feature_means[index]) / self._feature_stds[index]
                    for index in range(input_dim)
                ]
            )
        inputs = torch.tensor(normalized_rows, dtype=torch.float32)
        labels = torch.tensor(targets, dtype=torch.float32)
        network = _TorchMLPRegressor(
            input_dim=input_dim,
            hidden_layers=hidden_layers,
            dropout=dropout,
        )
        optimizer = torch.optim.Adam(
            network.parameters(),
            lr=learning_rate,
            weight_decay=weight_decay,
        )
        loss_fn = nn.MSELoss()
        best_state = {
            key: value.detach().cpu().clone() for key, value in network.state_dict().items()
        }
        best_score = float("inf")
        stale_epochs = 0
        valid_inputs = None
        valid_labels = None
        if valid_input is not None:
            valid_matrix = valid_input.blocks["feature_matrix"]
            normalized_valid = [
                [
                    (row[index] - self._feature_means[index]) / max(self._feature_stds[index], 1e-6)
                    for index in range(input_dim)
                ]
                for row in valid_matrix
            ]
            valid_inputs = torch.tensor(normalized_valid, dtype=torch.float32)
            valid_labels = torch.tensor(
                [sample.target for sample in valid_input.source_samples],
                dtype=torch.float32,
            )
        network.train()
        for _ in range(epochs):
            optimizer.zero_grad()
            predictions = network(inputs)
            loss = loss_fn(predictions, labels)
            loss.backward()
            optimizer.step()
            network.eval()
            with torch.no_grad():
                score = float(loss_fn(network(inputs), labels).item())
                if valid_inputs is not None and valid_labels is not None:
                    score = float(loss_fn(network(valid_inputs), valid_labels).item())
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
            predictions = network(inputs).detach().cpu().tolist()
        self._backend = "torch"
        self._model_state = {
            "state_dict": {
                key: value.detach().cpu() for key, value in network.state_dict().items()
            },
            "hidden_layers": hidden_layers,
            "dropout": dropout,
        }
        self._hidden_weights = []
        self._output_scale = 1.0
        return [float(value) for value in predictions]

    def _predict_matrix(self, matrix: list[list[float]]) -> list[float]:
        if self._backend == "torch" and self._model_state is not None and torch is not None:
            return self._predict_matrix_torch(matrix)
        return [self._predict_row_fallback(row) for row in matrix]

    def _should_use_torch(self) -> bool:
        if self._backend_mode == "fallback":
            return False
        if self._backend_mode == "torch":
            return torch is not None and _TorchMLPRegressor is not None
        return torch is not None and _TorchMLPRegressor is not None

    def _predict_matrix_torch(
        self,
        matrix: list[list[float]],
    ) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchMLPRegressor is not None
        hidden_layers = [int(value) for value in self._model_state["hidden_layers"]]
        network = _TorchMLPRegressor(
            input_dim=len(self._feature_names),
            hidden_layers=hidden_layers,
            dropout=float(self._model_state.get("dropout", 0.0)),
        )
        network.load_state_dict(self._model_state["state_dict"])
        normalized_rows = [
            [
                (row[index] - self._feature_means[index]) / max(self._feature_stds[index], 1e-6)
                for index in range(len(self._feature_names))
            ]
            for row in matrix
        ]
        inputs = torch.tensor(normalized_rows, dtype=torch.float32)
        network.eval()
        with torch.no_grad():
            outputs = network(inputs).detach().cpu().tolist()
        return [float(value) for value in outputs]

    def _predict_row_fallback(self, row: list[float]) -> float:
        if not self._hidden_weights:
            return 0.0
        hidden = average(
            weight * value for weight, value in zip(self._hidden_weights, row, strict=False)
        )
        return tanh(hidden) * self._output_scale

    def _build_confidences(self, predictions: list[float]) -> list[float]:
        scale = max(self._residual_scale, 1e-6)
        return [float(1.0 - exp(-abs(prediction) / scale)) for prediction in predictions]
