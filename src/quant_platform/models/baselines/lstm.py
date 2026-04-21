from __future__ import annotations

from math import exp
from pathlib import Path
from typing import Any

from quant_platform.common.enums.core import ModelFamily
from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.models.contracts import (
    AdvancedModelKind,
    BaseModelPlugin,
    ModelPredictionOutputs,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.inference.prediction_frame import build_prediction_frame
from quant_platform.models.support import average, load_saved_state, mae, save_model_artifact
from quant_platform.training.contracts.training import PredictionFrame, PredictionMetadata
from train.splits.rolling import (
    RollingWindowSpec,
    generate_rolling_windows,
)

try:  # pragma: no cover - depends on local environment
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    torch = None
    nn = None


if nn is not None:  # pragma: no cover - depends on local environment

    class _TorchHierarchicalLSTMRegressor(nn.Module):
        def __init__(
            self,
            input_dim: int,
            subsequence_hidden_dim: int,
            hidden_dim: int,
            num_layers: int,
            dropout: float,
            bidirectional: bool,
            layer_norm: bool,
        ) -> None:
            super().__init__()
            self.subsequence_encoder = nn.LSTM(
                input_size=input_dim,
                hidden_size=subsequence_hidden_dim,
                num_layers=1,
                dropout=0.0,
                bidirectional=False,
                batch_first=True,
            )
            self.sequence_lstm = nn.LSTM(
                input_size=subsequence_hidden_dim,
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
            batch_size, subsequence_count, subsequence_length, feature_dim = sequence.shape
            reshaped = sequence.reshape(batch_size * subsequence_count, subsequence_length, feature_dim)
            encoded, _ = self.subsequence_encoder(reshaped)
            subsequence_embedding = encoded[:, -1, :].reshape(batch_size, subsequence_count, -1)
            outputs, _state = self.sequence_lstm(subsequence_embedding)
            last_state = outputs[:, -1, :]
            if self.layer_norm is not None:
                last_state = self.layer_norm(last_state)
            return self.projection(last_state).squeeze(-1)
else:
    _TorchHierarchicalLSTMRegressor = None


def _feature_names_from_samples(
    samples: list[DatasetSample],
    explicit_feature_names: list[str] | None = None,
) -> list[str]:
    if explicit_feature_names:
        return list(explicit_feature_names)
    if not samples:
        return []
    return list(samples[0].features.keys())


def _build_dense_windows(
    samples: list[DatasetSample],
    feature_names: list[str],
    lookback: int,
) -> tuple[list[list[list[float]]], list[DatasetSample]]:
    if lookback < 1:
        raise ValueError("lookback must be >= 1")
    if not samples:
        return [], []
    feature_dim = len(feature_names)
    history_by_entity: dict[str, list[list[float]]] = {}
    windows: list[list[list[float]]] = []
    aligned_samples: list[DatasetSample] = []
    for sample in samples:
        row = [float(sample.features.get(feature_name, 0.0)) for feature_name in feature_names]
        history = history_by_entity.setdefault(sample.entity_key, [])
        observed = history[-(lookback - 1) :] + [row]
        padded = ([[0.0] * feature_dim for _ in range(max(0, lookback - len(observed)))]) + observed
        windows.append(padded)
        aligned_samples.append(sample)
        history.append(row)
    return windows, aligned_samples


def _build_supervised_windows(
    windows: list[list[list[float]]],
    samples: list[DatasetSample],
    *,
    forecast_horizon: int,
    stride: int,
    target_start_index: int = 0,
) -> tuple[list[list[list[float]]], list[DatasetSample]]:
    if stride < 1:
        raise ValueError("stride must be >= 1")
    if forecast_horizon < 1:
        raise ValueError("forecast_horizon must be >= 1")
    if not windows or not samples:
        return [], []
    selected_windows: list[list[list[float]]] = []
    selected_samples: list[DatasetSample] = []
    for label_index in range(max(forecast_horizon - 1, target_start_index), len(samples), stride):
        history_end_index = label_index - (forecast_horizon - 1)
        if history_end_index < 0 or history_end_index >= len(windows):
            continue
        selected_windows.append(windows[history_end_index])
        selected_samples.append(samples[label_index])
    return selected_windows, selected_samples


def _split_window_into_subsequences(
    window: list[list[float]],
    *,
    subsequence_length: int,
    subsequence_stride: int,
) -> list[list[list[float]]]:
    if not window:
        return []
    if subsequence_length < 1 or subsequence_stride < 1:
        raise ValueError("subsequence_length and subsequence_stride must be >= 1")
    if subsequence_length >= len(window):
        return [window]
    subsequences: list[list[list[float]]] = []
    for start_index in range(0, len(window) - subsequence_length + 1, subsequence_stride):
        subsequences.append(window[start_index : start_index + subsequence_length])
    if not subsequences:
        return [window]
    if subsequences[-1][-1] != window[-1]:
        subsequences.append(window[-subsequence_length:])
    return subsequences


def _hierarchical_windows(
    windows: list[list[list[float]]],
    *,
    subsequence_length: int,
    subsequence_stride: int,
) -> tuple[list[list[list[list[float]]]], int]:
    hierarchical: list[list[list[list[float]]]] = []
    max_subsequence_count = 0
    for window in windows:
        subsequences = _split_window_into_subsequences(
            window,
            subsequence_length=subsequence_length,
            subsequence_stride=subsequence_stride,
        )
        hierarchical.append(subsequences)
        max_subsequence_count = max(max_subsequence_count, len(subsequences))
    if not hierarchical:
        return [], 0
    padded: list[list[list[list[float]]]] = []
    pad_subsequence = [[0.0 for _ in step] for step in hierarchical[0][0]]
    for subsequences in hierarchical:
        if len(subsequences) < max_subsequence_count:
            subsequences = subsequences + [pad_subsequence for _ in range(max_subsequence_count - len(subsequences))]
        padded.append(subsequences)
    return padded, max_subsequence_count


def _effective_subsequence_layout(
    *,
    lookback: int,
    subsequence_length: int,
    subsequence_stride: int,
    subsequence_count: int,
) -> dict[str, int | bool]:
    return {
        "lookback": lookback,
        "subsequence_length": subsequence_length,
        "subsequence_stride": subsequence_stride,
        "subsequence_count": subsequence_count,
        "single_subsequence": subsequence_count <= 1,
        "tail_aligned": subsequence_count > 1 and subsequence_length < lookback,
    }


def _resolve_rolling_window_spec(
    hyperparams: dict[str, object],
    total_examples: int,
) -> dict[str, int | bool]:
    raw = hyperparams.get("rolling_window_spec")
    if not isinstance(raw, dict):
        raw = {}
    train_size = int(raw.get("train_size", max(2, total_examples // 2)) or max(2, total_examples // 2))
    valid_size = int(raw.get("valid_size", max(1, total_examples // 4)) or max(1, total_examples // 4))
    test_size = int(raw.get("test_size", max(1, total_examples // 4)) or max(1, total_examples // 4))
    step_size = int(raw.get("step_size", max(1, valid_size)) or max(1, valid_size))
    min_train_size = int(raw.get("min_train_size", max(1, min(train_size, total_examples))) or max(1, min(train_size, total_examples)))
    embargo = int(raw.get("embargo", 0) or 0)
    purge_gap = int(raw.get("purge_gap", 0) or 0)
    expanding_train = bool(raw.get("expanding_train", True))
    return {
        "train_size": train_size,
        "valid_size": valid_size,
        "test_size": test_size,
        "step_size": step_size,
        "min_train_size": min_train_size,
        "embargo": embargo,
        "purge_gap": purge_gap,
        "expanding_train": expanding_train,
    }


def _rolling_window_metadata(
    total_examples: int,
    rolling_window_spec: dict[str, int | bool],
) -> dict[str, object]:
    if total_examples <= 0:
        return {"window_count": 0, "windows": [], **rolling_window_spec}
    try:
        plans = generate_rolling_windows(
            total_examples,
            RollingWindowSpec(
                train_size=int(rolling_window_spec["train_size"]),
                valid_size=int(rolling_window_spec["valid_size"]),
                test_size=int(rolling_window_spec["test_size"]),
                step_size=int(rolling_window_spec["step_size"]),
                min_train_size=int(rolling_window_spec["min_train_size"]),
                embargo=int(rolling_window_spec["embargo"]),
                purge_gap=int(rolling_window_spec["purge_gap"]),
                expanding_train=bool(rolling_window_spec["expanding_train"]),
            ),
        )
    except Exception:
        return {"window_count": 0, "windows": [], **rolling_window_spec}
    return {
        **rolling_window_spec,
        "window_count": len(plans),
        "windows": [plan.to_dict() for plan in plans],
    }


class LSTMSequenceModel(BaseModelPlugin):
    advanced_kind = AdvancedModelKind.BASELINE
    default_family = ModelFamily.SEQUENCE
    default_input_adapter_key = "sequence_market"

    def __init__(self, spec) -> None:
        super().__init__(spec)
        self._lookback = spec.lookback or int(spec.hyperparams.get("lookback", 6) or 6)
        self._forecast_horizon = int(
            spec.hyperparams.get("forecast_horizon", spec.target_horizon or 1) or (spec.target_horizon or 1)
        )
        self._stride = int(spec.hyperparams.get("stride", 1) or 1)
        self._subsequence_length = int(
            spec.hyperparams.get("subsequence_length", max(1, self._lookback // 2)) or max(1, self._lookback // 2)
        )
        self._subsequence_stride = int(
            spec.hyperparams.get("subsequence_stride", self._subsequence_length) or self._subsequence_length
        )
        self._training_sample_count = 0
        self._feature_names = [field.name for field in spec.input_schema]
        self._backend = "fallback"
        self._residual_scale = 1.0
        self._model_state: dict[str, object] | None = None
        self._backend_mode = str(self.spec.hyperparams.get("force_backend", "auto"))
        self._window_spec = {
            "lookback": self._lookback,
            "forecast_horizon": self._forecast_horizon,
            "stride": self._stride,
            "label_alignment": "dataset_target_at_label_timestamp_with_horizon_offset",
        }
        self._subsequence_spec = {
            "subsequence_length": self._subsequence_length,
            "subsequence_stride": self._subsequence_stride,
        }
        self._rolling_window_spec = _resolve_rolling_window_spec(self.spec.hyperparams, 0)
        self._effective_subsequence_layout = _effective_subsequence_layout(
            lookback=self._lookback,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
            subsequence_count=1,
        )

    def fit(
        self,
        train_input: TrainInputBundle | list[DatasetSample],
        valid_input: PredictInputBundle | None = None,
    ) -> dict[str, float]:
        samples = train_input if isinstance(train_input, list) else train_input.source_samples
        feature_names = (
            _feature_names_from_samples(samples, self._feature_names)
            if isinstance(train_input, list)
            else list(train_input.feature_names)
        )
        self._feature_names = feature_names
        dense_windows, aligned_samples = _build_dense_windows(samples, feature_names, self._lookback)
        train_windows, train_aligned_samples = _build_supervised_windows(
            dense_windows,
            aligned_samples,
            forecast_horizon=self._forecast_horizon,
            stride=self._stride,
        )
        train_targets = [sample.target for sample in train_aligned_samples]
        if not train_targets:
            raise ValueError("training samples cannot be empty")
        valid_windows: list[list[list[float]]] = []
        valid_targets: list[float] = []
        if valid_input is not None:
            valid_target_start_index = int(valid_input.metadata.get("target_start_index", 0) or 0)
            valid_dense_windows, valid_aligned_samples = _build_dense_windows(
                valid_input.source_samples,
                list(valid_input.feature_names),
                self._lookback,
            )
            valid_windows, valid_selected_samples = _build_supervised_windows(
                valid_dense_windows,
                valid_aligned_samples,
                forecast_horizon=self._forecast_horizon,
                stride=self._stride,
                target_start_index=valid_target_start_index,
            )
            valid_targets = [sample.target for sample in valid_selected_samples]
        self._training_sample_count = len(train_targets)
        self._rolling_window_spec = _resolve_rolling_window_spec(
            self.spec.hyperparams,
            len(train_targets) + len(valid_targets),
        )
        rolling_metadata = _rolling_window_metadata(
            len(train_targets) + len(valid_targets),
            self._rolling_window_spec,
        )
        if self._should_use_torch():
            predictions = self._fit_torch(train_windows, train_targets, valid_windows, valid_targets)
        else:
            predictions = self._fit_fallback(train_windows, train_targets)
        metrics = {
            "mae": mae(predictions, train_targets),
            "sample_count": float(len(train_targets)),
            "rolling_window_count": float(rolling_metadata.get("window_count", 0)),
        }
        self._residual_scale = mae(predictions, train_targets) or 1.0
        if valid_windows and valid_targets:
            valid_predictions = self._predict_windows(valid_windows)
            metrics["valid_mae"] = mae(valid_predictions, valid_targets)
        return metrics

    def predict(
        self,
        predict_input: PredictInputBundle | list[DatasetSample],
        model_run_id: str | None = None,
    ):
        samples = predict_input if isinstance(predict_input, list) else predict_input.source_samples
        feature_names = (
            _feature_names_from_samples(samples, self._feature_names)
            if isinstance(predict_input, list)
            else list(predict_input.feature_names)
        )
        dense_windows, aligned_samples = _build_dense_windows(samples, feature_names, self._lookback)
        target_start_index = 0
        if not isinstance(predict_input, list):
            target_start_index = int(predict_input.metadata.get("target_start_index", 0) or 0)
        supervised_windows, label_samples = _build_supervised_windows(
            dense_windows,
            aligned_samples,
            forecast_horizon=self._forecast_horizon,
            stride=self._stride,
            target_start_index=target_start_index,
        )
        predictions = self._predict_windows(supervised_windows)
        confidences = self._build_confidences(predictions)
        raw_frame = build_prediction_frame(
            label_samples,
            predictions,
            model_run_id=model_run_id or "lstm",
            confidences=confidences,
        )
        prediction_frame = PredictionFrame(
            rows=raw_frame.rows,
            metadata=PredictionMetadata(
                prediction_time=label_samples[-1].timestamp if label_samples else None,
                inference_latency_ms=0,
                target_horizon=max(1, self._forecast_horizon),
            ),
        )
        if isinstance(predict_input, list):
            if model_run_id is None:
                raise ValueError("model_run_id is required when predicting from raw samples")
            return prediction_frame
        return prediction_frame

    def save(self, artifact_dir: Path):
        return save_model_artifact(
            artifact_dir=artifact_dir,
            run_id=artifact_dir.name,
            spec=self.spec,
            advanced_kind=self.advanced_kind,
            state={
                "estimator": {
                    "backend": self._backend,
                    "lookback": self._lookback,
                    "forecast_horizon": self._forecast_horizon,
                    "stride": self._stride,
                    "subsequence_length": self._subsequence_length,
                    "subsequence_stride": self._subsequence_stride,
                    "rolling_window_spec": self._rolling_window_spec,
                    "residual_scale": self._residual_scale,
                    "model_state": self._model_state,
                    "feature_names": self._feature_names,
                    "effective_subsequence_layout": self._effective_subsequence_layout,
                }
            },
            training_sample_count=self._training_sample_count,
            feature_names=self._feature_names,
            input_adapter_key="sequence_market",
            input_metadata={
                "window_spec": dict(self._window_spec),
                "subsequence_spec": dict(self._subsequence_spec),
                "rolling_window_spec": dict(self._rolling_window_spec),
                "effective_subsequence_layout": dict(self._effective_subsequence_layout),
                "effective_alignment_policy": "sequence_market_clock",
                "feature_frequency_profile": {"market": "1h"},
            },
            prediction_metadata={
                "window_spec": dict(self._window_spec),
                "subsequence_spec": dict(self._subsequence_spec),
                "rolling_window_spec": dict(self._rolling_window_spec),
                "effective_subsequence_layout": dict(self._effective_subsequence_layout),
                "effective_alignment_policy": "sequence_market_clock",
                "feature_frequency_profile": {"market": "1h"},
            },
        )

    @classmethod
    def load(cls, spec, artifact_dir: Path):
        state = load_saved_state(artifact_dir)["estimator"]
        model = cls(spec)
        model._backend = str(state.get("backend", "fallback"))
        model._lookback = int(state.get("lookback", model._lookback))
        model._forecast_horizon = int(state.get("forecast_horizon", model._forecast_horizon))
        model._stride = int(state.get("stride", model._stride))
        model._subsequence_length = int(state.get("subsequence_length", model._subsequence_length))
        model._subsequence_stride = int(state.get("subsequence_stride", model._subsequence_stride))
        rolling_spec = state.get("rolling_window_spec")
        if isinstance(rolling_spec, dict):
            model._rolling_window_spec = dict(rolling_spec)
        model._residual_scale = float(state.get("residual_scale", 1.0))
        model._model_state = state.get("model_state")
        model._feature_names = list(state.get("feature_names", model._feature_names))
        effective_layout = state.get("effective_subsequence_layout")
        if isinstance(effective_layout, dict):
            model._effective_subsequence_layout = dict(effective_layout)
        model._window_spec = {
            "lookback": model._lookback,
            "forecast_horizon": model._forecast_horizon,
            "stride": model._stride,
            "label_alignment": "dataset_target_at_label_timestamp_with_horizon_offset",
        }
        model._subsequence_spec = {
            "subsequence_length": model._subsequence_length,
            "subsequence_stride": model._subsequence_stride,
        }
        if not isinstance(effective_layout, dict):
            model._effective_subsequence_layout = _effective_subsequence_layout(
                lookback=model._lookback,
                subsequence_length=model._subsequence_length,
                subsequence_stride=model._subsequence_stride,
                subsequence_count=1,
            )
        return model

    def _fit_fallback(
        self,
        windows: list[list[list[float]]],
        targets: list[float],
    ) -> list[float]:
        self._backend = "fallback"
        horizon = int(self._window_spec["forecast_horizon"])
        subsequences, subsequence_count = _hierarchical_windows(
            windows,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
        )
        self._model_state = {
            "sequence_scale": average(targets) * max(1, horizon),
            "subsequence_count": subsequence_count,
        }
        self._effective_subsequence_layout = _effective_subsequence_layout(
            lookback=self._lookback,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
            subsequence_count=subsequence_count,
        )
        return [self._predict_window_fallback(window) for window in subsequences]

    def _fit_torch(
        self,
        windows: list[list[list[float]]],
        targets: list[float],
        valid_windows: list[list[list[float]]],
        valid_targets: list[float],
    ) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchHierarchicalLSTMRegressor is not None
        subsequences, subsequence_count = _hierarchical_windows(
            windows,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
        )
        valid_subsequences, _ = _hierarchical_windows(
            valid_windows,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
        )
        hidden_dim = int(self.spec.hyperparams.get("hidden_size", 16))
        subsequence_hidden_dim = int(
            self.spec.hyperparams.get("subsequence_hidden_size", max(4, hidden_dim // 2))
        )
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
        sequence_tensor = torch.tensor(subsequences, dtype=torch.float32)
        target_tensor = torch.tensor(targets, dtype=torch.float32)
        network = _TorchHierarchicalLSTMRegressor(
            input_dim=len(self._feature_names),
            subsequence_hidden_dim=subsequence_hidden_dim,
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
        valid_sequence_tensor = (
            torch.tensor(valid_subsequences, dtype=torch.float32) if valid_subsequences else None
        )
        valid_target_tensor = torch.tensor(valid_targets, dtype=torch.float32) if valid_targets else None
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
                    score = float(loss_fn(network(valid_sequence_tensor), valid_target_tensor).item())
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
            "state_dict": {
                key: value.detach().cpu().tolist() for key, value in best_state.items()
            },
            "hidden_dim": hidden_dim,
            "subsequence_hidden_dim": subsequence_hidden_dim,
            "num_layers": num_layers,
            "dropout": dropout,
            "bidirectional": bidirectional,
            "layer_norm": layer_norm,
            "subsequence_count": subsequence_count,
        }
        self._effective_subsequence_layout = _effective_subsequence_layout(
            lookback=self._lookback,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
            subsequence_count=subsequence_count,
        )
        return [float(item) for item in fitted]

    def _should_use_torch(self) -> bool:
        if self._backend_mode == "fallback":
            return False
        if self._backend_mode == "torch":
            return torch is not None and _TorchHierarchicalLSTMRegressor is not None
        return torch is not None and _TorchHierarchicalLSTMRegressor is not None

    def _predict_windows(self, windows: list[list[list[float]]]) -> list[float]:
        if self._backend == "torch" and self._model_state and torch is not None and _TorchHierarchicalLSTMRegressor is not None:
            return self._predict_torch(windows)
        subsequences, _ = _hierarchical_windows(
            windows,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
        )
        return [self._predict_window_fallback(window) for window in subsequences]

    def _predict_window_fallback(self, window: list[list[list[float]]]) -> float:
        if not window:
            return 0.0
        subsequence_means = [average(average(step) for step in subsequence) for subsequence in window if subsequence]
        scale = float((self._model_state or {}).get("sequence_scale", 0.0))
        return average(subsequence_means) + scale * 0.05

    def _predict_torch(self, windows: list[list[list[float]]]) -> list[float]:  # pragma: no cover - depends on local environment
        assert torch is not None
        assert _TorchHierarchicalLSTMRegressor is not None
        subsequences, _ = _hierarchical_windows(
            windows,
            subsequence_length=self._subsequence_length,
            subsequence_stride=self._subsequence_stride,
        )
        network = _TorchHierarchicalLSTMRegressor(
            input_dim=len(self._feature_names),
            subsequence_hidden_dim=int(self._model_state["subsequence_hidden_dim"]),
            hidden_dim=int(self._model_state["hidden_dim"]),
            num_layers=int(self._model_state["num_layers"]),
            dropout=float(self._model_state.get("dropout", 0.0)),
            bidirectional=bool(self._model_state.get("bidirectional", False)),
            layer_norm=bool(self._model_state.get("layer_norm", False)),
        )
        restored_state = {
            key: torch.tensor(value)
            for key, value in ((self._model_state or {}).get("state_dict") or {}).items()
        }
        network.load_state_dict(restored_state)
        network.eval()
        with torch.no_grad():
            sequence_tensor = torch.tensor(subsequences, dtype=torch.float32)
            outputs = network(sequence_tensor).detach().cpu().tolist()
        return [float(item) for item in outputs]

    def _build_confidences(self, predictions: list[float]) -> list[float]:
        scale = max(self._residual_scale, 1e-6)
        return [
            max(0.05, min(0.99, 1.0 / (1.0 + exp(-abs(prediction) / scale))))
            for prediction in predictions
        ]
