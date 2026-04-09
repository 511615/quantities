from __future__ import annotations

import math
import random
from dataclasses import dataclass

from quant_platform.models.contracts import ModelPredictionOutputs, PredictInputBundle
from quant_platform.models.support import average, mae


@dataclass(frozen=True)
class AdvancedTrainingArtifacts:
    weights: list[float]
    bias: float
    feature_means: list[float]
    feature_stds: list[float]
    train_loss: float
    valid_loss: float
    best_epoch: int
    trained_steps: int
    residual_scale: float
    confidence_source: str


def resolve_training_config(hyperparams: dict[str, object]) -> dict[str, float | int | bool]:
    return {
        "epochs": max(1, int(hyperparams.get("epochs", 24))),
        "batch_size": max(1, int(hyperparams.get("batch_size", 8))),
        "learning_rate": float(hyperparams.get("learning_rate", 0.03)),
        "weight_decay": float(hyperparams.get("weight_decay", 1e-4)),
        "patience": max(1, int(hyperparams.get("patience", 6))),
        "min_delta": float(hyperparams.get("min_delta", 1e-4)),
        "random_state": int(hyperparams.get("random_state", 7)),
        "deterministic": bool(hyperparams.get("deterministic", True)),
    }


def fit_linear_regressor(
    train_features: list[list[float]],
    train_targets: list[float],
    *,
    valid_features: list[list[float]] | None = None,
    valid_targets: list[float] | None = None,
    hyperparams: dict[str, object],
) -> AdvancedTrainingArtifacts:
    if not train_targets:
        raise ValueError("training samples cannot be empty")
    if not train_features:
        raise ValueError("training features cannot be empty")
    feature_dim = len(train_features[0])
    training = resolve_training_config(hyperparams)
    feature_means = [average(row[index] for row in train_features) for index in range(feature_dim)]
    feature_stds = []
    for index in range(feature_dim):
        variance = average((row[index] - feature_means[index]) ** 2 for row in train_features)
        feature_stds.append(max(variance**0.5, 1e-6))
    normalized_train = normalize_features(train_features, feature_means, feature_stds)
    normalized_valid = (
        normalize_features(valid_features or [], feature_means, feature_stds)
        if valid_features is not None
        else None
    )
    rng = random.Random(int(training["random_state"]))
    weights = [rng.uniform(-0.05, 0.05) for _ in range(feature_dim)]
    bias = rng.uniform(-0.05, 0.05)
    best_weights = list(weights)
    best_bias = bias
    best_epoch = 0
    best_score = float("inf")
    stale_epochs = 0
    trained_steps = 0
    epochs = int(training["epochs"])
    batch_size = int(training["batch_size"])
    learning_rate = float(training["learning_rate"])
    weight_decay = float(training["weight_decay"])
    patience = int(training["patience"])
    min_delta = float(training["min_delta"])
    for epoch in range(epochs):
        indices = list(range(len(normalized_train)))
        rng.shuffle(indices)
        for batch_start in range(0, len(indices), batch_size):
            batch_indices = indices[batch_start : batch_start + batch_size]
            batch_features = [normalized_train[index] for index in batch_indices]
            batch_targets = [train_targets[index] for index in batch_indices]
            predictions = [
                dot_product(weights, feature_row) + bias for feature_row in batch_features
            ]
            errors = [
                prediction - target
                for prediction, target in zip(predictions, batch_targets, strict=False)
            ]
            scale = 2.0 / max(1, len(batch_indices))
            for feature_index in range(feature_dim):
                gradient = scale * sum(
                    errors[row_index] * batch_features[row_index][feature_index]
                    for row_index in range(len(batch_indices))
                )
                gradient += 2.0 * weight_decay * weights[feature_index]
                weights[feature_index] -= learning_rate * gradient
            bias -= learning_rate * scale * sum(errors)
            trained_steps += 1
        train_predictions = [
            dot_product(weights, feature_row) + bias for feature_row in normalized_train
        ]
        current_train_loss = mae(train_predictions, train_targets)
        if normalized_valid is not None and valid_targets is not None:
            score_predictions = [
                dot_product(weights, feature_row) + bias for feature_row in normalized_valid
            ]
            current_valid_loss = mae(score_predictions, valid_targets)
        else:
            current_valid_loss = current_train_loss
        if current_valid_loss + min_delta < best_score:
            best_score = current_valid_loss
            best_weights = list(weights)
            best_bias = bias
            best_epoch = epoch + 1
            stale_epochs = 0
        else:
            stale_epochs += 1
            if stale_epochs >= patience:
                break
    final_train_predictions = [
        dot_product(best_weights, feature_row) + best_bias for feature_row in normalized_train
    ]
    if normalized_valid is not None and valid_targets is not None:
        final_valid_predictions = [
            dot_product(best_weights, feature_row) + best_bias for feature_row in normalized_valid
        ]
        valid_loss = mae(final_valid_predictions, valid_targets)
        residual_scale = mae(final_valid_predictions, valid_targets) or 1.0
        confidence_source = "validation_residual"
    else:
        valid_loss = mae(final_train_predictions, train_targets)
        residual_scale = mae(final_train_predictions, train_targets) or 1.0
        confidence_source = "fallback_residual"
    return AdvancedTrainingArtifacts(
        weights=best_weights,
        bias=best_bias,
        feature_means=feature_means,
        feature_stds=feature_stds,
        train_loss=mae(final_train_predictions, train_targets),
        valid_loss=valid_loss,
        best_epoch=best_epoch or 1,
        trained_steps=trained_steps,
        residual_scale=residual_scale,
        confidence_source=confidence_source,
    )


def normalize_features(
    features: list[list[float]],
    feature_means: list[float],
    feature_stds: list[float],
) -> list[list[float]]:
    return [
        [
            (row[index] - feature_means[index]) / max(feature_stds[index], 1e-6)
            for index in range(len(feature_means))
        ]
        for row in features
    ]


def predict_linear_regressor(
    features: list[list[float]],
    *,
    weights: list[float],
    bias: float,
    feature_means: list[float],
    feature_stds: list[float],
    residual_scale: float,
    confidence_source: str,
) -> ModelPredictionOutputs:
    normalized = normalize_features(features, feature_means, feature_stds)
    predictions = [dot_product(weights, feature_row) + bias for feature_row in normalized]
    return ModelPredictionOutputs(
        predictions=[float(value) for value in predictions],
        confidences=build_confidences(predictions, residual_scale),
        metadata={"confidence_source": confidence_source},
    )


def build_valid_targets(valid_input: PredictInputBundle | None) -> list[float] | None:
    if valid_input is None:
        return None
    start_index = int(valid_input.metadata.get("target_start_index", 0))
    return [float(sample.target) for sample in valid_input.source_samples[start_index:]]


def slice_valid_features(
    valid_input: PredictInputBundle | None,
    features: list[list[float]] | None,
) -> list[list[float]] | None:
    if valid_input is None or features is None:
        return None
    start_index = int(valid_input.metadata.get("target_start_index", 0))
    return features[start_index:]


def dot_product(left: list[float], right: list[float]) -> float:
    return sum(lhs * rhs for lhs, rhs in zip(left, right, strict=False))


def build_confidences(predictions: list[float], residual_scale: float) -> list[float]:
    scale = max(residual_scale, 1e-6)
    return [float(1.0 - math.exp(-abs(prediction) / scale)) for prediction in predictions]
