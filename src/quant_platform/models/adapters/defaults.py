from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.adapters.base import (
    ArtifactAdapter,
    CapabilityValidator,
    ModelInputAdapter,
    PredictionAdapter,
)
from quant_platform.models.contracts import (
    BaseModelPlugin,
    ModelArtifactMeta,
    ModelPredictionOutputs,
    ModelRegistration,
    ModelSpec,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.inference.prediction_frame import build_prediction_frame
from quant_platform.training.contracts.training import (
    PredictionFrame,
    PredictionMetadata,
    PredictionScope,
)


def _feature_names(samples: list[DatasetSample], spec: ModelSpec) -> list[str]:
    if samples:
        return list(samples[0].features.keys())
    return [field.name for field in spec.input_schema]


def _feature_matrix(samples: list[DatasetSample], feature_names: list[str]) -> list[list[float]]:
    return [[float(sample.features.get(name, 0.0)) for name in feature_names] for sample in samples]


def _targets(samples: list[DatasetSample]) -> list[float]:
    return [float(sample.target) for sample in samples]


def _resolve_lookback(spec: ModelSpec) -> int:
    raw = spec.lookback or int(spec.hyperparams.get("lookback", 1) or 1)
    return max(1, raw)


def _window_slice(matrix: list[list[float]], end_index: int, lookback: int) -> list[list[float]]:
    start_index = max(0, end_index - lookback + 1)
    window = matrix[start_index : end_index + 1]
    if len(window) == lookback:
        return window
    feature_dim = len(matrix[0]) if matrix else 0
    pad = [[0.0] * feature_dim for _ in range(lookback - len(window))]
    return pad + window


def _split_modal_features(
    feature_names: list[str],
    spec: ModelSpec,
) -> tuple[list[str], list[str]]:
    text_prefixes = tuple(
        spec.hyperparams.get("text_feature_prefixes", ["text_", "sentiment_", "news_"])
    )
    market_names: list[str] = []
    text_names: list[str] = []
    for feature_name in feature_names:
        if feature_name.startswith(text_prefixes):
            text_names.append(feature_name)
        else:
            market_names.append(feature_name)
    return market_names, text_names


class TabularInputAdapter(ModelInputAdapter):
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        feature_names = _feature_names(samples, spec)
        return TrainInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            targets=_targets(samples),
            blocks={"feature_matrix": _feature_matrix(samples, feature_names)},
            metadata={"registration": registration.model_name},
        )

    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        feature_names = _feature_names(samples, spec)
        return PredictInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            blocks={"feature_matrix": _feature_matrix(samples, feature_names)},
            metadata={"registration": registration.model_name},
        )


class SequenceMarketInputAdapter(ModelInputAdapter):
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        predict_bundle = self.build_predict_input(samples, dataset_ref, spec, registration)
        return TrainInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=predict_bundle.feature_names,
            targets=_targets(samples),
            blocks=dict(predict_bundle.blocks),
            metadata=dict(predict_bundle.metadata),
        )

    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        feature_names = _feature_names(samples, spec)
        matrix = _feature_matrix(samples, feature_names)
        lookback = _resolve_lookback(spec)
        market_block = [_window_slice(matrix, index, lookback) for index in range(len(samples))]
        return PredictInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            blocks={"market_block": market_block, "lookback": lookback},
            metadata={"registration": registration.model_name},
        )


class TemporalFusionInputAdapter(ModelInputAdapter):
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        predict_bundle = self.build_predict_input(samples, dataset_ref, spec, registration)
        return TrainInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=predict_bundle.feature_names,
            targets=_targets(samples),
            blocks=dict(predict_bundle.blocks),
            metadata=dict(predict_bundle.metadata),
        )

    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        feature_names = _feature_names(samples, spec)
        matrix = _feature_matrix(samples, feature_names)
        lookback = _resolve_lookback(spec)
        static_names = set(spec.hyperparams.get("static_feature_names", []))
        future_names = set(spec.hyperparams.get("future_feature_names", []))
        static_indices = [index for index, name in enumerate(feature_names) if name in static_names]
        future_indices = [index for index, name in enumerate(feature_names) if name in future_names]
        observed_indices = [
            index
            for index, name in enumerate(feature_names)
            if name not in static_names and name not in future_names
        ]
        observed_past = []
        known_future = []
        static_context = []
        for index in range(len(samples)):
            window = _window_slice(matrix, index, lookback)
            observed_past.append([[row[col] for col in observed_indices] for row in window])
            known_future.append(
                [[row[col] for col in future_indices] for row in window] if future_indices else []
            )
            static_context.append(
                [matrix[index][col] for col in static_indices] if static_indices else []
            )
        return PredictInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            blocks={
                "observed_past": observed_past,
                "known_future": known_future,
                "static_context": static_context,
                "lookback": lookback,
            },
            metadata={"registration": registration.model_name},
        )


class PatchSequenceInputAdapter(ModelInputAdapter):
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        predict_bundle = self.build_predict_input(samples, dataset_ref, spec, registration)
        return TrainInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=predict_bundle.feature_names,
            targets=_targets(samples),
            blocks=dict(predict_bundle.blocks),
            metadata=dict(predict_bundle.metadata),
        )

    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        feature_names = _feature_names(samples, spec)
        matrix = _feature_matrix(samples, feature_names)
        lookback = _resolve_lookback(spec)
        patch_size = max(1, int(spec.hyperparams.get("patch_size", 2)))
        sequence_block = [_window_slice(matrix, index, lookback) for index in range(len(samples))]
        patch_block = []
        patch_index = []
        for window in sequence_block:
            patches = [
                window[start : start + patch_size] for start in range(0, len(window), patch_size)
            ]
            patch_block.append(patches)
            patch_index.append(list(range(len(patches))))
        return PredictInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            blocks={
                "sequence_block": sequence_block,
                "patch_block": patch_block,
                "patch_index": patch_index,
                "patch_size": patch_size,
            },
            metadata={"registration": registration.model_name},
        )


class MarketTextAlignedInputAdapter(ModelInputAdapter):
    def build_train_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> TrainInputBundle:
        predict_bundle = self.build_predict_input(samples, dataset_ref, spec, registration)
        return TrainInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=predict_bundle.feature_names,
            targets=_targets(samples),
            blocks=dict(predict_bundle.blocks),
            metadata=dict(predict_bundle.metadata),
        )

    def build_predict_input(
        self,
        samples: list[DatasetSample],
        dataset_ref: DatasetRef,
        spec: ModelSpec,
        registration: ModelRegistration,
    ) -> PredictInputBundle:
        feature_names = _feature_names(samples, spec)
        market_names, text_names = _split_modal_features(feature_names, spec)
        market_matrix = [
            [float(sample.features.get(name, 0.0)) for name in market_names] for sample in samples
        ]
        text_matrix = [
            [float(sample.features.get(name, 0.0)) for name in text_names] for sample in samples
        ]
        lookback = _resolve_lookback(spec)
        market_block = [
            _window_slice(market_matrix, index, lookback) for index in range(len(samples))
        ]
        text_block = (
            [_window_slice(text_matrix, index, lookback) for index in range(len(samples))]
            if text_names
            else [[] for _ in samples]
        )
        text_mask = (
            [bool(text_names and any(abs(value) > 0 for value in row)) for row in text_matrix]
            if text_names
            else [False for _ in samples]
        )
        return PredictInputBundle(
            dataset_ref=dataset_ref,
            model_spec=spec,
            source_samples=samples,
            feature_names=feature_names,
            blocks={
                "market_block": market_block,
                "text_block": text_block,
                "text_mask": text_mask,
                "market_feature_names": market_names,
                "text_feature_names": text_names,
                "lookback": lookback,
            },
            metadata={"registration": registration.model_name},
        )


class StandardPredictionAdapter(PredictionAdapter):
    def build_prediction_frame(
        self,
        raw_output: ModelPredictionOutputs | PredictionFrame,
        predict_input: PredictInputBundle,
        *,
        model_run_id: str,
        prediction_scope: PredictionScope | None = None,
    ) -> PredictionFrame:
        if isinstance(raw_output, PredictionFrame):
            return raw_output
        frame = build_prediction_frame(
            predict_input.source_samples,
            raw_output.predictions,
            model_run_id=model_run_id,
            confidences=raw_output.confidences,
        )
        return PredictionFrame(
            rows=frame.rows,
            metadata=PredictionMetadata(
                feature_view_ref=predict_input.dataset_ref.feature_view_ref,
                prediction_time=prediction_scope.as_of_time if prediction_scope else None,
                target_horizon=predict_input.model_spec.target_horizon,
            ),
        )


class DefaultArtifactAdapter(ArtifactAdapter):
    def save_model(
        self,
        plugin: BaseModelPlugin,
        *,
        run_id: str,
        artifact_root: Path,
        registration: ModelRegistration,
        train_input: TrainInputBundle,
    ) -> ModelArtifactMeta:
        artifact_dir = artifact_root / "models" / run_id
        meta = plugin.save(artifact_dir)
        if meta.run_id != run_id:
            raise ValueError("model artifact run_id must match fit request run_id")
        if meta.registration.model_name != registration.model_name:
            raise ValueError("model artifact registration does not match selected model")
        return meta

    def load_model(
        self,
        model_cls: type[BaseModelPlugin],
        *,
        spec: ModelSpec,
        artifact_meta: ModelArtifactMeta,
        artifact_store: LocalArtifactStore,
    ) -> BaseModelPlugin:
        _ = artifact_store
        return model_cls.load(spec, artifact_meta.artifact_directory)


class DefaultCapabilityValidator(CapabilityValidator):
    def validate(
        self,
        registration: ModelRegistration,
        dataset_ref: DatasetRef,
        bundle: TrainInputBundle | PredictInputBundle,
    ) -> None:
        if not bundle.source_samples:
            raise ValueError(f"model '{registration.model_name}' requires at least one sample")
        if "sequence_input" in registration.capabilities:
            lookback = int(bundle.blocks.get("lookback", 1))
            if lookback < 1:
                raise ValueError("sequence_input capability requires lookback >= 1")
        if "aligned_text" in registration.capabilities:
            text_names = bundle.blocks.get("text_feature_names", [])
            if text_names:
                offending = [
                    sample
                    for sample in bundle.source_samples
                    if sample.available_time > sample.timestamp
                ]
                if offending:
                    raise ValueError("text_block available_time cannot exceed sample timestamp")
            elif "allow_missing_text" not in registration.capabilities:
                raise ValueError(
                    f"model '{registration.model_name}' requires aligned text features"
                )
        if any(
            sample.available_time > dataset_ref.feature_view_ref.as_of_time
            for sample in bundle.source_samples
        ):
            raise ValueError("bundle contains samples that exceed feature view as_of_time")


def build_default_adapters() -> dict[str, Any]:
    return {
        "artifact_adapters": {"json_manifest": DefaultArtifactAdapter()},
        "capability_validator": DefaultCapabilityValidator(),
        "input_adapters": {
            "tabular_passthrough": TabularInputAdapter(),
            "sequence_market": SequenceMarketInputAdapter(),
            "temporal_fusion": TemporalFusionInputAdapter(),
            "patch_sequence": PatchSequenceInputAdapter(),
            "market_text_aligned": MarketTextAlignedInputAdapter(),
        },
        "prediction_adapters": {"standard_prediction": StandardPredictionAdapter()},
    }
