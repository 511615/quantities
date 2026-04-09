from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC
from pathlib import Path
from statistics import mean
from typing import Any

from quant_platform.common.enums.core import ModelFamily
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.contracts import (
    AdvancedModelKind,
    ModelArtifactMeta,
    ModelPredictionOutputs,
    ModelRegistration,
    ModelSpec,
    PredictInputBundle,
    TrainInputBundle,
)
from quant_platform.models.inference.prediction_frame import build_prediction_frame
from quant_platform.models.serialization.artifact import read_artifact_state, write_artifact_bundle
from quant_platform.training.contracts.training import PredictionFrame, TrainerConfig


def average(values: Iterable[float]) -> float:
    resolved = [float(value) for value in values]
    return mean(resolved) if resolved else 0.0


def mae(predictions: Iterable[float], targets: Iterable[float]) -> float:
    paired = list(zip(predictions, targets, strict=False))
    if not paired:
        return 0.0
    return average(abs(prediction - target) for prediction, target in paired)


def flatten_numeric(values: Any) -> list[float]:
    if isinstance(values, (int, float)):
        return [float(values)]
    if isinstance(values, list):
        flattened: list[float] = []
        for value in values:
            flattened.extend(flatten_numeric(value))
        return flattened
    return []


def merge_training_hyperparams(
    spec: ModelSpec,
    trainer_config: TrainerConfig,
    *,
    seed: int,
) -> ModelSpec:
    hyperparams = dict(spec.hyperparams)
    hyperparams.setdefault("epochs", trainer_config.epochs)
    hyperparams.setdefault("batch_size", trainer_config.batch_size)
    hyperparams.setdefault("deterministic", trainer_config.deterministic)
    hyperparams.setdefault("random_state", seed)
    hyperparams.setdefault("patience", max(3, trainer_config.epochs // 4))
    hyperparams.setdefault("min_delta", 1e-4)
    return spec.model_copy(update={"hyperparams": hyperparams})


def default_registration(
    spec: ModelSpec,
    *,
    advanced_kind: AdvancedModelKind,
    input_adapter_key: str = "tabular_passthrough",
) -> ModelRegistration:
    return ModelRegistration(
        model_name=spec.model_name,
        family=spec.family,
        advanced_kind=advanced_kind,
        entrypoint=f"{__name__}.{spec.model_name}",
        input_adapter_key=input_adapter_key,
        prediction_adapter_key="standard_prediction",
        artifact_adapter_key="json_manifest",
        default_hyperparams=dict(spec.hyperparams),
    )


def build_tabular_train_bundle(spec: ModelSpec, samples: list[DatasetSample]) -> TrainInputBundle:
    if not samples:
        raise ValueError("training samples cannot be empty")
    feature_names = list(samples[0].features.keys())
    return TrainInputBundle(
        dataset_ref=_placeholder_dataset_ref(),
        model_spec=spec,
        source_samples=samples,
        feature_names=feature_names,
        targets=[float(sample.target) for sample in samples],
        blocks={
            "feature_matrix": [
                [float(sample.features.get(feature_name, 0.0)) for feature_name in feature_names]
                for sample in samples
            ]
        },
    )


def build_sequence_predict_frame(
    samples: list[DatasetSample],
    predictions: list[float],
    *,
    model_run_id: str,
    start_index: int = 0,
    confidences: list[float] | None = None,
) -> PredictionFrame:
    return build_prediction_frame(
        samples[start_index:],
        predictions,
        model_run_id=model_run_id,
        confidences=confidences,
    )


def bundle_to_prediction_frame(
    bundle: PredictInputBundle,
    outputs: ModelPredictionOutputs,
    *,
    model_run_id: str,
) -> PredictionFrame:
    return build_prediction_frame(
        bundle.source_samples,
        outputs.predictions,
        model_run_id=model_run_id,
        confidences=outputs.confidences,
    )


def load_saved_state(artifact_dir: Path) -> dict[str, Any]:
    meta_path = artifact_dir / "metadata.json"
    return read_artifact_state(meta_path)


def save_model_artifact(
    *,
    artifact_dir: Path,
    run_id: str,
    spec: ModelSpec,
    advanced_kind: AdvancedModelKind,
    state: dict[str, Any],
    training_sample_count: int,
    feature_names: list[str],
    input_adapter_key: str = "tabular_passthrough",
    training_config: dict[str, object] | None = None,
    training_metrics: dict[str, float] | None = None,
    best_epoch: int | None = None,
    trained_steps: int | None = None,
    checkpoint_tag: str | None = None,
    input_metadata: dict[str, object] | None = None,
    prediction_metadata: dict[str, object] | None = None,
) -> ModelArtifactMeta:
    registration = default_registration(
        spec, advanced_kind=advanced_kind, input_adapter_key=input_adapter_key
    )
    meta = ModelArtifactMeta(
        run_id=run_id,
        model_name=spec.model_name,
        model_family=spec.family.value
        if isinstance(spec.family, ModelFamily)
        else str(spec.family),
        advanced_kind=advanced_kind,
        model_spec=spec,
        registration=registration,
        artifact_uri=str(artifact_dir / "metadata.json"),
        artifact_dir=str(artifact_dir),
        state_uri=str(artifact_dir / "state.pkl"),
        backend="pickle_bundle",
        training_sample_count=training_sample_count,
        feature_names=feature_names,
        training_config=training_config or {},
        training_metrics=training_metrics or {},
        best_epoch=best_epoch,
        trained_steps=trained_steps,
        checkpoint_tag=checkpoint_tag,
        input_metadata=input_metadata or {},
        prediction_metadata=prediction_metadata or {},
    )
    return write_artifact_bundle(artifact_dir, meta=meta, state=state)


def _placeholder_dataset_ref() -> DatasetRef:
    from datetime import datetime

    from quant_platform.common.enums.core import LabelKind, SplitStrategy
    from quant_platform.common.types.core import FeatureField, TimeRange
    from quant_platform.data.contracts.data_asset import DataAssetRef
    from quant_platform.datasets.contracts.dataset import (
        DatasetRef,
        LabelSpec,
        SamplePolicy,
        SplitManifest,
    )
    from quant_platform.features.contracts.feature_view import FeatureViewRef

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    feature_view_ref = FeatureViewRef(
        feature_set_id="compat_placeholder",
        input_data_refs=[
            DataAssetRef(
                asset_id="compat_placeholder",
                schema_version=1,
                source="compat",
                symbol="COMPAT",
                venue="compat",
                frequency="1h",
                time_range=TimeRange(start=epoch, end=epoch.replace(year=1970, month=1, day=2)),
                storage_uri="memory://compat_placeholder",
                content_hash="compat",
            )
        ],
        as_of_time=epoch,
        feature_schema=[FeatureField(name="compat", dtype="float", lineage_source="compat")],
        build_config_hash="compat",
        storage_uri="memory://compat_placeholder",
    )
    return DatasetRef(
        dataset_id="compat_placeholder",
        feature_view_ref=feature_view_ref,
        label_spec=LabelSpec(target_column="target", horizon=1, kind=LabelKind.REGRESSION),
        split_manifest=SplitManifest(
            strategy=SplitStrategy.TIME_SERIES,
            train_range=TimeRange(start=epoch, end=epoch.replace(year=1971)),
            valid_range=TimeRange(start=epoch.replace(year=1971), end=epoch.replace(year=1972)),
            test_range=TimeRange(start=epoch.replace(year=1972), end=epoch.replace(year=1973)),
        ),
        sample_policy=SamplePolicy(),
        dataset_hash="compat",
    )
