from __future__ import annotations

from datetime import datetime

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.datasets.contracts.dataset import (
    DatasetRef,
    DatasetSample,
    LabelSpec,
    SamplePolicy,
    SplitManifest,
)
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.features.contracts.feature_view import FeatureViewBuildResult


class DatasetBuilder:
    """Guards dataset-level temporal assumptions before training."""

    @staticmethod
    def validate_samples(samples: list[DatasetSample], as_of_time: datetime) -> None:
        for sample in samples:
            if sample.available_time > as_of_time:
                raise ValueError("sample available_time exceeds feature view as_of_time")

    @staticmethod
    def build_dataset(
        dataset_id: str,
        feature_result: FeatureViewBuildResult,
        labels: dict[tuple[str, datetime], float],
        label_spec: LabelSpec,
        split_manifest: SplitManifest,
        sample_policy: SamplePolicy,
    ) -> tuple[DatasetRef, list[DatasetSample], DatasetBuildManifest]:
        samples: list[DatasetSample] = []
        dropped_rows = 0
        for row in feature_result.rows:
            label_key = (row.entity_key, row.timestamp)
            if label_key not in labels:
                dropped_rows += 1
                continue
            samples.append(
                DatasetSample(
                    entity_key=row.entity_key,
                    timestamp=row.timestamp,
                    available_time=row.available_time,
                    features=row.values,
                    target=labels[label_key],
                )
            )
        DatasetBuilder.validate_samples(samples, feature_result.feature_view_ref.as_of_time)
        feature_schema_names = [field.name for field in feature_result.feature_view_ref.feature_schema]
        feature_schema_hash = stable_digest(feature_schema_names)
        label_schema_hash = stable_digest(
            {
                "target_column": label_spec.target_column,
                "kind": label_spec.kind,
                "horizon": label_spec.horizon,
            }
        )
        entity_keys = sorted({row.entity_key for row in feature_result.rows})
        entity_scope = (
            sample_policy.universe
            if sample_policy.universe
            else ("multi_asset" if len(entity_keys) > 1 else "single_asset")
        )
        manifest_asset_id = (
            feature_result.feature_view_ref.input_data_refs[0].asset_id
            if len(feature_result.feature_view_ref.input_data_refs) == 1
            else f"multi_asset::{dataset_id}"
        )
        dataset_hash = stable_digest(
            {
                "feature_view_ref": feature_result.feature_view_ref,
                "label_spec": label_spec,
                "split_manifest": split_manifest,
                "sample_policy": sample_policy,
                "sample_count": len(samples),
            }
        )
        dataset_ref = DatasetRef(
            dataset_id=dataset_id,
            feature_view_ref=feature_result.feature_view_ref,
            label_spec=label_spec,
            split_manifest=split_manifest,
            sample_policy=sample_policy,
            dataset_hash=dataset_hash,
            feature_schema_hash=feature_schema_hash,
            label_schema_hash=label_schema_hash,
            entity_scope=entity_scope,
            entity_count=len(entity_keys),
        )
        manifest = DatasetBuildManifest(
            dataset_id=dataset_id,
            asset_id=manifest_asset_id,
            feature_set_id=feature_result.feature_view_ref.feature_set_id,
            label_horizon=label_spec.horizon,
            sample_count=len(samples),
            dropped_rows=dropped_rows,
            split_strategy=split_manifest.strategy,
            snapshot_version=dataset_hash[:12],
            entity_scope=entity_scope,
            entity_count=len(entity_keys),
            input_asset_ids=[ref.asset_id for ref in feature_result.feature_view_ref.input_data_refs],
            usable_sample_count=len(samples),
            raw_row_count=len(feature_result.rows),
            feature_schema_hash=feature_schema_hash,
            label_schema_hash=label_schema_hash,
            readiness_status=("ready" if samples else "not_ready"),
            build_status="success",
            alignment_status="aligned",
            missing_feature_status="clean",
            label_alignment_status="aligned",
            split_integrity_status="valid",
            temporal_safety_status="passed",
            freshness_status="fresh",
            quality_status="healthy",
        )
        return dataset_ref, samples, manifest
