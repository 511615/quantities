from __future__ import annotations

from pydantic import Field

from quant_platform.common.types.core import FrozenModel


class DatasetBuildManifest(FrozenModel):
    dataset_id: str
    asset_id: str
    feature_set_id: str
    label_horizon: int
    sample_count: int
    dropped_rows: int
    split_strategy: str
    snapshot_version: str | None = None
    entity_scope: str = "single_asset"
    entity_count: int = 1
    input_asset_ids: list[str] = Field(default_factory=list)
    usable_sample_count: int = 0
    raw_row_count: int = 0
    feature_schema_hash: str | None = None
    label_schema_hash: str | None = None
    readiness_status: str = "unknown"
    build_status: str = "success"
    alignment_status: str = "unknown"
    missing_feature_status: str = "unknown"
    label_alignment_status: str = "unknown"
    split_integrity_status: str = "unknown"
    temporal_safety_status: str = "unknown"
    freshness_status: str = "unknown"
    quality_status: str = "unknown"
    build_config: dict[str, object] = Field(default_factory=dict)
    acquisition_profile: dict[str, object] = Field(default_factory=dict)
