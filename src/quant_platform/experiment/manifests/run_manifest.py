from __future__ import annotations

from datetime import datetime, timezone

from pydantic import Field

from quant_platform.common.types.core import ArtifactRef, FrozenModel


class ReproContext(FrozenModel):
    config_hash: str
    data_hash: str
    code_version: str
    seed: int
    dependency_lock_hash: str | None = None


class RunManifest(FrozenModel):
    run_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    repro_context: ReproContext
    dataset_ref_uri: str
    dataset_id: str | None = None
    dataset_manifest_uri: str | None = None
    dataset_type: str | None = None
    data_domain: str | None = None
    data_domains: list[str] = Field(default_factory=list)
    snapshot_version: str | None = None
    entity_scope: str | None = None
    entity_count: int | None = None
    feature_schema_hash: str | None = None
    dataset_readiness_status: str | None = None
    dataset_readiness_warnings: list[str] = Field(default_factory=list)
    source_dataset_ids: list[str] = Field(default_factory=list)
    fusion_domains: list[str] = Field(default_factory=list)
    model_artifact: ArtifactRef
    metrics: dict[str, float]
