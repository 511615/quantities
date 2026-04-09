from __future__ import annotations

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.experiment.manifests.run_manifest import RunManifest


def test_training_manifest_captures_repro_context(facade) -> None:
    fit_result = facade.train_smoke()
    manifest = LocalArtifactStore(facade.artifact_root).read_model(
        fit_result.train_manifest_uri,
        RunManifest,
    )
    assert manifest.run_id == "smoke-train-run"
    assert manifest.repro_context.seed == 7
    assert manifest.repro_context.data_hash
    assert manifest.repro_context.config_hash
    assert manifest.repro_context.code_version == "0.1.0"
    assert manifest.dataset_id == "smoke_dataset"
    assert manifest.dataset_manifest_uri
    assert manifest.dataset_type is None
    assert manifest.data_domain is None
    assert manifest.snapshot_version
    assert manifest.entity_scope == "single_asset"
    assert manifest.entity_count == 1
    assert manifest.feature_schema_hash
    assert manifest.dataset_readiness_status == "ready"
    assert manifest.source_dataset_ids == []
    assert manifest.fusion_domains == []
    assert "dataset_freshness:stale" in manifest.dataset_readiness_warnings
