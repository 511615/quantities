from __future__ import annotations

import json
from pathlib import Path

from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryEntry
from quant_platform.webapi.services.dataset_cleanup import DatasetCleanupService


class DummyRegistry:
    def __init__(self, entry: DatasetRegistryEntry) -> None:
        self._entry = entry
        self.removed: list[str] = []

    def get_entry(self, dataset_id: str) -> DatasetRegistryEntry | None:
        if self._entry.dataset_id == dataset_id:
            return self._entry
        return None

    def remove_dataset(self, dataset_id: str) -> None:
        self.removed.append(dataset_id)


def _write_artifacts(root: Path, dataset_id: str) -> tuple[Path, Path, Path, Path]:
    base = root / "datasets"
    base.mkdir(parents=True, exist_ok=True)
    ref = base / f"{dataset_id}_dataset_ref.json"
    manifest = base / f"{dataset_id}_dataset_manifest.json"
    samples = base / f"{dataset_id}_dataset_samples.json"
    feature_view = base / f"{dataset_id}_feature_view_ref.json"
    for path in (ref, manifest, samples, feature_view):
        path.write_text(json.dumps({"id": dataset_id}), encoding="utf-8")
    return ref, manifest, samples, feature_view


def test_cleanup_service_removes_dataset_artifacts(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    dataset_id = "workbench_experiment_123"
    ref_path, manifest_path, samples_path, feature_view_path = _write_artifacts(artifact_root, dataset_id)
    entry = DatasetRegistryEntry(
        dataset_id=dataset_id,
        ref_uri=str(ref_path),
        manifest_uri=str(manifest_path),
        samples_uri=str(samples_path),
        feature_view_uri=str(feature_view_path),
        data_domain="market",
        dataset_type="training_panel",
        source_vendor=None,
        exchange=None,
        frequency="1h",
        entity_scope=None,
        entity_count=None,
        snapshot_version=None,
        build_status="ready",
        readiness_status="ready",
        quality_status=None,
        as_of_time="2024-01-01T00:00:00Z",
        data_start_time="2024-01-01T00:00:00Z",
        data_end_time="2024-01-02T00:00:00Z",
        raw_row_count=1,
        usable_row_count=1,
        feature_count=1,
        label_count=1,
        request_origin=None,
        payload_json=json.dumps({"dataset_id": dataset_id}),
        manifest_json=json.dumps({"acquisition_profile": {"dataset_type": "training_panel"}}),
        updated_at="2024-01-02T00:00:00Z",
    )
    registry = DummyRegistry(entry)
    service = DatasetCleanupService(repository, registry)  # type: ignore[arg-type]
    deleted = service.hard_delete_dataset(dataset_id)
    assert dataset_id in registry.removed
    assert len(deleted) >= 4
    for path in (ref_path, manifest_path, samples_path, feature_view_path):
        assert not path.exists()


def test_cleanup_service_no_entry(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    registry = DummyRegistry(
        DatasetRegistryEntry(
            dataset_id="other",
            ref_uri="",
            manifest_uri=None,
            samples_uri=None,
            feature_view_uri=None,
            data_domain="market",
            dataset_type="training_panel",
            source_vendor=None,
            exchange=None,
            frequency=None,
            entity_scope=None,
            entity_count=None,
            snapshot_version=None,
            build_status=None,
            readiness_status=None,
            quality_status=None,
            as_of_time=None,
            data_start_time=None,
            data_end_time=None,
            raw_row_count=None,
            usable_row_count=None,
            feature_count=None,
            label_count=None,
            request_origin=None,
            payload_json=json.dumps({"dataset_id": "other"}),
            manifest_json=None,
            updated_at="2024-01-01T00:00:00Z",
        )
    )
    service = DatasetCleanupService(repository, registry)  # type: ignore[arg-type]
    assert service.hard_delete_dataset("missing") == []
