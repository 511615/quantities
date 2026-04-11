from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import (
    DatasetRegistryEntry,
    DatasetRegistryRepository,
)


class DatasetCleanupService:
    def __init__(
        self,
        repository: ArtifactRepository,
        dataset_registry: DatasetRegistryRepository,
    ) -> None:
        self.repository = repository
        self.dataset_registry = dataset_registry

    def hard_delete_dataset(self, dataset_id: str) -> list[str]:
        entry = self.dataset_registry.get_entry(dataset_id)
        if entry is None:
            return []
        deleted_files: list[str] = []
        seen: set[Path] = set()
        for path in self._candidate_paths(entry):
            resolved = path.resolve()
            if resolved in seen or not resolved.exists():
                continue
            seen.add(resolved)
            if not self._is_within_artifacts(resolved):
                continue
            if resolved.is_dir():
                shutil.rmtree(resolved, ignore_errors=True)
            else:
                resolved.unlink(missing_ok=True)
            deleted_files.append(self.repository.display_uri(resolved))
        self.dataset_registry.remove_dataset(dataset_id)
        return sorted(deleted_files)

    def _candidate_paths(self, entry: DatasetRegistryEntry) -> Iterable[Path]:
        dataset_dir = self.repository.artifact_root / "datasets"
        yield from dataset_dir.glob(f"{entry.dataset_id}_*.json")
        yield from dataset_dir.glob(f"{entry.dataset_id}.json")
        for uri in [
            entry.ref_uri,
            entry.manifest_uri,
            entry.samples_uri,
            entry.feature_view_uri,
            self._feature_view_storage_uri(entry),
        ]:
            if isinstance(uri, str) and uri:
                yield self.repository.resolve_uri(uri)

    def _feature_view_storage_uri(self, entry: DatasetRegistryEntry) -> str | None:
        payload = entry.payload
        return (
            (payload.get("feature_view_ref") or {}).get("storage_uri")
            or (payload.get("feature_view_ref") or {}).get("feature_view_uri")
        )

    def _is_within_artifacts(self, path: Path) -> bool:
        try:
            path.relative_to(self.repository.artifact_root)
            return True
        except ValueError:
            return False
