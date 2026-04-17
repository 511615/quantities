from __future__ import annotations

import sqlite3
import shutil
from pathlib import Path

from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryRepository


def test_connect_recreates_registry_directory(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    registry = DatasetRegistryRepository(artifact_root, repository)

    shutil.rmtree(registry.registry_root)

    with registry._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT 1").fetchone()

    assert row is not None
    assert registry.registry_root.exists()


def test_connect_recovers_from_malformed_registry_database(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    registry = DatasetRegistryRepository(artifact_root, repository)

    registry.db_path.write_text("not-a-valid-sqlite-db", encoding="utf-8")

    with registry._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()

    assert row


def test_recoverable_error_classifier_includes_integrity_check_failures() -> None:
    exc = sqlite3.DatabaseError("database integrity check failed: corrupted page tree")

    assert DatasetRegistryRepository._is_recoverable_db_error(exc) is True  # noqa: SLF001


def test_bootstrap_from_artifacts_skips_reconnect_when_signature_is_unchanged(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    registry = DatasetRegistryRepository(artifact_root, repository)

    registry.bootstrap_from_artifacts()
    registry._connect = lambda: (_ for _ in ()).throw(AssertionError("should not reconnect"))  # type: ignore[method-assign]

    registry.bootstrap_from_artifacts()
