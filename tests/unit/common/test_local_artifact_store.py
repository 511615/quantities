from __future__ import annotations

from pathlib import Path

from quant_platform.common.io.files import LocalArtifactStore


def test_local_artifact_store_reads_windows_style_relative_uri(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    store = LocalArtifactStore(artifact_root)
    target = artifact_root / "backtests" / "sample" / "diagnostics.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"status": "ok"}', encoding="utf-8")

    payload = store.read_json(r"artifacts\backtests\sample\diagnostics.json")

    assert payload == {"status": "ok"}


def test_local_artifact_store_reads_artifact_scheme_uri(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    store = LocalArtifactStore(artifact_root)
    target = artifact_root / "datasets" / "sample.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"rows": 1}', encoding="utf-8")

    payload = store.read_json("artifact://datasets/sample.json")

    assert payload == {"rows": 1}
