from __future__ import annotations

from pathlib import Path


def build_artifact_root(tmp_path: Path, name: str) -> Path:
    return tmp_path / name
