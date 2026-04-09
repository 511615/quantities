from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from quant_platform.models.contracts.io import ModelArtifactMeta


def write_artifact_bundle(
    artifact_dir: Path,
    *,
    meta: ModelArtifactMeta,
    state: dict[str, Any],
) -> ModelArtifactMeta:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    state_path = artifact_dir / "state.pkl"
    meta_path = artifact_dir / "metadata.json"
    state_path.write_bytes(pickle.dumps(state))
    meta_path.write_text(meta.model_dump_json(indent=2), encoding="utf-8")
    return meta


def read_artifact_meta(artifact_uri: str | Path) -> ModelArtifactMeta:
    return ModelArtifactMeta.model_validate_json(Path(artifact_uri).read_text(encoding="utf-8"))


def read_artifact_state(artifact_uri: str | Path) -> dict[str, Any]:
    meta = read_artifact_meta(artifact_uri)
    return pickle.loads(Path(meta.state_uri).read_bytes())
