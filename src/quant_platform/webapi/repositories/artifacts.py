from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ArtifactRepository:
    def __init__(self, artifact_root: Path) -> None:
        self.artifact_root = artifact_root.resolve()
        self.workspace_root = self.artifact_root.parent.resolve()

    def resolve_uri(self, uri: str) -> Path:
        candidate = Path(uri)
        if candidate.is_absolute():
            return candidate.resolve()
        if candidate.parts and candidate.parts[0] == self.artifact_root.name:
            return (self.workspace_root / candidate).resolve()
        return (self.artifact_root / candidate).resolve()

    def display_uri(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.workspace_root).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    def exists(self, uri: str) -> bool:
        return self.resolve_uri(uri).exists()

    def read_json(self, uri: str) -> dict[str, Any]:
        return json.loads(self.resolve_uri(uri).read_text(encoding="utf-8"))

    def read_json_if_exists(self, uri: str) -> dict[str, Any] | None:
        path = self.resolve_uri(uri)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def list_json_paths(self, pattern: str) -> list[Path]:
        return sorted(self.artifact_root.glob(pattern))

    def list_paths(self, pattern: str) -> list[Path]:
        return sorted(self.artifact_root.glob(pattern))
