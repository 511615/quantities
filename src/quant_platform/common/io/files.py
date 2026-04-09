from __future__ import annotations

import json
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from quant_platform.common.hashing.digest import file_digest
from quant_platform.common.types.core import ArtifactRef

ModelT = TypeVar("ModelT", bound=BaseModel)


class LocalArtifactStore:
    """Simple local artifact store for manifests and smoke runs."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def write_model(self, relative_path: str, model: BaseModel) -> ArtifactRef:
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(model.model_dump_json(indent=2), encoding="utf-8")
        return ArtifactRef(
            kind=model.__class__.__name__, uri=str(target), content_hash=file_digest(target)
        )

    def write_json(self, relative_path: str, payload: dict[str, object]) -> ArtifactRef:
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8"
        )
        return ArtifactRef(kind="json", uri=str(target), content_hash=file_digest(target))

    def read_model(self, uri: str, model_cls: type[ModelT]) -> ModelT:
        return model_cls.model_validate_json(Path(uri).read_text(encoding="utf-8"))

    def read_json(self, uri: str) -> dict[str, object]:
        return json.loads(Path(uri).read_text(encoding="utf-8"))
