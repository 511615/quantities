from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def stable_digest(payload: Any) -> str:
    """Create a deterministic hash from JSON-serializable content."""

    if isinstance(payload, BaseModel):
        serializable = payload.model_dump(mode="json")
    else:
        serializable = payload
    body = json.dumps(serializable, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
