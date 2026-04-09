from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import ArtifactRef


class FileTrackingClient:
    def __init__(self, root: Path) -> None:
        self.store = LocalArtifactStore(root)

    def log_run_summary(
        self, run_id: str, metrics: dict[str, float], params: dict[str, str]
    ) -> ArtifactRef:
        payload = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
            "params": params,
        }
        return self.store.write_json(f"tracking/{run_id}.json", payload)
