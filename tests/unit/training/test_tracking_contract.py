from __future__ import annotations

from pathlib import Path

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.training.tracking.file_tracking import FileTrackingClient


def test_file_tracking_client_writes_expected_summary(artifact_root) -> None:
    artifact = FileTrackingClient(artifact_root).log_run_summary(
        "tracking-run",
        {"mae": 0.1},
        {"model_name": "mean_baseline", "dataset_id": "dataset_fixture"},
    )
    payload = LocalArtifactStore(artifact_root).read_json(artifact.uri)
    assert payload["run_id"] == "tracking-run"
    assert payload["metrics"]["mae"] == 0.1
    assert payload["params"]["dataset_id"] == "dataset_fixture"
    assert Path(artifact.uri).exists()
