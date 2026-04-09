from __future__ import annotations

import pytest

from src.train.contracts import TrainerOutput
from src.train.tracking import build_tracking_payload


pytestmark = pytest.mark.legacy


def test_build_tracking_payload_contains_required_contract_fields() -> None:
    config = {
        "experiment": {"name": "baseline", "task_type": "regression", "stage": "research"},
        "data": {"dataset_id": "dataset-v1"},
        "dataset": {
            "feature_set_id": "features-v1",
            "label_id": "label-v1",
            "split_policy": "rolling_walk_forward",
            "rolling": {"train_size": 120, "step_size": 20, "embargo": 1, "purge_gap": 0},
        },
        "model": {"name": "baseline_linear"},
        "trainer": {
            "batch_size": 256,
            "max_epochs": 10,
            "optimizer": {"name": "adam", "lr": 0.001},
            "scheduler": {"name": "none"},
        },
        "tracking": {"candidate": True, "registry_ready": False},
        "runtime": {"seed": 42},
    }
    output = TrainerOutput(
        run_id="run-001",
        primary_metric_name="loss",
        primary_metric_value=0.25,
        metrics_by_split={"valid": {"loss": 0.25}},
    )
    payload = build_tracking_payload(config, output, window_id="window_000")
    assert payload["params"]["experiment_name"] == "baseline"
    assert payload["params"]["dataset_id"] == "dataset-v1"
    assert payload["metrics"]["wf/window_000/valid/loss"] == 0.25
    assert payload["tags"]["split"] == "walk_forward"
    assert payload["artifacts"]["registry_manifest"] == "registry_manifest.json"
