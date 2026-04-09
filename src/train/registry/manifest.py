"""Registry-compatible artifact helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from src.train.contracts import TrainerOutput


def registry_model_dir(root: str | Path, model_name: str, run_id: str) -> Path:
    return Path(root) / "models" / model_name / run_id


def build_registry_manifest(config: Mapping[str, Any], output: TrainerOutput) -> dict[str, Any]:
    return {
        "model_name": config["model"]["name"],
        "model_version_hint": output.run_id,
        "input_schema": output.feature_schema_ref,
        "output_schema": {"prediction": "float"},
        "feature_set_id": config["dataset"]["feature_set_id"],
        "training_data_ref": output.dataset_manifest_ref,
        "task_type": config["experiment"]["task_type"],
        "promotion_gate_metrics": {output.primary_metric_name: output.primary_metric_value},
    }
