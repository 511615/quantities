"""MLflow-compatible tracking client and payload helpers."""

from __future__ import annotations

import json
import platform
import subprocess
from pathlib import Path
from typing import Any, Mapping, Optional

from src.train.contracts import TrainerOutput, flatten_mapping

try:
    import mlflow
except ImportError:  # pragma: no cover - optional dependency
    mlflow = None

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None


class MLflowTrackingClient:
    def __init__(
        self, tracking_uri: Optional[str] = None, experiment_name: Optional[str] = None
    ) -> None:
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        self.run_id: Optional[str] = None
        self.params_history: list[dict[str, Any]] = []
        self.metrics_history: list[tuple[dict[str, float], Optional[int]]] = []
        self.artifact_history: list[tuple[str, Optional[str]]] = []
        self.tags_history: list[dict[str, str]] = []

        if mlflow is not None and tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        if mlflow is not None and experiment_name:
            mlflow.set_experiment(experiment_name)

    def start_run(self, run_name: str, tags: Optional[Mapping[str, str]] = None) -> str:
        if mlflow is not None:
            active_run = mlflow.start_run(run_name=run_name)
            self.run_id = active_run.info.run_id
            if tags:
                mlflow.set_tags(dict(tags))
        else:
            self.run_id = run_name
            if tags:
                self.tags_history.append(dict(tags))
        return self.run_id

    def log_params(self, params: Mapping[str, Any]) -> None:
        flat = flatten_mapping(params)
        self.params_history.append(flat)
        if mlflow is not None:
            mlflow.log_params(flat)

    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None:
        normalised = {key: float(value) for key, value in metrics.items()}
        self.metrics_history.append((normalised, step))
        if mlflow is not None:
            mlflow.log_metrics(normalised, step=step)

    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None) -> None:
        self.artifact_history.append((local_path, artifact_path))
        if mlflow is not None:
            mlflow.log_artifact(local_path, artifact_path=artifact_path)

    def set_tags(self, tags: Mapping[str, str]) -> None:
        tag_map = dict(tags)
        self.tags_history.append(tag_map)
        if mlflow is not None:
            mlflow.set_tags(tag_map)

    def end_run(self, status: str = "FINISHED") -> None:
        if mlflow is not None:
            mlflow.end_run(status=status)


def build_tracking_payload(
    config: Mapping[str, Any],
    output: TrainerOutput,
    window_id: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "params": {
            "experiment_name": config["experiment"]["name"],
            "run_name": output.run_id,
            "task_type": config["experiment"]["task_type"],
            "model_family": config["model"]["name"],
            "dataset_id": config["data"]["dataset_id"],
            "feature_set_id": config["dataset"]["feature_set_id"],
            "label_id": config["dataset"]["label_id"],
            "split_policy": config["dataset"]["split_policy"],
            "window_size": config["dataset"]["rolling"]["train_size"],
            "step_size": config["dataset"]["rolling"]["step_size"],
            "embargo": config["dataset"]["rolling"]["embargo"],
            "purge_gap": config["dataset"]["rolling"]["purge_gap"],
            "seed": config["runtime"]["seed"],
            "batch_size": config["trainer"]["batch_size"],
            "lr": config["trainer"]["optimizer"]["lr"],
            "epochs": config["trainer"]["max_epochs"],
            "optimizer": config["trainer"]["optimizer"]["name"],
            "scheduler": config["trainer"]["scheduler"]["name"],
            "python_version": platform.python_version(),
            "torch_version": getattr(torch, "__version__", "not-installed"),
            "cuda": str(bool(torch and torch.cuda.is_available())),
            "git_commit": detect_git_commit(),
        },
        "metrics": build_metric_payload(output, window_id=window_id),
        "tags": {
            "stage": config["experiment"].get("stage", "research"),
            "task": config["experiment"]["task_type"],
            "split": "walk_forward" if window_id else "single",
            "candidate": str(config["tracking"].get("candidate", True)).lower(),
            "registry_ready": str(config["tracking"].get("registry_ready", False)).lower(),
        },
        "artifacts": {
            "config_resolved": "config_resolved.yaml",
            "dataset_manifest": "dataset_manifest.json",
            "feature_schema": "feature_schema.json",
            "metrics_summary": "metrics_summary.json",
            "predictions": "predictions.parquet",
            "best_checkpoint": "best.ckpt",
            "model_card": "model_card.json",
            "registry_manifest": "registry_manifest.json",
        },
    }


def build_metric_payload(
    output: TrainerOutput, window_id: Optional[str] = None
) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for split_name, split_metrics in output.metrics_by_split.items():
        for metric_name, value in split_metrics.items():
            key = f"{split_name}/{metric_name}"
            if window_id:
                key = f"wf/{window_id}/{key}"
            metrics[key] = float(value)

    primary_key = (
        f"wf/{window_id}/{output.primary_metric_name}" if window_id else output.primary_metric_name
    )
    metrics[primary_key] = float(output.primary_metric_value)
    return metrics


def write_json_artifact(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def detect_git_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        )
        return completed.stdout.strip()
    except Exception:
        return "unknown"
