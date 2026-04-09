"""Single-run trainer entrypoint."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from src.train.contracts import ArtifactRef, TrainerContext, TrainerInput, TrainerOutput
from src.train.registry import build_registry_manifest, registry_model_dir
from src.train.tracking import build_tracking_payload
from src.train.tracking.mlflow_client import write_json_artifact
from src.train.utils import capture_seed_state, set_global_seed


def train_one(
    trainer: Any,
    config: Mapping[str, Any],
    trainer_input: TrainerInput,
    run_id: str | None = None,
    window_id: str | None = None,
) -> TrainerOutput:
    resolved_run_id = run_id or str(uuid4())
    output_dir = Path(config["tracking"]["artifact_root"]) / resolved_run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    set_global_seed(config["runtime"]["seed"], deterministic=config["runtime"]["deterministic"])
    context = TrainerContext(
        run_id=resolved_run_id,
        seed=config["runtime"]["seed"],
        device=config["runtime"]["device"],
        objective_type=config["experiment"]["task_type"],
        output_dir=output_dir,
        window_id=window_id,
        tags={"stage": config["experiment"].get("stage", "research")},
    )
    trainer.setup(context, {"primary_metric": config["eval"]["primary_metric"]})
    output = trainer.fit(trainer_input)

    dataset_manifest_path = write_json_artifact(
        output_dir / "dataset_manifest.json",
        {"data": config["data"], "dataset": config["dataset"], "window_id": window_id},
    )
    feature_schema_path = write_json_artifact(
        output_dir / "feature_schema.json",
        {
            "feature_set_id": config["dataset"]["feature_set_id"],
            "features": config["dataset"].get("features", []),
        },
    )
    metrics_summary_path = write_json_artifact(
        output_dir / "metrics_summary.json", output.metrics_by_split
    )
    config_resolved_path = output_dir / "config_resolved.yaml"
    config_resolved_path.write_text(
        json.dumps(dict(config), indent=2, sort_keys=True), encoding="utf-8"
    )
    predictions_path = output_dir / "predictions.parquet"
    predictions_path.write_text("", encoding="utf-8")
    checkpoint_path = output_dir / "best.ckpt"
    checkpoint_path.write_text("scaffold checkpoint placeholder\n", encoding="utf-8")
    model_card_path = write_json_artifact(
        output_dir / "model_card.json",
        {
            "run_id": resolved_run_id,
            "task_type": config["experiment"]["task_type"],
            "model_name": config["model"]["name"],
            "primary_metric": output.primary_metric_name,
        },
    )

    output.dataset_manifest_ref = str(dataset_manifest_path)
    output.feature_schema_ref = str(feature_schema_path)
    output.checkpoint_paths.setdefault("best", str(checkpoint_path))
    output.prediction_artifacts.setdefault("default", str(predictions_path))
    output.artifacts.extend(
        [
            ArtifactRef(name="dataset_manifest", path=str(dataset_manifest_path), kind="json"),
            ArtifactRef(name="feature_schema", path=str(feature_schema_path), kind="json"),
            ArtifactRef(name="metrics_summary", path=str(metrics_summary_path), kind="json"),
            ArtifactRef(name="config_resolved", path=str(config_resolved_path), kind="yaml"),
            ArtifactRef(name="predictions", path=str(predictions_path), kind="parquet"),
            ArtifactRef(name="best_checkpoint", path=str(checkpoint_path), kind="checkpoint"),
            ArtifactRef(name="model_card", path=str(model_card_path), kind="json"),
        ]
    )

    registry_path = registry_model_dir(
        config["tracking"]["artifact_root"], config["model"]["name"], resolved_run_id
    )
    registry_path.mkdir(parents=True, exist_ok=True)
    registry_manifest_path = write_json_artifact(
        registry_path / "registry_manifest.json",
        build_registry_manifest(config, output),
    )

    tracking_payload = build_tracking_payload(config, output, window_id=window_id)
    if getattr(trainer, "tracking_client", None) is not None:
        trainer.tracking_client.log_params(tracking_payload["params"])
        trainer.tracking_client.set_tags(tracking_payload["tags"])
        trainer.tracking_client.log_metrics(tracking_payload["metrics"])
        trainer.tracking_client.log_artifact(str(metrics_summary_path))
        trainer.tracking_client.log_artifact(str(registry_manifest_path))

    output.metadata["tracking_payload"] = tracking_payload
    output.metadata["seed_state"] = capture_seed_state(
        config["runtime"]["seed"],
        deterministic=config["runtime"]["deterministic"],
    )
    return output
