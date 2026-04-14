from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.training.evaluation import build_regression_evaluation_summary
from quant_platform.webapi.repositories.artifacts import ArtifactRepository


@dataclass(frozen=True)
class ModelArtifactReport:
    run_id: str
    status: str
    model_name: str | None
    dataset_id: str | None
    missing_artifacts: tuple[str, ...]
    prediction_scopes: tuple[str, ...]

    @property
    def compliant(self) -> bool:
        return self.status == "compliant"

    @property
    def repairable(self) -> bool:
        return self.status == "repairable"


class ModelCleanupService:
    def __init__(
        self,
        repository: ArtifactRepository,
        facade: QuantPlatformFacade,
    ) -> None:
        self.repository = repository
        self.facade = facade
        self.store = LocalArtifactStore(repository.artifact_root)

    def list_reports(self) -> list[ModelArtifactReport]:
        return [self.inspect_run(run_id) for run_id in self._run_ids()]

    def inspect_run(self, run_id: str) -> ModelArtifactReport:
        paths = self._run_paths(run_id)
        manifest = self._load_json(paths["train_manifest"]) or self._load_json(paths["legacy_manifest"])
        metadata = self._load_json(paths["metadata"])
        tracking = self._load_json(paths["tracking"])
        model_name = self._str(metadata.get("model_name")) or self._str(
            (tracking.get("params") or {}).get("model_name")
        )
        dataset_id = self._str(manifest.get("dataset_id")) or self._str(
            (tracking.get("params") or {}).get("dataset_id")
        )
        prediction_scopes = tuple(
            sorted(
                path.stem
                for path in (self.repository.artifact_root / "predictions" / run_id).glob("*.json")
            )
        )
        missing_artifacts: list[str] = []
        if not paths["metadata"].exists():
            missing_artifacts.append("metadata.json")
        if not (paths["train_manifest"].exists() or paths["legacy_manifest"].exists()):
            missing_artifacts.append("train_manifest.json")
        if not paths["evaluation_summary"].exists():
            missing_artifacts.append("evaluation_summary.json")
        if "full" not in prediction_scopes:
            missing_artifacts.append("predictions/full.json")

        if not missing_artifacts:
            status = "compliant"
        elif self._can_repair(paths, dataset_id):
            status = "repairable"
        else:
            status = "irreparable"
        return ModelArtifactReport(
            run_id=run_id,
            status=status,
            model_name=model_name,
            dataset_id=dataset_id,
            missing_artifacts=tuple(missing_artifacts),
            prediction_scopes=prediction_scopes,
        )

    def repair_run(self, run_id: str) -> ModelArtifactReport:
        report = self.inspect_run(run_id)
        if not report.repairable:
            raise ValueError(f"run '{run_id}' is not repairable")
        paths = self._run_paths(run_id)
        dataset_ref = self._load_dataset_ref(report.dataset_id)
        samples = self._load_dataset_samples(report.dataset_id)
        self.facade.dataset_store[dataset_ref.dataset_id] = samples
        plugin, meta = self.facade.model_registry.load_from_artifact(str(paths["metadata"]))
        runtime = self.facade.model_registry.resolve_runtime(meta.model_name)
        predictions_dir = self.repository.artifact_root / "predictions" / run_id
        if predictions_dir.exists():
            shutil.rmtree(predictions_dir)
        scope_payloads = self.facade.training_runner._collect_prediction_scopes(
            plugin=plugin,
            runtime=runtime,
            dataset_ref=dataset_ref,
            effective_spec=meta.model_spec,
            run_id=run_id,
            samples=samples,
            artifact_store=self.store,
        )
        feature_importance = plugin.feature_importance() or {}
        if feature_importance:
            feature_importance_path = self.repository.artifact_root / "models" / run_id / "feature_importance.json"
            feature_importance_path.parent.mkdir(parents=True, exist_ok=True)
            feature_importance_path.write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "model_name": meta.model_name,
                        "feature_importance": feature_importance,
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
        evaluation_summary = build_regression_evaluation_summary(
            run_id=run_id,
            dataset_ref=dataset_ref,
            scope_payloads=scope_payloads,
            feature_importance=feature_importance,
        )
        paths["evaluation_summary"].parent.mkdir(parents=True, exist_ok=True)
        paths["evaluation_summary"].write_text(
            json.dumps(evaluation_summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self._ensure_tracking(run_id, dataset_ref.dataset_id, meta.model_name, evaluation_summary)
        return self.inspect_run(run_id)

    def hard_delete_run(self, run_id: str) -> list[str]:
        deleted: list[str] = []
        for path in [
            self.repository.artifact_root / "models" / run_id,
            self.repository.artifact_root / "predictions" / run_id,
            self.repository.artifact_root / "tracking" / f"{run_id}.json",
            self.repository.artifact_root / "trained_models" / f"{run_id}.json",
        ]:
            if not path.exists():
                continue
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
            deleted.append(self.repository.display_uri(path))
        self._remove_run_job_references(run_id)
        return deleted

    def normalize_repository(self, *, delete_irreparable: bool = True) -> dict[str, object]:
        reports = self.list_reports()
        repaired: list[str] = []
        deleted: list[str] = []
        for report in reports:
            if report.compliant:
                continue
            if report.repairable:
                self.repair_run(report.run_id)
                repaired.append(report.run_id)
                continue
            if delete_irreparable:
                self.hard_delete_run(report.run_id)
                deleted.append(report.run_id)
        final_reports = self.list_reports()
        return {
            "total_runs": len(reports),
            "compliant_before": sum(1 for item in reports if item.compliant),
            "repaired": repaired,
            "deleted": deleted,
            "compliant_after": sum(1 for item in final_reports if item.compliant),
            "remaining_reports": [
                {
                    "run_id": item.run_id,
                    "status": item.status,
                    "missing_artifacts": list(item.missing_artifacts),
                    "prediction_scopes": list(item.prediction_scopes),
                }
                for item in final_reports
            ],
        }

    def _can_repair(self, paths: dict[str, Path], dataset_id: str | None) -> bool:
        if dataset_id is None or not paths["metadata"].exists():
            return False
        return (
            (paths["train_manifest"].exists() or paths["legacy_manifest"].exists())
            and self._dataset_ref_path(dataset_id).exists()
            and self._dataset_samples_path(dataset_id).exists()
        )

    def _ensure_tracking(
        self,
        run_id: str,
        dataset_id: str,
        model_name: str,
        evaluation_summary: dict[str, object],
    ) -> None:
        tracking_path = self.repository.artifact_root / "tracking" / f"{run_id}.json"
        current = self._load_json(tracking_path)
        current["run_id"] = run_id
        current["created_at"] = current.get("created_at") or self._isoformat(
            datetime.fromtimestamp(
                (self.repository.artifact_root / "models" / run_id).stat().st_mtime,
                tz=UTC,
            )
        )
        current["metrics"] = dict(
            evaluation_summary.get("regression_metrics", {})  # type: ignore[arg-type]
            or current.get("metrics")
            or {}
        )
        params = dict(current.get("params") or {})
        params.setdefault("dataset_id", dataset_id)
        params.setdefault("model_name", model_name)
        current["params"] = params
        tracking_path.parent.mkdir(parents=True, exist_ok=True)
        tracking_path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding="utf-8")

    def _load_dataset_ref(self, dataset_id: str | None) -> DatasetRef:
        if not dataset_id:
            raise ValueError("dataset_id is required to load dataset_ref")
        return self.store.read_model(str(self._dataset_ref_path(dataset_id)), DatasetRef)

    def _load_dataset_samples(self, dataset_id: str | None) -> list[DatasetSample]:
        if not dataset_id:
            raise ValueError("dataset_id is required to load dataset samples")
        payload = self.store.read_json(str(self._dataset_samples_path(dataset_id)))
        return [DatasetSample.model_validate(item) for item in payload.get("samples", [])]

    def _dataset_ref_path(self, dataset_id: str) -> Path:
        return self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_ref.json"

    def _dataset_samples_path(self, dataset_id: str) -> Path:
        return self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"

    def _run_ids(self) -> list[str]:
        ids = {path.name for path in self.repository.list_paths("models/*") if path.is_dir()}
        ids.update(path.stem for path in self.repository.list_paths("tracking/*.json"))
        return sorted(ids)

    def _run_paths(self, run_id: str) -> dict[str, Path]:
        model_dir = self.repository.artifact_root / "models" / run_id
        return {
            "metadata": model_dir / "metadata.json",
            "train_manifest": model_dir / "train_manifest.json",
            "legacy_manifest": model_dir / "manifest.json",
            "evaluation_summary": model_dir / "evaluation_summary.json",
            "tracking": self.repository.artifact_root / "tracking" / f"{run_id}.json",
        }

    def _remove_run_job_references(self, run_id: str) -> None:
        jobs_root = self.repository.artifact_root / "webapi" / "jobs"
        if not jobs_root.exists():
            return
        run_href = f"/runs/{run_id}"
        for path in self.repository.list_paths("webapi/jobs/*.json"):
            payload = self._load_json(path)
            result = payload.get("result")
            if not isinstance(result, dict):
                continue

            changed = False
            run_ids = result.get("run_ids")
            if isinstance(run_ids, list) and any(item == run_id for item in run_ids):
                result["run_ids"] = [item for item in run_ids if isinstance(item, str) and item != run_id]
                changed = True

            deeplinks = result.get("deeplinks")
            if isinstance(deeplinks, dict):
                detail_href = deeplinks.get("run_detail")
                if isinstance(detail_href, str) and detail_href.rstrip("/").endswith(run_href):
                    deeplinks["run_detail"] = None
                    changed = True

            result_links = result.get("result_links")
            if isinstance(result_links, list):
                filtered_links = [
                    item
                    for item in result_links
                    if not (
                        isinstance(item, dict)
                        and (
                            item.get("kind") == "run_detail"
                            or (
                                isinstance(item.get("href"), str)
                                and str(item.get("href")).rstrip("/").endswith(run_href)
                            )
                            or (
                                isinstance(item.get("api_path"), str)
                                and str(item.get("api_path")).rstrip("/").endswith(run_href)
                            )
                        )
                    )
                ]
                if len(filtered_links) != len(result_links):
                    result["result_links"] = filtered_links
                    changed = True

            if changed:
                path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _load_json(self, path: Path) -> dict[str, object]:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _str(self, value: object) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    def _isoformat(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
