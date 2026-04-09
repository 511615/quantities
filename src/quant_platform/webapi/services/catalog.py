from __future__ import annotations

import json
import shutil
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import FeatureField, TimeRange
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.data.contracts.series import NormalizedSeriesPoint
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.features.contracts.feature_view import (
    FeatureRow,
    FeatureViewBuildResult,
    FeatureViewRef,
)
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import (
    DatasetDependencyEntry,
    DatasetRegistryEntry,
    DatasetRegistryRepository,
)
from quant_platform.webapi.schemas.views import (
    ArtifactPreviewResponse,
    ArtifactView,
    BacktestEngineView,
    BacktestListItemView,
    BacktestReportView,
    BacktestsResponse,
    BenchmarkDetailView,
    BenchmarkListItemView,
    BenchmarkRowView,
    ComparisonRowView,
    DataFreshnessView,
    DatasetDeleteResponse,
    DatasetAcquisitionSourceRequest,
    DatasetDependenciesResponse,
    DatasetDependencyView,
    DatasetDetailView,
    DatasetFacetBucketView,
    DatasetFacetsView,
    DatasetFieldGroupView,
    DatasetFreshnessView,
    DatasetFusionBuildResponse,
    DatasetFusionRequest,
    DatasetFusionSourceRequest,
    DatasetReadinessSummaryView,
    DatasetRequestOptionsView,
    DatasetRequestOptionView,
    DatasetSeriesResponse,
    DatasetSeriesView,
    DatasetSliceView,
    DatasetSlicesResponse,
    DatasetListResponse,
    DatasetQualitySummaryView,
    DatasetSummaryView,
    DeepLinkView,
    ExperimentListItem,
    ExperimentsResponse,
    GlossaryHintView,
    JobStatusView,
    ModelComparisonRequest,
    ModelComparisonView,
    ModelTemplateCreateRequest,
    ModelTemplateListResponse,
    ModelTemplateUpdateRequest,
    ModelTemplateView,
    OhlcvBarView,
    OhlcvBarsResponse,
    PredictionArtifactView,
    RecentJobView,
    RecommendedActionView,
    RelatedBacktestView,
    ReviewSummaryView,
    RunDetailView,
    StableSummaryView,
    TimeValuePoint,
    TrainingDatasetSummaryView,
    TrainingDatasetsResponse,
    TrainedModelDetailView,
    TrainedModelListResponse,
    TrainedModelSummaryView,
    WarningSummaryView,
    WorkbenchOverviewView,
)


class ResearchWorkbenchService:
    def __init__(
        self,
        repository: ArtifactRepository,
        dataset_registry: DatasetRegistryRepository,
        store: LocalArtifactStore,
        model_families: dict[str, str],
        model_registry_entries: dict[str, Any] | None = None,
        facade: Any | None = None,
    ) -> None:
        self.repository = repository
        self.dataset_registry = dataset_registry
        self.store = store
        self.model_families = model_families
        self.model_registry_entries = model_registry_entries or {}
        self.facade = facade
        self.templates_root = self.repository.artifact_root / "webapi" / "model_templates"
        self.trained_root = self.repository.artifact_root / "webapi" / "trained_models"
        self.templates_root.mkdir(parents=True, exist_ok=True)
        self.trained_root.mkdir(parents=True, exist_ok=True)

    def workbench_overview(self, jobs: list[JobStatusView]) -> WorkbenchOverviewView:
        runs = self.list_runs(
            page=1,
            per_page=5,
            search=None,
            sort_by="created_at",
            sort_order="desc",
            model_name=None,
            dataset_id=None,
            status=None,
        ).items
        backtests = self.list_backtests(page=1, per_page=5, search=None, status=None).items
        benchmarks = self.list_benchmarks()[:5]
        datasets = self.list_datasets(page=1, per_page=5).items
        latest_dataset = max(
            datasets,
            key=lambda d: d.as_of_time or datetime.fromtimestamp(0, tz=UTC),
            default=None,
        )
        return WorkbenchOverviewView(
            generated_at=datetime.now(UTC),
            data_updated_at=(latest_dataset.as_of_time if latest_dataset else None),
            recent_runs=runs,
            recent_backtests=backtests,
            recent_benchmarks=benchmarks,
            recent_jobs=[
                RecentJobView(
                    job_id=job.job_id,
                    job_type=job.job_type,
                    status=job.status,
                    updated_at=job.updated_at,
                    dataset_id=job.result.dataset_id,
                    summary=job.result.summary,
                    result_links=job.result.result_links,
                    primary_stage=(job.stages[-1].name if job.stages else None),
                    deeplinks={link.kind: link.href for link in job.result.result_links},
                )
                for job in jobs[:5]
            ],
            data_freshness=DataFreshnessView(
                dataset_id=(latest_dataset.dataset_id if latest_dataset else None),
                as_of_time=(latest_dataset.as_of_time if latest_dataset else None),
                freshness=(latest_dataset.freshness.status if latest_dataset else "unknown"),
                source=(latest_dataset.data_source if latest_dataset else None),
            ),
            datasets=datasets,
            recommended_actions=[
                RecommendedActionView(
                    key="launch-train",
                    action_id="launch-train",
                    title="Launch training",
                    description="Start a training job from a model template.",
                    target_path="/runs",
                    href="/runs",
                ),
                RecommendedActionView(
                    key="launch-backtest",
                    action_id="launch-backtest",
                    title="Launch backtest",
                    description="Run backtest from a trained run prediction.",
                    target_path="/backtests",
                    href="/backtests",
                ),
            ],
        )

    def list_experiments(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        sort_by: str,
        sort_order: str,
        model_name: str | None,
        dataset_id: str | None,
        status: str | None,
    ) -> ExperimentsResponse:
        items = [self._experiment_item(run_id) for run_id in self._run_ids()]
        filtered = [
            item
            for item in items
            if (search is None or search.lower() in f"{item.run_id} {item.model_name}".lower())
            and (model_name is None or item.model_name == model_name)
            and (dataset_id is None or item.dataset_id == dataset_id)
            and (status is None or item.status == status)
        ]
        reverse = sort_order == "desc"
        if sort_by == "model_name":
            filtered.sort(key=lambda x: x.model_name, reverse=reverse)
        else:
            filtered.sort(
                key=lambda x: x.created_at or datetime.fromtimestamp(0, tz=UTC),
                reverse=reverse,
            )
        start = (page - 1) * per_page
        end = start + per_page
        return ExperimentsResponse(
            items=filtered[start:end],
            total=len(filtered),
            page=page,
            per_page=per_page,
            available_models=sorted({item.model_name for item in items}),
            available_datasets=sorted({item.dataset_id for item in items if item.dataset_id}),
            available_statuses=sorted({item.status for item in items}),
        )

    def list_runs(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        sort_by: str,
        sort_order: str,
        model_name: str | None,
        dataset_id: str | None,
        status: str | None,
    ) -> ExperimentsResponse:
        return self.list_experiments(
            page=page,
            per_page=per_page,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            model_name=model_name,
            dataset_id=dataset_id,
            status=status,
        )

    def get_run_detail(self, run_id: str) -> RunDetailView | None:
        tracking = self.repository.read_json_if_exists(f"tracking/{run_id}.json")
        if tracking is None and not (self.repository.artifact_root / "models" / run_id).exists():
            return None
        tracking = tracking or {}
        manifest = self.repository.read_json_if_exists(f"models/{run_id}/train_manifest.json") or self.repository.read_json_if_exists(f"models/{run_id}/manifest.json") or {}
        metadata = self.repository.read_json_if_exists(f"models/{run_id}/metadata.json") or {}
        model_name = str((tracking.get("params") or {}).get("model_name") or metadata.get("model_name") or run_id)
        dataset_id = (tracking.get("params") or {}).get("dataset_id") or manifest.get("dataset_id")
        predictions: list[PredictionArtifactView] = []
        for path in self.repository.list_paths(f"predictions/{run_id}/*.json"):
            payload = self._load(path)
            rows = payload.get("rows", [])
            predictions.append(
                PredictionArtifactView(
                    scope=path.stem,
                    sample_count=len(rows) if isinstance(rows, list) else 0,
                    uri=self.repository.display_uri(path),
                )
            )
        return RunDetailView(
            run_id=run_id,
            model_name=model_name,
            dataset_id=(str(dataset_id) if isinstance(dataset_id, str) else None),
            family=self.model_families.get(model_name),
            backend=self._backend(model_name),
            status="success" if tracking else "partial",
            created_at=self._dt(tracking.get("created_at")) or self._dt(manifest.get("created_at")),
            metrics=self._metrics(tracking.get("metrics") or {}),
            tracking_params={str(k): str(v) for k, v in (tracking.get("params") or {}).items()},
            manifest_metrics=self._metrics(manifest.get("metrics") or {}),
            repro_context=dict(manifest.get("repro_context") or {}),
            feature_importance=self._metrics((self.repository.read_json_if_exists(f"models/{run_id}/feature_importance.json") or {}).get("feature_importance", {})),
            predictions=predictions,
            related_backtests=self._related_backtests(run_id),
            artifacts=self._artifacts([
                ("tracking_summary", self.repository.artifact_root / "tracking" / f"{run_id}.json"),
                ("train_manifest", self.repository.artifact_root / "models" / run_id / "train_manifest.json"),
                ("model_metadata", self.repository.artifact_root / "models" / run_id / "metadata.json"),
            ]),
            notes=[],
            summary=StableSummaryView(status="success", headline=f"Run {run_id}"),
            pipeline_summary=None,
            review_summary=self._review_unavailable(),
            warning_summary=WarningSummaryView(level="none", count=0, items=[]),
            glossary_hints=self._glossary(["mae", "prediction_scope"]),
        )

    def list_benchmarks(self) -> list[BenchmarkListItemView]:
        items: list[BenchmarkListItemView] = []
        for path in self.repository.list_paths("benchmarks/*.json"):
            payload = self._load(path)
            leaderboard = payload.get("leaderboard", [])
            top = leaderboard[0] if isinstance(leaderboard, list) and leaderboard else {}
            items.append(
                BenchmarkListItemView(
                    benchmark_name=path.stem,
                    dataset_id=str(payload.get("dataset_id", "unknown_dataset")),
                    data_source=self._str(payload.get("data_source")),
                    benchmark_type=str(payload.get("benchmark_type", "workflow")),
                    updated_at=datetime.fromtimestamp(path.stat().st_mtime, tz=UTC),
                    top_model_name=self._str(top.get("model_name")),
                    top_model_score=self._float(top.get("mean_test_mae")),
                )
            )
        return sorted(items, key=lambda x: x.updated_at, reverse=True)

    def get_benchmark_detail(self, benchmark_name: str) -> BenchmarkDetailView | None:
        path = self.repository.artifact_root / "benchmarks" / f"{benchmark_name}.json"
        if not path.exists():
            return None

        def to_row(r: dict[str, Any]) -> BenchmarkRowView:
            return BenchmarkRowView(
                rank=int(r.get("rank", 0) or 0),
                model_name=str(r.get("model_name", "unknown")),
                family=str(r.get("family", "unknown")),
                advanced_kind=str(r.get("advanced_kind", "baseline")),
                backend=str(r.get("backend", "unknown")),
                window_count=int(r.get("window_count", 0) or 0),
                mean_valid_mae=float(r.get("mean_valid_mae", 0.0) or 0.0),
                mean_test_mae=float(r.get("mean_test_mae", 0.0) or 0.0),
                artifact_uri=self._str(r.get("artifact_uri")),
            )

        payload = self._load(path)
        leaderboard = [to_row(r) for r in payload.get("leaderboard", []) if isinstance(r, dict)]
        return BenchmarkDetailView(
            benchmark_name=benchmark_name,
            dataset_id=str(payload.get("dataset_id", "unknown_dataset")),
            data_source=self._str(payload.get("data_source")),
            benchmark_type=str(payload.get("benchmark_type", "workflow")),
            updated_at=datetime.fromtimestamp(path.stat().st_mtime, tz=UTC),
            window_count=int(payload.get("window_count", 0) or 0),
            leaderboard=leaderboard,
            results=[to_row(r) for r in payload.get("results", []) if isinstance(r, dict)],
            deep_backend_comparison=list(payload.get("deep_backend_comparison", [])),
            validation_summary=dict(payload.get("validation_summary", {})),
            artifacts=self._artifacts([
                ("benchmark_json", path),
                ("benchmark_markdown", path.with_suffix(".md")),
                ("benchmark_csv", path.with_suffix(".csv")),
            ]),
            summary=StableSummaryView(
                status="success",
                headline=(f"Top model: {leaderboard[0].model_name}" if leaderboard else "Benchmark"),
            ),
            review_summary=self._review_unavailable(),
            warning_summary=WarningSummaryView(level="none", count=0, items=[]),
            glossary_hints=self._glossary(["benchmark", "mae"]),
        )

    def list_backtests(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        status: str | None,
    ) -> BacktestsResponse:
        items: list[BacktestListItemView] = []
        summary = self.repository.read_json_if_exists("workflows/backtest/backtest_summary.json") or {}
        for row in summary.get("rows", []):
            if not isinstance(row, dict):
                continue
            metrics = row.get("simulation_metrics", {}) if isinstance(row.get("simulation_metrics"), dict) else {}
            item = BacktestListItemView(
                backtest_id=self._backtest_id(row.get("research_result_uri")),
                run_id=self._str(row.get("run_id")),
                model_name=self._str(row.get("model_name")),
                status=("success" if row.get("passed_consistency_checks") else "failed"),
                passed_consistency_checks=bool(row.get("passed_consistency_checks")),
                annual_return=self._float(metrics.get("annual_return")),
                max_drawdown=self._float(metrics.get("max_drawdown")),
                warning_count=len(summary.get("comparison_warnings", [])),
                updated_at=datetime.now(UTC),
            )
            text = f"{item.backtest_id} {item.run_id or ''} {item.model_name or ''}".lower()
            if search and search.lower() not in text:
                continue
            if status and item.status != status:
                continue
            items.append(item)
        start = (page - 1) * per_page
        end = start + per_page
        return BacktestsResponse(
            items=items[start:end],
            total=len(items),
            page=page,
            per_page=per_page,
            available_statuses=sorted({i.status for i in items}),
        )

    def get_backtest_detail(self, backtest_id: str) -> BacktestReportView | None:
        summary = self.repository.read_json_if_exists("workflows/backtest/backtest_summary.json") or {}
        for row in summary.get("rows", []):
            if not isinstance(row, dict):
                continue
            if self._backtest_id(row.get("research_result_uri")) != backtest_id:
                continue
            return BacktestReportView(
                backtest_id=backtest_id,
                model_name=self._str(row.get("model_name")),
                run_id=self._str(row.get("run_id")),
                passed_consistency_checks=bool(row.get("passed_consistency_checks")),
                comparison_warnings=[str(x) for x in summary.get("comparison_warnings", []) if isinstance(x, str)],
                divergence_metrics=self._metrics(row.get("divergence_metrics", {})),
                scenario_metrics=self._metrics(row.get("scenario_metrics", {})),
                research=self._engine(row.get("research_result_uri")),
                simulation=self._engine(row.get("simulation_result_uri")),
                artifacts=[],
                summary=StableSummaryView(status="success", headline=f"Backtest {backtest_id}"),
                pipeline_summary=None,
                review_summary=self._review_unavailable(),
                warning_summary=WarningSummaryView(level="none", count=0, items=[]),
                glossary_hints=self._glossary(["consistency_check", "max_drawdown"]),
            )
        return None

    def compare_models(self, request: ModelComparisonRequest) -> ModelComparisonView:
        rows: list[ComparisonRowView] = []
        for run_id in request.run_ids:
            detail = self.get_run_detail(run_id)
            if detail is None:
                continue
            rows.append(
                ComparisonRowView(
                    row_id=f"run:{run_id}",
                    source_type="run",
                    label=run_id,
                    model_name=detail.model_name,
                    dataset_id=detail.dataset_id,
                    backend=detail.backend,
                    status=detail.status,
                    train_mae=self._float(detail.metrics.get("mae")),
                )
            )
        return ModelComparisonView(rows=rows)

    def preview_artifact(self, uri: str) -> ArtifactPreviewResponse:
        path = self.repository.resolve_uri(uri.replace("\\", "/"))
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            return ArtifactPreviewResponse(uri=uri, kind="json", is_json=True, content=json.loads(text))
        return ArtifactPreviewResponse(uri=uri, kind="text", is_json=False, content=text)

    def resolve_run_model_artifact_uri(self, run_id: str) -> str | None:
        metadata = self.repository.artifact_root / "models" / run_id / "metadata.json"
        if metadata.exists():
            return self.repository.display_uri(metadata)
        legacy = self.repository.artifact_root / "models" / run_id / "manifest.json"
        if legacy.exists():
            payload = self._load(legacy)
            uri = (payload.get("model_artifact") or {}).get("uri")
            return uri if isinstance(uri, str) else None
        return None

    def get_run_manifest(self, run_id: str) -> dict[str, Any]:
        return self.repository.read_json_if_exists(
            f"models/{run_id}/train_manifest.json"
        ) or self.repository.read_json_if_exists(f"models/{run_id}/manifest.json") or {}

    def get_run_model_metadata(self, run_id: str) -> dict[str, Any]:
        return self.repository.read_json_if_exists(f"models/{run_id}/metadata.json") or {}

    def get_dataset_payload(self, dataset_id: str) -> dict[str, Any] | None:
        return self._dataset_ref(dataset_id)

    def load_market_bars_for_dataset(self, dataset_id: str) -> list[NormalizedMarketBar]:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return []
        bars = self._normalized_market_bars_from_payload(payload)
        if bars:
            return bars
        acquisition_profile = self._dataset_manifest(payload).get("acquisition_profile") or {}
        anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
        if anchor_dataset_id and anchor_dataset_id != dataset_id:
            anchor_payload = self._dataset_ref(anchor_dataset_id)
            if anchor_payload is not None:
                return self._normalized_market_bars_from_payload(anchor_payload)
        return []

    def list_model_templates(self, *, include_deleted: bool = False) -> ModelTemplateListResponse:
        items = self._builtin_templates() + self._custom_templates()
        if not include_deleted:
            items = [item for item in items if item.deleted_at is None]
        return ModelTemplateListResponse(items=items, total=len(items), model_options_source="registry")

    def get_model_template(self, template_id: str) -> ModelTemplateView | None:
        for item in self._builtin_templates():
            if item.template_id == template_id:
                return item
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return None
        return ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))

    def create_model_template(self, request: ModelTemplateCreateRequest) -> ModelTemplateView:
        if request.model_name not in self._registry_models():
            raise ValueError(f"model '{request.model_name}' is not registered")
        now = datetime.now(UTC)
        item = ModelTemplateView(
            template_id=f"custom-{uuid.uuid4().hex}",
            name=request.name,
            model_name=request.model_name,
            description=request.description,
            source="custom",
            hyperparams=request.hyperparams,
            trainer_preset=request.trainer_preset,
            dataset_preset=request.dataset_preset,
            read_only=False,
            model_registered=True,
            created_at=now,
            updated_at=now,
        )
        (self.templates_root / f"{item.template_id}.json").write_text(
            item.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return item

    def update_model_template(
        self,
        template_id: str,
        request: ModelTemplateUpdateRequest,
    ) -> ModelTemplateView | None:
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return None
        cur = ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))
        nxt = cur.model_copy(
            update={
                "name": request.name if request.name is not None else cur.name,
                "description": request.description if request.description is not None else cur.description,
                "hyperparams": request.hyperparams if request.hyperparams is not None else cur.hyperparams,
                "trainer_preset": request.trainer_preset if request.trainer_preset is not None else cur.trainer_preset,
                "dataset_preset": request.dataset_preset if request.dataset_preset is not None else cur.dataset_preset,
                "updated_at": datetime.now(UTC),
            }
        )
        path.write_text(nxt.model_dump_json(indent=2), encoding="utf-8")
        return nxt

    def delete_model_template(self, template_id: str) -> bool:
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return False
        cur = ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))
        nxt = cur.model_copy(update={"deleted_at": datetime.now(UTC), "updated_at": datetime.now(UTC)})
        path.write_text(nxt.model_dump_json(indent=2), encoding="utf-8")
        return True

    def list_trained_models(self, *, include_deleted: bool = False) -> TrainedModelListResponse:
        items: list[TrainedModelSummaryView] = []
        for run_id in self._run_ids():
            detail = self.get_trained_model(run_id)
            if detail is None:
                continue
            if detail.is_deleted and not include_deleted:
                continue
            items.append(
                TrainedModelSummaryView(
                    run_id=detail.run_id,
                    model_name=detail.model_name,
                    family=detail.family,
                    dataset_id=detail.dataset_id,
                    created_at=detail.created_at,
                    status=detail.status,
                    metrics=detail.metrics,
                    note=detail.note,
                    is_deleted=detail.is_deleted,
                    links=detail.links,
                )
            )
        return TrainedModelListResponse(items=items, total=len(items))

    def get_trained_model(self, run_id: str) -> TrainedModelDetailView | None:
        detail = self.get_run_detail(run_id)
        if detail is None:
            return None
        meta_path = self.trained_root / f"{run_id}.json"
        meta = self._load(meta_path) if meta_path.exists() else {}
        return TrainedModelDetailView(
            run_id=detail.run_id,
            model_name=detail.model_name,
            family=detail.family,
            dataset_id=detail.dataset_id,
            created_at=detail.created_at,
            status=detail.status,
            metrics=detail.metrics,
            note=self._str(meta.get("note")),
            is_deleted=bool(meta.get("is_deleted", False)),
            artifacts=detail.artifacts,
            tracking_params=detail.tracking_params,
            model_spec={},
            links=[
                DeepLinkView(
                    kind="run_detail",
                    label=f"Run {run_id}",
                    href=f"/runs/{run_id}",
                    api_path=f"/api/runs/{run_id}",
                )
            ],
        )

    def soft_delete_trained_model(self, run_id: str) -> TrainedModelDetailView | None:
        if self.get_run_detail(run_id) is None:
            return None
        payload = {
            "run_id": run_id,
            "is_deleted": True,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        (self.trained_root / f"{run_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self.get_trained_model(run_id)

    def update_trained_model_note(self, run_id: str, note: str | None) -> TrainedModelDetailView | None:
        if self.get_run_detail(run_id) is None:
            return None
        current = self._load(self.trained_root / f"{run_id}.json")
        payload = {
            "run_id": run_id,
            "is_deleted": bool(current.get("is_deleted", False)),
            "note": note,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        (self.trained_root / f"{run_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self.get_trained_model(run_id)

    def list_datasets(self, *, page: int, per_page: int) -> DatasetListResponse:
        items_by_id: dict[str, DatasetSummaryView] = {}
        for payload in self._dataset_refs(visible_only=True):
            summary = self._dataset_summary(payload)
            existing = items_by_id.get(summary.dataset_id)
            existing_time = existing.as_of_time if existing else None
            if existing is None or (summary.as_of_time or datetime.fromtimestamp(0, tz=UTC)) >= (
                existing_time or datetime.fromtimestamp(0, tz=UTC)
            ):
                items_by_id[summary.dataset_id] = summary
        items = sorted(
            items_by_id.values(),
            key=lambda item: item.as_of_time or datetime.fromtimestamp(0, tz=UTC),
            reverse=True,
        )
        start = (page - 1) * per_page
        end = start + per_page
        return DatasetListResponse(items=items[start:end], total=len(items), page=page, per_page=per_page)

    def get_dataset_detail(self, dataset_id: str) -> DatasetDetailView | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        dataset_summary = self._dataset_summary(payload)
        detail_meta = self._dataset_detail_meta(payload)
        quality_summary = self._dataset_quality_summary(payload)
        readiness = self.get_dataset_readiness(dataset_id)
        return DatasetDetailView(
            dataset=dataset_summary,
            display_name=dataset_summary.display_name,
            subtitle=dataset_summary.subtitle,
            summary=detail_meta["summary"],
            intended_use=detail_meta["intended_use"],
            risk_note=detail_meta["risk_note"],
            row_count=dataset_summary.row_count,
            feature_count=dataset_summary.feature_count,
            label_count=dataset_summary.label_count,
            feature_columns_preview=detail_meta["feature_columns_preview"],
            label_columns=detail_meta["label_columns"],
            feature_groups=detail_meta["feature_groups"],
            quality_summary=quality_summary,
            glossary_hints=self._glossary(
                [
                    "as_of_time",
                    "freshness",
                    "label_horizon",
                    "split_strategy",
                    "sample_policy",
                    "temporal_safety",
                    "missing_ratio",
                    "duplicate_rows",
                    "feature_dimensions",
                    "label_columns",
                    "data_coverage",
                    "data_domain",
                    "alignment_policy",
                    "missing_feature_policy",
                    "entity_scope",
                    "snapshot_version",
                    "series_kind",
                ]
            ),
            label_spec=dict(payload.get("label_spec", {})),
            split_manifest=dict(payload.get("split_manifest", {})),
            sample_policy=dict(payload.get("sample_policy", {})),
            quality={
                "missing_ratio": quality_summary.missing_ratio,
                "duplicate_ratio": quality_summary.duplicate_ratio,
                "duplicate_rows": quality_summary.duplicate_rows,
                "status": quality_summary.status,
                "summary": quality_summary.summary,
                "checks": quality_summary.checks,
            },
            acquisition_profile=detail_meta["acquisition_profile"],
            build_profile=detail_meta["build_profile"],
            schema_profile=detail_meta["schema_profile"],
            readiness_profile=(
                readiness.model_dump(mode="json") if readiness is not None else detail_meta["readiness_profile"]
            ),
            training_profile=detail_meta["training_profile"],
            links=[
                DeepLinkView(
                    kind="dataset_series",
                    label="Series",
                    href=f"/datasets/{dataset_id}/series",
                    api_path=f"/api/datasets/{dataset_id}/series",
                ),
                *(
                    [
                        DeepLinkView(
                            kind="dataset_ohlcv",
                            label="OHLCV",
                            href=f"/datasets/{dataset_id}/ohlcv",
                            api_path=f"/api/datasets/{dataset_id}/ohlcv",
                        )
                    ]
                    if self._dataset_has_market_ohlcv(payload)
                    else []
                ),
            ],
        )

    def get_dataset_dependencies(self, dataset_id: str) -> DatasetDependenciesResponse | None:
        if self._dataset_ref(dataset_id) is None:
            return None
        dependencies = self.dataset_registry.list_dependencies(dataset_id)
        blocking_items = self._blocking_dataset_dependencies(dataset_id)
        return DatasetDependenciesResponse(
            dataset_id=dataset_id,
            items=[*[self._dependency_view(item) for item in dependencies], *blocking_items],
            can_delete=len(blocking_items) == 0,
            blocking_items=blocking_items,
        )

    def delete_dataset(self, dataset_id: str) -> DatasetDeleteResponse | None:
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            return None
        dependencies = self.get_dataset_dependencies(dataset_id)
        blocking_items = dependencies.blocking_items if dependencies is not None else []
        if blocking_items:
            return DatasetDeleteResponse(
                dataset_id=dataset_id,
                status="blocked",
                message="Dataset cannot be deleted because dependent resources still reference it.",
                blocking_items=blocking_items,
                deleted_files=[],
            )

        deleted_files = self._delete_dataset_artifacts(entry)
        self.dataset_registry.remove_dataset(dataset_id)
        return DatasetDeleteResponse(
            dataset_id=dataset_id,
            status="deleted",
            message="Dataset was permanently deleted from the registry and local artifacts.",
            blocking_items=[],
            deleted_files=deleted_files,
        )

    def build_fusion_dataset(self, request: DatasetFusionRequest) -> DatasetFusionBuildResponse:
        if self.facade is None:
            raise ValueError("Fusion dataset building is unavailable because the runtime facade is not configured.")
        if not request.sources:
            raise ValueError("Fusion dataset request requires at least one auxiliary source.")

        base_entry = self._dataset_entry(request.base_dataset_id)
        if base_entry is None:
            raise ValueError(f"Base dataset '{request.base_dataset_id}' was not found.")

        base_payload = base_entry.payload
        base_manifest_payload = self._dataset_manifest(base_payload)
        base_dataset_type = self._resolved_dataset_type(base_payload, base_manifest_payload)
        base_data_domain = (
            self._str((base_manifest_payload.get("acquisition_profile") or {}).get("data_domain"))
            or base_entry.data_domain
            or "market"
        )
        if base_data_domain != "market":
            raise ValueError("Fusion dataset building currently requires a market-domain base dataset.")
        if base_dataset_type not in {"training_panel", "fusion_training_panel"}:
            raise ValueError("Fusion dataset building requires a trainable market dataset as the base dataset.")

        dataset_id = self._slugify_dataset_id(request.request_name, suffix="fusion")
        if self._dataset_entry(dataset_id) is not None:
            raise ValueError(f"Fusion dataset '{dataset_id}' already exists.")

        base_dataset_ref = self.store.read_model(base_entry.ref_uri, DatasetRef)
        base_samples = self._load_dataset_samples(base_payload)
        if not base_samples:
            raise ValueError(f"Base dataset '{request.base_dataset_id}' has no materialized samples.")

        sample_timestamps = [sample.timestamp for sample in base_samples]
        start_time = min(sample_timestamps)
        end_time = max(sample_timestamps)
        missing_strategy = (
            self._str(request.missing_feature_policy.get("strategy"))
            or request.missing_feature_policy_name
            or "drop_if_missing"
        )
        keep_with_flags = missing_strategy == "keep_with_flags"

        source_contexts: list[dict[str, Any]] = []
        coverage_by_feature: dict[str, float] = {}
        missing_counts: dict[str, int] = {}
        fusion_domains: set[str] = {base_data_domain}
        for source in request.sources:
            if source.data_domain not in {"macro", "on_chain"}:
                raise ValueError(
                    "Fusion dataset building currently supports auxiliary sources from macro and on_chain domains."
                )
            points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                data_domain=source.data_domain,
                identifier=source.identifier,
                vendor=source.vendor,
                frequency=source.frequency,
                start_time=start_time,
                end_time=end_time + self._frequency_delta(source.frequency),
                options={
                    **source.options,
                    **({"exchange": source.exchange} if source.exchange else {}),
                    **({"metric_name": source.metric_name} if source.metric_name else {}),
                },
            )
            if not points:
                raise ValueError(
                    f"Fusion source '{source.data_domain}/{source.vendor}/{source.identifier}' returned no rows."
                )
            feature_name = source.feature_name or self._fusion_feature_name(source)
            snapshot_uri = self._write_fusion_series_rows(dataset_id, feature_name, points)
            source_contexts.append(
                {
                    "source": source,
                    "feature_name": feature_name,
                    "points": points,
                    "fetch_status": fetch_status,
                    "storage_uri": snapshot_uri,
                    "data_ref": DataAssetRef(
                        asset_id=f"{source.data_domain}_{source.identifier}_{source.frequency}",
                        schema_version=1,
                        source=source.vendor,
                        symbol=source.identifier,
                        venue=source.exchange or source.vendor,
                        frequency=source.frequency,
                        time_range=TimeRange(
                            start=min(point.event_time for point in points),
                            end=max(point.event_time for point in points),
                        ),
                        storage_uri=snapshot_uri,
                        content_hash=stable_digest([point.model_dump(mode="json") for point in points]),
                        entity_key=source.identifier,
                        tags=[
                            "fusion_input",
                            f"domain:{source.data_domain}",
                            f"metric:{source.metric_name or 'value'}",
                        ],
                        request_origin=fetch_status,
                        fallback_used=False,
                    ),
                }
            )
            missing_counts[feature_name] = 0
            fusion_domains.add(source.data_domain)

        feature_schema = list(base_dataset_ref.feature_view_ref.feature_schema)
        for context in source_contexts:
            feature_schema.append(
                FeatureField(
                    name=context["feature_name"],
                    dtype="float",
                    nullable=keep_with_flags,
                    description=(
                        f"Fusion input from {context['source'].data_domain}/"
                        f"{context['source'].vendor}/{context['source'].identifier}"
                    ),
                    lineage_source=(
                        f"{context['source'].data_domain}:"
                        f"{context['source'].vendor}:"
                        f"{context['source'].identifier}"
                    ),
                    max_available_time=min(
                        max(point.available_time for point in context["points"]),
                        base_dataset_ref.feature_view_ref.as_of_time,
                    ),
                )
            )
            if keep_with_flags:
                feature_schema.append(
                    FeatureField(
                        name=f"{context['feature_name']}__missing",
                        dtype="float",
                        nullable=False,
                        description=f"Missingness flag for {context['feature_name']}.",
                        lineage_source=f"fusion_missing_flag:{context['feature_name']}",
                        max_available_time=base_dataset_ref.feature_view_ref.as_of_time,
                    )
                )

        enriched_rows: list[FeatureRow] = []
        label_map = {(sample.entity_key, sample.timestamp): sample.target for sample in base_samples}
        dropped_missing_rows = 0
        for sample in sorted(base_samples, key=lambda item: (item.timestamp, item.entity_key)):
            values = dict(sample.features)
            row_missing = False
            for context in source_contexts:
                match = self._align_series_point(
                    context["points"],
                    timestamp=sample.timestamp,
                    available_time=sample.available_time,
                    alignment_policy_name=request.alignment_policy_name,
                )
                if match is None:
                    missing_counts[context["feature_name"]] += 1
                    row_missing = True
                    if keep_with_flags:
                        values[context["feature_name"]] = 0.0
                        values[f"{context['feature_name']}__missing"] = 1.0
                    continue
                values[context["feature_name"]] = match.value
                if keep_with_flags:
                    values[f"{context['feature_name']}__missing"] = 0.0
            if row_missing and not keep_with_flags:
                dropped_missing_rows += 1
                continue
            enriched_rows.append(
                FeatureRow(
                    entity_key=sample.entity_key,
                    timestamp=sample.timestamp,
                    available_time=sample.available_time,
                    values=values,
                )
            )

        total_candidate_rows = len(base_samples)
        for feature_name, missing_count in missing_counts.items():
            coverage_by_feature[feature_name] = (
                0.0 if total_candidate_rows == 0 else (total_candidate_rows - missing_count) / total_candidate_rows
            )

        feature_view_ref = FeatureViewRef(
            feature_set_id="multi_domain_fusion_v1",
            input_data_refs=[
                *list(base_dataset_ref.feature_view_ref.input_data_refs),
                *[context["data_ref"] for context in source_contexts],
            ],
            as_of_time=base_dataset_ref.feature_view_ref.as_of_time,
            feature_schema=feature_schema,
            build_config_hash=stable_digest(
                {
                    "base_dataset_id": request.base_dataset_id,
                    "request": request.model_dump(mode="json"),
                    "feature_names": [field.name for field in feature_schema],
                }
            ),
            storage_uri=f"artifact://datasets/{dataset_id}_feature_rows.json",
        )
        self.store.write_json(
            f"datasets/{dataset_id}_feature_rows.json",
            {"rows": [row.model_dump(mode="json") for row in enriched_rows]},
        )

        sample_policy = base_dataset_ref.sample_policy.model_copy(
            update={
                "recommended_training_use": "fusion_training_panel",
            }
        )
        feature_result = FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=enriched_rows)
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=dataset_id,
            feature_result=feature_result,
            labels=label_map,
            label_spec=base_dataset_ref.label_spec,
            split_manifest=base_dataset_ref.split_manifest,
            sample_policy=sample_policy,
        )

        coverage_values = list(coverage_by_feature.values())
        min_feature_coverage = min(coverage_values, default=1.0)
        coverage_warning = min_feature_coverage < 0.9
        coverage_failed = min_feature_coverage < float(
            request.missing_feature_policy.get("min_feature_coverage_ratio", 0.5) or 0.5
        )
        temporal_safety_passed = True
        freshness_candidates = [self._str(base_manifest_payload.get("freshness_status")) or "unknown"]
        for context in source_contexts:
            latest_point = max(context["points"], key=lambda item: item.available_time)
            if latest_point.available_time > base_dataset_ref.feature_view_ref.as_of_time:
                temporal_safety_passed = False
            freshness_candidates.append(
                self._freshness_status(
                    base_dataset_ref.feature_view_ref.as_of_time,
                    latest_point.available_time,
                )
            )
        freshness_rank = {"fresh": 0, "warning": 1, "stale": 2, "outdated": 3, "unknown": 4}
        worst_freshness = sorted(
            freshness_candidates,
            key=lambda item: freshness_rank.get(item or "unknown", 99),
        )[-1]

        dataset_manifest = dataset_manifest.model_copy(
            update={
                "asset_id": base_manifest_payload.get("asset_id") or request.base_dataset_id,
                "feature_set_id": "multi_domain_fusion_v1",
                "dropped_rows": dataset_manifest.dropped_rows + dropped_missing_rows,
                "raw_row_count": len(enriched_rows),
                "usable_sample_count": len(samples),
                "snapshot_version": stable_digest(
                    {
                        "base_dataset_id": request.base_dataset_id,
                        "feature_schema_hash": dataset_ref.feature_schema_hash,
                        "sample_count": len(samples),
                    }
                )[:12],
                "readiness_status": (
                    "not_ready"
                    if not samples or coverage_failed or not temporal_safety_passed
                    else ("warning" if coverage_warning else "ready")
                ),
                "alignment_status": (
                    "aligned" if request.alignment_policy_name in {"event_time_inner", "exact_inner"} else "aligned_asof"
                ),
                "missing_feature_status": (
                    "failed"
                    if coverage_failed
                    else ("warning" if coverage_warning or keep_with_flags else "clean")
                ),
                "label_alignment_status": "aligned",
                "split_integrity_status": "valid",
                "temporal_safety_status": "passed" if temporal_safety_passed else "failed",
                "freshness_status": worst_freshness,
                "quality_status": "warning" if coverage_warning else "healthy",
                "build_config": {
                    "sample_policy_name": request.sample_policy_name,
                    "alignment_policy_name": request.alignment_policy_name,
                    "missing_feature_policy_name": request.missing_feature_policy_name,
                    "sample_policy": request.sample_policy,
                    "alignment_policy": {
                        "mode": request.alignment_policy_name,
                        **request.alignment_policy,
                    },
                    "missing_feature_policy": {
                        "strategy": missing_strategy,
                        "coverage_by_feature": coverage_by_feature,
                        **request.missing_feature_policy,
                    },
                },
                "acquisition_profile": {
                    **dict(base_manifest_payload.get("acquisition_profile") or {}),
                    "request_name": request.request_name,
                    "data_domain": base_data_domain,
                    "data_domains": sorted(fusion_domains),
                    "dataset_type": "fusion_training_panel",
                    "request_origin": "fusion_dataset_request",
                    "base_dataset_id": request.base_dataset_id,
                    "market_anchor_dataset_id": request.base_dataset_id,
                    "source_dataset_ids": [request.base_dataset_id],
                    "fusion_domains": sorted(fusion_domains),
                    "source_specs": [
                        {
                            "data_domain": base_data_domain,
                            "source_vendor": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("source_vendor")),
                            "exchange": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("exchange")),
                            "frequency": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("frequency")),
                            "symbol_selector": {
                                "symbols": list((base_manifest_payload.get("acquisition_profile") or {}).get("symbols") or []),
                            },
                        },
                        *[
                            {
                                "data_domain": context["source"].data_domain,
                                "source_vendor": context["source"].vendor,
                                "exchange": context["source"].exchange,
                                "frequency": context["source"].frequency,
                                "identifier": context["source"].identifier,
                                "metric_name": context["source"].metric_name or "value",
                                "feature_name": context["feature_name"],
                            }
                            for context in source_contexts
                        ],
                    ],
                    "fusion_sources": [
                        {
                            "data_domain": context["source"].data_domain,
                            "vendor": context["source"].vendor,
                            "identifier": context["source"].identifier,
                            "feature_name": context["feature_name"],
                            "frequency": context["source"].frequency,
                            "metric_name": context["source"].metric_name or "value",
                            "fetch_status": context["fetch_status"],
                            "storage_uri": context["storage_uri"],
                        }
                        for context in source_contexts
                    ],
                    "coverage_by_feature": coverage_by_feature,
                    "connector_status_by_source": {
                        f"market:{request.base_dataset_id}": self._str(
                            (base_manifest_payload.get("acquisition_profile") or {}).get("request_origin")
                        )
                        or "unknown",
                        **{
                            (
                                f"{context['source'].data_domain}:"
                                f"{context['source'].vendor}:"
                                f"{context['source'].identifier}"
                            ): context["fetch_status"]
                            for context in source_contexts
                        },
                    },
                    "merge_policy_name": (
                        self._str(request.alignment_policy.get("merge_policy_name"))
                        or request.alignment_policy_name
                    ),
                },
            }
        )

        dataset_samples_artifact = self.store.write_json(
            f"datasets/{dataset_id}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        feature_view_artifact = self.store.write_model(
            f"datasets/{dataset_id}_feature_view_ref.json",
            feature_view_ref,
        )
        dataset_manifest_artifact = self.store.write_model(
            f"datasets/{dataset_id}_dataset_manifest.json",
            dataset_manifest,
        )
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": base_dataset_ref.entity_scope,
                "entity_count": base_dataset_ref.entity_count,
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_ref.json", dataset_ref)
        self.dataset_registry.bootstrap_from_artifacts()

        readiness = self.get_dataset_readiness(dataset_id)
        payload = self._dataset_ref(dataset_id) or {}
        summary = self._dataset_summary(payload) if payload else None
        training_summary = (
            TrainingDatasetSummaryView(
                dataset_id=summary.dataset_id,
                display_name=summary.display_name or summary.dataset_id,
                dataset_type=summary.dataset_type or "fusion_training_panel",
                data_domain=summary.data_domain,
                data_domains=list(summary.data_domains),
                snapshot_version=summary.snapshot_version,
                entity_scope=summary.entity_scope,
                universe_summary={
                    "entity_scope": summary.entity_scope,
                    "entity_count": summary.entity_count,
                    "symbols_preview": summary.symbols_preview,
                },
                sample_count=(readiness.usable_row_count if readiness else summary.sample_count),
                feature_count=summary.feature_count,
                label_count=summary.label_count,
                label_horizon=summary.label_horizon,
                split_strategy=summary.split_strategy,
                source_vendor=summary.source_vendor,
                frequency=summary.frequency,
                freshness_status=(readiness.freshness_status if readiness else summary.freshness.status),
                quality_status=summary.quality_status,
                readiness_status=(readiness.readiness_status if readiness else summary.readiness_status),
                readiness_reason=(
                    readiness.blocking_issues[0]
                    if readiness and readiness.blocking_issues
                    else (readiness.warnings[0] if readiness and readiness.warnings else None)
                ),
            )
            if summary is not None
            else None
        )
        return DatasetFusionBuildResponse(
            dataset_id=dataset_id,
            status="created",
            message="Fusion dataset was materialized and indexed for training.",
            detail_href=f"/datasets/{dataset_id}",
            training_href="/datasets/training",
            feature_view_uri=feature_view_artifact.uri,
            dataset_manifest_uri=dataset_manifest_artifact.uri,
            training_summary=training_summary,
            readiness=readiness,
        )

    def build_merged_dataset_from_sources(
        self,
        *,
        request_name: str,
        market_anchor_dataset_id: str,
        sources: list[DatasetAcquisitionSourceRequest],
        merge_policy_name: str = "strict_timestamp_inner",
        request_origin: str = "dataset_request_multi_domain",
    ) -> DatasetFusionBuildResponse:
        if self.facade is None:
            raise ValueError("Merged dataset building is unavailable because the runtime facade is not configured.")

        base_entry = self._dataset_entry(market_anchor_dataset_id)
        if base_entry is None:
            raise ValueError(f"Market anchor dataset '{market_anchor_dataset_id}' was not found.")
        base_manifest = self._dataset_manifest(base_entry.payload)
        base_acquisition_profile = dict(base_manifest.get("acquisition_profile") or {})
        base_frequency = (
            self._str(base_acquisition_profile.get("frequency"))
            or base_entry.frequency
            or "unknown"
        )
        base_samples = self._load_dataset_samples(base_entry.payload)
        if not base_samples:
            raise ValueError(f"Market anchor dataset '{market_anchor_dataset_id}' has no materialized samples.")
        base_timestamps = sorted({sample.timestamp for sample in base_samples})
        auxiliary_sources = [source for source in sources if source.data_domain != "market"]
        if not auxiliary_sources:
            raise ValueError("Multi-domain merged dataset requires at least one non-market source.")

        connector_status_by_source: dict[str, str] = {
            f"market:{market_anchor_dataset_id}": self._str(base_acquisition_profile.get("request_origin")) or "unknown"
        }
        fusion_sources: list[DatasetFusionSourceRequest] = []
        for source in auxiliary_sources:
            if source.data_domain not in {"macro", "on_chain"}:
                raise ValueError(
                    f"Multi-domain merged dataset currently supports macro/on_chain auxiliaries, got '{source.data_domain}'."
                )
            if source.frequency != base_frequency:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}' must use frequency '{base_frequency}', got '{source.frequency}'."
                )
            if not source.identifier:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}' requires an identifier under strict merge mode."
                )
            try:
                points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                    data_domain=source.data_domain,
                    identifier=source.identifier,
                    vendor=source.vendor,
                    frequency=source.frequency,
                    start_time=base_timestamps[0],
                    end_time=base_timestamps[-1] + self._frequency_delta(source.frequency),
                    options={
                        **({"exchange": source.exchange} if source.exchange else {}),
                        **dict(source.filters),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                if getattr(exc, "code", None) == "empty_result":
                    raise ValueError(
                        f"Empty result from {source.data_domain}/{source.vendor}/{source.identifier}: {exc}"
                    ) from exc
                raise
            if not points:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}/{source.vendor}/{source.identifier}' returned no rows."
                )
            if sorted({point.event_time for point in points}) != base_timestamps:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}/{source.vendor}/{source.identifier}' timestamps do not match the market anchor under strict_timestamp_inner."
                )
            connector_status_by_source[
                f"{source.data_domain}:{source.vendor}:{source.identifier}"
            ] = fetch_status
            fusion_sources.append(
                DatasetFusionSourceRequest(
                    data_domain=source.data_domain,
                    vendor=source.vendor,
                    identifier=source.identifier,
                    frequency=source.frequency,
                    feature_name=self._fusion_feature_name(source),
                    exchange=source.exchange,
                    metric_name=(
                        self._str(source.filters.get("metric_name"))
                        or self._str(source.filters.get("feature_name"))
                        or "value"
                    ),
                    options=dict(source.filters),
                )
            )

        response = self.build_fusion_dataset(
            DatasetFusionRequest(
                request_name=request_name,
                base_dataset_id=market_anchor_dataset_id,
                dataset_type="fusion_training_panel",
                sample_policy_name="fusion_training_panel_strict",
                alignment_policy_name=merge_policy_name,
                missing_feature_policy_name="drop_if_missing",
                alignment_policy={"merge_policy_name": merge_policy_name},
                missing_feature_policy={"min_feature_coverage_ratio": 1.0},
                sources=fusion_sources,
            )
        )
        source_specs = [
            {
                "data_domain": source.data_domain,
                "source_vendor": source.vendor,
                "exchange": source.exchange,
                "frequency": source.frequency,
                "identifier": source.identifier,
                "filters": dict(source.filters),
                "symbol_selector": (
                    source.symbol_selector.model_dump(mode="json")
                    if source.symbol_selector is not None
                    else None
                ),
            }
            for source in sources
        ]
        self._update_dataset_acquisition_profile(
            response.dataset_id,
            {
                "request_name": request_name,
                "request_origin": request_origin,
                "dataset_type": "training_panel",
                "data_domain": "market",
                "data_domains": list(dict.fromkeys([source.data_domain for source in sources])),
                "merge_policy_name": merge_policy_name,
                "market_anchor_dataset_id": market_anchor_dataset_id,
                "source_dataset_ids": [market_anchor_dataset_id],
                "source_specs": source_specs,
                "connector_status_by_source": connector_status_by_source,
                "internal_visibility": "public",
            },
        )
        return response.model_copy(update={"readiness": self.get_dataset_readiness(response.dataset_id)})

    def _update_dataset_acquisition_profile(
        self,
        dataset_id: str,
        acquisition_profile_updates: dict[str, Any],
    ) -> None:
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            raise ValueError(f"Dataset '{dataset_id}' was not found.")
        manifest_path = self._resolve_artifact_path(entry.manifest_uri) if entry.manifest_uri else (
            self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
        )
        dataset_manifest = self.store.read_model(str(manifest_path), DatasetBuildManifest)
        acquisition_profile = dict(dataset_manifest.acquisition_profile or {})
        acquisition_profile.update(acquisition_profile_updates)
        dataset_manifest = dataset_manifest.model_copy(
            update={"acquisition_profile": acquisition_profile}
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_manifest.json", dataset_manifest)

    def get_dataset_slices(self, dataset_id: str) -> DatasetSlicesResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        readiness = self.get_dataset_readiness(dataset_id)
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        split_manifest = dict(payload.get("split_manifest") or {})
        items = [
            DatasetSliceView(
                slice_id="full_dataset",
                label="Full Dataset",
                slice_kind="full",
                start_time=summary.freshness.data_start_time,
                end_time=summary.freshness.data_end_time,
                row_count=self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload),
                sample_count=self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload),
                readiness_status=(readiness.readiness_status if readiness else None),
                metadata={
                    "dataset_type": self._resolved_dataset_type(payload, manifest)
                },
            )
        ]
        for key in ["train_range", "valid_range", "test_range"]:
            range_payload = split_manifest.get(key)
            if not isinstance(range_payload, dict):
                continue
            items.append(
                DatasetSliceView(
                    slice_id=key,
                    label=key.replace("_range", "").title(),
                    slice_kind="split",
                    start_time=self._dt(range_payload.get("start")),
                    end_time=self._dt(range_payload.get("end")),
                    sample_count=self._int_or_none((manifest.get("split_counts") or {}).get(key.removesuffix("_range"))),
                    readiness_status=(readiness.readiness_status if readiness else None),
                    metadata={"strategy": split_manifest.get("strategy")},
                )
            )
        return DatasetSlicesResponse(dataset_id=dataset_id, items=items)

    def get_dataset_series(self, dataset_id: str) -> DatasetSeriesResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        default_domain = str((manifest.get("acquisition_profile") or {}).get("data_domain") or "market")
        items: list[DatasetSeriesView] = []
        for input_ref in self._dataset_input_refs(payload):
            coverage = input_ref.get("time_range") if isinstance(input_ref.get("time_range"), dict) else {}
            entity_key = self._str(input_ref.get("symbol")) or self._str(input_ref.get("asset_id"))
            series_key = self._str(input_ref.get("asset_id")) or self._str(input_ref.get("storage_uri")) or str(uuid.uuid4())
            input_domain = self._input_ref_domain(input_ref) or default_domain
            items.append(
                DatasetSeriesView(
                    series_key=series_key,
                    label=self._str(input_ref.get("symbol")) or series_key,
                    series_kind=(
                        "fusion_input_series"
                        if self._resolved_dataset_type(payload, manifest) == "fusion_training_panel"
                        else "input_series"
                    ),
                    data_domain=input_domain,
                    entity_key=entity_key,
                    frequency=self._str(input_ref.get("frequency")) or self._str((manifest.get("acquisition_profile") or {}).get("frequency")),
                    coverage={
                        "start_time": self._dt(coverage.get("start")),
                        "end_time": self._dt(coverage.get("end")),
                    },
                    metadata={
                        "source": self._str(input_ref.get("source")),
                        "venue": self._str(input_ref.get("venue")),
                        "storage_uri": self._str(input_ref.get("storage_uri")),
                        "tags": input_ref.get("tags") if isinstance(input_ref.get("tags"), list) else [],
                    },
                )
            )
        label_columns = self._label_columns(payload)
        for label_column in label_columns:
            items.append(
                DatasetSeriesView(
                    series_key=f"label::{label_column}",
                    label=label_column,
                    series_kind="label",
                    data_domain=default_domain,
                    entity_key=None,
                    frequency=summary.frequency,
                    coverage={
                        "start_time": summary.freshness.data_start_time,
                        "end_time": summary.freshness.data_end_time,
                    },
                    metadata={"horizon": (payload.get("label_spec") or {}).get("horizon")},
                )
            )
        return DatasetSeriesResponse(dataset_id=dataset_id, items=items)

    def query_dataset_ohlcv(
        self,
        dataset_id: str,
        *,
        page: int,
        per_page: int,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> OhlcvBarsResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        input_refs = self._dataset_input_refs(payload)
        rows = self._dataset_bars_rows(payload)
        items: list[OhlcvBarView] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            if not {"open", "high", "low", "close"}.issubset(row):
                continue
            event_time = self._dt(row.get("event_time"))
            if event_time is None:
                continue
            if start_time and event_time < start_time:
                continue
            if end_time and event_time > end_time:
                continue
            items.append(
                OhlcvBarView(
                    event_time=event_time,
                    available_time=self._dt(row.get("available_time")),
                    symbol=str(row.get("symbol", "UNKNOWN")),
                    venue=self._str(row.get("venue")),
                    open=float(row.get("open", 0.0) or 0.0),
                    high=float(row.get("high", 0.0) or 0.0),
                    low=float(row.get("low", 0.0) or 0.0),
                    close=float(row.get("close", 0.0) or 0.0),
                    volume=float(row.get("volume", 0.0) or 0.0),
                )
            )
        items.sort(key=lambda x: (x.event_time, x.symbol))
        start = (page - 1) * per_page
        end = start + per_page
        asset_ids = [self._str(ref.get("asset_id")) for ref in input_refs if self._str(ref.get("asset_id"))]
        symbols = [self._str(ref.get("symbol")) for ref in input_refs if self._str(ref.get("symbol"))]
        frequencies = [self._str(ref.get("frequency")) for ref in input_refs if self._str(ref.get("frequency"))]
        return OhlcvBarsResponse(
            dataset_id=dataset_id,
            asset_id=(asset_ids[0] if len(set(asset_ids)) == 1 and asset_ids else None),
            symbol=(symbols[0] if len(set(symbols)) == 1 and symbols else None),
            frequency=(frequencies[0] if len(set(frequencies)) == 1 and frequencies else None),
            total=len(items),
            page=page,
            per_page=per_page,
            start_time=start_time,
            end_time=end_time,
            items=items[start:end],
        )

    def get_dataset_request_options(self) -> DatasetRequestOptionsView:
        return DatasetRequestOptionsView(
            domains=[
                self._dataset_option("market", "市场数据", "首批真实接入域，默认用于价格序列与训练面板。", True),
                self._dataset_option("derivatives", "衍生品", "本期冻结统一契约、校验与缓存目录。"),
                self._dataset_option("on_chain", "链上数据", "首批真实接入域，面向公开链上指标。"),
                self._dataset_option("macro", "宏观数据", "首批真实接入域，面向宏观时间序列。"),
                self._dataset_option("sentiment_events", "情绪事件", "本期冻结统一契约、校验与缓存目录。"),
            ],
            asset_modes=[
                self._dataset_option("single_asset", "单资产", "单币种切片或单资产训练面板。", True),
                self._dataset_option("multi_asset", "多资产", "共享时间轴的多资产训练面板。"),
            ],
            selection_modes=[
                self._dataset_option("explicit", "显式选择", "请求中直接给出 symbols、series ids 或实体列表。", True),
                self._dataset_option("top_n", "Top N", "按预定义筛选规则自动选取前 N 个实体。"),
                self._dataset_option("facet_filter", "Facet 筛选", "由后端 facets 和约束组合出实体范围。"),
            ],
            symbol_types=[
                self._dataset_option("spot", "现货交易对", "默认使用交易所现货风格 symbol。", True),
                self._dataset_option("macro_series", "宏观序列", "FRED 一类宏观序列代码。"),
                self._dataset_option("protocol_metric", "链上指标", "协议、链或 TVL/费用类公开指标。"),
            ],
            source_vendors=[
                self._dataset_option("binance", "Binance Spot", "market 域首批真实连接器。", True),
                self._dataset_option("fred", "FRED", "macro 域首批真实连接器。"),
                self._dataset_option("defillama", "DeFiLlama", "on_chain 域首批真实连接器。"),
                self._dataset_option("contract_only", "Contract Only", "仅冻结接口，不承诺本期真实拉取。"),
                self._dataset_option("internal_smoke", "内部样例", "保留给 smoke 与现有自动化测试。"),
            ],
            exchanges=[
                self._dataset_option("binance", "Binance", "market 域默认交易场所。", True),
                self._dataset_option("fred", "FRED", "macro 域逻辑 source。"),
                self._dataset_option("defillama", "DeFiLlama", "on_chain 域逻辑 source。"),
            ],
            frequencies=[
                self._dataset_option("1h", "1小时", "适合价格与链上指标的训练面板主频率。", True),
                self._dataset_option("4h", "4小时", "适合低频链上指标。"),
                self._dataset_option("1d", "1天", "适合宏观与跨域对齐。"),
            ],
            feature_sets=[
                self._dataset_option(
                    "baseline_market_features",
                    "Baseline Market Features",
                    "内置市场基线特征集。",
                    True,
                ),
                self._dataset_option(
                    "macro_snapshot_features",
                    "Macro Snapshot Features",
                    "宏观序列标准化快照骨架。",
                ),
                self._dataset_option(
                    "on_chain_snapshot_features",
                    "On-chain Snapshot Features",
                    "链上指标标准化快照骨架。",
                ),
                self._dataset_option(
                    "multi_domain_fusion_v1",
                    "Multi-domain Fusion v1",
                    "首批 market + macro + on_chain 融合训练面板特征骨架。",
                ),
            ],
            label_horizons=[
                self._dataset_option("1", "1 Bar", "预测下一个 bar 的前向收益。", True),
                self._dataset_option("6", "6 Bar", "更适合低频跨域对齐的 horizon。"),
                self._dataset_option("24", "24 Bar", "适合日频或更长观察窗口。"),
            ],
            split_strategies=[
                self._dataset_option("time_series", "时间序列切分", "训练/验证/测试按时间顺序切分。", True)
            ],
            sample_policies=[
                self._dataset_option("training_panel_strict", "严格训练面板", "默认策略，丢弃缺失标签并要求最小历史长度。", True),
                self._dataset_option("fusion_training_panel_strict", "融合训练面板", "面向跨域对齐后的融合训练面板。"),
                self._dataset_option("display_slice_lenient", "宽松展示切片", "优先保留可浏览样本，适合详情与切片浏览。"),
            ],
            alignment_policies=[
                self._dataset_option("event_time_inner", "事件时间内连接", "按 entity_key + timestamp 对齐，只保留共同可用截面。", True),
                self._dataset_option("strict_timestamp_inner", "严格时间戳内连接", "多域 request 主链默认策略，要求所有源同频且时间戳完全一致。"),
                self._dataset_option("available_time_safe", "可用时间安全对齐", "要求 available_time 不晚于目标训练截面。"),
                self._dataset_option("available_time_safe_asof", "安全 asof 对齐", "按最新可用时间向后对齐，适合不同频率跨域融合。"),
            ],
            missing_feature_policies=[
                self._dataset_option("drop_if_missing", "缺失即丢弃", "默认训练策略，超过阈值直接剔除样本。", True),
                self._dataset_option("keep_with_flags", "保留并打标", "为浏览和调试保留缺失样本，同时打出缺失标记。"),
            ],
            domain_capabilities={
                "market": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["binance", "internal_smoke"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "4h", "1d"],
                },
                "macro": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["fred"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1d"],
                },
                "on_chain": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["defillama"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "4h", "1d"],
                },
                "derivatives": {
                    "supports_real_ingestion": False,
                    "supported_vendors": ["contract_only"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "1d"],
                },
                "sentiment_events": {
                    "supports_real_ingestion": False,
                    "supported_vendors": ["contract_only"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "1d"],
                },
            },
            constraints={
                "current_supported_domains": ["market", "macro", "on_chain"],
                "current_supported_asset_modes": ["single_asset", "multi_asset"],
                "current_supported_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DFF", "TOTAL"],
                "multi_asset_status": "multi_domain_registry_ready",
                "request_flow": "api_datasets_requests_via_jobs",
                "train_entry_mode": "dataset_id_gt_dataset_preset",
                "registry_backend": "sqlite_manifest_index",
                "artifact_source_of_truth": "registry_and_manifest",
                "fallback_mode": "disabled_on_mainline",
                "supported_merge_policies": ["strict_timestamp_inner"],
                "multi_domain_request_requires_market": True,
                "multi_domain_request_direct_merge": True,
            },
        )

    def get_dataset_facets(self) -> DatasetFacetsView:
        entries = [
            entry for entry in self.dataset_registry.list_entries() if self._is_public_dataset_payload(entry.payload)
        ]
        return DatasetFacetsView(
            domains=self._facet_buckets(entries, "data_domain"),
            dataset_types=self._facet_buckets(entries, "dataset_type"),
            source_vendors=self._facet_buckets(entries, "source_vendor"),
            frequencies=self._facet_buckets(entries, "frequency"),
            readiness_statuses=self._facet_buckets(entries, "readiness_status"),
        )

    def list_training_datasets(self) -> TrainingDatasetsResponse:
        items: list[TrainingDatasetSummaryView] = []
        for payload in self._dataset_refs(visible_only=True):
            dataset_id = str(payload.get("dataset_id", "unknown"))
            readiness = self.get_dataset_readiness(dataset_id)
            if readiness is None or readiness.readiness_status == "not_ready":
                continue
            summary = self._dataset_summary(payload)
            items.append(
                TrainingDatasetSummaryView(
                    dataset_id=summary.dataset_id,
                    display_name=summary.display_name or summary.dataset_id,
                    dataset_type=self._resolved_dataset_type(payload, self._dataset_manifest(payload)),
                    data_domain=str(
                        ((self._dataset_manifest(payload).get("acquisition_profile") or {}).get("data_domain"))
                        or "market"
                    ),
                    data_domains=list(summary.data_domains),
                    snapshot_version=summary.snapshot_version,
                    entity_scope=summary.entity_scope,
                    universe_summary={
                        "entity_scope": summary.entity_scope,
                        "entity_count": summary.entity_count,
                        "symbols_preview": summary.symbols_preview,
                    },
                    sample_count=readiness.usable_row_count or summary.sample_count,
                    feature_count=summary.feature_count,
                    label_count=summary.label_count,
                    label_horizon=summary.label_horizon,
                    split_strategy=summary.split_strategy,
                    source_vendor=summary.source_vendor,
                    frequency=summary.frequency,
                    freshness_status=readiness.freshness_status or summary.freshness.status,
                    quality_status=summary.quality_status,
                    readiness_status=readiness.readiness_status,
                    readiness_reason=(
                        readiness.blocking_issues[0]
                        if readiness.blocking_issues
                        else (readiness.warnings[0] if readiness.warnings else None)
                    ),
                )
            )
        items.sort(
            key=lambda item: (
                0 if item.readiness_status == "ready" else 1,
                item.snapshot_version or "",
                item.dataset_id,
            )
        )
        return TrainingDatasetsResponse(items=items, total=len(items))

    def get_dataset_readiness(self, dataset_id: str) -> DatasetReadinessSummaryView | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        quality_summary = self._dataset_quality_summary(payload)
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        build_status = self._str(manifest.get("build_status")) or "success"
        raw_row_count = self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload)
        usable_row_count = self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload)
        dropped_row_count = self._int_or_none(manifest.get("dropped_rows"))
        if dropped_row_count is None and raw_row_count is not None and usable_row_count is not None:
            dropped_row_count = max(raw_row_count - usable_row_count, 0)
        entity_scope = (
            self._str(payload.get("entity_scope"))
            or self._str(manifest.get("entity_scope"))
            or summary.entity_scope
            or "single_asset"
        )
        entity_count = (
            self._int_or_none(payload.get("entity_count"))
            or self._int_or_none(manifest.get("entity_count"))
            or summary.entity_count
            or 1
        )
        alignment_status = self._str(manifest.get("alignment_status")) or "aligned"
        missing_feature_status = self._str(manifest.get("missing_feature_status")) or (
            "warning" if (quality_summary.missing_ratio or 0.0) > 0.05 else "clean"
        )
        label_alignment_status = self._str(manifest.get("label_alignment_status")) or "aligned"
        split_integrity_status = self._str(manifest.get("split_integrity_status")) or (
            "valid" if payload.get("split_manifest") else "missing"
        )
        temporal_safety_status = self._str(manifest.get("temporal_safety_status")) or "passed"
        freshness_status = self._str(manifest.get("freshness_status")) or summary.freshness.status

        blocking_issues: list[str] = []
        warnings: list[str] = []
        if usable_row_count in {None, 0}:
            blocking_issues.append("usable_sample_count_is_zero")
        if not feature_schema:
            blocking_issues.append("feature_schema_missing")
        if not label_columns:
            blocking_issues.append("label_spec_missing")
        if not payload.get("split_manifest"):
            blocking_issues.append("split_manifest_missing")
        if self._resolved_dataset_type(payload, manifest) == "fusion_training_panel":
            market_anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
            if not market_anchor_dataset_id and not self._dataset_has_market_ohlcv(payload):
                blocking_issues.append("market_anchor_missing")
        if entity_scope == "multi_asset" and entity_count <= 1:
            blocking_issues.append("multi_asset_requires_multiple_entities")
        if label_alignment_status not in {"aligned", "passed", "unknown"}:
            blocking_issues.append("label_alignment_failed")
        if split_integrity_status not in {"valid", "passed", "unknown"}:
            blocking_issues.append("split_integrity_failed")
        if temporal_safety_status not in {"passed", "unknown"}:
            blocking_issues.append("temporal_safety_failed")
        if quality_summary.status == "risk":
            blocking_issues.append("quality_checks_failed")
        if missing_feature_status in {"risk", "failed"}:
            blocking_issues.append("missing_feature_threshold_exceeded")

        if freshness_status in {"stale", "outdated", "warning"}:
            warnings.append(f"freshness_{freshness_status}")
        if quality_summary.status == "warning":
            warnings.append("quality_warning")
        if acquisition_profile.get("fallback_used"):
            warnings.append("fallback_source_used")
        if entity_scope == "multi_asset" and entity_count <= 3:
            warnings.append("multi_asset_universe_is_small")

        readiness_status = self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status"))
        if blocking_issues:
            readiness_status = "not_ready"
        elif readiness_status in {None, "unknown"}:
            readiness_status = "warning" if warnings else "ready"
        elif readiness_status == "ready" and warnings:
            readiness_status = "warning"

        recommended_next_actions: list[str] = []
        if "feature_schema_missing" in blocking_issues:
            recommended_next_actions.append("重新构建特征视图并校验 feature schema。")
        if "label_alignment_failed" in blocking_issues:
            recommended_next_actions.append("检查标签是否按 (entity_key, timestamp) 对齐。")
        if "split_manifest_missing" in blocking_issues:
            recommended_next_actions.append("补齐时间序列 split manifest 后再训练。")
        if "fallback_source_used" in warnings:
            recommended_next_actions.append("优先切换到真实来源，避免长期依赖 fallback 样本。")
        if freshness_status in {"stale", "outdated", "warning"}:
            recommended_next_actions.append("刷新或重采集底层数据，再生成训练数据集。")

        return DatasetReadinessSummaryView(
            dataset_id=dataset_id,
            data_domains=self._resolved_data_domains(acquisition_profile),
            build_status=build_status,
            readiness_status=readiness_status,
            blocking_issues=blocking_issues,
            warnings=warnings,
            raw_row_count=raw_row_count,
            usable_row_count=usable_row_count,
            dropped_row_count=dropped_row_count,
            feature_count=len(feature_schema),
            feature_schema_hash=self._str(payload.get("feature_schema_hash")) or self._str(manifest.get("feature_schema_hash")),
            feature_dimension_consistent=bool(feature_schema),
            entity_scope=entity_scope,
            entity_count=entity_count,
            alignment_status=alignment_status,
            missing_feature_status=missing_feature_status,
            label_alignment_status=label_alignment_status,
            split_integrity_status=split_integrity_status,
            temporal_safety_status=temporal_safety_status,
            freshness_status=freshness_status,
            recommended_next_actions=list(dict.fromkeys(recommended_next_actions)),
        )

    def _resolved_dataset_type(
        self,
        payload: dict[str, Any],
        manifest: dict[str, Any] | None = None,
    ) -> str:
        manifest = manifest or self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        dataset_type = (
            self._str(acquisition_profile.get("dataset_type"))
            or self._str((payload.get("sample_policy") or {}).get("recommended_training_use"))
            or "display_slice"
        )
        if dataset_type in {
            "display_slice",
            "training_panel",
            "feature_snapshot",
            "fusion_training_panel",
        }:
            return dataset_type
        return "display_slice"

    def _resolved_data_domains(self, acquisition_profile: dict[str, Any]) -> list[str]:
        value = acquisition_profile.get("data_domains")
        if isinstance(value, list):
            resolved = [str(item) for item in value if isinstance(item, str) and item]
            if resolved:
                return list(dict.fromkeys(resolved))
        primary = self._str(acquisition_profile.get("data_domain")) or "market"
        fusion_domains = acquisition_profile.get("fusion_domains")
        if isinstance(fusion_domains, list):
            resolved = [primary]
            resolved.extend(
                str(item) for item in fusion_domains if isinstance(item, str) and item and item not in resolved
            )
            return resolved
        return [primary]

    def _dataset_has_market_ohlcv(self, payload: dict[str, Any]) -> bool:
        return any(
            {"open", "high", "low", "close"}.issubset(row)
            for row in self._dataset_bars_rows(payload)
            if isinstance(row, dict)
        )

    def _input_ref_domain(self, input_ref: dict[str, Any]) -> str | None:
        tags = input_ref.get("tags")
        if isinstance(tags, list):
            for item in tags:
                if isinstance(item, str) and item.startswith("domain:"):
                    return item.split(":", 1)[1]
        source = self._str(input_ref.get("source")) or ""
        lowered = source.lower()
        if "fred" in lowered:
            return "macro"
        if "llama" in lowered or "chain" in lowered:
            return "on_chain"
        if "binance" in lowered or "market" in lowered:
            return "market"
        return None

    def _load_dataset_samples(self, payload: dict[str, Any]) -> list[DatasetSample]:
        samples_uri = self._str(payload.get("dataset_samples_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        path = (
            self._resolve_artifact_path(samples_uri)
            if samples_uri
            else self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
        )
        loaded = self._load(path).get("samples", [])
        if not isinstance(loaded, list):
            return []
        return [DatasetSample.model_validate(item) for item in loaded if isinstance(item, dict)]

    def _align_series_point(
        self,
        points: list[NormalizedSeriesPoint],
        *,
        timestamp: datetime,
        available_time: datetime,
        alignment_policy_name: str,
    ) -> NormalizedSeriesPoint | None:
        if alignment_policy_name in {
            "event_time_inner",
            "exact_inner",
            "timestamp_inner",
            "strict_timestamp_inner",
        }:
            for point in reversed(points):
                if point.event_time == timestamp and point.available_time <= available_time:
                    return point
            return None
        candidate: NormalizedSeriesPoint | None = None
        for point in points:
            if point.event_time > timestamp or point.available_time > available_time:
                continue
            if candidate is None or point.event_time >= candidate.event_time:
                candidate = point
        return candidate

    def _write_fusion_series_rows(
        self,
        dataset_id: str,
        feature_name: str,
        points: list[NormalizedSeriesPoint],
    ) -> str:
        relative_path = f"datasets/{dataset_id}_{feature_name}_series_rows.json"
        self.store.write_json(
            relative_path,
            {"rows": [point.model_dump(mode="json") for point in points]},
        )
        return f"artifact://{relative_path}"

    def _fusion_feature_name(self, source: Any) -> str:
        identifier = getattr(source, "identifier", "series")
        domain = getattr(source, "data_domain", "aux")
        metric_name = getattr(source, "metric_name", None) or "value"
        raw = f"{domain}_{identifier}_{metric_name}"
        return self._slugify_dataset_id(raw, suffix="")

    def _slugify_dataset_id(self, value: str, suffix: str = "") -> str:
        cleaned = [character.lower() if character.isalnum() else "_" for character in value.strip()]
        slug = "".join(cleaned).strip("_")
        while "__" in slug:
            slug = slug.replace("__", "_")
        if suffix:
            if not slug.endswith(f"_{suffix}"):
                slug = f"{slug}_{suffix}" if slug else suffix
        return slug or f"dataset_{datetime.now(UTC):%Y%m%d%H%M%S}"

    @staticmethod
    def _frequency_delta(frequency: str) -> timedelta:
        mapping = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }
        return mapping.get(frequency, timedelta(hours=1))

    def _run_ids(self) -> list[str]:
        ids = {path.stem for path in self.repository.list_paths("tracking/*.json")}
        ids.update(path.name for path in self.repository.list_paths("models/*") if path.is_dir())
        return sorted(ids)

    def _related_backtests(self, run_id: str) -> list[RelatedBacktestView]:
        payload = self.repository.read_json_if_exists("workflows/backtest/backtest_summary.json") or {}
        results: list[RelatedBacktestView] = []
        for row in payload.get("rows", []):
            if not isinstance(row, dict) or row.get("run_id") != run_id:
                continue
            metrics = row.get("simulation_metrics", {}) if isinstance(row.get("simulation_metrics"), dict) else {}
            results.append(
                RelatedBacktestView(
                    backtest_id=self._backtest_id(row.get("research_result_uri")),
                    model_name=str(row.get("model_name", "unknown")),
                    run_id=run_id,
                    annual_return=self._float(metrics.get("annual_return")),
                    max_drawdown=self._float(metrics.get("max_drawdown")),
                    passed_consistency_checks=bool(row.get("passed_consistency_checks")),
                )
            )
        return results

    def _backtest_id(self, uri: Any) -> str:
        if not isinstance(uri, str) or not uri:
            return "unknown_backtest"
        path = self.repository.resolve_uri(uri.replace("\\", "/"))
        payload = self._load(path)
        backtest_id = payload.get("backtest_id")
        return str(backtest_id) if isinstance(backtest_id, str) and backtest_id else path.stem

    def _engine(self, uri: Any) -> BacktestEngineView | None:
        if not isinstance(uri, str) or not uri:
            return None
        payload = self._load(self.repository.resolve_uri(uri.replace("\\", "/")))
        if not payload:
            return None
        report = self._load(self.repository.resolve_uri(str(payload.get("report_uri", "")).replace("\\", "/")))
        pnl = self._load(self.repository.resolve_uri(str(payload.get("pnl_uri", "")).replace("\\", "/")))
        positions = self._load(self.repository.resolve_uri(str(payload.get("positions_uri", "")).replace("\\", "/")))
        return BacktestEngineView(
            backtest_id=str(payload.get("backtest_id", "unknown")),
            engine_type=str(payload.get("engine_type", "unknown")),
            report_summary=self._str(report.get("summary")),
            metrics=self._metrics(payload.get("risk_metrics", {})),
            diagnostics={},
            pnl_snapshot=self._metrics(pnl),
            positions=[
                TimeValuePoint(
                    label=str(r.get("timestamp", f"p{i}")),
                    value=float(r.get("target_weight", 0.0) or 0.0),
                )
                for i, r in enumerate(positions.get("positions", []))
                if isinstance(r, dict)
            ],
            scenarios=[],
            warnings=[],
            artifacts=[],
        )

    def _dataset_refs(self, *, visible_only: bool = False) -> list[dict[str, Any]]:
        payloads = [entry.payload for entry in self.dataset_registry.list_entries()]
        if not visible_only:
            return payloads
        return [payload for payload in payloads if self._is_public_dataset_payload(payload)]

    def _dataset_ref(self, dataset_id: str) -> dict[str, Any] | None:
        entry = self.dataset_registry.get_entry(dataset_id)
        return entry.payload if entry else None

    def _dataset_entry(self, dataset_id: str) -> DatasetRegistryEntry | None:
        return self.dataset_registry.get_entry(dataset_id)

    def _is_public_dataset_payload(self, payload: dict[str, Any]) -> bool:
        acquisition_profile = self._dataset_manifest(payload).get("acquisition_profile") or {}
        visibility = self._str(acquisition_profile.get("internal_visibility")) or "public"
        return visibility != "hidden"

    def _facet_buckets(
        self,
        entries: list[DatasetRegistryEntry],
        attribute: str,
    ) -> list[DatasetFacetBucketView]:
        counts: dict[str, int] = {}
        for entry in entries:
            value = getattr(entry, attribute) or "unknown"
            counts[value] = counts.get(value, 0) + 1
        return [
            DatasetFacetBucketView(value=value, label=value.replace("_", " "), count=count)
            for value, count in sorted(counts.items())
        ]

    def _dependency_view(self, item: DatasetDependencyEntry) -> DatasetDependencyView:
        return DatasetDependencyView(
            dependency_kind=item.dependency_kind,
            dependency_id=item.dependency_id,
            dependency_label=item.dependency_label,
            target_dataset_id=item.target_dataset_id,
            direction="depends_on",
            blocking=False,
            metadata=item.payload,
        )

    def _blocking_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        candidates = [
            *self._run_dataset_dependencies(dataset_id),
            *self._backtest_dataset_dependencies(dataset_id),
            *self._dataset_reference_dependencies(dataset_id),
        ]
        deduped: dict[tuple[str, str, str | None], DatasetDependencyView] = {}
        for item in candidates:
            deduped[(item.dependency_kind, item.dependency_id, item.target_dataset_id)] = item
        return sorted(
            deduped.values(),
            key=lambda item: (item.dependency_kind, item.dependency_label or item.dependency_id),
        )

    def _run_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        items: list[DatasetDependencyView] = []
        models_root = self.repository.artifact_root / "models"
        if not models_root.exists():
            return items

        seen_run_ids: set[str] = set()
        for model_dir in sorted(models_root.iterdir()):
            if not model_dir.is_dir():
                continue
            manifest = self._load(model_dir / "train_manifest.json") or self._load(model_dir / "manifest.json")
            tracking = self.repository.read_json_if_exists(f"tracking/{model_dir.name}.json") or {}
            manifest_dataset_id = self._str(manifest.get("dataset_id"))
            tracking_dataset_id = self._str((tracking.get("params") or {}).get("dataset_id"))
            dataset_ref_uri = self._str(manifest.get("dataset_ref_uri"))
            if dataset_id not in {manifest_dataset_id, tracking_dataset_id} and dataset_ref_uri != f"dataset://{dataset_id}":
                continue
            run_id = self._str(manifest.get("run_id")) or self._str(tracking.get("run_id")) or model_dir.name
            if run_id in seen_run_ids:
                continue
            seen_run_ids.add(run_id)
            metadata = self._load(model_dir / "metadata.json")
            model_name = (
                self._str((tracking.get("params") or {}).get("model_name"))
                or self._str((manifest.get("model_artifact") or {}).get("metadata", {}).get("model_name"))
                or self._str(metadata.get("model_name"))
                or run_id
            )
            items.append(
                DatasetDependencyView(
                    dependency_kind="run",
                    dependency_id=run_id,
                    dependency_label=model_name,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/runs/{run_id}",
                    metadata={
                        "run_id": run_id,
                        "model_name": model_name,
                        "artifact_dir": self.repository.display_uri(model_dir),
                    },
                )
            )
        return items

    def _backtest_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        summary = self.repository.read_json_if_exists("workflows/backtest/backtest_summary.json") or {}
        summary_dataset_id = self._str(summary.get("dataset_id"))
        run_ids = {item.dependency_id for item in self._run_dataset_dependencies(dataset_id)}
        items: list[DatasetDependencyView] = []
        for row in summary.get("rows", []):
            if not isinstance(row, dict):
                continue
            run_id = self._str(row.get("run_id"))
            if summary_dataset_id != dataset_id and run_id not in run_ids:
                continue
            backtest_id = self._backtest_id(row.get("research_result_uri"))
            model_name = self._str(row.get("model_name")) or run_id or backtest_id
            items.append(
                DatasetDependencyView(
                    dependency_kind="backtest",
                    dependency_id=backtest_id,
                    dependency_label=model_name,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/backtests/{backtest_id}",
                    metadata={
                        "run_id": run_id,
                        "model_name": model_name,
                        "prediction_scope": self._str(summary.get("prediction_scope")),
                    },
                )
            )
        return items

    def _dataset_reference_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        target_tokens = {dataset_id, f"dataset://{dataset_id}"}
        items: list[DatasetDependencyView] = []
        for entry in self.dataset_registry.list_entries():
            if entry.dataset_id == dataset_id:
                continue
            matched_paths = self._find_dataset_reference_paths(entry.payload, target_tokens)
            matched_paths.extend(self._find_dataset_reference_paths(entry.manifest, target_tokens))
            if not matched_paths:
                continue
            dependency_kind = "fusion_dataset" if self._is_fusion_entry(entry, matched_paths) else "training_panel"
            items.append(
                DatasetDependencyView(
                    dependency_kind=dependency_kind,
                    dependency_id=entry.dataset_id,
                    dependency_label=entry.snapshot_version or entry.dataset_id,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/datasets/{entry.dataset_id}",
                    metadata={
                        "dataset_type": entry.dataset_type,
                        "data_domain": entry.data_domain,
                        "matched_paths": matched_paths[:12],
                    },
                )
            )
        return items

    def _is_fusion_entry(self, entry: DatasetRegistryEntry, matched_paths: list[str]) -> bool:
        return "fusion" in " ".join([entry.dataset_type, entry.data_domain, *matched_paths]).lower()

    def _find_dataset_reference_paths(
        self,
        payload: Any,
        target_tokens: set[str],
        path: str = "$",
    ) -> list[str]:
        if isinstance(payload, dict):
            matched: list[str] = []
            for key, value in payload.items():
                matched.extend(self._find_dataset_reference_paths(value, target_tokens, f"{path}.{key}"))
            return matched
        if isinstance(payload, list):
            matched = []
            for index, value in enumerate(payload):
                matched.extend(self._find_dataset_reference_paths(value, target_tokens, f"{path}[{index}]"))
            return matched
        if isinstance(payload, str) and payload in target_tokens:
            return [path]
        return []

    def _dataset_option(
        self,
        value: str,
        label: str,
        description: str | None = None,
        recommended: bool = False,
    ) -> DatasetRequestOptionView:
        return DatasetRequestOptionView(
            value=value,
            label=label,
            description=description,
            recommended=recommended,
        )

    def _dataset_input_refs(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        input_refs = (payload.get("feature_view_ref") or {}).get("input_data_refs")
        if not isinstance(input_refs, list):
            return []
        return [item for item in input_refs if isinstance(item, dict)]

    def _dataset_manifest(self, payload: dict[str, Any]) -> dict[str, Any]:
        manifest_uri = self._str(payload.get("dataset_manifest_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        if manifest_uri:
            return self._load(self._resolve_artifact_path(manifest_uri))
        fallback_path = self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
        return self._load(fallback_path)

    def _resolve_artifact_path(self, uri: str) -> Path:
        if uri.startswith("artifact://"):
            return self.repository.artifact_root / uri.removeprefix("artifact://")
        return self.repository.resolve_uri(uri.replace("\\", "/"))

    def _delete_dataset_artifacts(self, entry: DatasetRegistryEntry) -> list[str]:
        dataset_dir = self.repository.artifact_root / "datasets"
        candidate_paths: list[Path] = []
        for pattern in [f"{entry.dataset_id}_*.json", f"{entry.dataset_id}.json"]:
            candidate_paths.extend(dataset_dir.glob(pattern))
        for uri in [
            entry.ref_uri,
            entry.manifest_uri,
            entry.samples_uri,
            entry.feature_view_uri,
            self._str((entry.payload.get("feature_view_ref") or {}).get("storage_uri")),
        ]:
            if isinstance(uri, str) and uri:
                candidate_paths.append(self._resolve_artifact_path(uri))

        all_entries = self.dataset_registry.list_entries()
        for dependency in self.dataset_registry.list_dependencies(entry.dataset_id):
            if dependency.dependency_kind != "data_asset":
                continue
            is_shared = any(
                upstream.dependency_kind == "data_asset" and upstream.dependency_id == dependency.dependency_id
                for dataset_entry in all_entries
                if dataset_entry.dataset_id != entry.dataset_id
                for upstream in self.dataset_registry.list_dependencies(dataset_entry.dataset_id)
            )
            if is_shared:
                continue
            for pattern in [f"{dependency.dependency_id}_*.json", f"{dependency.dependency_id}.json"]:
                candidate_paths.extend(dataset_dir.glob(pattern))

        deleted_files: list[str] = []
        seen: set[Path] = set()
        for path in candidate_paths:
            resolved = path.resolve()
            if resolved in seen or not resolved.exists():
                continue
            seen.add(resolved)
            try:
                resolved.relative_to(self.repository.artifact_root)
            except ValueError:
                continue
            if resolved.is_dir():
                shutil.rmtree(resolved)
            else:
                resolved.unlink(missing_ok=True)
            deleted_files.append(self.repository.display_uri(resolved))
        return sorted(deleted_files)

    def _dataset_sample_count(self, payload: dict[str, Any]) -> int | None:
        samples_uri = self._str(payload.get("dataset_samples_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        path = (
            self._resolve_artifact_path(samples_uri)
            if samples_uri
            else self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
        )
        rows = self._load(path).get("samples", [])
        return len(rows) if isinstance(rows, list) else None

    def _dataset_bars_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for input_ref in self._dataset_input_refs(payload):
            storage_uri = self._str(input_ref.get("storage_uri"))
            if not storage_uri:
                continue
            loaded_rows = self._load(self._resolve_artifact_path(storage_uri)).get("rows", [])
            if isinstance(loaded_rows, list):
                rows.extend(item for item in loaded_rows if isinstance(item, dict))
        return rows

    def _normalized_market_bars_from_payload(self, payload: dict[str, Any]) -> list[NormalizedMarketBar]:
        bars: list[NormalizedMarketBar] = []
        for row in self._dataset_bars_rows(payload):
            if not {"event_time", "available_time", "symbol", "open", "high", "low", "close", "volume"}.issubset(row):
                continue
            try:
                bars.append(NormalizedMarketBar.model_validate(row))
            except Exception:  # noqa: BLE001
                continue
        bars.sort(key=lambda item: (item.event_time, item.symbol))
        return bars

    def _dataset_raw_row_count(self, payload: dict[str, Any]) -> int | None:
        rows = self._dataset_bars_rows(payload)
        return len(rows) if rows else None

    def _dataset_summary(self, payload: dict[str, Any]) -> DatasetSummaryView:
        input_refs = self._dataset_input_refs(payload)
        manifest = self._dataset_manifest(payload)
        as_of_time = self._dt((payload.get("feature_view_ref") or {}).get("as_of_time"))
        display_meta = self._dataset_display_meta(payload)
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        sample_count = self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload)
        row_count = self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload) or sample_count
        starts = [
            self._dt(ref.get("time_range", {}).get("start"))
            for ref in input_refs
            if isinstance(ref.get("time_range"), dict)
        ]
        ends = [
            self._dt(ref.get("time_range", {}).get("end"))
            for ref in input_refs
            if isinstance(ref.get("time_range"), dict)
        ]
        data_start = min((item for item in starts if item is not None), default=None)
        data_end = max((item for item in ends if item is not None), default=None)
        symbols_preview = list(
            dict.fromkeys(
                [
                    value
                    for value in (
                        self._str(ref.get("symbol")) or self._asset_label(self._str(ref.get("asset_id")))
                        for ref in input_refs
                    )
                    if value
                ]
            )
        )
        frequencies = list(dict.fromkeys([value for value in (self._str(ref.get("frequency")) for ref in input_refs) if value]))
        sources = list(dict.fromkeys([value for value in (self._str(ref.get("source")) for ref in input_refs) if value]))
        venues = list(dict.fromkeys([value for value in (self._str(ref.get("venue")) for ref in input_refs) if value]))
        entity_scope = (
            self._str(payload.get("entity_scope"))
            or self._str(manifest.get("entity_scope"))
            or self._str((payload.get("sample_policy") or {}).get("universe"))
            or ("multi_asset" if len(symbols_preview) > 1 else "single_asset")
        )
        entity_count = (
            self._int_or_none(payload.get("entity_count"))
            or self._int_or_none(manifest.get("entity_count"))
            or max(len(symbols_preview), 1)
        )
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        data_domains = self._resolved_data_domains(acquisition_profile)
        source_vendor = self._str(acquisition_profile.get("source_vendor")) or (sources[0] if len(sources) == 1 else None)
        exchange = self._str(acquisition_profile.get("exchange")) or (venues[0] if len(venues) == 1 else None)
        frequency = frequencies[0] if len(frequencies) == 1 else (frequencies[0] if frequencies else None)
        asset_ids = [self._str(ref.get("asset_id")) for ref in input_refs if self._str(ref.get("asset_id"))]
        dataset_id = str(payload.get("dataset_id", "unknown"))
        quality_status = self._str(manifest.get("quality_status"))
        readiness_status = self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status"))
        build_status = self._str(manifest.get("build_status"))
        request_origin = self._str(acquisition_profile.get("request_origin")) or next(
            (self._str(ref.get("request_origin")) for ref in input_refs if self._str(ref.get("request_origin"))),
            None,
        )
        return DatasetSummaryView(
            dataset_id=dataset_id,
            display_name=display_meta["display_name"],
            subtitle=display_meta["subtitle"],
            dataset_category=display_meta["dataset_category"],
            data_domain=self._str(acquisition_profile.get("data_domain")) or "market",
            data_domains=data_domains,
            dataset_type=self._resolved_dataset_type(payload, manifest),
            asset_id=(asset_ids[0] if len(asset_ids) == 1 else None),
            data_source=source_vendor or (sources[0] if len(sources) == 1 else None),
            frequency=frequency,
            as_of_time=as_of_time,
            sample_count=sample_count,
            row_count=row_count,
            feature_count=len(feature_schema),
            label_count=len(label_columns),
            label_horizon=int((payload.get("label_spec") or {}).get("horizon", 0) or 0),
            split_strategy=self._str((payload.get("split_manifest") or {}).get("strategy")),
            time_range_label=self._time_range_label(data_start, data_end, frequency),
            is_smoke="smoke" in dataset_id,
            freshness=DatasetFreshnessView(
                as_of_time=as_of_time,
                data_start_time=data_start,
                data_end_time=data_end,
                lag_seconds=self._lag_seconds(as_of_time, data_end),
                status=self._freshness_status(as_of_time, data_end),
                summary=self._freshness_summary(as_of_time, data_end),
            ),
            temporal_safety_summary=self._temporal_safety_summary(payload),
            source_vendor=source_vendor,
            exchange=exchange,
            entity_scope=entity_scope,
            entity_count=entity_count,
            symbols_preview=symbols_preview[:6],
            snapshot_version=self._str(manifest.get("snapshot_version")),
            quality_status=quality_status,
            readiness_status=readiness_status,
            build_status=build_status,
            request_origin=request_origin,
            links=[
                DeepLinkView(
                    kind="dataset_detail",
                    label=display_meta["display_name"],
                    href=f"/datasets/{dataset_id}",
                    api_path=f"/api/datasets/{dataset_id}",
                )
            ],
        )

    def _experiment_item(self, run_id: str) -> ExperimentListItem:
        detail = self.get_run_detail(run_id)
        if detail is None:
            return ExperimentListItem(run_id=run_id, model_name=run_id, status="unknown")
        mae = detail.metrics.get("mae")
        return ExperimentListItem(
            run_id=detail.run_id,
            model_name=detail.model_name,
            dataset_id=detail.dataset_id,
            family=detail.family,
            backend=detail.backend,
            status=detail.status,
            created_at=detail.created_at,
            primary_metric_name=("mae" if mae is not None else None),
            primary_metric_value=mae,
            metrics=detail.metrics,
            backtest_count=len(detail.related_backtests),
            prediction_scopes=[p.scope for p in detail.predictions],
            tags={},
        )

    def _builtin_templates(self) -> list[ModelTemplateView]:
        now = datetime.now(UTC)
        return [
            ModelTemplateView(
                template_id=f"registry::{name}",
                name=f"{name} default",
                model_name=name,
                description="Template sourced from model registry.",
                source="registry",
                hyperparams=dict(self._entry(entry, "default_hyperparams", {})),
                trainer_preset="fast",
                dataset_preset="smoke",
                read_only=True,
                model_registered=True,
                created_at=now,
                updated_at=now,
            )
            for name, entry in sorted(self.model_registry_entries.items())
            if bool(self._entry(entry, "enabled", True))
        ]

    def _custom_templates(self) -> list[ModelTemplateView]:
        items: list[ModelTemplateView] = []
        for path in self.templates_root.glob("*.json"):
            try:
                items.append(ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception:  # noqa: BLE001
                continue
        return items

    def _registry_models(self) -> set[str]:
        return {
            name
            for name, entry in self.model_registry_entries.items()
            if bool(self._entry(entry, "enabled", True))
        }

    def _entry(self, entry: Any, key: str, default: Any) -> Any:
        return entry.get(key, default) if isinstance(entry, dict) else getattr(entry, key, default)

    def _review_unavailable(self) -> ReviewSummaryView:
        return ReviewSummaryView(
            status="unavailable",
            title="Review unavailable",
            summary="No review artifact found.",
            suggested_actions=[],
            proposed_actions=[],
        )

    def _glossary(self, keys: list[str]) -> list[GlossaryHintView]:
        dictionary = {
            "mae": ("MAE", "Average absolute prediction error."),
            "prediction_scope": ("预测范围", "说明预测结果覆盖全量样本还是测试切片。"),
            "benchmark": ("基准测试", "在同一规则下横向评估多个模型。"),
            "consistency_check": ("一致性检查", "检查研究引擎和模拟引擎结果是否一致。"),
            "max_drawdown": ("最大回撤", "累计收益从高点回落到低点的最大跌幅。"),
            "as_of_time": ("as_of_time", "这份数据在什么可用时点被截面固定下来。"),
            "freshness": ("新鲜度", "数据距离最新可用市场状态有多近。"),
            "label_horizon": ("标签窗口", "每个样本要预测未来多少个 bar。"),
            "split_strategy": ("切分方式", "训练、验证、测试样本是按什么规则拆分的。"),
            "sample_policy": ("样本策略", "哪些样本能进入训练，以及缺失标签如何处理。"),
            "temporal_safety": ("时间安全", "是否严格只使用当时能观测到的信息。"),
            "missing_ratio": ("缺失率", "关键字段中缺失值占全部样本的比例。"),
            "duplicate_rows": ("重复率", "重复时间点或重复样本写入的情况。"),
            "feature_dimensions": ("特征维度", "模型可用输入特征的数量和类别。"),
            "label_columns": ("标签列", "训练目标字段，也就是模型要学会预测的值。"),
            "data_coverage": ("数据覆盖范围", "数据覆盖的资产、周期和时间范围。"),
        }
        return [
            GlossaryHintView(key=key, term=dictionary[key][0], short=dictionary[key][1])
            for key in keys
            if key in dictionary
        ]

    def _dataset_display_meta(self, payload: dict[str, Any]) -> dict[str, str]:
        dataset_id = str(payload.get("dataset_id", "unknown_dataset"))
        input_refs = self._dataset_input_refs(payload)
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        frequency_candidates = [
            self._str(ref.get("frequency")) for ref in input_refs if self._str(ref.get("frequency"))
        ]
        symbols = [
            self._str(ref.get("symbol")) or self._asset_label(self._str(ref.get("asset_id")))
            for ref in input_refs
        ]
        symbols = [value for value in symbols if value]
        source_candidates = [
            self._str(ref.get("source")) for ref in input_refs if self._str(ref.get("source"))
        ]
        frequency = frequency_candidates[0] if frequency_candidates else "unknown"
        source_vendor = self._str(acquisition_profile.get("source_vendor")) or (
            source_candidates[0] if source_candidates else "unknown_source"
        )
        entity_scope = self._str(manifest.get("entity_scope")) or (
            "multi_asset" if len(set(symbols)) > 1 else "single_asset"
        )
        snapshot_version = self._str(manifest.get("snapshot_version"))
        known = {
            "smoke_dataset": {
                "display_name": "烟雾测试数据集 / Smoke Dataset",
                "subtitle": "最小可运行样例，用于快速验证训练与回测链路。",
                "dataset_category": "演示与联调",
            },
            "baseline_real_benchmark_dataset": {
                "display_name": "BTC 1小时真实基准数据集 / Real Benchmark",
                "subtitle": "真实 Binance 行情驱动的 1 小时收益预测基准集。",
                "dataset_category": "真实研究基准",
            },
            "baseline_reference_benchmark_dataset": {
                "display_name": "BTC 1小时参考基准数据集 / Reference Benchmark",
                "subtitle": "参考样本版 benchmark，用于和真实链路做对照。",
                "dataset_category": "参考基准",
            },
            "baseline_benchmark_dataset": {
                "display_name": "BTC 1小时基准数据集 / Baseline Benchmark",
                "subtitle": "基础收益预测 benchmark，适合作为模型横向比较底板。",
                "dataset_category": "研究基准",
            },
        }.get(dataset_id)
        if known:
            return known
        dataset_type_key = self._resolved_dataset_type(payload, manifest)
        dataset_type = {
            "training_panel": "训练面板",
            "fusion_training_panel": "融合训练面板",
            "feature_snapshot": "特征快照",
            "display_slice": "展示切片",
        }.get(dataset_type_key, "展示切片")
        domain_label = {
            "market": "市场",
            "derivatives": "衍生品",
            "macro": "宏观",
            "on_chain": "链上",
            "sentiment_events": "情绪事件",
        }.get(str(acquisition_profile.get("data_domain") or "market"), "数据")
        scope_label = "多资产" if entity_scope == "multi_asset" else (symbols[0] if symbols else "单资产")
        version = snapshot_version or dataset_id
        base_name = f"{domain_label} / {source_vendor} / {dataset_type} / {version}"
        subtitle = f"{scope_label} · {self._frequency_label(frequency)} · 技术标识 {dataset_id}"
        category = dataset_type
        return {
            "display_name": base_name,
            "subtitle": subtitle,
            "dataset_category": category,
        }

    def _dataset_detail_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        summary = self._dataset_summary(payload)
        manifest = self._dataset_manifest(payload)
        label_spec = payload.get("label_spec") or {}
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        feature_groups = self._feature_groups(feature_schema)
        symbols = summary.symbols_preview or ["目标市场"]
        symbol = "、".join(symbols[:3])
        freq_label = self._frequency_label(summary.frequency)
        time_label = summary.time_range_label or "时间范围待确认"
        task_kind = self._str(label_spec.get("kind")) or "预测"
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        dataset_type = self._resolved_dataset_type(payload, manifest)
        if dataset_type == "fusion_training_panel":
            fusion_domains = acquisition_profile.get("fusion_domains")
            fusion_label = (
                " / ".join(str(item) for item in fusion_domains)
                if isinstance(fusion_domains, list) and fusion_domains
                else "market / macro / on_chain"
            )
            intended_use = (
                f"适合把 {symbol} 的 {freq_label} 市场样本与 {fusion_label} 额外信号对齐后，用作跨域训练与 readiness 验证。"
            )
        else:
            intended_use = (
                f"适合用来做 {symbol} 的 {freq_label} 频率 {task_kind} 训练、walk-forward 基准比较和回测前置数据准备。"
            )
        risk_note = (
            "如果时间覆盖过短、缺失率偏高，或者标签窗口和你的交易周期不匹配，模型结论会明显失真。"
        )
        summary_text = (
            f"这是一份围绕 {symbol} 构建的 {freq_label} 频率数据集，"
            f"当前覆盖 {time_label}，包含 {summary.feature_count or 0} 个特征维度和 {summary.label_count or 0} 个标签列。"
        )
        acquisition_profile = {
            "source_vendor": summary.source_vendor,
            "exchange": summary.exchange,
            "request_origin": summary.request_origin,
            **acquisition_profile,
        }
        build_profile = {
            "build_status": self._str(manifest.get("build_status")) or "success",
            "snapshot_version": summary.snapshot_version,
            "entity_scope": summary.entity_scope,
            "entity_count": summary.entity_count,
            "raw_row_count": self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload),
            "usable_sample_count": self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload),
            "dropped_rows": self._int_or_none(manifest.get("dropped_rows")),
            "input_asset_ids": list(manifest.get("input_asset_ids") or []),
            "build_config": dict(manifest.get("build_config") or {}),
        }
        schema_profile = {
            "feature_count": len(feature_schema),
            "feature_schema_hash": self._str(payload.get("feature_schema_hash")) or self._str(manifest.get("feature_schema_hash")),
            "label_columns": label_columns,
            "label_schema_hash": self._str(payload.get("label_schema_hash")) or self._str(manifest.get("label_schema_hash")),
            "feature_dimension_consistent": bool(feature_schema),
            "missing_feature_policy": dict((manifest.get("build_config") or {}).get("missing_feature_policy") or {}),
        }
        readiness_profile = {
            "readiness_status": self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status")) or "unknown",
            "quality_status": self._str(manifest.get("quality_status")) or "unknown",
            "freshness_status": self._str(manifest.get("freshness_status")) or summary.freshness.status,
            "temporal_safety_status": self._str(manifest.get("temporal_safety_status")) or "unknown",
            "alignment_status": self._str(manifest.get("alignment_status")) or "unknown",
            "label_alignment_status": self._str(manifest.get("label_alignment_status")) or "unknown",
        }
        training_profile = {
            "sample_policy": dict(payload.get("sample_policy") or {}),
            "split_manifest": dict(payload.get("split_manifest") or {}),
            "entity_scope": summary.entity_scope,
            "entity_count": summary.entity_count,
            "symbols_preview": summary.symbols_preview,
            "label_horizon": summary.label_horizon,
            "recommended_training_use": (payload.get("sample_policy") or {}).get("recommended_training_use"),
        }
        return {
            "summary": summary_text,
            "intended_use": intended_use,
            "risk_note": risk_note,
            "feature_columns_preview": [item.get("name", "") for item in feature_schema[:8] if item.get("name")],
            "label_columns": label_columns,
            "feature_groups": feature_groups,
            "acquisition_profile": acquisition_profile,
            "build_profile": build_profile,
            "schema_profile": schema_profile,
            "readiness_profile": readiness_profile,
            "training_profile": training_profile,
        }

    def _dataset_quality_summary(self, payload: dict[str, Any]) -> DatasetQualitySummaryView:
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        missing_ratio = 0.0
        duplicate_rows = 0
        duplicate_ratio = 0.0
        checks: list[str] = []
        dataset_type = self._resolved_dataset_type(payload, manifest)
        is_multi_domain = len(self._resolved_data_domains(acquisition_profile)) > 1
        if dataset_type == "fusion_training_panel" or is_multi_domain:
            samples = self._load_dataset_samples(payload)
            if samples:
                total_cells = 0
                missing_cells = 0
                seen_keys: set[tuple[str, str]] = set()
                duplicates = 0
                for sample in samples:
                    total_cells += len(sample.features)
                    missing_cells += sum(
                        1
                        for value in sample.features.values()
                        if value is None or value == ""
                    )
                    key = (sample.entity_key, sample.timestamp.isoformat())
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)
                missing_ratio = (missing_cells / total_cells) if total_cells else 0.0
                duplicate_rows = duplicates
                duplicate_ratio = (duplicates / len(samples)) if samples else 0.0
                checks.append("已基于融合训练样本统计缺失特征和重复键情况。")
            else:
                checks.append("暂未找到物化后的融合训练样本，质量指标使用保守空值。")
        else:
            rows = self._dataset_bars_rows(payload)
            if isinstance(rows, list) and rows:
                total_cells = 0
                missing_cells = 0
                seen_keys: set[tuple[str, str]] = set()
                duplicates = 0
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    total_cells += len(row)
                    missing_cells += sum(
                        1 for value in row.values() if value is None or value == ""
                    )
                    event_time = str(row.get("event_time", ""))
                    symbol = str(row.get("symbol") or "")
                    key = (event_time, symbol)
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)
                missing_ratio = (missing_cells / total_cells) if total_cells else 0.0
                duplicate_rows = duplicates
                duplicate_ratio = (duplicates / len(rows)) if rows else 0.0
                checks.append("已基于 OHLCV 原始记录统计缺失和重复情况。")
            else:
                checks.append("暂未找到可供统计的底层行数据，质量指标使用保守空值。")

        status = self._str(manifest.get("quality_status")) or "healthy"
        if missing_ratio > 0.05 or duplicate_ratio > 0.01:
            status = "warning"
        if missing_ratio > 0.15 or duplicate_ratio > 0.05:
            status = "risk"
        summary = "数据质量整体稳定，可直接进入研究使用。"
        if status == "warning":
            summary = "存在轻度质量风险，建议先检查缺失段和重复时点。"
        if status == "risk":
            summary = "质量风险较高，建议清洗后再用于训练或回测。"
        return DatasetQualitySummaryView(
            status=status,
            summary=summary,
            missing_ratio=missing_ratio,
            duplicate_ratio=duplicate_ratio,
            duplicate_rows=duplicate_rows,
            checks=checks,
        )

    def _feature_schema(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        feature_schema = (payload.get("feature_view_ref") or {}).get("feature_schema")
        return [item for item in feature_schema if isinstance(item, dict)] if isinstance(feature_schema, list) else []

    def _label_columns(self, payload: dict[str, Any]) -> list[str]:
        label_spec = payload.get("label_spec") or {}
        labels: list[str] = []
        target = label_spec.get("target_column")
        if isinstance(target, str) and target:
            labels.append(target)
        extra = label_spec.get("label_columns")
        if isinstance(extra, list):
            labels.extend(str(item) for item in extra if isinstance(item, str) and item)
        return list(dict.fromkeys(labels))

    def _feature_groups(self, feature_schema: list[dict[str, Any]]) -> list[DatasetFieldGroupView]:
        buckets: dict[str, list[str]] = {
            "price_action": [],
            "volume_liquidity": [],
            "volatility_range": [],
            "other": [],
        }
        for item in feature_schema:
            name = str(item.get("name", ""))
            lowered = name.lower()
            if any(token in lowered for token in ["return", "momentum", "close", "open"]):
                buckets["price_action"].append(name)
            elif any(token in lowered for token in ["volume", "turnover", "liquidity"]):
                buckets["volume_liquidity"].append(name)
            elif any(token in lowered for token in ["vol", "range", "drawdown"]):
                buckets["volatility_range"].append(name)
            else:
                buckets["other"].append(name)
        labels = {
            "price_action": ("价格行为特征", "帮助模型判断趋势、收益和方向变化。"),
            "volume_liquidity": ("成交与流动性特征", "帮助识别量能放大、拥挤交易和流动性状态。"),
            "volatility_range": ("波动与区间特征", "帮助刻画波动率、振幅和风险抬升。"),
            "other": ("其他特征", "补充上下文或暂未归类的工程字段。"),
        }
        return [
            DatasetFieldGroupView(
                key=key,
                label=labels[key][0],
                description=labels[key][1],
                count=len(columns),
                columns=columns[:6],
            )
            for key, columns in buckets.items()
            if columns
        ]

    def _frequency_label(self, frequency: str | None) -> str:
        mapping = {"1m": "1分钟", "5m": "5分钟", "1h": "1小时", "1d": "日线"}
        return mapping.get(frequency or "", frequency or "未知周期")

    def _asset_label(self, asset_id: str | None) -> str | None:
        if not asset_id:
            return None
        parts = asset_id.replace("-", "_").split("_")
        for part in parts:
            if part.endswith("USDT") or part.endswith("USD"):
                return part
        return asset_id

    def _time_range_label(
        self,
        start_time: datetime | None,
        end_time: datetime | None,
        frequency: str | None,
    ) -> str | None:
        if start_time is None and end_time is None:
            return None
        start_label = start_time.strftime("%Y-%m-%d") if start_time else "未知开始"
        end_label = end_time.strftime("%Y-%m-%d") if end_time else "未知结束"
        freq_label = self._frequency_label(frequency)
        return f"{start_label} 至 {end_label} · {freq_label}"

    def _lag_seconds(self, as_of_time: datetime | None, data_end_time: datetime | None) -> float | None:
        if as_of_time is None or data_end_time is None:
            return None
        return max((as_of_time - data_end_time).total_seconds(), 0.0)

    def _freshness_status(self, as_of_time: datetime | None, data_end_time: datetime | None) -> str:
        lag_seconds = self._lag_seconds(as_of_time, data_end_time)
        if lag_seconds is None:
            return "unknown"
        if lag_seconds <= 3600:
            return "fresh"
        if lag_seconds <= 86400:
            return "stale"
        return "outdated"

    def _freshness_summary(self, as_of_time: datetime | None, data_end_time: datetime | None) -> str:
        status = self._freshness_status(as_of_time, data_end_time)
        mapping = {
            "fresh": "数据时间与最新可用截面基本对齐。",
            "stale": "数据存在轻微滞后，适合研究但应关注时效性。",
            "outdated": "数据已明显过时，结论可能无法反映当前市场。",
            "unknown": "缺少足够时间信息，暂时无法判断新鲜度。",
        }
        return mapping[status]

    def _temporal_safety_summary(self, payload: dict[str, Any]) -> str:
        as_of_time = self._dt((payload.get("feature_view_ref") or {}).get("as_of_time"))
        end_candidates = [
            self._dt(ref.get("time_range", {}).get("end"))
            for ref in self._dataset_input_refs(payload)
            if isinstance(ref.get("time_range"), dict)
        ]
        end_time = max((item for item in end_candidates if item is not None), default=None)
        if as_of_time and end_time and as_of_time >= end_time:
            return "以 as_of_time 固定观测边界，当前看起来没有明显前视泄漏。"
        return "需要结合 available_time 和切分边界继续确认时间安全性。"

    def _artifacts(self, pairs: list[tuple[str, Path]]) -> list[ArtifactView]:
        return [
            ArtifactView(
                kind=kind,
                label=kind.replace("_", " "),
                uri=self.repository.display_uri(path),
                exists=True,
                previewable=path.suffix.lower() in {".json", ".md", ".txt", ".csv"},
            )
            for kind, path in pairs
            if path.exists()
        ]

    def _load(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def _metrics(self, value: Any) -> dict[str, float]:
        if not isinstance(value, dict):
            return {}
        return {str(k): float(v) for k, v in value.items() if isinstance(v, (int, float))}

    def _dt(self, value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _str(self, value: Any) -> str | None:
        return str(value) if isinstance(value, str) and value else None

    def _int_or_none(self, value: Any) -> int | None:
        return int(value) if isinstance(value, (int, float)) else None

    def _float(self, value: Any) -> float | None:
        return float(value) if isinstance(value, (int, float)) else None

    def _backend(self, model_name: str) -> str | None:
        if model_name in {"mlp", "gru"}:
            return "torch"
        if model_name in {"elastic_net", "lightgbm", "mean_baseline"}:
            return "native"
        return None
