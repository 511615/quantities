from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.config.loader import load_app_config
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.ingestion import DataConnectorError
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryRepository
from quant_platform.webapi.services.catalog import ResearchWorkbenchService
from quant_platform.webapi.services.jobs import JobService
from quant_platform.webapi.services.model_cleanup import ModelCleanupService


@dataclass
class AppServices:
    workbench: ResearchWorkbenchService
    jobs: JobService


def get_services(request: Request) -> AppServices:
    return request.app.state.services  # type: ignore[no-any-return]


ServicesDep = Annotated[AppServices, Depends(get_services)]


ENV_TEST_ARTIFACT_ROOT = "QUANT_PLATFORM_TEST_ARTIFACT_ROOT"
SPA_HTML_HEADERS = {"Cache-Control": "no-store, no-cache, must-revalidate"}
logger = logging.getLogger(__name__)


def _resolve_app_artifact_root(config, override: Path | None) -> Path:
    if override is not None:
        candidate = override
    else:
        env_override = os.getenv(ENV_TEST_ARTIFACT_ROOT)
        candidate = Path(env_override) if env_override else Path(config.env.artifact_root)
    candidate = candidate.resolve()
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def _bootstrap_test_artifacts(facade: QuantPlatformFacade, artifact_root: Path) -> None:
    if not os.getenv(ENV_TEST_ARTIFACT_ROOT):
        return
    workspace_root = Path(__file__).resolve().parents[3]
    source_datasets_root = workspace_root / "artifacts" / "datasets"
    source_models_root = workspace_root / "artifacts" / "models"
    if source_datasets_root.exists():
        lightweight_official_multimodal_files = {
            "official_reddit_pullpush_multimodal_v2_fusion_dataset_ref.json",
            "official_reddit_pullpush_multimodal_v2_fusion_dataset_manifest.json",
            "official_reddit_pullpush_multimodal_v2_fusion_dataset_samples.json",
            "official_reddit_pullpush_multimodal_v2_fusion_feature_rows.json",
            "official_reddit_pullpush_multimodal_v2_fusion_feature_view_ref.json",
        }

        def _should_copy_dataset_artifact(file_name: str) -> bool:
            if not file_name.startswith("official_reddit_pullpush_multimodal_v2_fusion"):
                return True
            return file_name in lightweight_official_multimodal_files

        def _rewrite_dataset_uri(uri: object) -> object:
            if not isinstance(uri, str) or not uri:
                return uri
            source_path = Path(uri)
            if not source_path.is_absolute():
                return uri
            try:
                source_path.resolve().relative_to(source_datasets_root.resolve())
            except ValueError:
                return uri
            if not _should_copy_dataset_artifact(source_path.name):
                return str(source_path)
            target_path = artifact_root / "datasets" / source_path.name
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if source_path.exists() and not target_path.exists():
                shutil.copy2(source_path, target_path)
            return str(target_path)

        for dataset_id in (
            "baseline_real_benchmark_dataset",
            "official_reddit_pullpush_multimodal_v2_fusion",
        ):
            for source_path in source_datasets_root.glob(f"{dataset_id}_*"):
                if not _should_copy_dataset_artifact(source_path.name):
                    continue
                target_path = artifact_root / "datasets" / source_path.name
                target_path.parent.mkdir(parents=True, exist_ok=True)
                if not target_path.exists():
                    shutil.copy2(source_path, target_path)
            dataset_ref_path = source_datasets_root / f"{dataset_id}_dataset_ref.json"
            if dataset_ref_path.exists():
                try:
                    dataset_ref_payload = json.loads(dataset_ref_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    dataset_ref_payload = {}
                for field_name in ("dataset_manifest_uri", "dataset_samples_uri"):
                    dataset_ref_payload[field_name] = _rewrite_dataset_uri(dataset_ref_payload.get(field_name))
                feature_view_ref = dataset_ref_payload.get("feature_view_ref") or {}
                if isinstance(feature_view_ref, dict):
                    feature_view_ref["storage_uri"] = _rewrite_dataset_uri(feature_view_ref.get("storage_uri"))
                input_data_refs = feature_view_ref.get("input_data_refs") or []
                if isinstance(input_data_refs, list):
                    for input_ref in input_data_refs:
                        if not isinstance(input_ref, dict):
                            continue
                        storage_uri = input_ref.get("storage_uri")
                        if isinstance(storage_uri, str) and storage_uri.startswith("artifact://datasets/"):
                            file_name = storage_uri.removeprefix("artifact://datasets/")
                            source_path = source_datasets_root / file_name
                            if _should_copy_dataset_artifact(file_name):
                                target_path = artifact_root / "datasets" / file_name
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                if source_path.exists() and not target_path.exists():
                                    shutil.copy2(source_path, target_path)
                            elif source_path.exists():
                                input_ref["storage_uri"] = str(source_path)
                        else:
                            input_ref["storage_uri"] = _rewrite_dataset_uri(storage_uri)
                        asset_id = input_ref.get("asset_id")
                        if isinstance(asset_id, str) and asset_id:
                            for source_path in source_datasets_root.glob(f"{asset_id}_*"):
                                if not _should_copy_dataset_artifact(source_path.name):
                                    continue
                                target_path = artifact_root / "datasets" / source_path.name
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                if not target_path.exists():
                                    shutil.copy2(source_path, target_path)
                target_ref_path = artifact_root / "datasets" / dataset_ref_path.name
                target_ref_path.parent.mkdir(parents=True, exist_ok=True)
                target_ref_path.write_text(
                    json.dumps(dataset_ref_payload, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
    if source_models_root.exists():
        for run_id in (
            "multimodal-compose-20260413144642-80a31c",
            "workbench-train-20260413112342",
            "workbench-train-20260413111755",
        ):
            source_run_dir = source_models_root / run_id
            target_run_dir = artifact_root / "models" / run_id
            if source_run_dir.exists() and not target_run_dir.exists():
                shutil.copytree(source_run_dir, target_run_dir)
    smoke_dataset_path = artifact_root / "datasets" / "smoke_dataset_dataset_ref.json"
    if not smoke_dataset_path.exists():
        facade.build_smoke_dataset()
    smoke_run_dir = artifact_root / "models" / "smoke-train-run"
    if not smoke_run_dir.exists():
        facade.train_smoke()


def create_app(artifact_root_override: Path | None = None) -> FastAPI:
    from quant_platform.webapi.routers import (
        artifacts,
        backtests,
        benchmarks,
        comparisons,
        datasets,
        experiments,
        jobs,
        launches,
        models,
        runs,
        workbench,
    )

    config = load_app_config()
    artifact_root = _resolve_app_artifact_root(config, artifact_root_override)
    facade = QuantPlatformFacade(artifact_root)
    _bootstrap_test_artifacts(facade, artifact_root)
    repository = ArtifactRepository(artifact_root)
    ModelCleanupService(repository=repository, facade=facade).normalize_repository(
        delete_irreparable=not bool(os.getenv(ENV_TEST_ARTIFACT_ROOT))
    )
    dataset_registry = DatasetRegistryRepository(artifact_root, repository)
    registry_entries = facade.model_registry.registrations()
    workbench_service = ResearchWorkbenchService(
        repository=repository,
        dataset_registry=dataset_registry,
        store=LocalArtifactStore(artifact_root),
        model_families={name: entry.family.value for name, entry in registry_entries.items()},
        model_registry_entries=registry_entries,
        facade=facade,
    )
    try:
        workbench_service.ensure_official_multimodal_benchmark()
    except Exception:
        logger.warning(
            "Skipping official multimodal benchmark warmup during app startup.",
            exc_info=True,
        )
    app = FastAPI(title="Quant Platform Research Workbench", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1):517\d+$",
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.services = AppServices(
        workbench=workbench_service,
        jobs=JobService(
            artifact_root=artifact_root,
            workbench=workbench_service,
            facade=facade,
        ),
    )

    @app.exception_handler(DataConnectorError)
    async def handle_data_connector_error(
        request: Request,  # noqa: ARG001
        exc: DataConnectorError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "detail": exc.to_dict(),
            },
        )

    app.include_router(experiments.router)
    app.include_router(runs.router)
    app.include_router(backtests.router)
    app.include_router(benchmarks.router)
    app.include_router(comparisons.router)
    app.include_router(datasets.router)
    app.include_router(models.router)
    app.include_router(launches.router)
    app.include_router(jobs.router)
    app.include_router(workbench.router)
    app.include_router(artifacts.router)

    dist_dir = repository.workspace_root / "apps" / "web" / "dist"
    assets_dir = dist_dir / "assets"

    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

    favicon_path = dist_dir / "favicon.ico"

    @app.get("/favicon.ico", response_model=None)
    def favicon():
        if favicon_path.exists():
            return FileResponse(favicon_path)
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    @app.get("/health")
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_model=None)
    def root():
        index_path = dist_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path, headers=SPA_HTML_HEADERS)
        return JSONResponse(
            {
                "name": "Quant Platform Research Workbench",
                "api": "/api",
                "frontend": "Build apps/web to serve the SPA from this process.",
            }
        )

    @app.get("/{full_path:path}", response_model=None)
    def spa_fallback(full_path: str):
        if full_path.startswith("api/"):
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        index_path = dist_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path, headers=SPA_HTML_HEADERS)
        return JSONResponse({"detail": "Frontend build not found."}, status_code=404)

    return app
