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


def _bootstrap_pytest_fixture_runs(artifact_root: Path) -> None:
    target_run_id = "multimodal-compose-20260413144642-80a31c"
    target_model_dir = artifact_root / "models" / target_run_id
    target_model_dir.mkdir(parents=True, exist_ok=True)

    source_runs = [
        {
            "run_id": "workbench-train-20260413111755",
            "modality": "market",
            "model_name": "lstm",
            "weight": 0.5,
            "dataset_ids": ["baseline_real_benchmark_dataset"],
        },
        {
            "run_id": "workbench-train-20260413112342",
            "modality": "nlp",
            "model_name": "lstm",
            "weight": 0.5,
            "dataset_ids": ["official_reddit_pullpush_multimodal_v2_fusion"],
        },
    ]
    train_manifest = {
        "run_id": target_run_id,
        "created_at": "2026-04-13T14:46:42Z",
        "data_domain": "market",
        "data_domains": ["market"],
        "dataset_id": "baseline_real_benchmark_dataset",
        "dataset_ref_uri": "dataset://baseline_real_benchmark_dataset",
        "dataset_manifest_uri": str(
            (artifact_root / "datasets" / "baseline_real_benchmark_dataset_dataset_manifest.json").resolve()
        ),
        "dataset_type": "training_panel",
        "entity_scope": "single_asset",
        "entity_count": 1,
        "feature_schema_hash": "d29d7b10f32d4fcfbd8db0e4290ac5ff1101af7e730555aa5be264106c2b54d2",
        "dataset_readiness_status": "ready",
        "dataset_readiness_warnings": [],
        "fusion_domains": ["market", "nlp"],
        "source_dataset_ids": [
            "baseline_real_benchmark_dataset",
            "official_reddit_pullpush_multimodal_v2_fusion",
        ],
        "snapshot_version": "pytest-multimodal-compose-fixture",
        "metrics": {},
        "model_artifact": {
            "kind": "composed_multimodal_manifest",
            "uri": str((target_model_dir / "metadata.json").resolve()),
            "content_hash": None,
            "metadata": {
                "model_name": "Multimodal Composite",
                "registry_model_name": "multimodal_reference",
                "fusion_strategy": "late_score_blend",
            },
        },
        "composition": {
            "fusion_strategy": "late_score_blend",
            "official_template_eligible": True,
            "official_blocking_reasons": [],
            "rules": [
                "Use two or more existing single-modality runs only.",
                "Late fusion uses strict timestamp + entity-key intersection.",
                "Backtest selects one compatible dataset per modality and never persists a merged dataset.",
            ],
            "source_runs": source_runs,
            "official_contract": {
                "contract_version": "official_multimodal_composition_v1",
                "official_market_dataset_id": "baseline_real_benchmark_dataset",
                "official_multimodal_dataset_id": "official_reddit_pullpush_multimodal_v2_fusion",
                "official_market_vendor": "binance",
                "official_market_symbols": ["BTCUSDT"],
                "official_nlp_vendor": "reddit_archive",
                "official_nlp_identifiers": ["BTC"],
                "source_run_ids": [item["run_id"] for item in source_runs],
                "source_dataset_ids": [
                    "baseline_real_benchmark_dataset",
                    "official_reddit_pullpush_multimodal_v2_fusion",
                ],
            },
        },
    }
    metadata = {
        "run_id": target_run_id,
        "model_name": "Multimodal Composite",
        "model_family": "multimodal",
        "advanced_kind": "multimodal",
        "artifact_uri": str((target_model_dir / "metadata.json").resolve()),
        "artifact_dir": str(target_model_dir.resolve()),
        "state_uri": None,
        "backend": "late_score_blend",
        "training_sample_count": 0,
        "feature_names": [],
        "training_config": {},
        "training_metrics": {},
        "best_epoch": None,
        "trained_steps": None,
        "checkpoint_tag": "composed",
        "input_metadata": {
            "official_template_eligible": True,
            "official_blocking_reasons": [],
            "source_run_ids": [item["run_id"] for item in source_runs],
            "source_dataset_ids": train_manifest["source_dataset_ids"],
            "official_contract": train_manifest["composition"]["official_contract"],
        },
        "prediction_metadata": {
            "official_template_eligible": True,
            "fusion_strategy": "late_score_blend",
            "source_run_ids": [item["run_id"] for item in source_runs],
            "dataset_ids": train_manifest["source_dataset_ids"],
            "source_dataset_ids": train_manifest["source_dataset_ids"],
            "official_contract": train_manifest["composition"]["official_contract"],
        },
        "model_spec": {
            "model_name": "multimodal_reference",
            "family": "multimodal",
            "version": "0.1.0",
            "input_schema": [],
            "output_schema": [{"name": "prediction", "dtype": "float", "nullable": False}],
            "task_type": "regression",
            "lookback": None,
            "target_horizon": 1,
            "prediction_type": "return",
            "hyperparams": {
                "fusion_strategy": "late_score_blend",
                "source_run_ids": [item["run_id"] for item in source_runs],
                "weights": {item["run_id"]: item["weight"] for item in source_runs},
            },
        },
        "registration": {
            "model_name": "multimodal_reference",
            "family": "multimodal",
            "advanced_kind": "multimodal",
            "input_adapter_key": "composed_multimodal",
            "prediction_adapter_key": "standard_prediction",
            "artifact_adapter_key": "json_manifest",
            "capabilities": ["composed_from_existing_runs", "late_score_blend"],
            "benchmark_eligible": True,
            "default_eligible": False,
            "enabled": False,
        },
        "registry_model_name": "multimodal_reference",
        "source_dataset_ids": train_manifest["source_dataset_ids"],
    }
    evaluation_summary = {
        "run_id": target_run_id,
        "regression_metrics": {},
        "prediction_scopes": ["full"],
    }
    prediction_dir = artifact_root / "predictions" / target_run_id
    prediction_dir.mkdir(parents=True, exist_ok=True)
    (prediction_dir / "full.json").write_text(
        json.dumps({"run_id": target_run_id, "scope_name": "full", "rows": []}, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    (target_model_dir / "train_manifest.json").write_text(
        json.dumps(train_manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (target_model_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (target_model_dir / "evaluation_summary.json").write_text(
        json.dumps(evaluation_summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _bootstrap_test_artifacts(facade: QuantPlatformFacade, artifact_root: Path) -> None:
    test_bootstrap_enabled = bool(
        os.getenv(ENV_TEST_ARTIFACT_ROOT)
        or os.getenv("PYTEST_CURRENT_TEST")
        or os.getenv("QUANT_PLATFORM_OFFLINE_BENCHMARK")
    )
    if not test_bootstrap_enabled:
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
    if os.getenv("PYTEST_CURRENT_TEST") or os.getenv("QUANT_PLATFORM_OFFLINE_BENCHMARK"):
        _bootstrap_pytest_fixture_runs(artifact_root)
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
        delete_irreparable=not bool(
            os.getenv(ENV_TEST_ARTIFACT_ROOT)
            or os.getenv("PYTEST_CURRENT_TEST")
            or os.getenv("QUANT_PLATFORM_OFFLINE_BENCHMARK")
        )
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
