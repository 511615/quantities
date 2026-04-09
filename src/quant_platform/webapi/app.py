from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.config.loader import load_app_config
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.ingestion import DataConnectorError
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryRepository
from quant_platform.webapi.services.catalog import ResearchWorkbenchService
from quant_platform.webapi.services.jobs import JobService


@dataclass
class AppServices:
    workbench: ResearchWorkbenchService
    jobs: JobService


def get_services(request: Request) -> AppServices:
    return request.app.state.services  # type: ignore[no-any-return]


ServicesDep = Annotated[AppServices, Depends(get_services)]


def create_app() -> FastAPI:
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
    artifact_root = Path(config.env.artifact_root)
    facade = QuantPlatformFacade(artifact_root)
    repository = ArtifactRepository(artifact_root)
    dataset_registry = DatasetRegistryRepository(artifact_root, repository)
    workbench_service = ResearchWorkbenchService(
        repository=repository,
        dataset_registry=dataset_registry,
        store=LocalArtifactStore(artifact_root),
        model_families={name: entry.family.value for name, entry in config.model.models.items()},
        model_registry_entries=config.model.models,
        facade=facade,
    )
    app = FastAPI(title="Quant Platform Research Workbench", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
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

    @app.get("/health")
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_model=None)
    def root():
        index_path = dist_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
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
            return FileResponse(index_path)
        return JSONResponse({"detail": "Frontend build not found."}, status_code=404)

    return app
