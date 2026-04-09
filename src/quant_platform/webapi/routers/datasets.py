from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.launch import LaunchJobResponse
from quant_platform.webapi.schemas.views import (
    DatasetAcquisitionRequest,
    DatasetDeleteResponse,
    DatasetDependenciesResponse,
    DatasetDetailView,
    DatasetFacetsView,
    DatasetFusionBuildResponse,
    DatasetFusionRequest,
    DatasetListResponse,
    DatasetPipelinePlanView,
    DatasetPipelineRequest,
    DatasetReadinessSummaryView,
    DatasetRequestOptionsView,
    DatasetSeriesResponse,
    DatasetSlicesResponse,
    OhlcvBarsResponse,
    TrainingDatasetsResponse,
)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


@router.get("/request-options", response_model=DatasetRequestOptionsView)
def get_dataset_request_options(services: ServicesDep) -> DatasetRequestOptionsView:
    return services.workbench.get_dataset_request_options()


@router.get("/facets", response_model=DatasetFacetsView)
def get_dataset_facets(services: ServicesDep) -> DatasetFacetsView:
    return services.workbench.get_dataset_facets()


@router.post("/requests", response_model=LaunchJobResponse)
def launch_dataset_request(
    services: ServicesDep,
    request: DatasetAcquisitionRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_dataset_request(request)


@router.post("/pipelines", response_model=DatasetPipelinePlanView)
def launch_dataset_pipeline(
    services: ServicesDep,
    request: DatasetPipelineRequest,
) -> DatasetPipelinePlanView:
    return services.jobs.launch_dataset_pipeline(request)


@router.post("/fusions", response_model=DatasetFusionBuildResponse)
def build_fusion_dataset(
    services: ServicesDep,
    request: DatasetFusionRequest,
) -> DatasetFusionBuildResponse:
    return services.workbench.build_fusion_dataset(request)


@router.get("", response_model=DatasetListResponse)
def list_datasets(
    services: ServicesDep,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=200),
) -> DatasetListResponse:
    return services.workbench.list_datasets(page=page, per_page=per_page)


@router.get("/training", response_model=TrainingDatasetsResponse)
def list_training_datasets(services: ServicesDep) -> TrainingDatasetsResponse:
    return services.workbench.list_training_datasets()


@router.get("/{dataset_id}", response_model=DatasetDetailView)
def get_dataset_detail(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetDetailView:
    detail = services.workbench.get_dataset_detail(dataset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return detail


@router.get("/{dataset_id}/dependencies", response_model=DatasetDependenciesResponse)
def get_dataset_dependencies(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetDependenciesResponse:
    dependencies = services.workbench.get_dataset_dependencies(dataset_id)
    if dependencies is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return dependencies


@router.delete("/{dataset_id}", response_model=DatasetDeleteResponse)
def delete_dataset(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetDeleteResponse:
    result = services.workbench.delete_dataset(dataset_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return result


@router.get("/{dataset_id}/slices", response_model=DatasetSlicesResponse)
def get_dataset_slices(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetSlicesResponse:
    slices = services.workbench.get_dataset_slices(dataset_id)
    if slices is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return slices


@router.get("/{dataset_id}/series", response_model=DatasetSeriesResponse)
def get_dataset_series(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetSeriesResponse:
    series = services.workbench.get_dataset_series(dataset_id)
    if series is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return series


@router.get("/{dataset_id}/readiness", response_model=DatasetReadinessSummaryView)
def get_dataset_readiness(
    services: ServicesDep,
    dataset_id: str,
) -> DatasetReadinessSummaryView:
    readiness = services.workbench.get_dataset_readiness(dataset_id)
    if readiness is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return readiness


@router.get("/{dataset_id}/ohlcv", response_model=OhlcvBarsResponse)
def query_dataset_ohlcv(
    services: ServicesDep,
    dataset_id: str,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=200, ge=1, le=5000),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
) -> OhlcvBarsResponse:
    result = services.workbench.query_dataset_ohlcv(
        dataset_id,
        page=page,
        per_page=per_page,
        start_time=start_time,
        end_time=end_time,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return result
