from __future__ import annotations

from fastapi import APIRouter

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.launch import (
    BacktestLaunchPreflightView,
    BacktestLaunchOptionsView,
    LaunchBacktestRequest,
    LaunchBacktestPreflightRequest,
    LaunchDatasetMultimodalTrainRequest,
    LaunchJobResponse,
    LaunchModelCompositionRequest,
    LaunchTrainRequest,
    TrainLaunchOptionsView,
)

router = APIRouter(prefix="/api/launch", tags=["launch"])


@router.get("/train/options", response_model=TrainLaunchOptionsView)
def train_options(services: ServicesDep) -> TrainLaunchOptionsView:
    return services.jobs.get_train_options()


@router.get("/backtest/options", response_model=BacktestLaunchOptionsView)
def backtest_options(services: ServicesDep) -> BacktestLaunchOptionsView:
    return services.jobs.get_backtest_options()


@router.post("/backtest/preflight", response_model=BacktestLaunchPreflightView)
def backtest_preflight(
    services: ServicesDep,
    request: LaunchBacktestPreflightRequest,
) -> BacktestLaunchPreflightView:
    return services.jobs.get_backtest_preflight(request)


@router.post("/train", response_model=LaunchJobResponse)
def launch_train(
    services: ServicesDep,
    request: LaunchTrainRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_train(request)


@router.post("/dataset-multimodal-train", response_model=LaunchJobResponse)
def launch_dataset_multimodal_train(
    services: ServicesDep,
    request: LaunchDatasetMultimodalTrainRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_dataset_multimodal_train(request)


@router.post("/backtest", response_model=LaunchJobResponse)
def launch_backtest(
    services: ServicesDep,
    request: LaunchBacktestRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_backtest(request)


@router.post("/model-composition", response_model=LaunchJobResponse)
def launch_model_composition(
    services: ServicesDep,
    request: LaunchModelCompositionRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_model_composition(request)
