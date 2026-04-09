from __future__ import annotations

from fastapi import APIRouter

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.launch import (
    BacktestLaunchOptionsView,
    LaunchBacktestRequest,
    LaunchJobResponse,
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


@router.post("/train", response_model=LaunchJobResponse)
def launch_train(
    services: ServicesDep,
    request: LaunchTrainRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_train(request)


@router.post("/backtest", response_model=LaunchJobResponse)
def launch_backtest(
    services: ServicesDep,
    request: LaunchBacktestRequest,
) -> LaunchJobResponse:
    return services.jobs.launch_backtest(request)
