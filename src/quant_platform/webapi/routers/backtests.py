from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import (
    BacktestDeleteResponse,
    BacktestReportView,
    BacktestsResponse,
)

router = APIRouter(prefix="/api/backtests", tags=["backtests"])


@router.get("", response_model=BacktestsResponse)
def list_backtests(
    services: ServicesDep,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
    search: str | None = None,
    status: str | None = None,
) -> BacktestsResponse:
    return services.workbench.list_backtests(
        page=page,
        per_page=per_page,
        search=search,
        status=status,
    )


@router.get("/{backtest_id}", response_model=BacktestReportView)
def get_backtest_detail(
    services: ServicesDep,
    backtest_id: str,
) -> BacktestReportView:
    detail = services.workbench.get_backtest_detail(backtest_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Backtest report not found.")
    return detail


@router.delete("/{backtest_id}", response_model=BacktestDeleteResponse)
def delete_backtest(
    services: ServicesDep,
    backtest_id: str,
) -> BacktestDeleteResponse:
    result = services.workbench.delete_backtest(backtest_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Backtest report not found.")
    return result
