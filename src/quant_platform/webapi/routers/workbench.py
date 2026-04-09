from __future__ import annotations

from fastapi import APIRouter

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import WorkbenchOverviewView

router = APIRouter(prefix="/api/workbench", tags=["workbench"])


@router.get("/overview", response_model=WorkbenchOverviewView)
def get_workbench_overview(services: ServicesDep) -> WorkbenchOverviewView:
    jobs = services.jobs.list_jobs().items
    return services.workbench.workbench_overview(jobs)
