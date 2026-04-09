from __future__ import annotations

from fastapi import APIRouter, Query

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import ExperimentsResponse

router = APIRouter(prefix="/api/experiments", tags=["experiments"])


@router.get("", response_model=ExperimentsResponse)
def list_experiments(
    services: ServicesDep,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
    search: str | None = None,
    sort_by: str = Query(default="created_at"),
    sort_order: str = Query(default="desc", pattern="^(asc|desc)$"),
    model_name: str | None = None,
    dataset_id: str | None = None,
    status: str | None = None,
) -> ExperimentsResponse:
    return services.workbench.list_experiments(
        page=page,
        per_page=per_page,
        search=search,
        sort_by=sort_by,
        sort_order=sort_order,
        model_name=model_name,
        dataset_id=dataset_id,
        status=status,
    )
