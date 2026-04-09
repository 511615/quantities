from __future__ import annotations

from fastapi import APIRouter

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import ModelComparisonRequest, ModelComparisonView

router = APIRouter(prefix="/api/comparisons", tags=["comparisons"])


@router.post("/models", response_model=ModelComparisonView)
def compare_models(
    services: ServicesDep,
    request: ModelComparisonRequest,
) -> ModelComparisonView:
    return services.workbench.compare_models(request)
