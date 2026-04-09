from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Response

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import (
    ModelTemplateCreateRequest,
    ModelTemplateListResponse,
    ModelTemplateUpdateRequest,
    ModelTemplateView,
    TrainedModelDetailView,
    TrainedModelListResponse,
    TrainedModelUpdateRequest,
)

router = APIRouter(prefix="/api/models", tags=["models"])


@router.get("/templates", response_model=ModelTemplateListResponse)
def list_model_templates(
    services: ServicesDep,
    include_deleted: bool = Query(default=False),
) -> ModelTemplateListResponse:
    return services.workbench.list_model_templates(include_deleted=include_deleted)


@router.get("/templates/{template_id}", response_model=ModelTemplateView)
def get_model_template(
    services: ServicesDep,
    template_id: str,
) -> ModelTemplateView:
    item = services.workbench.get_model_template(template_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Model template not found.")
    return item


@router.post("/templates", response_model=ModelTemplateView)
def create_model_template(
    services: ServicesDep,
    request: ModelTemplateCreateRequest,
) -> ModelTemplateView:
    try:
        return services.workbench.create_model_template(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/templates/{template_id}", response_model=ModelTemplateView)
def update_model_template(
    services: ServicesDep,
    template_id: str,
    request: ModelTemplateUpdateRequest,
) -> ModelTemplateView:
    item = services.workbench.update_model_template(template_id, request)
    if item is None:
        raise HTTPException(status_code=404, detail="Model template not found.")
    return item


@router.delete("/templates/{template_id}", status_code=204)
def delete_model_template(
    services: ServicesDep,
    template_id: str,
) -> Response:
    deleted = services.workbench.delete_model_template(template_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Model template not found.")
    return Response(status_code=204)


@router.get("/trained", response_model=TrainedModelListResponse)
def list_trained_models(
    services: ServicesDep,
    include_deleted: bool = Query(default=False),
) -> TrainedModelListResponse:
    return services.workbench.list_trained_models(include_deleted=include_deleted)


@router.get("/trained/{run_id}", response_model=TrainedModelDetailView)
def get_trained_model(
    services: ServicesDep,
    run_id: str,
) -> TrainedModelDetailView:
    detail = services.workbench.get_trained_model(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Trained model not found.")
    return detail


@router.patch("/trained/{run_id}/note", response_model=TrainedModelDetailView)
def update_trained_model_note(
    services: ServicesDep,
    run_id: str,
    request: TrainedModelUpdateRequest,
) -> TrainedModelDetailView:
    detail = services.workbench.update_trained_model_note(run_id, request.note)
    if detail is None:
        raise HTTPException(status_code=404, detail="Trained model not found.")
    return detail


@router.delete("/trained/{run_id}", response_model=TrainedModelDetailView)
def soft_delete_trained_model(
    services: ServicesDep,
    run_id: str,
) -> TrainedModelDetailView:
    detail = services.workbench.soft_delete_trained_model(run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Trained model not found.")
    return detail
