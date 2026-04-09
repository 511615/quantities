from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import ArtifactPreviewResponse

router = APIRouter(prefix="/api/artifacts", tags=["artifacts"])


@router.get("/preview", response_model=ArtifactPreviewResponse)
def preview_artifact(
    services: ServicesDep,
    uri: str = Query(..., min_length=1),
) -> ArtifactPreviewResponse:
    try:
        return services.workbench.preview_artifact(uri)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Artifact not found.") from exc
