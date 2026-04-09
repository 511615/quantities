from __future__ import annotations

from fastapi import APIRouter, HTTPException

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import JobListResponse, JobStatusView

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("", response_model=JobListResponse)
def list_jobs(services: ServicesDep) -> JobListResponse:
    return services.jobs.list_jobs()


@router.get("/{job_id}", response_model=JobStatusView)
def get_job(
    services: ServicesDep,
    job_id: str,
) -> JobStatusView:
    job = services.jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job
