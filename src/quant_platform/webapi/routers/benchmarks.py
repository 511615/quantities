from __future__ import annotations

from fastapi import APIRouter, HTTPException

from quant_platform.webapi.app import ServicesDep
from quant_platform.webapi.schemas.views import BenchmarkDetailView, BenchmarkListItemView

router = APIRouter(prefix="/api/benchmarks", tags=["benchmarks"])


@router.get("", response_model=list[BenchmarkListItemView])
def list_benchmarks(services: ServicesDep) -> list[BenchmarkListItemView]:
    return services.workbench.list_benchmarks()


@router.get("/{benchmark_name}", response_model=BenchmarkDetailView)
def get_benchmark_detail(
    services: ServicesDep,
    benchmark_name: str,
) -> BenchmarkDetailView:
    detail = services.workbench.get_benchmark_detail(benchmark_name)
    if detail is None:
        raise HTTPException(status_code=404, detail="Benchmark detail not found.")
    return detail
