from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum

from pydantic import Field

from quant_platform.common.types.core import ArtifactRef, FrozenModel


class WorkflowStageName(StrEnum):
    PREPARE = "prepare"
    TRAIN = "train"
    PREDICT = "predict"
    BENCHMARK = "benchmark"
    BACKTEST = "backtest"
    REVIEW = "review"


class WorkflowStageStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowRunStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class WorkflowStageResult(FrozenModel):
    stage: WorkflowStageName
    status: WorkflowStageStatus
    request_digest: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    summary: str = ""
