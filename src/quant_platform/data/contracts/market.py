from __future__ import annotations

from datetime import datetime

from pydantic import Field, model_validator

from quant_platform.common.types.core import FrozenModel


class NormalizedMarketBar(FrozenModel):
    event_time: datetime
    available_time: datetime
    symbol: str
    venue: str
    open: float
    high: float
    low: float
    close: float
    volume: float = Field(ge=0.0)

    @model_validator(mode="after")
    def validate_bar(self) -> "NormalizedMarketBar":
        if self.available_time < self.event_time:
            raise ValueError("available_time cannot be earlier than event_time")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")
        return self


class DataQualityReport(FrozenModel):
    asset_id: str
    row_count: int
    duplicate_event_times: int = 0
    null_count: int = 0
    passed: bool
    checks: list[str]
    missing_ratio: float = 0.0
    duplicate_ratio: float = 0.0
    entity_count: int = 1
    entity_coverage_ratio: float = 1.0
    ordering_passed: bool = True
    quality_status: str = "healthy"
    warnings: list[str] = Field(default_factory=list)


class DataSnapshotManifest(FrozenModel):
    asset_id: str
    schema_name: str
    schema_version: int
    symbol: str
    venue: str
    frequency: str
    row_count: int
    event_start: datetime
    event_end: datetime
    available_start: datetime
    available_end: datetime
    columns: list[str]
    quality_report_uri: str | None = None
    entity_scope: str = "single_asset"
    entity_count: int = 1
    request_origin: str | None = None
    fallback_used: bool = False
