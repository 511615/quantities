from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FrozenModel(BaseModel):
    """Immutable base model for boundary contracts."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class TimeRange(FrozenModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_order(self) -> "TimeRange":
        if self.end <= self.start:
            raise ValueError("time range end must be greater than start")
        return self


class SchemaField(FrozenModel):
    name: str
    dtype: str
    nullable: bool = False
    description: str | None = None


class FeatureField(SchemaField):
    lineage_source: str
    max_available_time: datetime | None = None
    target_derived: bool = False

    @model_validator(mode="after")
    def forbid_target_derived(self) -> "FeatureField":
        if self.target_derived:
            raise ValueError("feature fields cannot be target-derived")
        return self


class ArtifactRef(FrozenModel):
    kind: str
    uri: str
    content_hash: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
