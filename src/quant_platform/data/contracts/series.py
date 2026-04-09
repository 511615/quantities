from __future__ import annotations

from datetime import datetime

from pydantic import Field, model_validator

from quant_platform.common.types.core import FrozenModel


class NormalizedSeriesPoint(FrozenModel):
    event_time: datetime
    available_time: datetime
    series_key: str
    entity_key: str
    domain: str
    vendor: str
    metric_name: str = "value"
    frequency: str
    value: float
    dimensions: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_point(self) -> "NormalizedSeriesPoint":
        if self.available_time < self.event_time:
            raise ValueError("available_time cannot be earlier than event_time")
        return self
