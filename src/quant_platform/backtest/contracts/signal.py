from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from quant_platform.common.types.core import FrozenModel

SignalType = Literal["score", "expected_return", "probability", "target_weight"]
DirectionMode = Literal["long_only", "long_short"]


class SignalRecord(FrozenModel):
    signal_id: str
    model_run_id: str
    instrument: str
    venue: str
    signal_time: datetime
    available_time: datetime
    tradable_from: datetime
    horizon_end: datetime | None = None
    signal_type: SignalType
    raw_value: float
    normalized_value: float | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    direction_mode: DirectionMode
    meta: dict[str, str | float | int] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_temporal_rules(self) -> "SignalRecord":
        if self.available_time < self.signal_time:
            raise ValueError("available_time cannot be earlier than signal_time")
        if self.tradable_from < self.available_time:
            raise ValueError("tradable_from cannot be earlier than available_time")
        if self.horizon_end and self.horizon_end <= self.signal_time:
            raise ValueError("horizon_end must be after signal_time")
        return self


class SignalFrame(FrozenModel):
    rows: list[SignalRecord]
    source_prediction_uri: str | None = None
    source_model_run_id: str | None = None

    @property
    def signal_count(self) -> int:
        return len(self.rows)
