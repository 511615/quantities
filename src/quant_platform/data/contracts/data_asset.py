from __future__ import annotations

from pydantic import Field

from quant_platform.common.types.core import FrozenModel, TimeRange


class DataAssetRef(FrozenModel):
    asset_id: str
    schema_version: int
    source: str
    symbol: str
    venue: str
    frequency: str
    time_range: TimeRange
    storage_uri: str
    content_hash: str
    entity_key: str | None = None
    tags: list[str] = Field(default_factory=list)
    request_origin: str | None = None
    fallback_used: bool = False
