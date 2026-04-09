from __future__ import annotations

from datetime import datetime

from pydantic import model_validator

from quant_platform.common.types.core import FeatureField, FrozenModel
from quant_platform.data.contracts.data_asset import DataAssetRef


class FeatureRow(FrozenModel):
    entity_key: str
    timestamp: datetime
    available_time: datetime
    values: dict[str, float]


class FeatureViewBuildResult(FrozenModel):
    feature_view_ref: "FeatureViewRef"
    rows: list[FeatureRow]


class FeatureViewRef(FrozenModel):
    feature_set_id: str
    input_data_refs: list[DataAssetRef]
    as_of_time: datetime
    feature_schema: list[FeatureField]
    build_config_hash: str
    storage_uri: str

    @model_validator(mode="after")
    def validate_lineage(self) -> "FeatureViewRef":
        for field in self.feature_schema:
            if field.max_available_time and field.max_available_time > self.as_of_time:
                raise ValueError(
                    f"feature '{field.name}' becomes available after as_of_time; leakage detected"
                )
        return self
