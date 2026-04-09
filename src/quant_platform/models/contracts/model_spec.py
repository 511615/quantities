from __future__ import annotations

from typing import Any

from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.types.core import FrozenModel, SchemaField


class ModelSpec(FrozenModel):
    model_name: str
    family: ModelFamily
    version: str
    input_schema: list[SchemaField]
    output_schema: list[SchemaField]
    task_type: str = "regression"
    lookback: int | None = None
    target_horizon: int = 1
    prediction_type: str = "return"
    hyperparams: dict[str, Any]
