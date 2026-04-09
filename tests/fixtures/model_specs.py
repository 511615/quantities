from __future__ import annotations

from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.types.core import SchemaField
from quant_platform.models.contracts.model_spec import ModelSpec


def build_model_spec(
    model_name: str = "mean_baseline",
    *,
    family: ModelFamily = ModelFamily.BASELINE,
    lookback: int | None = None,
    hyperparams: dict[str, object] | None = None,
) -> ModelSpec:
    return ModelSpec(
        model_name=model_name,
        family=family,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        lookback=lookback,
        hyperparams=hyperparams or {"alpha": 0.0},
    )
