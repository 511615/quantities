from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field

from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.types.core import FrozenModel


class AdvancedModelKind(StrEnum):
    BASELINE = "baseline"
    TRANSFORMER = "transformer"
    TEMPORAL_FUSION = "temporal_fusion"
    PATCH_MIXER = "patch_mixer"
    MULTIMODAL = "multimodal"
    MOE_EXTENSION = "moe_extension"


class ModelRegistration(FrozenModel):
    model_name: str
    family: ModelFamily
    advanced_kind: AdvancedModelKind
    entrypoint: str
    input_adapter_key: str
    prediction_adapter_key: str
    artifact_adapter_key: str
    capabilities: list[str] = Field(default_factory=list)
    default_hyperparams: dict[str, Any] = Field(default_factory=dict)
    config_schema_version: str = "1"
    aliases: list[str] = Field(default_factory=list)
    benchmark_eligible: bool = True
    default_eligible: bool = True
    enabled: bool = True
