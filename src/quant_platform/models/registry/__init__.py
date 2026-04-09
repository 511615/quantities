"""Model registry."""

from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry

__all__ = ["ModelRegistry", "register_default_models"]
