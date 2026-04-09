from __future__ import annotations

import pytest

from quant_platform.common.enums.core import ModelFamily
from quant_platform.models.baselines.elastic_net import ElasticNetModel
from quant_platform.models.baselines.mean_baseline import MeanBaselineModel
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from tests.fixtures.model_specs import build_model_spec


def test_model_registry_creates_registered_model() -> None:
    registry = ModelRegistry()
    spec = build_model_spec()
    registry.register("mean_baseline", MeanBaselineModel)
    model = registry.create(spec)
    assert isinstance(model, MeanBaselineModel)
    assert model.spec.input_schema == spec.input_schema
    assert model.spec.output_schema == spec.output_schema
    assert model.spec.hyperparams == spec.hyperparams


def test_model_registry_rejects_unregistered_model() -> None:
    registry = ModelRegistry()
    with pytest.raises(KeyError, match="is not registered"):
        registry.create(build_model_spec("missing_model"))


def test_model_registry_duplicate_registration_overrides_existing_factory() -> None:
    registry = ModelRegistry()
    registry.register("mean_baseline", MeanBaselineModel)
    registry.register("mean_baseline", MeanBaselineModel)
    model = registry.create(build_model_spec())
    assert isinstance(model, MeanBaselineModel)


def test_register_default_models_registers_new_baseline_family() -> None:
    registry = ModelRegistry()
    register_default_models(registry)
    model = registry.create(build_model_spec("elastic_net", family=ModelFamily.LINEAR))
    assert isinstance(model, ElasticNetModel)
