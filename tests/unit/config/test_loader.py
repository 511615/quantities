from __future__ import annotations

from quant_platform.common.config.loader import load_app_config


def test_load_app_config_returns_typed_model() -> None:
    config = load_app_config()
    assert config.project.name == "quant-platform"
    assert config.env.name == "local"
    assert config.data.dataset_policies.forbid_random_split is True
