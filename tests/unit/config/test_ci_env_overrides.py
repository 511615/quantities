from __future__ import annotations

from pathlib import Path

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from quant_platform.common.config.models import AppConfig


def test_ci_env_override_is_ci_friendly() -> None:
    config_dir = Path("conf").resolve()
    with initialize_config_dir(version_base=None, config_dir=str(config_dir)):
        cfg = compose(config_name="config", overrides=["env=ci"])
    app_config = AppConfig.model_validate(OmegaConf.to_container(cfg, resolve=True))
    assert app_config.env.name == "ci"
    assert app_config.env.tracking_backend == "file"
    assert app_config.env.artifact_root == "./artifacts"
    assert app_config.env.strict_mode is True
    assert ":" not in app_config.env.artifact_root
