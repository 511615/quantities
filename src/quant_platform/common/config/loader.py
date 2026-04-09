from __future__ import annotations

from pathlib import Path

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from quant_platform.common.config.models import AppConfig


def load_app_config(config_dir: str | Path = "conf", config_name: str = "config") -> AppConfig:
    config_path = Path(config_dir).resolve()
    with initialize_config_dir(version_base=None, config_dir=str(config_path)):
        cfg = compose(config_name=config_name)
    payload = OmegaConf.to_container(cfg, resolve=True)
    return AppConfig.model_validate(payload)
