from __future__ import annotations

from pathlib import Path

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.config.loader import load_app_config


def main() -> None:
    config = load_app_config()
    facade = QuantPlatformFacade(Path(config.env.artifact_root), model_config=config.model)
    result = facade.run_baseline_benchmark()
    print(result)


if __name__ == "__main__":
    main()
