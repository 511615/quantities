from __future__ import annotations

import os

import uvicorn


def run() -> None:
    host = os.environ.get("QUANT_PLATFORM_WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("QUANT_PLATFORM_WEB_PORT", "8000"))
    uvicorn.run(
        "quant_platform.webapi.app:create_app",
        host=host,
        port=port,
        factory=True,
    )


if __name__ == "__main__":
    run()
