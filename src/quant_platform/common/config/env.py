from __future__ import annotations

import os
from pathlib import Path


def load_dotenv_files(*, cwd: str | Path | None = None) -> None:
    root = Path(cwd or Path.cwd())
    for candidate in (root / ".env", root / ".env.local"):
        if not candidate.exists():
            continue
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            os.environ[key] = value.strip().strip("\"'")
