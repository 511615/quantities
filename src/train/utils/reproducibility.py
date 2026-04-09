"""Reproducibility helpers."""

from __future__ import annotations

import os
import random
from typing import Any, Dict

try:
    import numpy as np
except ImportError:  # pragma: no cover - optional dependency
    np = None

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency
    torch = None


def set_global_seed(seed: int, deterministic: bool = True) -> None:
    random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    if np is not None:
        np.random.seed(seed)
    if torch is not None:
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = deterministic
        torch.backends.cudnn.benchmark = not deterministic


def capture_seed_state(seed: int, deterministic: bool = True) -> Dict[str, Any]:
    return {
        "seed": seed,
        "deterministic": deterministic,
        "pythonhashseed": os.environ.get("PYTHONHASHSEED", str(seed)),
        "numpy": np is not None,
        "torch": torch is not None,
        "cuda": bool(torch and torch.cuda.is_available()),
    }


def make_worker_seed(base_seed: int, worker_id: int) -> int:
    return int(base_seed) + int(worker_id)
