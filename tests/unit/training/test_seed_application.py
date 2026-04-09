from __future__ import annotations

import os
import random

from quant_platform.common.clock.seed import apply_seed


def test_apply_seed_sets_pythonhashseed_and_random_sequence() -> None:
    apply_seed(123)
    first = random.random()
    apply_seed(123)
    second = random.random()
    assert os.environ["PYTHONHASHSEED"] == "123"
    assert first == second
