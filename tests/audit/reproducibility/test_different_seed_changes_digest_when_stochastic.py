from __future__ import annotations

import random

from quant_platform.common.clock.seed import apply_seed


def test_seed_changes_are_observable_even_if_current_model_is_deterministic() -> None:
    apply_seed(7)
    first = random.random()
    apply_seed(8)
    second = random.random()
    assert first != second
