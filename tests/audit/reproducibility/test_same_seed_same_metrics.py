from __future__ import annotations

from quant_platform.api.facade import QuantPlatformFacade


def test_same_seed_same_metrics(tmp_path) -> None:
    first = QuantPlatformFacade(tmp_path / "run1").train_smoke()
    second = QuantPlatformFacade(tmp_path / "run2").train_smoke()
    assert first.metrics == second.metrics
