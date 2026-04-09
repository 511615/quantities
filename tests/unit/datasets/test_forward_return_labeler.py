from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler
from quant_platform.features.contracts.feature_view import FeatureRow


def test_forward_return_labeler_rejects_non_positive_horizon() -> None:
    with pytest.raises(ValueError, match="horizon must be positive"):
        ForwardReturnLabeler().build([], {}, horizon=0)


def test_forward_return_labeler_drops_tail_without_future_label() -> None:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [
        FeatureRow(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=1),
            available_time=base.replace(hour=1),
            values={},
        ),
        FeatureRow(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=2),
            available_time=base.replace(hour=2),
            values={},
        ),
        FeatureRow(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=3),
            available_time=base.replace(hour=3),
            values={},
        ),
    ]
    labels = ForwardReturnLabeler().build(
        rows,
        {
            base.replace(hour=1): 100.0,
            base.replace(hour=2): 102.0,
            base.replace(hour=3): 101.0,
        },
        horizon=1,
    )
    assert set(labels) == {
        ("BTCUSDT", base.replace(hour=1)),
        ("BTCUSDT", base.replace(hour=2)),
    }


def test_forward_return_labeler_is_entity_aware_for_multi_asset_rows() -> None:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [
        FeatureRow(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=1),
            available_time=base.replace(hour=1),
            values={},
        ),
        FeatureRow(
            entity_key="ETHUSDT",
            timestamp=base.replace(hour=1),
            available_time=base.replace(hour=1),
            values={},
        ),
        FeatureRow(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=2),
            available_time=base.replace(hour=2),
            values={},
        ),
        FeatureRow(
            entity_key="ETHUSDT",
            timestamp=base.replace(hour=2),
            available_time=base.replace(hour=2),
            values={},
        ),
    ]
    labels = ForwardReturnLabeler().build(
        rows,
        {
            ("BTCUSDT", base.replace(hour=1)): 100.0,
            ("BTCUSDT", base.replace(hour=2)): 110.0,
            ("ETHUSDT", base.replace(hour=1)): 50.0,
            ("ETHUSDT", base.replace(hour=2)): 45.0,
        },
        horizon=1,
    )

    assert labels[("BTCUSDT", base.replace(hour=1))] == pytest.approx(0.10)
    assert labels[("ETHUSDT", base.replace(hour=1))] == pytest.approx(-0.10)
