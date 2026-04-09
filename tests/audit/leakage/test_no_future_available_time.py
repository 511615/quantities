from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import DatasetSample


def test_feature_rows_do_not_exceed_feature_view_as_of_time(facade) -> None:
    dataset_ref = facade.build_smoke_dataset()
    samples = facade.dataset_store[dataset_ref.dataset_id]
    assert all(
        sample.available_time <= dataset_ref.feature_view_ref.as_of_time for sample in samples
    )


def test_future_visible_sample_fails_hard() -> None:
    sample = DatasetSample(
        entity_key="BTCUSDT",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        available_time=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=10),
        features={"lag_return_1": 0.0},
        target=0.0,
    )
    with pytest.raises(ValueError, match="available_time exceeds"):
        DatasetBuilder.validate_samples([sample], datetime(2024, 1, 2, tzinfo=timezone.utc))
