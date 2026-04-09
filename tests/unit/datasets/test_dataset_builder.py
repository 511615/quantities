from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from quant_platform.common.enums.core import LabelKind
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import DatasetSample, LabelSpec, SamplePolicy


def test_validate_samples_rejects_future_available_time() -> None:
    sample = DatasetSample(
        entity_key="BTCUSDT",
        timestamp=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        available_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc) + timedelta(days=2),
        features={"lag_return_1": 0.01},
        target=0.02,
    )
    with pytest.raises(ValueError, match="available_time exceeds"):
        DatasetBuilder.validate_samples([sample], datetime(2024, 1, 2, tzinfo=timezone.utc))


def test_build_dataset_tracks_manifest_and_hash(facade) -> None:
    dataset_ref = facade.build_smoke_dataset()
    samples = facade.dataset_store[dataset_ref.dataset_id]
    assert dataset_ref.dataset_hash
    assert dataset_ref.dataset_manifest_uri
    assert dataset_ref.dataset_samples_uri
    assert dataset_ref.feature_schema_hash
    assert dataset_ref.label_schema_hash
    assert dataset_ref.entity_scope == "single_asset"
    assert dataset_ref.entity_count == 1
    assert dataset_ref.readiness_status == "ready"
    assert dataset_ref.label_spec == LabelSpec(
        target_column="future_return_1",
        horizon=1,
        kind=LabelKind.REGRESSION,
    )
    assert dataset_ref.sample_policy == SamplePolicy(min_history_bars=10)
    assert len(samples) == 4
