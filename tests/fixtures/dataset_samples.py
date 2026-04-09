from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.datasets.contracts.dataset import DatasetSample


def build_dataset_samples() -> list[DatasetSample]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=1),
            available_time=base.replace(hour=1),
            features={"lag_return_1": 0.01, "volume_zscore": -0.2},
            target=0.005,
        ),
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=2),
            available_time=base.replace(hour=2),
            features={"lag_return_1": -0.00495, "volume_zscore": 0.1},
            target=0.0149253731,
        ),
        DatasetSample(
            entity_key="BTCUSDT",
            timestamp=base.replace(hour=3),
            available_time=base.replace(hour=3),
            features={"lag_return_1": 0.0149253731, "volume_zscore": 1.5},
            target=0.0147058824,
        ),
    ]
