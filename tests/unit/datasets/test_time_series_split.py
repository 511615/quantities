from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_platform.datasets.splits.time_series import RollingWindowSpec, TimeSeriesSplitPlanner


def test_single_split_rejects_invalid_indexes() -> None:
    timestamps = [datetime(2024, 1, 1, hour, tzinfo=timezone.utc) for hour in range(5)]
    with pytest.raises(ValueError, match="invalid split indexes"):
        TimeSeriesSplitPlanner.single_split(
            timestamps=timestamps, train_end_index=0, valid_end_index=2
        )


def test_rolling_windows_respect_purge_gap_and_embargo() -> None:
    windows = TimeSeriesSplitPlanner.rolling_windows(
        length=20,
        spec=RollingWindowSpec(
            train_size=5,
            valid_size=2,
            test_size=2,
            step_size=2,
            embargo=1,
            purge_gap=1,
        ),
    )
    assert windows[0].train_end <= windows[0].valid_start - 1
    assert windows[0].valid_end <= windows[0].test_start - 1
    assert windows[0].window_id == "window_000"
