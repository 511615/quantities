from __future__ import annotations

from quant_platform.datasets.splits.time_series import RollingWindowSpec, TimeSeriesSplitPlanner


def test_split_windows_preserve_temporal_gaps() -> None:
    windows = TimeSeriesSplitPlanner.rolling_windows(
        length=24,
        spec=RollingWindowSpec(
            train_size=6,
            valid_size=3,
            test_size=3,
            step_size=3,
            embargo=2,
            purge_gap=1,
        ),
    )
    for window in windows:
        assert window.train_end <= window.valid_start - window.purge_gap
        assert window.valid_end <= window.test_start - window.embargo
