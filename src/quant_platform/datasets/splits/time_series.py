from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from quant_platform.common.enums.core import SplitStrategy
from quant_platform.common.types.core import TimeRange
from quant_platform.datasets.contracts.dataset import SplitManifest


@dataclass(slots=True, frozen=True)
class RollingWindowSpec:
    train_size: int
    valid_size: int
    test_size: int
    step_size: int = 1
    embargo: int = 0
    purge_gap: int = 0
    expanding_train: bool = False


@dataclass(slots=True, frozen=True)
class RollingWindowPlan:
    window_id: str
    train_start: int
    train_end: int
    valid_start: int
    valid_end: int
    test_start: int
    test_end: int
    embargo: int
    purge_gap: int


class TimeSeriesSplitPlanner:
    @staticmethod
    def single_split(
        timestamps: list[datetime],
        train_end_index: int,
        valid_end_index: int,
    ) -> SplitManifest:
        if (
            train_end_index <= 0
            or valid_end_index <= train_end_index
            or valid_end_index >= len(timestamps)
        ):
            raise ValueError("invalid split indexes")
        return SplitManifest(
            strategy=SplitStrategy.TIME_SERIES,
            train_range=TimeRange(start=timestamps[0], end=timestamps[train_end_index]),
            valid_range=TimeRange(
                start=timestamps[train_end_index], end=timestamps[valid_end_index]
            ),
            test_range=TimeRange(start=timestamps[valid_end_index], end=timestamps[-1]),
        )

    @staticmethod
    def rolling_windows(length: int, spec: RollingWindowSpec) -> list[RollingWindowPlan]:
        if length <= 0:
            raise ValueError("length must be positive")
        if min(spec.train_size, spec.valid_size, spec.test_size, spec.step_size) <= 0:
            raise ValueError("window sizes and step size must be positive")
        windows: list[RollingWindowPlan] = []
        train_start = 0
        train_end = spec.train_size
        idx = 0
        while True:
            valid_start = train_end + spec.purge_gap
            valid_end = valid_start + spec.valid_size
            test_start = valid_end + spec.embargo
            test_end = test_start + spec.test_size
            if test_end > length:
                break
            windows.append(
                RollingWindowPlan(
                    window_id=f"window_{idx:03d}",
                    train_start=0 if spec.expanding_train else train_start,
                    train_end=train_end,
                    valid_start=valid_start,
                    valid_end=valid_end,
                    test_start=test_start,
                    test_end=test_end,
                    embargo=spec.embargo,
                    purge_gap=spec.purge_gap,
                )
            )
            train_start += spec.step_size
            train_end += spec.step_size
            idx += 1
        if not windows:
            raise ValueError("no valid rolling windows generated")
        return windows
