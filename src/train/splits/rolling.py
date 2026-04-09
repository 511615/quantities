"""Rolling-window and walk-forward split planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List


@dataclass(slots=True)
class RollingWindowSpec:
    train_size: int
    valid_size: int
    test_size: int = 0
    step_size: int = 1
    min_train_size: int = 1
    embargo: int = 0
    purge_gap: int = 0
    expanding_train: bool = False


@dataclass(slots=True)
class RollingWindowPlan:
    window_id: str
    train_start: int
    train_end: int
    valid_start: int
    valid_end: int
    test_start: int | None = None
    test_end: int | None = None
    embargo: int = 0
    purge_gap: int = 0
    metadata: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, int | None | dict[str, int]]:
        return {
            "window_id": self.window_id,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "valid_start": self.valid_start,
            "valid_end": self.valid_end,
            "test_start": self.test_start,
            "test_end": self.test_end,
            "embargo": self.embargo,
            "purge_gap": self.purge_gap,
            "metadata": self.metadata,
        }


def generate_rolling_windows(length: int, spec: RollingWindowSpec) -> List[RollingWindowPlan]:
    if length <= 0:
        raise ValueError("length must be positive")
    if spec.train_size < spec.min_train_size:
        raise ValueError("train_size must be >= min_train_size")
    if spec.valid_size <= 0:
        raise ValueError("valid_size must be positive")
    if spec.step_size <= 0:
        raise ValueError("step_size must be positive")

    windows: list[RollingWindowPlan] = []
    base_train_start = 0
    train_start = 0
    train_end = spec.train_size
    window_idx = 0

    while True:
        valid_start = train_end + spec.purge_gap
        valid_end = valid_start + spec.valid_size
        if valid_end + spec.embargo > length:
            break

        test_start = None
        test_end = None
        if spec.test_size > 0:
            test_start = valid_end + spec.embargo
            test_end = test_start + spec.test_size
            if test_end > length:
                break

        windows.append(
            RollingWindowPlan(
                window_id=f"window_{window_idx:03d}",
                train_start=base_train_start if spec.expanding_train else train_start,
                train_end=train_end,
                valid_start=valid_start,
                valid_end=valid_end,
                test_start=test_start,
                test_end=test_end,
                embargo=spec.embargo,
                purge_gap=spec.purge_gap,
                metadata={"length": length, "step_size": spec.step_size},
            )
        )

        train_start += spec.step_size
        train_end += spec.step_size
        if spec.expanding_train:
            train_start = base_train_start
        window_idx += 1

    if not windows:
        raise ValueError("No valid rolling windows generated for the provided spec")
    return windows


def iter_window_ids(windows: Iterable[RollingWindowPlan]) -> list[str]:
    return [window.window_id for window in windows]
