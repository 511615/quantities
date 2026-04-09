from __future__ import annotations

from quant_platform.backtest.contracts.signal import DirectionMode, SignalRecord


def normalize_signal_value(signal: SignalRecord) -> float:
    value = signal.normalized_value if signal.normalized_value is not None else signal.raw_value
    if signal.direction_mode == "long_only":
        return max(0.0, value)
    return value


def apply_direction_mode(value: float, direction_mode: DirectionMode) -> float:
    if direction_mode == "long_only":
        return max(0.0, value)
    return value
