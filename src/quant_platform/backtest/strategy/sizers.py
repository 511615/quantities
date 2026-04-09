from __future__ import annotations


def scale_signal_by_confidence(value: float, confidence: float | None) -> float:
    if confidence is None:
        return value
    return value * confidence
