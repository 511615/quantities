from __future__ import annotations


def executable_quantity(
    requested_quantity: float,
    bar_volume: float,
    participation_cap: float,
    allow_partial_fills: bool,
) -> float:
    max_fill = max(0.0, bar_volume * participation_cap)
    if allow_partial_fills:
        return min(requested_quantity, max_fill)
    if requested_quantity <= max_fill:
        return requested_quantity
    return 0.0
