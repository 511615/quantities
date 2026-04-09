from __future__ import annotations


def initial_margin_for_notional(notional: float, max_gross_leverage: float) -> float:
    return abs(notional) / max(max_gross_leverage, 1.0)


def maintenance_margin_for_notional(notional: float) -> float:
    return abs(notional) * 0.2
