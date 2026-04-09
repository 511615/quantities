from __future__ import annotations

from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot
from quant_platform.backtest.portfolio.risk_checks import hhi


def compute_risk_metrics(
    snapshots: list[PortfolioSnapshot],
    risk_event_count: int,
) -> dict[str, float]:
    if not snapshots:
        return {"gross_exposure": 0.0, "position_count": 0.0}
    latest = snapshots[-1]
    weights = [abs(position.weight) for position in latest.positions]
    return {
        "gross_exposure": latest.gross_exposure,
        "net_exposure": latest.net_exposure,
        "long_exposure": latest.long_exposure,
        "short_exposure": latest.short_exposure,
        "gross_leverage": latest.gross_leverage,
        "net_leverage": latest.net_leverage,
        "margin_used": latest.margin_used,
        "maintenance_margin": latest.maintenance_margin,
        "drawdown": latest.drawdown,
        "position_count": float(len(latest.positions)),
        "max_gross_leverage_seen": max(snapshot.gross_leverage for snapshot in snapshots),
        "max_drawdown_seen": max(snapshot.drawdown for snapshot in snapshots),
        "concentration_hhi": hhi(weights),
        "risk_trigger_count": float(risk_event_count),
    }
