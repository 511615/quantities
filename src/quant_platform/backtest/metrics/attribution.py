from __future__ import annotations

from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot


def compute_pnl_attribution(
    snapshots: list[PortfolioSnapshot],
    benchmark_returns: list[float] | None = None,
) -> dict[str, float]:
    if not snapshots:
        return {
            "alpha_pnl": 0.0,
            "beta_or_benchmark_pnl": 0.0,
            "trading_cost": 0.0,
            "funding_pnl": 0.0,
            "borrow_cost": 0.0,
            "cash_pnl": 0.0,
        }
    latest = snapshots[-1]
    benchmark_component = sum(benchmark_returns) if benchmark_returns else 0.0
    return {
        "alpha_pnl": latest.realized_pnl + latest.unrealized_pnl - benchmark_component,
        "beta_or_benchmark_pnl": benchmark_component,
        "trading_cost": latest.fees_paid + latest.slippage_cost,
        "funding_pnl": latest.funding_pnl,
        "borrow_cost": latest.borrow_cost,
        "cash_pnl": latest.cash_free - snapshots[0].cash_free if snapshots else 0.0,
    }
