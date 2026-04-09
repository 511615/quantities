from __future__ import annotations

import math

from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot


def compute_performance_metrics(
    snapshots: list[PortfolioSnapshot],
    benchmark_returns: list[float] | None = None,
) -> dict[str, float]:
    if len(snapshots) < 2:
        return {
            "cumulative_return": 0.0,
            "annual_return": 0.0,
            "annual_volatility": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "calmar": 0.0,
            "max_drawdown": 0.0,
            "average_drawdown": 0.0,
            "drawdown_duration": 0.0,
            "var_95": 0.0,
            "cvar_95": 0.0,
        }
    navs = [snapshot.nav for snapshot in snapshots]
    returns = [
        (navs[index] / navs[index - 1] - 1.0)
        for index in range(1, len(navs))
        if abs(navs[index - 1]) > 1e-9
    ]
    if not returns:
        returns = [0.0]
    avg_return = sum(returns) / len(returns)
    downside = [value for value in returns if value < 0.0]
    volatility = (
        math.sqrt(sum((value - avg_return) ** 2 for value in returns) / len(returns))
        if returns
        else 0.0
    )
    downside_vol = (
        math.sqrt(sum(value * value for value in downside) / len(downside)) if downside else 0.0
    )
    annual_factor = math.sqrt(24.0 * 365.0)
    sorted_returns = sorted(returns)
    var_index = max(0, int(0.05 * len(sorted_returns)) - 1)
    var_95 = abs(sorted_returns[var_index])
    cvar_95_slice = sorted_returns[: max(1, int(0.05 * len(sorted_returns)))]
    cvar_95 = abs(sum(cvar_95_slice) / len(cvar_95_slice))
    drawdowns = [snapshot.drawdown for snapshot in snapshots]
    benchmark_returns = benchmark_returns or [0.0] * len(returns)
    benchmark_mean = sum(benchmark_returns) / len(benchmark_returns) if benchmark_returns else 0.0
    covariance = sum(
        (r - avg_return) * (b - benchmark_mean)
        for r, b in zip(returns, benchmark_returns, strict=False)
    ) / max(len(returns), 1)
    benchmark_var = (
        sum((b - benchmark_mean) ** 2 for b in benchmark_returns) / max(len(benchmark_returns), 1)
        if benchmark_returns
        else 0.0
    )
    beta = covariance / benchmark_var if benchmark_var > 1e-12 else 0.0
    alpha = avg_return - beta * benchmark_mean
    active_returns = [r - b for r, b in zip(returns, benchmark_returns, strict=False)]
    active_mean = sum(active_returns) / len(active_returns) if active_returns else 0.0
    active_vol = (
        math.sqrt(sum((value - active_mean) ** 2 for value in active_returns) / len(active_returns))
        if active_returns
        else 0.0
    )
    positive_strategy = [r for r, b in zip(returns, benchmark_returns, strict=False) if b > 0.0]
    negative_strategy = [r for r, b in zip(returns, benchmark_returns, strict=False) if b < 0.0]
    positive_bench = [b for b in benchmark_returns if b > 0.0]
    negative_bench = [b for b in benchmark_returns if b < 0.0]
    return {
        "cumulative_return": navs[-1] / navs[0] - 1.0 if abs(navs[0]) > 1e-9 else 0.0,
        "annual_return": avg_return * 24.0 * 365.0,
        "annual_volatility": volatility * annual_factor,
        "sharpe": (avg_return / volatility * annual_factor) if volatility > 1e-12 else 0.0,
        "sortino": (avg_return / downside_vol * annual_factor) if downside_vol > 1e-12 else 0.0,
        "calmar": ((avg_return * 24.0 * 365.0) / max(drawdowns)) if max(drawdowns) > 1e-12 else 0.0,
        "max_drawdown": max(drawdowns),
        "average_drawdown": sum(drawdowns) / len(drawdowns),
        "drawdown_duration": float(sum(1 for value in drawdowns if value > 0.0)),
        "var_95": var_95,
        "cvar_95": cvar_95,
        "alpha": alpha,
        "beta": beta,
        "information_ratio": (active_mean / active_vol * annual_factor)
        if active_vol > 1e-12
        else 0.0,
        "up_capture": (sum(positive_strategy) / sum(positive_bench))
        if positive_strategy and positive_bench
        else 0.0,
        "down_capture": (sum(negative_strategy) / sum(negative_bench))
        if negative_strategy and negative_bench
        else 0.0,
    }
