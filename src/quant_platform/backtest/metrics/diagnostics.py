from __future__ import annotations

from collections import Counter

from quant_platform.backtest.contracts.order import ChildOrder, FillEvent
from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot
from quant_platform.backtest.contracts.signal import SignalFrame


def compute_execution_metrics(
    orders: list[ChildOrder],
    fills: list[FillEvent],
    snapshots: list[PortfolioSnapshot],
    *,
    initial_cash: float | None = None,
    eligible_order_count: int | None = None,
    blocked_order_count: int | None = None,
    position_open_count: int | None = None,
) -> dict[str, float]:
    filled_orders = {fill.order_id for fill in fills}
    rejected = [order for order in orders if order.status == "REJECTED"]
    partial = [order for order in orders if order.status == "PARTIALLY_FILLED"]
    average_fee_bps = (
        sum(fill.fee / fill.notional * 10000.0 for fill in fills) / len(fills) if fills else 0.0
    )
    average_slippage_bps = (
        sum(fill.slippage_cost / fill.notional * 10000.0 for fill in fills) / len(fills)
        if fills
        else 0.0
    )
    capital_base = float(initial_cash) if initial_cash and initial_cash > 0.0 else 1.0
    metrics = {
        "order_count": float(len(orders)),
        "fill_count": float(len(fills)),
        "fill_rate": len(filled_orders) / len(orders) if orders else 0.0,
        "rejection_rate": len(rejected) / len(orders) if orders else 0.0,
        "partial_fill_rate": len(partial) / len(orders) if orders else 0.0,
        "average_fee_bps": average_fee_bps,
        "average_slippage_bps": average_slippage_bps,
        "turnover_total": sum(fill.notional for fill in fills) / capital_base,
        "implementation_shortfall": sum(fill.slippage_cost + fill.fee for fill in fills),
    }
    if eligible_order_count is not None:
        metrics["eligible_order_count"] = float(eligible_order_count)
    if blocked_order_count is not None:
        metrics["blocked_order_count"] = float(blocked_order_count)
    if position_open_count is not None:
        metrics["position_open_count"] = float(position_open_count)
    return metrics


def compute_signal_metrics(
    signal_frame: SignalFrame,
    realized_returns: list[float],
) -> dict[str, float]:
    if not signal_frame.rows:
        return {}
    raw_values = [row.raw_value for row in signal_frame.rows]
    avg_signal = sum(raw_values) / len(raw_values)
    hit_count = 0
    paired = list(zip(raw_values, realized_returns, strict=False))
    for signal_value, realized_return in paired:
        if signal_value == 0.0:
            continue
        if signal_value * realized_return > 0.0:
            hit_count += 1
    positive_pnl = [value for value in realized_returns if value > 0.0]
    negative_pnl = [abs(value) for value in realized_returns if value < 0.0]
    autocorr = 0.0
    if len(raw_values) > 1:
        mean = avg_signal
        numerator = sum(
            (raw_values[index] - mean) * (raw_values[index - 1] - mean)
            for index in range(1, len(raw_values))
        )
        denominator = sum((value - mean) ** 2 for value in raw_values)
        autocorr = numerator / denominator if denominator > 1e-12 else 0.0
    return {
        "signal_count": float(len(signal_frame.rows)),
        "average_signal": avg_signal,
        "hit_rate": hit_count / len(paired) if paired else 0.0,
        "profit_factor": (
            sum(positive_pnl) / sum(negative_pnl)
            if negative_pnl
            else float(sum(positive_pnl) > 0.0)
        ),
        "signal_autocorrelation": autocorr,
    }


def summarize_block_reasons(reasons: list[str]) -> list[str]:
    counts = Counter(reason for reason in reasons if reason)
    return [
        f"{reason} ({count})" if count > 1 else reason
        for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]
