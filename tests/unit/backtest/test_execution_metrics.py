from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.backtest.contracts.order import ChildOrder, FillEvent
from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot
from quant_platform.backtest.metrics.diagnostics import compute_execution_metrics


def _snapshot(turnover_1d: float) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        nav=100000.0,
        equity=100000.0,
        cash_free=100000.0,
        cash_locked=0.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        fees_paid=0.0,
        slippage_cost=0.0,
        funding_pnl=0.0,
        borrow_cost=0.0,
        gross_exposure=0.0,
        net_exposure=0.0,
        long_exposure=0.0,
        short_exposure=0.0,
        gross_leverage=0.0,
        net_leverage=0.0,
        turnover_1d=turnover_1d,
        margin_used=0.0,
        maintenance_margin=0.0,
        liquidation_buffer=100000.0,
        drawdown=0.0,
        positions=[],
    )


def test_execution_metrics_turnover_total_is_normalized_by_initial_cash() -> None:
    order = ChildOrder(
        order_id="ord-1",
        parent_order_id="parent-1",
        created_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        eligible_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        instrument="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        time_in_force="IOC",
        quantity=1.0,
        max_slippage_bps=100.0,
        participation_cap=1.0,
        status="FILLED",
    )
    fill = FillEvent(
        fill_id="fill-1",
        order_id="ord-1",
        instrument="BTCUSDT",
        side="BUY",
        fill_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        quantity=1.0,
        price=50000.0,
        notional=50000.0,
        fee=25.0,
        slippage_cost=10.0,
    )

    metrics = compute_execution_metrics(
        [order],
        [fill],
        [_snapshot(1000000.0)],
        initial_cash=100000.0,
    )

    assert metrics["turnover_total"] == 0.5
