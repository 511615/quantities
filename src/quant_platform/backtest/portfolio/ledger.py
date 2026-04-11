from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

from quant_platform.backtest.adapters.market_adapter import MarketEvent
from quant_platform.backtest.contracts.portfolio import (
    PortfolioSnapshot,
    PositionSnapshot,
)
from quant_platform.backtest.portfolio.margin import (
    initial_margin_for_notional,
    maintenance_margin_for_notional,
)
from quant_platform.backtest.portfolio.positions import PositionState


class PortfolioLedger:
    def __init__(self, initial_cash: float, max_gross_leverage: float) -> None:
        self.initial_cash = initial_cash
        self.cash_free = initial_cash
        self.cash_locked = 0.0
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.slippage_cost = 0.0
        self.funding_pnl = 0.0
        self.borrow_cost = 0.0
        self.max_gross_leverage = max_gross_leverage
        self.positions: dict[str, PositionState] = {}
        self.last_prices: dict[str, float] = {}
        self.peak_nav = initial_cash
        self.turnover_window: deque[tuple[object, float]] = deque()
        self.turnover_window_total = 0.0

    def mark(self, event: MarketEvent) -> None:
        self.last_prices[event.instrument] = event.close

    def apply_fill(
        self,
        instrument: str,
        signed_quantity: float,
        price: float,
        fee: float,
        slippage_cost: float,
        fill_time: object,
    ) -> float:
        position = self.positions.setdefault(instrument, PositionState(instrument=instrument))
        realized = position.apply_fill(
            signed_quantity=signed_quantity,
            price=price,
            fill_time=fill_time,
        )
        self.realized_pnl += realized
        cash_delta = -(signed_quantity * price) - fee
        self.cash_free += cash_delta
        self.fees_paid += fee
        self.slippage_cost += slippage_cost
        self.turnover_window.append((fill_time, abs(signed_quantity * price)))
        self.turnover_window_total += abs(signed_quantity * price)
        return realized

    def snapshot(self, timestamp: object) -> PortfolioSnapshot:
        self._prune_turnover_window(timestamp)
        positions: list[PositionSnapshot] = []
        gross_exposure = 0.0
        net_exposure = 0.0
        long_exposure = 0.0
        short_exposure = 0.0
        nav = self.cash_free
        unrealized_pnl = 0.0
        for instrument, state in self.positions.items():
            mark_price = self.last_prices.get(instrument, state.avg_entry_price or 0.0) or 0.0
            market_value = state.quantity * mark_price
            notional = abs(market_value)
            position_unrealized = (mark_price - state.avg_entry_price) * state.quantity
            unrealized_pnl += position_unrealized
            nav += market_value
            gross_exposure += notional
            net_exposure += market_value
            long_exposure += max(market_value, 0.0)
            short_exposure += abs(min(market_value, 0.0))
            equity = nav
            positions.append(
                PositionSnapshot(
                    instrument=instrument,
                    quantity=state.quantity,
                    side="long"
                    if state.quantity > 0
                    else "short"
                    if state.quantity < 0
                    else "flat",
                    avg_entry_price=max(state.avg_entry_price, 0.0),
                    mark_price=max(mark_price, 1e-9),
                    market_value=market_value,
                    notional=notional,
                    unrealized_pnl=position_unrealized,
                    realized_pnl_cum=state.realized_pnl_cum,
                    last_fill_time=state.last_fill_time,
                    weight=(market_value / equity) if abs(equity) > 1e-9 else 0.0,
                    initial_margin=initial_margin_for_notional(notional, self.max_gross_leverage),
                    maintenance_margin=maintenance_margin_for_notional(notional),
                )
            )
        equity = nav
        self.peak_nav = max(self.peak_nav, nav)
        margin_used = sum(position.initial_margin for position in positions)
        maintenance_margin = sum(position.maintenance_margin for position in positions)
        turnover_1d = self.turnover_window_total
        return PortfolioSnapshot(
            timestamp=timestamp,
            nav=nav,
            equity=equity,
            cash_free=self.cash_free,
            cash_locked=self.cash_locked,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=unrealized_pnl,
            fees_paid=self.fees_paid,
            slippage_cost=self.slippage_cost,
            funding_pnl=self.funding_pnl,
            borrow_cost=self.borrow_cost,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            long_exposure=long_exposure,
            short_exposure=short_exposure,
            gross_leverage=(gross_exposure / equity) if abs(equity) > 1e-9 else 0.0,
            net_leverage=(net_exposure / equity) if abs(equity) > 1e-9 else 0.0,
            turnover_1d=turnover_1d,
            margin_used=margin_used,
            maintenance_margin=maintenance_margin,
            liquidation_buffer=max(0.0, equity - maintenance_margin),
            drawdown=(self.peak_nav - nav) / self.peak_nav if self.peak_nav > 0 else 0.0,
            positions=positions,
        )

    def _prune_turnover_window(self, timestamp: object) -> None:
        if not isinstance(timestamp, datetime):
            return
        cutoff = timestamp - timedelta(days=1)
        while self.turnover_window:
            fill_time, notional = self.turnover_window[0]
            if not isinstance(fill_time, datetime) or fill_time >= cutoff:
                break
            self.turnover_window.popleft()
            self.turnover_window_total = max(0.0, self.turnover_window_total - notional)
