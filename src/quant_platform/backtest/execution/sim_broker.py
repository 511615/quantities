from __future__ import annotations

from quant_platform.backtest.adapters.market_adapter import MarketEvent
from quant_platform.backtest.contracts.backtest import CostModel, ExecutionConfig
from quant_platform.backtest.contracts.order import ChildOrder, FillEvent
from quant_platform.backtest.execution.costs import fee_cost
from quant_platform.backtest.execution.queue_model import executable_quantity
from quant_platform.backtest.execution.slippage import (
    execution_price,
    slippage_bps_for_order,
)
from quant_platform.common.hashing.digest import stable_digest


class SimulatedBroker:
    """Minimal event-driven broker with ACK and partial fill support."""

    def __init__(self, execution_config: ExecutionConfig, cost_model: CostModel) -> None:
        self.execution_config = execution_config
        self.cost_model = cost_model

    def process_event(
        self,
        order: ChildOrder,
        event: MarketEvent,
    ) -> tuple[ChildOrder, list[FillEvent]]:
        updated = order
        fills: list[FillEvent] = []
        if order.status == "NEW" and event.event_time > order.eligible_time:
            updated = order.model_copy(update={"status": "ACKED"})
        if updated.status not in {"ACKED", "PARTIALLY_FILLED"}:
            return updated, fills
        requested_quantity = updated.quantity
        fill_quantity = executable_quantity(
            requested_quantity=requested_quantity,
            bar_volume=event.volume,
            participation_cap=updated.participation_cap,
            allow_partial_fills=self.execution_config.allow_partial_fills,
        )
        if fill_quantity <= 0.0:
            return updated, fills
        participation_rate = min(1.0, fill_quantity / max(event.volume, 1e-9))
        slippage_bps = min(
            updated.max_slippage_bps,
            slippage_bps_for_order(self.cost_model, "normal", participation_rate),
        )
        base_price = (
            event.open if self.execution_config.fill_price == "next_bar_open" else event.mid
        )
        fill_price = execution_price(base_price, updated.side, slippage_bps)
        notional = fill_quantity * fill_price
        fee = fee_cost(notional, self.cost_model)
        fills.append(
            FillEvent(
                fill_id=stable_digest(
                    {
                        "order_id": updated.order_id,
                        "fill_time": event.event_time,
                        "qty": fill_quantity,
                    }
                ),
                order_id=updated.order_id,
                instrument=updated.instrument,
                side=updated.side,
                fill_time=event.event_time,
                quantity=fill_quantity,
                price=fill_price,
                notional=abs(notional),
                fee=fee,
                slippage_cost=abs(fill_price - base_price) * fill_quantity,
                liquidity_flag="taker",
            )
        )
        remaining_quantity = max(0.0, updated.quantity - fill_quantity)
        if remaining_quantity <= 1e-12:
            updated = updated.model_copy(update={"quantity": fill_quantity, "status": "FILLED"})
        else:
            updated = updated.model_copy(
                update={"quantity": remaining_quantity, "status": "PARTIALLY_FILLED"}
            )
        return updated, fills
