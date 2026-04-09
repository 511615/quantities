from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PositionState:
    instrument: str
    quantity: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl_cum: float = 0.0
    last_fill_time: object | None = None

    def apply_fill(self, signed_quantity: float, price: float, fill_time: object) -> float:
        realized = 0.0
        previous_quantity = self.quantity
        if previous_quantity == 0.0 or previous_quantity * signed_quantity > 0:
            new_quantity = previous_quantity + signed_quantity
            if abs(new_quantity) > 1e-12:
                weighted_notional = (
                    previous_quantity * self.avg_entry_price + signed_quantity * price
                )
                self.avg_entry_price = weighted_notional / new_quantity
            else:
                self.avg_entry_price = 0.0
            self.quantity = new_quantity
        else:
            closed_quantity = min(abs(previous_quantity), abs(signed_quantity))
            direction = 1.0 if previous_quantity > 0 else -1.0
            realized = (price - self.avg_entry_price) * closed_quantity * direction
            self.quantity = previous_quantity + signed_quantity
            if abs(self.quantity) <= 1e-12:
                self.quantity = 0.0
                self.avg_entry_price = 0.0
            elif previous_quantity * self.quantity < 0:
                self.avg_entry_price = price
        self.realized_pnl_cum += realized
        self.last_fill_time = fill_time
        return realized
