"""Deterministic paper broker for shared paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ..domain.enums import OrderIntentType, OrderStatus
from .order_models import FillEvent, OrderIntent


@dataclass(frozen=True)
class PaperPosition:
    quantity: int = 0
    average_price: Optional[Decimal] = None


class PaperBroker:
    """Deterministic paper broker with explicit fill-price inputs."""

    def __init__(self) -> None:
        self._connected = False
        self._position = PaperPosition()
        self._open_order_ids: list[str] = []
        self._order_status: dict[str, OrderStatus] = {}
        self._last_fill_timestamp: Optional[datetime] = None

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def submit_order(self, order_intent: OrderIntent) -> str:
        broker_order_id = f"paper-{order_intent.order_intent_id}"
        self._open_order_ids.append(broker_order_id)
        self._order_status[broker_order_id] = OrderStatus.ACKNOWLEDGED
        return broker_order_id

    def cancel_order(self, broker_order_id: str) -> None:
        if broker_order_id in self._open_order_ids:
            self._open_order_ids.remove(broker_order_id)
            self._order_status[broker_order_id] = OrderStatus.CANCELLED

    def get_order_status(self, broker_order_id: str) -> dict[str, str]:
        status = self._order_status.get(broker_order_id, OrderStatus.REJECTED)
        return {"broker_order_id": broker_order_id, "status": status.value}

    def get_open_orders(self) -> list[str]:
        return list(self._open_order_ids)

    def get_position(self) -> PaperPosition:
        return self._position

    def get_account_health(self) -> dict[str, str]:
        return {"status": "HEALTHY" if self._connected else "DISCONNECTED"}

    def restore_state(
        self,
        *,
        position: PaperPosition,
        open_order_ids: list[str],
        order_status: dict[str, OrderStatus],
        last_fill_timestamp: Optional[datetime] = None,
    ) -> None:
        """Restore deterministic broker state from persisted runtime artifacts."""
        self._position = position
        self._open_order_ids = list(open_order_ids)
        self._order_status = dict(order_status)
        self._last_fill_timestamp = last_fill_timestamp

    def snapshot_state(self) -> dict[str, object]:
        """Return a serializable broker snapshot for reconciliation."""
        return {
            "connected": self._connected,
            "position_quantity": self._position.quantity,
            "average_price": str(self._position.average_price) if self._position.average_price is not None else None,
            "open_order_ids": list(self._open_order_ids),
            "order_status": {key: status.value for key, status in self._order_status.items()},
            "last_fill_timestamp": self._last_fill_timestamp.isoformat() if self._last_fill_timestamp is not None else None,
        }

    def fill_order(self, order_intent: OrderIntent, fill_price: Decimal, fill_timestamp: datetime) -> FillEvent:
        """Create a deterministic fill event and update the simple paper position."""
        broker_order_id = f"paper-{order_intent.order_intent_id}"
        if broker_order_id in self._open_order_ids:
            self._open_order_ids.remove(broker_order_id)
        self._order_status[broker_order_id] = OrderStatus.FILLED
        self._last_fill_timestamp = fill_timestamp

        next_quantity = self._next_quantity(order_intent)
        average_price = self._next_average_price(order_intent=order_intent, fill_price=fill_price, next_quantity=next_quantity)
        self._position = PaperPosition(quantity=next_quantity, average_price=average_price)

        return FillEvent(
            order_intent_id=order_intent.order_intent_id,
            intent_type=order_intent.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=fill_timestamp,
            fill_price=fill_price,
            broker_order_id=broker_order_id,
            quantity=order_intent.quantity,
        )

    def _next_quantity(self, order_intent: OrderIntent) -> int:
        current_quantity = self._position.quantity
        if order_intent.intent_type == OrderIntentType.BUY_TO_OPEN:
            return current_quantity + order_intent.quantity
        if order_intent.intent_type == OrderIntentType.SELL_TO_OPEN:
            return current_quantity - order_intent.quantity
        if order_intent.intent_type == OrderIntentType.SELL_TO_CLOSE:
            return current_quantity - order_intent.quantity
        if order_intent.intent_type == OrderIntentType.BUY_TO_CLOSE:
            return current_quantity + order_intent.quantity
        raise ValueError(f"Unsupported order intent type: {order_intent.intent_type}")

    def _next_average_price(
        self,
        *,
        order_intent: OrderIntent,
        fill_price: Decimal,
        next_quantity: int,
    ) -> Optional[Decimal]:
        current_quantity = self._position.quantity
        current_average = self._position.average_price
        if next_quantity == 0:
            return None
        if order_intent.intent_type == OrderIntentType.BUY_TO_OPEN and current_quantity >= 0:
            current_cost = (current_average or Decimal("0")) * Decimal(str(current_quantity))
            next_cost = current_cost + (fill_price * Decimal(str(order_intent.quantity)))
            return next_cost / Decimal(str(next_quantity))
        if order_intent.intent_type == OrderIntentType.SELL_TO_OPEN and current_quantity <= 0:
            current_cost = (current_average or Decimal("0")) * Decimal(str(abs(current_quantity)))
            next_cost = current_cost + (fill_price * Decimal(str(order_intent.quantity)))
            return next_cost / Decimal(str(abs(next_quantity)))
        if (current_quantity > 0 and next_quantity > 0) or (current_quantity < 0 and next_quantity < 0):
            return current_average
        return fill_price
