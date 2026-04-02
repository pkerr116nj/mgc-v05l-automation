"""Order lifecycle models."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from ..domain.enums import OrderIntentType, OrderStatus


@dataclass(frozen=True)
class OrderIntent:
    order_intent_id: str
    bar_id: str
    symbol: str
    intent_type: OrderIntentType
    quantity: int
    created_at: datetime
    reason_code: str

    def __post_init__(self) -> None:
        if not self.order_intent_id:
            raise ValueError("OrderIntent.order_intent_id is required.")
        if not self.bar_id:
            raise ValueError("OrderIntent.bar_id is required.")
        if not self.symbol:
            raise ValueError("OrderIntent.symbol is required.")
        if self.quantity <= 0:
            raise ValueError("OrderIntent.quantity must be > 0.")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("OrderIntent.created_at must be timezone-aware.")
        if not self.reason_code:
            raise ValueError("OrderIntent.reason_code is required.")

    @property
    def is_entry(self) -> bool:
        """Return whether the order intent opens a position."""
        return self.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN)

    @property
    def is_exit(self) -> bool:
        """Return whether the order intent closes a position."""
        return self.intent_type in (OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE)


@dataclass(frozen=True)
class FillEvent:
    order_intent_id: str
    intent_type: OrderIntentType
    order_status: OrderStatus
    fill_timestamp: datetime
    fill_price: Optional[Decimal]
    broker_order_id: Optional[str]
    quantity: int = 1

    def __post_init__(self) -> None:
        if not self.order_intent_id:
            raise ValueError("FillEvent.order_intent_id is required.")
        if self.fill_timestamp.tzinfo is None or self.fill_timestamp.utcoffset() is None:
            raise ValueError("FillEvent.fill_timestamp must be timezone-aware.")
        if self.quantity <= 0:
            raise ValueError("FillEvent.quantity must be > 0.")
