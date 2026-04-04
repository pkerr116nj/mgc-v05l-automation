"""Real broker wrapper for the tightly gated MGC live-strategy pilot."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..config_models import StrategySettings
from ..domain.enums import OrderIntentType
from ..market_data import QuoteSnapshot, SchwabMarketDataProvider
from ..market_data.provider_interfaces import MarketDataProvider
from ..production_link.service import _normalize_orders, _single_leg_payload
from .provider_interfaces import ExecutionProvider
from .schwab_execution_provider import SchwabExecutionProvider
from .order_models import FillEvent, OrderIntent


class LiveStrategyPilotBroker:
    """Broker wrapper that reuses production-link truth and submits one-lot MGC futures pilot orders."""

    def __init__(
        self,
        *,
        settings: StrategySettings,
        repo_root: Path,
        market_data_provider: MarketDataProvider | None = None,
        execution_provider: ExecutionProvider | None = None,
    ) -> None:
        self._settings = settings
        self._repo_root = Path(repo_root)
        self._market_data_provider = market_data_provider or SchwabMarketDataProvider(settings, repo_root=self._repo_root)
        self._execution_provider = execution_provider or SchwabExecutionProvider(self._repo_root)
        self._connected = False
        self._last_snapshot: dict[str, Any] = {}
        self._last_submit_context: dict[str, Any] = {}

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def refresh_from_snapshot(self, payload: dict[str, Any]) -> None:
        self._last_snapshot = dict(payload or {})

    def load_snapshot(self, *, force_refresh: bool = False) -> dict[str, Any]:
        payload = self._execution_provider.snapshot_state(force_refresh=force_refresh)
        self.refresh_from_snapshot(payload if isinstance(payload, dict) else {})
        return dict(self._last_snapshot)

    def submit_order(self, order_intent: OrderIntent) -> str:
        if not self._connected:
            raise RuntimeError("Live strategy pilot broker is not connected.")
        account_hash = self._selected_account_hash()
        if account_hash is None:
            raise RuntimeError("No live Schwab account is currently selected for the strategy pilot.")
        side = _intent_to_broker_side(order_intent.intent_type)
        quotes = self._market_data_provider.fetch_quotes((order_intent.symbol,))
        if not quotes:
            raise RuntimeError(f"No live quote was returned for {order_intent.symbol}.")
        limit_price = _marketable_limit_price(intent_type=order_intent.intent_type, quote_snapshot=quotes[0])
        external_symbol = quotes[0].external_symbol
        order_payload = _single_leg_payload(
            symbol=external_symbol,
            asset_type="FUTURE",
            side=side,
            quantity=Decimal(order_intent.quantity),
            order_type="LIMIT",
            limit_price=limit_price,
            stop_price=None,
            client_order_id=None,
            session="NORMAL",
            time_in_force="DAY",
        )
        response = self._execution_provider.submit_order(account_hash, order_payload)
        broker_order_id = str(response.get("broker_order_id") or "").strip()
        if not broker_order_id:
            raise RuntimeError("Broker submit did not return a broker_order_id.")
        submitted_at = datetime.now(timezone.utc)
        self._last_submit_context = {
            "order_intent_id": order_intent.order_intent_id,
            "symbol": order_intent.symbol,
            "intent_type": order_intent.intent_type.value,
            "submit_attempted_at": submitted_at.isoformat(),
            "broker_order_id": broker_order_id,
            "status_code": response.get("status_code"),
            "request_payload": order_payload,
            "response_payload": response,
            "limit_price": str(limit_price),
            "quote_symbol": external_symbol,
        }
        return broker_order_id

    def cancel_order(self, broker_order_id: str) -> None:
        account_hash = self._selected_account_hash()
        if account_hash is None:
            raise RuntimeError("No live Schwab account is currently selected for cancel.")
        self._execution_provider.cancel_order(account_hash, broker_order_id)

    def get_order_status(self, broker_order_id: str) -> Any:
        account_hash = self._selected_account_hash()
        if account_hash is None:
            raise RuntimeError("No live Schwab account is currently selected for order-status lookup.")
        payload = self._execution_provider.get_order_status(account_hash, broker_order_id)
        normalized = _normalize_orders([payload], account_hash=account_hash, fetched_at=datetime.now(timezone.utc))
        current = normalized[0] if normalized else None
        fill_row = self.latest_recent_fill_for_order(broker_order_id)
        fill_timestamp = None
        fill_price = None
        if fill_row is not None:
            fill_timestamp = fill_row.get("closed_at") or fill_row.get("updated_at")
            fill_price = fill_row.get("fill_price")
        raw_payload = dict(payload or {})
        activity_collection = raw_payload.get("orderActivityCollection") or []
        if isinstance(activity_collection, list) and activity_collection:
            activity = dict(activity_collection[0] or {})
            execution_legs = activity.get("executionLegs") or []
            if isinstance(execution_legs, list) and execution_legs:
                execution_leg = dict(execution_legs[0] or {})
                fill_timestamp = execution_leg.get("time") or fill_timestamp
                fill_price = execution_leg.get("price") or fill_price
        return {
            "broker_order_id": broker_order_id,
            "status": (current.status if current is not None else str(payload.get("status") or "UNKNOWN")),
            "broker_order_status": current.status if current is not None else str(payload.get("status") or "UNKNOWN"),
            "filled_quantity": str(current.filled_quantity) if current is not None and current.filled_quantity is not None else None,
            "entered_at": current.entered_at.isoformat() if current is not None and current.entered_at is not None else None,
            "closed_at": current.closed_at.isoformat() if current is not None and current.closed_at is not None else None,
            "updated_at": current.updated_at.isoformat() if current is not None else datetime.now(timezone.utc).isoformat(),
            "fill_timestamp": fill_timestamp,
            "fill_price": str(fill_price) if fill_price not in (None, "") else None,
            "raw_payload": payload,
        }

    def get_open_orders(self) -> list[dict[str, Any]]:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        rows = list((snapshot.get("orders") or {}).get("open_rows") or [])
        return [dict(row) for row in rows if str(row.get("symbol") or "").strip().upper() == self._settings.symbol]

    def get_position(self) -> dict[str, Any]:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        for row in list((snapshot.get("portfolio") or {}).get("positions") or []):
            if str(row.get("symbol") or "").strip().upper() == self._settings.symbol:
                return dict(row)
        return {"symbol": self._settings.symbol, "side": "FLAT", "quantity": 0}

    def get_account_health(self) -> dict[str, Any]:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        return dict(snapshot.get("health") or {})

    def snapshot_state(self) -> dict[str, Any]:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        health = dict(snapshot.get("health") or {})
        reconciliation = dict(snapshot.get("reconciliation") or {})
        position_row = self.get_position()
        open_orders = self.get_open_orders()
        recent_fill = self.latest_recent_fill_for_symbol(self._settings.symbol)
        signed_quantity = _signed_position_quantity(position_row)
        auth_ok = bool(_health_ok(health, "auth")) or bool(_health_ok(health, "auth_healthy"))
        return {
            "connected": bool(_health_ok(health, "broker_reachable")) and auth_ok and bool(_health_ok(health, "account_selected")),
            "truth_complete": bool(_health_ok(health, "orders_fresh")) and bool(_health_ok(health, "positions_fresh")),
            "selected_account_hash": self._selected_account_hash(),
            "position_quantity": signed_quantity,
            "average_price": position_row.get("average_cost"),
            "open_order_ids": [str(row.get("broker_order_id") or "").strip() for row in open_orders if str(row.get("broker_order_id") or "").strip()],
            "order_status": {
                str(row.get("broker_order_id") or "").strip(): str(row.get("status") or "").strip().upper()
                for row in open_orders
                if str(row.get("broker_order_id") or "").strip()
            },
            "last_fill_timestamp": recent_fill.get("closed_at") or recent_fill.get("updated_at") if recent_fill else None,
            "reconciliation_status": reconciliation.get("status"),
            "reconciliation_mismatch_count": reconciliation.get("mismatch_count"),
            "health": health,
        }

    def fill_order(self, order_intent: OrderIntent, fill_price: Decimal, fill_timestamp: datetime) -> FillEvent:
        raise RuntimeError("Synthetic fills are not allowed for the live strategy pilot broker.")

    def latest_recent_fill_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        rows = list((snapshot.get("orders") or {}).get("recent_fill_rows") or [])
        matching = [
            dict(row)
            for row in rows
            if str(row.get("symbol") or "").strip().upper() == str(symbol).strip().upper()
        ]
        if not matching:
            return None
        return max(matching, key=lambda row: str(row.get("closed_at") or row.get("updated_at") or ""))

    def latest_recent_fill_for_order(self, broker_order_id: str) -> dict[str, Any] | None:
        snapshot = self._last_snapshot or self.load_snapshot(force_refresh=True)
        rows = list((snapshot.get("orders") or {}).get("recent_fill_rows") or [])
        matching = [
            dict(row)
            for row in rows
            if str(row.get("broker_order_id") or "").strip() == str(broker_order_id).strip()
        ]
        if not matching:
            return None
        return max(matching, key=lambda row: str(row.get("closed_at") or row.get("updated_at") or ""))

    def last_submit_context(self) -> dict[str, Any]:
        return dict(self._last_submit_context)

    def _selected_account_hash(self) -> str | None:
        snapshot = self._last_snapshot or {}
        provider = getattr(self, "_execution_provider", None)
        if provider is not None:
            return provider.selected_account_hash(snapshot)
        connection = dict(snapshot.get("connection") or {})
        accounts = dict(snapshot.get("accounts") or {})
        account_hash = str(
            connection.get("selected_account_hash")
            or accounts.get("selected_account_hash")
            or ""
        ).strip()
        return account_hash or None


def _intent_to_broker_side(intent_type: OrderIntentType) -> str:
    if intent_type in {OrderIntentType.BUY_TO_OPEN, OrderIntentType.BUY_TO_CLOSE}:
        return "BUY"
    return "SELL"


def _marketable_limit_price(*, intent_type: OrderIntentType, quote_snapshot: QuoteSnapshot) -> Decimal:
    fields = (
        (quote_snapshot.ask_price, quote_snapshot.last_price, quote_snapshot.mark_price)
        if intent_type in {OrderIntentType.BUY_TO_OPEN, OrderIntentType.BUY_TO_CLOSE}
        else (quote_snapshot.bid_price, quote_snapshot.last_price, quote_snapshot.mark_price)
    )
    for value in fields:
        if value is None:
            continue
        return Decimal(str(value))
    raise RuntimeError("Live strategy pilot could not derive a marketable limit price from the current quote.")


def _health_ok(health: dict[str, Any], key: str) -> bool:
    row = health.get(key)
    if not isinstance(row, dict):
        return False
    return bool(row.get("ok"))


def _signed_position_quantity(position_row: dict[str, Any]) -> int:
    side = str(position_row.get("side") or "").strip().upper()
    raw_qty = position_row.get("quantity") or 0
    try:
        qty = int(Decimal(str(raw_qty)))
    except Exception:
        qty = 0
    if side == "SHORT":
        return -abs(qty)
    if side == "LONG":
        return abs(qty)
    return 0
