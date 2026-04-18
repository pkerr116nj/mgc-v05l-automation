"""Real broker wrapper for the tightly gated MGC live-strategy pilot."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config_models import MarketDataProvider as MarketDataProviderSetting, StrategySettings
from ..market_data import DatabentoMarketDataProvider, SchwabMarketDataProvider
from ..market_data.provider_interfaces import MarketDataProvider
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
        self._market_data_provider = market_data_provider or _build_live_market_data_provider(settings=settings, repo_root=self._repo_root)
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
        account_id = self._selected_account_id()
        if account_id is None:
            raise RuntimeError("No live broker account is currently selected for the strategy pilot.")
        quotes = self._market_data_provider.fetch_quotes((order_intent.symbol,))
        if not quotes:
            raise RuntimeError(f"No live quote was returned for {order_intent.symbol}.")
        order_request = self._execution_provider.build_order_request(
            order_intent=order_intent,
            quote_snapshot=quotes[0],
        )
        response = self._execution_provider.submit_order(account_id, order_request)
        broker_order_id = str(response.get("broker_order_id") or "").strip()
        if not broker_order_id:
            raise RuntimeError("Broker submit did not return a broker_order_id.")
        submitted_at = datetime.now(timezone.utc)
        self._last_submit_context = {
            "order_intent_id": order_intent.order_intent_id,
            "symbol": order_intent.symbol,
            "intent_type": order_intent.intent_type.value,
            "submit_attempted_at": submitted_at.isoformat(),
            "account_id": account_id,
            "broker_order_id": broker_order_id,
            "status_code": response.get("status_code"),
            "request_payload": asdict(order_request),
            "response_payload": response,
            "limit_price": str(order_request.limit_price) if order_request.limit_price is not None else None,
            "quote_symbol": quotes[0].external_symbol,
            "pricing_source": order_request.pricing_source,
        }
        return broker_order_id

    def cancel_order(self, broker_order_id: str) -> None:
        account_id = self._selected_account_id()
        if account_id is None:
            raise RuntimeError("No live broker account is currently selected for cancel.")
        self._execution_provider.cancel_order(account_id, broker_order_id)

    def get_order_status(self, broker_order_id: str) -> Any:
        account_id = self._selected_account_id()
        if account_id is None:
            raise RuntimeError("No live broker account is currently selected for order-status lookup.")
        payload = self._execution_provider.get_order_status(account_id, broker_order_id)
        fill_row = self.latest_recent_fill_for_order(broker_order_id)
        fill_timestamp = None
        fill_price = None
        if fill_row is not None:
            fill_timestamp = fill_row.get("closed_at") or fill_row.get("updated_at")
            fill_price = fill_row.get("fill_price")
        raw_payload = dict(payload.get("raw_payload") or {})
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
            "status": str(payload.get("status") or "UNKNOWN"),
            "broker_order_status": str(payload.get("broker_order_status") or payload.get("status") or "UNKNOWN"),
            "filled_quantity": payload.get("filled_quantity"),
            "entered_at": payload.get("entered_at"),
            "closed_at": payload.get("closed_at"),
            "updated_at": payload.get("updated_at") or datetime.now(timezone.utc).isoformat(),
            "fill_timestamp": fill_timestamp,
            "fill_price": str(fill_price) if fill_price not in (None, "") else None,
            "raw_payload": raw_payload,
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
            "selected_account_id": self._selected_account_id(),
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

    def _selected_account_id(self) -> str | None:
        snapshot = self._last_snapshot or {}
        provider = getattr(self, "_execution_provider", None)
        if provider is not None and hasattr(provider, "selected_account_id"):
            selected_account_id = provider.selected_account_id(snapshot)
            if selected_account_id:
                return selected_account_id
        return self._selected_account_hash()

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


def _health_ok(health: dict[str, Any], key: str) -> bool:
    row = health.get(key)
    if not isinstance(row, dict):
        return False
    return bool(row.get("ok"))


class _ConfiguredLiveMarketDataProvider:
    def __init__(self, *, settings: StrategySettings, repo_root: Path) -> None:
        configured_provider = settings.market_data_provider
        if configured_provider == MarketDataProviderSetting.DATABENTO:
            self._primary = DatabentoMarketDataProvider(settings, repo_root=repo_root)
            self._quote_fallback = SchwabMarketDataProvider(settings, repo_root=repo_root)
        else:
            self._primary = SchwabMarketDataProvider(settings, repo_root=repo_root)
            self._quote_fallback = None
        self.provider_id = getattr(self._primary, "provider_id", str(configured_provider.value))

    def fetch_historical_bars(self, request):
        return self._primary.fetch_historical_bars(request)

    def fetch_quotes(self, internal_symbols):
        try:
            return self._primary.fetch_quotes(internal_symbols)
        except NotImplementedError:
            if self._quote_fallback is None:
                raise
            return self._quote_fallback.fetch_quotes(internal_symbols)

    def describe_symbol(self, internal_symbol: str) -> dict[str, Any]:
        description = dict(self._primary.describe_symbol(internal_symbol))
        if self._quote_fallback is not None:
            description.setdefault(
                "quote_provider_fallback",
                getattr(self._quote_fallback, "provider_id", "schwab_market_data"),
            )
        return description

    def subscribe_live_quotes(self, internal_symbols):
        try:
            return self._primary.subscribe_live_quotes(internal_symbols)
        except NotImplementedError:
            if self._quote_fallback is None:
                raise
            return self._quote_fallback.subscribe_live_quotes(internal_symbols)


def _build_live_market_data_provider(*, settings: StrategySettings, repo_root: Path) -> MarketDataProvider:
    return _ConfiguredLiveMarketDataProvider(settings=settings, repo_root=repo_root)


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
