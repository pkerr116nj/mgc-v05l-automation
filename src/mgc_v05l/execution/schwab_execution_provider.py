"""Execution-provider wrapper around the existing Schwab production link."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..market_data import SchwabOAuthClient, SchwabTokenStore, UrllibJsonTransport, load_schwab_market_data_config
from ..market_data.provider_models import QuoteSnapshot
from ..production_link import ProductionLinkService
from ..production_link.client import SchwabBrokerHttpClient
from ..production_link.service import _normalize_orders, _single_leg_payload
from .broker_requests import BrokerContractRequest, BrokerOrderRequest
from .order_models import OrderIntent
from .provider_interfaces import ExecutionProvider


class SchwabExecutionProvider(ExecutionProvider):
    """Schwab execution/account-truth provider kept separate from market-data providers."""

    provider_id = "schwab_execution"

    def __init__(
        self,
        repo_root: Path,
        *,
        production_link_service: ProductionLinkService | None = None,
        broker_client: SchwabBrokerHttpClient | None = None,
    ) -> None:
        self._repo_root = Path(repo_root).resolve(strict=False)
        self._production_link_service = production_link_service or ProductionLinkService(self._repo_root)
        schwab_config = load_schwab_market_data_config(self._production_link_service.config.market_data_config_path)
        oauth_client = SchwabOAuthClient(
            config=schwab_config.auth,
            transport=UrllibJsonTransport(timeout_seconds=self._production_link_service.config.request_timeout_seconds),
            token_store=SchwabTokenStore(schwab_config.auth.token_store_path),
        )
        self._client = broker_client or SchwabBrokerHttpClient(
            oauth_client=oauth_client,
            base_url=self._production_link_service.config.trader_api_base_url,
            timeout_seconds=self._production_link_service.config.request_timeout_seconds,
        )

    def snapshot_state(self, *, force_refresh: bool = False) -> dict[str, Any]:
        payload = self._production_link_service.snapshot(force_refresh=force_refresh)
        return dict(payload or {})

    def selected_account_id(self, snapshot: dict[str, Any] | None = None) -> str | None:
        return self.selected_account_hash(snapshot)

    def selected_account_hash(self, snapshot: dict[str, Any] | None = None) -> str | None:
        payload = snapshot or {}
        connection = dict(payload.get("connection") or {})
        accounts = dict(payload.get("accounts") or {})
        account_hash = str(
            connection.get("selected_account_id")
            or accounts.get("selected_account_id")
            or connection.get("selected_account_hash")
            or accounts.get("selected_account_hash")
            or ""
        ).strip()
        return account_hash or None

    def submit_order(self, account_id: str, order_request: BrokerOrderRequest) -> dict[str, Any]:
        order_payload = _single_leg_payload(
            symbol=str(order_request.contract.broker_symbol or order_request.contract.symbol),
            asset_type=order_request.contract.asset_class,
            side=order_request.side,
            quantity=order_request.quantity,
            order_type=order_request.order_type,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            client_order_id=order_request.client_order_id,
            session=order_request.session or "NORMAL",
            time_in_force=order_request.time_in_force,
        )
        response = dict(self._client.submit_order(account_id, order_payload) or {})
        response.setdefault("request_payload", order_payload)
        response.setdefault("normalized_order_request", order_request.to_dict())
        return response

    def cancel_order(self, account_id: str, broker_order_id: str) -> None:
        self._client.cancel_order(account_id, broker_order_id)

    def get_order_status(self, account_id: str, broker_order_id: str) -> dict[str, Any]:
        payload = dict(self._client.get_order_status(account_id, broker_order_id) or {})
        normalized = _normalize_orders(
            [payload],
            account_hash=account_id,
            fetched_at=datetime.now(timezone.utc),
            broker_provider_id=self._production_link_service.config.broker_provider_id,
        )
        current = normalized[0] if normalized else None
        return {
            "broker_order_id": broker_order_id,
            "status": current.status if current is not None else str(payload.get("status") or "UNKNOWN"),
            "broker_order_status": current.status if current is not None else str(payload.get("status") or "UNKNOWN"),
            "filled_quantity": str(current.filled_quantity) if current is not None and current.filled_quantity is not None else None,
            "entered_at": current.entered_at.isoformat() if current is not None and current.entered_at is not None else None,
            "closed_at": current.closed_at.isoformat() if current is not None and current.closed_at is not None else None,
            "updated_at": current.updated_at.isoformat() if current is not None and current.updated_at is not None else None,
            "raw_payload": payload,
        }

    def build_order_request(
        self,
        *,
        order_intent: OrderIntent,
        quote_snapshot: Any | None = None,
    ) -> BrokerOrderRequest:
        if not isinstance(quote_snapshot, QuoteSnapshot):
            raise RuntimeError("Schwab execution provider requires a QuoteSnapshot pricing context.")
        broker_symbol = str(quote_snapshot.external_symbol or "").strip()
        if not broker_symbol:
            raise RuntimeError(f"No external broker symbol is available for {order_intent.symbol}.")
        return BrokerOrderRequest(
            account_id=None,
            contract=BrokerContractRequest(
                asset_class="FUTURE",
                symbol=order_intent.symbol,
                broker_symbol=broker_symbol,
            ),
            side=_intent_to_broker_side(order_intent),
            quantity=Decimal(order_intent.quantity),
            order_type="LIMIT",
            time_in_force="DAY",
            session="NORMAL",
            intent_type=order_intent.intent_type.value,
            limit_price=_marketable_limit_price(order_intent=order_intent, quote_snapshot=quote_snapshot),
            stop_price=None,
            client_order_id=None,
            pricing_source=f"market_data:{quote_snapshot.provider or 'unknown'}",
            metadata={
                "quote_internal_symbol": quote_snapshot.internal_symbol,
                "quote_external_symbol": broker_symbol,
                "quote_provider": quote_snapshot.provider,
            },
        )



def _intent_to_broker_side(order_intent: OrderIntent) -> str:
    if order_intent.intent_type.value in {"BUY_TO_OPEN", "BUY_TO_CLOSE"}:
        return "BUY"
    return "SELL"


def _marketable_limit_price(*, order_intent: OrderIntent, quote_snapshot: QuoteSnapshot) -> Decimal:
    price_candidates = (
        (quote_snapshot.ask_price, quote_snapshot.last_price, quote_snapshot.mark_price)
        if order_intent.intent_type.value in {"BUY_TO_OPEN", "BUY_TO_CLOSE"}
        else (quote_snapshot.bid_price, quote_snapshot.last_price, quote_snapshot.mark_price)
    )
    for candidate in price_candidates:
        if candidate is None:
            continue
        return Decimal(str(candidate))
    raise RuntimeError(f"Could not derive a marketable execution price for {order_intent.symbol}.")
