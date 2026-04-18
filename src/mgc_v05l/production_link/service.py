"""Broker production-link service for broker truth and manual orders."""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from zoneinfo import ZoneInfo

from ..market_data import SchwabAuthError, SchwabOAuthClient, SchwabTokenStore, UrllibJsonTransport, load_schwab_auth_config_from_env
from ..market_data import SchwabQuoteHttpClient, load_schwab_market_data_config
from ..local_operator_auth import local_operator_auth_surface
from .client import BrokerHttpClient, BrokerHttpError
from .config import load_production_link_config
from .models import (
    BrokerAccountIdentity,
    BrokerBalanceSnapshot,
    BrokerOrderEvent,
    BrokerOrderRecord,
    BrokerPositionSnapshot,
    BrokerQuoteSnapshot,
    BrokerReconciliationRecord,
    ManualFlattenRequest,
    ManualOcoLegRequest,
    ManualOrderRequest,
    ProductionLinkConfig,
)
from .schema_validation import (
    build_broker_truth_shadow_validation_payload,
    validate_account_health_snapshot,
    validate_open_orders_rows,
    validate_order_status_sample,
    validate_position_rows,
)
from .store import ProductionLinkStore


class ProductionLinkActionError(RuntimeError):
    """Raised when a production-link operator action is invalid or fails."""


_ORDER_TYPE_LIVE_VERIFICATION_SEQUENCE: tuple[dict[str, Any], ...] = (
    {"step": 1, "key": "STOCK:LIMIT", "asset_class": "STOCK", "order_type": "LIMIT", "label": "STOCK LIMIT"},
    {"step": 2, "key": "STOCK:MARKET", "asset_class": "STOCK", "order_type": "MARKET", "label": "STOCK MARKET"},
    {"step": 3, "key": "STOCK:STOP", "asset_class": "STOCK", "order_type": "STOP", "label": "STOCK STOP"},
    {"step": 4, "key": "STOCK:STOP_LIMIT", "asset_class": "STOCK", "order_type": "STOP_LIMIT", "label": "STOCK STOP_LIMIT"},
    {"step": 5, "key": "STOCK:TRAIL_STOP", "asset_class": "STOCK", "order_type": "TRAIL_STOP", "label": "STOCK TRAIL_STOP"},
    {"step": 6, "key": "STOCK:TRAIL_STOP_LIMIT", "asset_class": "STOCK", "order_type": "TRAIL_STOP_LIMIT", "label": "STOCK TRAIL_STOP_LIMIT"},
    {"step": 7, "key": "STOCK:MARKET_ON_CLOSE", "asset_class": "STOCK", "order_type": "MARKET_ON_CLOSE", "label": "STOCK MARKET_ON_CLOSE"},
    {"step": 8, "key": "STOCK:LIMIT_ON_CLOSE", "asset_class": "STOCK", "order_type": "LIMIT_ON_CLOSE", "label": "STOCK LIMIT_ON_CLOSE"},
    {"step": 9, "key": "FUTURE:MARKET", "asset_class": "FUTURE", "order_type": "MARKET", "label": "FUTURE MARKET"},
    {"step": 10, "key": "FUTURE:LIMIT", "asset_class": "FUTURE", "order_type": "LIMIT", "label": "FUTURE LIMIT"},
    {"step": 11, "key": "FUTURE:STOP", "asset_class": "FUTURE", "order_type": "STOP", "label": "FUTURE STOP"},
    {"step": 12, "key": "FUTURE:STOP_LIMIT", "asset_class": "FUTURE", "order_type": "STOP_LIMIT", "label": "FUTURE STOP_LIMIT"},
    {"step": 13, "key": "FUTURE:TRAIL_STOP", "asset_class": "FUTURE", "order_type": "TRAIL_STOP", "label": "FUTURE TRAIL_STOP"},
    {"step": 14, "key": "FUTURE:TRAIL_STOP_LIMIT", "asset_class": "FUTURE", "order_type": "TRAIL_STOP_LIMIT", "label": "FUTURE TRAIL_STOP_LIMIT"},
    {"step": 15, "key": "ADVANCED:EXTO", "asset_class": "ADVANCED", "order_type": "EXTO", "label": "EXTO"},
    {"step": 16, "key": "ADVANCED:GTC_EXTO", "asset_class": "ADVANCED", "order_type": "GTC_EXTO", "label": "GTC_EXTO"},
    {"step": 17, "key": "ADVANCED:OCO", "asset_class": "ADVANCED", "order_type": "OCO", "label": "OCO"},
)

_NEAR_TERM_LIVE_VERIFICATION_RUNBOOKS: dict[str, dict[str, Any]] = {
    "STOCK:LIMIT": {
        "minimal_safe_test_shape": "1 share, STOCK, LIMIT, DAY, NORMAL, whitelisted symbol, review-confirmed, during regular US market hours.",
        "required_flags": [
            "MGC_PRODUCTION_LINK_ENABLED=1",
            "MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED=1",
            "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED=1",
            "MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED=1",
        ],
        "required_config": {
            "supported_manual_asset_classes": ["STOCK"],
            "supported_manual_order_types": ["LIMIT"],
            "manual_symbol_whitelist": ["<YOUR_SAFE_TEST_SYMBOL>"],
            "manual_max_quantity": "1",
            "supported_manual_time_in_force_values": ["DAY"],
            "supported_manual_session_values": ["NORMAL"],
        },
        "required_account_state": [
            "Selected account must be present and live-verified from Schwab.",
            "Auth must be healthy.",
            "Broker must be reachable.",
            "Reconciliation must be CLEAR.",
        ],
        "required_freshness_state": [
            "Balances refresh age must be inside broker_freshness_max_age_seconds.",
            "Positions refresh age must be inside broker_freshness_max_age_seconds.",
            "Orders refresh age must be inside broker_freshness_max_age_seconds.",
        ],
        "required_fields": ["account_hash", "symbol", "asset_class=STOCK", "side", "quantity=1", "order_type=LIMIT", "limit_price", "time_in_force=DAY", "session=NORMAL"],
        "submit_path": [
            "Open Positions -> Manual Order Ticket.",
            "Confirm selected account matches the intended live Schwab account.",
            "Set asset class STOCK, order type LIMIT, quantity 1, DAY, NORMAL, and a whitelisted symbol.",
            "Check Review confirmed.",
            "Use Review / Confirm / Send during regular US market hours.",
        ],
        "expected_broker_response_states": ["ACKNOWLEDGED", "WORKING", "FILLED", "REJECTED"],
        "expected_app_ui_panels": [
            "Positions -> Manual Order Ticket",
            "Positions -> Broker Orders and Fills",
            "Positions -> selected-position detail",
            "Diagnostics -> Production Link Diagnostics",
        ],
        "cancel_path": [
            "If the order remains WORKING, copy the broker order id from Broker Orders and Fills or Diagnostics.",
            "Enter that broker order id into Cancel Open Order in Positions -> Manual Order Ticket.",
            "Confirm cancel and refresh broker state.",
        ],
        "expected_reconciliation_checks": [
            "missing_local_orders = 0",
            "missing_broker_orders = 0",
            "quantity_mismatches = 0",
            "status_mismatches = 0",
            "position_mismatches = 0",
        ],
        "expected_post_submit_checks": [
            "Last Manual Order shows request, broker order id, and current status.",
            "Broker Orders and Fills shows the order as WORKING or FILLED.",
            "Selected-position detail shows linked order/fill events when the symbol matches.",
            "Diagnostics -> Production Link Diagnostics shows reconciliation remains CLEAR after refresh.",
        ],
        "cancel_expectation": "If the limit order rests WORKING, cancel should be tested next. If it fills immediately, do not force a second order just to test cancel.",
        "replace_expectation": "Replace remains disabled in this phase.",
    },
    "STOCK:MARKET": {
        "minimal_safe_test_shape": "1 share, STOCK, MARKET, DAY, NORMAL, whitelisted symbol, review-confirmed, only after STOCK LIMIT is live-verified.",
        "required_fields": ["account_hash", "symbol", "asset_class=STOCK", "side", "quantity", "order_type=MARKET", "time_in_force=DAY", "session=NORMAL"],
        "expected_broker_response_states": ["ACKNOWLEDGED", "FILLED", "REJECTED"],
        "expected_app_ui_panels": [
            "Positions -> Manual Order Ticket",
            "Positions -> Broker Orders and Fills",
            "Diagnostics -> Production Link Diagnostics",
        ],
        "expected_reconciliation_checks": [
            "last request persists locally",
            "broker order id appears in diagnostics",
            "reconciliation remains CLEAR after refresh",
        ],
        "cancel_expectation": "Do not expect cancel; market orders may fill immediately.",
        "replace_expectation": "Replace remains disabled in this phase.",
    },
    "STOCK:STOP": {
        "minimal_safe_test_shape": "1 share, STOCK, STOP, DAY, NORMAL, whitelisted symbol, non-marketable stop, review-confirmed, only after STOCK MARKET is live-verified.",
        "required_fields": ["account_hash", "symbol", "asset_class=STOCK", "side", "quantity", "order_type=STOP", "stop_price", "time_in_force=DAY", "session=NORMAL"],
        "expected_broker_response_states": ["ACKNOWLEDGED", "WORKING", "FILLED", "CANCELED", "REJECTED"],
        "expected_app_ui_panels": [
            "Positions -> Manual Order Ticket",
            "Positions -> Broker Orders and Fills",
            "Positions -> selected-position detail",
            "Diagnostics -> Production Link Diagnostics",
        ],
        "expected_reconciliation_checks": [
            "working stop appears in open orders",
            "broker/local status transitions stay aligned",
            "reconciliation remains CLEAR after cancel or fill refresh",
        ],
        "cancel_expectation": "If the stop remains WORKING, cancel should be attempted and reflected in recent events.",
        "replace_expectation": "Replace remains disabled in this phase.",
    },
    "STOCK:STOP_LIMIT": {
        "minimal_safe_test_shape": "1 share, STOCK, STOP_LIMIT, DAY, NORMAL, whitelisted symbol, non-marketable stop and limit, review-confirmed, only after STOCK STOP is live-verified.",
        "required_fields": [
            "account_hash",
            "symbol",
            "asset_class=STOCK",
            "side",
            "quantity",
            "order_type=STOP_LIMIT",
            "stop_price",
            "limit_price",
            "time_in_force=DAY",
            "session=NORMAL",
        ],
        "expected_broker_response_states": ["ACKNOWLEDGED", "WORKING", "FILLED", "CANCELED", "REJECTED"],
        "expected_app_ui_panels": [
            "Positions -> Manual Order Ticket",
            "Positions -> Broker Orders and Fills",
            "Positions -> selected-position detail",
            "Diagnostics -> Production Link Diagnostics",
        ],
        "expected_reconciliation_checks": [
            "open order reflects STOP_LIMIT fields",
            "broker/local status transitions stay aligned",
            "reconciliation remains CLEAR after refresh",
        ],
        "cancel_expectation": "If the stop-limit remains WORKING, cancel should be attempted and verified before any broader rollout.",
        "replace_expectation": "Replace remains disabled in this phase.",
    },
    "FUTURE:MARKET": {
        "minimal_safe_test_shape": "1 contract, FUTURE, MARKET, DAY, NORMAL, whitelisted mapped futures symbol, review-confirmed, manual operator initiated only.",
        "required_flags": [
            "MGC_PRODUCTION_LINK_ENABLED=1",
            "MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED=1",
            "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED=1",
            "MGC_PRODUCTION_FUTURES_PILOT_ENABLED=1",
            "MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED=1",
        ],
        "required_config": {
            "futures_supported_asset_classes": ["FUTURE"],
            "futures_supported_order_types": ["MARKET"],
            "futures_symbol_whitelist": ["<WHITELISTED_FUTURES_SYMBOL>"],
            "futures_max_quantity": "1",
            "futures_supported_time_in_force_values": ["DAY"],
            "futures_supported_session_values": ["NORMAL"],
        },
        "required_account_state": [
            "Selected account must be present and live-verified from Schwab.",
            "Auth must be healthy.",
            "Broker must be reachable.",
            "Reconciliation must be CLEAR.",
        ],
        "required_freshness_state": [
            "Balances refresh age must be inside broker_freshness_max_age_seconds.",
            "Positions refresh age must be inside broker_freshness_max_age_seconds.",
            "Orders refresh age must be inside broker_freshness_max_age_seconds.",
        ],
        "required_fields": [
            "account_hash",
            "symbol=<WHITELISTED_FUTURES_SYMBOL>",
            "asset_class=FUTURE",
            "side",
            "quantity=1",
            "order_type=MARKET",
            "time_in_force=DAY",
            "session=NORMAL",
        ],
        "submit_path": [
            "Open Positions -> Manual Order Ticket.",
            "Confirm selected account matches the intended live Schwab account.",
            "Set asset class FUTURE, order type MARKET, quantity 1, DAY, NORMAL, and a whitelisted mapped futures symbol.",
            "Check Review confirmed.",
            "Use Review / Confirm / Send through the narrow manual futures lane only.",
        ],
        "expected_broker_response_states": ["ACKNOWLEDGED", "FILLED", "REJECTED"],
        "expected_app_ui_panels": [
            "Positions -> Manual Order Ticket",
            "Positions -> Broker Orders and Fills",
            "Positions -> selected-position detail",
            "Diagnostics -> Production Link Diagnostics",
        ],
        "cancel_path": [
            "If broker acknowledgement is delayed or the order unexpectedly rests, copy the broker order id from Broker Orders and Fills or Diagnostics.",
            "Enter that broker order id into Cancel Open Order in Positions -> Manual Order Ticket.",
            "Confirm cancel and refresh broker state.",
        ],
        "expected_reconciliation_checks": [
            "missing_local_orders = 0",
            "missing_broker_orders = 0",
            "quantity_mismatches = 0",
            "status_mismatches = 0",
            "position_mismatches = 0",
        ],
        "expected_post_submit_checks": [
            "Last Manual Order shows request, broker order id, current status, and symbol authorization.",
            "Broker Orders and Fills shows the order as ACKNOWLEDGED or FILLED.",
            "Selected-position detail shows linked order/fill events when the symbol matches.",
            "Diagnostics -> Production Link Diagnostics shows reconciliation remains CLEAR after refresh.",
        ],
        "cancel_expectation": "Do not expect cancel by default; market orders may fill immediately. Only test cancel if the broker shows a genuine open order.",
        "replace_expectation": "Replace remains disabled in this phase.",
    },
}

_SYNC_CLOSED_ORDER_STATUS = "NOT_OPEN_ON_BROKER"
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", _SYNC_CLOSED_ORDER_STATUS}
_OPEN_ORDER_STATUSES = {"WORKING", "PENDING_ACTIVATION", "QUEUED", "AWAITING_PARENT_ORDER", "NEW", "OPEN"}
_MANUAL_LIVE_TERMINAL_STATES = {
    "FILLED",
    "CANCELED",
    "CANCELLED",
    "CANCELED_INFERRED",
    "SAFE_CLEANUP_RESOLVED",
    "REJECTED",
    "EXPIRED",
    "TERMINAL_NON_FILL_RESOLVED",
    "FAULT",
}
_DIRECT_STATUS_TERMINAL_STATES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
_MANUAL_LIVE_RESOLUTION_DIRECT_STATUS_RECHECK_SECONDS = 30


class SchwabProductionLinkService:
    """Thin, isolated broker truth surface for manual live operations."""

    def __init__(
        self,
        repo_root: Path,
        *,
        client_factory: Callable[[ProductionLinkConfig, SchwabOAuthClient], BrokerHttpClient] | None = None,
        quote_payload_fetcher: Callable[[Path, SchwabOAuthClient, list[str]], tuple[dict[str, Any], dict[str, Any]]] | None = None,
    ) -> None:
        self._repo_root = repo_root
        self._config = load_production_link_config(repo_root)
        self._store = ProductionLinkStore(self._config.database_path)
        self._store.save_provider_context(
            broker_provider_id=self._config.broker_provider_id,
            market_data_provider_id=self._config.market_data_provider_id,
        )
        self._lock = threading.RLock()
        self._cached_snapshot: dict[str, Any] | None = None
        self._cached_at: datetime | None = None
        self._last_error: str | None = None
        self._last_live_fetch_at: str | None = None
        self._client_factory = client_factory or self._default_client_factory
        self._quote_payload_fetcher = quote_payload_fetcher or self._default_quote_payload_fetcher
        self._manual_restore_validation_pending = True

    @property
    def config(self) -> ProductionLinkConfig:
        return self._config

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def snapshot(self, *, force_refresh: bool = False) -> dict[str, Any]:
        with self._lock:
            if not self._config.enabled:
                payload = self._disabled_snapshot("Production link is disabled by feature flag.")
                self._write_snapshot(payload)
                return payload

            now = datetime.now(timezone.utc)
            if not force_refresh and self._cached_snapshot and self._cached_at:
                cache_age = (now - self._cached_at).total_seconds()
                if cache_age <= self._config.cache_ttl_seconds:
                    cached = dict(self._cached_snapshot)
                    cached["diagnostics"] = {**as_dict(cached.get("diagnostics")), "cache_age_seconds": round(cache_age, 1)}
                    return cached

            try:
                live_snapshot = self._refresh_live_snapshot(now)
                self._cached_snapshot = live_snapshot
                self._cached_at = now
                self._last_error = None
                self._last_live_fetch_at = now.isoformat()
                self._write_snapshot(live_snapshot)
                return live_snapshot
            except (SchwabAuthError, BrokerHttpError, ProductionLinkActionError, FileNotFoundError, KeyError, ValueError) as exc:
                self._last_error = str(exc)
                degraded = self._degraded_snapshot(now, detail=str(exc))
                self._cached_snapshot = degraded
                self._cached_at = now
                self._write_snapshot(degraded)
                return degraded

    def run_action(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._config.enabled:
            raise ProductionLinkActionError("Schwab production link is disabled.")
        if action == "refresh":
            snapshot = self.snapshot(force_refresh=True)
            return {
                "ok": True,
                "action": action,
                "action_label": "Refresh Broker State",
                "message": f"Broker state refreshed from the live {_provider_label(self._config.broker_provider_id)} production-link path.",
                "output": snapshot.get("detail") or "Refresh completed.",
                "production_link": snapshot,
            }
        if action == "select-account":
            account_hash = str(payload.get("account_id") or payload.get("account_hash") or "").strip()
            if not account_hash:
                raise ProductionLinkActionError("select-account requires account_id or account_hash.")
            self._persist_selected_account(account_hash)
            snapshot = self.snapshot(force_refresh=True)
            return {
                "ok": True,
                "action": action,
                "action_label": "Select Broker Account",
                "message": f"Selected {_provider_label(self._config.broker_provider_id)} broker account {account_hash}.",
                "output": f"Selected account id {account_hash}.",
                "production_link": snapshot,
            }
        if action == "submit-order":
            self._require_manual_ticket_enabled()
            request = _manual_order_request_from_payload(payload, features=self._config.features)
            return self._submit_manual_order(request)
        if action == "preview-order":
            self._require_manual_ticket_enabled()
            request = _manual_order_request_from_payload(payload, features=self._config.features)
            return self._preview_manual_order(request)
        if action == "cancel-order":
            self._require_manual_ticket_enabled()
            account_hash = str(payload.get("account_id") or payload.get("account_hash") or "").strip()
            broker_order_id = str(payload.get("broker_order_id") or "").strip()
            if not account_hash or not broker_order_id:
                raise ProductionLinkActionError("cancel-order requires account_id or account_hash, plus broker_order_id.")
            self._assert_manual_cancel_safety(account_hash=account_hash, broker_order_id=broker_order_id)
            return self._cancel_order(account_hash=account_hash, broker_order_id=broker_order_id)
        if action == "replace-order":
            self._require_manual_ticket_enabled()
            if not self._config.features.replace_order_enabled:
                raise ProductionLinkActionError("Replace order is disabled until Schwab replace semantics are live-verified.")
            broker_order_id = str(payload.get("broker_order_id") or "").strip()
            if not broker_order_id:
                raise ProductionLinkActionError("replace-order requires broker_order_id.")
            request = _manual_order_request_from_payload(payload, features=self._config.features)
            return self._replace_order(request, broker_order_id=broker_order_id)
        if action == "flatten-position":
            self._require_manual_ticket_enabled()
            self._assert_manual_live_action_enabled()
            request = _manual_flatten_request_from_payload(payload)
            return self._flatten_position(request)
        if action == "reconcile":
            snapshot = self.snapshot(force_refresh=True)
            return {
                "ok": True,
                "action": action,
                "action_label": "Run Broker Reconciliation",
                "message": "Broker reconciliation refreshed from the latest live Schwab account truth.",
                "output": as_dict(snapshot.get("reconciliation")).get("detail") or "Reconciliation refreshed.",
                "production_link": snapshot,
            }
        if action == "validate-broker-truth":
            validation = self.validate_broker_truth_schemas(symbol=str(payload.get("symbol") or "MGC"))
            return {
                "ok": validation.get("summary", {}).get("result") in {"PASS", "WARN"},
                "action": action,
                "action_label": "Validate Broker Truth Schemas",
                "message": "Read-only broker-truth schema validation completed from the live Schwab shadow path.",
                "output": as_dict(validation.get("summary")).get("summary_line") or "Broker truth validation completed.",
                "production_link": self.snapshot(force_refresh=False),
                "validation": validation,
            }
        raise ProductionLinkActionError(f"Unsupported production-link action: {action}")

    def validate_broker_truth_schemas(self, *, symbol: str = "MGC") -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        snapshot = self.snapshot(force_refresh=True)
        target_symbol = str(symbol or "MGC").strip().upper() or "MGC"
        selected_account_hash = _selected_account_id_from_snapshot(snapshot) or None
        open_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("orders")).get("open_rows"))]
        recent_fill_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("orders")).get("recent_fill_rows"))]
        position_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("portfolio")).get("positions"))]

        direct_status_raw: dict[str, Any] | None = None
        direct_status_normalized: dict[str, Any] | None = None
        direct_status_error: str | None = None
        representative_order = next(
            (
                row for row in open_rows + recent_fill_rows
                if str(row.get("symbol") or "").strip().upper() == target_symbol and str(row.get("broker_order_id") or "").strip()
            ),
            None,
        ) or next(
            (row for row in open_rows + recent_fill_rows if str(row.get("broker_order_id") or "").strip()),
            None,
        )
        requested_broker_order_id = str(as_dict(representative_order).get("broker_order_id") or "").strip() or None
        if selected_account_hash and requested_broker_order_id:
            try:
                oauth_client, _ = self._build_oauth_client()
                client = self._client_factory(self._config, oauth_client)
                direct_status_raw = client.get_order_status(selected_account_hash, requested_broker_order_id)
                normalized = _normalize_orders(
                    [direct_status_raw],
                    account_hash=selected_account_hash,
                    fetched_at=now,
                    broker_provider_id=self._config.broker_provider_id,
                )
                if normalized:
                    direct_status_normalized = _broker_order_record_payload(normalized[0])
            except (SchwabAuthError, BrokerHttpError, ProductionLinkActionError, FileNotFoundError, KeyError, ValueError) as exc:
                direct_status_error = str(exc)

        order_status_validation = validate_order_status_sample(
            raw_payload=direct_status_raw,
            normalized_payload=direct_status_normalized,
            requested_broker_order_id=requested_broker_order_id,
        )
        if direct_status_error:
            order_status_validation["issues"] = list(order_status_validation.get("issues") or []) + ["direct_status_unavailable"]
            order_status_validation["direct_status_error"] = direct_status_error
        open_orders_validation = validate_open_orders_rows(normalized_rows=open_rows)
        position_validation = validate_position_rows(normalized_rows=position_rows, target_symbol=target_symbol)
        account_health_validation = validate_account_health_snapshot(snapshot=snapshot)
        payload = build_broker_truth_shadow_validation_payload(
            generated_at=now.isoformat(),
            selected_account_hash=selected_account_hash,
            target_symbol=target_symbol,
            timeframe="5m",
            direct_status_sample=order_status_validation,
            open_orders_validation=open_orders_validation,
            position_validation=position_validation,
            account_health_validation=account_health_validation,
        )
        payload["operator_path"] = "mgc-v05l probationary-broker-truth-shadow-validate"
        payload["summary"]["read_only_validation"] = True
        payload["summary"]["representative_broker_order_id"] = requested_broker_order_id
        json_path = self._config.snapshot_path.with_name("broker_truth_schema_validation_latest.json")
        markdown_path = self._config.snapshot_path.with_name("broker_truth_schema_validation_latest.md")
        payload["artifacts"] = {
            "json": str(json_path),
            "markdown": str(markdown_path),
        }
        json_path.write_text(_json_dumps(payload) + "\n", encoding="utf-8")
        markdown_path.write_text(_render_broker_truth_schema_validation_markdown(payload), encoding="utf-8")
        return payload

    def _refresh_live_snapshot(self, now: datetime) -> dict[str, Any]:
        oauth_client, auth_summary = self._build_oauth_client()
        client = self._client_factory(self._config, oauth_client)
        persisted_before_refresh = self._store.build_snapshot()

        account_number_rows = client.list_account_numbers()
        if not account_number_rows:
            raise ProductionLinkActionError("Schwab returned no broker accounts for the current token.")
        account_index = _account_number_index(account_number_rows)
        selection = self._resolve_selected_account(account_index)
        selected_account_hash = selection.get("account_hash")

        accounts_payload = client.list_accounts(fields=["positions"])
        normalized_accounts = _normalize_accounts(
            accounts_payload,
            account_index,
            selected_account_hash=selected_account_hash,
            fetched_at=now,
            broker_provider_id=self._config.broker_provider_id,
        )
        if not normalized_accounts:
            raise ProductionLinkActionError("Schwab account detail payload returned no normalized broker accounts.")
        if not selected_account_hash:
            selected_account_hash = normalized_accounts[0]["identity"].account_hash
        selected_account = next(
            (item for item in normalized_accounts if item["identity"].account_hash == selected_account_hash),
            normalized_accounts[0],
        )
        self._persist_selected_account(
            selected_account_hash,
            account_number=selected_account["identity"].account_number,
            display_name=selected_account["identity"].display_name,
            account_type=selected_account["identity"].account_type,
            source=str(selection.get("source") or "live_refresh"),
        )

        open_orders = client.get_orders(
            selected_account_hash,
            from_entered_time=(now - timedelta(days=self._config.open_orders_lookback_days)).isoformat(),
            to_entered_time=now.isoformat(),
            status="WORKING",
            max_results=100,
        )
        recent_orders = client.get_orders(
            selected_account_hash,
            from_entered_time=(now - timedelta(days=self._config.recent_fills_lookback_days)).isoformat(),
            to_entered_time=now.isoformat(),
            status="FILLED",
            max_results=100,
        )
        normalized_orders = _normalize_orders(
            open_orders + recent_orders,
            account_hash=selected_account_hash,
            fetched_at=now,
            broker_provider_id=self._config.broker_provider_id,
        )
        manual_state = self._load_manual_live_orders_state()
        direct_status_records = self._collect_post_ack_direct_status_records(
            client=client,
            selected_account_hash=selected_account_hash,
            manual_state=manual_state,
            normalized_orders=normalized_orders,
            live_positions=selected_account["positions"],
            now=now,
        )
        if direct_status_records:
            by_order_id = {order.broker_order_id: order for order in normalized_orders if order.broker_order_id}
            for order in direct_status_records:
                by_order_id[order.broker_order_id] = order
            normalized_orders = list(by_order_id.values())
        selected_symbols = sorted({position.symbol for position in selected_account["positions"] if position.symbol})
        normalized_quotes: list[BrokerQuoteSnapshot] = []
        quote_runtime: dict[str, Any] = {}
        quote_error: str | None = None
        quote_refresh_at = now.isoformat() if not selected_symbols else None
        if selected_symbols:
            try:
                quote_payload, quote_runtime = self._quote_payload_fetcher(
                    self._config.market_data_config_path,
                    oauth_client,
                    selected_symbols,
                )
                normalized_quotes = _normalize_quotes(
                    quote_payload,
                    account_hash=selected_account_hash,
                    symbols=selected_symbols,
                    fetched_at=now,
                    source=str(quote_runtime.get("source_label") or "schwab_quotes"),
                )
                quote_refresh_at = now.isoformat()
            except (FileNotFoundError, SchwabAuthError, BrokerHttpError, ValueError) as exc:
                quote_error = str(exc)
                quote_runtime = {
                    "auth_mode": "unavailable",
                    "source_label": "Broker quote overlay unavailable.",
                    "error": quote_error,
                }

        self._store.save_accounts(
            [item["identity"] for item in normalized_accounts],
            selected_account_hash=selected_account_hash,
        )
        for item in normalized_accounts:
            self._store.save_portfolio_snapshot(
                account_hash=item["identity"].account_hash,
                balances=item["balances"],
                positions=item["positions"],
            )
        self._store.save_quote_snapshot(account_hash=selected_account_hash, quotes=normalized_quotes)
        self._store.upsert_orders(normalized_orders, event_source="schwab_sync")
        live_open_order_ids = [
            order.broker_order_id
            for order in normalized_orders
            if str(order.status).upper() not in _TERMINAL_ORDER_STATUSES
        ]
        retired_open_order_ids = self._store.retire_absent_live_open_orders(
            account_hash=selected_account_hash,
            live_open_order_ids=live_open_order_ids,
            occurred_at=now,
            closed_status=_SYNC_CLOSED_ORDER_STATUS,
        )
        self._store.save_runtime_state(
            "last_refresh_summary",
            {
                "account_enumeration_at": now.isoformat(),
                "balances_refresh_at": selected_account["balances"].fetched_at.isoformat() if selected_account["balances"] else None,
                "positions_refresh_at": max((position.fetched_at.isoformat() for position in selected_account["positions"]), default=now.isoformat()),
                "orders_refresh_at": now.isoformat(),
                "quotes_refresh_at": quote_refresh_at,
                "quote_symbol_count": len(normalized_quotes),
                "quote_error": quote_error,
                "quote_runtime": quote_runtime,
                "selected_account_id": selected_account_hash,
                "selected_account_hash": selected_account_hash,
                "retired_open_order_ids": retired_open_order_ids,
            },
        )

        persisted_after_refresh = self._store.build_snapshot()
        reconciliation = self._reconcile(
            selected_account_hash=selected_account_hash,
            persisted_snapshot=persisted_after_refresh,
            live_orders=normalized_orders,
            live_positions=selected_account["positions"],
            manual_state=manual_state,
            now=now,
        )
        snapshot = self._store.build_snapshot()
        runtime_state = as_dict(snapshot.get("runtime_state"))
        last_refresh_summary = as_dict(runtime_state.get("last_refresh_summary"))
        last_manual_order = as_dict(runtime_state.get("last_manual_order"))
        last_manual_order_preview = as_dict(runtime_state.get("last_manual_order_preview"))
        snapshot["generated_at"] = now.isoformat()
        snapshot["status"] = "ready"
        snapshot["label"] = "CONNECTED"
        snapshot["detail"] = f"Using live {_provider_label(self._config.broker_provider_id)} broker truth for the selected production account."
        snapshot["source_of_record"] = f"{self._config.broker_provider_id}_broker"
        snapshot["enabled"] = True
        snapshot["feature_flags"] = asdict(self._config.features)
        snapshot["auth"] = auth_summary
        freshness = _build_live_freshness_summary(
            now=now,
            last_refresh_summary=last_refresh_summary,
            max_age_seconds=int(self._config.features.broker_freshness_max_age_seconds),
            quote_rows=as_list(as_dict(snapshot.get("quotes")).get("rows")),
        )
        snapshot["freshness"] = freshness
        snapshot["health"] = {
            "auth_healthy": {"ok": bool(auth_summary.get("ready")), "label": auth_summary.get("label"), "detail": auth_summary.get("detail")},
            "broker_reachable": {"ok": True, "label": "BROKER REACHABLE", "detail": "Live broker account endpoints responded in the current refresh cycle."},
            "account_selected": {"ok": bool(selected_account_hash), "label": "ACCOUNT SELECTED" if selected_account_hash else "ACCOUNT NOT SELECTED", "detail": selected_account["identity"].display_name},
            "balances_fresh": _health_from_freshness(freshness.get("balances")),
            "positions_fresh": _health_from_freshness(freshness.get("positions")),
            "quotes_fresh": _health_from_freshness(freshness.get("quotes")),
            "orders_fresh": _health_from_freshness(freshness.get("orders")),
            "fills_events_fresh": {
                **_health_from_freshness(freshness.get("fills")),
                "label": _health_from_freshness(freshness.get("fills")).get("label", "FILLS / EVENTS AVAILABLE"),
            },
            "reconciliation_fresh": {"ok": reconciliation.get("status") == "clear", "label": reconciliation.get("label"), "detail": reconciliation.get("detail")},
        }
        snapshot["capabilities"] = _capabilities_snapshot(self._config.features)
        snapshot["diagnostics"] = {
            "database_path": str(self._config.database_path),
            "snapshot_path": str(self._config.snapshot_path),
            "selected_account_path": str(self._config.selected_account_path),
            "selected_account_id": selected_account_hash,
            "selected_account_hash": selected_account_hash,
            "selected_account_number": selected_account["identity"].account_number,
            "selected_account_display_name": selected_account["identity"].display_name,
            "account_enumeration_at": last_refresh_summary.get("account_enumeration_at") or now.isoformat(),
            "last_balances_refresh_at": last_refresh_summary.get("balances_refresh_at"),
            "last_positions_refresh_at": last_refresh_summary.get("positions_refresh_at"),
            "last_quotes_refresh_at": last_refresh_summary.get("quotes_refresh_at"),
            "last_orders_refresh_at": last_refresh_summary.get("orders_refresh_at"),
            "last_fills_refresh_at": last_refresh_summary.get("orders_refresh_at"),
            "last_reconciliation_at": reconciliation.get("created_at"),
            "last_live_fetch_at": now.isoformat(),
            "cache_age_seconds": 0,
            "last_error": None,
            "last_quote_error": quote_error,
            "quote_runtime": quote_runtime,
            "last_manual_order_request": last_manual_order.get("request"),
            "last_manual_order_result": last_manual_order.get("result"),
            "last_manual_order_preview": last_manual_order_preview,
            "order_lifecycle_readiness": {
                "last_request": last_manual_order.get("request"),
                "last_result": last_manual_order.get("result"),
                "reconciliation_state": reconciliation.get("label") or reconciliation.get("status"),
            },
            "manual_order_live_verification": {
                "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                "live_verified_order_keys": snapshot["capabilities"].get("live_verified_order_keys"),
                "sequence": snapshot["capabilities"].get("order_type_live_verification_sequence"),
                "next_step": snapshot["capabilities"].get("next_live_verification_step"),
                "runbooks": snapshot["capabilities"].get("near_term_live_verification_runbooks"),
            },
            "config_path": str(self._config.config_path) if self._config.config_path else None,
            "trader_api_base_url": self._config.trader_api_base_url,
            "market_data_config_path": str(self._config.market_data_config_path),
            "attached_mode": "selected_account",
            "live_verified_endpoint_paths": [
                "/accounts/accountNumbers",
                "/accounts?fields=positions",
                "/marketdata/v1/quotes",
                f"/accounts/{selected_account_hash}/orders?status=WORKING",
                f"/accounts/{selected_account_hash}/orders?status=FILLED",
                f"/accounts/{selected_account_hash}/orders/{{broker_order_id}}",
            ],
            "implemented_endpoint_paths": [
                "/accounts/accountNumbers",
                "/accounts",
                f"/accounts/{selected_account_hash}",
                f"/accounts/{selected_account_hash}/orders",
                f"/accounts/{selected_account_hash}/orders/{{broker_order_id}}",
            ],
            "endpoint_uncertainty": [
                "Manual order endpoints and account-order status filters follow Schwab trader API conventions but were not live-verified in this sandboxed coding session.",
                "EXTO / GTC_EXTO and OCO payload shapes are available for dry-run review only in this phase; live submission remains disabled pending broker verification.",
            ],
        }
        snapshot["connection"] = {
            "broker_name": _provider_label(self._config.broker_provider_id),
            "broker_provider_id": self._config.broker_provider_id,
            "market_data_provider_id": self._config.market_data_provider_id,
            "selected_account_id": selected_account_hash,
            "selected_account_hash": selected_account_hash,
            "selected_account_number": selected_account["identity"].account_number,
            "selected_account_display_name": selected_account["identity"].display_name,
            "selection_source": selection.get("source"),
            "request_timeout_seconds": self._config.request_timeout_seconds,
            "cache_ttl_seconds": self._config.cache_ttl_seconds,
        }
        snapshot["reconciliation"] = reconciliation
        snapshot["local_operator_auth"] = self._local_operator_auth_surface()
        snapshot["manual_order_safety"] = self._manual_order_safety_snapshot(snapshot=snapshot, now=now)
        snapshot = self._augment_manual_live_order_surface(snapshot=snapshot, now=now)
        return self._attach_operator_status(snapshot)

    def _submit_manual_order(self, request: ManualOrderRequest) -> dict[str, Any]:
        futures_symbol_resolution = self._resolve_futures_live_submit_symbol(request)
        self._assert_manual_order_safety(request, futures_symbol_resolution=futures_symbol_resolution)
        broker_submit_symbol = str(futures_symbol_resolution.get("broker_submit_symbol") or "").strip() or request.symbol
        order_payload = _build_schwab_order_payload(
            request,
            features=self._config.features,
            broker_symbol_override=broker_submit_symbol,
            broker_order_type_override=str(futures_symbol_resolution.get("broker_transport_order_type") or "").strip() or None,
            broker_limit_price_override=_decimal(futures_symbol_resolution.get("broker_transport_limit_price")),
        )
        now = datetime.now(timezone.utc)
        time_session_policy_decision = _futures_pilot_time_session_policy_decision(request, now=now, features=self._config.features)
        action_phase = _futures_pilot_action_phase(request, preview=False)
        symbol_authorization = (
            _futures_symbol_authorization_decision(request.symbol, features=self._config.features)
            if _is_futures_pilot_request(request)
            else None
        )
        client = self._client_factory(self._config, self._build_oauth_client()[0])
        broker_preview_result = self._broker_preview_result(
            client=client,
            request=request,
            order_payload=order_payload,
            futures_symbol_resolution=futures_symbol_resolution,
            symbol_authorization=symbol_authorization,
            time_session_policy_decision=time_session_policy_decision,
        )
        self._manual_restore_validation_pending = False
        request_payload = _manual_order_request_json(request)
        if broker_preview_result is not None and not bool(broker_preview_result.get("ok")):
            failed_at = datetime.now(timezone.utc)
            failure_detail = str(
                broker_preview_result.get("error")
                or "Schwab broker preview rejected this futures payload before live submit."
            ).strip()
            self._store.record_order_event(
                BrokerOrderEvent(
                    account_hash=request.account_hash,
                    broker_order_id=None,
                    client_order_id=request.client_order_id,
                    event_type="submit_failed",
                    status="FAILED",
                    occurred_at=failed_at,
                    message=failure_detail,
                    request_payload=order_payload,
                    response_payload={
                        "error": failure_detail,
                        "action_phase": action_phase,
                        "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                        "symbol_authorization": symbol_authorization,
                        "futures_symbol_resolution": futures_symbol_resolution,
                        "time_session_policy_decision": time_session_policy_decision,
                        "broker_preview_result": broker_preview_result,
                    },
                    source="manual_ticket",
                )
            )
            self._record_manual_validation_event(
                scenario_type="manual_live_submit_failed",
                occurred_at=failed_at,
                payload={
                    "intent_parameters": request_payload,
                    "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                    "submit_attempted_at": now.isoformat(),
                    "failed_at": failed_at.isoformat(),
                    "error": failure_detail,
                    "duplicate_action_prevention_held": True,
                    "action_phase": action_phase,
                    "gate_summary": _futures_pilot_gate_summary(self.snapshot(force_refresh=False)) if _is_futures_pilot_request(request) else None,
                    "symbol_authorization": symbol_authorization,
                    "futures_symbol_resolution": futures_symbol_resolution,
                    "time_session_policy_decision": time_session_policy_decision,
                    "broker_preview_result": broker_preview_result,
                },
            )
            self._store.save_runtime_state(
                "last_manual_order",
                {
                    "request": {**request_payload, "requested_at": now.isoformat()},
                    "result": {
                        "ok": False,
                        "error": failure_detail,
                        "failed_at": failed_at.isoformat(),
                        "action_phase": action_phase,
                        "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                        "symbol_authorization": symbol_authorization,
                        "futures_symbol_resolution": futures_symbol_resolution,
                        "time_session_policy_decision": time_session_policy_decision,
                        "broker_preview_result": broker_preview_result,
                    },
                },
            )
            raise ProductionLinkActionError(failure_detail)
        self._store.save_runtime_state(
            "last_manual_order",
            {
                "request": {**request_payload, "requested_at": now.isoformat()},
                "result": None,
            },
        )
        self._store.record_order_event(
            BrokerOrderEvent(
                account_hash=request.account_hash,
                broker_order_id=None,
                client_order_id=request.client_order_id,
                event_type="submit_requested",
                status="REQUESTED",
                occurred_at=now,
                message=f"{request.order_type} {request.side} {request.quantity} {request.symbol}",
                request_payload=order_payload,
                response_payload={
                    "request": request_payload,
                    "action_phase": action_phase,
                    "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                    "symbol_authorization": symbol_authorization,
                    "futures_symbol_resolution": futures_symbol_resolution,
                    "time_session_policy_decision": time_session_policy_decision,
                    "broker_preview_result": broker_preview_result,
                },
                source="manual_ticket",
            )
        )
        try:
            response = client.submit_order(request.account_hash, order_payload)
        except BrokerHttpError as exc:
            failed_at = datetime.now(timezone.utc)
            failure_detail = str(exc)
            self._store.record_order_event(
                BrokerOrderEvent(
                    account_hash=request.account_hash,
                    broker_order_id=None,
                    client_order_id=request.client_order_id,
                    event_type="submit_failed",
                    status="FAILED",
                    occurred_at=failed_at,
                    message=failure_detail,
                    request_payload=order_payload,
                    response_payload={
                        "error": failure_detail,
                        "action_phase": action_phase,
                        "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                        "symbol_authorization": symbol_authorization,
                        "futures_symbol_resolution": futures_symbol_resolution,
                        "time_session_policy_decision": time_session_policy_decision,
                        "broker_preview_result": broker_preview_result,
                    },
                    source="manual_ticket",
                )
            )
            self._record_manual_validation_event(
                scenario_type="manual_live_submit_failed",
                occurred_at=failed_at,
                payload={
                    "intent_parameters": request_payload,
                    "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                    "submit_attempted_at": now.isoformat(),
                    "failed_at": failed_at.isoformat(),
                    "error": failure_detail,
                    "duplicate_action_prevention_held": True,
                    "action_phase": action_phase,
                    "gate_summary": _futures_pilot_gate_summary(self.snapshot(force_refresh=False)) if _is_futures_pilot_request(request) else None,
                    "symbol_authorization": symbol_authorization,
                    "futures_symbol_resolution": futures_symbol_resolution,
                    "time_session_policy_decision": time_session_policy_decision,
                    "broker_preview_result": broker_preview_result,
                },
            )
            self._store.save_runtime_state(
                "last_manual_order",
                {
                    "request": {**request_payload, "requested_at": now.isoformat()},
                    "result": {
                        "ok": False,
                        "error": failure_detail,
                        "failed_at": failed_at.isoformat(),
                        "action_phase": action_phase,
                        "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                        "symbol_authorization": symbol_authorization,
                        "futures_symbol_resolution": futures_symbol_resolution,
                        "time_session_policy_decision": time_session_policy_decision,
                        "broker_preview_result": broker_preview_result,
                    },
                },
            )
            raise
        broker_order_id = str(response.get("broker_order_id") or f"pending-{uuid.uuid4().hex[:12]}")
        acknowledged_at = datetime.now(timezone.utc)
        post_ack_grace_expires_at = (acknowledged_at + timedelta(seconds=int(self._config.manual_order_post_ack_grace_seconds))).isoformat()
        self._store.record_order_event(
            BrokerOrderEvent(
                account_hash=request.account_hash,
                broker_order_id=broker_order_id,
                client_order_id=request.client_order_id,
                event_type="submit_acknowledged",
                status="ACKNOWLEDGED",
                occurred_at=acknowledged_at,
                message=f"Schwab acknowledged manual order for {request.symbol}.",
                request_payload=order_payload,
                response_payload={
                    "action_phase": action_phase,
                    "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                    "symbol_authorization": symbol_authorization,
                    "futures_symbol_resolution": futures_symbol_resolution,
                    "broker_response": response,
                    "time_session_policy_decision": time_session_policy_decision,
                    "broker_preview_result": broker_preview_result,
                },
                source="manual_ticket",
            )
        )
        self._record_manual_validation_event(
            scenario_type="manual_live_submit",
            occurred_at=acknowledged_at,
            payload={
                "intent_parameters": request_payload,
                "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                "broker_order_id": broker_order_id,
                "submit_attempted_at": now.isoformat(),
                "acknowledged_at": acknowledged_at.isoformat(),
                "broker_status_snapshot": {
                    "status_code": response.get("status_code"),
                    "location": response.get("location"),
                    "body": response.get("body"),
                },
                "duplicate_action_prevention_held": True,
                "action_phase": action_phase,
                "gate_summary": _futures_pilot_gate_summary(self.snapshot(force_refresh=False)) if _is_futures_pilot_request(request) else None,
                "symbol_authorization": symbol_authorization,
                "futures_symbol_resolution": futures_symbol_resolution,
                "time_session_policy_decision": time_session_policy_decision,
                "broker_preview_result": broker_preview_result,
            },
        )
        self._store.upsert_orders(
            [
                BrokerOrderRecord(
                    broker_order_id=broker_order_id,
                    account_hash=request.account_hash,
                    client_order_id=request.client_order_id,
                    symbol=broker_submit_symbol,
                    description=None,
                    asset_class=request.asset_class,
                    instruction=request.side,
                    quantity=request.quantity,
                    filled_quantity=None,
                    order_type=request.order_type,
                    duration=request.time_in_force,
                    session=request.session,
                    status="WORKING",
                    entered_at=now,
                    closed_at=None,
                    updated_at=acknowledged_at,
                    limit_price=request.limit_price,
                    stop_price=request.stop_price,
                    source="manual_ticket_local",
                    raw_payload={
                        "manual_ticket": True,
                        "intent_type": request.intent_type,
                        "operator_note": request.operator_note,
                        "operator_authenticated": request.operator_authenticated,
                        "local_operator_identity": request.local_operator_identity,
                        "action_phase": action_phase,
                        "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                        "symbol_authorization": symbol_authorization,
                        "futures_symbol_resolution": futures_symbol_resolution,
                        "time_session_policy_decision": time_session_policy_decision,
                    },
                )
            ],
            event_source="manual_ticket_local",
        )
        self._upsert_manual_live_order_state(
            {
                "broker_order_id": broker_order_id,
                "client_order_id": request.client_order_id,
                "account_hash": request.account_hash,
                "symbol": broker_submit_symbol,
                "asset_class": request.asset_class,
                "intent_type": request.intent_type or "MANUAL",
                "operator_note": request.operator_note,
                "side": request.side,
                "quantity": str(request.quantity),
                "order_type": request.order_type,
                "structure_type": request.structure_type,
                "time_in_force": request.time_in_force,
                "session": request.session,
                "created_at": now.isoformat(),
                "submitted_at": now.isoformat(),
                "acknowledged_at": acknowledged_at.isoformat(),
                "post_ack_grace_started_at": acknowledged_at.isoformat(),
                "post_ack_grace_expires_at": post_ack_grace_expires_at,
                "direct_status_last_checked_at": None,
                "direct_status_last_outcome": "PENDING",
                "direct_status_last_detail": "Awaiting broker order-status confirmation inside the post-ack grace window.",
                "direct_status_confirmed_status": None,
                "filled_at": None,
                "first_open_order_observed_at": None,
                "first_fill_observed_at": None,
                "first_position_observed_at": None,
                "cancel_requested_at": None,
                "cancelled_at": None,
                "broker_order_status": "ACKNOWLEDGED",
                "broker_filled_quantity": None,
                "lifecycle_state": "SUBMITTED",
                "lifecycle_classification": "submitted_waiting_normally",
                "issue_code": None,
                "issue_detail": None,
                "recommended_action": "Monitor broker acknowledgement and fill state.",
                "manual_action_required": False,
                "active": True,
                "source": "manual_ticket",
                "operator_authenticated": request.operator_authenticated,
                "operator_reduce_only_authorized": request.operator_reduce_only_authorized,
                "operator_auth_policy": request.operator_auth_policy,
                "operator_auth_risk_bucket": request.operator_auth_risk_bucket,
                "local_operator_identity": request.local_operator_identity,
                "auth_session_id": request.auth_session_id,
                "auth_method": request.auth_method,
                "authenticated_at": request.authenticated_at,
                "action_phase": action_phase,
                "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                "symbol_authorization": symbol_authorization,
                "futures_symbol_resolution": futures_symbol_resolution,
                "time_session_policy_decision": time_session_policy_decision,
                "broker_preview_result": broker_preview_result,
            }
        )
        self._store.save_runtime_state(
            "last_manual_order",
            {
                "request": {**request_payload, "requested_at": now.isoformat()},
                "result": {
                    "broker_order_id": broker_order_id,
                    "status_code": response.get("status_code"),
                    "location": response.get("location"),
                    "received_at": acknowledged_at.isoformat(),
                    "operator_auth_policy": request.operator_auth_policy,
                    "operator_auth_risk_bucket": request.operator_auth_risk_bucket,
                    "action_phase": action_phase,
                    "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                    "gate_summary": _futures_pilot_gate_summary(self.snapshot(force_refresh=False)) if _is_futures_pilot_request(request) else None,
                    "symbol_authorization": symbol_authorization,
                    "futures_symbol_resolution": futures_symbol_resolution,
                    "time_session_policy_decision": time_session_policy_decision,
                    "broker_preview_result": broker_preview_result,
                },
            },
        )
        snapshot = self.snapshot(force_refresh=True)
        return {
            "ok": True,
            "action": "submit-order",
            "action_label": "Send Manual Broker Order",
            "message": f"Submitted manual broker order for {request.symbol}.",
            "output": json.dumps(response, sort_keys=True),
            "production_link": snapshot,
        }

    def _preview_manual_order(self, request: ManualOrderRequest) -> dict[str, Any]:
        snapshot = self.snapshot(force_refresh=True)
        self._assert_manual_preview_support(request)
        now = datetime.now(timezone.utc)
        futures_symbol_resolution = self._resolve_futures_live_submit_symbol(request)
        order_payload = _build_schwab_order_payload(
            request,
            features=self._config.features,
            broker_symbol_override=str(futures_symbol_resolution.get("broker_submit_symbol") or "").strip() or None,
            broker_order_type_override=str(futures_symbol_resolution.get("broker_transport_order_type") or "").strip() or None,
            broker_limit_price_override=_decimal(futures_symbol_resolution.get("broker_transport_limit_price")),
        )
        structure_summary = _manual_order_structure_summary(request)
        live_submit_blockers = self._manual_order_live_submit_blockers(snapshot=snapshot, request=request, now=now)
        live_submit_blockers.extend(as_list(futures_symbol_resolution.get("blockers")))
        live_submit_blockers = list(dict.fromkeys(str(item) for item in live_submit_blockers if str(item).strip()))
        time_session_policy_decision = _futures_pilot_time_session_policy_decision(request, now=now, features=self._config.features)
        action_phase = _futures_pilot_action_phase(request, preview=True)
        symbol_authorization = (
            _futures_symbol_authorization_decision(request.symbol, features=self._config.features)
            if _is_futures_pilot_request(request)
            else None
        )
        broker_preview_result = None
        if _is_futures_pilot_request(request) and len(live_submit_blockers) == 0:
            client = self._client_factory(self._config, self._build_oauth_client()[0])
            broker_preview_result = self._broker_preview_result(
                client=client,
                request=request,
                order_payload=order_payload,
                futures_symbol_resolution=futures_symbol_resolution,
                symbol_authorization=symbol_authorization,
                time_session_policy_decision=time_session_policy_decision,
            )
            if broker_preview_result is not None and not bool(broker_preview_result.get("ok")):
                live_submit_blockers.append(
                    str(
                        broker_preview_result.get("error")
                        or "Schwab broker preview rejected this futures payload before live submit."
                    ).strip()
                )
        live_submit_blockers = list(dict.fromkeys(str(item) for item in live_submit_blockers if str(item).strip()))
        preview_payload = {
            "requested_at": now.isoformat(),
            "request": _manual_order_request_json(request),
            "structure_summary": structure_summary,
            "route_scope": "futures_pilot" if _is_futures_pilot_request(request) else "stock_pilot_or_manual",
            "payload_summary": {
                "intended_schwab_payload": order_payload,
                "resolved_broker_symbol": str(futures_symbol_resolution.get("broker_submit_symbol") or "").strip()
                or _resolved_broker_symbol_for_request(request, features=self._config.features),
                "client_order_id_omitted": _omit_client_order_id_for_live_pilot(request, features=self._config.features),
                "advanced_mode": _advanced_mode_label(request),
                "unverified_fields": _advanced_unverified_fields(request),
                "futures_symbol_resolution": futures_symbol_resolution,
                "broker_transport_order_type": str(futures_symbol_resolution.get("broker_transport_order_type") or "").strip() or None,
                "broker_transport_limit_price": futures_symbol_resolution.get("broker_transport_limit_price"),
                "broker_preview_result": broker_preview_result,
            },
            "action_phase": action_phase,
            "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
            "symbol_authorization": symbol_authorization,
            "gate_summary": _futures_pilot_gate_summary(snapshot) if _is_futures_pilot_request(request) else None,
            "time_session_policy_decision": time_session_policy_decision,
            "live_submit_enabled": len(live_submit_blockers) == 0,
            "live_submit_blockers": live_submit_blockers,
            "feature_flags": {
                "stock_market_live_submit_enabled": self._config.features.stock_market_live_submit_enabled,
                "stock_limit_live_submit_enabled": self._config.features.stock_limit_live_submit_enabled,
                "stock_stop_live_submit_enabled": self._config.features.stock_stop_live_submit_enabled,
                "stock_stop_limit_live_submit_enabled": self._config.features.stock_stop_limit_live_submit_enabled,
                "advanced_tif_enabled": self._config.features.advanced_tif_enabled,
                "ext_exto_ticket_support_enabled": self._config.features.ext_exto_ticket_support_enabled,
                "oco_ticket_support_enabled": self._config.features.oco_ticket_support_enabled,
                "ext_exto_live_submit_enabled": self._config.features.ext_exto_live_submit_enabled,
                "oco_live_submit_enabled": self._config.features.oco_live_submit_enabled,
                "trailing_live_submit_enabled": self._config.features.trailing_live_submit_enabled,
                "close_order_live_submit_enabled": self._config.features.close_order_live_submit_enabled,
                "futures_live_submit_enabled": self._config.features.futures_live_submit_enabled,
                "live_verified_order_keys": list(self._config.features.live_verified_order_keys),
            },
            "mode": "dry_run_only",
        }
        self._store.save_runtime_state("last_manual_order_preview", preview_payload)
        self._store.record_order_event(
            BrokerOrderEvent(
                account_hash=request.account_hash,
                broker_order_id=None,
                client_order_id=request.client_order_id,
                event_type="preview_built",
                status="DRY_RUN",
                occurred_at=now,
                message=f"Built dry-run payload preview for {request.structure_type} {request.symbol}.",
                request_payload=_manual_order_request_json(request),
                response_payload=preview_payload["payload_summary"],
                source="manual_ticket",
            )
        )
        self._record_manual_validation_event(
            scenario_type="manual_live_preview",
            occurred_at=now,
            payload={
                "intent_parameters": _manual_order_request_json(request),
                "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                "preview_payload": preview_payload,
                "duplicate_action_prevention_held": True,
            },
        )
        refreshed_snapshot = json.loads(_json_dumps(self._cached_snapshot or self._store.build_snapshot()))
        runtime_state = as_dict(refreshed_snapshot.get("runtime_state"))
        runtime_state["last_manual_order_preview"] = preview_payload
        refreshed_snapshot["runtime_state"] = runtime_state
        diagnostics = as_dict(refreshed_snapshot.get("diagnostics"))
        diagnostics["last_manual_order_preview"] = preview_payload
        refreshed_snapshot["diagnostics"] = diagnostics
        refreshed_snapshot["local_operator_auth"] = self._local_operator_auth_surface()
        refreshed_snapshot = self._attach_operator_status(refreshed_snapshot)
        self._cached_snapshot = refreshed_snapshot
        self._cached_at = now
        self._write_snapshot(refreshed_snapshot)
        return {
            "ok": True,
            "action": "preview-order",
            "action_label": "Build Manual Order Dry-Run",
            "message": "Built a dry-run broker payload preview without sending a live order.",
            "output": _json_dumps(preview_payload),
            "payload": preview_payload,
            "production_link": refreshed_snapshot,
        }

    def _broker_preview_result(
        self,
        *,
        client: BrokerHttpClient,
        request: ManualOrderRequest,
        order_payload: dict[str, Any],
        futures_symbol_resolution: dict[str, Any] | None,
        symbol_authorization: dict[str, Any] | None,
        time_session_policy_decision: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not _is_futures_pilot_request(request):
            return None
        try:
            response = client.preview_order(request.account_hash, order_payload)
        except BrokerHttpError as exc:
            return {
                "ok": False,
                "error": f"Schwab broker preview rejected this futures payload before live submit: {exc}",
                "action_phase": _futures_pilot_action_phase(request, preview=True),
                "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
                "symbol_authorization": symbol_authorization,
                "futures_symbol_resolution": futures_symbol_resolution,
                "time_session_policy_decision": time_session_policy_decision,
            }
        return {
            "ok": True,
            "response": response,
            "action_phase": _futures_pilot_action_phase(request, preview=True),
            "allowing_rule": as_dict(time_session_policy_decision).get("audit_label") if time_session_policy_decision else None,
            "symbol_authorization": symbol_authorization,
            "futures_symbol_resolution": futures_symbol_resolution,
            "time_session_policy_decision": time_session_policy_decision,
        }

    def _cancel_order(self, *, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        client = self._client_factory(self._config, self._build_oauth_client()[0])
        self._manual_restore_validation_pending = False
        cancel_requested_at = datetime.now(timezone.utc)
        response = client.cancel_order(account_hash, broker_order_id)
        self._store.record_order_event(
            BrokerOrderEvent(
                account_hash=account_hash,
                broker_order_id=broker_order_id,
                client_order_id=None,
                event_type="cancel_requested",
                status="CANCEL_REQUESTED",
                occurred_at=cancel_requested_at,
                message=f"Cancel requested for broker order {broker_order_id}.",
                request_payload={"broker_order_id": broker_order_id},
                response_payload=response,
                source="manual_ticket",
            )
        )
        self._record_manual_validation_event(
            scenario_type="manual_live_cancel",
            occurred_at=cancel_requested_at,
            payload={
                "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                "broker_order_id": broker_order_id,
                "account_hash": account_hash,
                "cancel_requested_at": cancel_requested_at.isoformat(),
                "broker_status_snapshot": response,
                "duplicate_action_prevention_held": True,
            },
        )
        state = self._load_manual_live_orders_state()
        updated_orders: list[dict[str, Any]] = []
        for row in as_list(state.get("orders")):
            current = as_dict(row)
            if str(current.get("broker_order_id") or "").strip() == broker_order_id:
                updated_current = {
                    **current,
                    "cancel_requested_at": cancel_requested_at.isoformat(),
                    "broker_order_status": "CANCEL_REQUESTED",
                    "lifecycle_state": "CANCEL_REQUESTED",
                    "lifecycle_classification": "cancel_requested",
                    "recommended_action": "Wait for the broker cancel acknowledgement and refresh the broker order book.",
                    "manual_action_required": False,
                    "active": True,
                }
                updated_orders.append(updated_current)
            else:
                updated_orders.append(current)
        state["orders"] = updated_orders
        state["updated_at"] = cancel_requested_at.isoformat()
        self._save_manual_live_orders_state(state)
        snapshot = self.snapshot(force_refresh=True)
        return {
            "ok": True,
            "action": "cancel-order",
            "action_label": "Cancel Open Broker Order",
            "message": f"Cancel requested for broker order {broker_order_id}.",
            "output": json.dumps(response, sort_keys=True),
            "production_link": snapshot,
        }

    def _replace_order(self, request: ManualOrderRequest, *, broker_order_id: str) -> dict[str, Any]:
        order_payload = _build_schwab_order_payload(request, features=self._config.features)
        client = self._client_factory(self._config, self._build_oauth_client()[0])
        response = client.replace_order(request.account_hash, broker_order_id, order_payload)
        self._store.record_order_event(
            BrokerOrderEvent(
                account_hash=request.account_hash,
                broker_order_id=broker_order_id,
                client_order_id=request.client_order_id,
                event_type="replace_requested",
                status="REPLACE_REQUESTED",
                occurred_at=datetime.now(timezone.utc),
                message=f"Replace requested for broker order {broker_order_id}.",
                request_payload=order_payload,
                response_payload=response,
                source="manual_ticket",
            )
        )
        snapshot = self.snapshot(force_refresh=True)
        return {
            "ok": True,
            "action": "replace-order",
            "action_label": "Replace Broker Order",
            "message": f"Replace requested for broker order {broker_order_id}.",
            "output": json.dumps(response, sort_keys=True),
            "production_link": snapshot,
        }

    def _flatten_position(self, request: ManualFlattenRequest) -> dict[str, Any]:
        if request.asset_class not in set(self._config.features.supported_manual_asset_classes):
            raise ProductionLinkActionError(
                f"Flatten is not enabled for asset class {request.asset_class}. Supported classes: {', '.join(self._config.features.supported_manual_asset_classes)}."
            )
        reverse_side = "SELL" if request.side.upper() in {"LONG", "BUY"} else "BUY_TO_COVER"
        order_request = ManualOrderRequest(
            account_hash=request.account_hash,
            symbol=request.symbol,
            asset_class=request.asset_class,
            structure_type="SINGLE",
            intent_type="FLATTEN",
            side=reverse_side,
            quantity=request.quantity,
            order_type="MARKET",
            limit_price=None,
            stop_price=None,
            trail_value_type=None,
            trail_value=None,
            trail_trigger_basis=None,
            trail_limit_offset=None,
            time_in_force=request.time_in_force,
            session=request.session,
            review_confirmed=True,
            operator_note="Flatten broker position",
            client_order_id=f"flatten-{uuid.uuid4().hex[:10]}",
            operator_authenticated=request.operator_authenticated,
            operator_reduce_only_authorized=request.operator_reduce_only_authorized,
            operator_auth_policy=request.operator_auth_policy,
            operator_auth_risk_bucket=request.operator_auth_risk_bucket,
            local_operator_identity=request.local_operator_identity,
            auth_session_id=request.auth_session_id,
            auth_method=request.auth_method,
            authenticated_at=request.authenticated_at,
        )
        result = self._submit_manual_order(order_request)
        result["action"] = "flatten-position"
        result["action_label"] = "Flatten Broker Position"
        result["message"] = f"Submitted flatten order for {request.symbol}."
        return result

    def _collect_post_ack_direct_status_records(
        self,
        *,
        client: BrokerHttpClient,
        selected_account_hash: str,
        manual_state: dict[str, Any],
        normalized_orders: list[BrokerOrderRecord],
        live_positions: list[BrokerPositionSnapshot],
        now: datetime,
    ) -> list[BrokerOrderRecord]:
        known_order_ids = {order.broker_order_id for order in normalized_orders if order.broker_order_id}
        open_by_symbol: dict[str, list[BrokerOrderRecord]] = {}
        fill_order_ids: set[str] = set()
        fill_client_order_ids: set[str] = set()
        for order in normalized_orders:
            if order.broker_order_id and str(order.status).upper() in {"FILLED", "PARTIALLY_FILLED"}:
                fill_order_ids.add(order.broker_order_id)
            if order.client_order_id and str(order.status).upper() in {"FILLED", "PARTIALLY_FILLED"}:
                fill_client_order_ids.add(order.client_order_id)
            if str(order.status).upper() in _OPEN_ORDER_STATUSES:
                open_by_symbol.setdefault(str(order.symbol or "").upper(), []).append(order)
        positions_by_symbol = {
            str(position.symbol or "").upper(): position
            for position in live_positions
        }
        direct_status_records: list[BrokerOrderRecord] = []
        for row in as_list(manual_state.get("orders")):
            current = as_dict(row)
            broker_order_id = str(current.get("broker_order_id") or "").strip()
            if not broker_order_id or broker_order_id in known_order_ids:
                continue
            if str(current.get("account_hash") or "").strip() != selected_account_hash:
                continue
            needs_resolution_check = _manual_live_order_needs_resolution_status_check(
                current,
                now=now,
                recheck_seconds=_MANUAL_LIVE_RESOLUTION_DIRECT_STATUS_RECHECK_SECONDS,
            )
            if bool(current.get("cancel_requested_at")) and not needs_resolution_check:
                continue
            within_post_ack_grace = _manual_live_order_in_post_ack_grace(
                current,
                now=now,
                grace_seconds=int(self._config.manual_order_post_ack_grace_seconds),
            )
            if not within_post_ack_grace and not needs_resolution_check:
                continue
            checked_at = now.isoformat()
            try:
                payload = client.get_order_status(selected_account_hash, broker_order_id)
            except BrokerHttpError as exc:
                error_text = str(exc)
                direct_status = "NOT_FOUND" if "HTTP error 404" in error_text else "UNAVAILABLE"
                self._store.record_order_event(
                    BrokerOrderEvent(
                        account_hash=selected_account_hash,
                        broker_order_id=broker_order_id,
                        client_order_id=str(current.get("client_order_id") or "").strip() or None,
                        event_type="direct_status_check",
                        status=direct_status,
                        occurred_at=now,
                        message=(
                            f"Direct broker order-status check could not find this order: {exc}"
                            if direct_status == "NOT_FOUND"
                            else f"Direct broker order-status check did not confirm this order: {exc}"
                        ),
                        request_payload={"broker_order_id": broker_order_id},
                        response_payload={"error": error_text},
                        source="schwab_direct_status",
                    )
                )
                if (
                    direct_status == "NOT_FOUND"
                    and _manual_live_order_can_resolve_terminal_non_fill(
                        current,
                        now=now,
                        ack_timeout_seconds=int(self._config.manual_order_ack_timeout_seconds),
                        open_by_symbol=open_by_symbol,
                        positions_by_symbol=positions_by_symbol,
                        fill_order_ids=fill_order_ids,
                        fill_client_order_ids=fill_client_order_ids,
                    )
                ):
                    self._store.resolve_order_terminal_state(
                        broker_order_id=broker_order_id,
                        account_hash=selected_account_hash,
                        status=_SYNC_CLOSED_ORDER_STATUS,
                        occurred_at=now,
                        source="schwab_resolution",
                        message="Direct broker order-status lookup no longer found this acknowledged order, and broker open-order, fill, and position truth remained flat/clear.",
                        response_payload={
                            "resolution": "terminal_non_fill_confirmed_after_direct_status_not_found",
                            "broker_order_id": broker_order_id,
                            "symbol": str(current.get("symbol") or "").strip().upper() or None,
                            "client_order_id": str(current.get("client_order_id") or "").strip() or None,
                            "direct_status_error": error_text,
                        },
                    )
                continue

            status = str(payload.get("status") or "UNKNOWN").strip().upper()
            self._store.record_order_event(
                BrokerOrderEvent(
                    account_hash=selected_account_hash,
                    broker_order_id=broker_order_id,
                    client_order_id=str(payload.get("clientOrderId") or current.get("client_order_id") or "").strip() or None,
                    event_type="direct_status_check",
                    status=status,
                    occurred_at=now,
                    message=f"Direct broker order-status check returned {status}.",
                    request_payload={"broker_order_id": broker_order_id},
                    response_payload=payload,
                    source="schwab_direct_status",
                )
            )
            normalized = _normalize_orders(
                [payload],
                account_hash=selected_account_hash,
                fetched_at=now,
                broker_provider_id=self._config.broker_provider_id,
            )
            if not normalized:
                continue
            normalized_order = normalized[0]
            direct_status_records.append(
                BrokerOrderRecord(
                    broker_order_id=normalized_order.broker_order_id,
                    account_hash=normalized_order.account_hash,
                    client_order_id=normalized_order.client_order_id,
                    symbol=normalized_order.symbol,
                    description=normalized_order.description,
                    asset_class=normalized_order.asset_class,
                    instruction=normalized_order.instruction,
                    quantity=normalized_order.quantity,
                    filled_quantity=normalized_order.filled_quantity,
                    order_type=normalized_order.order_type,
                    duration=normalized_order.duration,
                    session=normalized_order.session,
                    status=normalized_order.status,
                    entered_at=normalized_order.entered_at,
                    closed_at=normalized_order.closed_at,
                    updated_at=normalized_order.updated_at,
                    limit_price=normalized_order.limit_price,
                    stop_price=normalized_order.stop_price,
                    source="schwab_direct_status",
                    raw_payload=normalized_order.raw_payload,
                )
            )
        return direct_status_records

    def _reconcile(
        self,
        *,
        selected_account_hash: str,
        persisted_snapshot: dict[str, Any],
        live_orders: list[BrokerOrderRecord],
        live_positions: list[BrokerPositionSnapshot],
        manual_state: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        persisted_orders = as_dict(persisted_snapshot.get("orders"))
        persisted_open_rows = [
            as_dict(row)
            for row in as_list(persisted_orders.get("open_rows"))
            if str(as_dict(row).get("account_hash") or selected_account_hash) == selected_account_hash
        ]
        persisted_open_by_id = {
            str(row.get("broker_order_id")): row
            for row in persisted_open_rows
            if str(row.get("broker_order_id") or "").strip()
        }
        grace_suppressed_order_ids = {
            str(as_dict(row).get("broker_order_id") or "").strip()
            for row in as_list(manual_state.get("orders"))
            if _manual_live_order_in_post_ack_grace(
                as_dict(row),
                now=now,
                grace_seconds=int(self._config.manual_order_post_ack_grace_seconds),
            )
        }
        live_open_orders = [order for order in live_orders if str(order.status).upper() not in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}]
        live_open_by_id = {order.broker_order_id: order for order in live_open_orders}
        live_any_by_id = {
            order.broker_order_id: order
            for order in live_orders
            if str(order.broker_order_id or "").strip()
        }

        missing_local_orders = [
            _order_record_json(order)
            for broker_order_id, order in live_open_by_id.items()
            if broker_order_id not in persisted_open_by_id
        ]
        suppressed_post_ack_missing_broker_orders: list[dict[str, Any]] = []
        missing_broker_orders: list[dict[str, Any]] = []
        for broker_order_id, row in persisted_open_by_id.items():
            if broker_order_id in live_open_by_id or broker_order_id in live_any_by_id:
                continue
            if broker_order_id in grace_suppressed_order_ids:
                suppressed_post_ack_missing_broker_orders.append(row)
                continue
            missing_broker_orders.append(row)
        quantity_mismatches: list[dict[str, Any]] = []
        status_mismatches: list[dict[str, Any]] = []
        for broker_order_id, live_order in live_open_by_id.items():
            persisted_row = persisted_open_by_id.get(broker_order_id)
            if not persisted_row:
                continue
            persisted_quantity = _decimal(persisted_row.get("quantity"))
            if persisted_quantity is not None and persisted_quantity != live_order.quantity:
                quantity_mismatches.append(
                    {
                        "broker_order_id": broker_order_id,
                        "symbol": live_order.symbol,
                        "persisted_quantity": str(persisted_quantity),
                        "live_quantity": str(live_order.quantity),
                    }
                )
            persisted_status = str(persisted_row.get("status") or "").upper()
            persisted_source = str(persisted_row.get("source") or "").strip().lower()
            live_status = str(live_order.status).upper()
            if persisted_status and persisted_status != live_status and persisted_source != "manual_ticket_local":
                status_mismatches.append(
                    {
                        "broker_order_id": broker_order_id,
                        "symbol": live_order.symbol,
                        "persisted_status": persisted_status,
                        "live_status": live_status,
                    }
                )

        persisted_portfolio = as_dict(persisted_snapshot.get("portfolio"))
        persisted_positions = [
            as_dict(row)
            for row in as_list(persisted_portfolio.get("positions"))
            if str(as_dict(row).get("account_hash") or selected_account_hash) == selected_account_hash
        ]
        had_prior_account_state = bool(
            persisted_open_rows
            or persisted_positions
            or any(
                str(as_dict(row).get("account_hash") or "") == selected_account_hash
                for row in as_list(as_dict(persisted_snapshot.get("accounts")).get("rows"))
            )
        )
        if not had_prior_account_state:
            payload = {
                "baseline_established": True,
                "missing_local_orders": [],
                "missing_broker_orders": [],
                "quantity_mismatches": [],
                "status_mismatches": [],
                "position_mismatches": [],
                "live_open_order_ids": sorted(live_open_by_id),
            }
            record = BrokerReconciliationRecord(
                account_hash=selected_account_hash,
                classification="order_state",
                status="clear",
                detail="Broker reconciliation baseline established from the first live Schwab refresh for this selected account.",
                mismatch_count=0,
                created_at=datetime.now(timezone.utc),
                payload=payload,
            )
            self._store.record_reconciliation(record)
            return {
                "status": "clear",
                "label": "CLEAR",
                "detail": record.detail,
                "mismatch_count": 0,
                "rows": [],
                "blocked": False,
                "action_required": False,
                "created_at": record.created_at.isoformat(),
                "categories": {
                    "missing_local_orders": 0,
                    "missing_broker_orders": 0,
                    "quantity_mismatches": 0,
                    "status_mismatches": 0,
                    "position_mismatches": 0,
                },
                "payload": payload,
            }
        persisted_position_index = _position_index_from_rows(persisted_positions)
        live_position_index = _position_index_from_records(live_positions)
        position_mismatches = _position_mismatches(persisted_position_index, live_position_index)

        mismatch_count = sum(
            len(rows)
            for rows in (
                missing_local_orders,
                missing_broker_orders,
                quantity_mismatches,
                status_mismatches,
                position_mismatches,
            )
        )
        status = "clear" if mismatch_count == 0 else "blocked"
        detail = (
            "Broker reconciliation is clear."
            if mismatch_count == 0
            else "Persisted broker state disagrees with the latest live Schwab broker snapshot."
        )
        payload = {
            "missing_local_orders": missing_local_orders,
            "missing_broker_orders": missing_broker_orders,
            "suppressed_post_ack_missing_broker_orders": suppressed_post_ack_missing_broker_orders,
            "quantity_mismatches": quantity_mismatches,
            "status_mismatches": status_mismatches,
            "position_mismatches": position_mismatches,
            "live_open_order_ids": sorted(live_open_by_id),
        }
        record = BrokerReconciliationRecord(
            account_hash=selected_account_hash,
            classification="order_state",
            status=status,
            detail=detail,
            mismatch_count=mismatch_count,
            created_at=datetime.now(timezone.utc),
            payload=payload,
        )
        self._store.record_reconciliation(record)
        return {
            "status": status,
            "label": "CLEAR" if status == "clear" else "BLOCKED",
            "detail": detail,
            "mismatch_count": mismatch_count,
            "rows": missing_broker_orders + quantity_mismatches + status_mismatches + position_mismatches,
            "blocked": mismatch_count > 0,
            "action_required": mismatch_count > 0,
            "created_at": record.created_at.isoformat(),
            "categories": {
                "missing_local_orders": len(missing_local_orders),
                "missing_broker_orders": len(missing_broker_orders),
                "quantity_mismatches": len(quantity_mismatches),
                "status_mismatches": len(status_mismatches),
                "position_mismatches": len(position_mismatches),
            },
            "payload": payload,
        }

    def _load_manual_live_orders_state(self) -> dict[str, Any]:
        state = as_dict(self._store.load_runtime_state("manual_live_orders"))
        orders = [as_dict(row) for row in as_list(state.get("orders")) if as_dict(row)]
        return {
            "orders": orders,
            "updated_at": state.get("updated_at"),
            "last_escalation": as_dict(state.get("last_escalation")),
            "last_safe_cleanup": as_dict(state.get("last_safe_cleanup")),
        }

    def _save_manual_live_orders_state(self, state: dict[str, Any]) -> None:
        payload = {
            "orders": [as_dict(row) for row in as_list(state.get("orders"))],
            "updated_at": state.get("updated_at") or datetime.now(timezone.utc).isoformat(),
            "last_escalation": as_dict(state.get("last_escalation")),
            "last_safe_cleanup": as_dict(state.get("last_safe_cleanup")),
        }
        self._store.save_runtime_state("manual_live_orders", payload)

    def _upsert_manual_live_order_state(self, row: dict[str, Any]) -> dict[str, Any]:
        state = self._load_manual_live_orders_state()
        orders = [as_dict(item) for item in as_list(state.get("orders"))]
        broker_order_id = str(row.get("broker_order_id") or "").strip()
        client_order_id = str(row.get("client_order_id") or "").strip()
        next_orders: list[dict[str, Any]] = []
        replaced = False
        for existing in orders:
            existing_broker_order_id = str(existing.get("broker_order_id") or "").strip()
            existing_client_order_id = str(existing.get("client_order_id") or "").strip()
            if (
                broker_order_id
                and existing_broker_order_id == broker_order_id
                or (client_order_id and existing_client_order_id == client_order_id)
            ):
                next_orders.append({**existing, **row})
                replaced = True
            else:
                next_orders.append(existing)
        if not replaced:
            next_orders.append(row)
        state["orders"] = next_orders
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._save_manual_live_orders_state(state)
        return row

    def _assert_manual_cancel_safety(self, *, account_hash: str, broker_order_id: str) -> None:
        snapshot = self.snapshot(force_refresh=True)
        health = as_dict(snapshot.get("health"))
        selected_account_hash = _selected_account_id_from_snapshot(snapshot)
        blockers: list[str] = []
        if as_dict(health.get("auth_healthy")).get("ok") is not True:
            blockers.append("Auth is not healthy.")
        if as_dict(health.get("broker_reachable")).get("ok") is not True:
            blockers.append("Broker is not reachable.")
        if as_dict(health.get("account_selected")).get("ok") is not True:
            blockers.append("No live-selected broker account is available.")
        if as_dict(health.get("orders_fresh")).get("ok") is not True:
            blockers.append("Orders refresh is stale beyond the configured safety limit.")
        if selected_account_hash and account_hash != selected_account_hash:
            blockers.append("Request account does not match the current live-selected broker account.")
        open_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("orders")).get("open_rows"))]
        tracked_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("manual_live_orders")).get("active_rows"))]
        known_order = any(str(row.get("broker_order_id") or "").strip() == broker_order_id for row in open_rows + tracked_rows)
        if not known_order:
            blockers.append(f"Broker order {broker_order_id} is not currently visible in the live/snapshot open-order state.")
        if blockers:
            raise ProductionLinkActionError("Manual broker cancel is blocked: " + " | ".join(blockers))

    def _record_manual_validation_event(
        self,
        *,
        scenario_type: str,
        occurred_at: datetime,
        payload: dict[str, Any],
    ) -> None:
        self._store.record_manual_validation_event(
            scenario_type=scenario_type,
            occurred_at=occurred_at,
            payload=payload,
        )

    def _record_manual_lifecycle_transition(
        self,
        *,
        scenario_type: str,
        occurred_at: datetime,
        row: dict[str, Any],
        snapshot: dict[str, Any],
        previous_row: dict[str, Any] | None = None,
    ) -> None:
        reconciliation = as_dict(snapshot.get("reconciliation"))
        payload = {
            "scenario_type": scenario_type,
            "broker_truth_snapshot": as_dict(snapshot.get("broker_state_snapshot")),
            "internal_manual_order_state": row,
            "previous_manual_order_state": previous_row,
            "lifecycle_transition_observed": {
                "from": previous_row.get("lifecycle_state") if previous_row else None,
                "to": row.get("lifecycle_state"),
            },
            "timeout_state": {
                "classification": row.get("lifecycle_classification"),
                "issue_code": row.get("issue_code"),
                "issue_detail": row.get("issue_detail"),
            },
            "reconciliation_state": {
                "status": reconciliation.get("status"),
                "detail": reconciliation.get("detail"),
            },
            "manual_review_required": row.get("manual_action_required"),
            "duplicate_action_prevention_held": True,
        }
        self._record_manual_validation_event(
            scenario_type=scenario_type,
            occurred_at=occurred_at,
            payload=payload,
        )

    def _augment_manual_live_order_surface(self, *, snapshot: dict[str, Any], now: datetime) -> dict[str, Any]:
        manual_state = self._load_manual_live_orders_state()
        previous_rows = {
            (
                str(as_dict(row).get("broker_order_id") or "").strip(),
                str(as_dict(row).get("client_order_id") or "").strip(),
            ): as_dict(row)
            for row in as_list(manual_state.get("orders"))
            if as_dict(row)
        }
        lifecycle = self._derive_manual_live_order_lifecycle(snapshot=snapshot, manual_state=manual_state, now=now)
        self._save_manual_live_orders_state(
            {
                "orders": lifecycle.get("rows"),
                "updated_at": now.isoformat(),
                "last_escalation": lifecycle.get("last_escalation"),
                "last_safe_cleanup": lifecycle.get("last_safe_cleanup"),
            }
        )
        for row in as_list(lifecycle.get("rows")):
            current = as_dict(row)
            key = (
                str(current.get("broker_order_id") or "").strip(),
                str(current.get("client_order_id") or "").strip(),
            )
            previous = previous_rows.get(key)
            if previous and previous.get("lifecycle_state") != current.get("lifecycle_state"):
                scenario_type = {
                    "OPEN_WAITING_FILL": "manual_live_acknowledged",
                    "FILLED": "manual_live_filled",
                    "CANCEL_REQUESTED": "manual_live_cancel_requested",
                    "CANCELED": "manual_live_cancel_confirmed",
                    "CANCELED_INFERRED": "manual_live_cancel_inferred",
                    "SAFE_CLEANUP_RESOLVED": "manual_live_safe_cleanup",
                    "RECONCILING": "manual_live_reconciling",
                    "FAULT": "manual_live_faulted",
                }.get(str(current.get("lifecycle_state") or ""), "manual_live_lifecycle_transition")
                self._record_manual_lifecycle_transition(
                    scenario_type=scenario_type,
                    occurred_at=now,
                    row=current,
                    snapshot=snapshot,
                    previous_row=previous,
                )
        if self._manual_restore_validation_pending:
            if as_list(lifecycle.get("rows")):
                self._record_manual_validation_event(
                    scenario_type="manual_live_restore_validation",
                    occurred_at=now,
                    payload={
                        "restore_started_at": now.isoformat(),
                        "restore_completed_at": now.isoformat(),
                        "restore_result": "RESTORED",
                        "restored_state_summary": as_dict(lifecycle.get("summary")),
                        "restored_rows": as_list(lifecycle.get("rows"))[:10],
                        "duplicate_action_prevention_held": True,
                    },
                )
            self._manual_restore_validation_pending = False
        snapshot["manual_validation"] = as_dict(self._store.build_snapshot().get("manual_validation"))
        previous_cycle = as_dict(self._store.load_runtime_state("last_completed_pilot_cycle"))
        current_cycle = self._derive_last_completed_pilot_cycle(snapshot=snapshot, lifecycle=lifecycle, now=now)
        runtime_state = as_dict(snapshot.get("runtime_state"))
        if current_cycle:
            self._store.save_runtime_state("last_completed_pilot_cycle", current_cycle)
            runtime_state["last_completed_pilot_cycle"] = current_cycle
            if str(current_cycle.get("close_order_id") or "") != str(previous_cycle.get("close_order_id") or ""):
                self._record_manual_validation_event(
                    scenario_type="manual_live_pilot_cycle_completed",
                    occurred_at=now,
                    payload=current_cycle,
                )
        else:
            current_cycle = previous_cycle
            if current_cycle:
                runtime_state["last_completed_pilot_cycle"] = current_cycle
        snapshot["runtime_state"] = runtime_state
        snapshot["manual_live_orders"] = lifecycle
        snapshot["pilot_cycle"] = {
            "last_completed": current_cycle or None,
        }
        snapshot["alerts"] = {
            "active": lifecycle.get("active_alerts", []),
            "recent": lifecycle.get("recent_alerts", []),
        }
        diagnostics = as_dict(snapshot.get("diagnostics"))
        diagnostics["manual_live_orders_summary"] = as_dict(lifecycle.get("summary"))
        diagnostics["manual_live_orders_last_checked_at"] = lifecycle.get("checked_at")
        diagnostics["manual_live_validation_latest"] = as_dict(as_dict(snapshot.get("manual_validation")).get("latest_event"))
        diagnostics["last_completed_pilot_cycle"] = current_cycle or None
        snapshot["diagnostics"] = diagnostics
        return snapshot

    def _derive_last_completed_pilot_cycle(
        self,
        *,
        snapshot: dict[str, Any],
        lifecycle: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any] | None:
        recent_rows = [as_dict(row) for row in as_list(lifecycle.get("recent_rows"))]
        reconciliation = as_dict(snapshot.get("reconciliation"))
        positions_by_symbol = as_dict(as_dict(snapshot.get("broker_state_snapshot")).get("positions_by_symbol"))
        open_rows = [as_dict(row) for row in as_list(as_dict(snapshot.get("orders")).get("open_rows"))]
        manual_validation_events = [as_dict(row) for row in as_list(as_dict(snapshot.get("manual_validation")).get("recent_events"))]
        close_row = next(
            (
                row
                for row in recent_rows
                if str(row.get("asset_class") or "").upper() == "STOCK"
                and str(row.get("intent_type") or "").upper() == "FLATTEN"
                and str(row.get("side") or "").upper() == "SELL"
                and str(row.get("order_type") or "").upper() == "LIMIT"
                and str(row.get("lifecycle_state") or "").upper() == "FILLED"
                and str(row.get("quantity") or "") in {"1", "1.0", "1.00"}
            ),
            None,
        )
        if not close_row:
            return None
        symbol = str(close_row.get("symbol") or "").upper()
        buy_row = next(
            (
                row
                for row in recent_rows
                if str(row.get("asset_class") or "").upper() == "STOCK"
                and str(row.get("intent_type") or "").upper() == "MANUAL_LIVE_PILOT"
                and str(row.get("side") or "").upper() == "BUY"
                and str(row.get("order_type") or "").upper() == "LIMIT"
                and str(row.get("lifecycle_state") or "").upper() == "FILLED"
                and str(row.get("quantity") or "") in {"1", "1.0", "1.00"}
                and str(row.get("symbol") or "").upper() == symbol
                and str(row.get("submitted_at") or "") <= str(close_row.get("submitted_at") or "")
            ),
            None,
        )
        if not buy_row:
            return None
        open_orders_remaining = [
            row for row in open_rows if str(row.get("symbol") or "").upper() == symbol
        ]
        flat_confirmed = not as_dict(positions_by_symbol.get(symbol)) and not open_orders_remaining and str(reconciliation.get("status") or "").lower() == "clear"
        buy_fill = _manual_live_fill_summary(buy_row)
        close_fill = _manual_live_fill_summary(close_row)
        passive_refresh_proof = _latest_manual_live_passive_refresh_proof(manual_validation_events)
        return {
            "cycle_completed_at": close_fill.get("fill_timestamp") or now.isoformat(),
            "symbol": symbol,
            "quantity": close_row.get("quantity") or "1",
            "locked_route": _manual_live_pilot_policy_snapshot(self._config.features),
            "buy_order_id": buy_row.get("broker_order_id"),
            "buy": {
                "broker_order_id": buy_row.get("broker_order_id"),
                "submitted_at": buy_row.get("submitted_at"),
                "acknowledged_at": buy_row.get("acknowledged_at"),
                "filled_at": buy_row.get("filled_at"),
                "fill_timestamp": buy_fill.get("fill_timestamp"),
                "fill_price": buy_fill.get("fill_price"),
                "lifecycle_state": buy_row.get("lifecycle_state"),
            },
            "close_order_id": close_row.get("broker_order_id"),
            "close": {
                "broker_order_id": close_row.get("broker_order_id"),
                "submitted_at": close_row.get("submitted_at"),
                "acknowledged_at": close_row.get("acknowledged_at"),
                "filled_at": close_row.get("filled_at"),
                "fill_timestamp": close_fill.get("fill_timestamp"),
                "fill_price": close_fill.get("fill_price"),
                "lifecycle_state": close_row.get("lifecycle_state"),
            },
            "flat_confirmation": {
                "confirmed": flat_confirmed,
                "confirmed_at": now.isoformat(),
                "position": positions_by_symbol.get(symbol),
                "open_order_count": len(open_orders_remaining),
            },
            "reconciliation_clear_confirmation": {
                "confirmed": str(reconciliation.get("status") or "").lower() == "clear",
                "status": reconciliation.get("status"),
                "detail": reconciliation.get("detail"),
                "mismatch_count": reconciliation.get("mismatch_count"),
            },
            "passive_refresh_restart_confirmation": passive_refresh_proof,
        }

    def _derive_manual_live_order_lifecycle(
        self,
        *,
        snapshot: dict[str, Any],
        manual_state: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        tracked_rows = [as_dict(row) for row in as_list(manual_state.get("orders"))]
        orders = as_dict(snapshot.get("orders"))
        open_rows = [as_dict(row) for row in as_list(orders.get("open_rows"))]
        fill_rows = [as_dict(row) for row in as_list(orders.get("recent_fill_rows"))]
        recent_events = [as_dict(row) for row in as_list(orders.get("recent_events"))]
        health = as_dict(snapshot.get("health"))
        broker_state_snapshot = as_dict(snapshot.get("broker_state_snapshot"))
        positions_by_symbol = {str(symbol): as_dict(row) for symbol, row in as_dict(broker_state_snapshot.get("positions_by_symbol")).items()}
        open_by_order_id = {
            str(row.get("broker_order_id") or ""): row
            for row in open_rows
            if str(row.get("broker_order_id") or "").strip()
            and str(row.get("source") or "").strip().lower() != "manual_ticket_local"
        }
        fill_by_order_id = {str(row.get("broker_order_id") or ""): row for row in fill_rows if str(row.get("broker_order_id") or "").strip()}
        open_by_client_order_id = {
            str(row.get("client_order_id") or ""): row
            for row in open_rows
            if str(row.get("client_order_id") or "").strip()
            and str(row.get("source") or "").strip().lower() != "manual_ticket_local"
        }
        fill_by_client_order_id = {str(row.get("client_order_id") or ""): row for row in fill_rows if str(row.get("client_order_id") or "").strip()}
        event_by_order_id: dict[str, list[dict[str, Any]]] = {}
        for event in recent_events:
            broker_order_id = str(event.get("broker_order_id") or "").strip()
            if broker_order_id:
                event_by_order_id.setdefault(broker_order_id, []).append(event)

        active_rows: list[dict[str, Any]] = []
        recent_rows: list[dict[str, Any]] = []
        active_alerts: list[dict[str, Any]] = []
        recent_alerts: list[dict[str, Any]] = []
        overdue_ack_count = 0
        overdue_fill_count = 0
        safe_cleanup_count = 0
        manual_review_count = 0
        last_escalation: dict[str, Any] | None = None
        last_safe_cleanup: dict[str, Any] | None = None

        for tracked in tracked_rows:
            row = dict(tracked)
            broker_order_id = str(row.get("broker_order_id") or "").strip()
            client_order_id = str(row.get("client_order_id") or "").strip()
            symbol = str(row.get("symbol") or "").strip().upper()
            side = str(row.get("side") or "").strip().upper()
            requested_at = _iso_datetime(row.get("submitted_at") or row.get("created_at"))
            live_open = open_by_order_id.get(broker_order_id) or open_by_client_order_id.get(client_order_id)
            live_fill = fill_by_order_id.get(broker_order_id) or fill_by_client_order_id.get(client_order_id)
            latest_events = event_by_order_id.get(broker_order_id, [])
            broker_position = positions_by_symbol.get(symbol, {})
            open_order_summary = as_dict(as_dict(broker_state_snapshot.get("open_orders_by_symbol")).get(symbol))
            reconcile_status = str(as_dict(snapshot.get("reconciliation")).get("status") or "").lower()
            age_seconds = (now - requested_at).total_seconds() if requested_at else None
            ack_deadline = int(self._config.manual_order_ack_timeout_seconds)
            fill_deadline = int(self._config.manual_order_fill_timeout_seconds)
            grace_seconds = int(self._config.manual_order_reconcile_grace_seconds)
            issue_code: str | None = None
            issue_detail: str | None = None
            recommended_action: str | None = None
            manual_action_required = False
            active = True
            lifecycle_state = str(row.get("lifecycle_state") or "SUBMITTED")
            lifecycle_classification = str(row.get("lifecycle_classification") or "submitted_waiting_normally")
            broker_order_status = str(row.get("broker_order_status") or "").strip().upper() or None
            cancel_resolution = str(row.get("cancel_resolution") or "").strip().upper() or None
            cancel_resolution_detail = str(row.get("cancel_resolution_detail") or "").strip() or None
            terminal_resolution = str(row.get("terminal_resolution") or "").strip().upper() or None
            terminal_resolution_detail = str(row.get("terminal_resolution_detail") or "").strip() or None
            last_open_order_observed_at = row.get("last_open_order_observed_at")
            last_broker_terminal_status_at = row.get("last_broker_terminal_status_at")
            first_open_order_observed_at = row.get("first_open_order_observed_at")
            first_fill_observed_at = row.get("first_fill_observed_at")
            first_position_observed_at = row.get("first_position_observed_at")
            direct_status_last_checked_at = row.get("direct_status_last_checked_at")
            direct_status_last_outcome = str(row.get("direct_status_last_outcome") or "").strip().upper() or None
            direct_status_last_detail = str(row.get("direct_status_last_detail") or "").strip() or None
            direct_status_confirmed_status = str(row.get("direct_status_confirmed_status") or "").strip().upper() or None
            post_ack_grace_started_at = row.get("post_ack_grace_started_at") or row.get("acknowledged_at")
            post_ack_grace_expires_at = row.get("post_ack_grace_expires_at")
            broker_truth_complete = (
                as_dict(health.get("broker_reachable")).get("ok") is True
                and as_dict(health.get("orders_fresh")).get("ok") is True
                and as_dict(health.get("positions_fresh")).get("ok") is True
            )
            explicit_terminal_cancel_event = next(
                (
                    event
                    for event in latest_events
                    if str(event.get("status") or "").strip().upper() in {"CANCELED", "CANCELLED"}
                    and str(event.get("source") or "").strip().lower() in {"schwab_sync", "schwab_direct_status"}
                ),
                None,
            )
            explicit_terminal_resolution_event = _latest_event_of_type(
                latest_events,
                event_type="terminal_resolution",
                source="schwab_resolution",
            )
            retired_after_live_sync_event = _latest_event_of_type(
                latest_events,
                event_type="retired_by_live_sync",
                source="schwab_sync",
            )
            latest_direct_status_event = _latest_event_of_type(
                latest_events,
                event_type="direct_status_check",
                source="schwab_direct_status",
            )
            last_open_broker_event = _latest_event_with_status(
                latest_events,
                statuses={"WORKING", "QUEUED", "PENDING_ACTIVATION", "AWAITING_PARENT_ORDER"},
                source=None,
            )
            cancel_requested = bool(row.get("cancel_requested_at")) or broker_order_status == "CANCEL_REQUESTED"
            within_post_ack_grace = _manual_live_order_in_post_ack_grace(
                row,
                now=now,
                grace_seconds=int(self._config.manual_order_post_ack_grace_seconds),
            )
            if latest_direct_status_event:
                direct_status_last_checked_at = latest_direct_status_event.get("occurred_at")
                direct_status_last_outcome = str(latest_direct_status_event.get("status") or "").strip().upper() or "UNKNOWN"
                direct_status_last_detail = str(latest_direct_status_event.get("message") or "").strip() or direct_status_last_detail
                if direct_status_last_outcome not in {"UNAVAILABLE", "UNKNOWN", "NOT_FOUND"}:
                    direct_status_confirmed_status = direct_status_last_outcome

            if live_fill:
                lifecycle_state = "FILLED"
                lifecycle_classification = (
                    "direct_status_confirmed_filled"
                    if str(live_fill.get("source") or "").strip().lower() == "schwab_direct_status"
                    else "filled"
                )
                row["filled_at"] = live_fill.get("updated_at") or live_fill.get("closed_at") or row.get("filled_at")
                row["broker_order_status"] = str(live_fill.get("status") or "FILLED").upper()
                row["broker_filled_quantity"] = live_fill.get("filled_quantity") or live_fill.get("quantity")
                first_fill_observed_at = first_fill_observed_at or row["filled_at"]
                cancel_resolution = None
                cancel_resolution_detail = None
                terminal_resolution = "EXPLICIT_FILL"
                terminal_resolution_detail = "Broker fill truth confirmed this manual live order as filled."
                direct_status_last_detail = (
                    "Direct broker order-status confirmation reported FILLED."
                    if lifecycle_classification == "direct_status_confirmed_filled"
                    else direct_status_last_detail
                )
                active = False
            elif live_open:
                broker_order_status = str(live_open.get("status") or "WORKING").upper()
                row["broker_order_status"] = broker_order_status
                row["last_broker_status_at"] = live_open.get("updated_at") or live_open.get("entered_at") or now.isoformat()
                row["acknowledged_at"] = row.get("acknowledged_at") or row["last_broker_status_at"]
                row["broker_filled_quantity"] = live_open.get("filled_quantity")
                last_open_order_observed_at = row["last_broker_status_at"]
                first_open_order_observed_at = first_open_order_observed_at or last_open_order_observed_at
                if age_seconds is not None and age_seconds > fill_deadline:
                    lifecycle_state = "FILL_OVERDUE"
                    lifecycle_classification = "fill_missing_broker_order_still_open"
                    issue_code = "LIVE_MANUAL_ORDER_FILL_TIMEOUT"
                    issue_detail = f"Live broker order {broker_order_id or client_order_id or symbol} is still open past the fill timeout."
                    recommended_action = "Review the live broker order and cancel or reconcile if it is no longer expected to rest."
                    manual_action_required = True
                    overdue_fill_count += 1
                elif cancel_requested:
                    lifecycle_state = "CANCEL_REQUESTED"
                    lifecycle_classification = "cancel_requested_broker_still_open"
                    cancel_resolution = "OPEN_STILL_VISIBLE"
                    cancel_resolution_detail = "Cancel was requested, but broker truth still shows the order open."
                    recommended_action = "Wait for broker cancel confirmation or refresh again if the order should already be gone."
                else:
                    if str(live_open.get("source") or "").strip().lower() == "schwab_direct_status":
                        lifecycle_state = "DIRECT_STATUS_CONFIRMED_WORKING"
                        lifecycle_classification = "direct_status_confirmed_working"
                        recommended_action = "Broker direct order-status confirmation reported the order as working; wait for open-order or fill truth to follow."
                        direct_status_last_detail = "Direct broker order-status confirmation reported WORKING."
                    else:
                        lifecycle_state = "OPEN_WAITING_FILL"
                        lifecycle_classification = "submitted_waiting_normally"
            else:
                flat_without_open_orders = (
                    (not broker_position or Decimal(str(broker_position.get("quantity") or "0")) == 0)
                    and not open_order_summary
                )
                if broker_position and Decimal(str(broker_position.get("quantity") or "0")) > 0:
                    first_position_observed_at = first_position_observed_at or now.isoformat()
                if explicit_terminal_cancel_event:
                    lifecycle_state = "CANCELED"
                    lifecycle_classification = (
                        "direct_status_confirmed_canceled"
                        if str(explicit_terminal_cancel_event.get("source") or "").strip().lower() == "schwab_direct_status"
                        else "cancel_confirmed_by_broker_terminal"
                    )
                    broker_order_status = str(explicit_terminal_cancel_event.get("status") or "CANCELED").upper()
                    cancel_resolution = "EXPLICIT_BROKER_TERMINAL"
                    cancel_resolution_detail = (
                        "Direct broker order-status confirmation reported the order as canceled."
                        if lifecycle_classification == "direct_status_confirmed_canceled"
                        else "Broker explicitly reported the order as canceled."
                    )
                    terminal_resolution = "EXPLICIT_BROKER_TERMINAL"
                    terminal_resolution_detail = cancel_resolution_detail
                    last_broker_terminal_status_at = explicit_terminal_cancel_event.get("occurred_at")
                    row["cancelled_at"] = row.get("cancelled_at") or last_broker_terminal_status_at or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif direct_status_last_outcome == "REJECTED":
                    lifecycle_state = "REJECTED"
                    lifecycle_classification = "direct_status_confirmed_rejected"
                    broker_order_status = "REJECTED"
                    terminal_resolution = "EXPLICIT_BROKER_TERMINAL"
                    terminal_resolution_detail = "Direct broker order-status confirmation reported the order as rejected."
                    last_broker_terminal_status_at = direct_status_last_checked_at or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif direct_status_last_outcome == "EXPIRED":
                    lifecycle_state = "EXPIRED"
                    lifecycle_classification = "direct_status_confirmed_expired"
                    broker_order_status = "EXPIRED"
                    terminal_resolution = "EXPLICIT_BROKER_TERMINAL"
                    terminal_resolution_detail = "Direct broker order-status confirmation reported the order as expired."
                    last_broker_terminal_status_at = direct_status_last_checked_at or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif explicit_terminal_resolution_event and str(explicit_terminal_resolution_event.get("status") or "").strip().upper() == _SYNC_CLOSED_ORDER_STATUS:
                    lifecycle_state = "TERMINAL_NON_FILL_RESOLVED"
                    lifecycle_classification = "terminal_non_fill_confirmed_after_direct_status_not_found"
                    broker_order_status = _SYNC_CLOSED_ORDER_STATUS
                    terminal_resolution = "DIRECT_STATUS_NOT_FOUND_AND_FLAT"
                    terminal_resolution_detail = str(explicit_terminal_resolution_event.get("message") or "").strip() or "Broker truth was sufficient to confirm terminal non-fill."
                    last_broker_terminal_status_at = explicit_terminal_resolution_event.get("occurred_at") or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif not cancel_requested and within_post_ack_grace:
                    lifecycle_state = "ACCEPTED_AWAITING_BROKER_CONFIRMATION"
                    lifecycle_classification = "post_ack_grace_window"
                    recommended_action = "Await broker confirmation during the short post-ack grace window."
                    direct_status_last_outcome = direct_status_last_outcome or "PENDING"
                    direct_status_last_detail = direct_status_last_detail or "Awaiting broker order-status confirmation inside the post-ack grace window."
                elif (
                    not cancel_requested
                    and direct_status_last_outcome == "NOT_FOUND"
                    and broker_truth_complete
                    and flat_without_open_orders
                    and not first_fill_observed_at
                    and not first_position_observed_at
                    and age_seconds is not None
                    and age_seconds > ack_deadline
                ):
                    lifecycle_state = "TERMINAL_NON_FILL_RESOLVED"
                    lifecycle_classification = "terminal_non_fill_confirmed_after_direct_status_not_found"
                    broker_order_status = _SYNC_CLOSED_ORDER_STATUS
                    terminal_resolution = "DIRECT_STATUS_NOT_FOUND_AND_FLAT"
                    terminal_resolution_detail = "Direct broker order-status lookup could not find the order, and broker open-order, fill, and position truth remained flat/clear."
                    last_broker_terminal_status_at = direct_status_last_checked_at or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif cancel_requested and broker_truth_complete and retired_after_live_sync_event and flat_without_open_orders:
                    lifecycle_state = "CANCELED_INFERRED"
                    lifecycle_classification = "cancel_inferred_from_open_order_disappearance"
                    broker_order_status = "NOT_OPEN_ON_BROKER"
                    cancel_resolution = "INFERRED_OPEN_ORDER_GONE"
                    cancel_resolution_detail = "Broker no longer showed the order in live open-order truth after cancel request, and no fill or position evidence remained."
                    terminal_resolution = "INFERRED_BROKER_DISAPPEARANCE_AFTER_CANCEL"
                    terminal_resolution_detail = cancel_resolution_detail
                    last_open_order_observed_at = last_open_order_observed_at or retired_after_live_sync_event.get("occurred_at")
                    row["cancelled_at"] = row.get("cancelled_at") or retired_after_live_sync_event.get("occurred_at") or now.isoformat()
                    recommended_action = "No manual action required."
                    active = False
                elif cancel_requested and not broker_truth_complete and age_seconds is not None and age_seconds > grace_seconds:
                    lifecycle_state = "RECONCILING"
                    lifecycle_classification = "cancel_unresolved_broker_truth_incomplete"
                    cancel_resolution = "UNRESOLVED_BROKER_TRUTH_INCOMPLETE"
                    cancel_resolution_detail = "Cancel was requested, but broker order/position truth is not fresh enough to confirm whether the order canceled or filled."
                    issue_code = "LIVE_MANUAL_ORDER_CANCEL_RECONCILING"
                    issue_detail = cancel_resolution_detail
                    recommended_action = "Refresh broker truth and inspect the order before sending another live order on this symbol."
                    manual_action_required = True
                    overdue_fill_count += 1
                    manual_review_count += 1
                    last_escalation = {
                        "status": "RECONCILING",
                        "broker_order_id": broker_order_id or None,
                        "symbol": symbol or None,
                        "occurred_at": now.isoformat(),
                        "detail": issue_detail,
                    }
                elif cancel_requested and age_seconds is not None and age_seconds > grace_seconds:
                    lifecycle_state = "RECONCILING"
                    lifecycle_classification = "cancel_unresolved_after_disappearance"
                    cancel_resolution = "UNRESOLVED_AFTER_DISAPPEARANCE"
                    cancel_resolution_detail = "Cancel was requested and the order is no longer open, but the remaining broker position/order context is not safe enough to infer terminal cancel."
                    issue_code = "LIVE_MANUAL_ORDER_CANCEL_RECONCILING"
                    issue_detail = cancel_resolution_detail
                    recommended_action = "Inspect broker order and position truth before treating this live order as canceled."
                    manual_action_required = True
                    overdue_fill_count += 1
                    manual_review_count += 1
                    last_escalation = {
                        "status": "RECONCILING",
                        "broker_order_id": broker_order_id or None,
                        "symbol": symbol or None,
                        "occurred_at": now.isoformat(),
                        "detail": issue_detail,
                    }
                elif broker_order_status == "CANCEL_REQUESTED":
                    lifecycle_state = "CANCEL_REQUESTED"
                    lifecycle_classification = "cancel_requested"
                    cancel_resolution = "PENDING_BROKER_CONFIRMATION"
                    cancel_resolution_detail = "Cancel was requested and broker terminal outcome is still pending."
                    issue_detail = None
                    recommended_action = "Wait for the broker cancel acknowledgement and next order refresh."
                elif _manual_live_order_is_unsafe_ambiguity(
                    requested_side=side,
                    broker_position=broker_position,
                ):
                    lifecycle_state = "FAULT"
                    lifecycle_classification = "unsafe_opposite_side_ambiguity"
                    issue_code = "LIVE_MANUAL_ORDER_FAULT"
                    issue_detail = "Broker position truth is opposite-side against the tracked live manual order and cannot be repaired safely."
                    recommended_action = "Do not send another live order on this symbol. Inspect broker position truth and resolve the ambiguity first."
                    manual_action_required = True
                    manual_review_count += 1
                    last_escalation = {
                        "status": "FAULT",
                        "broker_order_id": broker_order_id or None,
                        "symbol": symbol or None,
                        "occurred_at": now.isoformat(),
                        "detail": issue_detail,
                    }
                elif broker_order_status and broker_order_status in _TERMINAL_ORDER_STATUSES:
                    lifecycle_state = broker_order_status
                    lifecycle_classification = "terminal"
                    active = False
                elif age_seconds is not None and age_seconds > grace_seconds and flat_without_open_orders and not row.get("acknowledged_at"):
                    lifecycle_state = "SAFE_CLEANUP_RESOLVED"
                    lifecycle_classification = "broker_flat_no_open_order_safe_cleanup"
                    issue_code = "LIVE_MANUAL_ORDER_SAFE_CLEANUP"
                    issue_detail = "Broker is flat with no open order; stale internal manual-order tracking was resolved safely."
                    recommended_action = "No manual action required."
                    active = False
                    safe_cleanup_count += 1
                    last_safe_cleanup = {
                        "broker_order_id": broker_order_id or None,
                        "symbol": symbol or None,
                        "occurred_at": now.isoformat(),
                        "detail": issue_detail,
                    }
                elif age_seconds is not None and age_seconds > max(ack_deadline + grace_seconds, fill_deadline):
                    lifecycle_state = "RECONCILING"
                    lifecycle_classification = "pending_order_uncertainty"
                    issue_code = "LIVE_MANUAL_ORDER_RECONCILING"
                    issue_detail = "Manual live order is no longer represented cleanly in broker open-order or fill truth."
                    recommended_action = (
                        "Refresh broker state and inspect reconciliation detail before sending another live order on this symbol."
                    )
                    manual_action_required = True
                    overdue_fill_count += 1
                    manual_review_count += 1
                    last_escalation = {
                        "status": "RECONCILING",
                        "broker_order_id": broker_order_id or None,
                        "symbol": symbol or None,
                        "occurred_at": now.isoformat(),
                        "detail": issue_detail,
                    }
                elif age_seconds is not None and age_seconds > ack_deadline:
                    if row.get("acknowledged_at"):
                        lifecycle_state = "RECONCILING"
                        lifecycle_classification = "post_ack_broker_truth_unresolved_after_grace"
                        issue_code = "LIVE_MANUAL_ORDER_RECONCILING"
                        issue_detail = "Broker order-status confirmation, open-order truth, and fill/position evidence remained insufficient after the post-ack grace window."
                        recommended_action = "Inspect broker order-status truth before sending another live order on this symbol."
                        manual_action_required = True
                        overdue_ack_count += 1
                        manual_review_count += 1
                        last_escalation = {
                            "status": "RECONCILING",
                            "broker_order_id": broker_order_id or None,
                            "symbol": symbol or None,
                            "occurred_at": now.isoformat(),
                            "detail": issue_detail,
                        }
                    else:
                        lifecycle_state = "ACK_OVERDUE"
                        lifecycle_classification = "ack_overdue"
                        issue_code = "LIVE_MANUAL_ORDER_ACK_TIMEOUT"
                        issue_detail = "Manual live order has not reappeared in broker order truth within the acknowledgement window."
                        recommended_action = "Wait for the next broker refresh if the order was just sent; otherwise inspect the broker order book."
                        manual_action_required = True
                        overdue_ack_count += 1
                else:
                    lifecycle_state = "SUBMITTED"
                    lifecycle_classification = "submitted_waiting_normally"

            if reconcile_status != "clear" and lifecycle_state not in _MANUAL_LIVE_TERMINAL_STATES | {"FAULT", "CANCEL_REQUESTED", "ACCEPTED_AWAITING_BROKER_CONFIRMATION", "DIRECT_STATUS_CONFIRMED_WORKING", "ACK_OVERDUE"}:
                lifecycle_state = "RECONCILING"
                lifecycle_classification = "reconciliation_mismatch"
                issue_code = "LIVE_MANUAL_ORDER_RECONCILING"
                issue_detail = as_dict(snapshot.get("reconciliation")).get("detail") or "Broker reconciliation is not clear."
                recommended_action = "Resolve the broker reconciliation mismatch before sending another live order."
                manual_action_required = True
                manual_review_count += 1
                last_escalation = {
                    "status": "RECONCILING",
                    "broker_order_id": broker_order_id or None,
                    "symbol": symbol or None,
                    "occurred_at": now.isoformat(),
                    "detail": issue_detail,
                }

            row["active"] = active
            row["lifecycle_state"] = lifecycle_state
            row["lifecycle_classification"] = lifecycle_classification
            row["issue_code"] = issue_code
            row["issue_detail"] = issue_detail
            row["recommended_action"] = recommended_action
            row["manual_action_required"] = manual_action_required
            row["age_seconds"] = round(age_seconds, 1) if age_seconds is not None else None
            row["cancel_resolution"] = cancel_resolution
            row["cancel_resolution_detail"] = cancel_resolution_detail
            row["terminal_resolution"] = terminal_resolution
            row["terminal_resolution_detail"] = terminal_resolution_detail
            row["last_open_order_observed_at"] = last_open_order_observed_at
            row["last_broker_terminal_status_at"] = last_broker_terminal_status_at
            row["first_open_order_observed_at"] = first_open_order_observed_at
            row["first_fill_observed_at"] = first_fill_observed_at
            row["first_position_observed_at"] = first_position_observed_at
            row["direct_status_last_checked_at"] = direct_status_last_checked_at
            row["direct_status_last_outcome"] = direct_status_last_outcome
            row["direct_status_last_detail"] = direct_status_last_detail
            row["direct_status_confirmed_status"] = direct_status_confirmed_status
            row["post_ack_grace_started_at"] = post_ack_grace_started_at
            row["post_ack_grace_expires_at"] = post_ack_grace_expires_at
            row["last_checked_at"] = now.isoformat()
            row["latest_events"] = latest_events[:5]

            if active:
                active_rows.append(row)
            recent_rows.append(row)

            if issue_code:
                severity = "RECOVERY" if lifecycle_state == "SAFE_CLEANUP_RESOLVED" else "ACTION"
                alert_row = {
                    "occurred_at": now.isoformat(),
                    "severity": severity,
                    "category": "live_manual_order",
                    "code": issue_code,
                    "title": "Live Manual Order",
                    "message": issue_detail,
                    "recommended_action": recommended_action,
                    "active": lifecycle_state not in {"SAFE_CLEANUP_RESOLVED", "FILLED", "CANCELED", "CANCELLED"},
                    "detail": {
                        "broker_order_id": broker_order_id or None,
                        "client_order_id": client_order_id or None,
                        "symbol": symbol or None,
                        "state": lifecycle_state,
                    },
                }
                recent_alerts.append(alert_row)
                if alert_row["active"]:
                    active_alerts.append(alert_row)
            elif lifecycle_state in {"FILLED", "CANCELED", "CANCELLED", "CANCELED_INFERRED"}:
                recent_alerts.append(
                    {
                        "occurred_at": row.get("filled_at") or row.get("cancelled_at") or now.isoformat(),
                        "severity": "AUDIT_ONLY",
                        "category": "live_manual_order",
                        "code": f"LIVE_MANUAL_ORDER_{lifecycle_state}",
                        "title": "Live Manual Order",
                        "message": (
                            f"{symbol or 'Manual live order'} cancel inferred after disappearing from broker open-order truth."
                            if lifecycle_state == "CANCELED_INFERRED"
                            else f"{symbol or 'Manual live order'} {lifecycle_state.lower().replace('_', ' ')}."
                        ),
                        "recommended_action": "No manual action required.",
                        "active": False,
                        "detail": {
                            "broker_order_id": broker_order_id or None,
                            "client_order_id": client_order_id or None,
                            "symbol": symbol or None,
                            "state": lifecycle_state,
                            "cancel_resolution": cancel_resolution,
                        },
                    }
                )

        active_rows.sort(key=lambda row: str(row.get("submitted_at") or row.get("created_at") or ""), reverse=True)
        recent_rows.sort(key=lambda row: str(row.get("last_checked_at") or row.get("submitted_at") or ""), reverse=True)
        recent_alerts.sort(key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
        active_alerts.sort(key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
        return {
            "checked_at": now.isoformat(),
            "cadence_seconds": int(self._config.cache_ttl_seconds),
            "rows": recent_rows,
            "active_rows": active_rows,
            "recent_rows": recent_rows[:20],
            "summary": {
                "open_manual_order_count": len([row for row in recent_rows if row.get("active") is True]),
                "overdue_ack_count": overdue_ack_count,
                "overdue_fill_count": overdue_fill_count,
                "manual_review_required_count": manual_review_count,
                "safe_cleanup_count": safe_cleanup_count,
                "last_escalation": last_escalation,
                "last_safe_cleanup": last_safe_cleanup,
                "status": (
                    "MANUAL_REVIEW_REQUIRED"
                    if manual_review_count > 0
                    else "SAFE_CLEANUP_APPLIED"
                    if safe_cleanup_count > 0
                    else "HEALTHY"
                ),
            },
            "last_escalation": last_escalation,
            "last_safe_cleanup": last_safe_cleanup,
            "active_alerts": active_alerts[:20],
            "recent_alerts": recent_alerts[:40],
        }

    def _disabled_snapshot(self, detail: str) -> dict[str, Any]:
        persisted = self._store.build_snapshot()
        current_now = datetime.now(timezone.utc)
        freshness = _build_snapshot_freshness_summary(
            now=current_now,
            runtime_state=as_dict(persisted.get("runtime_state")),
            max_age_seconds=int(self._config.features.broker_freshness_max_age_seconds),
            source_label="SNAPSHOT",
        )
        payload = {
            "generated_at": current_now.isoformat(),
            "status": "disabled",
            "label": "DISABLED",
            "detail": detail,
            "enabled": False,
            "source_of_record": "disabled",
            "feature_flags": asdict(self._config.features),
            "auth": {
                "configured": False,
                "ready": False,
                "label": "DISABLED",
                "detail": detail,
            },
            "connection": {
                "broker_name": "Schwab",
                "selected_account_hash": None,
                "selection_source": "disabled",
            },
            "health": {
                "auth_healthy": {"ok": False, "label": "DISABLED", "detail": detail},
                "broker_reachable": {"ok": False, "label": "DISABLED", "detail": detail},
                "account_selected": {"ok": False, "label": "ACCOUNT NOT SELECTED", "detail": "Production link is disabled."},
                "balances_fresh": _health_from_freshness(freshness.get("balances"), fallback_label="BALANCES UNAVAILABLE", fallback_detail=detail),
                "positions_fresh": _health_from_freshness(freshness.get("positions"), fallback_label="POSITIONS UNAVAILABLE", fallback_detail=detail),
                "quotes_fresh": _health_from_freshness(freshness.get("quotes"), fallback_label="QUOTES UNAVAILABLE", fallback_detail=detail),
                "orders_fresh": _health_from_freshness(freshness.get("orders"), fallback_label="ORDERS UNAVAILABLE", fallback_detail=detail),
                "fills_events_fresh": _health_from_freshness(freshness.get("fills"), fallback_label="FILLS / EVENTS UNAVAILABLE", fallback_detail=detail),
                "reconciliation_fresh": {"ok": False, "label": "RECONCILIATION UNAVAILABLE", "detail": detail},
            },
            "freshness": freshness,
            "capabilities": _capabilities_snapshot(self._config.features),
            "diagnostics": {
                "database_path": str(self._config.database_path),
                "snapshot_path": str(self._config.snapshot_path),
                "selected_account_path": str(self._config.selected_account_path),
                "last_error": None,
                "last_quote_error": as_dict(as_dict(persisted.get("runtime_state")).get("last_refresh_summary")).get("quote_error"),
                "config_path": str(self._config.config_path) if self._config.config_path else None,
                "trader_api_base_url": self._config.trader_api_base_url,
                "market_data_config_path": str(self._config.market_data_config_path),
                "cache_age_seconds": None,
                "last_manual_order_preview": as_dict(as_dict(persisted.get("runtime_state")).get("last_manual_order_preview")),
                "manual_order_live_verification": {
                    "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                    "live_verified_order_keys": _capabilities_snapshot(self._config.features).get("live_verified_order_keys"),
                    "sequence": _capabilities_snapshot(self._config.features).get("order_type_live_verification_sequence"),
                    "next_step": _capabilities_snapshot(self._config.features).get("next_live_verification_step"),
                    "runbooks": _capabilities_snapshot(self._config.features).get("near_term_live_verification_runbooks"),
                },
            },
            **persisted,
            "manual_order_safety": self._manual_order_safety_snapshot(snapshot={**persisted, "enabled": False, "status": "disabled", "health": {}}, now=current_now),
        }
        payload["local_operator_auth"] = self._local_operator_auth_surface()
        payload = self._augment_manual_live_order_surface(snapshot=payload, now=current_now)
        return self._attach_operator_status(payload)

    def _degraded_snapshot(self, now: datetime, *, detail: str) -> dict[str, Any]:
        persisted = self._store.build_snapshot()
        runtime_state = as_dict(persisted.get("runtime_state"))
        last_refresh_summary = as_dict(runtime_state.get("last_refresh_summary"))
        last_manual_order = as_dict(runtime_state.get("last_manual_order"))
        last_manual_order_preview = as_dict(runtime_state.get("last_manual_order_preview"))
        selected_account_hash = as_dict(persisted.get("accounts")).get("selected_account_hash")
        freshness = _build_snapshot_freshness_summary(
            now=now,
            runtime_state=runtime_state,
            max_age_seconds=int(self._config.features.broker_freshness_max_age_seconds),
            source_label="SNAPSHOT",
        )
        auth_configured = all(
            bool(value)
            for value in (
                os_env("SCHWAB_APP_KEY"),
                os_env("SCHWAB_APP_SECRET"),
                os_env("SCHWAB_CALLBACK_URL"),
            )
        )
        payload = {
            "generated_at": now.isoformat(),
            "status": "degraded",
            "label": "DEGRADED",
            "detail": detail,
            "enabled": True,
            "source_of_record": "persisted_broker_cache" if persisted.get("accounts") else "schwab_broker",
            "feature_flags": asdict(self._config.features),
            "auth": {
                "configured": auth_configured,
                "ready": False,
                "label": "AUTH DEGRADED",
                "detail": detail,
            },
            "connection": {
                "broker_name": _provider_label(self._config.broker_provider_id),
                "broker_provider_id": self._config.broker_provider_id,
                "market_data_provider_id": self._config.market_data_provider_id,
                "selected_account_id": selected_account_hash,
                "selected_account_hash": selected_account_hash,
                "selection_source": "persisted_cache",
            },
            "health": {
                "auth_healthy": {"ok": False, "label": "AUTH DEGRADED", "detail": detail},
                "broker_reachable": {"ok": False, "label": "BROKER DEGRADED", "detail": detail},
                "account_selected": {
                    "ok": bool(selected_account_hash),
                    "label": "ACCOUNT SELECTED" if selected_account_hash else "ACCOUNT NOT SELECTED",
                    "detail": f"Persisted selected account hash: {selected_account_hash}" if selected_account_hash else "No persisted selected account is available.",
                },
                "balances_fresh": _health_from_freshness(freshness.get("balances"), fallback_label="BALANCES STALE", fallback_detail=detail),
                "positions_fresh": _health_from_freshness(freshness.get("positions"), fallback_label="POSITIONS STALE", fallback_detail=detail),
                "quotes_fresh": _health_from_freshness(freshness.get("quotes"), fallback_label="QUOTES STALE", fallback_detail=detail),
                "orders_fresh": _health_from_freshness(freshness.get("orders"), fallback_label="ORDERS STALE", fallback_detail=detail),
                "fills_events_fresh": _health_from_freshness(freshness.get("fills"), fallback_label="FILLS / EVENTS STALE", fallback_detail=detail),
                "reconciliation_fresh": {
                    "ok": as_dict(persisted.get("reconciliation")).get("status") == "clear",
                    "label": as_dict(persisted.get("reconciliation")).get("label") or "UNKNOWN",
                    "detail": as_dict(persisted.get("reconciliation")).get("detail") or "No reconciliation result is available.",
                },
            },
            "freshness": freshness,
            "capabilities": _capabilities_snapshot(self._config.features),
            "diagnostics": {
                "database_path": str(self._config.database_path),
                "snapshot_path": str(self._config.snapshot_path),
                "selected_account_path": str(self._config.selected_account_path),
                "last_error": detail,
                "last_live_fetch_at": self._last_live_fetch_at,
                "account_enumeration_at": last_refresh_summary.get("account_enumeration_at"),
                "last_balances_refresh_at": last_refresh_summary.get("balances_refresh_at"),
                "last_positions_refresh_at": last_refresh_summary.get("positions_refresh_at"),
                "last_quotes_refresh_at": last_refresh_summary.get("quotes_refresh_at"),
                "last_orders_refresh_at": last_refresh_summary.get("orders_refresh_at"),
                "last_fills_refresh_at": last_refresh_summary.get("orders_refresh_at"),
                "last_reconciliation_at": as_dict(persisted.get("reconciliation")).get("created_at"),
                "last_quote_error": last_refresh_summary.get("quote_error"),
                "quote_runtime": as_dict(last_refresh_summary.get("quote_runtime")),
                "last_manual_order_request": last_manual_order.get("request"),
                "last_manual_order_result": last_manual_order.get("result"),
                "last_manual_order_preview": last_manual_order_preview,
                "order_lifecycle_readiness": {
                    "last_request": last_manual_order.get("request"),
                    "last_result": last_manual_order.get("result"),
                    "reconciliation_state": as_dict(persisted.get("reconciliation")).get("label") or as_dict(persisted.get("reconciliation")).get("status"),
                },
                "manual_order_live_verification": {
                    "pilot_mode_enabled": self._config.features.manual_live_pilot_enabled,
                    "live_verified_order_keys": _capabilities_snapshot(self._config.features).get("live_verified_order_keys"),
                    "sequence": _capabilities_snapshot(self._config.features).get("order_type_live_verification_sequence"),
                    "next_step": _capabilities_snapshot(self._config.features).get("next_live_verification_step"),
                    "runbooks": _capabilities_snapshot(self._config.features).get("near_term_live_verification_runbooks"),
                },
                "config_path": str(self._config.config_path) if self._config.config_path else None,
                "trader_api_base_url": self._config.trader_api_base_url,
                "market_data_config_path": str(self._config.market_data_config_path),
                "cache_age_seconds": None,
                "attached_mode": "persisted_cache" if persisted.get("accounts") else "none",
                "live_verified_endpoint_paths": [],
                "implemented_endpoint_paths": [
                    "/accounts/accountNumbers",
                    "/accounts",
                    f"/accounts/{selected_account_hash}" if selected_account_hash else "/accounts/{selectedAccountHash}",
                    f"/accounts/{selected_account_hash}/orders" if selected_account_hash else "/accounts/{selectedAccountHash}/orders",
                ],
                "endpoint_uncertainty": [
                    "Replace order remains disabled until Schwab replace semantics are explicitly live-verified.",
                    "Manual order payloads are still scoped to configured asset classes only; broader asset-class coverage is intentionally deferred.",
                    "EXTO / GTC_EXTO and OCO payloads are review-only in this phase and will not be sent live.",
                ],
            },
            **persisted,
            "manual_order_safety": self._manual_order_safety_snapshot(snapshot={**persisted, "enabled": True, "status": "degraded", "health": {}}, now=now),
        }
        payload["local_operator_auth"] = self._local_operator_auth_surface()
        payload = self._augment_manual_live_order_surface(snapshot=payload, now=now)
        return self._attach_operator_status(payload)

    def _require_manual_ticket_enabled(self) -> None:
        if not self._config.features.manual_order_ticket_enabled:
            raise ProductionLinkActionError("Manual live broker actions are disabled by production-link feature flag.")

    def _assert_manual_order_safety(
        self,
        request: ManualOrderRequest,
        *,
        futures_symbol_resolution: dict[str, Any] | None = None,
    ) -> None:
        snapshot = self.snapshot(force_refresh=True)
        blockers = self._manual_order_live_submit_blockers(snapshot=snapshot, request=request, now=datetime.now(timezone.utc))
        if futures_symbol_resolution:
            blockers.extend(as_list(futures_symbol_resolution.get("blockers")))
            blockers = list(dict.fromkeys(str(item) for item in blockers if str(item).strip()))
        if blockers:
            raise ProductionLinkActionError("Manual live order submit is blocked: " + " | ".join(blockers))

    def _resolve_futures_live_submit_symbol(self, request: ManualOrderRequest) -> dict[str, Any]:
        configured_symbol = _resolved_broker_symbol_for_request(request, features=self._config.features)
        if not _is_futures_pilot_request(request):
            return {
                "policy_mode": "NOT_APPLICABLE",
                "requested_symbol": request.symbol,
                "configured_external_symbol": configured_symbol,
                "broker_submit_symbol": configured_symbol,
                "allowed": True,
                "blockers": [],
            }
        result: dict[str, Any] = {
            "policy_mode": "LIVE_CONTRACT_RESOLUTION",
            "requested_symbol": request.symbol,
            "configured_external_symbol": configured_symbol,
            "broker_submit_symbol": configured_symbol,
            "allowed": False,
            "blockers": [],
        }
        try:
            oauth_client, _ = self._build_oauth_client()
            quote_payload, quote_runtime = self._quote_payload_fetcher(
                self._config.market_data_config_path,
                oauth_client,
                [configured_symbol],
            )
        except (FileNotFoundError, SchwabAuthError, BrokerHttpError, ValueError) as exc:
            result["quote_resolution_error"] = str(exc)
            result["blockers"] = [
                f"Futures live submit requires a resolved live contract symbol for {request.symbol}: {exc}"
            ]
            return result
        resolved_quote_payload = _resolve_quote_payload(quote_payload, configured_symbol)
        result["quote_runtime"] = quote_runtime
        if not isinstance(resolved_quote_payload, dict):
            result["blockers"] = [
                f"Futures live submit requires a live quote payload for {request.symbol} via {configured_symbol}."
            ]
            return result
        broker_submit_symbol = _live_futures_contract_symbol_from_quote_payload(
            resolved_quote_payload,
            configured_external_symbol=configured_symbol,
        )
        result["resolved_quote_symbol"] = str(resolved_quote_payload.get("symbol") or "").strip() or None
        if not broker_submit_symbol:
            result["blockers"] = [
                (
                    f"Futures live submit requires a concrete contract symbol for {request.symbol}; "
                    f"quote truth did not expose one for {configured_symbol}."
                )
            ]
            return result
        transport_limit_price, transport_limit_price_source = _marketable_futures_limit_price_from_quote_payload(
            resolved_quote_payload,
            side=request.side,
        )
        if transport_limit_price is None:
            result["blockers"] = [
                (
                    f"Futures live submit requires a live quote price to derive a marketable broker limit for "
                    f"{request.symbol} via {configured_symbol}."
                )
            ]
            return result
        # Keep the broker submit symbol on the configured futures root while recording
        # the concrete live contract symbol for audit/debug truth. The existing futures
        # execution helper submits the configured root symbol, not the contract symbol.
        result["quote_contract_symbol"] = broker_submit_symbol
        result["broker_submit_symbol"] = configured_symbol
        result["broker_transport_order_type"] = "LIMIT"
        result["broker_transport_limit_price"] = transport_limit_price
        result["broker_transport_limit_price_source"] = transport_limit_price_source
        result["allowed"] = True
        result["blockers"] = []
        result["allowed_reason"] = (
            f"Allowed because live quote truth resolved {request.symbol} through {configured_symbol} "
            f"to the current contract {broker_submit_symbol}, while the broker submit path remains "
            f"anchored to the configured futures root {configured_symbol} and transports the operator MARKET "
            f"ticket as a marketable LIMIT at {transport_limit_price} from {transport_limit_price_source}."
        )
        return result

    def _assert_manual_live_action_enabled(self) -> None:
        snapshot = self.snapshot(force_refresh=True)
        blockers = [str(item) for item in as_list(as_dict(snapshot.get("manual_order_safety")).get("blockers")) if str(item).strip()]
        if blockers:
            raise ProductionLinkActionError("Manual live broker action is blocked: " + " | ".join(blockers))

    def _assert_manual_preview_support(self, request: ManualOrderRequest) -> None:
        blockers: list[str] = []
        advanced_mode = _advanced_mode_label(request)
        order_types = (
            [request.order_type]
            if request.structure_type == "SINGLE"
            else [leg.order_type for leg in request.oco_legs]
        )
        if _is_futures_pilot_request(request) and not self._config.features.futures_pilot_enabled:
            blockers.append("Futures pilot preview is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false.")
        supported_dry_run_types = set(_supported_dry_run_order_types_for_asset(self._config.features, request.asset_class))
        for order_type in order_types:
            if order_type not in supported_dry_run_types:
                blockers.append(f"Order type {order_type} is not enabled for dry-run review on asset class {request.asset_class}.")
        if request.structure_type == "OCO" and not self._config.features.oco_ticket_support_enabled:
            blockers.append("OCO review support is disabled by feature flag.")
        if advanced_mode in {"EXT", "EXTO", "GTC_EXTO"}:
            if not self._config.features.advanced_tif_enabled:
                blockers.append("Advanced TIF review support is disabled by feature flag.")
            if not self._config.features.ext_exto_ticket_support_enabled:
                blockers.append("EXT / EXTO ticket support is disabled by feature flag.")
            blockers.extend(_advanced_review_matrix_blockers(request))
        if blockers:
            raise ProductionLinkActionError("Manual dry-run preview is blocked: " + " | ".join(blockers))

    def _local_operator_auth_surface(self) -> dict[str, Any]:
        return local_operator_auth_surface(self._repo_root)

    def _attach_operator_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["operator_status"] = _pilot_status_export_payload(payload)
        payload["futures_pilot_policy"] = _futures_pilot_policy_snapshot(payload)
        payload["futures_pilot_status"] = _futures_pilot_status_export_payload(payload)
        return payload

    def _manual_order_safety_snapshot(self, *, snapshot: dict[str, Any], now: datetime) -> dict[str, Any]:
        health = as_dict(snapshot.get("health"))
        diagnostics = as_dict(snapshot.get("diagnostics"))
        capabilities = as_dict(snapshot.get("capabilities")) or _capabilities_snapshot(self._config.features)
        selected_account_hash = _selected_account_id_from_snapshot(snapshot)
        selected_accounts = [as_dict(row) for row in as_list(as_dict(snapshot.get("accounts")).get("rows")) if bool(as_dict(row).get("selected"))]
        selected_account = selected_accounts[0] if selected_accounts else {}
        max_age = int(self._config.features.broker_freshness_max_age_seconds)
        balances_age = _age_seconds(diagnostics.get("last_balances_refresh_at"), now=now)
        positions_age = _age_seconds(diagnostics.get("last_positions_refresh_at"), now=now)
        orders_age = _age_seconds(diagnostics.get("last_orders_refresh_at"), now=now)
        fills_age = _age_seconds(diagnostics.get("last_fills_refresh_at"), now=now)
        blockers: list[str] = []
        warnings: list[str] = []

        if not self._config.enabled:
            blockers.append("Production link is disabled.")
        if not self._config.features.manual_order_ticket_enabled:
            blockers.append("Manual order ticket is disabled because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED is false.")
        if not self._config.features.live_order_submit_enabled:
            blockers.append("Live order submit safety mode is disabled because MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED is false.")
        if not self._config.features.manual_live_pilot_enabled:
            warnings.append("Manual live pilot mode is off because MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED is false; preview remains available but live submit stays blocked.")
        if str(snapshot.get("status") or "").lower() != "ready":
            blockers.append(f"Broker snapshot is not ready ({snapshot.get('label') or snapshot.get('status') or 'unknown'}).")
        if as_dict(health.get("auth_healthy")).get("ok") is not True:
            blockers.append("Auth is not healthy.")
        if as_dict(health.get("broker_reachable")).get("ok") is not True:
            blockers.append("Broker is not reachable.")
        if as_dict(health.get("account_selected")).get("ok") is not True or not selected_account_hash:
            blockers.append("No live-selected broker account is available.")
        if selected_account_hash and str(selected_account.get("source") or "") != _live_account_source(self._config.broker_provider_id):
            blockers.append(f"Selected account is not currently live-verified from {_provider_label(self._config.broker_provider_id)}.")
        if balances_age is None or balances_age > max_age:
            blockers.append("Balances refresh is stale beyond the configured safety limit.")
        if positions_age is None or positions_age > max_age:
            blockers.append("Positions refresh is stale beyond the configured safety limit.")
        if orders_age is None or orders_age > max_age:
            blockers.append("Orders refresh is stale beyond the configured safety limit.")
        if as_dict(snapshot.get("reconciliation")).get("status") != "clear":
            blockers.append("Reconciliation is not clear.")
        active_futures_lane = _current_active_manual_lane_is_futures(self._config.features)
        if active_futures_lane:
            if "FUTURE" not in set(self._config.features.futures_supported_asset_classes):
                blockers.append("Futures manual live submit requires FUTURE in MGC_PRODUCTION_FUTURES_SUPPORTED_ASSET_CLASSES.")
            if _futures_pilot_order_type() not in set(self._config.features.futures_supported_order_types):
                blockers.append(
                    f"Futures manual live submit requires {_futures_pilot_order_type()} in MGC_PRODUCTION_FUTURES_SUPPORTED_ORDER_TYPES."
                )
            if not self._config.features.futures_symbol_whitelist:
                blockers.append("Futures manual live submit is blocked because MGC_PRODUCTION_FUTURES_SYMBOL_WHITELIST is empty.")
            if "DAY" not in set(self._config.features.futures_supported_time_in_force_values):
                blockers.append("Futures manual live submit requires DAY in MGC_PRODUCTION_FUTURES_SUPPORTED_TIF_VALUES.")
            if "NORMAL" not in set(self._config.features.futures_supported_session_values):
                blockers.append("Futures manual live submit requires NORMAL in MGC_PRODUCTION_FUTURES_SUPPORTED_SESSION_VALUES.")
            if self._config.features.futures_max_quantity != Decimal("1"):
                blockers.append("Futures manual live submit requires MGC_PRODUCTION_FUTURES_MAX_QUANTITY=1.")
            if not self._config.features.futures_live_submit_enabled:
                blockers.append("Futures live submission is disabled because MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED is false.")
            if not _futures_pilot_live_verified(self._config.features):
                blockers.append("Futures pilot live submit remains preview-only until FUTURE:MARKET is explicitly live-verified.")
        else:
            if not self._config.features.supported_manual_order_types:
                blockers.append("No manual order types are configured for live submit.")
            if not self._config.features.manual_symbol_whitelist:
                blockers.append("Manual live submit is blocked because MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST is empty.")
        if self._config.features.ext_exto_ticket_support_enabled and not self._config.features.ext_exto_live_submit_enabled:
            warnings.append("EXTO / GTC_EXTO ticket support is available in dry-run mode only; live submit remains disabled.")
        if self._config.features.oco_ticket_support_enabled and not self._config.features.oco_live_submit_enabled:
            warnings.append("OCO ticket support is available in dry-run mode only; live submit remains disabled.")
        if fills_age is None:
            warnings.append("Recent fills/events have not been refreshed yet.")

        active_futures_lane = _current_active_manual_lane_is_futures(self._config.features)
        submit_status = _manual_submit_status_summary(blockers=blockers)
        locked_policy = _current_active_manual_live_policy_snapshot(self._config.features)
        historical_stock_policy = _manual_live_pilot_policy_snapshot(self._config.features)
        active_route_key = _futures_pilot_verification_key() if active_futures_lane else "STOCK:LIMIT"
        active_runbook = _NEAR_TERM_LIVE_VERIFICATION_RUNBOOKS[active_route_key]
        submit_enabled = len(blockers) == 0
        active_supported_asset_classes = (
            list(self._config.features.futures_supported_asset_classes)
            if active_futures_lane
            else list(self._config.features.supported_manual_asset_classes)
        )
        active_supported_order_types = (
            list(self._config.features.futures_supported_order_types)
            if active_futures_lane
            else list(self._config.features.supported_manual_order_types)
        )
        active_supported_time_in_force_values = (
            list(self._config.features.futures_supported_time_in_force_values)
            if active_futures_lane
            else list(self._config.features.supported_manual_time_in_force_values)
        )
        active_supported_session_values = (
            list(self._config.features.futures_supported_session_values)
            if active_futures_lane
            else list(self._config.features.supported_manual_session_values)
        )
        active_symbol_whitelist = (
            list(self._config.features.futures_symbol_whitelist)
            if active_futures_lane
            else list(self._config.features.manual_symbol_whitelist)
        )
        active_max_quantity = (
            str(self._config.features.futures_max_quantity)
            if active_futures_lane
            else str(self._config.features.manual_max_quantity)
        )

        return {
            "submit_enabled": submit_enabled,
            "submit_status_label": submit_status["label"],
            "submit_status_detail": submit_status["detail"],
            "dry_run_enabled": bool(self._config.features.manual_order_ticket_enabled),
            "blockers": blockers,
            "warnings": warnings,
            "pilot_mode": {
                "enabled": bool(self._config.features.futures_pilot_enabled if active_futures_lane else self._config.features.manual_live_pilot_enabled),
                "label": (
                    "MANUAL FUTURES PILOT ACTIVE"
                    if active_futures_lane and self._config.features.futures_pilot_enabled
                    else "HISTORICAL STOCK PILOT ACTIVE"
                    if self._config.features.manual_live_pilot_enabled
                    else "MANUAL LIVE PILOT OFF"
                ),
                "detail": (
                    "Real Schwab manual futures pilot is enabled for the narrow whitelist-controlled live validation scope."
                    if active_futures_lane and self._config.features.futures_pilot_enabled
                    else "Historical stock pilot support remains present as historical proof, but it is not the current active live-test lane."
                    if self._config.features.manual_live_pilot_enabled
                    else "Manual live pilot mode is off. Dry-run preview is still available, but live submit remains blocked."
                ),
                "scope": locked_policy,
                "current_active_lane": "FUTURES" if active_futures_lane else "STOCK_HISTORICAL",
            },
            "pilot_readiness": {
                "enabled": bool(self._config.features.futures_pilot_enabled if active_futures_lane else self._config.features.manual_live_pilot_enabled),
                "submit_eligible": submit_enabled,
                "label": (
                    "LIVE MANUAL FUTURES PILOT READY"
                    if active_futures_lane and submit_enabled
                    else "LIVE MANUAL FUTURES PILOT BLOCKED"
                    if active_futures_lane
                    else "HISTORICAL STOCK PILOT READY"
                    if submit_enabled
                    else "HISTORICAL STOCK PILOT BLOCKED"
                ),
                "detail": submit_status["detail"],
                "blocked_reason": blockers[0] if blockers else None,
                "locked_policy": locked_policy,
                "historical_stock_policy": historical_stock_policy,
                "auth_healthy": as_dict(health.get("auth_healthy")).get("ok") is True,
                "broker_reachable": as_dict(health.get("broker_reachable")).get("ok") is True,
                "account_selected": as_dict(health.get("account_selected")).get("ok") is True,
                "orders_fresh": orders_age is not None and orders_age <= max_age,
                "balances_fresh": balances_age is not None and balances_age <= max_age,
                "positions_fresh": positions_age is not None and positions_age <= max_age,
                "reconciliation_status": as_dict(snapshot.get("reconciliation")).get("status"),
                "reconciliation_detail": as_dict(snapshot.get("reconciliation")).get("detail"),
                "reconciliation_mismatch_count": as_dict(snapshot.get("reconciliation")).get("mismatch_count"),
            },
            "selected_account_id": selected_account_hash or None,
            "selected_account_hash": selected_account_hash or None,
            "selected_account_live_verified": bool(
                selected_account_hash
                and str(selected_account.get("source") or "") == _live_account_source(self._config.broker_provider_id)
            ),
            "constraints": {
                "current_active_lane": "FUTURES" if active_futures_lane else "STOCK_HISTORICAL",
                "supported_asset_classes": active_supported_asset_classes,
                "supported_order_types": active_supported_order_types,
                "supported_dry_run_order_types": list(self._config.features.supported_manual_dry_run_order_types),
                "supported_time_in_force_values": active_supported_time_in_force_values,
                "supported_session_values": active_supported_session_values,
                "symbol_whitelist": active_symbol_whitelist,
                "max_quantity": active_max_quantity,
                "require_reconciliation_clear": True,
                "broker_freshness_max_age_seconds": max_age,
                "manual_order_ack_timeout_seconds": int(self._config.manual_order_ack_timeout_seconds),
                "manual_order_fill_timeout_seconds": int(self._config.manual_order_fill_timeout_seconds),
                "manual_order_reconcile_grace_seconds": int(self._config.manual_order_reconcile_grace_seconds),
                "manual_order_post_ack_grace_seconds": int(self._config.manual_order_post_ack_grace_seconds),
                "stock_regular_hours_only": True,
                "allowed_time_in_force": ["DAY"],
                "allowed_sessions": ["NORMAL"],
                "advanced_tif_ticket_support_enabled": self._config.features.advanced_tif_enabled and self._config.features.ext_exto_ticket_support_enabled,
                "stock_market_live_submit_enabled": self._config.features.stock_market_live_submit_enabled,
                "stock_limit_live_submit_enabled": self._config.features.stock_limit_live_submit_enabled,
                "stock_stop_live_submit_enabled": self._config.features.stock_stop_live_submit_enabled,
                "stock_stop_limit_live_submit_enabled": self._config.features.stock_stop_limit_live_submit_enabled,
                "ext_exto_live_submit_enabled": self._config.features.ext_exto_live_submit_enabled,
                "oco_ticket_support_enabled": self._config.features.oco_ticket_support_enabled,
                "oco_live_submit_enabled": self._config.features.oco_live_submit_enabled,
                "trailing_live_submit_enabled": self._config.features.trailing_live_submit_enabled,
                "close_order_live_submit_enabled": self._config.features.close_order_live_submit_enabled,
                "futures_live_submit_enabled": self._config.features.futures_live_submit_enabled,
                "futures_pilot_enabled": self._config.features.futures_pilot_enabled,
                "futures_symbol_whitelist": list(self._config.features.futures_symbol_whitelist),
                "futures_supported_asset_classes": list(self._config.features.futures_supported_asset_classes),
                "futures_supported_order_types": list(self._config.features.futures_supported_order_types),
                "futures_supported_time_in_force_values": list(self._config.features.futures_supported_time_in_force_values),
                "futures_supported_session_values": list(self._config.features.futures_supported_session_values),
                "futures_max_quantity": str(self._config.features.futures_max_quantity),
                "futures_market_data_symbol_map": dict(self._config.features.futures_market_data_symbol_map),
                "supported_order_structures": ["SINGLE", "OCO"] if self._config.features.oco_ticket_support_enabled else ["SINGLE"],
                "live_verified_order_keys": capabilities.get("live_verified_order_keys"),
                "order_type_matrix_by_asset_class": capabilities.get("order_type_matrix_by_asset_class"),
                "live_enabled_order_types_by_asset_class": capabilities.get("live_enabled_order_types_by_asset_class"),
                "dry_run_only_order_types_by_asset_class": capabilities.get("dry_run_only_order_types_by_asset_class"),
                "order_type_live_verification_matrix": capabilities.get("order_type_live_verification_matrix"),
                "order_type_live_verification_sequence": capabilities.get("order_type_live_verification_sequence"),
                "next_live_verification_step": {
                    "verification_key": active_route_key,
                    "label": active_runbook.get("minimal_safe_test_shape"),
                    "blocked": not submit_enabled,
                    "blocker_reason": blockers[0] if blockers else None,
                    "route_key": _futures_pilot_route_key() if active_futures_lane else "MANUAL_LIVE_PILOT_STOCK_LIMIT_BUY",
                },
                "near_term_live_verification_runbooks": capabilities.get("near_term_live_verification_runbooks"),
                "current_active_live_runbook": active_runbook,
                "first_live_stock_limit_test": _NEAR_TERM_LIVE_VERIFICATION_RUNBOOKS["STOCK:LIMIT"],
                "historical_stock_pilot_policy": historical_stock_policy,
            },
            "ages_seconds": {
                "balances": balances_age,
                "positions": positions_age,
                "orders": orders_age,
                "fills_events": fills_age,
            },
        }

    def _manual_order_gate_blockers(
        self,
        *,
        snapshot: dict[str, Any],
        request: ManualOrderRequest,
        now: datetime,
    ) -> list[str]:
        safety = as_dict(snapshot.get("manual_order_safety"))
        blockers = [str(item) for item in as_list(safety.get("blockers")) if str(item).strip()]
        selected_account_hash = str(safety.get("selected_account_id") or safety.get("selected_account_hash") or "")
        is_futures_pilot_request = _is_futures_pilot_request(request)

        if request.account_id != selected_account_hash:
            blockers.append("Request account does not match the current live-selected broker account.")
        if not _manual_order_has_live_auth(request):
            if _manual_order_is_reduce_only(request):
                blockers.append("Reduce-only live broker orders require explicit reduce-only authorization when no active local operator auth session is present.")
            else:
                blockers.append("Manual live broker orders require a current authenticated local operator session.")
        if is_futures_pilot_request:
            if not self._config.features.futures_pilot_enabled:
                blockers.append("Futures pilot live submit is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false.")
            if request.asset_class not in set(self._config.features.futures_supported_asset_classes):
                blockers.append(
                    "Asset class "
                    f"{request.asset_class} is not enabled for the futures pilot. Allowed: {', '.join(self._config.features.futures_supported_asset_classes)}."
                )
            if request.order_type not in set(self._config.features.futures_supported_order_types):
                blockers.append(
                    "Order type "
                    f"{request.order_type} is not enabled for the futures pilot. Allowed: {', '.join(self._config.features.futures_supported_order_types)}."
                )
            if request.quantity > self._config.features.futures_max_quantity:
                blockers.append(
                    f"Quantity {request.quantity} exceeds the configured futures pilot max quantity {self._config.features.futures_max_quantity}."
                )
            futures_whitelist = set(self._config.features.futures_symbol_whitelist)
            if futures_whitelist and request.symbol not in futures_whitelist:
                blockers.append(f"Symbol {request.symbol} is not in the configured futures pilot whitelist.")
            if not futures_whitelist:
                blockers.append("Futures pilot live submit requires a non-empty futures symbol whitelist.")
            if request.time_in_force not in set(self._config.features.futures_supported_time_in_force_values):
                blockers.append(
                    "Only "
                    f"{', '.join(self._config.features.futures_supported_time_in_force_values)} time-in-force is enabled in the futures pilot."
                )
            if request.session not in set(self._config.features.futures_supported_session_values):
                blockers.append(
                    f"Only {', '.join(self._config.features.futures_supported_session_values)} session is enabled in the futures pilot."
                )
        else:
            if request.asset_class not in set(self._config.features.supported_manual_asset_classes):
                blockers.append(f"Asset class {request.asset_class} is not enabled for live manual submit.")
            if request.order_type not in set(self._config.features.supported_manual_order_types):
                blockers.append(
                    f"Order type {request.order_type} is not enabled for the first live-order safety mode. Allowed: {', '.join(self._config.features.supported_manual_order_types)}."
                )
            if request.quantity > self._config.features.manual_max_quantity:
                blockers.append(f"Quantity {request.quantity} exceeds the configured manual max quantity {self._config.features.manual_max_quantity}.")
            whitelist = set(self._config.features.manual_symbol_whitelist)
            if whitelist and request.symbol not in whitelist:
                blockers.append(f"Symbol {request.symbol} is not in the configured manual live-order whitelist.")
            if not whitelist:
                blockers.append("Manual live submit requires a non-empty manual symbol whitelist.")
            if request.time_in_force != "DAY":
                blockers.append("Only DAY time-in-force is enabled in the first live-order safety mode.")
            if request.session != "NORMAL":
                blockers.append("Only NORMAL session is enabled in the first live-order safety mode.")
            if request.asset_class == "STOCK" and not _is_us_regular_hours(now):
                blockers.append("Live stock manual orders are blocked outside regular US market hours in the first live-order safety mode.")
        if not request.review_confirmed:
            blockers.append("Manual live broker orders require explicit review confirmation.")
        if request.asset_class == "STOCK" and request.order_type == "LIMIT":
            if not self._config.features.stock_limit_live_submit_enabled:
                blockers.append("First live STOCK LIMIT submit is blocked because MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED is false.")
            if request.quantity != Decimal("1"):
                blockers.append("First live STOCK LIMIT test requires quantity 1.")
        manual_live_orders = [as_dict(row) for row in as_list(as_dict(snapshot.get("manual_live_orders")).get("active_rows"))]
        conflicting_manual_orders = [
            row
            for row in manual_live_orders
            if str(row.get("account_hash") or "").strip() == request.account_hash
            and str(row.get("symbol") or "").strip().upper() == request.symbol
            and row.get("active") is True
        ]
        if conflicting_manual_orders:
            blockers.append(f"An unresolved live manual order already exists for {request.symbol}.")
        broker_position = as_dict(as_dict(as_dict(snapshot.get("broker_state_snapshot")).get("positions_by_symbol")).get(request.symbol))
        broker_side = str(broker_position.get("side") or "").strip().upper()
        broker_quantity = _decimal(broker_position.get("quantity")) or Decimal("0")
        requested_side = request.side.upper()
        if broker_quantity > 0 and broker_side == "LONG" and requested_side in {"SELL_SHORT", "BUY_TO_COVER"}:
            blockers.append(f"Requested side {requested_side} is incompatible with the current long broker position in {request.symbol}.")
        if broker_quantity > 0 and broker_side == "SHORT" and requested_side == "BUY":
            blockers.append(f"BUY on {request.symbol} would be opposite-side against the current short broker position without an explicit flatten/cover path.")
        if (request.intent_type or "").upper() != "FLATTEN":
            symbol_open_orders = [
                as_dict(row)
                for row in as_list(as_dict(snapshot.get("orders")).get("open_rows"))
                if str(as_dict(row).get("symbol") or "").strip().upper() == request.symbol
            ]
            conflicting_open_orders = [
                row
                for row in symbol_open_orders
                if _manual_order_instruction_conflicts(
                    requested_side=request.side,
                    live_instruction=str(row.get("instruction") or "").strip().upper(),
                    broker_side=broker_side,
                )
            ]
            if conflicting_open_orders:
                blockers.append(
                    f"Broker already shows incompatible open order flow on {request.symbol}; resolve the pending broker order before sending this live manual ticket."
                )
        if is_futures_pilot_request:
            blockers.extend(
                _locked_futures_live_pilot_route_blockers(
                    request=request,
                    broker_position=broker_position,
                    features=self._config.features,
                )
            )
        else:
            blockers.extend(
                _locked_manual_live_pilot_route_blockers(
                    request=request,
                    broker_position=broker_position,
                )
            )
        return blockers

    def _manual_order_live_submit_blockers(
        self,
        *,
        snapshot: dict[str, Any],
        request: ManualOrderRequest,
        now: datetime,
    ) -> list[str]:
        blockers = self._manual_order_gate_blockers(snapshot=snapshot, request=request, now=now)
        advanced_mode = _advanced_mode_label(request)
        order_types = (
            [request.order_type]
            if request.structure_type == "SINGLE"
            else [leg.order_type for leg in request.oco_legs]
        )
        for order_type in order_types:
            blockers.extend(
                _live_order_type_blockers(
                    asset_class=request.asset_class,
                    order_type=order_type,
                    features=self._config.features,
                )
            )
        if request.structure_type == "OCO":
            if not self._config.features.oco_ticket_support_enabled:
                blockers.append("OCO ticket support is disabled by feature flag.")
            blockers.append("OCO live submission remains disabled pending live Schwab verification.")
        if advanced_mode in {"EXT", "EXTO", "GTC_EXTO"}:
            if not self._config.features.advanced_tif_enabled:
                blockers.append("Advanced TIF review support is disabled by feature flag.")
            if not self._config.features.ext_exto_ticket_support_enabled:
                blockers.append("EXT / EXTO ticket support is disabled by feature flag.")
            blockers.extend(_advanced_review_matrix_blockers(request))
            blockers.append("EXTO / GTC_EXTO live submission remains disabled pending live Schwab verification.")
        return list(dict.fromkeys(blockers))

    def _build_oauth_client(self) -> tuple[SchwabOAuthClient, dict[str, Any]]:
        auth_config = load_schwab_auth_config_from_env()
        token_store = SchwabTokenStore(auth_config.token_store_path)
        oauth_client = SchwabOAuthClient(
            config=auth_config,
            transport=UrllibJsonTransport(timeout_seconds=self._config.request_timeout_seconds),
            token_store=token_store,
        )
        token_set = token_store.load()
        access_token = oauth_client.get_access_token()
        refreshed_token = token_store.load()
        return oauth_client, {
            "configured": True,
            "ready": bool(access_token),
            "label": "AUTH READY" if access_token else "AUTH NOT READY",
            "detail": f"Using token file {auth_config.token_store_path}.",
            "token_store_path": str(auth_config.token_store_path),
            "callback_url": auth_config.callback_url,
            "expires_at": refreshed_token.expires_at.isoformat() if refreshed_token and refreshed_token.expires_at else None,
            "issued_at": refreshed_token.issued_at.isoformat() if refreshed_token else (token_set.issued_at.isoformat() if token_set else None),
        }

    def _resolve_selected_account(self, account_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
        persisted = self._load_persisted_selected_account()
        candidates = [
            {"source": "persisted_selection", "account_hash": persisted.get("account_hash"), "account_number": persisted.get("account_number")},
            {"source": "config_default_hash", "account_hash": self._config.default_account_hash, "account_number": None},
            {"source": "config_default_number", "account_hash": None, "account_number": self._config.default_account_number},
        ]
        for candidate in candidates:
            account_hash = candidate.get("account_hash")
            account_number = candidate.get("account_number")
            if account_hash and account_hash in account_index:
                return {"source": candidate["source"], "account_hash": account_hash}
            if account_number:
                matched = next((row for row in account_index.values() if row.get("account_number") == account_number), None)
                if matched:
                    return {"source": candidate["source"], "account_hash": matched["account_hash"]}
        first = next(iter(account_index.values()))
        return {"source": "first_available", "account_hash": first["account_hash"]}

    def _persist_selected_account(
        self,
        account_hash: str,
        *,
        account_number: str | None = None,
        display_name: str | None = None,
        account_type: str | None = None,
        source: str | None = None,
    ) -> None:
        existing = self._store.load_runtime_state("selected_account") or {}
        payload = {
            "account_hash": account_hash,
            "account_number": account_number or existing.get("account_number"),
            "display_name": display_name or existing.get("display_name"),
            "account_type": account_type or existing.get("account_type"),
            "source": source or existing.get("source"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._config.selected_account_path.parent.mkdir(parents=True, exist_ok=True)
        self._config.selected_account_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self._store.save_runtime_state("selected_account", payload)

    def _load_persisted_selected_account(self) -> dict[str, Any]:
        if self._config.selected_account_path.exists():
            payload = json.loads(self._config.selected_account_path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        return self._store.load_runtime_state("selected_account") or {}

    def _write_snapshot(self, payload: dict[str, Any]) -> None:
        self._config.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self._config.snapshot_path.write_text(_json_dumps(payload) + "\n", encoding="utf-8")
        pilot_status_path = self._config.snapshot_path.with_name("pilot_status_v1.json")
        pilot_status_payload = as_dict(payload.get("operator_status")) or _pilot_status_export_payload(payload)
        pilot_status_path.write_text(_json_dumps(pilot_status_payload) + "\n", encoding="utf-8")
        futures_policy_path = self._config.snapshot_path.with_name("futures_pilot_policy_snapshot.json")
        futures_policy_payload = as_dict(payload.get("futures_pilot_policy")) or _futures_pilot_policy_snapshot(payload)
        futures_policy_path.write_text(_json_dumps(futures_policy_payload) + "\n", encoding="utf-8")
        futures_status_path = self._config.snapshot_path.with_name("futures_pilot_status.json")
        futures_status_payload = as_dict(payload.get("futures_pilot_status")) or _futures_pilot_status_export_payload(payload)
        futures_status_path.write_text(_json_dumps(futures_status_payload) + "\n", encoding="utf-8")

    def _default_client_factory(self, config: ProductionLinkConfig, oauth_client: SchwabOAuthClient) -> BrokerHttpClient:
        return BrokerHttpClient(
            oauth_client=oauth_client,
            base_url=config.trader_api_base_url,
            timeout_seconds=config.request_timeout_seconds,
        )

    def _default_quote_payload_fetcher(
        self,
        config_path: Path,
        oauth_client: SchwabOAuthClient,
        external_symbols: list[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not config_path.exists():
            raise FileNotFoundError(f"Broker market-data config not found at {config_path}.")
        schwab_config = load_schwab_market_data_config(config_path)
        quote_client = SchwabQuoteHttpClient(
            oauth_client=oauth_client,
            market_data_config=schwab_config,
            transport=UrllibJsonTransport(timeout_seconds=self._config.request_timeout_seconds),
        )
        payload = quote_client.fetch_quotes(external_symbols)
        return payload, {
            "auth_mode": "env_oauth",
            "source_label": "Direct Schwab /quotes via broker monitor polling.",
            "config_path": str(config_path),
            "symbol_count": len(external_symbols),
        }


def _account_number_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        account_hash = str(row.get("hashValue") or row.get("accountHashValue") or row.get("accountHash") or "").strip()
        if not account_hash:
            continue
        result[account_hash] = {
            "account_hash": account_hash,
            "account_number": str(row.get("accountNumber") or "").strip() or None,
            "raw": row,
        }
    return result


def _normalize_accounts(
    payload: list[dict[str, Any]],
    account_index: dict[str, dict[str, Any]],
    *,
    selected_account_hash: str | None,
    fetched_at: datetime,
    broker_provider_id: str = "schwab",
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw_account in payload:
        securities_account = raw_account.get("securitiesAccount") if isinstance(raw_account.get("securitiesAccount"), dict) else raw_account
        if not isinstance(securities_account, dict):
            continue
        account_hash = str(
            securities_account.get("hashValue")
            or securities_account.get("accountHashValue")
            or raw_account.get("hashValue")
            or ""
        ).strip()
        if not account_hash:
            matched = next(iter(account_index.values()), None)
            account_hash = matched["account_hash"] if matched and len(account_index) == 1 else ""
        if not account_hash:
            continue
        account_number = (
            str(securities_account.get("accountNumber") or account_index.get(account_hash, {}).get("account_number") or "").strip()
            or None
        )
        account_type = str(securities_account.get("type") or securities_account.get("accountType") or "").strip() or None
        display_name = f"{account_type or 'Account'} {account_number or account_hash}"
        identity = BrokerAccountIdentity(
            broker_name=_provider_label(broker_provider_id),
            account_hash=account_hash,
            account_number=account_number,
            display_name=display_name,
            account_type=account_type,
            selected=account_hash == selected_account_hash,
            source=_live_account_source(broker_provider_id),
            updated_at=fetched_at,
            raw_payload=securities_account,
        )
        balances_payload = _first_dict(
            securities_account.get("currentBalances"),
            securities_account.get("balances"),
            securities_account.get("initialBalances"),
            raw_account.get("currentBalances"),
        )
        balances = (
            BrokerBalanceSnapshot(
                account_hash=account_hash,
                currency=str(balances_payload.get("currency") or "USD"),
                liquidation_value=_decimal(balances_payload.get("liquidationValue")),
                buying_power=_decimal(balances_payload.get("buyingPower") or balances_payload.get("cashAvailableForTrading")),
                available_funds=_decimal(balances_payload.get("availableFunds") or balances_payload.get("availableFundsNonMarginableTrade")),
                cash_balance=_decimal(balances_payload.get("cashBalance") or balances_payload.get("cashAvailableForTrading")),
                long_market_value=_decimal(balances_payload.get("longMarketValue")),
                short_market_value=_decimal(balances_payload.get("shortMarketValue")),
                day_trading_buying_power=_decimal(balances_payload.get("dayTradingBuyingPower")),
                maintenance_requirement=_decimal(balances_payload.get("maintenanceRequirement")),
                margin_balance=_decimal(balances_payload.get("marginBalance")),
                fetched_at=fetched_at,
                raw_payload=balances_payload,
            )
            if balances_payload
            else None
        )
        positions_payload = securities_account.get("positions")
        positions = _normalize_positions(
            positions_payload if isinstance(positions_payload, list) else [],
            account_hash=account_hash,
            fetched_at=fetched_at,
        )
        normalized.append(
            {
                "identity": identity,
                "balances": balances,
                "positions": positions,
            }
        )
    return normalized


def _normalize_positions(rows: list[dict[str, Any]], *, account_hash: str, fetched_at: datetime) -> list[BrokerPositionSnapshot]:
    positions: list[BrokerPositionSnapshot] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
        symbol = str(instrument.get("symbol") or row.get("symbol") or "").strip()
        if not symbol:
            continue
        asset_class = _normalize_asset_class(
            instrument.get("assetType")
            or row.get("assetType")
            or instrument.get("type")
            or "UNKNOWN"
        )
        long_qty = _decimal(row.get("longQuantity"))
        short_qty = _decimal(row.get("shortQuantity"))
        net_quantity = long_qty or Decimal("0")
        side = "LONG"
        if short_qty and short_qty > 0:
            net_quantity = short_qty
            side = "SHORT"
        elif net_quantity < 0:
            net_quantity = abs(net_quantity)
            side = "SHORT"
        elif net_quantity == 0 and _decimal(row.get("quantity")):
            quantity_value = _decimal(row.get("quantity")) or Decimal("0")
            net_quantity = abs(quantity_value)
            side = "SHORT" if quantity_value < 0 else "LONG"
        positions.append(
            BrokerPositionSnapshot(
                account_hash=account_hash,
                position_key=f"{account_hash}:{symbol}:{index}",
                symbol=symbol,
                description=str(instrument.get("description") or row.get("description") or "").strip() or None,
                asset_class=asset_class,
                quantity=net_quantity,
                side=side,
                average_cost=_decimal(row.get("averagePrice") or row.get("averageLongPrice") or row.get("averageShortPrice")),
                mark_price=_decimal(instrument.get("mark") or row.get("marketPrice") or row.get("mark")),
                market_value=_decimal(row.get("marketValue") or row.get("marketValueDouble")),
                current_day_pnl=_decimal(row.get("currentDayProfitLoss") or row.get("currentDayProfitLossPercentage")),
                open_pnl=_position_open_pnl(row),
                ytd_pnl=_decimal(row.get("ytdGainLoss") or row.get("yearToDateProfitLoss")),
                margin_impact=_decimal(row.get("maintenanceRequirement") or row.get("marginRequirement")),
                broker_position_id=str(row.get("positionId") or "").strip() or None,
                fetched_at=fetched_at,
                raw_payload=row,
            )
        )
    return positions


def _normalize_orders(
    rows: list[dict[str, Any]],
    *,
    account_hash: str,
    fetched_at: datetime,
    broker_provider_id: str = "schwab",
) -> list[BrokerOrderRecord]:
    normalized: list[BrokerOrderRecord] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        broker_order_id = str(row.get("orderId") or row.get("orderID") or "").strip()
        if not broker_order_id:
            continue
        legs = row.get("orderLegCollection") if isinstance(row.get("orderLegCollection"), list) else []
        first_leg = legs[0] if legs and isinstance(legs[0], dict) else {}
        instrument = first_leg.get("instrument") if isinstance(first_leg.get("instrument"), dict) else {}
        normalized.append(
            BrokerOrderRecord(
                broker_order_id=broker_order_id,
                account_hash=account_hash,
                client_order_id=str(row.get("clientOrderId") or "").strip() or None,
                symbol=str(instrument.get("symbol") or row.get("symbol") or "").strip() or "UNKNOWN",
                description=str(instrument.get("description") or row.get("description") or "").strip() or None,
                asset_class=_normalize_asset_class(instrument.get("assetType") or row.get("assetType") or "UNKNOWN"),
                instruction=str(first_leg.get("instruction") or row.get("instruction") or "UNKNOWN"),
                quantity=_decimal(first_leg.get("quantity") or row.get("quantity") or 0) or Decimal("0"),
                filled_quantity=_decimal(row.get("filledQuantity")),
                order_type=str(row.get("orderType") or "UNKNOWN"),
                duration=str(row.get("duration") or "").strip() or None,
                session=str(row.get("session") or "").strip() or None,
                status=str(row.get("status") or "UNKNOWN"),
                entered_at=_iso_datetime(row.get("enteredTime")),
                closed_at=_iso_datetime(row.get("closeTime") or row.get("cancelTime")),
                updated_at=_iso_datetime(row.get("enteredTime")) or fetched_at,
                limit_price=_decimal(row.get("price")),
                stop_price=_decimal(row.get("stopPrice")),
                source=_live_account_source(broker_provider_id),
                raw_payload=row,
            )
        )
    return normalized


def _normalize_quotes(
    payload: dict[str, Any],
    *,
    account_hash: str,
    symbols: list[str],
    fetched_at: datetime,
    source: str,
) -> list[BrokerQuoteSnapshot]:
    normalized: list[BrokerQuoteSnapshot] = []
    for symbol in symbols:
        resolved = _resolve_quote_payload(payload, symbol)
        if resolved is None:
            continue
        quote = resolved.get("quote") if isinstance(resolved.get("quote"), dict) else {}
        bid_price = _first_decimal_with_source(("bidPrice", quote.get("bidPrice")), ("bid", quote.get("bid")))[0]
        ask_price = _first_decimal_with_source(("askPrice", quote.get("askPrice")), ("ask", quote.get("ask")))[0]
        last_price = _first_decimal_with_source(
            ("lastPrice", quote.get("lastPrice")),
            ("last", quote.get("last")),
            ("mark", quote.get("mark")),
            ("markPrice", quote.get("markPrice")),
        )[0]
        mark_price = _first_decimal_with_source(
            ("mark", quote.get("mark")),
            ("markPrice", quote.get("markPrice")),
            ("lastPrice", quote.get("lastPrice")),
            ("last", quote.get("last")),
        )[0]
        close_price = _first_decimal_with_source(("closePrice", quote.get("closePrice")),)[0]
        net_change = _first_decimal_with_source(("netChange", quote.get("netChange")), ("markChange", quote.get("markChange")))[0]
        net_percent_change = _first_decimal_with_source(
            ("netPercentChange", quote.get("netPercentChange")),
            ("markPercentChange", quote.get("markPercentChange")),
            ("percentChange", quote.get("percentChange")),
            ("futurePercentChange", quote.get("futurePercentChange")),
        )[0]
        normalized.append(
            BrokerQuoteSnapshot(
                account_hash=account_hash,
                symbol=symbol,
                external_symbol=symbol,
                bid_price=bid_price,
                ask_price=ask_price,
                last_price=last_price,
                mark_price=mark_price,
                close_price=close_price,
                net_change=net_change,
                net_percent_change=net_percent_change,
                delayed=_quote_delay_flag(resolved, quote),
                quote_time=_quote_timestamp(resolved, quote),
                fetched_at=fetched_at,
                source=source,
                raw_payload=resolved,
            )
        )
    return normalized


def _provider_label(provider_id: str | None) -> str:
    value = str(provider_id or "").strip().lower()
    if value == "ibkr":
        return "IBKR"
    if value == "schwab":
        return "Schwab"
    return str(provider_id or "Broker").strip() or "Broker"


def _live_account_source(provider_id: str | None) -> str:
    value = str(provider_id or "").strip().lower()
    if not value:
        return "broker_live"
    return f"{value}_live"


def _selected_account_id_from_snapshot(snapshot: dict[str, Any]) -> str:
    connection = as_dict(snapshot.get("connection"))
    accounts = as_dict(snapshot.get("accounts"))
    return str(
        connection.get("selected_account_id")
        or accounts.get("selected_account_id")
        or connection.get("selected_account_hash")
        or accounts.get("selected_account_hash")
        or ""
    ).strip()


def _manual_order_request_from_payload(payload: dict[str, Any], *, features) -> ManualOrderRequest:
    structure_type = str(payload.get("structure_type") or "SINGLE").strip().upper()
    quantity = _decimal(payload.get("quantity"))
    if quantity is None or quantity <= 0:
        raise ProductionLinkActionError("quantity must be a positive number.")
    review_confirmed = bool(payload.get("review_confirmed"))
    if not review_confirmed:
        raise ProductionLinkActionError("Manual broker orders require explicit review confirmation.")
    raw_oco_legs = payload.get("oco_legs")
    oco_legs = _manual_oco_legs_from_payload(raw_oco_legs)
    request = ManualOrderRequest(
        account_hash=str(payload.get("account_id") or payload.get("account_hash") or "").strip(),
        symbol=str(payload.get("symbol") or "").strip().upper(),
        asset_class=_normalize_asset_class(payload.get("asset_class") or "EQUITY"),
        structure_type=structure_type,
        intent_type=str(payload.get("intent_type") or "").strip().upper() or None,
        side=str(payload.get("side") or "").strip().upper(),
        quantity=quantity,
        order_type=str(payload.get("order_type") or "").strip().upper(),
        limit_price=_decimal(payload.get("limit_price")),
        stop_price=_decimal(payload.get("stop_price")),
        trail_value_type=str(payload.get("trail_value_type") or "").strip().upper() or None,
        trail_value=_decimal(payload.get("trail_value")),
        trail_trigger_basis=str(payload.get("trail_trigger_basis") or "").strip().upper() or None,
        trail_limit_offset=_decimal(payload.get("trail_limit_offset")),
        time_in_force=str(payload.get("time_in_force") or "DAY").strip().upper(),
        session=str(payload.get("session") or "NORMAL").strip().upper(),
        review_confirmed=review_confirmed,
        operator_note=str(payload.get("operator_note") or "").strip() or None,
        client_order_id=str(payload.get("client_order_id") or "").strip() or f"manual-{uuid.uuid4().hex[:10]}",
        broker_account_number=str(payload.get("broker_account_number") or "").strip() or None,
        oco_group_id=str(payload.get("oco_group_id") or "").strip() or None,
        oco_legs=oco_legs,
        operator_authenticated=bool(payload.get("operator_authenticated")),
        operator_reduce_only_authorized=bool(payload.get("operator_reduce_only_authorized")),
        operator_auth_policy=str(payload.get("operator_auth_policy") or "").strip() or None,
        operator_auth_risk_bucket=str(payload.get("operator_auth_risk_bucket") or "").strip() or None,
        local_operator_identity=str(payload.get("local_operator_identity") or "").strip() or None,
        auth_session_id=str(payload.get("auth_session_id") or "").strip() or None,
        auth_method=str(payload.get("auth_method") or "").strip() or None,
        authenticated_at=str(payload.get("authenticated_at") or "").strip() or None,
    )
    if structure_type not in {"SINGLE", "OCO"}:
        raise ProductionLinkActionError("structure_type must be SINGLE or OCO.")
    if not request.account_id or not request.symbol:
        raise ProductionLinkActionError("account_id/account_hash and symbol are required.")
    if _is_futures_pilot_request(request):
        if request.time_in_force not in set(features.futures_supported_time_in_force_values):
            raise ProductionLinkActionError(
                "Futures pilot time_in_force "
                f"{request.time_in_force} is not enabled. Supported values: {', '.join(features.futures_supported_time_in_force_values)}."
            )
        if request.session not in set(features.futures_supported_session_values):
            raise ProductionLinkActionError(
                f"Futures pilot session {request.session} is not enabled. Supported values: {', '.join(features.futures_supported_session_values)}."
            )
        if request.asset_class not in set(features.futures_supported_asset_classes):
            raise ProductionLinkActionError(
                "Futures pilot asset class "
                f"{request.asset_class} is not enabled. Supported classes: {', '.join(features.futures_supported_asset_classes)}."
            )
    else:
        if request.time_in_force not in set(features.supported_manual_time_in_force_values):
            raise ProductionLinkActionError(
                f"time_in_force {request.time_in_force} is not enabled. Supported values: {', '.join(features.supported_manual_time_in_force_values)}."
            )
        if request.session not in set(features.supported_manual_session_values):
            raise ProductionLinkActionError(
                f"session {request.session} is not enabled. Supported values: {', '.join(features.supported_manual_session_values)}."
            )
        if request.asset_class not in set(features.supported_manual_asset_classes):
            raise ProductionLinkActionError(
                f"Asset class {request.asset_class} is not enabled for manual live orders. Supported classes: {', '.join(features.supported_manual_asset_classes)}."
            )
    if structure_type == "SINGLE":
        if not request.side or not request.order_type:
            raise ProductionLinkActionError("side and order_type are required for single orders.")
        if request.side == "SELL_SHORT" and not features.sell_short_enabled:
            raise ProductionLinkActionError("SELL_SHORT is disabled until account permissions and Schwab product support are explicitly verified.")
        _validate_order_leg_fields(
            asset_class=request.asset_class,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            limit_price=request.limit_price,
            stop_price=request.stop_price,
            trail_value_type=request.trail_value_type,
            trail_value=request.trail_value,
            trail_trigger_basis=request.trail_trigger_basis,
            trail_limit_offset=request.trail_limit_offset,
        )
    else:
        if request.side not in {"", "OCO"}:
            raise ProductionLinkActionError("Top-level side must be blank or OCO for OCO structures.")
        if request.order_type not in {"", "OCO"}:
            raise ProductionLinkActionError("Top-level order_type must be blank or OCO for OCO structures.")
        if len(oco_legs) != 2:
            raise ProductionLinkActionError("OCO review requires exactly two legs.")
        for leg in oco_legs:
            if leg.side == "SELL_SHORT" and not features.sell_short_enabled:
                raise ProductionLinkActionError("SELL_SHORT is disabled until account permissions and Schwab product support are explicitly verified.")
            _validate_order_leg_fields(
                asset_class=request.asset_class,
                side=leg.side,
                order_type=leg.order_type,
                quantity=leg.quantity,
                limit_price=leg.limit_price,
                stop_price=leg.stop_price,
                trail_value_type=leg.trail_value_type,
                trail_value=leg.trail_value,
                trail_trigger_basis=leg.trail_trigger_basis,
                trail_limit_offset=leg.trail_limit_offset,
            )
        if request.quantity <= 0:
            raise ProductionLinkActionError("OCO quantity must be a positive number.")
    return request


def _manual_flatten_request_from_payload(payload: dict[str, Any]) -> ManualFlattenRequest:
    quantity = _decimal(payload.get("quantity"))
    if quantity is None or quantity <= 0:
        raise ProductionLinkActionError("Flatten requires a positive quantity.")
    request = ManualFlattenRequest(
        account_hash=str(payload.get("account_id") or payload.get("account_hash") or "").strip(),
        symbol=str(payload.get("symbol") or "").strip().upper(),
        asset_class=_normalize_asset_class(payload.get("asset_class") or "EQUITY"),
        quantity=quantity,
        side=str(payload.get("side") or "").strip().upper(),
        time_in_force=str(payload.get("time_in_force") or "DAY").strip().upper(),
        session=str(payload.get("session") or "NORMAL").strip().upper(),
        operator_authenticated=bool(payload.get("operator_authenticated")),
        operator_reduce_only_authorized=bool(payload.get("operator_reduce_only_authorized")),
        operator_auth_policy=str(payload.get("operator_auth_policy") or "").strip() or None,
        operator_auth_risk_bucket=str(payload.get("operator_auth_risk_bucket") or "").strip() or None,
        local_operator_identity=str(payload.get("local_operator_identity") or "").strip() or None,
        auth_session_id=str(payload.get("auth_session_id") or "").strip() or None,
        auth_method=str(payload.get("auth_method") or "").strip() or None,
        authenticated_at=str(payload.get("authenticated_at") or "").strip() or None,
    )
    if not request.account_id or not request.symbol or not request.side:
        raise ProductionLinkActionError("Flatten requires account_id/account_hash, symbol, and side.")
    return request


def _build_schwab_order_payload(
    request: ManualOrderRequest,
    *,
    features: Any | None = None,
    broker_symbol_override: str | None = None,
    broker_order_type_override: str | None = None,
    broker_limit_price_override: Decimal | None = None,
) -> dict[str, Any]:
    asset_type = _schwab_asset_type(request.asset_class)
    futures_position_effect = _futures_position_effect_for_request(request) if request.asset_class == "FUTURE" else None
    order_symbol = str(broker_symbol_override or "").strip() or _resolved_broker_symbol_for_request(request, features=features)
    client_order_id = None if _omit_client_order_id_for_live_pilot(request, features=features) else request.client_order_id
    transport_order_type = str(broker_order_type_override or request.order_type).strip().upper()
    transport_limit_price = broker_limit_price_override if broker_limit_price_override is not None else request.limit_price
    if request.structure_type == "OCO":
        payload: dict[str, Any] = {
            "session": _schwab_session_value(request.session),
            "duration": _schwab_duration_value(request.time_in_force),
            "orderStrategyType": "OCO",
            "childOrderStrategies": [
                _single_leg_payload(
                    symbol=order_symbol,
                    asset_type=asset_type,
                    side=leg.side,
                    quantity=leg.quantity,
                    order_type=transport_order_type if request.asset_class == "FUTURE" else leg.order_type,
                    limit_price=transport_limit_price if request.asset_class == "FUTURE" else leg.limit_price,
                    stop_price=leg.stop_price,
                    trail_value_type=leg.trail_value_type,
                    trail_value=leg.trail_value,
                    trail_trigger_basis=leg.trail_trigger_basis,
                    trail_limit_offset=leg.trail_limit_offset,
                    client_order_id=None,
                    session=request.session,
                    time_in_force=request.time_in_force,
                    complex_order_strategy_type="NONE" if request.asset_class == "FUTURE" else None,
                    top_level_quantity=leg.quantity if request.asset_class == "FUTURE" else None,
                    leg_id=1 if request.asset_class == "FUTURE" else None,
                    order_leg_type=asset_type if request.asset_class == "FUTURE" else None,
                    position_effect=futures_position_effect if request.asset_class == "FUTURE" else None,
                )
                for leg in request.oco_legs
            ],
        }
        if request.oco_group_id:
            payload["ocoGroupId"] = request.oco_group_id
        if client_order_id:
            payload["clientOrderId"] = client_order_id
        return payload
    return _single_leg_payload(
        symbol=order_symbol,
        asset_type=asset_type,
        side=request.side,
        quantity=request.quantity,
        order_type=transport_order_type,
        limit_price=transport_limit_price,
        stop_price=request.stop_price,
        trail_value_type=request.trail_value_type,
        trail_value=request.trail_value,
        trail_trigger_basis=request.trail_trigger_basis,
        trail_limit_offset=request.trail_limit_offset,
        client_order_id=client_order_id,
        session=request.session,
        time_in_force=request.time_in_force,
        complex_order_strategy_type="NONE" if request.asset_class == "FUTURE" else None,
        top_level_quantity=request.quantity if request.asset_class == "FUTURE" else None,
        leg_id=1 if request.asset_class == "FUTURE" else None,
        order_leg_type=asset_type if request.asset_class == "FUTURE" else None,
        position_effect=futures_position_effect if request.asset_class == "FUTURE" else None,
    )


def _resolved_broker_symbol_for_request(request: ManualOrderRequest, *, features: Any | None = None) -> str:
    if _is_futures_pilot_request(request):
        return _futures_pilot_external_symbol(request.symbol, features=features)
    return request.symbol


def _futures_pilot_external_symbol(symbol: str, *, features: Any | None = None) -> str:
    normalized = str(symbol or "").strip().upper()
    if features is not None:
        configured = str(getattr(features, "futures_market_data_symbol_map", {}).get(normalized, "") or "").strip()
        if configured:
            return configured
    return normalized


def _live_futures_contract_symbol_from_quote_payload(
    payload: dict[str, Any],
    *,
    configured_external_symbol: str,
) -> str | None:
    candidates = [
        payload.get("symbol"),
        as_dict(payload.get("reference")).get("symbol"),
        as_dict(payload.get("quote")).get("symbol"),
    ]
    for candidate in candidates:
        normalized = str(candidate or "").strip().upper()
        if normalized and _looks_like_specific_futures_contract_symbol(normalized):
            return normalized
    fallback = str(configured_external_symbol or "").strip().upper()
    if _looks_like_specific_futures_contract_symbol(fallback):
        return fallback
    return None


def _looks_like_specific_futures_contract_symbol(symbol: str) -> bool:
    normalized = str(symbol or "").strip().upper().lstrip("/")
    if len(normalized) < 3:
        return False
    index = len(normalized) - 1
    digit_count = 0
    while index >= 0 and normalized[index].isdigit():
        digit_count += 1
        index -= 1
    if digit_count < 1 or index < 1:
        return False
    return normalized[index] in set("FGHJKMNQUVXZ")


def _omit_client_order_id_for_live_pilot(request: ManualOrderRequest, *, features: Any | None = None) -> bool:
    intent_type = (request.intent_type or "").upper()
    if _is_scoped_manual_futures_pilot_lane(request, features=features):
        return True
    return (
        request.structure_type == "SINGLE"
        and request.asset_class == "STOCK"
        and request.order_type == "LIMIT"
        and request.quantity == Decimal("1")
        and request.time_in_force == "DAY"
        and request.session == "NORMAL"
        and (
            (intent_type == "MANUAL_LIVE_PILOT" and request.side == "BUY")
            or (intent_type == "FLATTEN" and request.side == "SELL")
        )
    )


def _manual_oco_legs_from_payload(raw_value: Any) -> tuple[ManualOcoLegRequest, ...]:
    if raw_value in (None, ""):
        return ()
    if not isinstance(raw_value, list):
        raise ProductionLinkActionError("oco_legs must be an array when provided.")
    legs: list[ManualOcoLegRequest] = []
    for index, raw_leg in enumerate(raw_value):
        if not isinstance(raw_leg, dict):
            raise ProductionLinkActionError("Each OCO leg must be an object.")
        quantity = _decimal(raw_leg.get("quantity"))
        if quantity is None or quantity <= 0:
            raise ProductionLinkActionError("Each OCO leg requires a positive quantity.")
        legs.append(
            ManualOcoLegRequest(
                leg_label=str(raw_leg.get("leg_label") or f"Leg {index + 1}").strip() or f"Leg {index + 1}",
                side=str(raw_leg.get("side") or "").strip().upper(),
                quantity=quantity,
                order_type=str(raw_leg.get("order_type") or "").strip().upper(),
                limit_price=_decimal(raw_leg.get("limit_price")),
                stop_price=_decimal(raw_leg.get("stop_price")),
                trail_value_type=str(raw_leg.get("trail_value_type") or "").strip().upper() or None,
                trail_value=_decimal(raw_leg.get("trail_value")),
                trail_trigger_basis=str(raw_leg.get("trail_trigger_basis") or "").strip().upper() or None,
                trail_limit_offset=_decimal(raw_leg.get("trail_limit_offset")),
            )
        )
    return tuple(legs)


def _validate_order_leg_fields(
    *,
    asset_class: str,
    side: str,
    order_type: str,
    quantity: Decimal,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    trail_value_type: str | None,
    trail_value: Decimal | None,
    trail_trigger_basis: str | None,
    trail_limit_offset: Decimal | None,
) -> None:
    if not side or not order_type:
        raise ProductionLinkActionError("Each order leg requires side and order_type.")
    if quantity <= 0:
        raise ProductionLinkActionError("Each order leg requires a positive quantity.")
    if order_type not in set(_manual_order_type_matrix_for_asset(asset_class)):
        raise ProductionLinkActionError(f"Order type {order_type} is not supported for asset class {asset_class}.")
    if order_type in {"LIMIT", "STOP_LIMIT"} and limit_price is None:
        raise ProductionLinkActionError("limit_price is required for LIMIT and STOP_LIMIT orders.")
    if order_type in {"STOP", "STOP_LIMIT"} and stop_price is None:
        raise ProductionLinkActionError("stop_price is required for STOP and STOP_LIMIT orders.")
    if order_type == "LIMIT_ON_CLOSE" and limit_price is None:
        raise ProductionLinkActionError("limit_price is required for LIMIT_ON_CLOSE orders.")
    if order_type in {"TRAIL_STOP", "TRAIL_STOP_LIMIT"}:
        if trail_value_type not in {"AMOUNT", "PERCENT"}:
            raise ProductionLinkActionError("trail_value_type must be AMOUNT or PERCENT for trailing orders.")
        if trail_value is None or trail_value <= 0:
            raise ProductionLinkActionError("trail_value must be a positive number for trailing orders.")
        if not trail_trigger_basis:
            raise ProductionLinkActionError("trail_trigger_basis is required for trailing orders.")
    if order_type == "TRAIL_STOP_LIMIT" and trail_limit_offset is None:
        raise ProductionLinkActionError("trail_limit_offset is required for TRAIL_STOP_LIMIT orders.")


def _single_leg_payload(
    *,
    symbol: str,
    asset_type: str,
    side: str,
    quantity: Decimal,
    order_type: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    trail_value_type: str | None = None,
    trail_value: Decimal | None = None,
    trail_trigger_basis: str | None = None,
    trail_limit_offset: Decimal | None = None,
    client_order_id: str | None,
    session: str = "NORMAL",
    time_in_force: str = "DAY",
    complex_order_strategy_type: str | None = None,
    top_level_quantity: Decimal | None = None,
    leg_id: int | None = None,
    order_leg_type: str | None = None,
    position_effect: str | None = None,
) -> dict[str, Any]:
    order_payload: dict[str, Any] = {
        "session": _schwab_session_value(session),
        "duration": _schwab_duration_value(time_in_force),
        "orderType": order_type,
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": complex_order_strategy_type,
        "quantity": _schwab_quantity_value(top_level_quantity) if top_level_quantity is not None else None,
        "orderLegCollection": [
            {
                "orderLegType": order_leg_type,
                "legId": leg_id,
                "instruction": side,
                "positionEffect": position_effect,
                "quantity": _schwab_quantity_value(quantity),
                "instrument": {
                    "symbol": symbol,
                    "assetType": asset_type,
                },
            }
        ],
    }
    if limit_price is not None and order_type not in {"MARKET", "STOP", "TRAIL_STOP", "MARKET_ON_CLOSE"}:
        order_payload["price"] = str(limit_price)
    if stop_price is not None:
        order_payload["stopPrice"] = str(stop_price)
    if order_type in {"TRAIL_STOP", "TRAIL_STOP_LIMIT"}:
        order_payload["stopPriceLinkType"] = "VALUE"
        order_payload["stopPriceOffset"] = str(trail_value) if trail_value is not None else None
        order_payload["stopPriceLinkBasis"] = trail_trigger_basis
        order_payload["trailValueType"] = trail_value_type
    if order_type == "TRAIL_STOP_LIMIT":
        order_payload["priceLinkType"] = "OFFSET"
        order_payload["priceOffset"] = str(trail_limit_offset) if trail_limit_offset is not None else None
    if client_order_id:
        order_payload["clientOrderId"] = client_order_id
    order_payload = {key: value for key, value in order_payload.items() if value is not None}
    return order_payload


def _schwab_quantity_value(quantity: Decimal) -> int | float:
    if quantity == quantity.to_integral_value():
        return int(quantity)
    return float(quantity)


def _futures_position_effect_for_request(request: ManualOrderRequest) -> str:
    intent_type = str(request.intent_type or "").strip().upper()
    if intent_type == "FLATTEN":
        return "CLOSING"
    return "OPENING"


def _manual_live_order_in_post_ack_grace(
    row: dict[str, Any],
    *,
    now: datetime,
    grace_seconds: int,
) -> bool:
    acknowledged_at = _iso_datetime(row.get("acknowledged_at"))
    if acknowledged_at is None:
        return False
    explicit_expiry = _iso_datetime(row.get("post_ack_grace_expires_at"))
    expires_at = explicit_expiry or (acknowledged_at + timedelta(seconds=grace_seconds))
    return now <= expires_at


def _manual_live_order_needs_resolution_status_check(
    row: dict[str, Any],
    *,
    now: datetime,
    recheck_seconds: int,
) -> bool:
    if not bool(row.get("active")):
        return False
    if _iso_datetime(row.get("acknowledged_at")) is None:
        return False
    broker_order_id = str(row.get("broker_order_id") or "").strip()
    if not broker_order_id:
        return False
    lifecycle_state = str(row.get("lifecycle_state") or "").strip().upper()
    if lifecycle_state in _MANUAL_LIVE_TERMINAL_STATES:
        return False
    if bool(row.get("cancel_requested_at")) and lifecycle_state != "RECONCILING":
        return False
    eligible_states = {
        "RECONCILING",
        "ACCEPTED_AWAITING_BROKER_CONFIRMATION",
        "DIRECT_STATUS_CONFIRMED_WORKING",
        "ACK_OVERDUE",
        "FILL_OVERDUE",
        "SUBMITTED",
        "OPEN_WAITING_FILL",
        "CANCEL_REQUESTED",
        "WORKING",
    }
    if lifecycle_state and lifecycle_state not in eligible_states:
        return False
    last_checked = _iso_datetime(row.get("direct_status_last_checked_at"))
    if last_checked is None:
        return True
    return (now - last_checked).total_seconds() >= recheck_seconds


def _manual_live_order_can_resolve_terminal_non_fill(
    row: dict[str, Any],
    *,
    now: datetime,
    ack_timeout_seconds: int,
    open_by_symbol: dict[str, list[BrokerOrderRecord]],
    positions_by_symbol: dict[str, BrokerPositionSnapshot],
    fill_order_ids: set[str],
    fill_client_order_ids: set[str],
) -> bool:
    if _iso_datetime(row.get("acknowledged_at")) is None:
        return False
    if bool(row.get("cancel_requested_at")):
        return False
    submitted_at = _iso_datetime(row.get("submitted_at") or row.get("created_at"))
    if submitted_at is None:
        return False
    if (now - submitted_at).total_seconds() <= ack_timeout_seconds:
        return False
    broker_order_id = str(row.get("broker_order_id") or "").strip()
    client_order_id = str(row.get("client_order_id") or "").strip()
    symbol = str(row.get("symbol") or "").strip().upper()
    if broker_order_id and broker_order_id in fill_order_ids:
        return False
    if client_order_id and client_order_id in fill_client_order_ids:
        return False
    if open_by_symbol.get(symbol):
        return False
    broker_position = positions_by_symbol.get(symbol)
    if broker_position is not None and broker_position.quantity > 0:
        return False
    if row.get("first_fill_observed_at") or row.get("first_position_observed_at"):
        return False
    return True


def _latest_event_with_status(
    events: list[dict[str, Any]],
    *,
    statuses: set[str],
    source: str | None,
) -> dict[str, Any] | None:
    normalized_statuses = {str(item).upper() for item in statuses}
    candidates = [
        as_dict(event)
        for event in events
        if str(as_dict(event).get("status") or "").strip().upper() in normalized_statuses
        and (source is None or str(as_dict(event).get("source") or "").strip() == source)
    ]
    candidates.sort(key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
    return candidates[0] if candidates else None


def _latest_event_of_type(
    events: list[dict[str, Any]],
    *,
    event_type: str,
    source: str | None,
) -> dict[str, Any] | None:
    candidates = [
        as_dict(event)
        for event in events
        if str(as_dict(event).get("event_type") or "").strip() == event_type
        and (source is None or str(as_dict(event).get("source") or "").strip() == source)
    ]
    candidates.sort(key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
    return candidates[0] if candidates else None


def _schwab_duration_value(time_in_force: str) -> str:
    return {
        "DAY": "DAY",
        "GTC": "GOOD_TILL_CANCEL",
    }.get(time_in_force, time_in_force)


def _schwab_session_value(session: str) -> str:
    return {
        "NORMAL": "NORMAL",
        "EXT": "EXT",
        "EXTO": "EXTO",
    }.get(session, session)


def _advanced_mode_label(request: ManualOrderRequest) -> str:
    if request.session == "EXTO" and request.time_in_force == "GTC":
        return "GTC_EXTO"
    if request.session == "EXTO":
        return "EXTO"
    if request.session == "EXT":
        return "EXT"
    if request.time_in_force == "GTC":
        return "GTC"
    return "STANDARD"


def _advanced_review_matrix_blockers(request: ManualOrderRequest) -> list[str]:
    advanced_mode = _advanced_mode_label(request)
    if advanced_mode not in {"EXT", "EXTO", "GTC_EXTO"}:
        return []
    blockers: list[str] = []
    supported_order_types = {"LIMIT", "STOP_LIMIT"}
    if request.asset_class != "STOCK":
        blockers.append("Advanced EXTO / GTC_EXTO review is only modeled for STOCK in this phase.")
    if request.structure_type == "SINGLE":
        if request.order_type not in supported_order_types:
            blockers.append("Advanced EXTO / GTC_EXTO review is only modeled for LIMIT and STOP_LIMIT orders in this phase.")
    else:
        for leg in request.oco_legs:
            if leg.order_type not in supported_order_types:
                blockers.append("Advanced EXTO / GTC_EXTO OCO review is only modeled for LIMIT and STOP_LIMIT legs in this phase.")
                break
    return blockers


def _manual_order_type_matrix_for_asset(asset_class: str) -> tuple[str, ...]:
    normalized = _normalize_asset_class(asset_class)
    matrix = {
        "STOCK": (
            "MARKET",
            "LIMIT",
            "STOP",
            "STOP_LIMIT",
            "TRAIL_STOP",
            "TRAIL_STOP_LIMIT",
            "MARKET_ON_CLOSE",
            "LIMIT_ON_CLOSE",
        ),
        "FUTURE": (
            "MARKET",
            "LIMIT",
            "STOP",
            "STOP_LIMIT",
            "TRAIL_STOP",
            "TRAIL_STOP_LIMIT",
        ),
    }
    return matrix.get(normalized, ())


def _live_verified_order_key_set(features: Any) -> set[str]:
    return {str(item).strip().upper() for item in getattr(features, "live_verified_order_keys", ()) if str(item).strip()}


def _verification_sequence_prerequisite_blocker(entry_key: str, *, verified_keys: set[str]) -> str | None:
    for entry in _ORDER_TYPE_LIVE_VERIFICATION_SEQUENCE:
        key = str(entry["key"])
        if key == entry_key:
            return None
        if key not in verified_keys:
            return f"Await live verification of {entry['label']} before enabling this step."
    return None


def _verification_previewable(features: Any, entry: dict[str, Any]) -> tuple[bool, list[str]]:
    asset_class = str(entry["asset_class"])
    order_type = str(entry["order_type"])
    reasons: list[str] = []
    if asset_class == "STOCK":
        if asset_class not in set(features.supported_manual_asset_classes):
            reasons.append(f"Asset class {asset_class} is not enabled for this environment.")
        if order_type not in set(features.supported_manual_dry_run_order_types):
            reasons.append(f"Order type {order_type} is not enabled for dry-run review in this environment.")
        return len(reasons) == 0, reasons
    if asset_class == "FUTURE":
        if not features.futures_pilot_enabled:
            reasons.append("Futures pilot preview is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false.")
        if asset_class not in set(features.futures_supported_asset_classes):
            reasons.append(f"Asset class {asset_class} is not enabled for the futures pilot environment.")
        if order_type not in set(features.supported_manual_dry_run_order_types):
            reasons.append(f"Order type {order_type} is not enabled for dry-run review in this environment.")
        return len(reasons) == 0, reasons
    if order_type in {"EXTO", "GTC_EXTO"}:
        if not features.manual_order_ticket_enabled:
            reasons.append("Manual order ticket is disabled.")
        if not features.advanced_tif_enabled:
            reasons.append("Advanced TIF review support is disabled by feature flag.")
        if not features.ext_exto_ticket_support_enabled:
            reasons.append("EXT / EXTO ticket support is disabled by feature flag.")
        return len(reasons) == 0, reasons
    if order_type == "OCO":
        if not features.manual_order_ticket_enabled:
            reasons.append("Manual order ticket is disabled.")
        if not features.oco_ticket_support_enabled:
            reasons.append("OCO review support is disabled by feature flag.")
        return len(reasons) == 0, reasons
    reasons.append("Order type is not modeled for preview in this phase.")
    return False, reasons


def _verification_live_gate_reasons(features: Any, entry: dict[str, Any]) -> list[str]:
    asset_class = str(entry["asset_class"])
    order_type = str(entry["order_type"])
    reasons: list[str] = []
    if asset_class == "STOCK":
        if order_type not in set(features.supported_manual_order_types):
            reasons.append(f"Order type {order_type} is not live-enabled for this environment.")
        stock_gate_map = {
            "MARKET": (
                features.stock_market_live_submit_enabled,
                "Stock MARKET live submit is disabled because MGC_PRODUCTION_STOCK_MARKET_LIVE_SUBMIT_ENABLED is false.",
            ),
            "LIMIT": (
                features.stock_limit_live_submit_enabled,
                "Stock LIMIT live submit is disabled because MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED is false.",
            ),
            "STOP": (
                features.stock_stop_live_submit_enabled,
                "Stock STOP live submit is disabled because MGC_PRODUCTION_STOCK_STOP_LIVE_SUBMIT_ENABLED is false.",
            ),
            "STOP_LIMIT": (
                features.stock_stop_limit_live_submit_enabled,
                "Stock STOP_LIMIT live submit is disabled because MGC_PRODUCTION_STOCK_STOP_LIMIT_LIVE_SUBMIT_ENABLED is false.",
            ),
        }
        gate_state = stock_gate_map.get(order_type)
        if gate_state and not gate_state[0]:
            reasons.append(gate_state[1])
        if order_type in {"TRAIL_STOP", "TRAIL_STOP_LIMIT"} and not features.trailing_live_submit_enabled:
            reasons.append("Trailing order live submission remains disabled pending live Schwab verification.")
        if order_type in {"MARKET_ON_CLOSE", "LIMIT_ON_CLOSE"} and not features.close_order_live_submit_enabled:
            reasons.append("Market-on-close / limit-on-close live submission remains disabled pending live Schwab verification.")
        return reasons
    if asset_class == "FUTURE":
        if not features.futures_pilot_enabled:
            reasons.append("Futures pilot live submit is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false.")
        if order_type not in set(features.futures_supported_order_types):
            reasons.append(f"Order type {order_type} is not live-enabled for the futures pilot environment.")
        if not features.futures_live_submit_enabled:
            reasons.append("Futures live submission remains disabled pending live Schwab verification.")
        if features.futures_pilot_enabled and order_type == _futures_pilot_order_type() and not _futures_pilot_live_verified(features):
            reasons.append("Futures pilot live submit remains preview-only until FUTURE:MARKET is explicitly live-verified.")
        if order_type in {"TRAIL_STOP", "TRAIL_STOP_LIMIT"} and not features.trailing_live_submit_enabled:
            reasons.append("Trailing order live submission remains disabled pending live Schwab verification.")
        return reasons
    if order_type in {"EXTO", "GTC_EXTO"}:
        if not features.ext_exto_live_submit_enabled:
            reasons.append("EXTO / GTC_EXTO live submit feature flag is disabled.")
        reasons.append("EXTO / GTC_EXTO live submission remains blocked in the current verification phase.")
        return reasons
    if order_type == "OCO":
        if not features.oco_live_submit_enabled:
            reasons.append("OCO live submit feature flag is disabled.")
        reasons.append("OCO live submission remains blocked in the current verification phase.")
        return reasons
    reasons.append("Order type is not modeled for live verification in this phase.")
    return reasons


def _manual_submit_status_summary(*, blockers: list[str]) -> dict[str, str]:
    normalized = [str(item).strip() for item in blockers if str(item).strip()]
    if not normalized:
        return {
            "label": "LIVE SUBMIT ELIGIBLE",
            "detail": "All current manual live-submit safety gates are satisfied for the configured pilot scope.",
        }
    blocker_set = set(normalized)
    if {
        "Manual order ticket is disabled because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED is false.",
        "Live order submit safety mode is disabled because MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED is false.",
    }.issubset(blocker_set):
        return {
            "label": "CONFIG FLAGS OFF",
            "detail": "Blocked because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED and MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED are false in the running dashboard environment.",
        }
    if "Manual order ticket is disabled because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED is false." in blocker_set:
        return {
            "label": "TICKET FLAG OFF",
            "detail": "Blocked because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED is false in the running dashboard environment.",
        }
    if "Live order submit safety mode is disabled because MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED is false." in blocker_set:
        return {
            "label": "SUBMIT SAFETY OFF",
            "detail": "Blocked because MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED is false in the running dashboard environment.",
        }
    if "Manual live submit is blocked because MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST is empty." in blocker_set:
        return {
            "label": "WHITELIST REQUIRED",
            "detail": "Blocked because MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST is empty in the running dashboard environment.",
        }
    if "Futures manual live submit is blocked because MGC_PRODUCTION_FUTURES_SYMBOL_WHITELIST is empty." in blocker_set:
        return {
            "label": "WHITELIST REQUIRED",
            "detail": "Blocked because MGC_PRODUCTION_FUTURES_SYMBOL_WHITELIST is empty in the running dashboard environment.",
        }
    if "Futures live submission is disabled because MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED is false." in blocker_set:
        return {
            "label": "SUBMIT SAFETY OFF",
            "detail": "Blocked because MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED is false in the running dashboard environment.",
        }
    if "Reconciliation is not clear." in blocker_set:
        return {
            "label": "RECONCILIATION BLOCKED",
            "detail": "Blocked because production-link reconciliation is not currently CLEAR.",
        }
    if "Auth is not healthy." in blocker_set:
        return {
            "label": "AUTH BLOCKED",
            "detail": "Blocked because Schwab auth is not currently healthy for live submit.",
        }
    if "Broker is not reachable." in blocker_set:
        return {
            "label": "BROKER BLOCKED",
            "detail": "Blocked because the broker is not currently reachable for live submit.",
        }
    return {
        "label": "LIVE SUBMIT BLOCKED",
        "detail": normalized[0],
    }


def _manual_live_pilot_policy_snapshot(features: Any) -> dict[str, Any]:
    return {
        "capability_status": "HISTORICAL_PROVEN_STOCK_PILOT",
        "policy_scope": "HISTORICAL_RECORD_ONLY",
        "asset_class": "STOCK",
        "submit_order_type": "LIMIT",
        "order_type": "LIMIT",
        "max_quantity": "1",
        "time_in_force": "DAY",
        "session": "NORMAL",
        "regular_hours_only": True,
        "symbol_whitelist": list(features.manual_symbol_whitelist),
        "allowed_open_route": {
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "operator_label": "BUY_TO_OPEN",
        },
        "allowed_close_route": {
            "intent_type": "FLATTEN",
            "side": "SELL",
            "operator_label": "SELL_TO_CLOSE",
        },
        "omit_client_order_id_for_proven_route": True,
    }


def _current_active_manual_lane_is_futures(features: Any) -> bool:
    return bool(features.futures_pilot_enabled)


def _current_active_manual_live_policy_snapshot(features: Any) -> dict[str, Any]:
    if _current_active_manual_lane_is_futures(features):
        return _futures_pilot_policy_snapshot_from_features(features)
    return _manual_live_pilot_policy_snapshot(features)


def _futures_pilot_order_type() -> str:
    return "MARKET"


def _futures_pilot_route_key() -> str:
    return "FUTURES_MARKET_ONE_LOT_WHITELISTED_PILOT"


def _is_futures_pilot_request(request: ManualOrderRequest) -> bool:
    intent_type = (request.intent_type or "").upper()
    return request.asset_class == "FUTURE" and intent_type in {"MANUAL_LIVE_FUTURES_PILOT", "FLATTEN"}


def _futures_pilot_live_verified(features: Any) -> bool:
    return _futures_pilot_verification_key() in set(features.live_verified_order_keys)


def _futures_pilot_verification_key() -> str:
    return "FUTURE:MARKET"


def _configured_futures_symbol_whitelist(features: Any | None) -> list[str]:
    if features is None:
        return []
    raw_values = getattr(features, "futures_symbol_whitelist", ()) or ()
    normalized: list[str] = []
    for item in raw_values:
        symbol = str(item or "").strip().upper()
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


def _configured_futures_symbol_map(features: Any | None) -> dict[str, str]:
    if features is None:
        return {}
    raw_map = getattr(features, "futures_market_data_symbol_map", {}) or {}
    normalized: dict[str, str] = {}
    for key, value in raw_map.items():
        symbol = str(key or "").strip().upper()
        resolved = str(value or "").strip()
        if symbol and resolved:
            normalized[symbol] = resolved
    return normalized


def _futures_pilot_representative_symbol(features: Any | None) -> str:
    whitelist = _configured_futures_symbol_whitelist(features)
    if whitelist:
        return whitelist[0]
    symbol_map = _configured_futures_symbol_map(features)
    if symbol_map:
        return next(iter(symbol_map))
    return "UNCONFIGURED"


def _futures_symbol_authorization_decision(symbol: str, *, features: Any | None) -> dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    whitelist = _configured_futures_symbol_whitelist(features)
    symbol_map = _configured_futures_symbol_map(features)
    is_whitelisted = normalized_symbol in set(whitelist)
    mapped_symbol = symbol_map.get(normalized_symbol)
    allowed = bool(normalized_symbol) and is_whitelisted and bool(mapped_symbol)
    if not normalized_symbol:
        reason = "Blocked because no futures symbol was provided."
    elif not whitelist:
        reason = "Blocked because the futures symbol whitelist is empty."
    elif not is_whitelisted:
        reason = f"Blocked because {normalized_symbol} is not in the configured futures symbol whitelist."
    elif not mapped_symbol:
        reason = f"Blocked because {normalized_symbol} does not have a configured futures market-data mapping."
    else:
        reason = f"Allowed because {normalized_symbol} is whitelisted for the manual futures lane and resolves to {mapped_symbol}."
    return {
        "policy_mode": "WHITELIST_CONTROLLED",
        "requested_symbol": normalized_symbol or None,
        "allowed": allowed,
        "is_whitelisted": is_whitelisted,
        "resolved_external_symbol": mapped_symbol,
        "whitelisted_symbols": whitelist,
        "reason": reason,
    }


# This narrow lane is an intentional ongoing manual-entry capability for the
# approved one-lot manual futures pilot. Symbol scope is controlled by the
# configured futures whitelist and market-data mapping, not by a hardcoded
# single-symbol assumption and not by a general futures session bypass.
def _is_scoped_manual_futures_pilot_lane(request: ManualOrderRequest, *, features: Any | None = None) -> bool:
    intent_type = (request.intent_type or "").upper()
    symbol_authorization = _futures_symbol_authorization_decision(request.symbol, features=features)
    return (
        request.structure_type == "SINGLE"
        and request.asset_class == "FUTURE"
        and bool(symbol_authorization.get("allowed"))
        and request.order_type == _futures_pilot_order_type()
        and request.quantity == Decimal("1")
        and request.time_in_force == "DAY"
        and request.session == "NORMAL"
        and (
            (intent_type == "MANUAL_LIVE_FUTURES_PILOT" and request.side == "BUY")
            or (intent_type == "FLATTEN" and request.side == "SELL")
        )
    )


def _futures_pilot_time_session_policy_snapshot(features: Any | None = None) -> dict[str, Any]:
    whitelist = _configured_futures_symbol_whitelist(features)
    return {
        "policy_mode": "SCOPED_POLICY_AMENDMENT",
        "audit_label": "MANUAL_FUTURES_PILOT_TIME_SESSION_POLICY",
        "capability_status": "DURABLE_NARROW_MANUAL_CAPABILITY",
        "capability_intent": "Remain available for approved manual operator use until intentionally changed later.",
        "submitted_time_in_force": "DAY",
        "submitted_session": "NORMAL",
        "current_clock_gate_applied": False,
        "allowed_outside_current_clock_window": True,
        "detail": (
            "The narrow whitelist-controlled manual futures pilot lane keeps Schwab duration DAY and session NORMAL, "
            "but production-link does not apply a separate current-clock session rejection to an approved lane request. "
            "This is an explicit futures-pilot policy amendment, not a general routing bypass."
        ),
        "allowed_lane": {
            "asset_class": "FUTURE",
            "symbol_scope": "WHITELIST_CONTROLLED",
            "allowed_symbols": whitelist,
            "order_type": _futures_pilot_order_type(),
            "quantity": "1",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "allowed_open_route": "MANUAL_LIVE_FUTURES_PILOT / BUY_TO_OPEN",
            "allowed_close_route": "FLATTEN / SELL_TO_CLOSE",
        },
    }


def _futures_pilot_time_session_policy_decision(
    request: ManualOrderRequest,
    *,
    now: datetime,
    features: Any | None = None,
) -> dict[str, Any] | None:
    if not _is_scoped_manual_futures_pilot_lane(request, features=features):
        return None
    eastern_now = now.astimezone(ZoneInfo("America/New_York"))
    symbol_authorization = _futures_symbol_authorization_decision(request.symbol, features=features)
    return {
        **_futures_pilot_time_session_policy_snapshot(features),
        "decision": "ALLOWED",
        "allowed": True,
        "evaluated_at": now.isoformat(),
        "current_eastern_time": eastern_now.isoformat(),
        "current_us_regular_hours": _is_us_regular_hours(now),
        "symbol_authorization": symbol_authorization,
        "allowed_reason": (
            "Allowed because this request matches the narrow one-lot manual futures pilot lane, the requested symbol "
            "is explicitly whitelisted and mapped, and that lane is exempt from a separate wall-clock DAY/NORMAL rejection."
        ),
    }


def _futures_pilot_outside_sandbox_runbook(snapshot: dict[str, Any]) -> dict[str, Any]:
    connection = as_dict(snapshot.get("connection"))
    features = SimpleNamespace(**as_dict(snapshot.get("feature_flags")))
    allowed_symbols = _configured_futures_symbol_whitelist(features)
    representative_symbol = _futures_pilot_representative_symbol(features)
    return {
        "title": "Outside-Sandbox Manual Futures Live Validation",
        "runbook_path": "docs/MANUAL_FUTURES_PILOT_RUNBOOK.md",
        "launch_path": [
            "Use the existing service-first host path only.",
            "If the host is not already running, start it with bash scripts/run_headless_supervised_paper_service.sh --wait-timeout-seconds 120.",
            "Verify host health at curl -sS http://127.0.0.1:8790/health.",
            "Prime local operator auth with bash scripts/run_local_operator_auth.sh before preview or submit.",
            "Use the running operator/dashboard surface at http://127.0.0.1:8790/ and Positions > Manual Order Ticket.",
        ],
        "symbol_scope": {
            "mode": "WHITELIST_CONTROLLED",
            "allowed_symbols": allowed_symbols,
            "representative_symbol": representative_symbol,
        },
        "account_assumptions": {
            "selected_account_id": connection.get("selected_account_id") or connection.get("selected_account_hash"),
            "selected_account_hash": connection.get("selected_account_hash"),
            "selected_account_number": connection.get("selected_account_number"),
            "selected_account_display_name": connection.get("selected_account_display_name"),
            "selection_source": connection.get("selection_source"),
        },
        "open_sequence": [
            "Confirm the futures pilot status shows no live-submit blockers.",
            f"Build MANUAL_LIVE_FUTURES_PILOT / BUY / FUTURE / MARKET / quantity 1 / a configured whitelisted futures symbol such as {representative_symbol} / DAY / NORMAL.",
            "Preview first and require action_phase OPEN_PREVIEW, allowing_rule MANUAL_FUTURES_PILOT_TIME_SESSION_POLICY, and an empty blocker list.",
            "Submit only after explicit operator review confirmation.",
            "Capture broker order id, acknowledgement, lifecycle state, and broker/manual position truth.",
        ],
        "close_sequence": [
            "Require an existing broker LONG 1 position in the same configured futures symbol before flatten.",
            "Build FLATTEN / SELL / FUTURE / MARKET / quantity 1 / DAY / NORMAL.",
            "Preview first and require action_phase FLATTEN_PREVIEW, allowing_rule MANUAL_FUTURES_PILOT_TIME_SESSION_POLICY, and an empty blocker list.",
            "Submit only after explicit operator review confirmation.",
            "Capture broker acknowledgement, fill, return-to-flat proof, and reconciliation CLEAR.",
        ],
        "expected_surfaces": [
            "futures_pilot_status",
            "futures_pilot_policy_snapshot",
            "runtime_state.last_manual_order_preview",
            "runtime_state.last_manual_order",
            "manual_live_orders.recent_rows",
            "broker_state_snapshot.positions_by_symbol",
            "orders.open_rows",
            "reconciliation",
            "local_operator_auth",
        ],
        "abort_criteria": [
            "Any live-submit blocker appears before submit.",
            "Broker order id is missing after submit acknowledgement.",
            "Broker/account/auth freshness turns degraded before the next irreversible step.",
            "Same-symbol ambiguity appears between broker and tracked manual state.",
            "Open leg does not resolve to broker-truth-backed LONG 1 before flatten.",
            "Close leg does not restore flat state and CLEAR reconciliation.",
        ],
        "pass_fail_rule": "Pass only when a configured whitelisted one-lot futures lane opens, broker truth shows LONG 1 in that symbol, flatten closes it, flat state is restored, and reconciliation returns CLEAR with no residual same-symbol ambiguity.",
    }


def _futures_pilot_live_validation_checklist(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    connection = as_dict(snapshot.get("connection"))
    health = as_dict(snapshot.get("health"))
    reconciliation = as_dict(snapshot.get("reconciliation"))
    local_operator_auth = as_dict(snapshot.get("local_operator_auth"))
    gate_summary = _futures_pilot_gate_summary(snapshot)
    return [
        {"label": "Operator auth ready", "ok": gate_summary["local_operator_auth_ready"], "detail": local_operator_auth.get("detail")},
        {"label": "Live broker account selected", "ok": bool(connection.get("selected_account_id") or connection.get("selected_account_hash")), "detail": connection.get("selected_account_display_name")},
        {"label": "Broker reachable", "ok": gate_summary["broker_reachable"], "detail": as_dict(health.get("broker_reachable")).get("detail")},
        {"label": "Balances freshness in bounds", "ok": gate_summary["balances_fresh"], "detail": as_dict(health.get("balances_fresh")).get("detail")},
        {"label": "Positions freshness in bounds", "ok": gate_summary["positions_fresh"], "detail": as_dict(health.get("positions_fresh")).get("detail")},
        {"label": "Orders freshness in bounds", "ok": gate_summary["orders_fresh"], "detail": as_dict(health.get("orders_fresh")).get("detail")},
        {"label": "Reconciliation CLEAR before open", "ok": gate_summary["reconciliation_clear"], "detail": reconciliation.get("detail")},
        {"label": "Manual open allowed for approved lane", "ok": True, "detail": "Approved open route is MANUAL_LIVE_FUTURES_PILOT / BUY_TO_OPEN for a whitelisted FUTURE MARKET quantity 1 DAY NORMAL symbol."},
        {"label": "Broker accepted order", "ok": False, "detail": "Require submit acknowledgement with a broker order id."},
        {"label": "Position established correctly", "ok": False, "detail": "Require broker truth to show LONG 1 in the selected futures symbol before flatten."},
        {"label": "Flatten path allowed for approved lane", "ok": True, "detail": "Approved close route is FLATTEN / SELL_TO_CLOSE for a whitelisted FUTURE MARKET quantity 1 DAY NORMAL symbol."},
        {"label": "Broker accepted flatten", "ok": False, "detail": "Require flatten acknowledgement with a broker order id."},
        {"label": "Flat state restored", "ok": False, "detail": "Require no broker/manual position remaining in the selected futures symbol after close."},
        {"label": "Reconciliation CLEAR after flatten", "ok": False, "detail": "Require reconciliation to return CLEAR after the close leg."},
        {"label": "No unresolved same-symbol ambiguity", "ok": gate_summary["same_symbol_unresolved_ambiguity_absent"], "detail": "Broker/manual ambiguity must remain absent before, during, and after the cycle."},
        {"label": "No stuck or residual state afterward", "ok": False, "detail": "Require no residual open orders, no unresolved manual-live row, and no stuck lifecycle state after the cycle."},
    ]


def _futures_pilot_proof_boundary() -> dict[str, Any]:
    return {
        "proven_inside_sandbox": [
            "Whitelist-controlled one-lot manual futures lane is explicitly defined and durable by design.",
            "Scoped manual futures time/session policy allows the narrow DAY + NORMAL lane without a separate wall-clock rejection.",
            "Preview payload and live-submit gate can be modeled with a whitelisted FUTURE MARKET route and audited allowing rule.",
            "Stock pilot remains unchanged and ANYTIME remains disabled/deferred.",
        ],
        "requires_outside_sandbox_live_validation": [
            "Real Schwab broker acceptance for open submit.",
            "Real broker/manual lifecycle progression through acknowledgement and fill.",
            "Real broker position truth showing LONG 1 in the selected futures symbol after open.",
            "Real flatten submit, return to flat, and reconciliation CLEAR after close.",
        ],
        "unsupported_claim_guardrail": "Do not claim end-to-end live Schwab execution is proven until the outside-sandbox operator run completes successfully.",
    }


def _futures_pilot_action_phase(request: ManualOrderRequest, *, preview: bool) -> str | None:
    if not _is_futures_pilot_request(request):
        return None
    intent_type = (request.intent_type or "").upper()
    if intent_type == "MANUAL_LIVE_FUTURES_PILOT":
        return "OPEN_PREVIEW" if preview else "OPEN_SUBMIT"
    if intent_type == "FLATTEN":
        return "FLATTEN_PREVIEW" if preview else "FLATTEN_SUBMIT"
    return "FUTURES_PILOT_ACTION"


def _futures_pilot_gate_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    health = as_dict(snapshot.get("health"))
    local_operator_auth = as_dict(snapshot.get("local_operator_auth"))
    reconciliation = as_dict(snapshot.get("reconciliation"))
    connection = as_dict(snapshot.get("connection"))
    return {
        "local_operator_auth_ready": bool(local_operator_auth.get("ready")),
        "selected_account_present": bool(connection.get("selected_account_id") or connection.get("selected_account_hash")),
        "broker_reachable": as_dict(health.get("broker_reachable")).get("ok") is True,
        "auth_healthy": as_dict(health.get("auth_healthy")).get("ok") is True,
        "balances_fresh": as_dict(health.get("balances_fresh")).get("ok") is True,
        "positions_fresh": as_dict(health.get("positions_fresh")).get("ok") is True,
        "orders_fresh": as_dict(health.get("orders_fresh")).get("ok") is True,
        "reconciliation_clear": str(reconciliation.get("status") or "").lower() == "clear",
        "same_symbol_unresolved_ambiguity_absent": str(reconciliation.get("mismatch_count") or "0") in {"0", "0.0", "0.00"},
    }


def _futures_pilot_durability_snapshot(features: Any) -> dict[str, Any]:
    return {
        "durable_by_design": True,
        "capability_classification": "ONGOING_NARROW_MANUAL_FUTURES_PILOT",
        "change_control": "Intentional configuration or code change required to remove or widen this lane.",
        "not_a_temporary_exception": True,
        "not_test_only": True,
        "stable_rule_boundaries": {
            "asset_class": "FUTURE",
            "symbol_scope": "WHITELIST_CONTROLLED",
            "order_type": _futures_pilot_order_type(),
            "quantity": "1",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "open_route": "MANUAL_LIVE_FUTURES_PILOT / BUY_TO_OPEN",
            "close_route": "FLATTEN / SELL_TO_CLOSE",
        },
        "required_feature_flags": {
            "production_connectivity_enabled": bool(features.production_connectivity_enabled),
            "manual_order_ticket_enabled": bool(features.manual_order_ticket_enabled),
            "live_order_submit_enabled": bool(features.live_order_submit_enabled),
            "futures_pilot_enabled": bool(features.futures_pilot_enabled),
            "futures_live_submit_enabled": bool(features.futures_live_submit_enabled),
            "live_verified_future_market_enabled": _futures_pilot_live_verified(features),
        },
        "hidden_dependency_check": {
            "depends_on_stock_pilot_flags": False,
            "depends_on_anytime_widening": False,
            "depends_on_test_only_runtime_hook": False,
            "depends_on_desktop_process_boundary": False,
        },
    }


def _futures_pilot_policy_snapshot_from_features(features: Any) -> dict[str, Any]:
    symbol_whitelist = _configured_futures_symbol_whitelist(features)
    primary_symbol = _futures_pilot_representative_symbol(features)
    symbol_map = _configured_futures_symbol_map(features)
    external_symbol = str(symbol_map.get(primary_symbol, "")).strip() or primary_symbol
    if not features.futures_pilot_enabled:
        status = "DISABLED"
    elif bool(features.futures_live_submit_enabled) and _futures_pilot_live_verified(features):
        status = "PILOT READY"
    else:
        status = "PREVIEW READY"
    return {
        "lane_key": _futures_pilot_route_key(),
        "separate_from_stock_pilot": True,
        "enabled": bool(features.futures_pilot_enabled),
        "status": status,
        "capability_status": "DURABLE_NARROW_MANUAL_FUTURES_PILOT",
        "asset_class": "FUTURE",
        "symbol_scope": "WHITELIST_CONTROLLED",
        "symbol": primary_symbol,
        "representative_symbol": primary_symbol,
        "symbol_whitelist": symbol_whitelist,
        "submit_order_type": _futures_pilot_order_type(),
        "order_type": _futures_pilot_order_type(),
        "max_quantity": str(features.futures_max_quantity),
        "time_in_force": "DAY",
        "session": "NORMAL",
        "operator_requested_market_hours": "DAY + NORMAL ONLY",
        "recommended_first_market_hours": "DAY + NORMAL",
        "regular_us_market_hours_only": False,
        "time_session_policy": _futures_pilot_time_session_policy_snapshot(features),
        "durability": _futures_pilot_durability_snapshot(features),
        "allowed_open_route": {
            "intent_type": "MANUAL_LIVE_FUTURES_PILOT",
            "side": "BUY",
            "operator_label": "BUY_TO_OPEN",
        },
        "allowed_close_route": {
            "intent_type": "FLATTEN",
            "side": "SELL",
            "operator_label": "SELL_TO_CLOSE",
            "existing_broker_position_required": "LONG 1",
        },
        "selected_account_requirements": {
            "selected_live_schwab_account_required": True,
            "account_live_verified_required": True,
            "allowed_account_source": "schwab_live",
        },
        "market_data_requirements": {
            "symbol_scope": "WHITELIST_CONTROLLED",
            "representative_internal_symbol": primary_symbol,
            "representative_resolved_external_symbol": external_symbol,
            "market_data_symbol_map": symbol_map,
            "mapping_must_resolve_external_futures_symbol": True,
            "quote_payload_required": True,
        },
        "broker_truth_requirements": {
            "broker_reachable_required": True,
            "auth_healthy_required": True,
            "balances_fresh_required": True,
            "positions_fresh_required": True,
            "orders_fresh_required": True,
            "same_symbol_unresolved_broker_manual_ambiguity_allowed": False,
        },
        "reconciliation_requirements": {
            "pre_entry_status_required": "CLEAR",
            "post_close_status_required": "CLEAR",
            "mismatch_count_required": 0,
        },
        "client_order_id_policy": {
            "omit_for_approved_futures_pilot_route": True,
            "existing_live_strategy_broker": "OMITTED",
            "manual_futures_pilot_route": "OMITTED",
            "detail": "The approved whitelist-controlled futures pilot route omits clientOrderId to match the existing one-lot futures broker helper behavior, without broadening that exception beyond the manual futures lane.",
        },
        "futures_config": {
            "futures_pilot_enabled": bool(features.futures_pilot_enabled),
            "futures_live_submit_enabled": bool(features.futures_live_submit_enabled),
            "futures_symbol_whitelist": symbol_whitelist,
            "futures_supported_asset_classes": list(features.futures_supported_asset_classes),
            "futures_supported_order_types": list(features.futures_supported_order_types),
            "futures_supported_time_in_force_values": list(features.futures_supported_time_in_force_values),
            "futures_supported_session_values": list(features.futures_supported_session_values),
            "futures_max_quantity": str(features.futures_max_quantity),
            "futures_market_data_symbol_map": dict(features.futures_market_data_symbol_map),
        },
    }


def _futures_pilot_policy_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    feature_flags = as_dict(snapshot.get("feature_flags"))
    return _futures_pilot_policy_snapshot_from_features(SimpleNamespace(**feature_flags))


def _futures_pilot_gap_analysis(snapshot: dict[str, Any]) -> dict[str, Any]:
    feature_flags = as_dict(snapshot.get("feature_flags"))
    return {
        "current_stock_pilot": {
            "already_exists": [
                "Separate proven live pilot already exists for STOCK only.",
                "Current operator workflow and production-link pilot snapshot are locked to the approved one-lot route, quantity 1, DAY, NORMAL, BUY_TO_OPEN / SELL_TO_CLOSE.",
            ],
            "hard_locks": [
                "Current manual live pilot route only allows STOCK.",
                "Current clientOrderId omission is limited to the exact stock pilot shape.",
            ],
        },
        "existing_futures_broker_path": {
            "already_exists": [
                "LiveStrategyPilotBroker already supports the narrow one-lot futures manual-entry lane used by the operator path.",
                "Existing futures broker helper already uses asset_type=FUTURE, quantity=1, session=NORMAL, time_in_force=DAY.",
                "Existing futures broker helper omits clientOrderId.",
            ],
            "unverified_live_behavior": [
                "Production-link manual futures submission has not yet been live-verified against Schwab.",
                "Operator-facing futures submit/ack/fill/flatten surfaces are not yet separated from the stock pilot lane.",
            ],
        },
        "current_production_link_restrictions": {
            "blocked_only_by_policy": [
                "Futures live verification matrix already models FUTURE MARKET preview and live-gate rows.",
                "Futures exact-route preview can be enabled separately from stock without widening the stock pilot lane.",
            ],
            "blocked_by_hard_coded_route_checks": [
                "Current stock pilot route remains hard-locked to STOCK and must stay separate.",
                "Futures submit must remain preview-only until the first exact FUTURE MARKET live verification is explicitly requested.",
            ],
            "current_active_environment": {
                "supported_manual_asset_classes": as_list(feature_flags.get("supported_manual_asset_classes")),
                "supported_manual_order_types": as_list(feature_flags.get("supported_manual_order_types")),
                "manual_symbol_whitelist": as_list(feature_flags.get("manual_symbol_whitelist")),
                "futures_pilot_enabled": bool(feature_flags.get("futures_pilot_enabled")),
                "futures_live_submit_enabled": bool(feature_flags.get("futures_live_submit_enabled")),
                "futures_symbol_whitelist": as_list(feature_flags.get("futures_symbol_whitelist")),
            },
        },
        "requested_operator_target": {
            "requested_shape": "FUTURE + MARKET + quantity 1 + whitelisted futures symbol + DAY + NORMAL",
            "reconciled_first_scope": "FUTURE + MARKET + quantity 1 + whitelisted futures symbol + DAY + NORMAL",
            "anytime_delta_if_adopted": [
                "ANYTIME would be a deliberate widening beyond the current futures broker implementation.",
                "ANYTIME would require explicit futures session semantics in production-link policy and operator wording.",
                "ANYTIME would require live Schwab verification for a whitelisted futures symbol outside the current DAY + NORMAL assumption before enablement.",
            ],
        },
    }


def _futures_pilot_preview_blockers(snapshot: dict[str, Any]) -> list[str]:
    feature_flags = as_dict(snapshot.get("feature_flags"))
    blockers: list[str] = []
    whitelist = _configured_futures_symbol_whitelist(SimpleNamespace(**feature_flags))
    symbol_map = _configured_futures_symbol_map(SimpleNamespace(**feature_flags))
    if not bool(feature_flags.get("production_connectivity_enabled")):
        blockers.append("Futures pilot preview is disabled because MGC_PRODUCTION_LINK_ENABLED is false.")
    if not bool(feature_flags.get("manual_order_ticket_enabled")):
        blockers.append("Futures pilot preview is disabled because MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED is false.")
    if not bool(feature_flags.get("futures_pilot_enabled")):
        blockers.append("Futures pilot preview is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false.")
    if "FUTURE" not in set(as_list(feature_flags.get("futures_supported_asset_classes"))):
        blockers.append("Futures pilot preview requires FUTURE in the futures-supported asset classes.")
    if _futures_pilot_order_type() not in set(as_list(feature_flags.get("futures_supported_order_types"))):
        blockers.append(f"Futures pilot preview requires {_futures_pilot_order_type()} in the futures-supported order types.")
    if not whitelist:
        blockers.append("Futures pilot preview requires a non-empty futures symbol whitelist.")
    if "DAY" not in set(as_list(feature_flags.get("futures_supported_time_in_force_values"))):
        blockers.append("Futures pilot preview requires DAY in the futures-supported time-in-force values.")
    if "NORMAL" not in set(as_list(feature_flags.get("futures_supported_session_values"))):
        blockers.append("Futures pilot preview requires NORMAL in the futures-supported session values.")
    if (_decimal(feature_flags.get("futures_max_quantity")) or Decimal("0")) != Decimal("1"):
        blockers.append("Futures pilot preview requires futures_max_quantity=1.")
    if whitelist:
        missing_symbols = [symbol for symbol in whitelist if not symbol_map.get(symbol)]
        if missing_symbols:
            blockers.append(
                "Futures pilot preview requires market-data mappings for all whitelisted futures symbols: "
                + ", ".join(missing_symbols)
                + "."
            )
    latest_preview = as_dict(as_dict(snapshot.get("runtime_state")).get("last_manual_order_preview"))
    broker_preview_result = as_dict(as_dict(latest_preview.get("payload_summary")).get("broker_preview_result"))
    if broker_preview_result.get("ok") is False:
        blockers.append(str(broker_preview_result.get("error") or "Latest broker preview rejected the futures payload."))
    return list(dict.fromkeys(blockers))


def _futures_pilot_live_submit_blockers(snapshot: dict[str, Any]) -> list[str]:
    feature_flags = as_dict(snapshot.get("feature_flags"))
    blockers = _futures_pilot_preview_blockers(snapshot)
    for blocker in _production_link_current_live_blockers(snapshot):
        blockers.append(blocker)
    if not bool(feature_flags.get("futures_live_submit_enabled")):
        blockers.append("Futures live submission is disabled because MGC_PRODUCTION_FUTURES_LIVE_SUBMIT_ENABLED is false.")
    if _futures_pilot_verification_key() not in set(as_list(feature_flags.get("live_verified_order_keys"))):
        blockers.append("Futures pilot live submit remains preview-only until FUTURE:MARKET is explicitly live-verified.")
    return list(dict.fromkeys(blockers))


def _futures_pilot_operator_workflow() -> list[dict[str, Any]]:
    return [
        {
            "step": "A",
            "label": "Authenticate Now",
            "detail": "Prime the shared local Touch ID session before preview, open, or flatten actions, then reuse it throughout the active pilot window.",
            "proof_surface": "futures_pilot_status.local_operator_auth",
        },
        {
            "step": "B",
            "label": "Pre-entry checklist",
            "detail": "Confirm local operator auth is active, the selected Schwab account is live-verified, broker/auth health are green, freshness checks are in bounds, reconciliation is CLEAR, and the selected futures symbol is explicitly whitelisted and mapped.",
            "proof_surface": "futures_pilot_status.prerequisites",
        },
        {
            "step": "C",
            "label": "Set long-entry ticket",
            "detail": "Build the ticket as MANUAL_LIVE_FUTURES_PILOT / BUY / FUTURE / MARKET / quantity 1 / whitelisted futures symbol / DAY / NORMAL.",
            "proof_surface": "futures_pilot_policy_snapshot.allowed_open_route",
        },
        {
            "step": "D",
            "label": "Preview",
            "detail": "Run preview first and require the intended Schwab payload to stay one-lot whitelisted FUTURE MARKET with the futures pilot blockers cleared. The narrow lane keeps DAY + NORMAL in the Schwab payload, but does not apply a separate wall-clock session rejection.",
            "proof_surface": "futures_pilot_status.next_live_verification_step",
        },
        {
            "step": "E",
            "label": "Review / confirm / send",
            "detail": "Submit only after explicit operator review confirmation and only through the dedicated futures pilot lane.",
            "proof_surface": "futures_pilot_status.live_submit_blockers",
        },
        {
            "step": "F",
            "label": "Broker ack / fill proof",
            "detail": "Capture broker acknowledgement, order id, and transition to WORKING or FILLED from the dedicated futures proof surfaces.",
            "proof_surface": "futures_pilot_status.proof_surfaces.last_submitted_futures_order",
        },
        {
            "step": "G",
            "label": "Position proof",
            "detail": "Refresh broker truth and confirm the live futures position is LONG 1 in the selected symbol before preparing the flatten ticket.",
            "proof_surface": "futures_pilot_status.proof_surfaces.live_futures_position",
        },
        {
            "step": "H",
            "label": "Flatten preview",
            "detail": "Build and preview the close ticket exactly as FLATTEN / SELL / FUTURE / MARKET / quantity 1 / DAY / NORMAL with an existing LONG 1.",
            "proof_surface": "futures_pilot_policy_snapshot.allowed_close_route",
        },
        {
            "step": "I",
            "label": "Flatten review / confirm / send",
            "detail": "Submit the flatten leg only after explicit review confirmation and broker/manual state still agrees on the single open futures position.",
            "proof_surface": "futures_pilot_status.live_submit_blockers",
        },
        {
            "step": "J",
            "label": "Flat + reconciliation clear proof",
            "detail": "Refresh broker truth, confirm the selected futures symbol returns to flat, and require reconciliation to return CLEAR with no same-symbol open orders left behind.",
            "proof_surface": "futures_pilot_status.proof_surfaces.reconciliation_after_close",
        },
    ]


def _futures_pilot_status_export_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    manual_order_safety = as_dict(snapshot.get("manual_order_safety"))
    local_operator_auth = as_dict(snapshot.get("local_operator_auth"))
    health = as_dict(snapshot.get("health"))
    connection = as_dict(snapshot.get("connection"))
    reconciliation = as_dict(snapshot.get("reconciliation"))
    policy = _futures_pilot_policy_snapshot(snapshot)
    time_session_policy = as_dict(policy.get("time_session_policy")) or _futures_pilot_time_session_policy_snapshot(SimpleNamespace(**as_dict(snapshot.get("feature_flags"))))
    preview_blockers = _futures_pilot_preview_blockers(snapshot)
    live_submit_blockers = _futures_pilot_live_submit_blockers(snapshot)
    preview_enabled = not preview_blockers and policy.get("enabled")
    live_submit_enabled = len(live_submit_blockers) == 0
    if live_submit_enabled:
        status = "PILOT READY"
        label = "FUTURES PILOT READY"
    elif preview_enabled:
        status = "PREVIEW READY"
        label = "FUTURES PREVIEW READY"
    else:
        status = "NOT READY"
        label = "FUTURES PREVIEW BLOCKED"
    prerequisites = [
        {
            "label": "Local operator auth ready",
            "ok": local_operator_auth.get("ready") is True,
            "detail": local_operator_auth.get("detail"),
        },
        {
            "label": "Selected Schwab account live-verified",
            "ok": bool(manual_order_safety.get("selected_account_live_verified")),
            "detail": as_dict(health.get("account_selected")).get("detail"),
        },
        {
            "label": "Broker reachable",
            "ok": as_dict(health.get("broker_reachable")).get("ok") is True,
            "detail": as_dict(health.get("broker_reachable")).get("detail"),
        },
        {
            "label": "Auth healthy",
            "ok": as_dict(health.get("auth_healthy")).get("ok") is True,
            "detail": as_dict(health.get("auth_healthy")).get("detail"),
        },
        {
            "label": "Balances freshness in bounds",
            "ok": as_dict(health.get("balances_fresh")).get("ok") is True,
            "detail": as_dict(health.get("balances_fresh")).get("detail"),
        },
        {
            "label": "Positions freshness in bounds",
            "ok": as_dict(health.get("positions_fresh")).get("ok") is True,
            "detail": as_dict(health.get("positions_fresh")).get("detail"),
        },
        {
            "label": "Orders freshness in bounds",
            "ok": as_dict(health.get("orders_fresh")).get("ok") is True,
            "detail": as_dict(health.get("orders_fresh")).get("detail"),
        },
        {
            "label": "Reconciliation CLEAR",
            "ok": str(reconciliation.get("status") or "").lower() == "clear",
            "detail": reconciliation.get("detail"),
        },
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "label": label,
        "capability_status": "DURABLE_NARROW_MANUAL_FUTURES_PILOT",
        "separate_lane_required": True,
        "preview_enabled": preview_enabled,
        "preview_blockers": preview_blockers,
        "live_submit_enabled": live_submit_enabled,
        "live_submit_blocked_pending_first_verification": not live_submit_enabled,
        "selected_account": {
            "account_hash": connection.get("selected_account_hash"),
            "account_number": connection.get("selected_account_number"),
            "display_name": connection.get("selected_account_display_name"),
            "selection_source": connection.get("selection_source"),
        },
        "local_operator_auth": local_operator_auth,
        "prerequisites": prerequisites,
        "requested_operator_target": {
            "asset_class": "FUTURE",
            "order_type": _futures_pilot_order_type(),
            "quantity": "1",
            "symbol_scope": "WHITELIST_CONTROLLED",
            "representative_symbol": policy.get("representative_symbol"),
            "time_in_force": "DAY",
            "session": "NORMAL",
            "operator_requested_market_hours": "DAY + NORMAL ONLY",
        },
        "recommended_first_lane": {
            "asset_class": "FUTURE",
            "order_type": _futures_pilot_order_type(),
            "quantity": "1",
            "symbol_scope": "WHITELIST_CONTROLLED",
            "representative_symbol": policy.get("representative_symbol"),
            "time_in_force": "DAY",
            "session": "NORMAL",
            "operator_requested_market_hours": "DAY + NORMAL ONLY",
            "recommended_market_hours_policy": "Keep DAY + NORMAL only for the current live futures route.",
        },
        "time_session_policy": time_session_policy,
        "durability": as_dict(policy.get("durability")),
        "outside_sandbox_live_validation": _futures_pilot_outside_sandbox_runbook(snapshot),
        "live_cycle_checklist": _futures_pilot_live_validation_checklist(snapshot),
        "proof_boundary": _futures_pilot_proof_boundary(),
        "policy_snapshot": policy,
        "gap_analysis": _futures_pilot_gap_analysis(snapshot),
        "live_submit_blockers": live_submit_blockers,
        "next_live_verification_step": {
            "route_key": _futures_pilot_route_key(),
            "label": "First Futures Live Verification Lane",
            "preview_allowed_now": preview_enabled,
            "live_submit_allowed_now": live_submit_enabled,
            "blocked_reason": (preview_blockers or live_submit_blockers or [None])[0],
            "operator_path": "Positions > Manual Order Ticket",
            "time_session_policy": time_session_policy,
            "exact_open_shape": {
                "intent_type": "MANUAL_LIVE_FUTURES_PILOT",
                "operator_label": "BUY_TO_OPEN",
                "side": "BUY",
                "asset_class": "FUTURE",
                "symbol": policy.get("representative_symbol"),
                "allowed_symbols": policy.get("symbol_whitelist"),
                "order_type": _futures_pilot_order_type(),
                "quantity": policy.get("max_quantity"),
                "time_in_force": policy.get("time_in_force"),
                "session": policy.get("session"),
                "omit_client_order_id": True,
            },
            "exact_close_shape": {
                "intent_type": "FLATTEN",
                "operator_label": "SELL_TO_CLOSE",
                "side": "SELL",
                "asset_class": "FUTURE",
                "symbol": policy.get("representative_symbol"),
                "allowed_symbols": policy.get("symbol_whitelist"),
                "order_type": _futures_pilot_order_type(),
                "quantity": policy.get("max_quantity"),
                "time_in_force": policy.get("time_in_force"),
                "session": policy.get("session"),
                "existing_broker_position_required": "LONG 1",
                "omit_client_order_id": True,
            },
        },
        "proof_surfaces": {
            "futures_pilot_policy_snapshot": "/api/operator-artifact/production-link-futures-pilot-policy",
            "futures_pilot_status": "/api/operator-artifact/production-link-futures-pilot-status",
            "futures_preview_blocker_list": preview_blockers,
            "futures_live_submit_blocker_list": live_submit_blockers,
            "next_live_verification_step": "futures_pilot_status.next_live_verification_step",
            "last_futures_order_preview": as_dict(as_dict(snapshot.get("diagnostics")).get("last_manual_order_preview")),
            "last_submitted_futures_order": {},
            "live_futures_position": {},
            "reconciliation_after_close": {},
        },
        "operator_workflow": _futures_pilot_operator_workflow(),
    }


def _locked_futures_live_pilot_route_blockers(
    *,
    request: ManualOrderRequest,
    broker_position: dict[str, Any],
    features: Any,
) -> list[str]:
    if request.asset_class != "FUTURE":
        return ["Locked futures pilot route only supports FUTURE."]
    if request.order_type != _futures_pilot_order_type():
        return [f"Locked futures pilot route only supports {_futures_pilot_order_type()} submit."]
    if request.quantity != Decimal("1"):
        return ["Locked futures pilot route only supports quantity 1."]
    if request.time_in_force != "DAY":
        return ["Locked futures pilot route only supports DAY time-in-force."]
    if request.session != "NORMAL":
        return ["Locked futures pilot route only supports NORMAL session."]
    if request.symbol not in set(features.futures_symbol_whitelist):
        return [f"Locked futures pilot route only supports whitelisted futures symbols: {', '.join(features.futures_symbol_whitelist) or 'none'}."]
    if not bool(features.futures_market_data_symbol_map.get(request.symbol)):
        return [f"Locked futures pilot route requires a market-data mapping for {request.symbol}."]
    intent_type = (request.intent_type or "").upper()
    side = request.side.upper()
    if intent_type == "MANUAL_LIVE_FUTURES_PILOT":
        if side != "BUY":
            return ["Locked futures pilot open route only supports BUY_TO_OPEN."]
        return []
    if intent_type == "FLATTEN":
        broker_side = str(broker_position.get("side") or "").strip().upper()
        broker_quantity = _decimal(broker_position.get("quantity")) or Decimal("0")
        blockers: list[str] = []
        if side != "SELL":
            blockers.append("Locked futures pilot close route only supports SELL_TO_CLOSE.")
        if broker_side != "LONG" or broker_quantity != Decimal("1"):
            blockers.append("Locked futures pilot close route requires an existing LONG 1 broker position.")
        return blockers
    return ["Locked futures pilot route only supports MANUAL_LIVE_FUTURES_PILOT BUY_TO_OPEN and FLATTEN SELL_TO_CLOSE."]


def _locked_manual_live_pilot_route_blockers(
    *,
    request: ManualOrderRequest,
    broker_position: dict[str, Any],
) -> list[str]:
    if request.asset_class != "STOCK":
        return ["Historical stock pilot route only supports STOCK."]
    if request.order_type != "LIMIT":
        return ["Historical stock pilot route only supports LIMIT submit."]
    if request.quantity != Decimal("1"):
        return ["Historical stock pilot route only supports quantity 1."]
    intent_type = (request.intent_type or "").upper()
    side = request.side.upper()
    if intent_type == "MANUAL_LIVE_PILOT":
        if side != "BUY":
            return ["Historical stock pilot open route only supports BUY_TO_OPEN."]
        return []
    if intent_type == "FLATTEN":
        broker_side = str(broker_position.get("side") or "").strip().upper()
        broker_quantity = _decimal(broker_position.get("quantity")) or Decimal("0")
        blockers: list[str] = []
        if side != "SELL":
            blockers.append("Historical stock pilot close route only supports SELL_TO_CLOSE.")
        if broker_side != "LONG" or broker_quantity != Decimal("1"):
            blockers.append("Historical stock pilot close route requires an existing LONG 1 broker position.")
        return blockers
    return ["Historical stock pilot route only supports MANUAL_LIVE_PILOT BUY_TO_OPEN and FLATTEN SELL_TO_CLOSE."]


def _manual_live_fill_summary(row: dict[str, Any]) -> dict[str, Any]:
    latest_events = [as_dict(event) for event in as_list(row.get("latest_events"))]
    fill_event = _latest_event_with_status(latest_events, statuses={"FILLED", "PARTIALLY_FILLED"}, source="schwab_sync") or _latest_event_with_status(
        latest_events,
        statuses={"FILLED", "PARTIALLY_FILLED"},
        source="schwab_direct_status",
    )
    response = as_dict(fill_event.get("response")) if fill_event else {}
    activities = [as_dict(item) for item in as_list(response.get("orderActivityCollection"))]
    execution_legs = [as_dict(item) for item in as_list(as_dict(activities[0]).get("executionLegs"))] if activities else []
    execution_leg = execution_legs[0] if execution_legs else {}
    return {
        "fill_timestamp": execution_leg.get("time") or row.get("filled_at"),
        "fill_price": execution_leg.get("price"),
    }


def _latest_manual_live_passive_refresh_proof(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    for event in events:
        payload = as_dict(event.get("payload"))
        for key in ("refresh_restart_proof", "restart_refresh_proof", "buy_refresh_restart_proof"):
            proof = as_dict(payload.get(key))
            if proof and proof.get("passive_refresh_held") is not None:
                return {
                    "scenario_type": event.get("scenario_type"),
                    "occurred_at": event.get("occurred_at"),
                    **proof,
                }
    return None


def _order_type_live_verification_bundle(features: Any) -> dict[str, Any]:
    verified_keys = _live_verified_order_key_set(features)
    by_asset_class: dict[str, dict[str, Any]] = {"STOCK": {}, "FUTURE": {}, "ADVANCED": {}}
    sequence_rows: list[dict[str, Any]] = []
    next_step: dict[str, Any] | None = None

    for entry in _ORDER_TYPE_LIVE_VERIFICATION_SEQUENCE:
        key = str(entry["key"])
        previewable, preview_reasons = _verification_previewable(features, entry)
        gate_reasons = _verification_live_gate_reasons(features, entry)
        sequence_blocker = _verification_sequence_prerequisite_blocker(key, verified_keys=verified_keys)
        if key == _futures_pilot_verification_key() and features.futures_pilot_enabled:
            sequence_blocker = None
        combined_reasons = [*preview_reasons, *gate_reasons]
        if sequence_blocker and key not in verified_keys:
            combined_reasons.append(sequence_blocker)
        blocker_reason = " | ".join(dict.fromkeys(reason for reason in combined_reasons if reason))
        live_verified = key in verified_keys
        live_enabled = previewable and len(gate_reasons) == 0 and sequence_blocker is None and not blocker_reason
        status_row = {
            "step": int(entry["step"]),
            "verification_key": key,
            "label": str(entry["label"]),
            "asset_class": str(entry["asset_class"]),
            "order_type": str(entry["order_type"]),
            "modeled_in_ticket": True,
            "previewable": previewable,
            "live_enabled": live_enabled,
            "live_verified": live_verified,
            "blocked": not live_enabled,
            "blocker_reason": blocker_reason or None,
        }
        by_asset_class[str(entry["asset_class"])][str(entry["order_type"])] = status_row
        sequence_rows.append(status_row)
        if next_step is None and not live_verified:
            next_step = status_row
    if features.futures_pilot_enabled:
        active_row = next((row for row in sequence_rows if str(row.get("verification_key")) == _futures_pilot_verification_key()), None)
        if active_row is not None:
            next_step = active_row

    return {
        "by_asset_class": by_asset_class,
        "sequence": sequence_rows,
        "next_step": next_step,
        "live_verified_order_keys": sorted(verified_keys),
        "runbooks": _NEAR_TERM_LIVE_VERIFICATION_RUNBOOKS,
    }


def _capabilities_snapshot(features: Any) -> dict[str, Any]:
    verification = _order_type_live_verification_bundle(features)
    historical_stock_policy = _manual_live_pilot_policy_snapshot(features)
    active_policy = _current_active_manual_live_policy_snapshot(features)
    futures_policy = _futures_pilot_policy_snapshot_from_features(features)
    return {
        "manual_live_pilot": bool(features.manual_live_pilot_enabled),
        "futures_pilot": bool(features.futures_pilot_enabled),
        "manual_order_submit": bool(features.manual_order_ticket_enabled and features.live_order_submit_enabled),
        "manual_order_cancel": bool(features.manual_order_ticket_enabled and features.live_order_submit_enabled),
        "manual_order_replace": bool(features.manual_order_ticket_enabled and features.live_order_submit_enabled and features.replace_order_enabled),
        "manual_order_preview": bool(features.manual_order_ticket_enabled),
        "sell_short": bool(features.sell_short_enabled),
        "supported_manual_asset_classes": (
            list(features.futures_supported_asset_classes)
            if _current_active_manual_lane_is_futures(features)
            else list(features.supported_manual_asset_classes)
        ),
        "supported_manual_order_types": (
            list(features.futures_supported_order_types)
            if _current_active_manual_lane_is_futures(features)
            else list(features.supported_manual_order_types)
        ),
        "supported_manual_dry_run_order_types": list(features.supported_manual_dry_run_order_types),
        "supported_manual_time_in_force_values": (
            list(features.futures_supported_time_in_force_values)
            if _current_active_manual_lane_is_futures(features)
            else list(features.supported_manual_time_in_force_values)
        ),
        "supported_manual_session_values": (
            list(features.futures_supported_session_values)
            if _current_active_manual_lane_is_futures(features)
            else list(features.supported_manual_session_values)
        ),
        "live_verified_order_keys": verification["live_verified_order_keys"],
        "advanced_tif_ticket_support": bool(features.advanced_tif_enabled and features.ext_exto_ticket_support_enabled),
        "oco_ticket_support": bool(features.oco_ticket_support_enabled),
        "advanced_payload_preview": bool(
            features.manual_order_ticket_enabled
            and (features.advanced_tif_enabled or features.ext_exto_ticket_support_enabled or features.oco_ticket_support_enabled)
        ),
        "stock_market_live_submit": bool(features.stock_market_live_submit_enabled),
        "stock_limit_live_submit": bool(features.stock_limit_live_submit_enabled),
        "stock_stop_live_submit": bool(features.stock_stop_live_submit_enabled),
        "stock_stop_limit_live_submit": bool(features.stock_stop_limit_live_submit_enabled),
        "ext_exto_live_submit": bool(features.ext_exto_live_submit_enabled),
        "oco_live_submit": bool(features.oco_live_submit_enabled),
        "trailing_live_submit": bool(features.trailing_live_submit_enabled),
        "close_order_live_submit": bool(features.close_order_live_submit_enabled),
        "futures_live_submit": bool(features.futures_live_submit_enabled),
        "order_type_matrix_by_asset_class": {
            asset_class: _supported_dry_run_order_types_for_asset(features, asset_class)
            for asset_class in ("STOCK", "FUTURE")
        },
        "live_enabled_order_types_by_asset_class": {
            asset_class: _live_enabled_order_types_for_asset(features, asset_class)
            for asset_class in ("STOCK", "FUTURE")
        },
        "dry_run_only_order_types_by_asset_class": {
            asset_class: _dry_run_only_order_types_for_asset(features, asset_class)
            for asset_class in ("STOCK", "FUTURE")
        },
        "order_type_live_verification_matrix": verification["by_asset_class"],
        "order_type_live_verification_sequence": verification["sequence"],
        "next_live_verification_step": verification["next_step"],
        "near_term_live_verification_runbooks": verification["runbooks"],
        "manual_live_pilot_scope": active_policy,
        "historical_stock_pilot_scope": historical_stock_policy,
        "futures_pilot_scope": futures_policy,
    }


def _supported_dry_run_order_types_for_asset(features: Any, asset_class: str) -> list[str]:
    matrix = set(_manual_order_type_matrix_for_asset(asset_class))
    configured = set(features.supported_manual_dry_run_order_types)
    return [order_type for order_type in _manual_order_type_matrix_for_asset(asset_class) if order_type in matrix and order_type in configured]


def _live_enabled_order_types_for_asset(features: Any, asset_class: str) -> list[str]:
    normalized_asset = _normalize_asset_class(asset_class)
    matrix = as_dict(_order_type_live_verification_bundle(features)["by_asset_class"]).get(normalized_asset)
    rows = as_dict(matrix)
    return [
        order_type
        for order_type in _supported_dry_run_order_types_for_asset(features, normalized_asset)
        if as_dict(rows.get(order_type)).get("live_enabled") is True
    ]


def _dry_run_only_order_types_for_asset(features: Any, asset_class: str) -> list[str]:
    dry_run = _supported_dry_run_order_types_for_asset(features, asset_class)
    live_enabled = set(_live_enabled_order_types_for_asset(features, asset_class))
    return [order_type for order_type in dry_run if order_type not in live_enabled]


def _live_order_type_blockers(
    *,
    asset_class: str,
    order_type: str,
    features: Any,
    include_type_disabled: bool = True,
) -> list[str]:
    normalized_asset = _normalize_asset_class(asset_class)
    matrix = as_dict(_order_type_live_verification_bundle(features)["by_asset_class"]).get(normalized_asset)
    status_row = as_dict(as_dict(matrix).get(order_type))
    blocker_reason = str(status_row.get("blocker_reason") or "").strip()
    if status_row:
        if include_type_disabled or order_type in set(features.supported_manual_order_types):
            return [blocker_reason] if blocker_reason else []
        return []
    return [f"Order type {order_type} is not modeled for asset class {normalized_asset}."]


def _manual_order_structure_summary(request: ManualOrderRequest) -> dict[str, Any]:
    if request.structure_type == "OCO":
        return {
            "structure_type": "OCO",
            "symbol": request.symbol,
            "asset_class": request.asset_class,
            "intent_type": request.intent_type,
            "operator_note": request.operator_note,
            "time_in_force": request.time_in_force,
            "session": request.session,
            "advanced_mode": _advanced_mode_label(request),
            "oco_group_id": request.oco_group_id,
            "relationship": "Cancel remaining leg when one leg fills.",
            "legs": [
                {
                    "leg_label": leg.leg_label,
                    "side": leg.side,
                    "quantity": str(leg.quantity),
                    "order_type": leg.order_type,
                    "limit_price": str(leg.limit_price) if leg.limit_price is not None else None,
                    "stop_price": str(leg.stop_price) if leg.stop_price is not None else None,
                    "trail_value_type": leg.trail_value_type,
                    "trail_value": str(leg.trail_value) if leg.trail_value is not None else None,
                    "trail_trigger_basis": leg.trail_trigger_basis,
                    "trail_limit_offset": str(leg.trail_limit_offset) if leg.trail_limit_offset is not None else None,
                }
                for leg in request.oco_legs
            ],
        }
    return {
        "structure_type": "SINGLE",
        "symbol": request.symbol,
        "asset_class": request.asset_class,
        "intent_type": request.intent_type,
        "operator_note": request.operator_note,
        "side": request.side,
        "quantity": str(request.quantity),
        "order_type": request.order_type,
        "time_in_force": request.time_in_force,
        "session": request.session,
        "advanced_mode": _advanced_mode_label(request),
        "limit_price": str(request.limit_price) if request.limit_price is not None else None,
        "stop_price": str(request.stop_price) if request.stop_price is not None else None,
        "trail_value_type": request.trail_value_type,
        "trail_value": str(request.trail_value) if request.trail_value is not None else None,
        "trail_trigger_basis": request.trail_trigger_basis,
        "trail_limit_offset": str(request.trail_limit_offset) if request.trail_limit_offset is not None else None,
    }


def _manual_order_instruction_conflicts(*, requested_side: str, live_instruction: str, broker_side: str) -> bool:
    requested = str(requested_side or "").strip().upper()
    live = str(live_instruction or "").strip().upper()
    broker = str(broker_side or "").strip().upper()
    if not requested or not live:
        return False
    if requested == live:
        return False
    if broker == "LONG" and requested == "SELL" and live == "BUY":
        return False
    if broker == "SHORT" and requested == "BUY_TO_COVER" and live == "SELL_SHORT":
        return False
    return True


def _manual_live_order_is_unsafe_ambiguity(*, requested_side: str, broker_position: dict[str, Any]) -> bool:
    broker_side = str(as_dict(broker_position).get("side") or "").strip().upper()
    broker_quantity = _decimal(as_dict(broker_position).get("quantity")) or Decimal("0")
    requested = str(requested_side or "").strip().upper()
    if broker_quantity <= 0 or not broker_side or not requested:
        return False
    if broker_side == "LONG" and requested in {"SELL_SHORT", "BUY_TO_COVER"}:
        return True
    if broker_side == "SHORT" and requested == "BUY":
        return True
    return False


def _advanced_unverified_fields(request: ManualOrderRequest) -> list[str]:
    warnings: list[str] = []
    advanced_mode = _advanced_mode_label(request)
    if advanced_mode in {"EXT", "EXTO", "GTC_EXTO"}:
        warnings.append("Session/duration mapping for EXT / EXTO / GTC_EXTO is review-only and not yet live-verified with Schwab.")
    if request.structure_type == "OCO":
        warnings.append("OCO payload structure is review-only and not yet live-verified with Schwab.")
    order_types = [request.order_type] if request.structure_type == "SINGLE" else [leg.order_type for leg in request.oco_legs]
    if any(order_type in {"TRAIL_STOP", "TRAIL_STOP_LIMIT"} for order_type in order_types):
        warnings.append("Trailing order payload fields are modeled for review but not yet live-verified with Schwab.")
    if any(order_type in {"MARKET_ON_CLOSE", "LIMIT_ON_CLOSE"} for order_type in order_types):
        warnings.append("Market-on-close and limit-on-close payload semantics are modeled for review but not yet live-verified with Schwab.")
    if request.asset_class == "FUTURE":
        warnings.append("Futures order payload mapping is modeled for review but remains unverified for live submission in this phase.")
    return warnings


def _manual_order_request_json(request: ManualOrderRequest) -> dict[str, Any]:
    return {
        "account_hash": request.account_hash,
        "symbol": request.symbol,
        "asset_class": request.asset_class,
        "structure_type": request.structure_type,
        "intent_type": request.intent_type,
        "side": request.side,
        "quantity": str(request.quantity),
        "order_type": request.order_type,
        "limit_price": str(request.limit_price) if request.limit_price is not None else None,
        "stop_price": str(request.stop_price) if request.stop_price is not None else None,
        "trail_value_type": request.trail_value_type,
        "trail_value": str(request.trail_value) if request.trail_value is not None else None,
        "trail_trigger_basis": request.trail_trigger_basis,
        "trail_limit_offset": str(request.trail_limit_offset) if request.trail_limit_offset is not None else None,
        "time_in_force": request.time_in_force,
        "session": request.session,
        "review_confirmed": request.review_confirmed,
        "operator_note": request.operator_note,
        "client_order_id": request.client_order_id,
        "broker_account_number": request.broker_account_number,
        "operator_authenticated": request.operator_authenticated,
        "operator_reduce_only_authorized": request.operator_reduce_only_authorized,
        "operator_auth_policy": request.operator_auth_policy,
        "operator_auth_risk_bucket": request.operator_auth_risk_bucket,
        "local_operator_identity": request.local_operator_identity,
        "auth_session_id": request.auth_session_id,
        "auth_method": request.auth_method,
        "authenticated_at": request.authenticated_at,
        "oco_group_id": request.oco_group_id,
        "oco_legs": [
            {
                "leg_label": leg.leg_label,
                "side": leg.side,
                "quantity": str(leg.quantity),
                "order_type": leg.order_type,
                "limit_price": str(leg.limit_price) if leg.limit_price is not None else None,
                "stop_price": str(leg.stop_price) if leg.stop_price is not None else None,
                "trail_value_type": leg.trail_value_type,
                "trail_value": str(leg.trail_value) if leg.trail_value is not None else None,
                "trail_trigger_basis": leg.trail_trigger_basis,
                "trail_limit_offset": str(leg.trail_limit_offset) if leg.trail_limit_offset is not None else None,
            }
            for leg in request.oco_legs
        ],
    }


def _schwab_asset_type(asset_class: str) -> str:
    normalized = _normalize_asset_class(asset_class)
    return {
        "STOCK": "EQUITY",
        "OPTION": "OPTION",
        "FUTURE": "FUTURE",
        "BOND": "FIXED_INCOME",
        "CASH": "CASH_EQUIVALENT",
    }.get(normalized, normalized)


def _normalize_asset_class(raw_value: Any) -> str:
    text = str(raw_value or "UNKNOWN").strip().upper()
    return {
        "EQUITY": "STOCK",
        "STOCK": "STOCK",
        "OPTION": "OPTION",
        "OPTIONS": "OPTION",
        "FUTURE": "FUTURE",
        "FUTURES": "FUTURE",
        "FIXED_INCOME": "BOND",
        "BOND": "BOND",
        "CASH_EQUIVALENT": "CASH",
        "CASH": "CASH",
    }.get(text, text)


def _first_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict):
            return value
    return {}


def _position_open_pnl(row: dict[str, Any]) -> Decimal | None:
    explicit = _decimal(row.get("profitLoss") or row.get("longOpenProfitLoss") or row.get("shortOpenProfitLoss"))
    if explicit is not None:
        return explicit
    market_value = _decimal(row.get("marketValue"))
    average_cost = _decimal(row.get("averagePrice") or row.get("averageLongPrice") or row.get("averageShortPrice"))
    quantity = _decimal(row.get("longQuantity") or row.get("shortQuantity") or row.get("quantity"))
    if market_value is None or average_cost is None or quantity is None:
        return None
    return market_value - (average_cost * abs(quantity))


def _order_record_json(order: BrokerOrderRecord) -> dict[str, Any]:
    return {
        "broker_order_id": order.broker_order_id,
        "account_hash": order.account_hash,
        "client_order_id": order.client_order_id,
        "symbol": order.symbol,
        "description": order.description,
        "asset_class": order.asset_class,
        "instruction": order.instruction,
        "quantity": str(order.quantity),
        "filled_quantity": str(order.filled_quantity) if order.filled_quantity is not None else None,
        "order_type": order.order_type,
        "duration": order.duration,
        "session": order.session,
        "status": order.status,
        "entered_at": order.entered_at.isoformat() if order.entered_at else None,
        "closed_at": order.closed_at.isoformat() if order.closed_at else None,
        "updated_at": order.updated_at.isoformat(),
        "limit_price": str(order.limit_price) if order.limit_price is not None else None,
        "stop_price": str(order.stop_price) if order.stop_price is not None else None,
        "source": order.source,
    }


def _broker_order_record_payload(order: BrokerOrderRecord) -> dict[str, Any]:
    return {
        "broker_order_id": order.broker_order_id,
        "account_hash": order.account_hash,
        "client_order_id": order.client_order_id,
        "symbol": order.symbol,
        "description": order.description,
        "asset_class": order.asset_class,
        "instruction": order.instruction,
        "quantity": str(order.quantity),
        "filled_quantity": str(order.filled_quantity) if order.filled_quantity is not None else None,
        "order_type": order.order_type,
        "duration": order.duration,
        "session": order.session,
        "status": order.status,
        "entered_at": order.entered_at.isoformat() if order.entered_at else None,
        "closed_at": order.closed_at.isoformat() if order.closed_at else None,
        "updated_at": order.updated_at.isoformat(),
        "limit_price": str(order.limit_price) if order.limit_price is not None else None,
        "stop_price": str(order.stop_price) if order.stop_price is not None else None,
        "source": order.source,
        "raw_payload": order.raw_payload,
    }


def _position_index_from_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], Decimal]:
    index: dict[tuple[str, str, str], Decimal] = {}
    for row in rows:
        key = (
            str(row.get("symbol") or "").strip().upper(),
            str(row.get("asset_class") or "").strip().upper(),
            str(row.get("side") or "").strip().upper(),
        )
        if not key[0]:
            continue
        quantity = _decimal(row.get("quantity")) or Decimal("0")
        index[key] = quantity
    return index


def _position_index_from_records(rows: list[BrokerPositionSnapshot]) -> dict[tuple[str, str, str], Decimal]:
    return {
        (row.symbol.strip().upper(), row.asset_class.strip().upper(), row.side.strip().upper()): row.quantity
        for row in rows
        if row.symbol.strip()
    }


def _position_mismatches(
    persisted_index: dict[tuple[str, str, str], Decimal],
    live_index: dict[tuple[str, str, str], Decimal],
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    keys = sorted(set(persisted_index) | set(live_index))
    for key in keys:
        persisted_quantity = persisted_index.get(key)
        live_quantity = live_index.get(key)
        if persisted_quantity == live_quantity:
            continue
        symbol, asset_class, side = key
        mismatches.append(
            {
                "symbol": symbol,
                "asset_class": asset_class,
                "side": side,
                "persisted_quantity": str(persisted_quantity) if persisted_quantity is not None else None,
                "live_quantity": str(live_quantity) if live_quantity is not None else None,
            }
        )
    return mismatches


def _build_live_freshness_summary(
    *,
    now: datetime,
    last_refresh_summary: dict[str, Any],
    max_age_seconds: int,
    quote_rows: list[Any],
) -> dict[str, Any]:
    quotes_delayed = any(as_dict(row).get("delayed") is True for row in quote_rows)
    quote_count = len(quote_rows)
    return {
        "balances": _freshness_entry(
            state=_timestamp_freshness_state(last_refresh_summary.get("balances_refresh_at"), now=now, max_age_seconds=max_age_seconds),
            updated_at=last_refresh_summary.get("balances_refresh_at"),
            detail="Broker balances are being refreshed from the live Schwab account endpoint.",
            source="schwab_trader_accounts",
            transport="polling",
        ),
        "positions": _freshness_entry(
            state=_timestamp_freshness_state(last_refresh_summary.get("positions_refresh_at"), now=now, max_age_seconds=max_age_seconds),
            updated_at=last_refresh_summary.get("positions_refresh_at"),
            detail="Broker positions are being refreshed from the live Schwab account endpoint.",
            source="schwab_trader_accounts",
            transport="polling",
        ),
        "quotes": _freshness_entry(
            state=_quote_freshness_state(
                updated_at=last_refresh_summary.get("quotes_refresh_at"),
                now=now,
                max_age_seconds=max_age_seconds,
                delayed=quotes_delayed,
                quote_count=quote_count,
                had_error=bool(last_refresh_summary.get("quote_error")),
            ),
            updated_at=last_refresh_summary.get("quotes_refresh_at"),
            detail=(
                f"Quote overlay refreshed for {quote_count} broker-held symbol{'s' if quote_count != 1 else ''}."
                if quote_count
                else "No broker-held symbols currently require a quote overlay."
            )
            if not last_refresh_summary.get("quote_error")
            else f"Quote overlay unavailable: {last_refresh_summary.get('quote_error')}",
            source="schwab_quotes",
            transport="polling",
        ),
        "orders": _freshness_entry(
            state=_timestamp_freshness_state(last_refresh_summary.get("orders_refresh_at"), now=now, max_age_seconds=max_age_seconds),
            updated_at=last_refresh_summary.get("orders_refresh_at"),
            detail="Broker orders are being refreshed from the live Schwab trader order endpoint.",
            source="schwab_trader_orders",
            transport="polling",
        ),
        "fills": _freshness_entry(
            state=_timestamp_freshness_state(last_refresh_summary.get("orders_refresh_at"), now=now, max_age_seconds=max_age_seconds),
            updated_at=last_refresh_summary.get("orders_refresh_at"),
            detail="Recent broker fills/executions are being refreshed from the live Schwab trader order endpoint.",
            source="schwab_trader_orders",
            transport="polling",
        ),
    }


def _build_snapshot_freshness_summary(
    *,
    now: datetime,
    runtime_state: dict[str, Any],
    max_age_seconds: int,
    source_label: str,
) -> dict[str, Any]:
    last_refresh_summary = as_dict(runtime_state.get("last_refresh_summary"))
    return {
        "balances": _snapshot_freshness_entry(
            updated_at=last_refresh_summary.get("balances_refresh_at"),
            now=now,
            max_age_seconds=max_age_seconds,
            detail="Using persisted broker balances snapshot.",
            source=source_label,
        ),
        "positions": _snapshot_freshness_entry(
            updated_at=last_refresh_summary.get("positions_refresh_at"),
            now=now,
            max_age_seconds=max_age_seconds,
            detail="Using persisted broker positions snapshot.",
            source=source_label,
        ),
        "quotes": _snapshot_freshness_entry(
            updated_at=last_refresh_summary.get("quotes_refresh_at"),
            now=now,
            max_age_seconds=max_age_seconds,
            detail=(
                f"Using persisted broker quote snapshot. Last quote error: {last_refresh_summary.get('quote_error')}."
                if last_refresh_summary.get("quote_error")
                else "Using persisted broker quote snapshot."
            ),
            source=source_label,
        ),
        "orders": _snapshot_freshness_entry(
            updated_at=last_refresh_summary.get("orders_refresh_at"),
            now=now,
            max_age_seconds=max_age_seconds,
            detail="Using persisted broker orders snapshot.",
            source=source_label,
        ),
        "fills": _snapshot_freshness_entry(
            updated_at=last_refresh_summary.get("orders_refresh_at"),
            now=now,
            max_age_seconds=max_age_seconds,
            detail="Using persisted broker fills/executions snapshot.",
            source=source_label,
        ),
    }


def _freshness_entry(*, state: str, updated_at: Any, detail: str, source: str, transport: str) -> dict[str, Any]:
    return {
        "state": state,
        "updated_at": updated_at,
        "detail": detail,
        "source": source,
        "transport": transport,
        "label": _freshness_label(state, transport=transport),
        "ok": state in {"LIVE", "DELAYED"},
    }


def _snapshot_freshness_entry(*, updated_at: Any, now: datetime, max_age_seconds: int, detail: str, source: str) -> dict[str, Any]:
    state = _snapshot_or_stale_state(updated_at, now=now, max_age_seconds=max_age_seconds)
    return _freshness_entry(
        state=state,
        updated_at=updated_at,
        detail=detail,
        source=source,
        transport="snapshot",
    )


def _health_from_freshness(
    entry: Any,
    *,
    fallback_label: str | None = None,
    fallback_detail: str | None = None,
) -> dict[str, Any]:
    record = as_dict(entry)
    if not record:
        return {
            "ok": False,
            "label": fallback_label or "UNAVAILABLE",
            "detail": fallback_detail or "No freshness record is available.",
        }
    state = str(record.get("state") or "STALE").upper()
    transport = str(record.get("transport") or "").strip()
    return {
        "ok": state in {"LIVE", "DELAYED"},
        "label": _health_label_from_state(state, transport=transport),
        "detail": str(record.get("detail") or fallback_detail or "").strip() or "No detail available.",
    }


def _timestamp_freshness_state(raw_value: Any, *, now: datetime, max_age_seconds: int) -> str:
    age = _age_seconds(raw_value, now=now)
    if age is None:
        return "STALE"
    if age > max_age_seconds:
        return "STALE"
    return "LIVE"


def _quote_freshness_state(
    *,
    updated_at: Any,
    now: datetime,
    max_age_seconds: int,
    delayed: bool,
    quote_count: int,
    had_error: bool,
) -> str:
    if had_error:
        return "STALE"
    if quote_count <= 0:
        return "LIVE"
    base = _timestamp_freshness_state(updated_at, now=now, max_age_seconds=max_age_seconds)
    if base != "LIVE":
        return base
    return "DELAYED" if delayed else "LIVE"


def _snapshot_or_stale_state(raw_value: Any, *, now: datetime, max_age_seconds: int) -> str:
    age = _age_seconds(raw_value, now=now)
    if age is None:
        return "SNAPSHOT"
    if age > max_age_seconds:
        return "STALE"
    return "SNAPSHOT"


def _freshness_label(state: str, *, transport: str) -> str:
    if state == "LIVE" and transport == "polling":
        return "LIVE / POLLING"
    if state == "DELAYED":
        return "DELAYED / POLLING"
    return state


def _health_label_from_state(state: str, *, transport: str) -> str:
    if state == "LIVE":
        return "LIVE" if transport != "polling" else "LIVE / POLLING"
    if state == "DELAYED":
        return "DELAYED / POLLING"
    return state


def _age_seconds(raw_value: Any, *, now: datetime) -> int | None:
    parsed = _iso_datetime(raw_value)
    if parsed is None:
        return None
    return max(0, int((now - parsed).total_seconds()))


def _is_us_regular_hours(now: datetime) -> bool:
    eastern = now.astimezone(ZoneInfo("America/New_York"))
    if eastern.weekday() >= 5:
        return False
    minutes = eastern.hour * 60 + eastern.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


def _decimal(raw_value: Any) -> Decimal | None:
    if raw_value in (None, ""):
        return None
    try:
        return Decimal(str(raw_value))
    except (InvalidOperation, ValueError):
        return None


def _iso_datetime(raw_value: Any) -> datetime | None:
    if raw_value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _first_decimal_with_source(*candidates: tuple[str, Any]) -> tuple[Decimal | None, str | None]:
    for source, value in candidates:
        decimal_value = _decimal(value)
        if decimal_value is not None:
            return decimal_value, source
    return None, None


def _resolve_quote_payload(payload: dict[str, Any], external_symbol: str) -> dict[str, Any] | None:
    if external_symbol in payload and isinstance(payload[external_symbol], dict):
        return payload[external_symbol]

    candidate_keys = _quote_symbol_aliases(external_symbol)
    for candidate in candidate_keys:
        resolved = payload.get(candidate)
        if isinstance(resolved, dict):
            return resolved

    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        reference = value.get("reference")
        if not isinstance(reference, dict):
            reference = {}
        reference_symbol = reference.get("symbol")
        reference_product = reference.get("product")
        payload_symbol = value.get("symbol")
        if isinstance(reference_symbol, str) and reference_symbol in candidate_keys:
            return value
        if isinstance(reference_product, str) and reference_product in candidate_keys:
            return value
        if isinstance(payload_symbol, str) and payload_symbol in candidate_keys:
            return value
        if isinstance(key, str) and key in candidate_keys:
            return value
    return None


def _quote_symbol_aliases(external_symbol: str) -> set[str]:
    aliases = {
        external_symbol,
        external_symbol.upper(),
        external_symbol.lower(),
    }
    stripped = external_symbol.lstrip("/")
    if stripped:
        aliases.add(stripped)
        aliases.add(stripped.upper())
        aliases.add(stripped.lower())
        aliases.add(f"/{stripped}")
        aliases.add(f"/{stripped.upper()}")
        aliases.add(f"/{stripped.lower()}")
    if external_symbol.startswith("$"):
        aliases.add(external_symbol[1:])
    else:
        aliases.add(f"${external_symbol}")
    aliases.add(external_symbol.replace("/", ""))
    aliases.add(external_symbol.replace("/", "").upper())
    aliases.add(external_symbol.replace("/", "").lower())
    return {alias for alias in aliases if alias}


def _quote_delay_flag(payload: dict[str, Any], quote: dict[str, Any]) -> bool | None:
    for value in (
        quote.get("delayed"),
        quote.get("isDelayed"),
        payload.get("delayed"),
        payload.get("isDelayed"),
    ):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "1", "delayed"}:
                return True
            if normalized in {"false", "no", "0", "live"}:
                return False
    return None


def _quote_timestamp(payload: dict[str, Any], quote: dict[str, Any]) -> datetime | None:
    for raw_value in (
        quote.get("quoteTime"),
        quote.get("tradeTime"),
        quote.get("regularMarketTradeTime"),
        quote.get("quoteTimeInLong"),
        quote.get("tradeTimeInLong"),
        payload.get("quoteTime"),
    ):
        if raw_value in (None, ""):
            continue
        if isinstance(raw_value, (int, float)) or (isinstance(raw_value, str) and str(raw_value).isdigit()):
            try:
                return datetime.fromtimestamp(int(raw_value) / 1000.0, tz=timezone.utc)
            except (OverflowError, ValueError):
                continue
        parsed = _iso_datetime(raw_value)
        if parsed is not None:
            return parsed
    return None


def _marketable_futures_limit_price_from_quote_payload(
    payload: dict[str, Any],
    *,
    side: str,
) -> tuple[Decimal | None, str | None]:
    quote = as_dict(payload.get("quote"))
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        candidates = (
            (quote.get("askPrice"), "askPrice"),
            (quote.get("lastPrice"), "lastPrice"),
            (quote.get("mark"), "mark"),
            (quote.get("closePrice"), "closePrice"),
        )
    else:
        candidates = (
            (quote.get("bidPrice"), "bidPrice"),
            (quote.get("lastPrice"), "lastPrice"),
            (quote.get("mark"), "mark"),
            (quote.get("closePrice"), "closePrice"),
        )
    for raw_value, source in candidates:
        decimal_value = _decimal(raw_value)
        if decimal_value is not None and decimal_value > 0:
            return decimal_value, source
    return None, None


def _manual_order_is_reduce_only(request: ManualOrderRequest) -> bool:
    return str(request.intent_type or "").strip().upper() == "FLATTEN"


def _manual_order_has_live_auth(request: ManualOrderRequest) -> bool:
    if request.operator_authenticated:
        return True
    return bool(request.operator_reduce_only_authorized and _manual_order_is_reduce_only(request))


def _production_link_current_live_blockers(snapshot: dict[str, Any]) -> list[str]:
    manual_order_safety = as_dict(snapshot.get("manual_order_safety"))
    local_operator_auth = as_dict(snapshot.get("local_operator_auth"))
    blockers = [str(item).strip() for item in as_list(manual_order_safety.get("blockers")) if str(item).strip()]
    operator_auth_blocker = str(local_operator_auth.get("blocker") or "").strip()
    if operator_auth_blocker:
        blockers.append(operator_auth_blocker)
    return list(dict.fromkeys(blockers))


def _production_link_first_live_verification_route(snapshot: dict[str, Any]) -> dict[str, Any]:
    feature_flags = SimpleNamespace(**as_dict(snapshot.get("feature_flags")))
    if _current_active_manual_lane_is_futures(feature_flags):
        return as_dict(_futures_pilot_status_export_payload(snapshot).get("next_live_verification_step"))
    manual_order_safety = as_dict(snapshot.get("manual_order_safety"))
    constraints = as_dict(manual_order_safety.get("constraints"))
    locked_policy = (
        as_dict(as_dict(manual_order_safety.get("pilot_readiness")).get("locked_policy"))
        or as_dict(as_dict(manual_order_safety.get("pilot_mode")).get("scope"))
    )
    next_step = as_dict(constraints.get("next_live_verification_step"))
    stock_limit_runbook = as_dict(as_dict(constraints.get("near_term_live_verification_runbooks")).get("STOCK:LIMIT"))
    blockers = _production_link_current_live_blockers(snapshot)
    live_submit_eligible_now = len(blockers) == 0
    blocked_reason = blockers[0] if blockers else str(next_step.get("blocker_reason") or "").strip() or None
    prerequisites = [
        {
            "label": "Local operator auth ready",
            "ok": as_dict(snapshot.get("local_operator_auth")).get("ready") is True,
            "detail": as_dict(snapshot.get("local_operator_auth")).get("detail"),
        },
        {
            "label": "Selected Schwab account live-verified",
            "ok": bool(manual_order_safety.get("selected_account_live_verified")),
            "detail": as_dict(as_dict(snapshot.get("health")).get("account_selected")).get("detail"),
        },
        {
            "label": "Broker reachable",
            "ok": as_dict(as_dict(snapshot.get("health")).get("broker_reachable")).get("ok") is True,
            "detail": as_dict(as_dict(snapshot.get("health")).get("broker_reachable")).get("detail"),
        },
        {
            "label": "Auth healthy",
            "ok": as_dict(as_dict(snapshot.get("health")).get("auth_healthy")).get("ok") is True,
            "detail": as_dict(as_dict(snapshot.get("health")).get("auth_healthy")).get("detail"),
        },
        {
            "label": "Balances freshness in bounds",
            "ok": as_dict(as_dict(snapshot.get("health")).get("balances_fresh")).get("ok") is True,
            "detail": as_dict(as_dict(snapshot.get("health")).get("balances_fresh")).get("detail"),
        },
        {
            "label": "Positions freshness in bounds",
            "ok": as_dict(as_dict(snapshot.get("health")).get("positions_fresh")).get("ok") is True,
            "detail": as_dict(as_dict(snapshot.get("health")).get("positions_fresh")).get("detail"),
        },
        {
            "label": "Orders freshness in bounds",
            "ok": as_dict(as_dict(snapshot.get("health")).get("orders_fresh")).get("ok") is True,
            "detail": as_dict(as_dict(snapshot.get("health")).get("orders_fresh")).get("detail"),
        },
        {
            "label": "Reconciliation CLEAR",
            "ok": str(as_dict(snapshot.get("reconciliation")).get("status") or "").lower() == "clear",
            "detail": as_dict(as_dict(snapshot.get("health")).get("reconciliation_fresh")).get("detail"),
        },
        {
            "label": "Manual symbol whitelist non-empty",
            "ok": bool(as_list(locked_policy.get("symbol_whitelist"))),
            "detail": f"Configured whitelist: {', '.join(str(item) for item in as_list(locked_policy.get('symbol_whitelist'))) or 'none'}",
        },
        {
            "label": "Pilot mode enabled",
            "ok": bool(as_dict(manual_order_safety.get("pilot_mode")).get("enabled")),
            "detail": as_dict(manual_order_safety.get("pilot_mode")).get("detail"),
        },
        {
            "label": "Live submit safety mode enabled",
            "ok": bool(snapshot.get("feature_flags", {}).get("live_order_submit_enabled")),
            "detail": manual_order_safety.get("submit_status_detail"),
        },
        {
            "label": "Same-symbol unresolved broker/manual ambiguity absent",
            "ok": True,
            "detail": "This is checked again at submit time against the requested symbol, broker open orders, and tracked manual live orders.",
        },
    ]
    return {
        "route_key": "MANUAL_LIVE_PILOT_STOCK_LIMIT_BUY",
        "label": "First Live Verification Route",
        "allowed_now": live_submit_eligible_now and str(next_step.get("verification_key") or "STOCK:LIMIT") == "STOCK:LIMIT",
        "blocked_reason": blocked_reason,
        "operator_path": "Positions > Manual Order Ticket",
        "exact_open_shape": {
            "intent_type": "MANUAL_LIVE_PILOT",
            "operator_label": "BUY_TO_OPEN",
            "side": "BUY",
            "asset_class": locked_policy.get("asset_class"),
            "order_type": locked_policy.get("submit_order_type") or locked_policy.get("order_type"),
            "quantity": locked_policy.get("max_quantity"),
            "time_in_force": locked_policy.get("time_in_force"),
            "session": locked_policy.get("session"),
            "regular_hours_only": locked_policy.get("regular_hours_only"),
            "whitelist_only": True,
            "review_confirmed_required": True,
            "omit_client_order_id": bool(locked_policy.get("omit_client_order_id_for_proven_route")),
        },
        "exact_close_shape": {
            "intent_type": "FLATTEN",
            "operator_label": "SELL_TO_CLOSE",
            "side": "SELL",
            "asset_class": locked_policy.get("asset_class"),
            "order_type": locked_policy.get("submit_order_type") or locked_policy.get("order_type"),
            "quantity": locked_policy.get("max_quantity"),
            "time_in_force": locked_policy.get("time_in_force"),
            "session": locked_policy.get("session"),
            "regular_hours_only": locked_policy.get("regular_hours_only"),
            "existing_broker_position_required": "LONG 1",
            "review_confirmed_required": True,
            "omit_client_order_id": bool(locked_policy.get("omit_client_order_id_for_proven_route")),
        },
        "prerequisites": prerequisites,
        "submit_path": as_list(stock_limit_runbook.get("submit_path")),
        "runbook_checks": {
            "expected_broker_response_states": as_list(stock_limit_runbook.get("expected_broker_response_states")),
            "expected_post_submit_checks": as_list(stock_limit_runbook.get("expected_post_submit_checks")),
            "expected_reconciliation_checks": as_list(stock_limit_runbook.get("expected_reconciliation_checks")),
        },
    }


def _production_link_pilot_workflow(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    feature_flags = SimpleNamespace(**as_dict(snapshot.get("feature_flags")))
    if _current_active_manual_lane_is_futures(feature_flags):
        return _futures_pilot_operator_workflow()
    first_route = _production_link_first_live_verification_route(snapshot)
    return [
        {
            "step": "A",
            "label": "Authenticate Now",
            "detail": "Prime the shared local Touch ID session before preview, open, or flatten actions, then reuse it through the active live-pilot window.",
            "proof_surface": "operator_status.local_operator_auth",
        },
        {
            "step": "B",
            "label": "Pre-entry checklist",
            "detail": "Confirm local operator auth is active, selected account is live-verified, broker/auth are healthy, balances/positions/orders freshness are green, reconciliation is CLEAR, and the chosen symbol is in the whitelist.",
            "proof_surface": "operator_status.first_live_verification.prerequisites",
        },
        {
            "step": "C",
            "label": "Set BUY_TO_OPEN ticket",
            "detail": "Build the manual ticket exactly as MANUAL_LIVE_PILOT / BUY / STOCK / LIMIT / quantity 1 / DAY / NORMAL on a whitelisted symbol during regular US market hours.",
            "proof_surface": "operator_status.first_live_verification.exact_open_shape",
        },
        {
            "step": "D",
            "label": "Review / confirm / send",
            "detail": "Use dry-run preview first, verify the payload still matches the locked route, then send only after explicit operator review confirmation.",
            "proof_surface": "proof_surfaces.last_manual_order_preview",
        },
        {
            "step": "E",
            "label": "Broker ack / working / filled truth",
            "detail": "Verify broker acknowledgement and any WORKING or FILLED transition from the tracked manual-live order row and recent broker order events.",
            "proof_surface": "proof_surfaces.last_submitted_manual_order",
        },
        {
            "step": "F",
            "label": "Verify live position",
            "detail": "Refresh broker truth and confirm the live broker position appears on the intended symbol before preparing the close ticket.",
            "proof_surface": "snapshot.portfolio.positions",
        },
        {
            "step": "G",
            "label": "Set SELL_TO_CLOSE ticket",
            "detail": "Build the close ticket exactly as FLATTEN / SELL / STOCK / LIMIT / quantity 1 / DAY / NORMAL, only when the broker shows an existing LONG 1.",
            "proof_surface": "operator_status.first_live_verification.exact_close_shape",
        },
        {
            "step": "H",
            "label": "Review / confirm / send close",
            "detail": "Preview the close payload, confirm it remains on the locked pilot route, then send the close with explicit operator confirmation.",
            "proof_surface": "proof_surfaces.last_manual_order_preview",
        },
        {
            "step": "I",
            "label": "Broker ack / filled / flat confirmed",
            "detail": "Confirm the close order is acknowledged and filled, then verify broker positions and open orders return to flat on that symbol.",
            "proof_surface": "proof_surfaces.last_completed_live_cycle",
        },
        {
            "step": "J",
            "label": "Reconciliation returns CLEAR",
            "detail": "Refresh broker truth and require reconciliation to return CLEAR after the close cycle completes.",
            "proof_surface": "proof_surfaces.reconciliation_after_close",
        },
        {
            "step": "K",
            "label": "Refresh / restart remains passive",
            "detail": "Only passive refresh or passive restart confirmation is allowed after the cycle; there must be no autonomous follow-on submit.",
            "proof_surface": "proof_surfaces.passive_refresh_restart_proof",
        },
    ]


def _pilot_status_export_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    manual_order_safety = as_dict(snapshot.get("manual_order_safety"))
    pilot_readiness = as_dict(manual_order_safety.get("pilot_readiness"))
    pilot_mode = as_dict(manual_order_safety.get("pilot_mode"))
    feature_flags = SimpleNamespace(**as_dict(snapshot.get("feature_flags")))
    active_futures_lane = _current_active_manual_lane_is_futures(feature_flags)
    active_policy = as_dict(pilot_readiness.get("locked_policy")) or as_dict(pilot_mode.get("scope"))
    historical_stock_policy = as_dict(as_dict(pilot_readiness.get("historical_stock_policy"))) or _manual_live_pilot_policy_snapshot(feature_flags)
    pilot_cycle = as_dict(as_dict(snapshot.get("pilot_cycle")).get("last_completed"))
    runtime_state = as_dict(snapshot.get("runtime_state"))
    diagnostics = as_dict(snapshot.get("diagnostics"))
    connection = as_dict(snapshot.get("connection"))
    local_operator_auth = as_dict(snapshot.get("local_operator_auth"))
    current_blockers = _production_link_current_live_blockers(snapshot)
    first_live_verification = _production_link_first_live_verification_route(snapshot)
    live_submit_eligible_now = len(current_blockers) == 0
    last_submitted_row = next(
        (
            as_dict(row)
            for row in as_list(as_dict(snapshot.get("manual_live_orders")).get("recent_rows"))
            if as_dict(row)
        ),
        {},
    )
    last_completed_cycle = (
        pilot_cycle
        or as_dict(runtime_state.get("last_completed_pilot_cycle"))
        or as_dict(diagnostics.get("last_completed_pilot_cycle"))
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": snapshot.get("status"),
        "label": snapshot.get("label"),
        "operator_path": "Positions > Manual Order Ticket",
        "selected_account": {
            "account_hash": connection.get("selected_account_hash"),
            "account_number": connection.get("selected_account_number"),
            "display_name": connection.get("selected_account_display_name"),
            "selection_source": connection.get("selection_source"),
            "live_verified": bool(manual_order_safety.get("selected_account_live_verified")),
        },
        "health": {
            "broker_reachable": as_dict(as_dict(snapshot.get("health")).get("broker_reachable")),
            "auth_healthy": as_dict(as_dict(snapshot.get("health")).get("auth_healthy")),
        },
        "freshness": {
            "balances": as_dict(as_dict(snapshot.get("health")).get("balances_fresh")),
            "positions": as_dict(as_dict(snapshot.get("health")).get("positions_fresh")),
            "orders": as_dict(as_dict(snapshot.get("health")).get("orders_fresh")),
            "fills_events": as_dict(as_dict(snapshot.get("health")).get("fills_events_fresh")),
        },
        "reconciliation": as_dict(snapshot.get("reconciliation")),
        "local_operator_auth": local_operator_auth,
        "current_active_lane": "FUTURES" if active_futures_lane else "STOCK_HISTORICAL",
        "manual_live_pilot_enabled": bool(pilot_mode.get("enabled")),
        "live_submit_safety_mode_enabled": bool(as_dict(snapshot.get("feature_flags")).get("live_order_submit_enabled")),
        "live_submit_eligible_now": live_submit_eligible_now,
        "pilot_readiness": {
            "enabled": pilot_readiness.get("enabled"),
            "submit_eligible": live_submit_eligible_now,
            "label": pilot_readiness.get("label"),
            "detail": pilot_readiness.get("detail"),
            "blocked_reason": current_blockers[0] if current_blockers else pilot_readiness.get("blocked_reason"),
            "reconciliation_status": pilot_readiness.get("reconciliation_status"),
            "reconciliation_mismatch_count": pilot_readiness.get("reconciliation_mismatch_count"),
        },
        "locked_pilot_policy": active_policy,
        "allowed_scope": active_policy,
        "historical_stock_pilot": {
            "status": "HISTORICAL_RECORD",
            "policy": historical_stock_policy,
        },
        "current_blockers": current_blockers,
        "warning_list": as_list(manual_order_safety.get("warnings")),
        "first_live_verification": first_live_verification,
        "proof_surfaces": {
            "live_manual_pilot_readiness": {
                "submit_status_label": manual_order_safety.get("submit_status_label"),
                "submit_status_detail": manual_order_safety.get("submit_status_detail"),
                "current_blockers": current_blockers,
                "warning_list": as_list(manual_order_safety.get("warnings")),
            },
            "last_manual_order_preview": as_dict(diagnostics.get("last_manual_order_preview")),
            "last_submitted_manual_order": {
                "request": as_dict(diagnostics.get("last_manual_order_request")),
                "result": as_dict(diagnostics.get("last_manual_order_result")),
                "latest_row": last_submitted_row,
            },
            "last_completed_live_cycle": last_completed_cycle,
            "reconciliation_after_close": as_dict(last_completed_cycle.get("reconciliation_clear_confirmation")),
            "passive_refresh_restart_proof": as_dict(last_completed_cycle.get("passive_refresh_restart_confirmation")),
            "futures_pilot_policy_snapshot": "/api/operator-artifact/production-link-futures-pilot-policy",
            "futures_pilot_status": "/api/operator-artifact/production-link-futures-pilot-status",
        },
        "operator_workflow": _production_link_pilot_workflow(snapshot),
        "futures_pilot_status": _futures_pilot_status_export_payload(snapshot),
        "broader_live_routing": {
            "enabled": False,
            "detail": (
                "Live broker submit remains locked to the narrow current route only: MANUAL_LIVE_FUTURES_PILOT BUY_TO_OPEN "
                "and FLATTEN SELL_TO_CLOSE on FUTURE MARKET quantity 1, DAY, NORMAL, whitelist-controlled symbols only."
                if active_futures_lane
                else "Historical stock pilot support remains locked to MANUAL_LIVE_PILOT BUY_TO_OPEN and FLATTEN SELL_TO_CLOSE on STOCK LIMIT quantity 1, DAY, NORMAL, regular-hours only, whitelist only."
            ),
            "guardrail": (
                "Any request outside the current narrow futures route stays preview-only or is blocked fail-closed by the locked futures pilot route checks."
                if active_futures_lane
                else "Any request outside the historical stock route stays preview-only or is blocked fail-closed by the historical stock pilot route checks."
            ),
        },
        "last_completed_cycle": last_completed_cycle,
    }


def _render_broker_truth_schema_validation_markdown(payload: dict[str, Any]) -> str:
    summary = as_dict(payload.get("summary"))
    validations = as_dict(payload.get("validations"))
    lines = [
        "# Broker Truth Schema Validation",
        "",
        f"- Generated At: `{payload.get('generated_at')}`",
        f"- Result: `{summary.get('result', 'UNKNOWN')}`",
        f"- Overall Classification: `{summary.get('overall_classification', 'UNKNOWN')}`",
        f"- Selected Account Hash: `{payload.get('selected_account_hash') or 'NONE'}`",
        f"- Representative Broker Order Id: `{summary.get('representative_broker_order_id') or 'NONE'}`",
        "",
        "## Components",
    ]
    for key in ("order_status", "open_orders", "position", "account_health"):
        row = as_dict(validations.get(key))
        lines.append(
            f"- `{key}` `{row.get('classification', 'UNKNOWN')}` issues="
            f"`{', '.join(str(item) for item in as_list(row.get('issues'))) or 'none'}`"
        )
    lines.append("")
    lines.append("## Missing Or Ambiguous Fields")
    missing_rows = as_list(summary.get("missing_or_ambiguous_fields"))
    if not missing_rows:
        lines.append("- None")
    else:
        for row in missing_rows:
            current = as_dict(row)
            lines.append(
                f"- `{current.get('schema_name', 'unknown')}` missing_required="
                f"`{', '.join(str(item) for item in as_list(current.get('missing_required_fields'))) or 'none'}` "
                f"missing_optional=`{', '.join(str(item) for item in as_list(current.get('missing_optional_fields'))) or 'none'}` "
                f"issues=`{', '.join(str(item) for item in as_list(current.get('issues'))) or 'none'}`"
            )
    return "\n".join(lines) + "\n"


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=_json_default)


def os_env(key: str) -> str | None:
    import os

    return os.environ.get(key)


ProductionLinkService = SchwabProductionLinkService
