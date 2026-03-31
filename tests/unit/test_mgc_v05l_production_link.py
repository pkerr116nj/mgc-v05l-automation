"""Tests for the isolated Schwab production-link service."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import mgc_v05l.production_link.service as production_link_service
from mgc_v05l.production_link.models import BrokerAccountIdentity, BrokerOrderEvent, BrokerOrderRecord, BrokerPositionSnapshot
from mgc_v05l.production_link.service import ProductionLinkActionError, SchwabProductionLinkService


class FakeSchwabBrokerClient:
    def __init__(self) -> None:
        self.submitted_orders: list[dict] = []
        self.replaced_orders: list[tuple[str, dict]] = []
        self.cancelled_orders: list[str] = []
        self.direct_status_checks: list[str] = []
        self.submitted_order_status: str = "WORKING"
        self.submit_broker_order_id: str | None = "broker-999"
        self.dynamic_positions: list[dict] = []
        self.hide_submitted_from_open_orders: bool = False
        self.direct_status_payloads: dict[str, dict | None] = {}

    def list_account_numbers(self) -> list[dict]:
        return [{"accountNumber": "123456789", "hashValue": "hash-123"}]

    def list_accounts(self, *, fields: list[str] | None = None) -> list[dict]:
        return [
            {
                "securitiesAccount": {
                    "accountNumber": "123456789",
                    "hashValue": "hash-123",
                    "type": "MARGIN",
                    "currentBalances": {
                        "cashBalance": "15000.25",
                        "buyingPower": "45000.75",
                        "liquidationValue": "62000.50",
                        "longMarketValue": "47000.25",
                    },
                    "positions": [
                        {
                            "longQuantity": "2",
                            "averagePrice": "100.25",
                            "marketValue": "210.50",
                            "currentDayProfitLoss": "4.25",
                            "instrument": {
                                "symbol": "AAPL",
                                "description": "Apple Inc.",
                                "assetType": "EQUITY",
                                "mark": "105.25",
                            },
                        }
                    ]
                    + self.dynamic_positions,
                }
            }
        ]

    def get_orders(
        self,
        account_hash: str,
        *,
        from_entered_time: str | None = None,
        to_entered_time: str | None = None,
        status: str | None = None,
        max_results: int | None = None,
    ) -> list[dict]:
        if status == "WORKING":
            rows = [
                {
                    "orderId": "broker-1",
                    "status": "WORKING",
                    "enteredTime": "2026-03-22T20:01:00+00:00",
                    "orderType": "LIMIT",
                    "duration": "DAY",
                    "session": "NORMAL",
                    "price": "101.50",
                    "orderLegCollection": [
                        {
                            "instruction": "BUY",
                            "quantity": "1",
                            "instrument": {"symbol": "AAPL", "assetType": "EQUITY", "description": "Apple Inc."},
                        }
                    ],
                }
            ]
            for index, payload in enumerate(self.submitted_orders, start=1):
                broker_order_id = self.submit_broker_order_id or f"submitted-{index}"
                if (
                    self.hide_submitted_from_open_orders
                    or broker_order_id in self.cancelled_orders
                    or self.submitted_order_status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
                ):
                    continue
                rows.append(
                    {
                        "orderId": broker_order_id,
                        "clientOrderId": payload.get("clientOrderId"),
                        "status": self.submitted_order_status,
                        "enteredTime": "2026-03-22T20:05:00+00:00",
                        "orderType": payload.get("orderType") or "LIMIT",
                        "duration": payload.get("duration") or "DAY",
                        "session": payload.get("session") or "NORMAL",
                        "price": payload.get("price"),
                        "stopPrice": payload.get("stopPrice"),
                        "filledQuantity": payload.get("filledQuantity"),
                        "orderLegCollection": payload.get("orderLegCollection") or [],
                    }
                )
            return rows
        rows = [
            {
                "orderId": "broker-2",
                "status": "FILLED",
                "enteredTime": "2026-03-22T19:45:00+00:00",
                "orderType": "MARKET",
                "duration": "DAY",
                "session": "NORMAL",
                "filledQuantity": "1",
                "orderLegCollection": [
                    {
                        "instruction": "SELL",
                        "quantity": "1",
                        "instrument": {"symbol": "MSFT", "assetType": "EQUITY", "description": "Microsoft"},
                    }
                ],
            }
        ]
        if self.submitted_orders and self.submitted_order_status in {"FILLED", "PARTIALLY_FILLED"}:
            payload = self.submitted_orders[-1]
            rows.append(
                {
                    "orderId": self.submit_broker_order_id or "submitted-filled",
                    "clientOrderId": payload.get("clientOrderId"),
                    "status": self.submitted_order_status,
                    "enteredTime": "2026-03-22T20:05:00+00:00",
                    "orderType": payload.get("orderType") or "LIMIT",
                    "duration": payload.get("duration") or "DAY",
                    "session": payload.get("session") or "NORMAL",
                    "price": payload.get("price"),
                    "stopPrice": payload.get("stopPrice"),
                    "filledQuantity": payload.get("quantity"),
                    "orderLegCollection": payload.get("orderLegCollection") or [],
                }
            )
        return rows

    def submit_order(self, account_hash: str, order_payload: dict) -> dict:
        self.submitted_orders.append(order_payload)
        location = f"/accounts/hash-123/orders/{self.submit_broker_order_id}" if self.submit_broker_order_id else None
        return {"status_code": 201, "location": location, "broker_order_id": self.submit_broker_order_id}

    def get_order_status(self, account_hash: str, broker_order_id: str) -> dict:
        self.direct_status_checks.append(broker_order_id)
        if broker_order_id in self.direct_status_payloads:
            payload = self.direct_status_payloads[broker_order_id]
            if payload is None:
                raise production_link_service.SchwabBrokerHttpError(
                    f"Schwab trader HTTP error 404 for GET /accounts/{account_hash}/orders/{broker_order_id}: not found"
                )
            return payload

        if self.submitted_orders and broker_order_id == self.submit_broker_order_id:
            payload = self.submitted_orders[-1]
            return {
                "orderId": broker_order_id,
                "clientOrderId": payload.get("clientOrderId"),
                "status": self.submitted_order_status,
                "enteredTime": "2026-03-22T20:05:00+00:00",
                "orderType": payload.get("orderType") or "LIMIT",
                "duration": payload.get("duration") or "DAY",
                "session": payload.get("session") or "NORMAL",
                "price": payload.get("price"),
                "stopPrice": payload.get("stopPrice"),
                "filledQuantity": payload.get("quantity") if self.submitted_order_status in {"FILLED", "PARTIALLY_FILLED"} else None,
                "orderLegCollection": payload.get("orderLegCollection") or [],
            }
        raise production_link_service.SchwabBrokerHttpError(
            f"Schwab trader HTTP error 404 for GET /accounts/{account_hash}/orders/{broker_order_id}: not found"
        )

    def cancel_order(self, account_hash: str, broker_order_id: str) -> dict:
        self.cancelled_orders.append(broker_order_id)
        return {"status_code": 200, "broker_order_id": broker_order_id}

    def replace_order(self, account_hash: str, broker_order_id: str, order_payload: dict) -> dict:
        self.replaced_orders.append((broker_order_id, order_payload))
        return {"status_code": 200, "broker_order_id": broker_order_id}


def _write_token_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_type": "Bearer",
        "expires_in": 3600,
        "scope": "trader",
        "issued_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _live_quote_payload(*, delayed: bool = False) -> dict[str, dict]:
    return {
        "AAPL": {
            "quote": {
                "bidPrice": "109.75",
                "askPrice": "110.25",
                "lastPrice": "110.00",
                "mark": "110.00",
                "closePrice": "108.00",
                "netChange": "2.00",
                "netPercentChange": "1.85",
                "delayed": delayed,
            }
        }
    }


def _manual_auth_payload() -> dict[str, object]:
    return {
        "operator_authenticated": True,
        "local_operator_identity": "test_operator",
        "auth_session_id": "auth-session-1",
        "auth_method": "TOUCH_ID",
        "authenticated_at": "2026-03-22T20:00:00+00:00",
    }


def test_production_link_snapshot_disabled_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("MGC_PRODUCTION_LINK_ENABLED", raising=False)
    service = SchwabProductionLinkService(tmp_path)

    snapshot = service.snapshot()

    assert snapshot["status"] == "disabled"
    assert snapshot["enabled"] is False


def test_manual_live_pilot_surface_reports_scope_and_status(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "ABBV")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["capabilities"]["manual_live_pilot"] is True
    assert snapshot["manual_order_safety"]["pilot_mode"]["enabled"] is True
    assert snapshot["manual_order_safety"]["pilot_mode"]["scope"]["submit_order_type"] == "LIMIT"
    assert snapshot["manual_order_safety"]["pilot_readiness"]["submit_eligible"] is True
    assert snapshot["manual_order_safety"]["pilot_mode"]["scope"]["symbol_whitelist"] == ["ABBV"]
    assert snapshot["manual_order_safety"]["pilot_readiness"]["locked_policy"]["allowed_open_route"]["operator_label"] == "BUY_TO_OPEN"
    assert snapshot["manual_order_safety"]["pilot_readiness"]["locked_policy"]["allowed_close_route"]["operator_label"] == "SELL_TO_CLOSE"
    assert snapshot["capabilities"]["manual_live_pilot_scope"]["omit_client_order_id_for_proven_route"] is True
    pilot_status_export = json.loads((tmp_path / "outputs" / "operator_dashboard" / "pilot_status_v1.json").read_text(encoding="utf-8"))
    assert pilot_status_export["pilot_readiness"]["submit_eligible"] is True
    assert pilot_status_export["allowed_scope"]["submit_order_type"] == "LIMIT"
    assert pilot_status_export["allowed_scope"]["allowed_open_route"]["operator_label"] == "BUY_TO_OPEN"


def test_locked_manual_live_pilot_route_blocks_out_of_scope_live_submit(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    class FlatClient(FakeSchwabBrokerClient):
        def list_accounts(self, *, fields: list[str] | None = None) -> list[dict]:
            return [
                {
                    "securitiesAccount": {
                        "accountNumber": "123456789",
                        "hashValue": "hash-123",
                        "type": "MARGIN",
                        "currentBalances": {
                            "cashBalance": "15000.25",
                            "buyingPower": "45000.75",
                            "liquidationValue": "62000.50",
                            "longMarketValue": "0",
                        },
                        "positions": [],
                    }
                }
            ]

        def get_orders(
            self,
            account_hash: str,
            *,
            from_entered_time: str | None = None,
            to_entered_time: str | None = None,
            status: str | None = None,
            max_results: int | None = None,
        ) -> list[dict]:
            return []

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FlatClient(),
    )

    with pytest.raises(ProductionLinkActionError, match="Locked manual-live pilot open route only supports BUY_TO_OPEN."):
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "intent_type": "MANUAL_LIVE_PILOT",
                "side": "SELL",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.50",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )

    with pytest.raises(ProductionLinkActionError, match="Locked manual-live pilot route only supports quantity 1."):
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "intent_type": "MANUAL_LIVE_PILOT",
                "side": "BUY",
                "quantity": "2",
                "order_type": "LIMIT",
                "limit_price": "101.50",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )


def test_manual_live_submit_gate_diagnosis_names_exact_config_blockers(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    snapshot = service.snapshot(force_refresh=True)
    safety = snapshot["manual_order_safety"]
    stock_limit_status = snapshot["capabilities"]["order_type_live_verification_matrix"]["STOCK"]["LIMIT"]

    assert safety["submit_status_label"] == "CONFIG FLAGS OFF"
    assert "MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED" in safety["submit_status_detail"]
    assert "MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED" in safety["submit_status_detail"]
    assert any("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST" in item for item in safety["blockers"])
    assert stock_limit_status["live_enabled"] is False
    assert stock_limit_status["previewable"] is True
    assert stock_limit_status["blocker_reason"] == "Stock LIMIT live submit is disabled because MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED is false."


def test_live_refresh_retires_stale_schwab_open_orders_and_clears_reconciliation(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )
    service._store.upsert_orders(
        [
            BrokerOrderRecord(
                broker_order_id="stale-broker-order",
                account_hash="hash-123",
                client_order_id=None,
                symbol="SPXW  260325P06555000",
                description="stale order",
                asset_class="OPTION",
                instruction="BUY_TO_CLOSE",
                quantity=Decimal("10"),
                filled_quantity=Decimal("0"),
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="WORKING",
                entered_at=datetime(2026, 3, 25, 16, 25, tzinfo=timezone.utc),
                closed_at=None,
                updated_at=datetime(2026, 3, 25, 16, 25, tzinfo=timezone.utc),
                limit_price=Decimal("0.30"),
                stop_price=None,
                source="schwab_live",
                raw_payload={"seeded": True},
            )
        ],
        event_source="seed",
    )

    snapshot = service.snapshot(force_refresh=True)

    open_ids = {row["broker_order_id"] for row in snapshot["orders"]["open_rows"]}
    recent_events = snapshot["orders"]["recent_events"]
    refresh_summary = snapshot["runtime_state"]["last_refresh_summary"]

    assert "stale-broker-order" not in open_ids
    assert snapshot["reconciliation"]["status"] == "clear"
    assert snapshot["manual_order_safety"]["submit_enabled"] is True
    assert refresh_summary["retired_open_order_ids"] == ["stale-broker-order"]
    assert any(
        row["event_type"] == "retired_by_live_sync" and row["broker_order_id"] == "stale-broker-order"
        for row in recent_events
    )


def test_production_link_snapshot_and_manual_order_flow(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_PORTFOLIO_STATEMENT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL,MSFT")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: fake_client,
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["status"] == "ready"
    assert snapshot["label"] == "CONNECTED"
    assert snapshot["accounts"]["selected_account_hash"] == "hash-123"
    assert snapshot["portfolio"]["positions"][0]["symbol"] == "AAPL"
    assert snapshot["orders"]["open_rows"][0]["broker_order_id"] == "broker-1"
    assert snapshot["health"]["broker_reachable"]["ok"] is True
    assert snapshot["capabilities"]["manual_order_submit"] is True
    assert snapshot["manual_order_safety"]["submit_enabled"] is True
    assert (tmp_path / "outputs" / "operator_dashboard" / "production_link_snapshot.json").exists()
    selected_account_payload = json.loads((tmp_path / "outputs" / "production_link" / "selected_account.json").read_text(encoding="utf-8"))
    assert selected_account_payload["account_hash"] == "hash-123"
    assert selected_account_payload["account_number"] == "123456789"
    assert selected_account_payload["display_name"] == "MARGIN 123456789"
    assert selected_account_payload["source"] in {"first_available", "persisted_selection", "config_default_hash", "config_default_number"}

    result = service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    assert result["ok"] is True
    assert fake_client.submitted_orders[0]["orderType"] == "LIMIT"
    assert fake_client.submitted_orders[0]["orderLegCollection"][0]["quantity"] == 1
    refreshed = result["production_link"]
    assert refreshed["orders"]["recent_events"][0]["event_type"] in {"submit_acknowledged", "status_sync", "order_seen"}


def test_manual_live_submit_failure_is_persisted_for_operator_audit(tmp_path: Path, monkeypatch) -> None:
    class FailingSubmitClient(FakeSchwabBrokerClient):
        def submit_order(self, account_hash: str, order_payload: dict) -> dict:
            raise production_link_service.SchwabBrokerHttpError(
                "Schwab trader HTTP error 400 for POST /accounts/hash-123/orders: A validation error occurred while processing the request."
            )

    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FailingSubmitClient(),
    )

    try:
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "intent_type": "MANUAL_LIVE_PILOT",
                "side": "BUY",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.50",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )
    except production_link_service.SchwabBrokerHttpError:
        pass
    else:
        raise AssertionError("Expected SchwabBrokerHttpError from failing submit client.")

    snapshot = service._store.build_snapshot()  # type: ignore[attr-defined]
    latest_validation = snapshot["manual_validation"]["latest_event"]
    latest_result = service._store.load_runtime_state("last_manual_order")["result"]  # type: ignore[attr-defined]
    recent_events = snapshot["orders"]["recent_events"]

    assert latest_validation["scenario_type"] == "manual_live_submit_failed"
    assert "validation error" in latest_validation["payload"]["error"].lower()
    assert latest_result["ok"] is False
    assert "validation error" in latest_result["error"].lower()
    assert any(row["event_type"] == "submit_failed" for row in recent_events)


def test_manual_order_preview_emits_integral_quantity_for_whole_share_stock_order(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    intended = preview["payload"]["payload_summary"]["intended_schwab_payload"]
    assert intended["orderLegCollection"][0]["quantity"] == 1


def test_manual_live_buy_pilot_preview_omits_client_order_id_for_narrow_stock_limit_probe(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    intended = preview["payload"]["payload_summary"]["intended_schwab_payload"]
    assert "clientOrderId" not in intended


def test_manual_live_flatten_sell_preview_omits_client_order_id_for_narrow_stock_limit_close_probe(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "FLATTEN",
            "side": "SELL",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "0.01",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    intended = preview["payload"]["payload_summary"]["intended_schwab_payload"]
    assert "clientOrderId" not in intended


def test_non_pilot_manual_order_preview_keeps_client_order_id(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "ENTRY",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    intended = preview["payload"]["payload_summary"]["intended_schwab_payload"]
    assert intended["clientOrderId"].startswith("manual-")


def test_production_link_snapshot_overlays_live_quotes_into_monitor_rows(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
        quote_payload_fetcher=lambda config_path, oauth_client, symbols: (
            _live_quote_payload(),
            {"auth_mode": "test", "source_label": "test quotes"},
        ),
    )

    snapshot = service.snapshot(force_refresh=True)

    position = snapshot["portfolio"]["positions"][0]
    assert position["mark_price"] == "110.00"
    assert position["market_value"] == "220.00"
    assert position["current_day_pnl"] == "4.00"
    assert position["open_pnl"] == "19.50"
    assert snapshot["quotes"]["rows"][0]["mark_price"] == "110.00"
    assert snapshot["freshness"]["positions"]["state"] == "LIVE"
    assert snapshot["freshness"]["quotes"]["state"] == "LIVE"
    assert snapshot["diagnostics"]["last_quotes_refresh_at"] is not None
    assert snapshot["broker_state_snapshot"]["positions_by_symbol"]["AAPL"]["mark_price"] == "110.00"


def test_production_link_quote_refresh_failure_stays_monitor_safe_and_honest(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
        quote_payload_fetcher=lambda config_path, oauth_client, symbols: (_ for _ in ()).throw(FileNotFoundError("missing config")),
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["status"] == "ready"
    assert snapshot["freshness"]["positions"]["state"] == "LIVE"
    assert snapshot["freshness"]["quotes"]["state"] == "STALE"
    assert snapshot["diagnostics"]["last_quote_error"] == "missing config"
    assert snapshot["portfolio"]["positions"][0]["mark_price"] == "105.25"


def test_manual_order_requires_review_confirmation(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    try:
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "MARKET",
                "review_confirmed": False,
            },
        )
    except ProductionLinkActionError as exc:
        assert "review confirmation" in str(exc)
    else:
        raise AssertionError("Expected ProductionLinkActionError when review confirmation is missing.")


def test_manual_order_feature_and_asset_gates(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.delenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", raising=False)

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    try:
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "MARKET",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "disabled by production-link feature flag" in str(exc)
    else:
        raise AssertionError("Expected production-link feature flag gate.")

    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )
    try:
        service.run_action(
            "submit-order",
            {
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.25",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "Live order submit safety mode is disabled" in str(exc)
    else:
        raise AssertionError("Expected live-order safety mode gate.")

    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )
    service.snapshot(force_refresh=True)

    for payload, expected_fragment in (
        (
            {
                "account_hash": "hash-123",
                "symbol": "AAPL240621C00200000",
                "asset_class": "OPTION",
                "side": "BUY",
                "quantity": "1",
                "order_type": "MARKET",
                "review_confirmed": True,
            },
            "not enabled for manual live orders",
        ),
        (
            {
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "SELL_SHORT",
                "quantity": "1",
                "order_type": "MARKET",
                "review_confirmed": True,
            },
            "SELL_SHORT is disabled",
        ),
    ):
        try:
            service.run_action("submit-order", {**_manual_auth_payload(), **payload})
        except ProductionLinkActionError as exc:
            assert expected_fragment in str(exc)
        else:
            raise AssertionError(f"Expected ProductionLinkActionError containing {expected_fragment!r}.")

    try:
        service.run_action(
            "replace-order",
            {
                **_manual_auth_payload(),
                "broker_order_id": "broker-1",
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.25",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "Replace order is disabled" in str(exc)
    else:
        raise AssertionError("Expected replace-order to remain disabled by default.")


def test_advanced_exto_preview_is_available_but_live_submit_stays_blocked(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_ADVANCED_TIF_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_EXT_EXTO_TICKET_SUPPORT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,STOP_LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_TIF_VALUES", "DAY,GTC")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_SESSION_VALUES", "NORMAL,EXT,EXTO")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: fake_client,
    )

    preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.25",
            "time_in_force": "GTC",
            "session": "EXTO",
            "review_confirmed": True,
        },
    )

    assert preview["ok"] is True
    assert preview["payload"]["structure_summary"]["advanced_mode"] == "GTC_EXTO"
    assert preview["payload"]["payload_summary"]["intended_schwab_payload"]["duration"] == "GOOD_TILL_CANCEL"
    assert preview["payload"]["payload_summary"]["intended_schwab_payload"]["session"] == "EXTO"
    assert preview["payload"]["live_submit_enabled"] is False
    assert "EXTO / GTC_EXTO live submission remains disabled pending live Schwab verification." in preview["payload"]["live_submit_blockers"]
    assert fake_client.submitted_orders == []

    try:
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.25",
                "time_in_force": "GTC",
                "session": "EXTO",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "EXTO / GTC_EXTO live submission remains disabled" in str(exc)
    else:
        raise AssertionError("Expected advanced EXTO submit to remain blocked.")


def test_oco_preview_supported_but_live_submit_disabled(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_OCO_TICKET_SUPPORT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,STOP,STOP_LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_TIF_VALUES", "DAY,GTC")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_SESSION_VALUES", "NORMAL,EXT,EXTO")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: fake_client,
    )

    preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "structure_type": "OCO",
            "side": "OCO",
            "order_type": "OCO",
            "quantity": "1",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
            "oco_group_id": "oco-1",
            "oco_legs": [
                {"leg_label": "Profit", "side": "SELL", "quantity": "1", "order_type": "LIMIT", "limit_price": "110.00"},
                {"leg_label": "Stop", "side": "SELL", "quantity": "1", "order_type": "STOP", "stop_price": "95.00"},
            ],
        },
    )

    assert preview["ok"] is True
    assert preview["payload"]["structure_summary"]["structure_type"] == "OCO"
    assert preview["payload"]["payload_summary"]["intended_schwab_payload"]["orderStrategyType"] == "OCO"
    assert len(preview["payload"]["payload_summary"]["intended_schwab_payload"]["childOrderStrategies"]) == 2
    assert preview["payload"]["live_submit_enabled"] is False
    assert "OCO live submission remains disabled pending live Schwab verification." in preview["payload"]["live_submit_blockers"]
    assert fake_client.submitted_orders == []

    try:
        service.run_action(
            "submit-order",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "structure_type": "OCO",
                "side": "OCO",
                "order_type": "OCO",
                "quantity": "1",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
                "oco_group_id": "oco-1",
                "oco_legs": [
                    {"leg_label": "Profit", "side": "SELL", "quantity": "1", "order_type": "LIMIT", "limit_price": "110.00"},
                    {"leg_label": "Stop", "side": "SELL", "quantity": "1", "order_type": "STOP", "stop_price": "95.00"},
                ],
            },
        )
    except ProductionLinkActionError as exc:
        assert "OCO live submission remains disabled" in str(exc)
    else:
        raise AssertionError("Expected OCO submit to remain blocked.")


def test_stock_close_order_types_preview_and_future_exclusion(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK,FUTURE")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_DRY_RUN_ORDER_TYPES", "MARKET,LIMIT,STOP,STOP_LIMIT,TRAIL_STOP,TRAIL_STOP_LIMIT,MARKET_ON_CLOSE,LIMIT_ON_CLOSE")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL,ESM6")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    moc_preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "side": "SELL",
            "quantity": "1",
            "order_type": "MARKET_ON_CLOSE",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    assert moc_preview["ok"] is True
    assert moc_preview["payload"]["payload_summary"]["intended_schwab_payload"]["orderType"] == "MARKET_ON_CLOSE"
    assert any(
        "Market-on-close / limit-on-close live submission remains disabled pending live Schwab verification." in item
        for item in moc_preview["payload"]["live_submit_blockers"]
    )

    loc_preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "side": "SELL",
            "quantity": "1",
            "order_type": "LIMIT_ON_CLOSE",
            "limit_price": "110.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    assert loc_preview["ok"] is True
    assert loc_preview["payload"]["payload_summary"]["intended_schwab_payload"]["price"] == "110.50"

    try:
        service.run_action(
            "preview-order",
            {
                "account_hash": "hash-123",
                "symbol": "ESM6",
                "asset_class": "FUTURE",
                "side": "BUY",
                "quantity": "1",
                "order_type": "MARKET_ON_CLOSE",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "not supported for asset class FUTURE" in str(exc)
    else:
        raise AssertionError("Expected MARKET_ON_CLOSE preview to be blocked for FUTURE.")


def test_trailing_order_preview_validation_and_live_gate(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_DRY_RUN_ORDER_TYPES", "MARKET,LIMIT,STOP,STOP_LIMIT,TRAIL_STOP,TRAIL_STOP_LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    try:
        service.run_action(
            "preview-order",
            {
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "TRAIL_STOP_LIMIT",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "trail_value_type must be AMOUNT or PERCENT" in str(exc)
    else:
        raise AssertionError("Expected missing trailing fields to be blocked.")

    preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "side": "BUY",
            "quantity": "1",
            "order_type": "TRAIL_STOP_LIMIT",
            "trail_value_type": "AMOUNT",
            "trail_value": "2.5",
            "trail_trigger_basis": "LAST",
            "trail_limit_offset": "0.75",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    intended = preview["payload"]["payload_summary"]["intended_schwab_payload"]
    assert preview["ok"] is True
    assert intended["orderType"] == "TRAIL_STOP_LIMIT"
    assert intended["stopPriceOffset"] == "2.5"
    assert intended["priceOffset"] == "0.75"
    assert any(
        "Trailing order live submission remains disabled pending live Schwab verification." in item
        for item in preview["payload"]["live_submit_blockers"]
    )


def test_future_order_types_preview_and_live_submit_gate(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK,FUTURE")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_DRY_RUN_ORDER_TYPES", "MARKET,LIMIT,STOP,STOP_LIMIT,TRAIL_STOP,TRAIL_STOP_LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET,STOP")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "ESM6")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    preview = service.run_action(
        "preview-order",
        {
            "account_hash": "hash-123",
            "symbol": "ESM6",
            "asset_class": "FUTURE",
            "side": "SELL",
            "quantity": "1",
            "order_type": "STOP",
            "stop_price": "4999.25",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    assert preview["ok"] is True
    assert preview["payload"]["payload_summary"]["intended_schwab_payload"]["orderType"] == "STOP"
    assert any(
        "Futures live submission remains disabled pending live Schwab verification." in item
        for item in preview["payload"]["live_submit_blockers"]
    )


def test_live_verification_matrix_and_sequence_surface(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK,FUTURE")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET,STOP,STOP_LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_DRY_RUN_ORDER_TYPES", "MARKET,LIMIT,STOP,STOP_LIMIT,TRAIL_STOP,TRAIL_STOP_LIMIT,MARKET_ON_CLOSE,LIMIT_ON_CLOSE")
    monkeypatch.setenv("MGC_PRODUCTION_ADVANCED_TIF_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_EXT_EXTO_TICKET_SUPPORT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_OCO_TICKET_SUPPORT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    snapshot = service.snapshot(force_refresh=True)
    capabilities = snapshot["capabilities"]
    matrix = capabilities["order_type_live_verification_matrix"]
    next_step = capabilities["next_live_verification_step"]
    runbooks = capabilities["near_term_live_verification_runbooks"]

    assert matrix["STOCK"]["LIMIT"]["previewable"] is True
    assert matrix["STOCK"]["LIMIT"]["live_enabled"] is True
    assert matrix["STOCK"]["LIMIT"]["live_verified"] is False
    assert matrix["STOCK"]["MARKET"]["blocked"] is True
    assert "Await live verification of STOCK LIMIT" in matrix["STOCK"]["MARKET"]["blocker_reason"]
    assert matrix["ADVANCED"]["OCO"]["previewable"] is True
    assert matrix["ADVANCED"]["OCO"]["live_enabled"] is False
    assert "blocked in the current verification phase" in matrix["ADVANCED"]["OCO"]["blocker_reason"]
    assert next_step["verification_key"] == "STOCK:LIMIT"
    assert runbooks["STOCK:LIMIT"]["replace_expectation"] == "Replace remains disabled in this phase."
    assert "MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED=1" in runbooks["STOCK:LIMIT"]["required_flags"]
    assert "quantity=1" in runbooks["STOCK:LIMIT"]["required_fields"]
    assert runbooks["STOCK:LIMIT"]["submit_path"][0] == "Open Positions -> Manual Order Ticket."
    assert snapshot["diagnostics"]["manual_order_live_verification"]["next_step"]["verification_key"] == "STOCK:LIMIT"
    assert snapshot["manual_order_safety"]["constraints"]["next_live_verification_step"]["verification_key"] == "STOCK:LIMIT"
    assert snapshot["manual_order_safety"]["constraints"]["first_live_stock_limit_test"]["required_flags"][3] == "MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED=1"


def test_live_verification_sequence_advances_only_after_verified_prefix(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ASSET_CLASSES", "STOCK")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET,STOP")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_MARKET_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_STOP_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_VERIFIED_ORDER_KEYS", "STOCK:LIMIT,STOCK:MARKET")

    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: FakeSchwabBrokerClient(),
    )

    snapshot = service.snapshot(force_refresh=True)
    matrix = snapshot["capabilities"]["order_type_live_verification_matrix"]

    assert matrix["STOCK"]["LIMIT"]["live_verified"] is True
    assert matrix["STOCK"]["MARKET"]["live_verified"] is True
    assert matrix["STOCK"]["STOP"]["live_enabled"] is True
    assert snapshot["capabilities"]["next_live_verification_step"]["verification_key"] == "STOCK:STOP"


def test_cancel_flatten_and_reconciliation_surface(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_MARKET_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_VERIFIED_ORDER_KEYS", "STOCK:LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_PORTFOLIO_STATEMENT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT,MARKET")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_MAX_QUANTITY", "5")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(
        tmp_path,
        client_factory=lambda config, oauth_client: fake_client,
    )

    fetched_at = datetime(2026, 3, 22, 20, 0, tzinfo=timezone.utc)
    service._store.save_accounts(  # type: ignore[attr-defined]
        [
            BrokerAccountIdentity(
                broker_name="Schwab",
                account_hash="hash-123",
                account_number="123456789",
                display_name="MARGIN 123456789",
                account_type="MARGIN",
                selected=True,
                source="persisted_seed",
                updated_at=fetched_at,
                raw_payload={},
            )
        ],
        selected_account_hash="hash-123",
    )
    service._store.save_portfolio_snapshot(  # type: ignore[attr-defined]
        account_hash="hash-123",
        balances=None,
        positions=[
            BrokerPositionSnapshot(
                account_hash="hash-123",
                position_key="hash-123:AAPL:seed",
                symbol="AAPL",
                description="Apple Inc.",
                asset_class="STOCK",
                quantity=Decimal("3"),
                side="LONG",
                average_cost=Decimal("100"),
                mark_price=Decimal("105"),
                market_value=Decimal("315"),
                current_day_pnl=Decimal("5"),
                open_pnl=Decimal("15"),
                ytd_pnl=None,
                margin_impact=None,
                broker_position_id="pos-seed",
                fetched_at=fetched_at,
                raw_payload={},
            )
        ],
    )
    service._store.upsert_orders(  # type: ignore[attr-defined]
        [
            BrokerOrderRecord(
                broker_order_id="broker-1",
                account_hash="hash-123",
                client_order_id="seed-1",
                symbol="AAPL",
                description="Apple Inc.",
                asset_class="STOCK",
                instruction="BUY",
                quantity=Decimal("2"),
                filled_quantity=None,
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="PENDING_ACTIVATION",
                entered_at=fetched_at,
                closed_at=None,
                updated_at=fetched_at,
                limit_price=Decimal("101.50"),
                stop_price=None,
                source="persisted_seed",
                raw_payload={},
            )
        ],
        event_source="persisted_seed",
    )

    snapshot = service.snapshot(force_refresh=True)
    assert snapshot["reconciliation"]["status"] == "clear"
    assert snapshot["reconciliation"]["mismatch_count"] == 0

    cancel_result = service.run_action("cancel-order", {"account_hash": "hash-123", "broker_order_id": "broker-1"})
    assert cancel_result["ok"] is True
    assert fake_client.cancelled_orders == ["broker-1"]

    with pytest.raises(ProductionLinkActionError, match="Locked manual-live pilot route only supports LIMIT submit."):
        service.run_action(
            "flatten-position",
            {
                **_manual_auth_payload(),
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "quantity": "2",
                "side": "LONG",
            },
        )


def test_manual_live_order_submit_persists_intent_note_and_broker_order_id(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    def _raise_unavailable(account_hash: str, broker_order_id: str) -> dict:
        raise production_link_service.SchwabBrokerHttpError(
            f"Schwab trader HTTP error 500 for GET /accounts/{account_hash}/orders/{broker_order_id}: unavailable"
        )

    fake_client.get_order_status = _raise_unavailable  # type: ignore[method-assign]
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    result = service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "operator_note": "Controlled live validation order",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    assert result["ok"] is True
    lifecycle_rows = result["production_link"]["manual_live_orders"]["recent_rows"]
    assert lifecycle_rows[0]["broker_order_id"] == "broker-999"
    assert lifecycle_rows[0]["intent_type"] == "MANUAL_LIVE_PILOT"
    assert lifecycle_rows[0]["operator_note"] == "Controlled live validation order"
    assert lifecycle_rows[0]["local_operator_identity"] == "test_operator"
    assert lifecycle_rows[0]["lifecycle_state"] in {"OPEN_WAITING_FILL", "SUBMITTED"}
    assert result["production_link"]["diagnostics"]["last_manual_order_request"]["intent_type"] == "MANUAL_LIVE_PILOT"


def test_manual_live_order_submit_rejects_without_operator_auth(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: FakeSchwabBrokerClient())

    try:
        service.run_action(
            "submit-order",
            {
                "account_hash": "hash-123",
                "symbol": "AAPL",
                "asset_class": "STOCK",
                "side": "BUY",
                "quantity": "1",
                "order_type": "LIMIT",
                "limit_price": "101.50",
                "time_in_force": "DAY",
                "session": "NORMAL",
                "review_confirmed": True,
            },
        )
    except ProductionLinkActionError as exc:
        assert "authenticated local operator session" in str(exc)
    else:
        raise AssertionError("Expected operator-auth gate for live manual submit.")


def test_manual_live_order_timeout_escalates_to_reconciling_and_alerts(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")

    fake_client = FakeSchwabBrokerClient()
    fake_client.dynamic_positions = [
        {
            "longQuantity": "1",
            "averagePrice": "101.50",
            "marketValue": "101.50",
            "currentDayProfitLoss": "0.00",
            "instrument": {
                "symbol": "TSLA",
                "description": "Tesla Inc.",
                "assetType": "EQUITY",
                "mark": "101.50",
            },
        }
    ]
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-timeout-1",
                    "client_order_id": "manual-timeout-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "ENTRY",
                    "created_at": stale_at,
                    "submitted_at": stale_at,
                    "broker_order_status": "REQUESTED",
                    "active": True,
                }
            ]
        },
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["manual_live_orders"]["summary"]["manual_review_required_count"] == 1
    assert snapshot["manual_live_orders"]["active_rows"][0]["lifecycle_state"] == "RECONCILING"
    assert snapshot["alerts"]["active"][0]["code"] == "LIVE_MANUAL_ORDER_RECONCILING"
    assert snapshot["manual_validation"]["latest_event"]["scenario_type"] in {"manual_live_restore_validation", "manual_live_reconciling"}


def test_manual_live_order_ack_overdue_is_visible_without_auto_submit(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_ACK_TIMEOUT_SECONDS", "5")

    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: FakeSchwabBrokerClient())
    stale_at = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-ack-timeout-1",
                    "client_order_id": "manual-ack-timeout-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "ENTRY",
                    "created_at": stale_at,
                    "submitted_at": stale_at,
                    "broker_order_status": "REQUESTED",
                    "active": True,
                }
            ]
        },
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["manual_live_orders"]["active_rows"][0]["lifecycle_state"] == "ACK_OVERDUE"
    assert snapshot["manual_live_orders"]["summary"]["overdue_ack_count"] == 1


def test_manual_live_order_post_ack_empty_open_snapshot_stays_in_grace_window(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_POST_ACK_GRACE_SECONDS", "30")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    fake_client.hide_submitted_from_open_orders = True
    fake_client.direct_status_payloads["broker-999"] = None
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    snapshot = service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )["production_link"]

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "ACCEPTED_AWAITING_BROKER_CONFIRMATION"
    assert recent_row["lifecycle_classification"] == "post_ack_grace_window"
    assert recent_row["direct_status_last_outcome"] == "NOT_FOUND"
    assert snapshot["reconciliation"]["status"] == "clear"


def test_manual_live_order_post_ack_direct_status_later_confirms_working(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_POST_ACK_GRACE_SECONDS", "30")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    fake_client.hide_submitted_from_open_orders = True
    fake_client.direct_status_payloads["broker-999"] = None
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    fake_client.direct_status_payloads["broker-999"] = {
        "orderId": "broker-999",
        "status": "WORKING",
        "enteredTime": "2026-03-22T20:05:00+00:00",
        "orderType": "LIMIT",
        "duration": "DAY",
        "session": "NORMAL",
        "price": "101.50",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": "1",
                "instrument": {"symbol": "TSLA", "assetType": "EQUITY", "description": "Tesla Inc."},
            }
        ],
    }

    snapshot = service.snapshot(force_refresh=True)

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "DIRECT_STATUS_CONFIRMED_WORKING"
    assert recent_row["lifecycle_classification"] == "direct_status_confirmed_working"
    assert recent_row["direct_status_last_outcome"] == "WORKING"
    assert fake_client.direct_status_checks
    assert snapshot["reconciliation"]["status"] == "clear"


def test_manual_live_order_post_ack_direct_status_confirms_filled(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    fake_client.hide_submitted_from_open_orders = True
    fake_client.direct_status_payloads["broker-999"] = {
        "orderId": "broker-999",
        "status": "FILLED",
        "enteredTime": "2026-03-22T20:05:00+00:00",
        "orderType": "LIMIT",
        "duration": "DAY",
        "session": "NORMAL",
        "price": "101.50",
        "filledQuantity": "1",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": "1",
                "instrument": {"symbol": "TSLA", "assetType": "EQUITY", "description": "Tesla Inc."},
            }
        ],
    }
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    snapshot = service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )["production_link"]

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "FILLED"
    assert recent_row["lifecycle_classification"] == "direct_status_confirmed_filled"
    assert recent_row["direct_status_last_outcome"] == "FILLED"


def test_manual_live_order_post_ack_direct_status_confirms_cancelled(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    fake_client.hide_submitted_from_open_orders = True
    fake_client.direct_status_payloads["broker-999"] = {
        "orderId": "broker-999",
        "status": "CANCELED",
        "enteredTime": "2026-03-22T20:05:00+00:00",
        "cancelTime": "2026-03-22T20:06:00+00:00",
        "orderType": "LIMIT",
        "duration": "DAY",
        "session": "NORMAL",
        "price": "101.50",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": "1",
                "instrument": {"symbol": "TSLA", "assetType": "EQUITY", "description": "Tesla Inc."},
            }
        ],
    }
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    snapshot = service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )["production_link"]

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "CANCELED"
    assert recent_row["lifecycle_classification"] == "direct_status_confirmed_canceled"
    assert recent_row["cancel_resolution"] == "EXPLICIT_BROKER_TERMINAL"


def test_manual_live_order_post_ack_without_truth_after_grace_reconciles(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    fake_client = FakeSchwabBrokerClient()
    def _raise_unavailable(account_hash: str, broker_order_id: str) -> dict:
        raise production_link_service.SchwabBrokerHttpError(
            f"Schwab trader HTTP error 500 for GET /accounts/{account_hash}/orders/{broker_order_id}: unavailable"
        )

    fake_client.get_order_status = _raise_unavailable  # type: ignore[method-assign]
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-post-ack-1",
                    "client_order_id": "manual-post-ack-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "MANUAL_LIVE_PILOT",
                    "created_at": stale_at.isoformat(),
                    "submitted_at": stale_at.isoformat(),
                    "acknowledged_at": stale_at.isoformat(),
                    "post_ack_grace_started_at": stale_at.isoformat(),
                    "post_ack_grace_expires_at": (stale_at + timedelta(seconds=5)).isoformat(),
                    "broker_order_status": "ACKNOWLEDGED",
                    "active": True,
                }
            ]
        },
    )

    snapshot = service.snapshot(force_refresh=True)

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "RECONCILING"
    assert recent_row["lifecycle_classification"] == "post_ack_broker_truth_unresolved_after_grace"
    assert snapshot["alerts"]["active"][0]["code"] == "LIVE_MANUAL_ORDER_RECONCILING"


def test_manual_live_order_stuck_acknowledged_order_resolves_by_direct_terminal_status(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    fake_client = FakeSchwabBrokerClient()
    fake_client.direct_status_payloads["broker-stuck-1"] = {
        "orderId": "broker-stuck-1",
        "status": "EXPIRED",
        "enteredTime": "2026-03-22T20:05:00+00:00",
        "orderType": "LIMIT",
        "duration": "DAY",
        "session": "NORMAL",
        "price": "101.50",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": "1",
                "instrument": {"symbol": "AAPL", "assetType": "EQUITY", "description": "Apple Inc."},
            }
        ],
    }
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-stuck-1",
                    "client_order_id": "manual-stuck-1",
                    "account_hash": "hash-123",
                    "symbol": "AAPL",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "MANUAL_LIVE_PILOT",
                    "created_at": stale_at.isoformat(),
                    "submitted_at": stale_at.isoformat(),
                    "acknowledged_at": stale_at.isoformat(),
                    "post_ack_grace_started_at": stale_at.isoformat(),
                    "post_ack_grace_expires_at": (stale_at + timedelta(seconds=5)).isoformat(),
                    "broker_order_status": "ACKNOWLEDGED",
                    "lifecycle_state": "RECONCILING",
                    "active": True,
                }
            ]
        },
    )
    service._store.upsert_orders(  # type: ignore[attr-defined]
        [
            BrokerOrderRecord(
                broker_order_id="broker-stuck-1",
                account_hash="hash-123",
                client_order_id="manual-stuck-1",
                symbol="AAPL",
                description="Apple Inc.",
                asset_class="STOCK",
                instruction="BUY",
                quantity=Decimal("1"),
                filled_quantity=None,
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="WORKING",
                entered_at=stale_at,
                closed_at=None,
                updated_at=stale_at,
                limit_price=Decimal("101.50"),
                stop_price=None,
                source="manual_ticket_local",
                raw_payload={"manual_ticket": True},
            )
        ],
        event_source="manual_ticket_local",
    )

    snapshot = service.snapshot(force_refresh=True)

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "EXPIRED"
    assert recent_row["terminal_resolution"] == "EXPLICIT_BROKER_TERMINAL"
    assert recent_row["direct_status_last_outcome"] == "EXPIRED"
    assert snapshot["reconciliation"]["status"] == "clear"


def test_manual_live_order_absent_after_direct_not_found_resolves_terminal_non_fill_and_reopens_gate(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    fake_client.direct_status_payloads["broker-stuck-2"] = None
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-stuck-2",
                    "client_order_id": "manual-stuck-2",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "MANUAL_LIVE_PILOT",
                    "created_at": stale_at.isoformat(),
                    "submitted_at": stale_at.isoformat(),
                    "acknowledged_at": stale_at.isoformat(),
                    "post_ack_grace_started_at": stale_at.isoformat(),
                    "post_ack_grace_expires_at": (stale_at + timedelta(seconds=5)).isoformat(),
                    "broker_order_status": "ACKNOWLEDGED",
                    "lifecycle_state": "RECONCILING",
                    "active": True,
                }
            ]
        },
    )
    service._store.upsert_orders(  # type: ignore[attr-defined]
        [
            BrokerOrderRecord(
                broker_order_id="broker-stuck-2",
                account_hash="hash-123",
                client_order_id="manual-stuck-2",
                symbol="TSLA",
                description="Tesla Inc.",
                asset_class="STOCK",
                instruction="BUY",
                quantity=Decimal("1"),
                filled_quantity=None,
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="WORKING",
                entered_at=stale_at,
                closed_at=None,
                updated_at=stale_at,
                limit_price=Decimal("101.50"),
                stop_price=None,
                source="manual_ticket_local",
                raw_payload={"manual_ticket": True},
            )
        ],
        event_source="manual_ticket_local",
    )

    snapshot = service.snapshot(force_refresh=True)

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "TERMINAL_NON_FILL_RESOLVED"
    assert recent_row["terminal_resolution"] == "DIRECT_STATUS_NOT_FOUND_AND_FLAT"
    assert recent_row["direct_status_last_outcome"] == "NOT_FOUND"
    assert snapshot["reconciliation"]["status"] == "clear"

    eligible_preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    assert eligible_preview["payload"]["live_submit_enabled"] is True


def test_manual_live_order_same_symbol_entry_stays_blocked_while_old_order_is_unresolved(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-stuck-3",
                    "client_order_id": "manual-stuck-3",
                    "account_hash": "hash-123",
                    "symbol": "AAPL",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "MANUAL_LIVE_PILOT",
                    "created_at": stale_at.isoformat(),
                    "submitted_at": stale_at.isoformat(),
                    "acknowledged_at": stale_at.isoformat(),
                    "post_ack_grace_started_at": stale_at.isoformat(),
                    "post_ack_grace_expires_at": (stale_at + timedelta(seconds=5)).isoformat(),
                    "broker_order_status": "ACKNOWLEDGED",
                    "lifecycle_state": "RECONCILING",
                    "active": True,
                }
            ]
        },
    )
    service._store.upsert_orders(  # type: ignore[attr-defined]
        [
            BrokerOrderRecord(
                broker_order_id="broker-stuck-3",
                account_hash="hash-123",
                client_order_id="manual-stuck-3",
                symbol="AAPL",
                description="Apple Inc.",
                asset_class="STOCK",
                instruction="BUY",
                quantity=Decimal("1"),
                filled_quantity=None,
                order_type="LIMIT",
                duration="DAY",
                session="NORMAL",
                status="WORKING",
                entered_at=stale_at,
                closed_at=None,
                updated_at=stale_at,
                limit_price=Decimal("101.50"),
                stop_price=None,
                source="manual_ticket_local",
                raw_payload={"manual_ticket": True},
            )
        ],
        event_source="manual_ticket_local",
    )

    preview = service.run_action(
        "preview-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    assert preview["payload"]["live_submit_enabled"] is False
    assert "An unresolved live manual order already exists for AAPL." in preview["payload"]["live_submit_blockers"]


def test_manual_live_order_cancel_before_fill_is_inferred_when_order_disappears_and_broker_truth_is_flat(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "TSLA")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "TSLA",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    snapshot = service.run_action(
        "cancel-order",
        {"account_hash": "hash-123", "broker_order_id": "broker-999"},
    )["production_link"]

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "CANCELED_INFERRED"
    assert recent_row["cancel_resolution"] == "INFERRED_OPEN_ORDER_GONE"
    assert snapshot["manual_validation"]["latest_event"]["scenario_type"] in {"manual_live_cancel_inferred", "manual_live_restore_validation"}


def test_manual_live_order_cancel_disappearance_reconciling_when_position_context_is_not_safe_to_infer(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_RECONCILE_GRACE_SECONDS", "0")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )

    snapshot = service.run_action(
        "cancel-order",
        {"account_hash": "hash-123", "broker_order_id": "broker-999"},
    )["production_link"]

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "RECONCILING"
    assert recent_row["cancel_resolution"] == "UNRESOLVED_AFTER_DISAPPEARANCE"
    assert snapshot["alerts"]["active"][0]["code"] == "LIVE_MANUAL_ORDER_CANCEL_RECONCILING"


def test_manual_live_order_explicit_broker_terminal_cancel_is_respected(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: FakeSchwabBrokerClient())
    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=1))
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-cancel-explicit-1",
                    "client_order_id": "manual-cancel-explicit-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "ENTRY",
                    "created_at": stale_at.isoformat(),
                    "submitted_at": stale_at.isoformat(),
                    "acknowledged_at": stale_at.isoformat(),
                    "cancel_requested_at": stale_at.isoformat(),
                    "broker_order_status": "CANCEL_REQUESTED",
                    "active": True,
                }
            ]
        },
    )
    service._store.record_order_event(  # type: ignore[attr-defined]
        BrokerOrderEvent(
            account_hash="hash-123",
            broker_order_id="broker-cancel-explicit-1",
            client_order_id="manual-cancel-explicit-1",
            event_type="status_sync",
            status="CANCELED",
            occurred_at=stale_at,
            message="schwab_sync: CANCELED",
            request_payload=None,
            response_payload={"status": "CANCELED"},
            source="schwab_sync",
        )
    )

    snapshot = service.snapshot(force_refresh=True)

    recent_row = snapshot["manual_live_orders"]["recent_rows"][0]
    assert recent_row["lifecycle_state"] == "CANCELED"
    assert recent_row["cancel_resolution"] == "EXPLICIT_BROKER_TERMINAL"


def test_manual_live_order_safe_cleanup_when_broker_truth_is_flat_and_clean(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: FakeSchwabBrokerClient())
    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-cleanup-1",
                    "client_order_id": "manual-cleanup-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "ENTRY",
                    "created_at": stale_at,
                    "submitted_at": stale_at,
                    "broker_order_status": "REQUESTED",
                    "active": True,
                }
            ]
        },
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["manual_live_orders"]["summary"]["safe_cleanup_count"] == 1
    assert snapshot["manual_live_orders"]["recent_rows"][0]["lifecycle_state"] == "SAFE_CLEANUP_RESOLVED"
    assert snapshot["alerts"]["recent"][0]["code"] == "LIVE_MANUAL_ORDER_SAFE_CLEANUP"
    assert snapshot["manual_validation"]["latest_event"]["scenario_type"] in {"manual_live_restore_validation", "manual_live_safe_cleanup"}


def test_manual_live_order_fill_is_restored_without_duplicate_submission(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    service.run_action(
        "submit-order",
        {
            **_manual_auth_payload(),
            "account_hash": "hash-123",
            "symbol": "AAPL",
            "asset_class": "STOCK",
            "intent_type": "MANUAL_LIVE_PILOT",
            "side": "BUY",
            "quantity": "1",
            "order_type": "LIMIT",
            "limit_price": "101.50",
            "time_in_force": "DAY",
            "session": "NORMAL",
            "review_confirmed": True,
        },
    )
    fake_client.submitted_order_status = "FILLED"

    restarted_service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    snapshot = restarted_service.snapshot(force_refresh=True)

    assert fake_client.submitted_orders[-1]["orderType"] == "LIMIT"
    assert len(fake_client.submitted_orders) == 1
    assert snapshot["manual_live_orders"]["recent_rows"][0]["lifecycle_state"] == "FILLED"
    assert snapshot["manual_validation"]["latest_event"]["scenario_type"] in {"manual_live_filled", "manual_live_restore_validation"}


def test_completed_live_pilot_cycle_is_persisted_and_reopens_pilot_readiness(tmp_path: Path, monkeypatch) -> None:
    class FlatPilotCycleClient(FakeSchwabBrokerClient):
        def list_accounts(self, *, fields: list[str] | None = None) -> list[dict]:
            return [
                {
                    "securitiesAccount": {
                        "accountNumber": "123456789",
                        "hashValue": "hash-123",
                        "type": "MARGIN",
                        "currentBalances": {
                            "cashBalance": "15000.25",
                            "buyingPower": "45000.75",
                            "liquidationValue": "62000.50",
                            "longMarketValue": "0",
                        },
                        "positions": [],
                    }
                }
            ]

        def get_orders(
            self,
            account_hash: str,
            *,
            from_entered_time: str | None = None,
            to_entered_time: str | None = None,
            status: str | None = None,
            max_results: int | None = None,
        ) -> list[dict]:
            return []

    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_LIVE_PILOT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_STOCK_LIMIT_LIVE_SUBMIT_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_SUPPORTED_MANUAL_ORDER_TYPES", "LIMIT")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_SYMBOL_WHITELIST", "AAPL")
    monkeypatch.setattr(production_link_service, "_is_us_regular_hours", lambda now: True)

    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: FlatPilotCycleClient())
    buy_time = datetime(2026, 3, 26, 19, 22, 35, tzinfo=timezone.utc)
    close_time = datetime(2026, 3, 26, 19, 55, 35, tzinfo=timezone.utc)
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "buy-filled-1",
                    "client_order_id": "manual-buy-filled-1",
                    "account_hash": "hash-123",
                    "symbol": "AAPL",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "MANUAL_LIVE_PILOT",
                    "submitted_at": buy_time.isoformat(),
                    "acknowledged_at": buy_time.isoformat(),
                    "filled_at": buy_time.isoformat(),
                    "broker_order_status": "FILLED",
                    "lifecycle_state": "FILLED",
                    "active": False,
                },
                {
                    "broker_order_id": "close-filled-1",
                    "client_order_id": "manual-close-filled-1",
                    "account_hash": "hash-123",
                    "symbol": "AAPL",
                    "asset_class": "STOCK",
                    "side": "SELL",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "FLATTEN",
                    "submitted_at": close_time.isoformat(),
                    "acknowledged_at": close_time.isoformat(),
                    "filled_at": close_time.isoformat(),
                    "broker_order_status": "FILLED",
                    "lifecycle_state": "FILLED",
                    "active": False,
                },
            ],
            "updated_at": close_time.isoformat(),
        },
    )
    for event in (
        BrokerOrderEvent(
            account_hash="hash-123",
            broker_order_id="buy-filled-1",
            client_order_id="manual-buy-filled-1",
            event_type="submit_acknowledged",
            status="ACKNOWLEDGED",
            occurred_at=buy_time,
            message="buy ack",
            request_payload={"orderType": "LIMIT"},
            response_payload={"broker_order_id": "buy-filled-1", "status_code": 201},
            source="manual_ticket",
        ),
        BrokerOrderEvent(
            account_hash="hash-123",
            broker_order_id="buy-filled-1",
            client_order_id=None,
            event_type="status_sync",
            status="FILLED",
            occurred_at=buy_time,
            message="schwab_sync: FILLED",
            request_payload=None,
            response_payload={
                "status": "FILLED",
                "orderActivityCollection": [
                    {"executionLegs": [{"price": 253.565, "time": buy_time.isoformat()}]}
                ],
            },
            source="schwab_sync",
        ),
        BrokerOrderEvent(
            account_hash="hash-123",
            broker_order_id="close-filled-1",
            client_order_id="manual-close-filled-1",
            event_type="submit_acknowledged",
            status="ACKNOWLEDGED",
            occurred_at=close_time,
            message="close ack",
            request_payload={"orderType": "LIMIT"},
            response_payload={"broker_order_id": "close-filled-1", "status_code": 201},
            source="manual_ticket",
        ),
        BrokerOrderEvent(
            account_hash="hash-123",
            broker_order_id="close-filled-1",
            client_order_id=None,
            event_type="status_sync",
            status="FILLED",
            occurred_at=close_time,
            message="schwab_sync: FILLED",
            request_payload=None,
            response_payload={
                "status": "FILLED",
                "orderActivityCollection": [
                    {"executionLegs": [{"price": 252.78, "time": close_time.isoformat()}]}
                ],
            },
            source="schwab_sync",
        ),
    ):
        service._store.record_order_event(event)  # type: ignore[attr-defined]
    service._store.record_manual_validation_event(  # type: ignore[attr-defined]
        scenario_type="manual_live_sell_close_only_pilot",
        occurred_at=close_time,
        payload={
            "refresh_restart_proof": {
                "submit_requested_count_before_refresh": 2,
                "submit_requested_count_after_refresh": 2,
                "passive_refresh_held": True,
            }
        },
    )

    snapshot = service.snapshot(force_refresh=True)
    cycle = snapshot["pilot_cycle"]["last_completed"]

    assert snapshot["manual_order_safety"]["pilot_readiness"]["submit_eligible"] is True
    assert cycle["symbol"] == "AAPL"
    assert cycle["buy"]["broker_order_id"] == "buy-filled-1"
    assert cycle["buy"]["fill_price"] == 253.565
    assert cycle["close"]["broker_order_id"] == "close-filled-1"
    assert cycle["close"]["fill_price"] == 252.78
    assert cycle["flat_confirmation"]["confirmed"] is True
    assert cycle["reconciliation_clear_confirmation"]["confirmed"] is True
    assert cycle["passive_refresh_restart_confirmation"]["passive_refresh_held"] is True
    assert snapshot["runtime_state"]["last_completed_pilot_cycle"]["close_order_id"] == "close-filled-1"
    pilot_status_export = json.loads((tmp_path / "outputs" / "operator_dashboard" / "pilot_status_v1.json").read_text(encoding="utf-8"))
    assert pilot_status_export["last_completed_cycle"]["close_order_id"] == "close-filled-1"
    assert pilot_status_export["last_completed_cycle"]["flat_confirmation"]["confirmed"] is True


def test_manual_live_order_unsafe_opposite_side_ambiguity_enters_fault(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")

    fake_client = FakeSchwabBrokerClient()
    fake_client.dynamic_positions = [
        {
            "shortQuantity": "1",
            "averagePrice": "101.50",
            "marketValue": "-101.50",
            "currentDayProfitLoss": "0.00",
            "instrument": {
                "symbol": "TSLA",
                "description": "Tesla Inc.",
                "assetType": "EQUITY",
                "mark": "101.50",
            },
        }
    ]
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)
    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    service._store.save_runtime_state(  # type: ignore[attr-defined]
        "manual_live_orders",
        {
            "orders": [
                {
                    "broker_order_id": "broker-fault-1",
                    "client_order_id": "manual-fault-1",
                    "account_hash": "hash-123",
                    "symbol": "TSLA",
                    "asset_class": "STOCK",
                    "side": "BUY",
                    "quantity": "1",
                    "order_type": "LIMIT",
                    "intent_type": "ENTRY",
                    "created_at": stale_at,
                    "submitted_at": stale_at,
                    "acknowledged_at": stale_at,
                    "broker_order_status": "ACKNOWLEDGED",
                    "active": True,
                }
            ]
        },
    )

    snapshot = service.snapshot(force_refresh=True)

    assert snapshot["manual_live_orders"]["active_rows"][0]["lifecycle_state"] == "FAULT"
    assert snapshot["alerts"]["active"][0]["code"] == "LIVE_MANUAL_ORDER_FAULT"


def test_snapshot_refresh_does_not_autonomously_submit_live_orders(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_MANUAL_ORDER_TICKET_ENABLED", "1")
    monkeypatch.setenv("MGC_PRODUCTION_LIVE_ORDER_SUBMIT_ENABLED", "1")

    fake_client = FakeSchwabBrokerClient()
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    service.snapshot(force_refresh=True)
    service.snapshot(force_refresh=True)

    assert fake_client.submitted_orders == []


def test_production_link_broker_truth_shadow_validation_writes_read_only_artifacts(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / ".local" / "schwab" / "tokens.json"
    _write_token_file(token_path)
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("MGC_PRODUCTION_LINK_ENABLED", "1")

    fake_client = FakeSchwabBrokerClient()
    fake_client.direct_status_payloads["broker-1"] = {
        "orderId": "broker-1",
        "status": "WORKING",
        "enteredTime": "2026-03-22T20:01:00+00:00",
        "orderType": "LIMIT",
        "duration": "DAY",
        "session": "NORMAL",
        "price": "101.50",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": "1",
                "instrument": {"symbol": "AAPL", "assetType": "EQUITY", "description": "Apple Inc."},
            }
        ],
    }
    service = SchwabProductionLinkService(tmp_path, client_factory=lambda config, oauth_client: fake_client)

    payload = service.validate_broker_truth_schemas(symbol="AAPL")

    assert payload["summary"]["result"] in {"PASS", "WARN"}
    assert payload["selected_account_hash"] == "hash-123"
    assert payload["summary"]["representative_broker_order_id"] == "broker-1"
    assert payload["validations"]["order_status"]["normalized_payload"]["broker_order_id"] == "broker-1"
    assert fake_client.submitted_orders == []
    assert Path(payload["artifacts"]["json"]).exists()
    assert Path(payload["artifacts"]["markdown"]).exists()
