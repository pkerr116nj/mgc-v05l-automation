from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_leak_test_entry_order_resolution import (
    LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED,
    LEAK_TEST_ENTRY_ORDER_CANCEL_READY,
    LEAK_TEST_ENTRY_ORDER_NOT_FOUND,
    LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED,
    LeakTestEntryOrderResolutionConfig,
    resolve_known_leak_test_entry_order,
)

NOW = datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc)


def test_known_leak_test_entry_order_cancel_readiness(tmp_path: Path) -> None:
    report = resolve_known_leak_test_entry_order(
        config=LeakTestEntryOrderResolutionConfig(repo_root=tmp_path, broker_order_id="12", client_id=11940, perm_id=614043263),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == LEAK_TEST_ENTRY_ORDER_CANCEL_READY
    assert report["apply"] is False
    assert report["known_order"]["broker_order_id"] == "12"


def test_unknown_open_order_refuses_resolution(tmp_path: Path) -> None:
    report = resolve_known_leak_test_entry_order(
        config=LeakTestEntryOrderResolutionConfig(repo_root=tmp_path, broker_order_id="12"),
        now=NOW,
        reconciliation_runner=lambda _config: {
            **_reconciliation_report(known_orders=[]),
            "unknown_broker_open_order_count": 1,
            "unknown_broker_open_orders": [{"broker_order_id": "12"}],
        },
    )

    assert report["classification"] == LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED
    assert "unknown broker open orders" in str(report["detail"])


def test_missing_known_order_reports_not_found(tmp_path: Path) -> None:
    report = resolve_known_leak_test_entry_order(
        config=LeakTestEntryOrderResolutionConfig(repo_root=tmp_path, broker_order_id="99"),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == LEAK_TEST_ENTRY_ORDER_NOT_FOUND


def test_apply_cancels_exact_known_entry_order(tmp_path: Path) -> None:
    adapter = _FakeAdapter()

    report = resolve_known_leak_test_entry_order(
        config=LeakTestEntryOrderResolutionConfig(
            repo_root=tmp_path,
            broker_order_id="12",
            client_id=11940,
            perm_id=614043263,
            apply=True,
        ),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: adapter,
    )

    assert report["classification"] == LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED
    assert adapter.cancelled_order_ids == ["12"]
    assert adapter.registered_order_ids == ["12"]
    assert report["live_money_eligible"] is False
    assert report["paper_proof_invoked"] is False


def _reconciliation_report(*, known_orders: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    known_order = {
        "managed_order_status": "KNOWN_LEAK_TEST_ENTRY_ORDER_WORKING",
        "broker_order_id": "12",
        "client_id": 11940,
        "perm_id": 614043263,
        "account_id": "DUM882026",
        "symbol": "GC",
        "local_symbol": "GCM6",
        "expiry": "20260626",
        "con_id": 430360630,
        "action": "BUY",
        "quantity": "1",
        "order_type": "LMT",
        "limit_price": "4542.0",
        "tif": "DAY",
        "lane_id": "gc_1x_all_lanes__london_early_long",
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
    }
    orders = [known_order] if known_orders is None else known_orders
    return {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_LEAK_TEST_ENTRY_ORDER",
        "broker_reconciled": True,
        "review_required_count": 0,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "track_b_broker_position_count": 0,
        "track_b_broker_open_order_count": len(orders),
        "unknown_broker_open_order_count": 0,
        "known_leak_test_entry_order_count": len(orders),
        "known_leak_test_entry_orders": orders,
    }


class _FakeAdapter:
    def __init__(self) -> None:
        self.cancelled_order_ids: list[str] = []
        self.registered_order_ids: list[str] = []

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def managed_accounts(self) -> tuple[str, ...]:
        return ("DUM882026",)

    def require_configured_account(self) -> str:
        return "DUM882026"

    def register_existing_order_for_cancel(self, *, broker_order_id: str, **_kwargs: Any) -> None:
        self.registered_order_ids.append(str(broker_order_id))

    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None:
        del submit_attempt_id
        self.cancelled_order_ids.append(str(broker_order_id))

    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None:
        del submit_attempt_id, timeout_seconds
        return None

    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]:
        return {"submit_attempt_id": submit_attempt_id}
