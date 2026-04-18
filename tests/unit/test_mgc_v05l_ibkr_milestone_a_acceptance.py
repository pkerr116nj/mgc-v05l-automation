from __future__ import annotations

from mgc_v05l.app.ibkr_milestone_a_acceptance import evaluate_ibkr_milestone_a_snapshot


def test_ibkr_milestone_a_acceptance_passes_for_ready_truth_snapshot() -> None:
    snapshot = {
        "provider_id": "ibkr_execution",
        "selected_account_id": "DU1234567",
        "accounts": [{"account_id": "DU1234567", "display_name": "IBKR DU1234567"}],
        "balances": [{"account_id": "DU1234567", "buying_power": "100000"}],
        "positions": [{"account_id": "DU1234567", "symbol": "MGC", "quantity": "1", "side": "LONG"}],
        "open_orders": [{"account_id": "DU1234567", "broker_order_id": "7001", "symbol": "MGC", "status": "Submitted"}],
        "completed_orders": [{"account_id": "DU1234567", "broker_order_id": "6998", "symbol": "MGC", "status": "Filled"}],
        "executions": [{"account_id": "DU1234567", "execution_id": "exec-1", "symbol": "MGC", "quantity": "1"}],
        "health": {"connected": True, "checked_at": "2026-04-12T15:00:00+00:00"},
        "generated_at": "2026-04-12T15:00:00+00:00",
        "metadata": {"visibility_scope": {"managed_accounts": ["DU1234567"]}},
        "connected": True,
        "truth_complete": True,
        "position_quantity": 1,
        "open_order_ids": ["7001"],
        "order_status": {"7001": "Submitted"},
        "last_fill_timestamp": "2026-04-12T15:00:00+00:00",
    }

    result = evaluate_ibkr_milestone_a_snapshot(snapshot)

    assert result["ready"] is True
    assert all(check["ok"] for check in result["checks"].values())


def test_ibkr_milestone_a_acceptance_flags_missing_scope_and_reconciliation_bridge() -> None:
    snapshot = {
        "provider_id": "ibkr_execution",
        "selected_account_id": "DU1234567",
        "accounts": [],
        "balances": [],
        "positions": [],
        "open_orders": [],
        "completed_orders": [],
        "executions": [],
        "health": {"connected": False, "checked_at": None},
        "generated_at": None,
        "metadata": {"visibility_scope": {"managed_accounts": []}},
    }

    result = evaluate_ibkr_milestone_a_snapshot(snapshot)

    assert result["ready"] is False
    assert result["checks"]["health_connected"]["ok"] is False
    assert result["checks"]["account_truth_present"]["ok"] is False
    assert result["checks"]["balances_present_for_scope"]["ok"] is False
    assert result["checks"]["reconciliation_ingestion_ready"]["ok"] is False

