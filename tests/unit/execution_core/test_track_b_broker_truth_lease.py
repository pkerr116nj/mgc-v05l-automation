from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_broker_truth_lease as lease_module
from mgc_v05l.execution_core.track_b_broker_truth_lease import (
    classify_broker_truth_lease,
    write_broker_truth_lease,
)


NOW = "2026-05-18T15:00:00+00:00"
TRUTH_TIME = "2026-05-18T14:58:00+00:00"
RECON_TIME = "2026-05-18T14:58:30+00:00"


def base_inputs() -> dict[str, object]:
    return {
        "account_id": "DUM882026",
        "allowed_instruments": ["MGC", "MNQ"],
        "current_time": NOW,
        "policy": {
            "max_entry_age_seconds": 300,
            "max_exit_age_seconds": 900,
            "degraded_refresh_grace_seconds": 120,
        },
        "last_successful_broker_truth": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "generated_at": TRUTH_TIME,
            "positions_complete": True,
            "open_orders_complete": True,
            "position_count": 2,
            "open_order_count": 0,
            "positions": [
                {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "0.0"},
                {"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "0.0"},
            ],
            "open_orders": [],
            "positions_snapshot_path": "outputs/reports/positions.json",
            "open_orders_snapshot_path": "outputs/reports/open_orders.json",
            "live_money_eligible": False,
            "submit_authority": False,
        },
        "latest_attempt_status": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "generated_at": TRUTH_TIME,
            "positions_complete": True,
            "open_orders_complete": True,
            "positions": [
                {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "0.0"},
                {"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "0.0"},
            ],
            "open_orders": [],
            "live_money_eligible": False,
        },
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": RECON_TIME,
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": True},
            "live_money_eligible": False,
            "submit_authority": False,
        },
        "lifecycle": {
            "generated_at": RECON_TIME,
            "open_position_count": 0,
            "open_positions": [],
            "manual_broker_action_detected": False,
            "live_money_eligible": False,
        },
        "order_state": {
            "generated_at": RECON_TIME,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
        "source_artifact_paths": {
            "broker_truth_status": "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
            "reconciliation": "outputs/reports/track_b_paper_broker_reconciliation/latest.json",
        },
    }


def test_active_lease_from_fresh_truth_and_clean_reconciliation() -> None:
    result = classify_broker_truth_lease(base_inputs())

    assert result["lease_state"] == "ACTIVE"
    assert result["submit_entry_allowed"] is True
    assert result["submit_exit_allowed"] is True
    assert result["valid_until"] == "2026-05-18T15:03:00+00:00"
    assert result["exit_valid_until"] == "2026-05-18T15:13:00+00:00"
    assert result["live_money_eligible"] is False
    assert result["operator_action_required"] is False
    assert result["blockers"] == []


def test_failed_latest_attempt_degrades_unexpired_lease_without_invalidation() -> None:
    inputs = base_inputs()
    inputs["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_FAILED",
        "generated_at": "2026-05-18T14:59:00+00:00",
        "last_failure": True,
        "last_success": False,
        "last_error": "TWS paper API error 502",
        "live_money_eligible": False,
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "ACTIVE_DEGRADED_REFRESH_FAILING"
    assert result["submit_entry_allowed"] is True
    assert result["submit_exit_allowed"] is True
    assert _warning_codes(result) >= {"broker_truth_refresh_failing"}
    assert result["contradiction_details"] == []


def test_entry_expiry_blocks_new_entries() -> None:
    inputs = base_inputs()
    inputs["current_time"] = "2026-05-18T15:04:00+00:00"

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "EXPIRED_BLOCK_NEW_ENTRIES"
    assert result["submit_entry_allowed"] is False
    assert result["submit_exit_allowed"] is False
    assert _blocker_codes(result) >= {"entry_lease_expired"}


def test_expired_entry_allows_lifecycle_owned_exit_inside_exit_window() -> None:
    inputs = base_inputs()
    inputs["current_time"] = "2026-05-18T15:04:00+00:00"
    inputs["last_successful_broker_truth"] = {
        **dict(inputs["last_successful_broker_truth"]),
        "positions": [{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0"}],
    }
    inputs["latest_attempt_status"] = {
        **dict(inputs["latest_attempt_status"]),
        "positions": [{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0"}],
    }
    inputs["reconciliation"] = {
        **dict(inputs["reconciliation"]),
        "track_b_broker_position_count": 1,
        "lifecycle_open_position_count": 1,
        "position_match_report": {"state": "BROKER_AND_LIFECYCLE_MATCH", "matched": True},
    }
    inputs["lifecycle"] = {
        **dict(inputs["lifecycle"]),
        "open_position_count": 1,
        "owned_open_position_count": 1,
        "open_positions": [{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "owned": True}],
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "EXPIRED_EXITS_ONLY"
    assert result["submit_entry_allowed"] is False
    assert result["submit_exit_allowed"] is True


def test_unknown_open_orders_invalidate_immediately() -> None:
    inputs = base_inputs()
    inputs["reconciliation"] = {
        **dict(inputs["reconciliation"]),
        "unknown_broker_open_order_count": 1,
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "INVALIDATED_UNKNOWN_OPEN_ORDERS"
    assert result["submit_entry_allowed"] is False
    assert result["submit_exit_allowed"] is False
    assert result["operator_action_required"] is True


def test_unexpected_broker_position_invalidates_immediately() -> None:
    inputs = base_inputs()
    inputs["last_successful_broker_truth"] = {
        **dict(inputs["last_successful_broker_truth"]),
        "positions": [{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0"}],
    }
    inputs["latest_attempt_status"] = {}

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "INVALIDATED_CONTRADICTION"
    assert _contradiction_codes(result) >= {"unexpected_broker_position"}
    assert result["submit_entry_allowed"] is False


def test_manual_broker_action_lifecycle_mismatch_invalidates_until_cleanup() -> None:
    inputs = base_inputs()
    inputs["lifecycle"] = {
        **dict(inputs["lifecycle"]),
        "manual_close_detected": True,
        "stale_after_manual_close": True,
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "INVALIDATED_MANUAL_BROKER_ACTION"
    assert _contradiction_codes(result) >= {"manual_broker_action"}
    assert result["operator_action_required"] is True


def test_contradictory_successful_latest_truth_beats_last_good_truth() -> None:
    inputs = base_inputs()
    inputs["current_time"] = "2026-05-18T15:01:00+00:00"
    inputs["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": "2026-05-18T15:00:30+00:00",
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": [{"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "-1.0"}],
        "open_orders": [],
        "live_money_eligible": False,
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "INVALIDATED_CONTRADICTION"
    assert _contradiction_codes(result) >= {"unexpected_broker_position"}
    assert result["submit_entry_allowed"] is False


def test_live_money_eligibility_blocks_paper_lease() -> None:
    inputs = base_inputs()
    inputs["reconciliation"] = {
        **dict(inputs["reconciliation"]),
        "live_money_eligible": True,
    }

    result = classify_broker_truth_lease(inputs)

    assert result["lease_state"] == "INVALIDATED_CONTRADICTION"
    assert result["live_money_eligible"] is False
    assert result["submit_entry_allowed"] is False
    assert _contradiction_codes(result) >= {"live_money_eligible_enabled"}


def test_writer_outputs_latest_and_history(tmp_path: Path) -> None:
    result = classify_broker_truth_lease(base_inputs())
    latest_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
    history_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "broker_truth_lease_history.jsonl"

    write_broker_truth_lease(output_path=latest_path, lease=result, history_path=history_path)

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    history_rows = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
    assert latest["schema_version"] == "track_b_broker_truth_lease_v1"
    assert latest["lease_state"] == "ACTIVE"
    assert history_rows[-1]["lease_id"] == latest["lease_id"]


def test_source_has_no_broker_order_or_runtime_action_calls() -> None:
    source = inspect.getsource(lease_module)

    forbidden = [
        "place" + "Order",
        "cancel" + "Order",
        "global" + "Cancel",
        "req" + "Global" + "Cancel",
        "submit" + "_limit_order",
        "sub" + "process" + ".",
        "os" + ".kill",
        "launch" + "ctl",
        "EC" + "lient",
    ]
    for token in forbidden:
        assert token not in source

    assert "write_broker_truth_lease" in source
    assert "live_money_eligible" in source


def _warning_codes(result: dict[str, object]) -> set[str]:
    return {str(row.get("code")) for row in result.get("warnings", []) if isinstance(row, dict)}


def _blocker_codes(result: dict[str, object]) -> set[str]:
    return {str(row.get("code")) for row in result.get("blockers", []) if isinstance(row, dict)}


def _contradiction_codes(result: dict[str, object]) -> set[str]:
    return {str(row.get("code")) for row in result.get("contradiction_details", []) if isinstance(row, dict)}
