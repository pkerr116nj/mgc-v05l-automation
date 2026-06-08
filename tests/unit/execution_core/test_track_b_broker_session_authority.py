from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_broker_session_authority as authority_module
from mgc_v05l.execution_core.track_b_broker_session_authority import (
    build_broker_session_authority,
    diagnostic_probe_policy,
    should_use_published_authority_for_diagnostic,
    write_broker_session_authority,
)
from mgc_v05l.execution_core.track_b_broker_truth_lease import classify_broker_truth_lease


NOW = "2026-05-18T15:00:00+00:00"
TRUTH_TIME = "2026-05-18T14:58:00+00:00"


def _base_inputs() -> dict[str, object]:
    return {
        "account_id": "DUM882026",
        "allowed_instruments": ["MNQ", "MES"],
        "current_time": NOW,
        "policy": {
            "max_entry_age_seconds": 300,
            "max_exit_age_seconds": 900,
            "max_position_lease_age_seconds": 300,
            "max_open_order_lease_age_seconds": 300,
            "max_fill_evidence_lease_age_seconds": 300,
        },
        "last_successful_broker_truth": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "generated_at": TRUTH_TIME,
            "positions_complete": True,
            "open_orders_complete": True,
            "position_count": 0,
            "open_order_count": 0,
            "positions": [],
            "open_orders": [],
            "client_id": 9077,
            "pid": 54210,
            "server_version": 157,
            "live_money_eligible": False,
            "submit_authority": False,
        },
        "latest_attempt_status": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "generated_at": TRUTH_TIME,
            "positions_complete": True,
            "open_orders_complete": True,
            "positions": [],
            "open_orders": [],
            "client_id": 9077,
            "pid": 54210,
            "live_money_eligible": False,
        },
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": TRUTH_TIME,
            "broker_reconciled": True,
            "current_scope_review_required_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "live_money_eligible": False,
            "submit_authority": False,
        },
        "lifecycle": {
            "generated_at": TRUTH_TIME,
            "open_position_count": 0,
            "open_positions": [],
            "live_money_eligible": False,
        },
        "order_state": {
            "generated_at": TRUTH_TIME,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "order_status_callbacks_complete": True,
            "last_order_status_at": TRUTH_TIME,
            "live_money_eligible": False,
        },
        "source_artifact_paths": {"broker_truth_status": "outputs/reports/status.json"},
    }


def test_session_authority_publishes_owner_metadata_and_schema() -> None:
    lease = classify_broker_truth_lease(_base_inputs())
    lease["authority_generation_id"] = "ibkr-broker-truth-refresher-20260518T150000Z"
    lease["authority_writer"] = "ibkr_broker_truth_refresher"
    lease["authority_source_timestamp"] = TRUTH_TIME

    authority = build_broker_session_authority(
        lease=lease,
        generated_at=NOW,
        source_lease_path="outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
    )

    assert authority["schema_version"] == "track_b_broker_session_authority_v1"
    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE"
    assert authority["authority_generation_id"] == lease["authority_generation_id"]
    assert authority["authority_writer"] == "ibkr_broker_truth_refresher"
    assert authority["authority_source_timestamp"] == TRUTH_TIME
    assert authority["broker_session_owner"]["pid"] == 54210
    assert authority["broker_session_owner"]["client_id"] == 9077
    assert authority["pid"] == 54210
    assert authority["client_id"] == 9077
    assert authority["server_version"] == 157
    assert authority["last_position_at"] == TRUTH_TIME
    assert authority["last_open_order_at"] == TRUTH_TIME
    assert authority["last_order_status_at"] == TRUTH_TIME
    assert authority["position_snapshot_timestamp"] == TRUTH_TIME
    assert authority["open_order_snapshot_timestamp"] == TRUTH_TIME
    assert authority["callback_timestamps"]["last_order_status_at"] == TRUTH_TIME
    assert authority["allowed_uses"]["new_entry"] is True
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is True
    assert authority["broker_mutation_allowed"] is False
    assert authority["live_money_eligible"] is False
    assert authority["paper_proof_invoked"] is False


def test_split_submit_position_client_ownership_classifies_degraded_diagnostic() -> None:
    lease = classify_broker_truth_lease(_base_inputs())

    authority = build_broker_session_authority(
        lease=lease,
        generated_at=NOW,
        observed_submit_client_ids=[11121, 11143],
    )

    assert authority["split_session_ownership"]["detected"] is True
    assert authority["split_session_ownership"]["broker_truth_client_id"] == 9077
    assert authority["split_session_ownership"]["observed_submit_client_ids"] == [11121, 11143]
    assert authority["split_session_ownership"]["classification"] == "SPLIT_BROKER_SESSION_OWNERSHIP"
    assert authority["allowed_uses"]["new_entry"] is True


def test_missing_order_status_blocks_submit_and_close_even_with_fresh_open_orders() -> None:
    inputs = _base_inputs()
    inputs["order_state"] = {
        **dict(inputs["order_state"]),
        "classification": "ORDER_TRUTH_STALE",
        "order_status_callbacks_complete": False,
        "last_order_status_at": None,
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["connection_mode"] == "ORDER_STATUS_UNRELIABLE"
    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert authority["allowed_uses"]["new_entry"] is False
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False
    assert authority["callback_ownership_attribution"]["classification"] == "CALLBACK_ATTRIBUTION_GAP"
    assert authority["callback_missing_reason"] == "order_status_callback_missing"
    assert _blocker_codes(authority) >= {"order_status_unreliable_blocks_submit_and_close"}


def test_degraded_exact_risk_reducing_close_is_published_without_new_entry() -> None:
    lease = {
        "lease_state": "ACTIVE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "submit_entry_allowed": False,
        "submit_exit_allowed": True,
        "allowed_uses": {
            "new_entry": False,
            "managed_risk_reducing_close": True,
            "broker_observed_adoption_diagnosis": True,
            "fill_callback_adoption": False,
        },
        "connection_health": {
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "position_truth_available": True,
            "order_status_reliable": False,
            "fill_callback_capable": False,
            "broker_observed_adoption_diagnosis_allowed": True,
            "callback_ownership_attribution": {
                "classification": "CALLBACK_ATTRIBUTION_GAP",
                "callback_missing_reason": "order_status_callback_missing",
            },
        },
        "callback_ownership_attribution": {
            "classification": "CALLBACK_ATTRIBUTION_GAP",
            "callback_missing_reason": "order_status_callback_missing",
        },
        "degraded_exact_risk_reducing_close_context": {
            "ready": True,
            "broker_position_exactly_one": True,
            "broker_open_orders_zero": True,
            "unknown_open_orders_zero": True,
            "broker_lifecycle_reconciled": True,
        },
    }

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert authority["allowed_uses"]["new_entry"] is False
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is True
    assert authority["risk_reducing_close_connection_mode"] == "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED"
    assert authority["degraded_exact_risk_reducing_close_context"]["ready"] is True
    assert authority["callback_missing_reason"] == "order_status_callback_missing"


def test_degraded_exact_risk_reducing_close_requires_lease_allowed_use() -> None:
    lease = {
        "lease_state": "ACTIVE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "submit_exit_allowed": False,
        "allowed_uses": {"new_entry": False, "managed_risk_reducing_close": False},
        "connection_health": {
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "callback_ownership_attribution": {
                "classification": "CALLBACK_ATTRIBUTION_GAP",
                "callback_missing_reason": "order_status_callback_missing",
            },
        },
        "callback_ownership_attribution": {
            "classification": "CALLBACK_ATTRIBUTION_GAP",
            "callback_missing_reason": "order_status_callback_missing",
        },
        "degraded_exact_risk_reducing_close_context": {"ready": True},
    }

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["allowed_uses"]["new_entry"] is False
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False


def test_split_callback_ownership_is_published_with_specific_blocker_reason() -> None:
    inputs = _base_inputs()
    inputs["order_state"] = {
        **dict(inputs["order_state"]),
        "last_order_status_at": TRUTH_TIME,
        "last_order_status_client_id": 17086,
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert authority["position_truth_client_id"] == 9077
    assert authority["open_order_truth_client_id"] == 9077
    assert authority["last_order_status_client_id"] == 17086
    assert authority["session_match"]["position_vs_order_status_same_session"] is False
    assert authority["callback_ownership_attribution"]["classification"] == "SPLIT_CALLBACK_OWNERSHIP"
    assert authority["callback_missing_reason"] == "position_order_status_client_mismatch"
    blocker = next(row for row in authority["authority_blockers"] if row["code"] == "order_status_unreliable_blocks_submit_and_close")
    assert "position_order_status_client_mismatch" in blocker["detail"]


def test_aligned_callback_ownership_keeps_stronger_submit_capable_classification() -> None:
    inputs = _base_inputs()
    inputs["order_state"] = {
        **dict(inputs["order_state"]),
        "last_order_status_at": TRUTH_TIME,
        "last_order_status_client_id": 9077,
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE"
    assert authority["callback_ownership_attribution"]["classification"] == "CALLBACK_OWNERSHIP_ALIGNED"
    assert authority["session_match"]["position_vs_order_status_same_session"] is True
    assert authority["callback_age_seconds"]["order_status"] == 120.0
    assert authority["allowed_uses"]["new_entry"] is True
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is True


def test_flat_no_order_submit_capable_without_recent_order_events_publishes_new_entry_only() -> None:
    inputs = _base_inputs()
    inputs["order_state"] = {
        "generated_at": TRUTH_TIME,
        "classification": "NO_OPEN_ORDERS",
        "unknown_open_order_count": 0,
        "unresolved_intent_count": 0,
        "open_order_end_observed": True,
        "live_money_eligible": False,
    }
    inputs["reconciliation"] = {
        **dict(inputs["reconciliation"]),
        "current_scope_lifecycle_open_position_count": 0,
        "current_scope_lifecycle_open_order_count": 0,
        "managed_position_count": 0,
        "managed_open_position_count": 0,
        "owner_resolution": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0},
    }
    inputs["submit_session_readiness"] = {
        "submit_session_ready": True,
        "next_valid_id_received": True,
        "next_valid_id": 1001,
        "managed_accounts_observed": True,
        "managed_accounts": ["DUM882026"],
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["connection_mode"] == "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
    assert authority["allowed_uses"]["new_entry"] is True
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False
    assert authority["connection_allowed_uses"]["new_entry_connection"] is True
    assert authority["connection_allowed_uses"]["managed_risk_reducing_close_connection"] is False
    assert authority["callback_health"]["fill_callback_capable"] is False


def test_flat_no_order_submit_ready_lease_overrides_missing_order_status_for_new_entry() -> None:
    lease = {
        "lease_state": "ACTIVE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": False,
        "allowed_uses": {
            "new_entry": True,
            "managed_risk_reducing_close": False,
            "broker_observed_adoption_diagnosis": True,
            "fill_callback_adoption": False,
        },
        "connection_health": {
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "position_truth_available": True,
            "order_status_reliable": False,
            "fill_callback_capable": False,
            "broker_observed_adoption_diagnosis_allowed": True,
            "flat_no_order_submit_capable_context": {
                "ready": True,
                "broker_flat": True,
                "broker_no_open_orders": True,
                "open_order_end_observed": True,
                "submit_session_liveness_proven": True,
            },
            "callback_ownership_attribution": {
                "classification": "CALLBACK_ATTRIBUTION_GAP",
                "callback_missing_reason": "order_status_callback_missing",
            },
        },
        "callback_ownership_attribution": {
            "classification": "CALLBACK_ATTRIBUTION_GAP",
            "callback_missing_reason": "order_status_callback_missing",
        },
    }

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["connection_mode"] == "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
    assert authority["allowed_uses"]["new_entry"] is True
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False
    assert authority["callback_ownership_attribution"]["classification"] == "CALLBACK_ATTRIBUTION_GAP"
    assert authority["callback_missing_reason"] == "order_status_callback_missing"
    assert "order_status_unreliable_blocks_submit_and_close" not in _blocker_codes(authority)


def test_flat_no_order_lease_with_new_entry_false_keeps_bsa_blocked() -> None:
    lease = {
        "lease_state": "ACTIVE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "submit_entry_allowed": False,
        "allowed_uses": {"new_entry": False, "managed_risk_reducing_close": False},
        "connection_health": {
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "position_truth_available": True,
            "order_status_reliable": False,
            "flat_no_order_submit_capable_context": {
                "ready": False,
                "flat_no_order_candidate": True,
                "submit_session_liveness_proven": False,
            },
            "callback_ownership_attribution": {
                "classification": "CALLBACK_ATTRIBUTION_GAP",
                "callback_missing_reason": "order_status_callback_missing",
            },
        },
        "callback_ownership_attribution": {
            "classification": "CALLBACK_ATTRIBUTION_GAP",
            "callback_missing_reason": "order_status_callback_missing",
        },
    }

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert authority["allowed_uses"]["new_entry"] is False
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False
    assert _blocker_codes(authority) >= {"order_status_unreliable_blocks_submit_and_close"}


def test_flat_no_order_missing_submit_session_liveness_publishes_specific_blocker() -> None:
    inputs = _base_inputs()
    inputs["order_state"] = {
        "generated_at": TRUTH_TIME,
        "classification": "NO_OPEN_ORDERS",
        "unknown_open_order_count": 0,
        "unresolved_intent_count": 0,
        "open_order_end_observed": True,
        "live_money_eligible": False,
    }
    inputs["reconciliation"] = {
        **dict(inputs["reconciliation"]),
        "current_scope_lifecycle_open_position_count": 0,
        "current_scope_lifecycle_open_order_count": 0,
        "managed_position_count": 0,
        "managed_open_position_count": 0,
        "owner_resolution": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0},
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert authority["allowed_uses"]["new_entry"] is False
    assert _blocker_codes(authority) >= {"submit_session_not_proven", "order_status_unreliable_blocks_submit_and_close"}


def test_position_truth_only_allows_adoption_diagnosis_only() -> None:
    inputs = _base_inputs()
    inputs["connection_mode"] = "POSITION_TRUTH_ONLY"
    inputs["submit_ownership"] = {"available": True, "trade_id": "trade-mes", "lifecycle_id": "life-mes"}
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["classification"] == "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY"
    assert authority["allowed_uses"]["new_entry"] is False
    assert authority["allowed_uses"]["managed_risk_reducing_close"] is False
    assert authority["allowed_uses"]["broker_observed_adoption_diagnosis"] is True
    assert authority["connection_allowed_uses"]["broker_observed_adoption_diagnosis_connection"] is True
    assert authority["callback_ownership_attribution"]["classification"] == "CALLBACK_OWNERSHIP_UNKNOWN"


def test_missing_fill_callback_is_visible_as_callback_health() -> None:
    inputs = _base_inputs()
    inputs["fill_evidence"] = {
        "classification": "FILL_CALLBACK_MISSING",
        "generated_at": TRUTH_TIME,
        "fill_callbacks_complete": False,
    }
    lease = classify_broker_truth_lease(inputs)

    authority = build_broker_session_authority(lease=lease, generated_at=NOW)

    assert authority["callback_health"]["missing_execution_callbacks_visible"] is True
    assert authority["allowed_uses"]["fill_callback_adoption"] is False
    assert authority["connection_health"]["fill_callback_capable"] is False


def test_active_exposure_diagnostic_policy_prefers_published_authority() -> None:
    lease = classify_broker_truth_lease(_base_inputs())
    authority = build_broker_session_authority(lease=lease, generated_at=NOW, active_track_b_exposure=True)

    assert authority["diagnostics_policy"]["active_track_b_exposure"] is True
    assert authority["diagnostics_policy"]["reconnect_probe_allowed"] is False
    assert should_use_published_authority_for_diagnostic(authority) is True


def test_no_exposure_policy_keeps_operator_probe_diagnostic_only() -> None:
    policy = diagnostic_probe_policy(active_track_b_exposure=False)

    assert policy["classification"] == "DIAGNOSTIC_OPERATOR_PROBE_ALLOWED_NO_ACTIVE_EXPOSURE"
    assert policy["operator_diagnostic_only"] is True
    assert policy["independent_ibkr_probe_hot_path_authority"] is False


def test_writer_outputs_latest_and_history(tmp_path: Path) -> None:
    lease = classify_broker_truth_lease(_base_inputs())
    authority = build_broker_session_authority(lease=lease, generated_at=NOW)
    latest_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"
    history_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "broker_session_authority_history.jsonl"

    write_broker_session_authority(output_path=latest_path, authority=authority, history_path=history_path)

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    history_rows = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
    assert latest["schema_version"] == "track_b_broker_session_authority_v1"
    assert history_rows[-1]["source_lease_id"] == latest["source_lease_id"]


def test_source_has_no_broker_api_process_or_runtime_mutation_paths() -> None:
    source = inspect.getsource(authority_module)

    forbidden = [
        "place" + "Order",
        "cancel" + "Order",
        "req" + "Global" + "Cancel",
        "submit" + "_limit_order",
        "sub" + "process" + ".",
        "os" + ".kill",
        "launch" + "ctl",
        "EC" + "lient",
    ]
    for token in forbidden:
        assert token not in source

    assert "write_broker_session_authority" in source
    assert "broker_mutation_allowed" in source


def _blocker_codes(authority: dict[str, object]) -> set[str]:
    return {str(row.get("code")) for row in authority.get("authority_blockers", []) if isinstance(row, dict)}
