from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.execution_core import track_b_readiness_maintenance_supervisor as supervisor
from mgc_v05l.execution_core.track_b_readiness_maintenance_supervisor import (
    classify_readiness_maintenance,
    write_maintenance_supervisor_decision,
)


def base_inputs() -> dict[str, object]:
    return {
        "generated_at": "2026-05-18T13:30:00+00:00",
        "canonical_readiness": {
            "canonical_readiness": "READY_SUBMIT_CAPABLE",
            "ready_submit_capable": True,
            "live_money_eligible": False,
        },
        "root_guard": {"root_match": True},
        "ibkr_connectivity": {"classification": "IBKR_CONNECTED_READ_ONLY"},
        "broker_truth": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "live_money_eligible": False,
        },
        "market_data": {"classification": "MARKET_DATA_FRESH", "fresh": True},
        "runtime": {"running": True, "healthy": True, "eligible_lane_count": 3},
        "lane_quarantine": {"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0},
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "fresh": True,
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_position_count": 0,
            "live_money_eligible": False,
        },
    }


def test_ibkr_managed_accounts_timeout_recommends_retry_rotate_and_submit_block() -> None:
    inputs = base_inputs()
    inputs["ibkr_connectivity"] = {"classification": "MANAGED_ACCOUNTS_TIMEOUT"}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert {"RETRY", "ROTATE_CLIENT_ID", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert result["submit_block_required"] is True
    assert result["operator_action_required"] is False
    assert "read_only" in result["action_scope"]


def test_tws_not_listening_requires_operator_and_blocks_submit() -> None:
    inputs = base_inputs()
    inputs["ibkr_connectivity"] = {"classification": "TWS_NOT_LISTENING"}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "OPERATOR_REQUIRED"
    assert {"ALERT_OPERATOR", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert result["operator_action_required"] is True
    assert result["submit_block_required"] is True


def test_broker_truth_stale_with_last_good_preserved_refreshes_truth_and_blocks_submit() -> None:
    inputs = base_inputs()
    inputs["broker_truth"] = {
        "classification": "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED",
        "fresh": False,
        "positions_complete": True,
        "open_orders_complete": True,
        "last_successful_broker_truth": {"classification": "BROKER_TRUTH_REFRESH_READY"},
        "live_money_eligible": False,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert {"REFRESH_BROKER_TRUTH", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert _warning_codes(result) >= {"broker_truth_last_good_preserved"}


def test_expired_entry_lease_with_dead_broker_refresher_restarts_sidecar_first() -> None:
    inputs = base_inputs()
    inputs["canonical_readiness"] = {"canonical_readiness": "NOT_READY_DEPENDENCY", "live_money_eligible": False}
    inputs["broker_truth_lease"] = {
        "lease_state": "EXPIRED_BLOCK_NEW_ENTRIES",
        "submit_entry_allowed": False,
        "live_money_eligible": False,
    }
    inputs["root_guard"] = {
        "root_match": True,
        "processes": [{"name": "broker_truth_refresher", "running": False}],
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "REPAIRING_RECOMMENDED"
    assert result["recommended_actions"][:3] == ["BLOCK_SUBMIT", "REFRESH_BROKER_TRUTH", "RESTART_SIDECAR"]
    assert result["submit_block_required"] is True
    assert result["blockers"][0]["code"] == "broker_truth_lease_expired"


def test_expired_entry_lease_with_failed_latest_attempt_retries_and_rotates_client_id() -> None:
    inputs = base_inputs()
    inputs["broker_truth_lease"] = {
        "lease_state": "EXPIRED_BLOCK_NEW_ENTRIES",
        "submit_entry_allowed": False,
        "live_money_eligible": False,
    }
    inputs["broker_truth"]["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_FAILED",
        "last_failure": True,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "REPAIRING_RECOMMENDED"
    assert result["recommended_actions"][:2] == ["BLOCK_SUBMIT", "REFRESH_BROKER_TRUTH"]
    assert {"RETRY", "ROTATE_CLIENT_ID"}.issubset(result["recommended_actions"])
    assert result["retry_count"] == 1


def test_active_degraded_lease_recommends_repair_without_operator_required() -> None:
    inputs = base_inputs()
    inputs["broker_truth_lease"] = {
        "lease_state": "ACTIVE_DEGRADED_REFRESH_FAILING",
        "submit_entry_allowed": True,
        "live_money_eligible": False,
    }
    inputs["broker_truth"]["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_FAILED",
        "last_failure": True,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert {"REFRESH_BROKER_TRUTH", "RETRY", "ROTATE_CLIENT_ID"}.issubset(result["recommended_actions"])
    assert result["operator_action_required"] is False
    assert _warning_codes(result) >= {"broker_truth_lease_degraded_refresh_failing"}


def test_invalidated_unknown_open_orders_requires_operator_not_sidecar_repair() -> None:
    inputs = base_inputs()
    inputs["broker_truth_lease"] = {
        "lease_state": "INVALIDATED_UNKNOWN_OPEN_ORDERS",
        "submit_entry_allowed": False,
        "live_money_eligible": False,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "OPERATOR_REQUIRED"
    assert {"BLOCK_SUBMIT", "ALERT_OPERATOR"}.issubset(result["recommended_actions"])
    assert "RESTART_SIDECAR" not in result["recommended_actions"]
    assert result["blockers"][0]["code"] == "broker_truth_lease_unknown_open_orders"


def test_expired_lease_broker_repair_precedes_market_data_repair() -> None:
    inputs = base_inputs()
    inputs["broker_truth_lease"] = {
        "lease_state": "EXPIRED_EXITS_ONLY",
        "submit_entry_allowed": False,
        "live_money_eligible": False,
    }
    inputs["market_data"] = {"classification": "DATABENTO_SOCKET_CLOSED", "fresh": False, "socket_closed": True}

    result = classify_readiness_maintenance(inputs)

    assert result["recommended_actions"][:2] == ["BLOCK_SUBMIT", "REFRESH_BROKER_TRUTH"]
    assert "RECONNECT_MARKET_DATA" in result["recommended_actions"]
    assert result["blockers"][0]["code"] == "broker_truth_lease_expired"


def test_databento_socket_close_reconnects_market_data_and_blocks_submit() -> None:
    inputs = base_inputs()
    inputs["market_data"] = {"classification": "DATABENTO_SOCKET_CLOSED", "fresh": False, "socket_closed": True}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert {"RECONNECT_MARKET_DATA", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert result["submit_block_required"] is True


def test_databento_socket_close_can_remain_observation_only_without_submit_block_action() -> None:
    inputs = base_inputs()
    inputs["canonical_readiness"] = {"canonical_readiness": "READY_OBSERVATION_ONLY", "ready_submit_capable": False}
    inputs["runtime"] = {"running": True, "healthy": True, "eligible_lane_count": 0, "observation_only_safe": True}
    inputs["market_data"] = {"classification": "DATABENTO_SOCKET_CLOSED", "fresh": False, "socket_closed": True}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert "RECONNECT_MARKET_DATA" in result["recommended_actions"]
    assert "BLOCK_SUBMIT" not in result["recommended_actions"]
    assert result["submit_block_required"] is False
    assert _warning_codes(result) >= {"market_data_degraded_observation_only"}


def test_lane_startup_reconciliation_failure_quarantines_lane_not_fatal_when_others_eligible() -> None:
    inputs = base_inputs()
    inputs["runtime"] = {"running": True, "healthy": True, "eligible_lane_count": 2}
    inputs["lane_quarantine"] = {
        "classification": "PAPER_LANE_QUARANTINE_RECOMMENDED",
        "lane_scoped_startup_reconciliation_failure": True,
        "quarantine_count": 1,
        "quarantined_lane_ids": ["mnq-long-v5"],
        "healthy_lane_count": 2,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "DEGRADED"
    assert result["recommended_actions"] == ["QUARANTINE_LANE"]
    assert result["submit_block_required"] is False
    assert _warning_codes(result) >= {"lane_quarantine_recommended"}


def test_wrong_root_blocks_and_alerts_operator() -> None:
    inputs = base_inputs()
    inputs["root_guard"] = {"root_match": False, "wrong_root_processes": [{"pid": 123}]}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "BLOCKED"
    assert {"ALERT_OPERATOR", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert result["operator_action_required"] is True
    assert result["blockers"][0]["code"] == "wrong_root_process"


def test_stale_lifecycle_after_manual_close_uses_guarded_lifecycle_scope_only() -> None:
    inputs = base_inputs()
    inputs["reconciliation"] = {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        "fresh": True,
        "broker_reconciled": False,
        "broker_flat": True,
        "track_b_broker_position_count": 0,
        "lifecycle_open_position_count": 1,
        "review_required_count": 1,
        "guarded_cleanup_dry_run_ready": True,
        "live_money_eligible": False,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "REPAIRING_RECOMMENDED"
    assert {"REQUIRE_OPERATOR_APPROVAL", "BLOCK_SUBMIT"}.issubset(result["recommended_actions"])
    assert "operator_only" in result["action_scope"]
    assert result["authority"]["lifecycle_mutation_allowed"] == "guarded_lifecycle_tool_only_with_operator_approval"
    assert result["operator_action_required"] is True


def test_clean_ready_submit_capable_returns_recovered_no_action() -> None:
    result = classify_readiness_maintenance(base_inputs())

    assert result["supervisor_state"] == "RECOVERED"
    assert result["recommended_actions"] == ["NO_ACTION"]
    assert result["submit_block_required"] is False
    assert result["live_money_eligible"] is False


def test_live_money_eligible_true_blocks_paper_maintenance() -> None:
    inputs = base_inputs()
    inputs["canonical_readiness"] = {"canonical_readiness": "READY_SUBMIT_CAPABLE", "live_money_eligible": True}

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "BLOCKED"
    assert {"BLOCK_SUBMIT", "ALERT_OPERATOR"}.issubset(result["recommended_actions"])
    assert result["operator_action_required"] is True
    assert result["live_money_eligible"] is False
    assert result["authority"]["live_money_eligible"] is False


def test_artifact_pressure_recommends_archive_without_mutating_authority() -> None:
    inputs = base_inputs()
    inputs["artifact_pressure"] = {
        "classification": "ARTIFACT_HOT_PATH_BLOAT",
        "hot_path_bloat": True,
        "unresolved_authoritative_evidence": False,
    }

    result = classify_readiness_maintenance(inputs)

    assert result["supervisor_state"] == "REPAIRING_RECOMMENDED"
    assert result["recommended_actions"] == ["ARCHIVE_ARTIFACTS"]
    assert result["action_scope"] == ["read_only"]


def test_writer_only_writes_decision_artifact(tmp_path: Path) -> None:
    output_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"
    decision = classify_readiness_maintenance(base_inputs())

    write_maintenance_supervisor_decision(output_path=output_path, decision=decision)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "track_b_readiness_maintenance_supervisor_v1"
    assert payload["recommended_actions"] == ["NO_ACTION"]


def test_supervisor_source_has_no_broker_or_runtime_mutation_calls() -> None:
    source = inspect.getsource(supervisor)

    forbidden = [
        "placeOrder",
        "cancelOrder",
        "globalCancel",
        "reqGlobalCancel",
        "submit_limit_order",
        "transmit=True",
        "subprocess.run",
        "os.kill",
        "launchctl",
    ]
    for token in forbidden:
        assert token not in source

    assert "broker_mutation_allowed\": False" in source
    assert "order_api_allowed\": False" in source


def _warning_codes(result: dict[str, object]) -> set[str]:
    return {str(row.get("code")) for row in result.get("warnings", []) if isinstance(row, dict)}
