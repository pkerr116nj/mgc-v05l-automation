from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_operator_decision_surface import (
    build_operator_decision_surface,
    write_operator_decision_surface,
)

NOW = datetime(2026, 6, 7, 6, 0, 0, tzinfo=timezone.utc)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_sources(tmp_path: Path, *, submit_allowed: bool = True, bsa_new_entry: bool = True) -> None:
    generated_at = NOW.isoformat()
    _write(
        tmp_path / "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
        {
            "schema_version": "track_b_runtime_environment_truth_v1",
            "generated_at": generated_at,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "runtime": {
                "pid_alive": True,
                "pid": 12345,
                "current_head": "abc123",
                "profile": "mnq_mes_full_session_active_evidence",
                "lane_count": 13,
                "runtime_truth_age_seconds": 5,
            },
        },
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "schema_version": "track_b_broker_truth_lease_v1",
            "generated_at": generated_at,
            "account_id": "DUM882026",
            "broker_reconciled": True,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "schema_version": "track_b_paper_broker_reconciliation_v1",
            "generated_at": generated_at,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "current_scope_review_required_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        {
            "schema_version": "track_b_canonical_readiness_v1",
            "generated_at": generated_at,
            "canonical_readiness": "READY_SUBMIT_CAPABLE" if submit_allowed else "NOT_READY_DEPENDENCY",
            "submit_allowed": submit_allowed,
            "readiness_blockers": []
            if submit_allowed
            else [
                {
                    "code": "execution_core_reconciliation_not_clean",
                    "detail": "Execution-core reconciliation is blocked.",
                    "source": "execution_core_reconciliation",
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "schema_version": "track_b_control_plane_snapshot_v1",
            "generated_at": generated_at,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "blockers": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_authority_ownership.json",
        {
            "schema_version": "track_b_broker_authority_ownership_v1",
            "generated_at": generated_at,
            "classification": "BROKER_AUTHORITY_PUBLISHER_HEALTHY",
            "authority_writer": "ibkr_broker_truth_refresher",
            "lease_bsa_generation_aligned": True,
            "running_writer_needs_reload": False,
            "duplicate_hot_writer_detected": False,
            "non_owner_hot_write_attempt_count": 0,
            "next_safe_action": "NO_ACTION",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
        {
            "schema_version": "track_b_broker_session_authority_v1",
            "generated_at": generated_at,
            "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
            if bsa_new_entry
            else "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
            "connection_mode": "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS" if bsa_new_entry else "ORDER_STATUS_UNRELIABLE",
            "allowed_uses": {"new_entry": bsa_new_entry, "managed_risk_reducing_close": False},
            "authority_blockers": []
            if bsa_new_entry
            else [{"code": "connection_not_submit_capable", "detail": "Connection mode blocks submit."}],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )


def test_ods_v1_summarizes_clean_submit_ready_state(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    _write(
        tmp_path
        / "outputs/probationary_pattern_engine/paper_session/lanes/mnq_us_active_participation_short/accepted_strategy_intent_latest.json",
        {
            "generated_at": NOW.isoformat(),
            "bridge_classification": "PAPER_STRATEGY_INTENT_ACCEPTED",
            "lane_id": "mnq_us_active_participation_short",
            "side": "SELL",
            "instrument": "MNQ",
            "decision_bar_timestamp": "2026-06-07T05:59:00+00:00",
            "signal_timestamp": "2026-06-07T05:59:05+00:00",
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["runtime_live"]["state"] == "LIVE"
    assert ods["broker_state"] == "FLAT"
    assert ods["submit_allowed"] == {"submit_allowed": True, "canonical_readiness": "READY_SUBMIT_CAPABLE"}
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"
    assert ods["authority_health"]["lease_bsa_aligned"] is True
    assert ods["latest_accepted_signal"]["lane_id"] == "mnq_us_active_participation_short"
    assert ods["latest_accepted_signal"]["stage"] == "PAPER_STRATEGY_INTENT_ACCEPTED"
    assert ods["broker_mutation_allowed"] is False
    assert ods["submit_attempted"] is False


def test_canonical_readiness_owns_submit_allowed_and_root_blocker(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=False, bsa_new_entry=True)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["submit_allowed"]["submit_allowed"] is False
    assert ods["first_blocker"] == {
        "code": "execution_core_reconciliation_not_clean",
        "detail": "Execution-core reconciliation is blocked.",
        "source": "execution_core_reconciliation",
    }
    assert ods["next_safe_action"] == "REFRESH_AUTHORITY"


def test_bsa_blocks_new_entry_when_canonical_otherwise_ready(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=True, bsa_new_entry=False)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["first_blocker"] == {
        "code": "connection_not_submit_capable",
        "detail": "Connection mode blocks submit.",
        "source": "broker_session_authority",
    }
    assert ods["next_safe_action"] == "REFRESH_AUTHORITY"


def test_broker_open_orders_beat_projection_ready_state(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["track_b_broker_open_order_count"] = 1
    _write(lease_path, lease)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "OPEN_ORDERS"
    assert ods["first_blocker"]["code"] == "broker_state_open_orders"
    assert ods["first_blocker"]["source"] == "broker_state"


def test_missing_or_stale_sources_are_unknown(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    runtime_path = tmp_path / "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json"
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    runtime["generated_at"] = "2026-06-07T05:00:00+00:00"
    _write(runtime_path, runtime)
    (tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json").unlink()

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["runtime_live"]["state"] == "UNKNOWN"
    assert ods["broker_state"] == "UNKNOWN"
    assert ods["first_blocker"]["code"] == "broker_state_unknown"


def test_reload_needed_maps_to_reload_authority_refresher_action(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    ownership_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_authority_ownership.json"
    ownership = json.loads(ownership_path.read_text(encoding="utf-8"))
    ownership["running_writer_needs_reload"] = True
    ownership["classification"] = "BROKER_AUTHORITY_PUBLISHER_RELOAD_REQUIRED"
    ownership["next_safe_action"] = "RELOAD_AUTHORITY_REFRESHER_SERVICE"
    _write(ownership_path, ownership)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["authority_health"]["reload_needed"] is True
    assert ods["next_safe_action"] == "RELOAD_AUTHORITY_REFRESHER_SERVICE"


def test_write_operator_decision_surface_writes_one_canonical_artifact(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    output_path = tmp_path / "outputs/track_b_execution_core/operator_decision_surface/latest_operator_decision_surface.json"

    payload = write_operator_decision_surface(repo_root=tmp_path, output_path=output_path, now=NOW)

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == "track_b_operator_decision_surface_v1"
    assert written["generated_at"] == payload["generated_at"]
    assert written["read_only"] is True
