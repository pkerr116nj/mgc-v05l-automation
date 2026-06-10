from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "schema_version": "track_b_open_order_truth_v1",
            "generated_at": generated_at,
            "classification": "NO_OPEN_ORDERS",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": generated_at,
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_orders_v1",
            "generated_at": generated_at,
            "classification": "NO_MANAGED_ORDERS",
            "managed_orders": [],
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


def _seed_managed_mes_position(tmp_path: Path) -> None:
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "schema_version": "track_b_broker_truth_lease_v1",
            "generated_at": NOW.isoformat(),
            "account_id": "DUM882026",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "positions": [{"local_symbol": "MESM6", "quantity": "1.0", "account_id": "DUM882026"}],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "schema_version": "track_b_paper_broker_reconciliation_v1",
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 1,
            "current_exposure_owner_resolution": {"classification": "OWNED_MANAGED_EXPOSURE"},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [
                {
                    "local_symbol": "MESM6",
                    "quantity": "1",
                    "side": "LONG",
                    "lifecycle_id": "reserved_submit_mes_globex_active_participation_long",
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_orders_v1",
            "generated_at": NOW.isoformat(),
            "classification": "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
            "managed_orders": [],
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


def test_ods_prefers_fresh_current_scope_state_over_stale_projection_blockers(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=True, bsa_new_entry=False)
    _write(
        tmp_path / "outputs/track_b_execution_core/current_scope_state/latest_current_scope_state.json",
        {
            "schema_version": "track_b_current_scope_state_v1",
            "generated_at": NOW.isoformat(),
            "classification": "FLAT",
            "broker_state_for_ods": "FLAT",
            "first_blocker_for_ods": None,
            "next_safe_action_for_ods": "NO_ACTION",
            "canonical_readiness": {
                "classification": "READY_SUBMIT_CAPABLE",
                "submit_allowed": True,
            },
            "diagnostics": [
                {
                    "code": "shared_truth_stale",
                    "source": "shared_truth",
                    "diagnostic_only": True,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["submit_allowed"] == {
        "canonical_readiness": "READY_SUBMIT_CAPABLE",
        "submit_allowed": True,
        "source": "current_scope_state",
    }
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"
    assert ods["current_scope_state"]["classification"] == "FLAT"


def test_degraded_refresher_failure_overrides_submit_and_surfaces_blocker(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=True, bsa_new_entry=True)
    _seed_managed_mes_position(tmp_path)
    _write(
        tmp_path / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {
                    "step": "shared_truth",
                    "code": "shared_truth_refresh_failed",
                    "returncode": 2,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "EXPOSED_MANAGED"
    assert ods["submit_allowed"] == {
        "submit_allowed": False,
        "canonical_readiness": "READY_SUBMIT_CAPABLE",
        "degraded": True,
        "degraded_reason": "shared_truth_refresh_failed",
    }
    assert ods["first_blocker"] == {
        "code": "shared_truth_refresh_failed",
        "detail": "shared_truth refresh failed with returncode=2.",
        "source": "operator_readiness_refresher",
    }
    assert ods["next_safe_action"] == "REFRESH_AUTHORITY"


def test_later_clean_current_authority_supersedes_earlier_refresher_failure(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=True, bsa_new_entry=True)
    _write(
        tmp_path / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": (NOW - timedelta(seconds=120)).isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {
                    "step": "track_b_paper_broker_reconciliation",
                    "code": "track_b_paper_broker_reconciliation_refresh_failed",
                    "returncode": 1,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["submit_allowed"] == {"submit_allowed": True, "canonical_readiness": "READY_SUBMIT_CAPABLE"}
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"
    assert ods["diagnostic_warnings"] == [
        {
            "code": "operator_readiness_refresh_failure_superseded",
            "detail": "Earlier operator readiness refresh failure was superseded by newer clean current authority.",
            "source": "operator_readiness_refresher",
            "superseded_blocker": {
                "code": "track_b_paper_broker_reconciliation_refresh_failed",
                "detail": "track_b_paper_broker_reconciliation refresh failed with returncode=1.",
                "source": "operator_readiness_refresher",
            },
        }
    ]


def test_flat_reconciliation_supersedes_refresh_failure_when_broker_lease_is_stale(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=True, bsa_new_entry=True)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease.update(
        {
            "generated_at": (NOW - timedelta(seconds=600)).isoformat(),
            "broker_reconciled": False,
            "lease_state": "INVALIDATED_CONTRADICTION",
            "blockers": ["stale_lifecycle_projection"],
        }
    )
    _write(lease_path, lease)
    _write(
        tmp_path / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": (NOW - timedelta(seconds=120)).isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {
                    "step": "shared_truth",
                    "code": "shared_truth_refresh_failed",
                    "returncode": 2,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["submit_allowed"] == {"submit_allowed": True, "canonical_readiness": "READY_SUBMIT_CAPABLE"}
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"
    assert ods["diagnostic_warnings"][0]["code"] == "operator_readiness_refresh_failure_superseded"


def test_later_current_managed_exposure_supersedes_earlier_refresher_failure(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=False, bsa_new_entry=False)
    _seed_managed_mes_position(tmp_path)
    _write(
        tmp_path / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": (NOW - timedelta(seconds=120)).isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {
                    "step": "shared_truth",
                    "code": "shared_truth_refresh_failed",
                    "returncode": 2,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/operator_decision_surface/latest_operator_decision_surface.json",
        {
            "schema_version": "track_b_operator_decision_surface_v1",
            "generated_at": (NOW - timedelta(seconds=240)).isoformat(),
            "broker_state": "FLAT",
            "first_blocker": None,
            "next_safe_action": "NO_ACTION",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "EXPOSED_MANAGED"
    assert ods["submit_allowed"]["submit_allowed"] is False
    assert ods["first_blocker"]["source"] in {"canonical_readiness", "execution_core_reconciliation", "broker_session_authority"}
    assert ods["diagnostic_warnings"][0]["code"] == "operator_readiness_refresh_failure_superseded"


def test_newer_broker_position_without_current_managed_owner_is_ambiguous(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/operator_decision_surface/latest_operator_decision_surface.json",
        {
            "schema_version": "track_b_operator_decision_surface_v1",
            "generated_at": (NOW - timedelta(seconds=240)).isoformat(),
            "broker_state": "FLAT",
            "first_blocker": None,
            "next_safe_action": "NO_ACTION",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["track_b_broker_position_count"] = 1
    lease["positions"] = [{"local_symbol": "TESTM6", "quantity": "1.0", "account_id": "DUM882026"}]
    _write(lease_path, lease)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["track_b_broker_position_count"] = 1
    reconciliation["current_scope_lifecycle_open_position_count"] = 0
    reconciliation["current_exposure_owner_resolution"] = {"classification": "UNKNOWN_OWNER"}
    _write(reconciliation_path, reconciliation)
    open_order_path = tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"
    open_order_truth = json.loads(open_order_path.read_text(encoding="utf-8"))
    open_order_truth["generated_at"] = (NOW + timedelta(seconds=30)).isoformat()
    _write(open_order_path, open_order_truth)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "EXPOSED_AMBIGUOUS"
    assert ods["first_blocker"] == {
        "code": "broker_state_exposed_ambiguous",
        "detail": "Broker state is EXPOSED_AMBIGUOUS.",
        "source": "broker_state",
    }


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


def test_clean_flat_authority_tolerates_reconciliation_action_ttl_age(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["generated_at"] = "2026-06-07T05:56:00+00:00"
    reconciliation["max_age_seconds"] = 120
    _write(reconciliation_path, reconciliation)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["source_states"]["broker_reconciliation"] == {"state": "FRESH", "age_seconds": 240.0}
    assert ods["broker_state"] == "FLAT"
    assert ods["first_blocker"] is None


def test_zero_quantity_broker_rows_do_not_make_flat_state_exposed(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["positions"] = [
        {"local_symbol": "MESM6", "quantity": "0.0", "account_id": "DUM882026"},
        {"local_symbol": "MNQM6", "quantity": "0.0", "account_id": "DUM882026"},
    ]
    _write(lease_path, lease)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["first_blocker"] is None


def test_clean_current_flat_authority_beats_historical_managed_review_projection(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": NOW.isoformat(),
            "classification": "REVIEW_REQUIRED",
            "managed_positions": [
                {
                    "classification": "REVIEW_REQUIRED",
                    "local_symbol": "MNQM6",
                    "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-22T17:42:00Z|BUY_TO_OPEN",
                    "current_hot_path_scope": "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY",
                    "historical_only": True,
                }
            ],
            "historical_review_positions": [
                {
                    "local_symbol": "MNQM6",
                    "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-22T17:42:00Z|BUY_TO_OPEN",
                    "historical_only": True,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["first_blocker"] is None


def test_post_v1_1_close_flat_authority_beats_stale_managed_and_lifecycle_debris(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease["track_b_broker_position_count"] = 0
    lease["track_b_broker_open_order_count"] = 0
    lease["unknown_broker_open_order_count"] = 0
    lease["positions"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MESM6",
            "quantity": "-1.0",
            "source": "stale_pre_close_projection",
        }
    ]
    _write(lease_path, lease)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 0,
            "track_b_broker_positions": [],
            "track_b_broker_open_order_count": 0,
            "track_b_broker_open_orders": [],
            "unknown_broker_open_order_count": 0,
            "unknown_broker_open_orders": [],
            "current_scope_lifecycle_open_position_count": 0,
            "current_scope_review_required_count": 0,
            "track_b_lifecycle_positions": [],
            "review_required_positions": [],
        }
    )
    _write(reconciliation_path, reconciliation)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": NOW.isoformat(),
            "classification": "REVIEW_REQUIRED",
            "managed_positions": [
                {
                    "classification": "REVIEW_REQUIRED",
                    "local_symbol": "MESM6",
                    "lifecycle_id": "reserved_submit_mes_london_open_active_participation_short_old",
                    "current_hot_path_scope": "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY",
                    "historical_only": True,
                }
            ],
            "historical_review_positions": [
                {
                    "local_symbol": "MESM6",
                    "lifecycle_id": "reserved_submit_mes_london_open_active_participation_short_old",
                    "historical_only": True,
                }
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"


def test_ods_consumes_current_scope_flat_after_post_close_refresh_degradation(tmp_path: Path) -> None:
    _seed_sources(tmp_path, submit_allowed=False, bsa_new_entry=False)
    _write(
        tmp_path / "outputs/track_b_execution_core/current_scope_state/latest_current_scope_state.json",
        {
            "schema_version": "track_b_current_scope_state_v1",
            "generated_at": NOW.isoformat(),
            "classification": "FLAT",
            "broker_state_for_ods": "FLAT",
            "first_blocker_for_ods": None,
            "next_safe_action_for_ods": "NO_ACTION",
            "broker_positions": [],
            "broker_open_orders": [],
            "unknown_broker_open_order_count": 0,
            "canonical_readiness": {
                "classification": "NOT_READY_DEPENDENCY",
                "submit_allowed": False,
            },
            "diagnostics": [
                {
                    "code": "post_mutation_refresh_degraded",
                    "source": "post_mutation_refresh",
                    "diagnostic_only": True,
                },
                {
                    "code": "reconciliation_stale",
                    "source": "reconciliation",
                    "diagnostic_only": True,
                },
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [{"local_symbol": "MNQM6", "quantity": "-1"}],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_orders_v1",
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [{"local_symbol": "MNQM6", "action": "BUY", "quantity": "1"}],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "FLAT"
    assert ods["first_blocker"] is None
    assert ods["next_safe_action"] == "NO_ACTION"
    assert ods["current_scope_state"]["diagnostics"][0]["code"] == "post_mutation_refresh_degraded"


def test_stale_reconciliation_still_makes_broker_state_unknown(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["generated_at"] = "2026-06-07T05:54:30+00:00"
    _write(reconciliation_path, reconciliation)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["source_states"]["broker_reconciliation"]["state"] == "STALE"
    assert ods["broker_state"] == "UNKNOWN"
    assert ods["first_blocker"]["code"] == "broker_state_unknown"


def test_dirty_reconciliation_keeps_flat_projection_unknown(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["classification"] = "TRACK_B_PAPER_BROKER_POSITION_MISMATCH"
    reconciliation["broker_reconciled"] = False
    _write(reconciliation_path, reconciliation)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "UNKNOWN"
    assert ods["first_blocker"]["code"] == "broker_state_unknown"


def test_current_flat_open_order_truth_overrides_stale_lease_open_order_projection(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    lease.update(
        {
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 1,
            "unknown_broker_open_order_count": 0,
        }
    )
    _write(lease_path, lease)
    reconciliation_path = (
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "broker_reconciled": False,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
        }
    )
    _write(reconciliation_path, reconciliation)
    open_order_path = tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"
    open_order_truth = json.loads(open_order_path.read_text(encoding="utf-8"))
    open_order_truth["generated_at"] = (NOW + timedelta(seconds=30)).isoformat()
    _write(open_order_path, open_order_truth)

    ods = build_operator_decision_surface(repo_root=tmp_path, now=NOW)

    assert ods["broker_state"] == "UNKNOWN"
    assert ods["first_blocker"]["code"] == "broker_state_unknown"


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
