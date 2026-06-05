from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_readiness_state import (
    CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS,
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    _broker_truth_input,
    _market_data_input,
    _reconciliation_input,
    _runtime_input,
    build_root_process_guard,
    classify_canonical_readiness,
    write_canonical_readiness_artifact,
)


def _clean_inputs() -> dict:
    return {
        "generated_at": "2026-05-18T12:00:00+00:00",
        "paper_only": True,
        "live_money_eligible": False,
        "root_guard_summary": {
            "expected_root": "/Users/patrick/Dev/MGC-v05l-automation",
            "active_root": "/Users/patrick/Dev/MGC-v05l-automation",
            "root_match": True,
            "wrong_root_processes": [],
            "unknown_root_processes": [],
        },
        "backend": {"healthy": True, "running": True},
        "runtime": {
            "running": True,
            "healthy": True,
            "loaded_lane_count": 3,
            "eligible_lane_count": 3,
            "entries_enabled": True,
            "operator_halt": False,
            "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
            "runtime_ingestion_fresh": True,
            "ingestion_age_seconds": 60.0,
            "ingestion_freshness_threshold_seconds": 180.0,
        },
        "broker_truth": {
            "available": True,
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "open_order_count": 0,
            "live_money_eligible": False,
            "latest_attempt_status": {"classification": "BROKER_TRUTH_REFRESH_READY", "last_failure": False},
        },
        "broker_session_authority": {
            "available": True,
            "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
            "connection_mode": "SUBMIT_CAPABLE",
            "allowed_uses": {"new_entry": True, "managed_risk_reducing_close": True, "status_diagnostic": True},
            "authority_blockers": [],
            "callback_ownership_attribution": None,
        },
        "phase1_reconciliation": {
            "available": True,
            "fresh": True,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "live_money_eligible": False,
        },
        "market_data": {"fresh": True, "market_data_ok": True},
        "lane_quarantine": {
            "classification": "PAPER_LANE_QUARANTINE_CLEAR",
            "quarantine_count": 0,
            "quarantined_lane_ids": [],
            "live_money_eligible": False,
        },
        "submit_bridge": {
            "submit_route_ready": True,
            "submit_authority_explicit": True,
            "eligible_lane_count": 3,
            "live_money_eligible": False,
        },
        "control_plane_authorization": {
            "available": True,
            "fresh": True,
            "generated_at": "2026-05-18T11:59:00+00:00",
            "age_seconds": 60.0,
            "max_age_seconds": CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS,
            "source_artifact_path": str(DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT),
            "control_plane_snapshot_id": "track-b-control-plane-test",
            "shared_truth_refresh_generation_id": "track-b-shared-truth-test",
            "runtime_supervisor_decision_id": "track-b-paper-supervisor-test",
            "shared_truth_coherence_status": "COHERENT",
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "live_money_eligible": False,
            "bridge_pre_action_authority": True,
        },
    }


def _shared_truth_evidence(
    *,
    proof_classification: str = "READY_FOR_PROOF",
    open_order_truth: str = "NO_OPEN_ORDERS",
    managed_order_registry: str = "NO_MANAGED_ORDERS",
    order_adjustment_planner: str = "NO_ACTION_NEEDED",
    position_truth: str = "CLEAN_FLAT_READY",
    runtime_environment_truth: str = "RUNTIME_ACTIVE_TRADE_CAPABLE",
    managed_position_registry: str = "NO_MANAGED_POSITIONS",
    reconciliation: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_truth_lease: str = "ACTIVE",
    phase1_reason: str | None = None,
) -> dict:
    return {
        "available": True,
        "evidence_only": True,
        "readiness_authority": True,
        "source": "execution_core_authority",
        "proof_readiness": {
            "available": True,
            "classification": proof_classification,
            "ready_for_proof": proof_classification == "READY_FOR_PROOF",
            "primary_blocker": {
                "code": "phase1_mgc_1m_not_ready",
                "detail": "Phase-1 MGC 1m runtime candles are not proof-ready.",
            }
            if proof_classification in {"MARKET_CLOSED_NO_FRESH_BARS", "PHASE1_DATA_UNHEALTHY"}
            else {},
            "secondary_warnings": [],
            "broker_lease_warning": {
                "lease_state": broker_truth_lease,
                "code": "broker_truth_lease_not_clean_for_runtime_start",
                "detail": f"Broker Truth Lease is {broker_truth_lease}.",
            }
            if broker_truth_lease != "ACTIVE"
            else {},
            "phase1_session_reason": phase1_reason,
            "blockers": [],
        },
        "classifications": {
            "Open Order Truth": open_order_truth,
            "Managed Order Registry": managed_order_registry,
            "Order Adjustment Planner": order_adjustment_planner,
            "Position Truth": position_truth,
            "Runtime Environment Truth": runtime_environment_truth,
            "Managed Position Registry": managed_position_registry,
            "Reconciliation": reconciliation,
            "Broker Truth Lease": broker_truth_lease,
        },
        "live_money_eligible": False,
    }


def _write_control_plane_snapshot(repo_root: Path, generated_at: str = "2026-05-18T11:59:30+00:00") -> None:
    path = repo_root / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "CONTROL_PLANE_SNAPSHOT_READY",
                "generated_at": generated_at,
                "control_plane_snapshot_id": "track-b-control-plane-test",
                "shared_truth_refresh_generation_id": "track-b-shared-truth-test",
                "runtime_supervisor_decision_id": "track-b-paper-supervisor-test",
                "shared_truth_coherence_status": "COHERENT",
                "live_money_eligible": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _write_broker_session_authority_snapshot(repo_root: Path, generated_at: str = "2026-05-18T11:59:40+00:00") -> None:
    path = repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "track_b_broker_session_authority_v1",
                "generated_at": generated_at,
                "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
                "connection_mode": "SUBMIT_CAPABLE",
                "allowed_uses": {
                    "new_entry": True,
                    "managed_risk_reducing_close": True,
                    "status_diagnostic": True,
                },
                "authority_blockers": [],
                "callback_ownership_attribution": None,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "read_only": True,
                "submit_attempted": False,
                "cancel_attempted": False,
                "close_attempted": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _phase1_listener_status(*, rows: list[dict], generated_at: str = "2026-05-18T11:59:50+00:00", **overrides: object) -> dict:
    payload = {
        "schema_version": "phase1_databento_live_listener_status_v1",
        "generated_at": generated_at,
        "source": "DATABENTO_REALTIME_PHASE1",
        "provider_status": "RUNNING",
        "listener_alive": True,
        "historical_seed_ready": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "databento_live_api_replay": False,
        "symbols": [row["symbol"] for row in rows],
        "required_for_readiness_symbols": [
            row["symbol"] for row in rows if row.get("required_for_readiness") is True
        ],
        "optional_symbols": [
            row["symbol"] for row in rows if row.get("required_for_readiness") is not True
        ],
        "rows": rows,
    }
    payload.update(overrides)
    return payload


def _listener_row(
    symbol: str,
    *,
    required: bool = True,
    realtime_feed_confirmed: bool = True,
    latest_completed_bar_ts: str = "2026-05-18T11:59:00+00:00",
    bar_count: int = 10,
    min_confirmed_bars: int = 8,
    freshness_threshold_seconds: int = 180,
    source: str = "DATABENTO_REALTIME_PHASE1",
    **overrides: object,
) -> dict:
    row = {
        "symbol": symbol,
        "required_for_readiness": required,
        "realtime_feed_confirmed": realtime_feed_confirmed,
        "latest_completed_bar_ts": latest_completed_bar_ts,
        "bar_count": bar_count,
        "min_confirmed_bars": min_confirmed_bars,
        "freshness_threshold_seconds": freshness_threshold_seconds,
        "source": source,
        "historical_seed_ready": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "databento_live_api_replay": False,
        "databento_symbol": f"{symbol}.v.0",
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
    }
    row.update(overrides)
    return row


def test_clean_submit_capable_state_returns_ready_submit_capable() -> None:
    result = classify_canonical_readiness(_clean_inputs())

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["readiness_blockers"] == []
    assert result["runtime_truth_heartbeat"] == {}


def test_broker_lifecycle_reconciled_alias_preserves_submit_capable_readiness() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"] = {
        **inputs["phase1_reconciliation"],
        "classification": "BROKER_LIFECYCLE_RECONCILED",
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(reconciliation="BROKER_LIFECYCLE_RECONCILED")

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["readiness_blockers"] == []


def test_clean_shared_truth_and_fresh_phase1_preserve_submit_capable_readiness() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence()

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["execution_core_shared_truth"]["classifications"]["Position Truth"] == "CLEAN_FLAT_READY"
    assert result["readiness_blockers"] == []


def test_reconciliation_input_prefers_current_scope_lifecycle_count_over_raw_projection() -> None:
    payload = {
        "generated_at": "2026-05-18T11:59:30+00:00",
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "broker_reconciled": True,
        "review_required_count": 0,
        "current_scope_review_required_count": 0,
        "lifecycle_open_position_count": 1,
        "track_b_lifecycle_positions": [],
        "registry_reconciliation": {
            "classification": "REGISTRY_RECONCILIATION_MATCHED",
            "lifecycle_position_count": 0,
        },
    }

    normalized = _reconciliation_input(payload, now=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))

    assert normalized["lifecycle_open_position_count"] == 0
    assert normalized["review_required_count"] == 0
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"] = normalized
    result = classify_canonical_readiness(inputs)
    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"


def test_stale_bridge_control_plane_snapshot_blocks_submit_capable_readiness() -> None:
    inputs = _clean_inputs()
    inputs["control_plane_authorization"] = {
        **inputs["control_plane_authorization"],
        "fresh": False,
        "age_seconds": CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS + 1.0,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["ready_submit_capable"] is False
    assert result["submit_allowed"] is False
    assert result["control_plane_authorization"]["source_artifact_path"] == str(DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT)
    assert {row["code"] for row in result["readiness_blockers"]} == {"control_plane_snapshot_stale"}


def test_fresh_bridge_control_plane_snapshot_preserves_submit_capable_readiness() -> None:
    inputs = _clean_inputs()

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["control_plane_authorization"]["fresh"] is True


def test_closed_market_proof_readiness_blocks_with_clear_market_evidence() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        proof_classification="MARKET_CLOSED_NO_FRESH_BARS",
        phase1_reason="GLOBEX_WEEKEND_HALT",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "WAITING_FOR_MARKET_REOPEN"
    assert result["ready_submit_capable"] is False
    assert result["market_schedule_state"] == "SCHEDULED_MARKET_HALT"
    assert result["stale_market_data_expected"] is True
    assert result["readiness_block_is_scheduled_halt"] is True
    assert result["readiness_blockers"] == []


def test_sunday_pre_reopen_halt_waits_without_infrastructure_block() -> None:
    now = datetime(2026, 5, 31, 17, 30, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["generated_at"] = now.isoformat()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        proof_classification="MARKET_CLOSED_NO_FRESH_BARS",
        phase1_reason="WEEKEND_GLOBEX_HALT_BEFORE_SUNDAY_REOPEN",
    )
    inputs["market_data"] = _market_data_input(
        {"active_symbols": ["MNQ", "MES"]},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MNQ", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None),
                _listener_row("MES", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None),
            ],
            generated_at=now.isoformat(),
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "WAITING_FOR_MARKET_REOPEN"
    assert result["ready_submit_capable"] is False
    assert result["market_schedule_state"] == "SCHEDULED_MARKET_HALT"
    assert result["stale_market_data_expected"] is True
    assert result["next_expected_reopen_time"] == "2026-05-31T22:00:00+00:00"
    assert result["market_data_grace_until"] == "2026-05-31T22:10:00+00:00"
    assert result["readiness_block_is_scheduled_halt"] is True
    assert result["readiness_blockers"] == []


def test_sunday_pre_reopen_halt_allows_runtime_start_but_not_submit_when_runtime_down() -> None:
    now = datetime(2026, 5, 31, 17, 30, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["generated_at"] = now.isoformat()
    inputs["runtime"]["running"] = False
    inputs["runtime"]["healthy"] = False
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        proof_classification="MARKET_CLOSED_NO_FRESH_BARS",
        phase1_reason="WEEKEND_GLOBEX_HALT_BEFORE_SUNDAY_REOPEN",
    )
    inputs["market_data"] = _market_data_input(
        {"active_symbols": ["MNQ", "MES"]},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MNQ", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None),
                _listener_row("MES", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None),
            ],
            generated_at=now.isoformat(),
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_TO_START_DIAGNOSTIC_ONLY"
    assert result["runtime_start_allowed"] is True
    assert result["submit_allowed"] is False
    assert result["ready_submit_capable"] is False
    assert result["market_schedule_state"] == "SCHEDULED_MARKET_HALT"
    assert result["readiness_blockers"] == []


def test_degraded_shared_broker_lease_is_diagnostic_when_truth_clean() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        broker_truth_lease="ACTIVE_DEGRADED_REFRESH_FAILING",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["broker_lease_degraded_diagnostic"] is True
    assert {row["code"] for row in result["readiness_blockers"]} == set()
    assert {
        row["code"] for row in result["readiness_warnings"]
    } >= {"broker_truth_lease_degraded_refresh_failing"}
    assert result["phase1_reconciliation"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"


def test_broker_exposure_blocks_via_position_truth() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(position_truth="ATTENTION_REQUIRED")

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "position_truth_not_clean"
    assert "ATTENTION_REQUIRED" in result["readiness_blockers"][0]["detail"]


def test_open_order_blocks_via_open_order_truth_before_legacy_reconstruction() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="UNKNOWN_OPEN_ORDER",
        managed_order_registry="WORKING_CLOSE_ORDER",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "open_order_truth_not_clean"
    assert result["readiness_blockers"][0]["source"] == "execution_core_shared_truth"


def test_managed_exit_pending_allows_exit_submit_readiness_with_active_exit_lease() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["lifecycle_open_position_count"] = 1
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "broker_reconciled": True,
        "unknown_broker_open_order_count": 0,
        "review_required_count": 0,
        "live_money_eligible": False,
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_registry="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        position_truth="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        managed_position_registry="OPEN_MANAGED_MATCHED",
        runtime_environment_truth="RUNTIME_ACTIVE_OBSERVATION_ONLY",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert {row["code"] for row in result["readiness_blockers"]} == set()
    assert "managed_exit_pending_submit_capable" in {row["code"] for row in result["readiness_warnings"]}


def test_managed_exit_due_allows_exit_submit_readiness_with_active_exit_lease() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["lifecycle_open_position_count"] = 1
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "broker_reconciled": True,
        "unknown_broker_open_order_count": 0,
        "review_required_count": 0,
        "live_money_eligible": False,
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_registry="POSITION_WITHOUT_CLOSE_ORDER",
        position_truth="ATTENTION_REQUIRED",
        managed_position_registry="OPEN_MANAGED_EXIT_DUE",
        runtime_environment_truth="RUNTIME_ACTIVE_OBSERVATION_ONLY",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert {row["code"] for row in result["readiness_blockers"]} == set()
    assert "managed_exit_due_submit_capable" in {row["code"] for row in result["readiness_warnings"]}


def test_managed_exit_due_without_close_order_allows_order_adjustment_not_found() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["lifecycle_open_position_count"] = 1
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "broker_reconciled": True,
        "unknown_broker_open_order_count": 0,
        "review_required_count": 0,
        "live_money_eligible": False,
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_registry="POSITION_WITHOUT_CLOSE_ORDER",
        order_adjustment_planner="ORDER_NOT_FOUND",
        position_truth="ATTENTION_REQUIRED",
        managed_position_registry="OPEN_MANAGED_EXIT_DUE",
        runtime_environment_truth="RUNTIME_ACTIVE_OBSERVATION_ONLY",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["readiness_blockers"] == []
    assert "managed_exit_due_submit_capable" in {row["code"] for row in result["readiness_warnings"]}


def test_managed_exit_pending_uses_position_truth_summary_classification() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["lifecycle_open_position_count"] = 1
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "broker_reconciled": True,
        "unknown_broker_open_order_count": 0,
        "review_required_count": 0,
        "live_money_eligible": False,
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_registry="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        position_truth="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        managed_position_registry="OPEN_MANAGED_MATCHED",
        runtime_environment_truth="RUNTIME_ACTIVE_OBSERVATION_ONLY",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"


def test_managed_exit_pending_still_blocks_when_exit_lease_is_not_allowed() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["lifecycle_open_position_count"] = 1
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": False,
        "broker_reconciled": True,
        "unknown_broker_open_order_count": 0,
        "review_required_count": 0,
        "live_money_eligible": False,
    }
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        open_order_truth="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_registry="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        position_truth="ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        managed_position_registry="OPEN_MANAGED_MATCHED",
        runtime_environment_truth="RUNTIME_ACTIVE_OBSERVATION_ONLY",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "open_order_truth_not_clean"


def test_order_adjustment_planner_review_required_blocks_submit_capable_readiness() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        order_adjustment_planner="REVIEW_REQUIRED_SUSPICIOUS_STATE",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "order_adjustment_planner_not_clean"
    assert "REVIEW_REQUIRED_SUSPICIOUS_STATE" in result["readiness_blockers"][0]["detail"]


def test_bad_runtime_environment_truth_blocks_submit_capable_readiness() -> None:
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(
        runtime_environment_truth="DUPLICATE_RUNTIME_WRITERS",
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "runtime_environment_truth_not_clean"


def test_dashboard_position_truth_projection_is_not_canonical_authority() -> None:
    source_path = Path("src/mgc_v05l/execution_core/track_b_readiness_state.py")
    source = source_path.read_text(encoding="utf-8")

    assert "latest_track_b_position_truth.json" not in source
    assert "latest_track_b_open_order_truth.json" not in source
    assert "latest_track_b_managed_orders.json" not in source
    assert "latest_track_b_order_adjustment_plan.json" not in source


def test_runtime_truth_heartbeat_is_evidence_without_readiness_authority() -> None:
    inputs = _clean_inputs()
    inputs["runtime_truth_heartbeat"] = {
        "available": True,
        "fresh": False,
        "heartbeat_state": "ARTIFACT_STALE",
        "writer_authority": "SINGLE_WRITER",
        "runtime_instance_id": "runtime-a",
        "restart_generation": 4,
        "evidence_only": True,
        "readiness_authority": False,
        "restart_authority": False,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["runtime_truth_heartbeat"]["runtime_instance_id"] == "runtime-a"
    warning_codes = {row["code"] for row in result["readiness_warnings"]}
    assert "runtime_truth_heartbeat_stale" in warning_codes


def test_runtime_truth_heartbeat_cannot_override_runtime_down() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["running"] = False
    inputs["runtime_truth_heartbeat"] = {
        "available": True,
        "fresh": True,
        "heartbeat_state": "HEALTHY",
        "writer_authority": "SINGLE_WRITER",
        "runtime_instance_id": "runtime-a",
        "restart_generation": 4,
        "evidence_only": True,
        "readiness_authority": False,
        "restart_authority": False,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_OBSERVATION_ONLY"
    assert result["ready_submit_capable"] is False
    assert result["runtime_truth_heartbeat"]["heartbeat_state"] == "HEALTHY"


def test_required_symbol_fresh_from_phase1_listener_satisfies_market_data() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(rows=[_listener_row("MGC")]),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["market_data"]["source"] == "phase1_databento_live_listener"
    assert result["market_data"]["required_blocked_symbols"] == []


def test_required_symbol_stale_from_phase1_listener_blocks_market_data() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[_listener_row("MGC", latest_completed_bar_ts="2026-05-18T11:40:00+00:00")]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_not_fresh"
    assert result["market_data"]["required_blocked_symbols"] == ["MGC"]


def test_post_reopen_grace_waits_for_market_data_before_blocking() -> None:
    now = datetime(2026, 5, 31, 22, 5, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["generated_at"] = now.isoformat()
    inputs["market_data"] = _market_data_input(
        {"active_symbols": ["MNQ"]},
        {},
        _phase1_listener_status(
            rows=[_listener_row("MNQ", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None)],
            generated_at=now.isoformat(),
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "WAITING_FOR_MARKET_REOPEN"
    assert result["market_schedule_state"] == "POST_REOPEN_GRACE"
    assert result["market_data_grace_until"] == "2026-05-31T22:10:00+00:00"
    assert result["readiness_blockers"] == []


def test_after_reopen_grace_expiry_stale_data_is_infrastructure_dependency() -> None:
    now = datetime(2026, 5, 31, 22, 12, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["generated_at"] = now.isoformat()
    inputs["market_data"] = _market_data_input(
        {"active_symbols": ["MNQ"]},
        {},
        _phase1_listener_status(
            rows=[_listener_row("MNQ", realtime_feed_confirmed=False, bar_count=0, latest_completed_bar_ts=None)],
            generated_at=now.isoformat(),
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_not_fresh"
    assert result["market_schedule_state"] == "MARKET_OPEN_EXPECT_FRESH_BARS"
    assert result["stale_market_data_expected"] is False
    assert result["readiness_block_is_scheduled_halt"] is False


def test_fresh_data_after_reopen_can_pass_when_other_gates_clean() -> None:
    now = datetime(2026, 5, 31, 22, 12, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["generated_at"] = now.isoformat()
    inputs["runtime"]["last_processed_bar_end_ts"] = "2026-05-31T22:11:00+00:00"
    inputs["market_data"] = _market_data_input(
        {"active_symbols": ["MNQ"]},
        {},
        _phase1_listener_status(
            rows=[_listener_row("MNQ", latest_completed_bar_ts="2026-05-31T22:11:00+00:00")],
            generated_at=now.isoformat(),
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["readiness_blockers"] == []
    assert result["market_schedule_state"] == "MARKET_OPEN_EXPECT_FRESH_BARS"


def test_ready_proof_readiness_downgrades_quiet_required_listener_staleness_to_warning() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["execution_core_shared_truth"] = _shared_truth_evidence(proof_classification="READY_FOR_PROOF")
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[_listener_row("MGC", latest_completed_bar_ts="2026-05-18T11:56:00+00:00")]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["readiness_blockers"] == []
    assert "market_data_freshness_delegated_to_proof_readiness" in {
        warning["code"] for warning in result["readiness_warnings"]
    }


def test_required_symbol_unconfirmed_or_under_min_bars_blocks_market_data() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(rows=[_listener_row("MGC", realtime_feed_confirmed=False, bar_count=2)]),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_not_fresh"
    assert result["market_data"]["rows"][0]["realtime_feed_confirmed"] is False


def test_required_symbol_with_fresh_primary_1m_bars_allows_runtime_startup_when_derived_timeframes_pending() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row(
                    "MGC",
                    realtime_feed_confirmed=False,
                    latest_completed_bar_ts="2026-05-18T11:59:00+00:00",
                    bar_count=90,
                    min_confirmed_bars=8,
                    schema="ohlcv-1m",
                )
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["readiness_blockers"] == []
    row = result["market_data"]["rows"][0]
    assert row["ready"] is True
    assert row["realtime_feed_confirmed"] is False
    assert row["primary_1m_live_ready"] is True
    assert row["derived_timeframes_pending"] is True
    assert row["block_reason"] == "READY_PRIMARY_1M_FEED_DERIVED_TIMEFRAMES_PENDING"


def test_required_symbol_confirmed_but_under_min_bars_blocks_market_data() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row(
                    "MGC",
                    realtime_feed_confirmed=True,
                    bar_count=2,
                    min_confirmed_bars=8,
                )
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_not_fresh"
    assert result["market_data"]["rows"][0]["bar_count"] == 2
    assert result["market_data"]["rows"][0]["min_confirmed_bars"] == 8


def test_optional_symbol_stale_warns_without_hard_market_data_blocker() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MGC", required=True),
                _listener_row(
                    "PL",
                    required=False,
                    latest_completed_bar_ts="2026-05-18T11:40:00+00:00",
                    freshness_threshold_seconds=180,
                ),
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["market_data"]["optional_degraded_symbols"] == ["PL"]
    assert "optional_market_data_degraded" in {row["code"] for row in result["readiness_warnings"]}
    assert result["readiness_blockers"] == []


def test_active_lane_symbols_scope_required_market_data_readiness() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    operator_status = {
        "active_lane_ids": ["mgc_lane"],
        "lanes": [{"lane_id": "mgc_lane", "symbol": "MGC"}],
        "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
        "health": {"market_data_ok": True},
    }
    inputs["market_data"] = _market_data_input(
        operator_status,
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MGC", required=True),
                _listener_row("GC", required=True, realtime_feed_confirmed=False, bar_count=2),
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["market_data"]["required_symbols"] == ["MGC"]
    assert result["market_data"]["configured_required_symbols"] == ["MGC", "GC"]
    assert result["market_data"]["optional_degraded_symbols"] == ["GC"]
    assert "optional_market_data_degraded" in {row["code"] for row in result["readiness_warnings"]}
    assert result["readiness_blockers"] == []


def test_mnq_mes_active_lanes_do_not_require_stale_gold_feeds() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    operator_status = {
        "active_lane_ids": ["mnq_evidence", "mes_evidence"],
        "lanes": [
            {"lane_id": "mnq_evidence", "symbol": "MNQ"},
            {"lane_id": "mes_evidence", "symbol": "MES"},
        ],
        "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
        "health": {"market_data_ok": True},
    }
    inputs["market_data"] = _market_data_input(
        operator_status,
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MNQ", required=True),
                _listener_row("MES", required=True),
                _listener_row("NQ", required=True),
                _listener_row("ES", required=True),
                _listener_row("GC", required=True, realtime_feed_confirmed=False, bar_count=0),
                _listener_row("MGC", required=True, realtime_feed_confirmed=False, bar_count=0),
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["market_data"]["required_symbols"] == ["MES", "MNQ"]
    assert result["market_data"]["required_blocked_symbols"] == []
    assert result["market_data"]["optional_degraded_symbols"] == ["GC", "MGC"]
    assert "optional_market_data_degraded" in {row["code"] for row in result["readiness_warnings"]}


def test_stale_gold_feed_still_blocks_when_gold_lane_is_active() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    operator_status = {
        "active_lane_ids": ["mgc_lane"],
        "lanes": [{"lane_id": "mgc_lane", "symbol": "MGC"}],
        "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
        "health": {"market_data_ok": True},
    }
    inputs["market_data"] = _market_data_input(
        operator_status,
        {},
        _phase1_listener_status(
            rows=[
                _listener_row("MNQ", required=True),
                _listener_row("MES", required=True),
                _listener_row("MGC", required=True, realtime_feed_confirmed=False, bar_count=0),
            ]
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_not_fresh"
    assert result["market_data"]["required_symbols"] == ["MGC"]
    assert result["market_data"]["required_blocked_symbols"] == ["MGC"]


def test_submit_capable_blocks_when_runtime_ingestion_is_stale_even_if_phase1_listener_is_fresh() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["runtime"]["last_processed_bar_end_ts"] = "2026-05-18T11:44:00+00:00"
    inputs["runtime"]["latest_runtime_ingested_bar"] = "2026-05-18T11:44:00+00:00"
    inputs["runtime"]["runtime_ingestion_fresh"] = False
    inputs["runtime"]["ingestion_age_seconds"] = 960.0
    inputs["runtime"]["ingestion_freshness_threshold_seconds"] = 180.0
    inputs["runtime"]["affected_symbols"] = ["MGC"]
    inputs["runtime"]["affected_lanes"] = ["mgc_lane"]
    inputs["runtime"]["runtime_pid"] = 5677
    inputs["runtime"]["runtime_commit"] = "abc123"
    inputs["runtime"]["profile"] = "mnq_mes_full_session_active_evidence"
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(rows=[_listener_row("MGC")]),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    blocker = result["readiness_blockers"][0]
    assert blocker["code"] == "runtime_ingestion_not_fresh"
    assert blocker["classification"] == "RUNTIME_ALIVE_PHASE1_FRESH_RUNTIME_INGESTION_STALE"
    assert blocker["latest_phase1_bar_by_symbol_timeframe"] == {"MGC": {"1m": "2026-05-18T11:59:00+00:00"}}
    assert blocker["latest_runtime_ingested_bar"] == "2026-05-18T11:44:00+00:00"
    assert blocker["ingestion_lag_seconds"] == 900.0
    assert blocker["threshold_seconds"] == 180.0
    assert blocker["affected_symbols"] == ["MGC"]
    assert blocker["affected_lanes"] == ["mgc_lane"]
    assert blocker["runtime_pid"] == 5677
    assert blocker["runtime_commit"] == "abc123"
    assert blocker["profile"] == "mnq_mes_full_session_active_evidence"
    assert "Phase-1 listener/feed freshness is evaluated separately" in blocker["distinction_from_listener_feed_failure"]
    assert result["market_data"]["fresh"] is True


def test_runtime_ingestion_uses_latest_lane_processed_bar_when_top_level_lags() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    runtime = _runtime_input(
        {
            "strategy_status": "RUNNING",
            "operator_halt": False,
            "entries_enabled": True,
            "source_runtime_pid": 5677,
            "source_runtime_git_head": "abc123",
            "source_runtime_command": "/repo/src/mgc_v05l/app/main.py probationary-paper-soak --config /repo/outputs/probationary_pattern_engine/paper_session/runtime/paper_stack_mnq_mes_full_session_active_evidence.yaml",
            "active_lane_ids": ["lane_a", "lane_b"],
            "last_processed_bar_end_ts": "2026-05-18T11:44:00+00:00",
            "lanes": [
                {"lane_id": "lane_a", "symbol": "MNQ", "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00"},
                {"lane_id": "lane_b", "symbol": "MES", "last_processed_bar_end_ts": "2026-05-18T11:58:00+00:00"},
            ],
        },
        {"lanes": [{"id": "lane_a"}, {"id": "lane_b"}]},
        {
            "processes": [
                {"name": "paper_runtime", "running": True},
            ]
        },
        now=now,
    )

    assert runtime["last_processed_bar_end_ts"] == "2026-05-18T11:59:00+00:00"
    assert runtime["runtime_ingestion_fresh"] is True
    assert runtime["ingestion_age_seconds"] == 60.0
    assert runtime["latest_runtime_ingested_bar"] == "2026-05-18T11:59:00+00:00"
    assert runtime["runtime_pid"] == 5677
    assert runtime["runtime_commit"] == "abc123"
    assert runtime["profile"] == "mnq_mes_full_session_active_evidence"
    assert runtime["runtime_lane_ingestion"][0]["lane_id"] == "lane_a"
    assert runtime["affected_lanes"] == []


def test_invalid_phase1_listener_provenance_blocks_market_data() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(rows=[_listener_row("MGC")], source="DATABENTO_HISTORICAL_SEED"),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_invalid_provenance"


def test_absent_phase1_listener_status_preserves_operator_runtime_fallback() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    market_data = _market_data_input(
        {
            "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
            "health": {"market_data_ok": True},
        },
        {"runtime_ready": True},
        {},
        now=now,
    )

    assert market_data["fresh"] is True
    assert market_data["source"] == "operator_runtime_fallback"
    assert market_data["fallback_used"] is True


def test_stale_phase1_listener_status_can_use_explicit_fresh_runtime_fallback() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    market_data = _market_data_input(
        {
            "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
            "health": {"market_data_ok": True},
        },
        {"runtime_ready": True},
        _phase1_listener_status(rows=[_listener_row("MGC")], generated_at="2026-05-18T11:00:00+00:00"),
        now=now,
    )

    assert market_data["fresh"] is True
    assert market_data["source"] == "operator_runtime_fallback"
    assert market_data["listener_global_issue"] is True
    assert market_data["warnings"][0]["code"] == "phase1_listener_status_fallback_used"


def test_research_historical_seed_listener_artifacts_are_rejected() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    inputs = _clean_inputs()
    inputs["market_data"] = _market_data_input(
        {},
        {},
        _phase1_listener_status(
            rows=[
                _listener_row(
                    "MGC",
                    historical_seed_ready=True,
                    research_artifact_used=True,
                )
            ],
            historical_seed_ready=True,
            research_artifact_used=True,
        ),
        now=now,
    )

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "market_data_invalid_provenance"


def test_wrong_root_blocks() -> None:
    inputs = _clean_inputs()
    inputs["root_guard_summary"] = {
        "expected_root": "/Users/patrick/Dev/MGC-v05l-automation",
        "active_root": "/Users/patrick/Dev/MGC-v05l-automation",
        "root_match": False,
        "wrong_root_processes": [{"name": "paper_runtime", "pid": 123, "detected_root": "/tmp/wrong"}],
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_WRONG_ROOT"
    assert result["operator_action_required"] is True
    assert result["readiness_blockers"][0]["code"] == "wrong_root_process"


def test_healthy_runtime_without_eligible_lanes_is_observation_only() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["eligible_lane_count"] = 0
    inputs["submit_bridge"]["eligible_lane_count"] = 0
    inputs["submit_bridge"]["submit_authority_explicit"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_OBSERVATION_ONLY"
    assert result["ready_submit_capable"] is False


def test_stale_broker_truth_blocks_submit() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["fresh"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "broker_truth_not_fresh_or_complete"


def test_active_broker_truth_lease_clears_broker_freshness_blocker() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["fresh"] = False
    inputs["broker_truth"]["positions_complete"] = False
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert {row["code"] for row in result["readiness_blockers"]} == set()
    assert result["broker_truth_lease"]["lease_state"] == "ACTIVE"


def test_readiness_blocks_submit_when_broker_session_new_entry_not_allowed() -> None:
    inputs = _clean_inputs()
    inputs["broker_session_authority"] = {
        "available": True,
        "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        "connection_mode": "ORDER_STATUS_UNRELIABLE",
        "allowed_uses": {"new_entry": False, "managed_risk_reducing_close": False, "status_diagnostic": True},
        "authority_blockers": [{"code": "order_status_unreliable_blocks_submit_and_close"}],
        "callback_ownership_attribution": {"last_order_status_client_id": None},
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "DEGRADED_NO_SUBMIT"
    assert result["submit_allowed"] is False
    assert result["broker_session_submit_alignment"] == "ALIGNED"
    assert result["readiness_blockers"][0]["code"] == "BROKER_SESSION_NEW_ENTRY_NOT_ALLOWED"
    assert result["broker_session_authority_classification"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert result["broker_session_connection_mode"] == "ORDER_STATUS_UNRELIABLE"
    assert result["broker_session_allowed_uses"]["new_entry"] is False
    assert result["broker_session_authority_blockers"][0]["code"] == "order_status_unreliable_blocks_submit_and_close"


def test_readiness_surfaces_blocked_broker_session_allowed_alignment_without_unblocking() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["running"] = False
    inputs["broker_session_authority"] = {
        "available": True,
        "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
        "connection_mode": "SUBMIT_CAPABLE",
        "allowed_uses": {"new_entry": True, "managed_risk_reducing_close": True},
        "authority_blockers": [],
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_OBSERVATION_ONLY"
    assert result["submit_allowed"] is False
    assert result["broker_session_submit_alignment"] == "READINESS_BLOCKED_BROKER_SESSION_ALLOWED"


def test_readiness_missing_broker_session_authority_alignment_unknown() -> None:
    inputs = _clean_inputs()
    inputs.pop("broker_session_authority")

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "DEGRADED_NO_SUBMIT"
    assert result["submit_allowed"] is False
    assert result["broker_session_authority_classification"] is None
    assert result["broker_session_submit_alignment"] == "UNKNOWN"
    assert result["readiness_blockers"][0]["code"] == "BROKER_SESSION_NEW_ENTRY_NOT_ALLOWED"


def test_readiness_surfaces_aligned_broker_session_submit_state() -> None:
    inputs = _clean_inputs()
    inputs["broker_session_authority"] = {
        "available": True,
        "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
        "connection_mode": "SUBMIT_CAPABLE",
        "allowed_uses": {"new_entry": True, "managed_risk_reducing_close": True},
        "authority_blockers": [],
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["submit_allowed"] is True
    assert result["broker_session_submit_alignment"] == "ALIGNED"


def test_degraded_broker_truth_lease_warns_without_dependency_blocker() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["fresh"] = False
    inputs["broker_truth"]["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_FAILED",
        "last_failure": True,
    }
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE_DEGRADED_REFRESH_FAILING",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["broker_lease_degraded_diagnostic"] is True
    warning_codes = {row["code"] for row in result["readiness_warnings"]}
    assert "latest_broker_attempt_failed" in warning_codes
    assert "broker_truth_lease_degraded_refresh_failing" in warning_codes
    assert {row["code"] for row in result["readiness_blockers"]} == set()


def test_expired_broker_truth_lease_blocks_submit_capable_state() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "EXPIRED_BLOCK_NEW_ENTRIES",
        "submit_entry_allowed": False,
        "submit_exit_allowed": False,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "broker_truth_lease_expired"


def test_canonical_readiness_refreshes_stale_broker_truth_lease_from_fresh_sources(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    report_dir = repo_root / "outputs" / "reports"
    lease_dir = repo_root / "outputs" / "operator_dashboard" / "runtime"
    lanes_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mnq"
    runtime_dir.mkdir(parents=True)
    (report_dir / "ibkr_read_only_verification").mkdir(parents=True)
    (report_dir / "track_b_paper_broker_reconciliation").mkdir(parents=True)
    lease_dir.mkdir(parents=True)
    lanes_dir.mkdir(parents=True)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json").write_text(
        (
            '{'
            f'"source_runtime_pid":100,"source_runtime_repo_root":"{repo_root}",'
            '"active_lane_ids":["mnq"],"usable_lane_count":1,"entries_enabled":true,'
            '"operator_halt":false,"last_processed_bar_end_ts":"2026-05-18T11:59:00+00:00",'
            '"health":{"market_data_ok":true}}'
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text('{"lanes": [{"lane_id": "mnq"}]}', encoding="utf-8")
    (runtime_dir / "paper_lane_quarantine_status.json").write_text(
        '{"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": false}',
        encoding="utf-8",
    )
    (runtime_dir / "market_data_transport_probe.json").write_text('{"status": "ok", "runtime_ready": true}', encoding="utf-8")
    (report_dir / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json").write_text(
        '{"classification":"BROKER_TRUTH_REFRESH_READY","generated_at":"2026-05-18T11:59:10+00:00",'
        '"last_success":true,"positions_complete":true,"open_orders_complete":true,"live_money_eligible":false}',
        encoding="utf-8",
    )
    (report_dir / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json").write_text(
        '{"classification":"TRACK_B_PAPER_BROKER_RECONCILED","generated_at":"2026-05-18T11:59:20+00:00",'
        '"broker_reconciled":true,"review_required_count":0,"lifecycle_open_position_count":0,'
        '"live_money_eligible":false}',
        encoding="utf-8",
    )
    (lease_dir / "latest_broker_truth_lease.json").write_text(
        '{"lease_state":"ACTIVE","generated_at":"2026-05-18T05:30:00+00:00",'
        '"valid_until":"2026-05-18T05:35:00+00:00",'
        '"entry_valid_until":"2026-05-18T05:35:00+00:00",'
        '"exit_valid_until":"2026-05-18T05:45:00+00:00",'
        '"submit_entry_allowed":true,"submit_exit_allowed":true,"live_money_eligible":false}',
        encoding="utf-8",
    )
    (lanes_dir / "live_timing_summary_latest.json").write_text(
        '{"lane_id":"mnq","broker_truth":{"account_health":{"status":"HEALTHY",'
        '"route_destination":"ibkr_paper_bridge_submit_capable"}}}',
        encoding="utf-8",
    )
    _write_control_plane_snapshot(repo_root)
    _write_broker_session_authority_snapshot(repo_root)
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)

    result = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=repo_root,
        output_path=lease_dir / "latest_canonical_readiness.json",
        now=now,
    )
    refreshed_lease = json.loads((lease_dir / "latest_broker_truth_lease.json").read_text(encoding="utf-8"))

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["broker_truth_lease"]["source_lease_state"] == "ACTIVE"
    assert result["broker_truth_lease"]["lease_state"] == "ACTIVE"
    assert result["broker_truth_lease"]["previous_lease_state"] == "ACTIVE"
    assert result["broker_truth_lease"]["refreshed_by_canonical_readiness"] is True
    assert refreshed_lease["lease_state"] == "ACTIVE"
    assert refreshed_lease["refreshed_by_canonical_readiness"] is True
    assert result["readiness_blockers"] == []


def test_canonical_readiness_keeps_stale_broker_truth_blocked_when_lease_is_refreshed(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    report_dir = repo_root / "outputs" / "reports"
    lease_dir = repo_root / "outputs" / "operator_dashboard" / "runtime"
    lanes_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mnq"
    runtime_dir.mkdir(parents=True)
    (report_dir / "ibkr_read_only_verification").mkdir(parents=True)
    (report_dir / "track_b_paper_broker_reconciliation").mkdir(parents=True)
    lease_dir.mkdir(parents=True)
    lanes_dir.mkdir(parents=True)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json").write_text(
        (
            '{'
            f'"source_runtime_pid":100,"source_runtime_repo_root":"{repo_root}",'
            '"active_lane_ids":["mnq"],"usable_lane_count":1,"entries_enabled":true,'
            '"operator_halt":false,"last_processed_bar_end_ts":"2026-05-18T11:59:00+00:00",'
            '"health":{"market_data_ok":true}}'
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text('{"lanes": [{"lane_id": "mnq"}]}', encoding="utf-8")
    (runtime_dir / "paper_lane_quarantine_status.json").write_text(
        '{"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": false}',
        encoding="utf-8",
    )
    (runtime_dir / "market_data_transport_probe.json").write_text('{"status": "ok", "runtime_ready": true}', encoding="utf-8")
    (report_dir / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json").write_text(
        '{"classification":"BROKER_TRUTH_REFRESH_READY","generated_at":"2026-05-18T11:40:00+00:00",'
        '"last_success":true,"positions_complete":true,"open_orders_complete":true,"live_money_eligible":false}',
        encoding="utf-8",
    )
    (report_dir / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json").write_text(
        '{"classification":"TRACK_B_PAPER_BROKER_RECONCILED","generated_at":"2026-05-18T11:59:20+00:00",'
        '"broker_reconciled":true,"review_required_count":0,"lifecycle_open_position_count":0,'
        '"live_money_eligible":false}',
        encoding="utf-8",
    )
    (lease_dir / "latest_broker_truth_lease.json").write_text(
        '{"lease_state":"ACTIVE","generated_at":"2026-05-18T05:30:00+00:00",'
        '"valid_until":"2026-05-18T05:35:00+00:00",'
        '"entry_valid_until":"2026-05-18T05:35:00+00:00",'
        '"exit_valid_until":"2026-05-18T05:45:00+00:00",'
        '"submit_entry_allowed":true,"submit_exit_allowed":true,"live_money_eligible":false}',
        encoding="utf-8",
    )
    (lanes_dir / "live_timing_summary_latest.json").write_text(
        '{"lane_id":"mnq","broker_truth":{"account_health":{"status":"HEALTHY",'
        '"route_destination":"ibkr_paper_bridge_submit_capable"}}}',
        encoding="utf-8",
    )
    _write_control_plane_snapshot(repo_root)
    _write_broker_session_authority_snapshot(repo_root)
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)

    result = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=repo_root,
        output_path=lease_dir / "latest_canonical_readiness.json",
        now=now,
    )

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["broker_truth_lease"]["lease_state"] == "EXPIRED_BLOCK_NEW_ENTRIES"
    assert result["broker_truth_lease"]["refreshed_by_canonical_readiness"] is True
    assert result["readiness_blockers"][0]["code"] == "broker_truth_lease_expired"


def test_invalidated_unknown_open_orders_lease_blocks_submit_capable_state() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "INVALIDATED_UNKNOWN_OPEN_ORDERS",
        "submit_entry_allowed": False,
        "submit_exit_allowed": False,
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "broker_truth_lease_unknown_open_orders"


def test_broker_truth_uses_fresh_complete_last_good_when_latest_attempt_failed() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    result = _broker_truth_input(
        {
            "classification": "BROKER_TRUTH_REFRESH_FAILED",
            "generated_at": "2026-05-18T11:59:50+00:00",
            "last_success": False,
            "positions_complete": False,
            "open_orders_complete": False,
            "latest_attempt_status": {
                "classification": "BROKER_TRUTH_REFRESH_FAILED",
                "last_failure": True,
            },
            "last_successful_broker_truth": {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "generated_at": "2026-05-18T11:59:00+00:00",
                "positions_complete": True,
                "open_orders_complete": True,
                "open_order_count": 0,
                "position_count": 0,
                "live_money_eligible": False,
            },
        },
        now=now,
    )

    assert result["fresh"] is True
    assert result["using_last_successful_broker_truth"] is True
    assert result["latest_attempt_status"]["last_failure"] is True


def test_missing_reconciliation_blocks() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["available"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_RECONCILIATION"
    assert result["readiness_blockers"][0]["code"] == "phase1_reconciliation_not_clean"


def test_live_money_eligible_true_blocks_paper_readiness() -> None:
    inputs = _clean_inputs()
    inputs["live_money_eligible"] = True

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_CONFIG"
    assert result["readiness_blockers"][0]["code"] == "live_money_eligible_true"


def test_broker_truth_lease_live_money_flag_blocks_paper_readiness() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth_lease"] = {
        "available": True,
        "lease_state": "ACTIVE",
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "live_money_eligible": True,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_CONFIG"
    assert result["readiness_blockers"][0]["source"] == "paper_safety"


def test_lane_quarantine_with_other_eligible_lanes_keeps_submit_ready_with_warning() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["loaded_lane_count"] = 3
    inputs["runtime"]["eligible_lane_count"] = 2
    inputs["lane_quarantine"] = {
        "classification": "PAPER_LANE_QUARANTINE_ACTIVE",
        "quarantine_count": 1,
        "quarantined_lane_ids": ["lane-a"],
        "healthy_lane_ids": ["lane-b", "lane-c"],
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["readiness_warnings"][0]["code"] == "lane_quarantine_active"


def test_broker_wide_ambiguity_remains_fatal() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["positions_complete"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["source"] == "broker_truth"


def test_root_guard_classifies_wrong_root_process(monkeypatch) -> None:
    repo_root = Path("/Users/patrick/Dev/MGC-v05l-automation")
    expected_root = repo_root
    artifacts = {
        "operator_status": {
            "source_runtime_pid": 777,
            "source_runtime_repo_root": str(repo_root),
        }
    }
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 777)

    result = build_root_process_guard(
        repo_root=repo_root,
        expected_root=expected_root,
        artifacts=artifacts,
        process_cwd_resolver=lambda pid: Path("/Users/patrick/Documents/MGC-v05l-automation"),
        now=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc),
    )

    assert result["root_match"] is False
    assert result["wrong_root_processes"][0]["name"] == "paper_runtime"


def test_write_canonical_readiness_artifact_without_dashboard(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    report_dir = repo_root / "outputs" / "reports"
    lanes_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mnq"
    stale_lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mgc_stale"
    runtime_dir.mkdir(parents=True)
    (report_dir / "ibkr_read_only_verification").mkdir(parents=True)
    (report_dir / "track_b_paper_broker_reconciliation").mkdir(parents=True)
    lanes_dir.mkdir(parents=True)
    stale_lane_dir.mkdir(parents=True)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json").write_text(
        """
        {
          "source_runtime_pid": 100,
          "source_runtime_repo_root": "%s",
          "active_lane_ids": ["mnq"],
          "usable_lane_count": 1,
          "entries_enabled": true,
          "operator_halt": false,
          "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
          "health": {"market_data_ok": true}
        }
        """
        % repo_root,
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text('{"lanes": [{"lane_id": "mnq"}]}', encoding="utf-8")
    (runtime_dir / "paper_lane_quarantine_status.json").write_text(
        '{"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": false}',
        encoding="utf-8",
    )
    (runtime_dir / "market_data_transport_probe.json").write_text('{"status": "ok", "runtime_ready": true}', encoding="utf-8")
    (report_dir / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json").write_text(
        """
        {
          "classification": "BROKER_TRUTH_REFRESH_READY",
          "generated_at": "2026-05-18T11:59:10+00:00",
          "last_success": true,
          "positions_complete": true,
          "open_orders_complete": true,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (
        report_dir
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    ).write_text(
        """
        {
          "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
          "generated_at": "2026-05-18T11:59:20+00:00",
          "broker_reconciled": true,
          "review_required_count": 0,
          "lifecycle_open_position_count": 0,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (lanes_dir / "live_timing_summary_latest.json").write_text(
        """
        {
          "lane_id": "mnq",
          "broker_truth": {
            "account_health": {
              "status": "HEALTHY",
              "route_destination": "ibkr_paper_bridge_submit_capable"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    (stale_lane_dir / "live_timing_summary_latest.json").write_text(
        """
        {
          "lane_id": "mgc_stale",
          "broker_truth": {
            "account_health": {
              "status": "HEALTHY",
              "route_destination": "ibkr_paper_bridge_submit_capable"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    _write_control_plane_snapshot(repo_root)
    _write_broker_session_authority_snapshot(repo_root)
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)

    output = repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    result = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=repo_root,
        output_path=output,
        now=now,
    )

    assert output.exists()
    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert [row["lane_id"] for row in result["submit_bridge"]["route_rows"]] == ["mnq"]
