"""Tests for the operator dashboard data surface."""

from __future__ import annotations

import json
import gzip
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import mgc_v05l.app.operator_dashboard as operator_dashboard_module
from mgc_v05l.app.experimental_canaries_dashboard_payloads import load_experimental_canaries_snapshot
from mgc_v05l.app.operator_dashboard import (
    DASHBOARD_PAYLOAD_SCHEMA_VERSION,
    DashboardServerInfo,
    OperatorDashboardService,
    _archived_paper_trade_log_rows,
    _bind_dashboard_server,
    _build_handler,
    _canonical_readiness_dashboard_summary,
    _json_ready,
    _market_index_rows,
    _market_data_semantics,
    _treasury_curve_rows,
)
from mgc_v05l.app.tracked_paper_strategies import build_tracked_paper_strategies_payload
from mgc_v05l.execution_core.track_b_control_plane_top_line import build_track_b_control_plane_top_line
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.db import create_schema
from mgc_v05l.persistence.tables import research_capture_status_table


_DASHBOARD_DB_SCHEMA = """
    create table features (
      bar_id text primary key,
      payload_json text not null,
      created_at text not null
    );
    create table signals (
      bar_id text primary key,
      payload_json text not null,
      created_at text not null
    );
    create table order_intents (
      order_intent_id text primary key,
      bar_id text,
      symbol text,
      intent_type text,
      quantity integer,
      created_at text,
      reason_code text,
      broker_order_id text,
      order_status text
    );
    create table fills (
      fill_id integer primary key autoincrement,
      order_intent_id text,
      intent_type text,
      order_status text,
      fill_timestamp text,
      fill_price text,
      broker_order_id text
    );
    create table bars (
      bar_id text primary key,
      data_source text,
      ticker text,
      symbol text,
      timeframe text,
      timestamp text,
      start_ts text,
      end_ts text,
      open text,
      high text,
      low text,
      close text,
      volume integer,
      is_final integer,
      session_asia integer,
      session_london integer,
      session_us integer,
      session_allowed integer,
      created_at text
    );
    create table processed_bars (
      bar_id text primary key,
      end_ts text
    );
"""


def _init_dashboard_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "intent-1",
                "bar-1",
                "MGC",
                "BUY_TO_OPEN",
                1,
                "2026-03-18T14:00:00-04:00",
                "asiaEarlyNormalBreakoutRetestHoldTurn",
                "paper-intent-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "intent-1",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-18T14:05:00-04:00",
                "100.0",
                "paper-intent-1",
            ),
        )
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "bar-1",
                "schwab_live_poll",
                "MGC",
                "MGC",
                "5m",
                "2026-03-18T14:05:00-04:00",
                "2026-03-18T14:00:00-04:00",
                "2026-03-18T14:05:00-04:00",
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                "2026-03-18T14:05:00-04:00",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _init_empty_dashboard_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.commit()
    finally:
        connection.close()


def test_canonical_readiness_dashboard_summary_consumes_artifact() -> None:
    payload = {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": "2026-05-18T12:00:00+00:00",
        "paper_only": True,
        "canonical_readiness": "READY_SUBMIT_CAPABLE",
        "ready_submit_capable": True,
        "readiness_reasons": ["ready"],
        "readiness_blockers": [],
        "readiness_warnings": [{"code": "latest_broker_attempt_failed"}],
        "operator_action_required": False,
        "root_guard_summary": {"root_match": True},
        "broker_truth": {"fresh": True, "open_order_count": 0},
        "broker_truth_lease": {"lease_state": "EXPIRED_BLOCK_NEW_ENTRIES", "age_seconds": 23400.0},
        "phase1_reconciliation": {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        "execution_core_shared_truth": {
            "available": True,
            "proof_readiness": {
                "classification": "MARKET_CLOSED_NO_FRESH_BARS",
                "phase1_session_reason": "GLOBEX_WEEKEND_HALT",
            },
            "classifications": {
                "Open Order Truth": "NO_OPEN_ORDERS",
                "Managed Order Registry": "NO_MANAGED_ORDERS",
                "Order Adjustment Planner": "NO_ACTION_NEEDED",
                "Position Truth": "CLEAN_FLAT_READY",
                "Runtime Environment Truth": "RUNTIME_DOWN_CLEAN",
                "Managed Position Registry": "NO_MANAGED_POSITIONS",
                "Reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
                "Broker Truth Lease": "ACTIVE",
            },
        },
        "live_money_eligible": False,
    }

    summary = _canonical_readiness_dashboard_summary(
        payload,
        Path("outputs/operator_dashboard/runtime/latest_canonical_readiness.json"),
    )

    assert summary["available"] is True
    assert summary["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert summary["ready_submit_capable"] is True
    assert summary["root_guard_summary"]["root_match"] is True
    assert summary["broker_truth"]["fresh"] is True
    assert summary["broker_truth_lease"]["lease_state"] == "EXPIRED_BLOCK_NEW_ENTRIES"
    assert summary["phase1_reconciliation"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert summary["shared_truth"]["projection_only"] is True
    assert summary["shared_truth"]["not_routing_authority"] is True
    assert summary["shared_truth"]["proof_readiness"] == "MARKET_CLOSED_NO_FRESH_BARS"
    assert summary["shared_truth"]["operator_message"] == "Market closed/no fresh bars expected"
    assert summary["shared_truth"]["open_order_truth"] == "NO_OPEN_ORDERS"
    assert summary["shared_truth"]["order_adjustment_planner"] == "NO_ACTION_NEEDED"


def test_canonical_readiness_dashboard_summary_surfaces_attention_and_suspicious_order_truth() -> None:
    payload = {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": "2026-05-18T12:00:00+00:00",
        "paper_only": True,
        "canonical_readiness": "NOT_READY_DEPENDENCY",
        "readiness_reasons": ["blocked"],
        "readiness_blockers": [{"code": "position_truth_not_clean"}],
        "readiness_warnings": [],
        "operator_action_required": True,
        "root_guard_summary": {"root_match": True},
        "broker_truth": {"fresh": True},
        "broker_truth_lease": {"lease_state": "ACTIVE"},
        "phase1_reconciliation": {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        "execution_core_shared_truth": {
            "available": True,
            "proof_readiness": {"classification": "SHARED_TRUTH_BLOCKED"},
            "classifications": {
                "Open Order Truth": "SUSPICIOUS_ORDER_STATE",
                "Managed Order Registry": "CLOSE_ORDER_SUSPICIOUS",
                "Order Adjustment Planner": "REVIEW_REQUIRED_SUSPICIOUS_STATE",
                "Position Truth": "ATTENTION_REQUIRED",
                "Runtime Environment Truth": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                "Managed Position Registry": "REVIEW_REQUIRED",
                "Reconciliation": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
                "Broker Truth Lease": "INVALIDATED_CONTRADICTION",
            },
        },
        "live_money_eligible": False,
    }

    summary = _canonical_readiness_dashboard_summary(
        payload,
        Path("outputs/operator_dashboard/runtime/latest_canonical_readiness.json"),
    )

    assert summary["shared_truth"]["position_truth"] == "ATTENTION_REQUIRED"
    assert summary["shared_truth"]["open_order_truth"] == "SUSPICIOUS_ORDER_STATE"
    assert summary["shared_truth"]["managed_order_registry"] == "CLOSE_ORDER_SUSPICIOUS"
    assert summary["shared_truth"]["order_adjustment_planner"] == "REVIEW_REQUIRED_SUSPICIOUS_STATE"
    assert summary["shared_truth"]["runtime_environment_truth"] == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
    assert summary["shared_truth"]["not_routing_authority"] is True


def test_track_b_control_plane_status_projection_displays_closed_market_services(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="WAIT_MARKET_CLOSED",
        crash_loop_classification="NO_CRASH_LOOP",
        runtime_resume_classification="RESUME_BLOCKED_MARKET_CLOSED",
        runtime_resume_reason="MARKET_CLOSED_NO_FRESH_BARS",
        paper_action_policy="OBSERVE",
        paper_recovery_reason="MARKET_CLOSED_NO_FRESH_BARS",
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001
    snapshot = json.loads(
        (
            tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
        ).read_text(encoding="utf-8")
    )
    top_line = build_track_b_control_plane_top_line(snapshot)

    assert summary["projection_only"] is True
    assert summary["not_routing_authority"] is True
    assert summary["source_authority"] == "execution_core_authority"
    assert summary["source_authority_path"] is None
    assert summary["source_authority_paths"]
    assert summary["control_plane_snapshot_required"] is True
    assert summary["generated_from_control_plane_snapshot_id"] == "test-control-plane-snapshot"
    assert summary["projection_metadata_complete"] is True
    assert summary["projection_degraded"] is False
    assert summary["agent_registry"] == "AGENT_REGISTRY_READY"
    assert summary["agent_health"] == "AGENT_HEALTH_READY"
    assert summary["agent_health_schema_version"] == "track_b_agent_health_v2"
    assert summary["agent_health_classification"] == "AGENT_HEALTH_READY"
    assert summary["agent_health_summary"]["agent_count"] == 14
    assert summary["agent_health_blocks_proof"] is False
    assert summary["agent_health_blocks_runtime_submit"] is False
    assert summary["agent_health_blocks_recovery"] is False
    assert summary["agent_health_has_duplicate_writer"] is False
    assert summary["agent_health_top_blockers"] == []
    assert summary["self_recover_recommendation"] == "WAIT_MARKET_CLOSED"
    assert summary["self_recover_schema_version"] == "v2"
    assert summary["recommended_recovery_action"] == "WAIT_MARKET_CLOSED"
    assert summary["self_recover_paper_action_policy"] == "OBSERVE"
    assert summary["self_recover_autonomous_recovery_plan_classification"] == "WAIT_MARKET_CLOSED"
    assert summary["self_recover_recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert summary["self_recover_attempts_remaining"] == 2
    assert summary["self_recover_quarantine_required"] is False
    assert summary["recovery_attempt_history_no_history"] is True
    assert summary["latest_recovery_attempt_id"] == ""
    assert summary["artifact_archive_plan_classification"] == "ARCHIVE_PLAN_EMPTY"
    assert summary["artifact_archive_cold_archive_candidate_count"] == 0
    assert summary["artifact_archive_dry_run_only"] is True
    assert summary["artifact_archive_execution_enabled"] is False
    assert summary["artifact_archive_diagnostic_only"] is True
    assert summary["artifact_archive_not_routing_authority"] is True
    assert summary["crash_loop_classification"] == "NO_CRASH_LOOP"
    assert summary["runtime_resume_classification"] == "RESUME_BLOCKED_MARKET_CLOSED"
    assert summary["runtime_resume_allowed"] is False
    assert summary["runtime_resume_safe_to_start_runtime"] is False
    assert summary["runtime_resume_semantics_version"] == "v2"
    assert summary["runtime_resume_action_policy"] == "HOLD_MARKET_CLOSED"
    assert summary["runtime_supervisor_classification"] == "SUPERVISOR_WAIT_MARKET_CLOSED"
    assert summary["runtime_supervisor_mode"] == "MARKET_CLOSED_WAIT"
    assert summary["runtime_supervisor_proof_window_status"] == "market_closed"
    assert summary["runtime_supervisor_shared_truth_refresh_generation_id"] == "test-shared-truth-generation"
    assert summary["runtime_supervisor_shared_truth_coherence_status"] == "COHERENT"
    assert summary["control_plane_snapshot_id"] == "test-control-plane-snapshot"
    assert summary["control_plane_status_classification"] == "CONTROL_PLANE_READY"
    assert summary["control_plane_diagnostic_only"] is False
    assert summary["control_plane_snapshot_safe_to_start_runtime"] is False
    assert summary["control_plane_snapshot_classification"] == "CONTROL_PLANE_SNAPSHOT_READY"
    assert summary["control_plane_snapshot_shared_truth_generation_id"] == "test-shared-truth-generation"
    assert summary["control_plane_snapshot_shared_truth_coherence_status"] == "COHERENT"
    assert summary["control_plane_snapshot_supervisor_mode"] == "MARKET_CLOSED_WAIT"
    assert summary["top_line_classification"] == top_line["top_line_classification"]
    assert summary["top_line_status"] == top_line["top_line_status"]
    assert summary["top_line_classification"] == "MARKET_CLOSED_WAIT"
    assert "Market closed/no fresh bars expected" in summary["top_line_status"]
    assert summary["runtime_supervisor_recommended_next_command"] == (
        "wait for market reopen; rerun proof readiness before any runtime start"
    )
    assert summary["runtime_supervisor_operator_ack_required"] is False
    assert summary["autonomous_recovery_plan_classification"] == "WAIT_MARKET_CLOSED"
    assert summary["autonomous_recovery_next_action"] == "WAIT_MARKET_CLOSED"
    assert summary["autonomous_recovery_execution_enabled"] is False
    assert summary["primary_blocking_agent_id"] == "market_session"
    assert "no fresh Phase-1 bars are expected" in summary["operator_explanation"]
    assert "Wait for Globex/session reopen" in summary["recommended_observation_step"]
    assert summary["paper_recovery_policy"] == "OBSERVE"
    assert summary["paper_recovery_diagnostic"] == "WAIT_MARKET_CLOSED"
    assert summary["requires_operator_ack_for_paper"] is False
    assert summary["market_closed_no_fresh_bars_expected"] is True
    assert summary["operator_message"] == (
        "MARKET_CLOSED_WAIT: market closed/no fresh bars expected; wait and rerun proof readiness after reopen"
    )
    assert "outputs/operator_dashboard/runtime/latest_track_b_runtime_resume_semantics.json" not in json.dumps(summary)
    assert "outputs/operator_dashboard/runtime/latest_track_b_runtime_supervisor_authority.json" not in json.dumps(summary)
    assert "outputs/operator_dashboard/runtime/latest_track_b_paper_recovery_policy.json" not in json.dumps(summary)


def test_track_b_control_plane_status_missing_snapshot_is_diagnostic_only(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="RESTART_RUNTIME_ALLOWED",
        runtime_resume_classification="RESUME_ALLOWED_PAPER_BOUNDED_RETRY",
        runtime_resume_reason="PAPER Recovery Policy permits bounded retry.",
        runtime_resume_allowed=True,
        runtime_resume_safe_to_start_runtime=True,
        runtime_supervisor_classification="SUPERVISOR_RUNTIME_START_ALLOWED",
        runtime_supervisor_mode="READY_FOR_OPERATOR_START",
        runtime_supervisor_proof_window_status="ready",
        paper_action_policy="AUTONOMOUS_RETRY_ELIGIBLE",
        paper_autonomous_recovery_allowed=True,
    )
    (
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
    ).unlink()
    _write_json_file(
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json",
        {
            "projection_only": True,
            "not_routing_authority": True,
            "control_plane_snapshot_id": "dashboard-projection-only",
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
        },
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001

    assert summary["control_plane_status_classification"] == "CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY"
    assert summary["control_plane_diagnostic_only"] is True
    assert summary["control_plane_snapshot_missing"] is True
    assert summary["control_plane_snapshot_safe_to_start_runtime"] is False
    assert summary["runtime_resume_raw_safe_to_start_runtime"] is True
    assert summary["runtime_resume_safe_to_start_runtime"] is False
    assert summary["control_plane_snapshot_id"] is None
    assert "dashboard-projection-only" not in json.dumps(summary)


def test_track_b_control_plane_status_stale_snapshot_is_diagnostic_only(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="RESTART_RUNTIME_ALLOWED",
        runtime_resume_classification="RESUME_ALLOWED_PAPER_BOUNDED_RETRY",
        runtime_resume_reason="PAPER Recovery Policy permits bounded retry.",
        runtime_resume_allowed=True,
        runtime_resume_safe_to_start_runtime=True,
        runtime_supervisor_classification="SUPERVISOR_RUNTIME_START_ALLOWED",
        runtime_supervisor_mode="READY_FOR_OPERATOR_START",
        runtime_supervisor_proof_window_status="ready",
        paper_action_policy="AUTONOMOUS_RETRY_ELIGIBLE",
        paper_autonomous_recovery_allowed=True,
    )
    path = tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["generated_at"] = "2026-05-23T10:00:00+00:00"
    _write_json_file(path, payload)

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001

    assert summary["control_plane_status_classification"] == "CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY"
    assert summary["control_plane_snapshot_stale"] is True
    assert summary["control_plane_snapshot_safe_to_start_runtime"] is False
    assert summary["runtime_resume_safe_to_start_runtime"] is False


def test_track_b_control_plane_status_projection_displays_crash_loop_and_operator_ack(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="RESTART_RUNTIME_BLOCKED",
        crash_loop_classification="OPERATOR_ACK_REQUIRED",
        crash_loop_restart_blocked=True,
        runtime_resume_classification="RESUME_BLOCKED_OPERATOR_ACK_REQUIRED",
        runtime_resume_reason="Prior unsafe stop requires operator acknowledgement.",
        runtime_resume_required_operator_ack=True,
        runtime_resume_blockers=[{"code": "operator_ack_required", "detail": "paper_reconciliation_mismatch"}],
        paper_action_policy="QUARANTINE_OBSERVE_ONLY",
        paper_recovery_severity="ATTENTION",
        paper_budget={"budget_exhausted": True},
        latest_recovery_attempt_id="attempt-quarantine",
        latest_recovery_attempt_classification="EXECUTOR_BLOCKED_BUDGET_EXHAUSTED",
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001

    assert summary["crash_loop_classification"] == "OPERATOR_ACK_REQUIRED"
    assert summary["crash_loop_restart_blocked"] is True
    assert summary["runtime_resume_classification"] == "RESUME_BLOCKED_OPERATOR_ACK_REQUIRED"
    assert summary["runtime_resume_required_operator_ack"] is True
    assert summary["runtime_supervisor_mode"] == "CRASH_LOOP_HOLD"
    assert summary["runtime_supervisor_operator_ack_required"] is True
    assert summary["runtime_supervisor_operator_ack_required_for_paper"] is False
    assert summary["operator_ack_advisory_only_for_paper"] is True
    assert summary["runtime_supervisor_operator_ack"]["ack_type"] == "crash_loop_hold"
    assert summary["paper_recovery_policy"] == "QUARANTINE_OBSERVE_ONLY"
    assert summary["paper_recovery_diagnostic"] == "QUARANTINE_OBSERVE_ONLY"
    assert summary["recommended_recovery_action"] == "QUARANTINE_OBSERVE_ONLY"
    assert summary["self_recover_paper_action_policy"] == "QUARANTINE_OBSERVE_ONLY"
    assert summary["self_recover_attempts_remaining"] == 0
    assert summary["self_recover_cooldown_until"] == "2026-05-23T12:15:00+00:00"
    assert summary["self_recover_quarantine_required"] is True
    assert summary["latest_recovery_attempt_id"] == "attempt-quarantine"
    assert summary["latest_recovery_attempt_classification"] == "EXECUTOR_BLOCKED_BUDGET_EXHAUSTED"
    assert summary["recovery_attempt_quarantine_required"] is True
    assert summary["bounded_recovery_budget"]["budget_exhausted"] is True
    assert summary["runtime_resume_action_policy"] == "QUARANTINE_OBSERVE_ONLY"
    assert summary["runtime_resume_attempts_remaining"] == 0
    assert summary["runtime_resume_cooldown_until"] == "2026-05-23T12:15:00+00:00"
    assert summary["attention_required"] is True
    assert summary["runtime_resume_blockers"] == [
        {"code": "operator_ack_required", "detail": "paper_reconciliation_mismatch"}
    ]


def test_track_b_control_plane_status_projection_displays_bounded_autonomous_retry(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="RESTART_RUNTIME_ALLOWED",
        runtime_resume_classification="RESUME_ALLOWED_PAPER_BOUNDED_RETRY",
        runtime_resume_reason="PAPER Recovery Policy permits bounded retry.",
        runtime_resume_allowed=True,
        runtime_resume_safe_to_start_runtime=True,
        runtime_supervisor_classification="SUPERVISOR_RUNTIME_START_ALLOWED",
        runtime_supervisor_mode="READY_FOR_OPERATOR_START",
        runtime_supervisor_proof_window_status="ready",
        paper_action_policy="AUTONOMOUS_RETRY_ELIGIBLE",
        paper_autonomous_recovery_allowed=True,
        latest_recovery_attempt_id="attempt-1",
        latest_recovery_attempt_classification="EXECUTOR_DRY_RUN_READY",
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001

    assert summary["paper_recovery_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert summary["paper_recovery_diagnostic"] == "BOUNDED_AUTONOMOUS_RETRY"
    assert summary["recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert summary["self_recover_paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert summary["self_recover_autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert summary["self_recover_recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert summary["self_recover_attempts_remaining"] == 2
    assert summary["self_recover_quarantine_required"] is False
    assert summary["latest_recovery_attempt_id"] == "attempt-1"
    assert summary["latest_recovery_attempt_action_type"] == "RUNTIME_RETRY"
    assert summary["latest_recovery_attempt_classification"] == "EXECUTOR_DRY_RUN_READY"
    assert summary["recovery_attempt_recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert summary["recovery_attempt_recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert summary["recovery_attempt_attempts_remaining"] == 2
    assert summary["recovery_attempt_history_no_history"] is False
    assert summary["autonomous_recovery_allowed"] is True
    assert summary["autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert summary["autonomous_recovery_next_action"] == "RUNTIME_RETRY"
    assert summary["autonomous_recovery_execution_enabled"] is False
    assert summary["requires_operator_ack_for_paper"] is False
    assert summary["runtime_resume_allowed"] is True
    assert summary["runtime_resume_action_policy"] == "NEW_RUNTIME_GENERATION_ALLOWED"
    assert summary["runtime_resume_proposed_next_runtime_generation_id"] == "runtime-generation-next"
    assert summary["runtime_resume_attempts_remaining"] == 2
    assert summary["attention_required"] is False


def test_track_b_control_plane_status_projection_displays_blocked_artifact_archive_plan(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="RESTART_RUNTIME_ALLOWED",
        runtime_resume_classification="RESUME_ALLOWED_CLEAN",
        runtime_resume_reason="ready",
        runtime_resume_allowed=True,
        runtime_resume_safe_to_start_runtime=True,
        runtime_supervisor_classification="SUPERVISOR_RUNTIME_START_ALLOWED",
        runtime_supervisor_mode="READY_FOR_OPERATOR_START",
        artifact_archive_plan_classification="ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE",
        artifact_archive_active_lifecycle_protected_count=1,
        artifact_archive_blocked_candidate_count=0,
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001

    assert summary["artifact_archive_plan_classification"] == "ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE"
    assert summary["artifact_archive_active_lifecycle_protected_count"] == 1
    assert summary["artifact_archive_blocked_candidate_count"] == 0
    assert summary["artifact_archive_dry_run_only"] is True
    assert summary["artifact_archive_execution_enabled"] is False
    assert summary["artifact_archive_diagnostic_only"] is True
    assert summary["artifact_archive_not_routing_authority"] is True


def test_track_b_control_plane_status_projection_displays_hard_unsafe_paper_policy(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="DO_NOT_RECOVER_UNSAFE_STATE",
        runtime_resume_classification="RESUME_BLOCKED_HARD_UNSAFE",
        runtime_resume_reason="live_money_eligible=true",
        runtime_resume_blockers=[{"code": "live_money_eligible_true", "detail": "hard unsafe"}],
        runtime_supervisor_classification="SUPERVISOR_HARD_UNSAFE_HOLD",
        runtime_supervisor_mode="HARD_UNSAFE_HOLD",
        paper_action_policy="HARD_UNSAFE_HOLD",
        paper_recovery_severity="UNSAFE",
        paper_recovery_live_action_policy="HOLD_DOWN",
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001
    snapshot = json.loads(
        (
            tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
        ).read_text(encoding="utf-8")
    )
    top_line = build_track_b_control_plane_top_line(snapshot)

    assert summary["paper_recovery_policy"] == "HARD_UNSAFE_HOLD"
    assert summary["paper_recovery_diagnostic"] == "HARD_UNSAFE_HOLD"
    assert summary["recommended_recovery_action"] == "HARD_UNSAFE_HOLD"
    assert summary["self_recover_paper_action_policy"] == "HARD_UNSAFE_HOLD"
    assert summary["paper_recovery_severity"] == "UNSAFE"
    assert summary["autonomous_recovery_execution_enabled"] is False
    assert summary["live_action_policy"] == "HOLD_DOWN"
    assert summary["attention_required"] is True


def test_track_b_control_plane_status_projection_displays_duplicate_writer_hard_unsafe(tmp_path: Path) -> None:
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="DO_NOT_RECOVER_UNSAFE_STATE",
        runtime_resume_classification="RESUME_BLOCKED_HARD_UNSAFE",
        runtime_resume_reason="Duplicate runtime writer evidence is present.",
        runtime_resume_blockers=[{"code": "duplicate_runtime_writer", "detail": "hard unsafe"}],
        runtime_supervisor_classification="SUPERVISOR_HARD_UNSAFE_HOLD",
        runtime_supervisor_mode="HARD_UNSAFE_HOLD",
        paper_action_policy="HARD_UNSAFE_HOLD",
        paper_recovery_severity="UNSAFE",
        paper_recovery_reason="duplicate_runtime_writer",
        paper_recovery_live_action_policy="HOLD_DOWN",
    )

    summary = operator_dashboard_module._track_b_control_plane_services_summary(tmp_path)  # noqa: SLF001
    snapshot = json.loads(
        (
            tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
        ).read_text(encoding="utf-8")
    )
    top_line = build_track_b_control_plane_top_line(snapshot)

    assert summary["paper_recovery_policy"] == "HARD_UNSAFE_HOLD"
    assert summary["paper_recovery_diagnostic"] == "HARD_UNSAFE_HOLD"
    assert summary["recommended_recovery_action"] == "HARD_UNSAFE_HOLD"
    assert summary["self_recover_paper_action_policy"] == "HARD_UNSAFE_HOLD"
    assert summary["runtime_resume_blockers"] == [{"code": "duplicate_runtime_writer", "detail": "hard unsafe"}]
    assert summary["runtime_resume_action_policy"] == "HOLD_DUPLICATE_WRITER"
    assert summary["primary_blocking_agent_id"] == "track_b_paper_runtime"
    assert summary["top_line_classification"] == top_line["top_line_classification"]
    assert summary["top_line_status"] == top_line["top_line_status"]
    assert summary["top_line_classification"] == "HARD_UNSAFE_DUPLICATE_WRITER"
    assert summary["prioritized_blockers"][0]["status"] == "DUPLICATE_PROCESS"
    assert "Hard PAPER invariant" in summary["operator_explanation"]
    assert summary["attention_required"] is True


def test_latest_track_b_operator_status_payload_includes_control_plane_projection(tmp_path: Path) -> None:
    status_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "operator_status"
        / "latest_operator_status_summary.json"
    )
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps({"generated_at": "2026-05-23T12:00:00+00:00"}) + "\n", encoding="utf-8")
    _write_track_b_control_plane_artifacts(
        tmp_path,
        self_recover_recommendation="WAIT_MARKET_CLOSED",
        runtime_resume_classification="RESUME_BLOCKED_MARKET_CLOSED",
        runtime_resume_reason="MARKET_CLOSED_NO_FRESH_BARS",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    control_plane = payload["track_b_control_plane"]
    assert control_plane["projection_only"] is True
    assert control_plane["not_routing_authority"] is True
    assert control_plane["source_authority"] == "execution_core_authority"
    assert control_plane["source_authority_paths"]
    assert control_plane["control_plane_snapshot_required"] is True
    assert control_plane["generated_from_control_plane_snapshot_id"] == "test-control-plane-snapshot"
    assert control_plane["projection_metadata_complete"] is True
    assert control_plane["runtime_resume_classification"] == "RESUME_BLOCKED_MARKET_CLOSED"
    assert control_plane["runtime_supervisor_mode"] == "MARKET_CLOSED_WAIT"
    assert control_plane["runtime_supervisor_proof_window_status"] == "market_closed"
    assert control_plane["operator_message"] == (
        "MARKET_CLOSED_WAIT: market closed/no fresh bars expected; wait and rerun proof readiness after reopen"
    )


def _write_track_b_control_plane_artifacts(
    root: Path,
    *,
    self_recover_recommendation: str,
    crash_loop_classification: str = "NO_CRASH_LOOP",
    crash_loop_restart_blocked: bool = False,
    runtime_resume_classification: str,
    runtime_resume_reason: str,
    runtime_resume_allowed: bool = False,
    runtime_resume_safe_to_start_runtime: bool = False,
    runtime_resume_required_operator_ack: bool = False,
    runtime_resume_blockers: list[dict[str, str]] | None = None,
    runtime_supervisor_classification: str | None = None,
    runtime_supervisor_mode: str | None = None,
    runtime_supervisor_proof_window_status: str | None = None,
    runtime_supervisor_recommended_next_command: str | None = None,
    runtime_supervisor_operator_ack: dict[str, object] | None = None,
    paper_action_policy: str = "AUTONOMOUS_RETRY_ELIGIBLE",
    paper_recovery_severity: str = "INFO",
    paper_recovery_reason: str = "",
    paper_autonomous_recovery_allowed: bool = False,
    paper_budget: dict[str, object] | None = None,
    paper_recovery_live_action_policy: str = "REQUIRE_ACK",
    autonomous_recovery_plan_classification: str | None = None,
    autonomous_recovery_next_action: str | None = None,
    latest_recovery_attempt_id: str = "",
    latest_recovery_attempt_classification: str = "",
    artifact_archive_plan_classification: str = "ARCHIVE_PLAN_EMPTY",
    artifact_archive_hot_authority_protected_count: int = 24,
    artifact_archive_active_lifecycle_protected_count: int = 0,
    artifact_archive_warm_diagnostic_count: int = 0,
    artifact_archive_cold_archive_candidate_count: int = 0,
    artifact_archive_blocked_candidate_count: int = 0,
    artifact_archive_estimated_bytes: int = 0,
) -> None:
    if runtime_resume_classification == "RESUME_BLOCKED_MARKET_CLOSED":
        resume_action_policy = "HOLD_MARKET_CLOSED"
    elif "Duplicate runtime writer" in runtime_resume_reason:
        resume_action_policy = "HOLD_DUPLICATE_WRITER"
    elif "live_money_eligible" in runtime_resume_reason:
        resume_action_policy = "HOLD_LIVE_MONEY"
    elif paper_action_policy == "QUARANTINE_OBSERVE_ONLY":
        resume_action_policy = "QUARANTINE_OBSERVE_ONLY"
    else:
        resume_action_policy = "NEW_RUNTIME_GENERATION_ALLOWED"
    self_recover_plan_classification = autonomous_recovery_plan_classification or (
        "WAIT_MARKET_CLOSED"
        if runtime_resume_classification == "RESUME_BLOCKED_MARKET_CLOSED"
        else "PLAN_RUNTIME_RETRY"
        if paper_action_policy == "AUTONOMOUS_RETRY_ELIGIBLE"
        else "PLAN_HARD_UNSAFE_HOLD"
        if paper_action_policy == "HARD_UNSAFE_HOLD"
        else "PLAN_QUARANTINE_OBSERVE_ONLY"
    )
    self_recover_recommended_action = (
        "WAIT_MARKET_CLOSED"
        if runtime_resume_classification == "RESUME_BLOCKED_MARKET_CLOSED"
        else "RUNTIME_RETRY_DRY_RUN"
        if paper_action_policy == "AUTONOMOUS_RETRY_ELIGIBLE"
        else "HARD_UNSAFE_HOLD"
        if paper_action_policy == "HARD_UNSAFE_HOLD"
        else "QUARANTINE_OBSERVE_ONLY"
    )
    self_recover_attempts_remaining = 0 if paper_action_policy == "QUARANTINE_OBSERVE_ONLY" else 2
    self_recover_cooldown_until = (
        "2026-05-23T12:15:00+00:00" if paper_action_policy == "QUARANTINE_OBSERVE_ONLY" else None
    )
    self_recover_budget_key = "track_b_paper_runtime|RUNTIME_RETRY|test"
    _write_json_file(
        root / "outputs/track_b_execution_core/agent_registry/latest_agent_registry.json",
        {"classification": "AGENT_REGISTRY_READY"},
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "schema_version": "track_b_agent_health_v2",
            "classification": "AGENT_HEALTH_READY",
            "summary": {
                "agent_count": 14,
                "blocking_for_proof_count": 0,
                "blocking_for_runtime_submit_count": 0,
                "blocking_for_recovery_count": 0,
                "duplicate_process_count": 0,
                "missing_artifact_count": 0,
                "stale_pid_count": 0,
                "source_commit_mismatch_count": 0,
                "root_mismatch_count": 0,
            },
            "agents": [],
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json",
        {
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "self_recover_schema_version": "v2",
            "recovery_plan_id": "test-self-recover-plan",
            "control_plane_snapshot_id": "test-control-plane-snapshot",
            "shared_truth_generation_id": "test-shared-truth-generation",
            "paper_action_policy": paper_action_policy,
            "autonomous_recovery_plan_classification": self_recover_plan_classification,
            "recommended_recovery_action": self_recover_recommended_action,
            "recovery_budget_key": self_recover_budget_key,
            "attempts_remaining": self_recover_attempts_remaining,
            "cooldown_until": self_recover_cooldown_until,
            "quarantine_required": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            "agent_health_top_blockers": [],
            "operator_explanation": paper_recovery_reason,
            "execution_enabled": False,
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json",
        {"classification": crash_loop_classification, "restart_blocked": crash_loop_restart_blocked},
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "classification": runtime_resume_classification,
            "resume_semantics_version": "v2",
            "resume_action_policy": resume_action_policy,
            "previous_runtime_generation_id": "runtime-generation-previous",
            "proposed_next_runtime_generation_id": "runtime-generation-next",
            "bounded_retry_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": 0 if paper_action_policy == "QUARANTINE_OBSERVE_ONLY" else 2,
            "cooldown_until": "2026-05-23T12:15:00+00:00"
            if paper_action_policy == "QUARANTINE_OBSERVE_ONLY"
            else None,
            "generation_reuse_allowed": False,
            "must_start_new_generation": True,
            "allowed": runtime_resume_allowed,
            "safe_to_start_runtime": runtime_resume_safe_to_start_runtime,
            "required_operator_ack": runtime_resume_required_operator_ack,
            "resume_mode": "hold_down_market_closed",
            "reason": runtime_resume_reason,
            "blockers": runtime_resume_blockers or [],
            "warnings": [],
        },
    )
    supervisor_classification = runtime_supervisor_classification or (
        "SUPERVISOR_WAIT_MARKET_CLOSED"
        if runtime_resume_classification == "RESUME_BLOCKED_MARKET_CLOSED"
        else "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP"
        if runtime_resume_required_operator_ack or crash_loop_restart_blocked
        else "SUPERVISOR_RUNTIME_START_ALLOWED"
    )
    supervisor_mode = runtime_supervisor_mode or (
        "MARKET_CLOSED_WAIT"
        if supervisor_classification == "SUPERVISOR_WAIT_MARKET_CLOSED"
        else "CRASH_LOOP_HOLD"
        if supervisor_classification == "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP"
        else "READY_FOR_OPERATOR_START"
    )
    operator_ack = runtime_supervisor_operator_ack or {
        "required": supervisor_mode == "CRASH_LOOP_HOLD",
        "reason": "Crash Loop Protection is OPERATOR_ACK_REQUIRED." if supervisor_mode == "CRASH_LOOP_HOLD" else "",
        "ack_type": "crash_loop_hold" if supervisor_mode == "CRASH_LOOP_HOLD" else None,
        "ack_id_expected": "track_b_paper_crash_loop_hold_test" if supervisor_mode == "CRASH_LOOP_HOLD" else None,
    }
    _write_json_file(
        root / "outputs/track_b_execution_core/paper_recovery_policy/latest_paper_recovery_policy.json",
        {
            "severity": paper_recovery_severity,
            "paper_action_policy": paper_action_policy,
            "live_action_policy": paper_recovery_live_action_policy,
            "autonomous_recovery_allowed": paper_autonomous_recovery_allowed,
            "requires_operator_ack_for_paper": False,
            "reason": paper_recovery_reason,
            "bounded_recovery_budget": paper_budget
            or {
                "max_attempts_per_target": 1,
                "max_attempts_per_window": 2,
                "cooldown_seconds": 300,
                "budget_exhausted": False,
            },
            "artifact_paths": {
                "authority": "outputs/track_b_execution_core/paper_recovery_policy/latest_paper_recovery_policy.json",
            },
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "proof_window_status": runtime_supervisor_proof_window_status
            or ("market_closed" if supervisor_mode == "MARKET_CLOSED_WAIT" else "blocked"),
            "recommended_next_command": runtime_supervisor_recommended_next_command
            or (
                "wait for market reopen; rerun proof readiness before any runtime start"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "acknowledge crash-loop hold before any restart planning"
                if supervisor_mode == "CRASH_LOOP_HOLD"
                else "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
            ),
            "operator_ack": operator_ack,
            "autonomous_recovery_plan_classification": autonomous_recovery_plan_classification
            or (
                "WAIT_MARKET_CLOSED"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "PLAN_RUNTIME_RETRY"
                if supervisor_mode == "READY_FOR_OPERATOR_START"
                else "PLAN_QUARANTINE_OBSERVE_ONLY"
            ),
            "autonomous_recovery_next_action": autonomous_recovery_next_action
            or (
                "WAIT_MARKET_CLOSED"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "RUNTIME_RETRY"
                if supervisor_mode == "READY_FOR_OPERATOR_START"
                else "QUARANTINE_OBSERVE_ONLY"
            ),
            "autonomous_recovery_execution_enabled": False,
            "self_recover_schema_version": "v2",
            "self_recover_recovery_plan_id": "test-self-recover-plan",
            "self_recover_control_plane_snapshot_id": "test-control-plane-snapshot",
            "self_recover_shared_truth_generation_id": "test-shared-truth-generation",
            "self_recover_recommended_recovery_action": self_recover_recommended_action,
            "self_recover_paper_action_policy": paper_action_policy,
            "self_recover_autonomous_recovery_plan_classification": self_recover_plan_classification,
            "self_recover_recovery_budget_key": self_recover_budget_key,
            "self_recover_attempts_remaining": self_recover_attempts_remaining,
            "self_recover_cooldown_until": self_recover_cooldown_until,
            "self_recover_quarantine_required": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            "self_recover_agent_health_top_blockers": [],
            "self_recover_operator_explanation": paper_recovery_reason,
            "self_recover_execution_enabled": False,
            "shared_truth_refresh_generation_id": "test-shared-truth-generation",
            "shared_truth_coherence_status": "COHERENT",
            "stale_or_mixed_sources": [],
            "autonomous_recovery_blockers": [],
            "autonomous_recovery_budget_summary": paper_budget or {"budget_exhausted": False},
            "blockers": [{"code": "test_blocker", "detail": "test"}] if operator_ack.get("required") else [],
            "warnings": [],
            "decision_precedence": [
                {
                    "rank": 1,
                    "service": "Proof Readiness / Phase-1 Readiness",
                    "classification": supervisor_classification,
                    "decisive": True,
                }
            ],
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "control_plane_snapshot_id": "test-control-plane-snapshot",
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "shared_truth_refresh_generation_id": "test-shared-truth-generation",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "proof_window_status": runtime_supervisor_proof_window_status
            or ("market_closed" if supervisor_mode == "MARKET_CLOSED_WAIT" else "blocked"),
            "recommended_next_command": runtime_supervisor_recommended_next_command
            or (
                "wait for market reopen; rerun proof readiness before any runtime start"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "acknowledge crash-loop hold before any restart planning"
                if supervisor_mode == "CRASH_LOOP_HOLD"
                else "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
            ),
            "paper_recovery_policy": paper_action_policy,
            "self_recover_schema_version": "v2",
            "self_recover_recovery_plan_id": "test-self-recover-plan",
            "self_recover_control_plane_snapshot_id": "test-control-plane-snapshot",
            "self_recover_shared_truth_generation_id": "test-shared-truth-generation",
            "recommended_recovery_action": self_recover_recommended_action,
            "paper_action_policy": paper_action_policy,
            "self_recover_autonomous_recovery_plan_classification": self_recover_plan_classification,
            "recovery_budget_key": self_recover_budget_key,
            "attempts_remaining": self_recover_attempts_remaining,
            "cooldown_until": self_recover_cooldown_until,
            "quarantine_required": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            "self_recover_agent_health_top_blockers": [],
            "self_recover_operator_explanation": paper_recovery_reason,
            "self_recover_execution_enabled": False,
            "latest_recovery_attempt_id": latest_recovery_attempt_id,
            "latest_recovery_attempt_action_type": "RUNTIME_RETRY" if latest_recovery_attempt_id else "",
            "latest_recovery_attempt_classification": latest_recovery_attempt_classification,
            "recovery_attempt_recommended_recovery_action": self_recover_recommended_action
            if latest_recovery_attempt_id
            else "",
            "recovery_attempt_recovery_budget_key": self_recover_budget_key if latest_recovery_attempt_id else "",
            "recovery_attempt_attempts_remaining": self_recover_attempts_remaining
            if latest_recovery_attempt_id
            else None,
            "recovery_attempt_quarantine_required": (
                paper_action_policy == "QUARANTINE_OBSERVE_ONLY" and bool(latest_recovery_attempt_id)
            ),
            "recovery_attempt_last_success_at": None,
            "recovery_attempt_last_failure_at": None,
            "recovery_attempt_history_no_history": not bool(latest_recovery_attempt_id),
            "recovery_attempt_recent_attempts": (
                [
                    {
                        "recovery_attempt_id": latest_recovery_attempt_id,
                        "action_type": "RUNTIME_RETRY",
                        "classification": latest_recovery_attempt_classification,
                        "recommended_recovery_action": self_recover_recommended_action,
                        "recovery_budget_key": self_recover_budget_key,
                        "quarantine_required": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
                        "execution_enabled": False,
                    }
                ]
                if latest_recovery_attempt_id
                else []
            ),
            "artifact_archive_plan_classification": artifact_archive_plan_classification,
            "artifact_archive_hot_authority_protected_count": artifact_archive_hot_authority_protected_count,
            "artifact_archive_active_lifecycle_protected_count": artifact_archive_active_lifecycle_protected_count,
            "artifact_archive_warm_diagnostic_count": artifact_archive_warm_diagnostic_count,
            "artifact_archive_cold_archive_candidate_count": artifact_archive_cold_archive_candidate_count,
            "artifact_archive_blocked_candidate_count": artifact_archive_blocked_candidate_count,
            "artifact_archive_estimated_bytes": artifact_archive_estimated_bytes,
            "artifact_archive_dry_run_only": True,
            "artifact_archive_execution_enabled": False,
            "artifact_archive_diagnostic_only": True,
            "artifact_archive_not_routing_authority": True,
            "runtime_resume_semantics_version": "v2",
            "runtime_resume_action_policy": resume_action_policy,
            "runtime_resume_previous_runtime_generation_id": "runtime-generation-previous",
            "runtime_resume_proposed_next_runtime_generation_id": "runtime-generation-next",
            "runtime_resume_bounded_retry_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "runtime_resume_attempts_remaining": 0
            if paper_action_policy == "QUARANTINE_OBSERVE_ONLY"
            else 2,
            "runtime_resume_cooldown_until": "2026-05-23T12:15:00+00:00"
            if paper_action_policy == "QUARANTINE_OBSERVE_ONLY"
            else None,
            "runtime_resume_generation_reuse_allowed": False,
            "runtime_resume_must_start_new_generation": True,
            "agent_health_schema_version": "track_b_agent_health_v2",
            "agent_health_classification": "AGENT_HEALTH_READY",
            "agent_health_summary": {
                "agent_count": 14,
                "blocking_for_proof_count": 0,
                "blocking_for_runtime_submit_count": 0,
                "blocking_for_recovery_count": 0,
                "duplicate_process_count": 0,
                "missing_artifact_count": 0,
                "stale_pid_count": 0,
                "source_commit_mismatch_count": 0,
                "root_mismatch_count": 0,
            },
            "agent_health_top_blockers": [],
            "agent_health_blocks_proof": False,
            "agent_health_blocks_runtime_submit": False,
            "agent_health_blocks_recovery": False,
            "agent_health_has_duplicate_writer": False,
            "duplicate_process_count": 0,
            "missing_artifact_count": 0,
            "stale_pid_count": 0,
            "source_commit_mismatch_count": 0,
            "root_mismatch_count": 0,
            "primary_blocking_agent_id": (
                "market_session"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "track_b_paper_runtime"
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "primary_blocking_reason": (
                "MARKET_CLOSED_NO_FRESH_BARS"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "duplicate runtime writer detected"
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "operator_explanation": (
                "Market/session is closed; no fresh Phase-1 bars are expected, and PAPER should wait without treating this as a process failure."
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "Hard PAPER invariant blocked recovery: Track B PAPER runtime reports duplicate runtime writer detected."
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "recommended_observation_step": (
                "Wait for Globex/session reopen, then rebuild the Control Plane Snapshot before any proof attempt."
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "Preserve evidence and do not run autonomous recovery until the hard invariant clears in execution_core authority."
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "prioritized_blockers": (
                [
                    {
                        "agent_id": "track_b_paper_runtime",
                        "display_name": "Track B PAPER runtime",
                        "status": "DUPLICATE_PROCESS",
                        "reason": "duplicate runtime writer detected",
                        "blocking_for_proof": True,
                        "blocking_for_runtime_submit": True,
                        "blocking_for_recovery": True,
                        "diagnostic_only": False,
                        "source": "agent_health",
                        "priority": 0,
                    }
                ]
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else []
            ),
            "autonomous_recovery_plan_classification": autonomous_recovery_plan_classification
            or (
                "WAIT_MARKET_CLOSED"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "PLAN_RUNTIME_RETRY"
                if supervisor_mode == "READY_FOR_OPERATOR_START"
                else "PLAN_QUARANTINE_OBSERVE_ONLY"
            ),
            "autonomous_recovery_next_action": autonomous_recovery_next_action
            or (
                "WAIT_MARKET_CLOSED"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "RUNTIME_RETRY"
                if supervisor_mode == "READY_FOR_OPERATOR_START"
                else "QUARANTINE_OBSERVE_ONLY"
            ),
            "autonomous_recovery_execution_enabled": False,
            "safe_to_start_runtime": supervisor_mode == "READY_FOR_OPERATOR_START",
            "blockers": [{"code": "test_blocker", "detail": "test"}] if supervisor_mode != "READY_FOR_OPERATOR_START" else [],
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "classification": autonomous_recovery_plan_classification
            or (
                "WAIT_MARKET_CLOSED"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "PLAN_RUNTIME_RETRY"
                if supervisor_mode == "READY_FOR_OPERATOR_START"
                else "PLAN_QUARANTINE_OBSERVE_ONLY"
            ),
            "execution_enabled": False,
            "primary_blocking_agent_id": (
                "market_session"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "track_b_paper_runtime"
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "primary_blocking_reason": (
                "MARKET_CLOSED_NO_FRESH_BARS"
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "duplicate runtime writer detected"
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "operator_explanation": (
                "Market/session is closed; no fresh Phase-1 bars are expected, and PAPER should wait without treating this as a process failure."
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "Hard PAPER invariant blocked recovery: Track B PAPER runtime reports duplicate runtime writer detected."
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "recommended_observation_step": (
                "Wait for Globex/session reopen, then rebuild the Control Plane Snapshot before any proof attempt."
                if supervisor_mode == "MARKET_CLOSED_WAIT"
                else "Preserve evidence and do not run autonomous recovery until the hard invariant clears in execution_core authority."
                if supervisor_classification == "SUPERVISOR_HARD_UNSAFE_HOLD"
                else ""
            ),
            "proposed_actions": [
                {
                    "action_type": autonomous_recovery_next_action
                    or (
                        "WAIT_MARKET_CLOSED"
                        if supervisor_mode == "MARKET_CLOSED_WAIT"
                        else "RUNTIME_RETRY"
                        if supervisor_mode == "READY_FOR_OPERATOR_START"
                        else "QUARANTINE_OBSERVE_ONLY"
                    ),
                    "execution_enabled": False,
                }
            ],
            "blockers": [],
            "evidence_summary": {"bounded_recovery_budget": paper_budget or {"budget_exhausted": False}},
        },
    )
    _write_json_file(
        root / "outputs/track_b_execution_core/artifact_retention/latest_artifact_archive_plan.json",
        {
            "classification": artifact_archive_plan_classification,
            "hot_authority_protected_count": artifact_archive_hot_authority_protected_count,
            "active_lifecycle_protected_count": artifact_archive_active_lifecycle_protected_count,
            "warm_diagnostic_count": artifact_archive_warm_diagnostic_count,
            "cold_archive_candidate_count": artifact_archive_cold_archive_candidate_count,
            "blocked_candidate_count": artifact_archive_blocked_candidate_count,
            "estimated_bytes": artifact_archive_estimated_bytes,
            "dry_run_only": True,
            "execution_enabled": False,
            "projection_only": False,
            "not_routing_authority": True,
        },
    )


def _write_json_file(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_lane_bar_authority_db(
    path: Path,
    *,
    symbol: str,
    execution_timeframe: str = "1m",
    context_timeframe: str = "3m",
    observed_completed_bar_end_ts: str | None = None,
    observed_bar_created_at: str | None = None,
    observed_data_source: str = "schwab_live_poll",
    processed_bar_end_ts: str | None = None,
    feature_bar_ts: str | None = None,
    signal_bar_ts: str | None = None,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        if observed_completed_bar_end_ts is not None:
            connection.execute(
                "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-exec-bar",
                    observed_data_source,
                    symbol,
                    symbol,
                    execution_timeframe,
                    observed_completed_bar_end_ts,
                    observed_completed_bar_end_ts,
                    observed_completed_bar_end_ts,
                    "100.0",
                    "101.0",
                    "99.0",
                    "100.5",
                    100,
                    1,
                    0,
                    0,
                    1,
                    1,
                    observed_bar_created_at or observed_completed_bar_end_ts,
                ),
            )
        if feature_bar_ts is not None:
            connection.execute(
                "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-context-bar",
                    "internal",
                    symbol,
                    symbol,
                    context_timeframe,
                    feature_bar_ts,
                    feature_bar_ts,
                    feature_bar_ts,
                    "100.0",
                    "101.0",
                    "99.0",
                    "100.5",
                    100,
                    1,
                    0,
                    0,
                    1,
                    1,
                    feature_bar_ts,
                ),
            )
            connection.execute(
                "insert into features values (?, ?, ?)",
                (
                    f"{symbol}-context-bar",
                    json.dumps({"feature": "value"}),
                    feature_bar_ts,
                ),
            )
        if signal_bar_ts is not None:
            connection.execute(
                "insert into signals values (?, ?, ?)",
                (
                    f"{symbol}-signal-bar",
                    json.dumps({"signal": "BUY"}),
                    signal_bar_ts,
                ),
            )
        if processed_bar_end_ts is not None:
            connection.execute(
                "insert into processed_bars values (?, ?)",
                (
                    f"{symbol}-processed-bar",
                    processed_bar_end_ts,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _write_experimental_canary_snapshot(
    repo_root: Path,
    *,
    enabled: bool = True,
    kill_switch_active: bool = False,
) -> Path:
    canary_root = repo_root / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine"
    lane_dir = canary_root / "lanes" / "atpe_long_medium_high_canary"
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "enabled": enabled,
                "experimental_status": "experimental_canary",
                "generated_at": "2026-03-23T19:45:00-04:00",
                "kill_switch_active": kill_switch_active,
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "latest_atp_state": {
                    "bias_state": "LONG_BIAS",
                    "bias_reasons": ["ema_aligned_up", "close_above_vwap"],
                    "pullback_state": "NORMAL_PULLBACK",
                    "pullback_envelope_state": "STANDARD",
                    "pullback_depth_score": 0.82,
                    "pullback_violence_score": 0.44,
                    "pullback_reason": None,
                    "standard_pullback_envelope": {
                        "min_reset_depth": 0.3,
                        "standard_depth": 0.75,
                        "stretched_depth": 1.05,
                        "disqualify_depth": 1.35,
                    },
                },
                "latest_atp_entry_state": {
                    "family_name": "atp_v1_long_pullback_continuation",
                    "continuation_trigger_state": "CONTINUATION_TRIGGER_CONFIRMED",
                    "entry_state": "ENTRY_ELIGIBLE",
                    "primary_blocker": None,
                    "blocker_codes": [],
                },
                "latest_atp_timing_state": {
                    "timing_state": "ATP_TIMING_CONFIRMED",
                    "vwap_price_quality_state": "VWAP_FAVORABLE",
                    "primary_blocker": None,
                    "blocker_codes": [],
                    "entry_executed": True,
                },
                "paper_only": True,
                "priority_tier": "lower_priority_than_live_strategies",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_count": 3,
                "trade_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (lane_dir / "signals.jsonl").write_text(
        json.dumps(
            {
                "decision": "allowed",
                "experimental_status": "experimental_canary",
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "override_reason": "paper_only_experimental_canary",
                "paper_only": True,
                "quality_bucket": "MEDIUM",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_passed_flag": True,
                "signal_timestamp": "2026-03-23T19:40:00-04:00",
                "symbol": "MES",
            }
        )
        + "\n"
        + json.dumps(
            {
                "decision": "blocked",
                "experimental_status": "experimental_canary",
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "override_reason": "paper_only_experimental_canary",
                "paper_only": True,
                "quality_bucket": "HIGH",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_passed_flag": False,
                "signal_timestamp": "2026-03-23T19:41:00-04:00",
                "symbol": "MNQ",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (lane_dir / "events.jsonl").write_text(
        json.dumps({"event_type": "snapshot_written", "timestamp": "2026-03-23T19:45:00-04:00"}) + "\n",
        encoding="utf-8",
    )
    (canary_root / "experimental_canaries_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-23T19:45:00-04:00",
                "kill_switch": {
                    "active": kill_switch_active,
                    "operator_action": "Toggle the canary kill switch file.",
                    "path": str(canary_root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"),
                },
                "module": "Active Trend Participation Engine",
                "operator_summary_line": "ATPE Long Medium+High Canary ready for dashboard observation.",
                "rows": [
                    {
                        "artifacts": {
                            "events": str((lane_dir / "events.jsonl").resolve()),
                            "operator_status": str((lane_dir / "operator_status.json").resolve()),
                            "signals": str((lane_dir / "signals.jsonl").resolve()),
                        },
                        "experimental_status": "experimental_canary",
                        "lane_id": "atpe_long_medium_high_canary",
                        "lane_name": "ATPE Long Medium+High Canary",
                        "latest_atp_state": {
                            "bias_state": "LONG_BIAS",
                            "pullback_state": "NORMAL_PULLBACK",
                            "pullback_depth_score": 0.82,
                            "pullback_violence_score": 0.44,
                            "pullback_reason": None,
                        },
                        "latest_atp_entry_state": {
                            "entry_state": "ENTRY_ELIGIBLE",
                            "primary_blocker": None,
                            "continuation_trigger_state": "CONTINUATION_TRIGGER_CONFIRMED",
                        },
                        "latest_atp_timing_state": {
                            "timing_state": "ATP_TIMING_CONFIRMED",
                            "vwap_price_quality_state": "VWAP_FAVORABLE",
                            "primary_blocker": None,
                        },
                        "metrics": {
                            "max_drawdown": 42.5,
                            "net_pnl_cash": 18.75,
                            "total_trades": 1,
                        },
                        "operator_summary": {
                            "what_it_is": "Paper-only canary.",
                            "what_it_is_not": "Not a production alpha strategy.",
                        },
                        "paper_only": True,
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "side": "LONG",
                        "symbols": ["MES", "MNQ"],
                        "variant_id": "trend_participation.pullback_continuation.long.conservative",
                    }
                ],
                "scope_label": "Experimental paper canaries for Active Trend Participation Engine",
                "status": "available",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (canary_root / "experimental_canaries_snapshot.md").write_text("# Experimental Canaries\n", encoding="utf-8")
    (canary_root / "operator_summary.md").write_text("# Operator Summary\n", encoding="utf-8")
    return canary_root


def _init_strategy_lane_dashboard_db(
    path: Path,
    *,
    symbol: str,
    entry_reason: str,
    closed_trade_pnl: str | None,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{symbol}-entry",
                f"{symbol}-bar-entry",
                symbol,
                "BUY_TO_OPEN",
                1,
                "2026-03-22T13:30:00-04:00",
                entry_reason,
                f"{symbol}-broker-entry",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{symbol}-entry",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-22T13:35:00-04:00",
                "100.0",
                f"{symbol}-broker-entry",
            ),
        )
        if closed_trade_pnl is not None:
            exit_price = "102.5" if closed_trade_pnl == "25.0" else "100.5"
            connection.execute(
                "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-exit",
                    f"{symbol}-bar-exit",
                    symbol,
                    "SELL_TO_CLOSE",
                    1,
                    "2026-03-22T13:40:00-04:00",
                    entry_reason,
                    f"{symbol}-broker-exit",
                    "FILLED",
                ),
            )
            connection.execute(
                "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-exit",
                    "SELL_TO_CLOSE",
                    "FILLED",
                    "2026-03-22T13:45:00-04:00",
                    exit_price,
                    f"{symbol}-broker-exit",
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _append_strategy_lane_closed_trade(
    path: Path,
    *,
    symbol: str,
    trade_id: str,
    entry_reason: str,
    entry_created_at: str,
    entry_fill_at: str,
    exit_created_at: str,
    exit_fill_at: str,
    entry_price: str,
    exit_price: str,
    entry_bar_id: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-entry",
                entry_bar_id,
                symbol,
                "BUY_TO_OPEN",
                1,
                entry_created_at,
                entry_reason,
                f"{trade_id}-broker-entry",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-entry",
                "BUY_TO_OPEN",
                "FILLED",
                entry_fill_at,
                entry_price,
                f"{trade_id}-broker-entry",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-exit",
                f"{entry_bar_id}-exit",
                symbol,
                "SELL_TO_CLOSE",
                1,
                exit_created_at,
                entry_reason,
                f"{trade_id}-broker-exit",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-exit",
                "SELL_TO_CLOSE",
                "FILLED",
                exit_fill_at,
                exit_price,
                f"{trade_id}-broker-exit",
            ),
        )
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_bar_id,
                "schwab_live_poll",
                symbol,
                symbol,
                "5m",
                entry_created_at,
                entry_created_at,
                entry_fill_at,
                entry_price,
                entry_price,
                entry_price,
                entry_price,
                100,
                1,
                0,
                0,
                1,
                1,
                entry_created_at,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_bar(
    path: Path,
    *,
    bar_id: str,
    symbol: str,
    start_ts: str,
    end_ts: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                bar_id,
                "schwab_live_poll",
                symbol,
                symbol,
                "5m",
                end_ts,
                start_ts,
                end_ts,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                end_ts,
            ),
        )
        connection.execute(
            "insert into processed_bars values (?, ?)",
            (bar_id, end_ts),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_signal(
    path: Path,
    *,
    bar_id: str,
    created_at: str,
    payload: dict[str, object],
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into signals values (?, ?, ?)",
            (
                bar_id,
                json.dumps(payload),
                created_at,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_intent(
    path: Path,
    *,
    order_intent_id: str,
    bar_id: str,
    symbol: str,
    intent_type: str,
    created_at: str,
    reason_code: str,
    broker_order_id: str,
    order_status: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order_intent_id,
                bar_id,
                symbol,
                intent_type,
                1,
                created_at,
                reason_code,
                broker_order_id,
                order_status,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_fill(
    path: Path,
    *,
    order_intent_id: str,
    intent_type: str,
    order_status: str,
    fill_timestamp: str,
    fill_price: str,
    broker_order_id: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                order_intent_id,
                intent_type,
                order_status,
                fill_timestamp,
                fill_price,
                broker_order_id,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _write_jsonl_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def _write_dashboard_local_operator_auth_state(
    repo_root: Path,
    *,
    active: bool,
    auth_available: bool = True,
    touch_id_available: bool = True,
) -> Path:
    path = repo_root / "outputs" / "operator_dashboard" / "local_operator_auth_state.json"
    events_path = repo_root / "outputs" / "operator_dashboard" / "local_operator_auth_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    ttl_seconds = 28800
    authenticated_at = now - timedelta(minutes=5 if active else 600)
    expires_at = authenticated_at + timedelta(seconds=ttl_seconds)
    payload = {
        "auth_available": auth_available,
        "touch_id_available": touch_id_available,
        "auth_method": "TOUCH_ID" if auth_available else "NONE",
        "last_authenticated_at": authenticated_at.isoformat(),
        "last_auth_result": "SUCCESS" if active else ("UNAVAILABLE" if not auth_available or not touch_id_available else "EXPIRED"),
        "last_auth_detail": (
            "Local operator auth session is active for live broker actions."
            if active
            else (
                "Touch ID is unavailable or not enrolled on this Mac."
                if not auth_available or not touch_id_available
                else "Local operator auth session expired and must be renewed before live broker actions."
            )
        ),
        "auth_session_expires_at": expires_at.isoformat(),
        "auth_session_ttl_seconds": ttl_seconds,
        "auth_session_active": active,
        "local_operator_identity": "pilot_operator" if auth_available else None,
        "auth_session_id": "auth-session-1" if active else None,
        "updated_at": now.isoformat(),
        "artifacts": {
            "state_path": str(path),
            "events_path": str(events_path),
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rows: list[dict[str, object]] = []
    if auth_available and active:
        rows.append(
            {
                "event_type": "local_operator_auth_succeeded",
                "occurred_at": authenticated_at.isoformat(),
                "authenticated_at": authenticated_at.isoformat(),
                "auth_method": "TOUCH_ID",
                "local_operator_identity": "pilot_operator",
                "auth_session_id": "auth-session-1",
                "auth_result": "SUCCEEDED",
            }
        )
    _write_jsonl_rows(events_path, rows)
    return path


def _same_underlying_snapshot(
    tmp_path: Path,
    *,
    lanes: list[dict[str, object]],
    production_link_snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    service = _same_underlying_service(
        tmp_path,
        lanes=lanes,
        production_link_snapshot=production_link_snapshot,
    )
    return service.snapshot()


def _same_underlying_service(
    tmp_path: Path,
    *,
    lanes: list[dict[str, object]],
    production_link_snapshot: dict[str, object] | None = None,
) -> OperatorDashboardService:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True, exist_ok=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T13:45:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": lanes,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }
    service._production_link_service.snapshot = lambda: production_link_snapshot or {  # type: ignore[method-assign]
        "portfolio": {"positions": []},
        "orders": {"open_rows": []},
        "reconciliation": {"status": "CLEAR", "label": "CLEAR"},
    }
    return service


class _TestEnum(Enum):
    READY = "ready"


@dataclass
class _TestPayload:
    value: Decimal
    when: datetime


def test_json_ready_normalizes_nested_dashboard_payload_values(tmp_path: Path) -> None:
    payload = {
        "rows": [
            {
                "lane_id": "lane-a",
                "realized_pnl": Decimal("12.34"),
                "as_of": datetime(2026, 3, 22, 12, 34, 56, tzinfo=timezone.utc),
                "artifact": tmp_path / "row.json",
                "status": _TestEnum.READY,
                "detail": _TestPayload(
                    value=Decimal("56.78"),
                    when=datetime(2026, 3, 22, 12, 35, 0, tzinfo=timezone.utc),
                ),
            }
        ],
        "trade_log": (
            {
                "fill_price": Decimal("100.25"),
                "closed_at": datetime(2026, 3, 22, 12, 40, 0, tzinfo=timezone.utc),
            },
        ),
    }

    normalized = _json_ready(payload)

    assert normalized["rows"][0]["realized_pnl"] == "12.34"
    assert normalized["rows"][0]["as_of"] == "2026-03-22T12:34:56+00:00"
    assert normalized["rows"][0]["artifact"] == str(tmp_path / "row.json")
    assert normalized["rows"][0]["status"] == "ready"
    assert normalized["rows"][0]["detail"]["value"] == "56.78"
    assert normalized["trade_log"][0]["fill_price"] == "100.25"
    json.dumps(normalized, sort_keys=True)


def test_json_ready_replaces_non_finite_numbers_with_none() -> None:
    payload = {
        "profit_factor": float("inf"),
        "max_drawdown": float("nan"),
        "detail": {
            "decimal_inf": Decimal("Infinity"),
            "decimal_nan": Decimal("NaN"),
        },
    }

    normalized = _json_ready(payload)

    assert normalized == {
        "profit_factor": None,
        "max_drawdown": None,
        "detail": {
            "decimal_inf": None,
            "decimal_nan": None,
        },
    }
    json.dumps(normalized, sort_keys=True)


def test_same_underlying_conflicts_treat_multiple_runtime_instances_as_informational_only(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull.sqlite3"
    gc_bear_db = tmp_path / "gc_bear.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    conflicts = snapshot["same_underlying_conflicts"]["rows"]
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict["instrument"] == "GC"
    assert conflict["conflict_kind"] == "multiple_runtime_instances_same_instrument"
    assert conflict["severity"] == "INFO"
    assert conflict["operator_action_required"] is False
    assert conflict["execution_risk"] is False
    assert conflict["observational_only"] is True
    assert conflict["overlap_scope"] == "STRATEGY_ONLY"

    audit_rows = [row for row in snapshot["paper"]["signal_intent_fill_audit"]["rows"] if row["instrument"] == "GC"]
    assert len(audit_rows) == 2
    assert all(row["same_underlying_conflict_present"] is True for row in audit_rows)
    assert all(row["same_underlying_conflict_severity"] == "INFO" for row in audit_rows)


def test_same_underlying_conflicts_detect_pending_order_overlap_as_blocking(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_pending.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_pending.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    _append_dashboard_bar(
        gc_bull_db,
        bar_id="gc-bull-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )
    _append_dashboard_bar(
        gc_bear_db,
        bar_id="gc-bear-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:35:00-04:00",
        end_ts="2026-03-23T09:40:00-04:00",
    )
    _append_dashboard_intent(
        gc_bull_db,
        order_intent_id="gc-bull-intent-1",
        bar_id="gc-bull-bar-1",
        symbol="GC",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:35:05-04:00",
        reason_code="bullSnap",
        broker_order_id="gc-bull-broker-1",
        order_status="WORKING",
    )
    _append_dashboard_intent(
        gc_bear_db,
        order_intent_id="gc-bear-intent-1",
        bar_id="gc-bear-bar-1",
        symbol="GC",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T09:40:05-04:00",
        reason_code="bearSnap",
        broker_order_id="gc-bear-broker-1",
        order_status="WORKING",
    )

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "multiple_pending_orders_same_instrument"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["operator_action_required"] is True
    assert conflict["execution_risk"] is True
    assert conflict["observational_only"] is False
    assert conflict["pending_order_overlap_present"] is True
    assert conflict["broker_overlap_present"] is False


def test_same_underlying_conflicts_detect_opposite_side_in_position_overlap_as_blocking(tmp_path: Path) -> None:
    gc_long_db = tmp_path / "gc_long.sqlite3"
    gc_short_db = tmp_path / "gc_short.sqlite3"
    _init_empty_dashboard_db(gc_long_db)
    _init_empty_dashboard_db(gc_short_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_long_lane",
                "display_name": "GC Long",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_db}",
            },
            {
                "lane_id": "gc_short_lane",
                "display_name": "GC Short",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "SHORT",
                "strategy_status": "IN_SHORT_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_short_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "opposite_side_in_position_overlap"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["in_position_overlap_present"] is True
    assert conflict["operator_action_required"] is True
    assert conflict["execution_risk"] is True
    assert conflict["position_side_profile"] == "BOTH"


def test_same_underlying_conflicts_treat_same_side_in_position_overlap_as_actionable_warning(tmp_path: Path) -> None:
    gc_long_a_db = tmp_path / "gc_long_a.sqlite3"
    gc_long_b_db = tmp_path / "gc_long_b.sqlite3"
    _init_empty_dashboard_db(gc_long_a_db)
    _init_empty_dashboard_db(gc_long_b_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_long_a_lane",
                "display_name": "GC Long A",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapA"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_a_db}",
            },
            {
                "lane_id": "gc_long_b_lane",
                "display_name": "GC Long B",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapB"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_b_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "same_side_in_position_overlap"
    assert conflict["severity"] == "ACTION"
    assert conflict["in_position_overlap_present"] is True
    assert conflict["operator_action_required"] is False
    assert conflict["execution_risk"] is False
    assert conflict["observational_only"] is False
    assert conflict["position_side_profile"] == "LONG_ONLY"


def test_same_underlying_conflicts_detect_broker_runtime_overlap(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_broker.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_broker.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
        production_link_snapshot={
            "portfolio": {"positions": [{"symbol": "GC", "quantity": "1"}]},
            "orders": {"open_rows": [{"symbol": "GC", "broker_order_id": "gc-live-1"}]},
            "reconciliation": {"status": "DRIFTED", "label": "DRIFTED"},
        },
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "broker_vs_strategy_overlap_mismatch"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["broker_overlap_present"] is True
    assert conflict["overlap_scope"] == "BROKER_AND_STRATEGY"
    assert conflict["reconciliation_state"] == "DRIFTED"
    assert conflict["reconciliation_clear"] is False


def test_same_underlying_conflicts_ignore_different_instruments(tmp_path: Path) -> None:
    gc_db = tmp_path / "gc.sqlite3"
    cl_db = tmp_path / "cl.sqlite3"
    _init_empty_dashboard_db(gc_db)
    _init_empty_dashboard_db(cl_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_db}",
            },
            {
                "lane_id": "cl_lane",
                "display_name": "CL Bear",
                "symbol": "CL",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{cl_db}",
            },
        ],
    )

    assert snapshot["same_underlying_conflicts"]["summary"]["conflict_count"] == 0
    assert snapshot["same_underlying_conflicts"]["rows"] == []


def test_same_underlying_snapshot_exposes_production_link_pilot_artifacts(tmp_path: Path) -> None:
    gc_db = tmp_path / "gc.sqlite3"
    _init_empty_dashboard_db(gc_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_db}",
            }
        ],
        production_link_snapshot={
            "status": "ready",
            "label": "CONNECTED",
            "portfolio": {"positions": []},
            "orders": {"open_rows": []},
            "reconciliation": {"status": "CLEAR", "label": "CLEAR"},
        },
    )

    assert snapshot["production_link"]["artifacts"]["snapshot"] == "/api/operator-artifact/production-link-snapshot"
    assert snapshot["production_link"]["artifacts"]["pilot_status"] == "/api/operator-artifact/production-link-pilot-status"
    assert snapshot["production_link"]["artifacts"]["futures_pilot_policy"] == "/api/operator-artifact/production-link-futures-pilot-policy"
    assert snapshot["production_link"]["artifacts"]["futures_pilot_status"] == "/api/operator-artifact/production-link-futures-pilot-status"


def test_operator_artifact_file_exposes_futures_pilot_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(
        service,
        "_runtime_paths",
        lambda runtime_name: {
            "artifacts_dir": tmp_path / "outputs" / runtime_name,
            "pid_file": tmp_path / "outputs" / runtime_name / "runtime.pid",
            "log_file": tmp_path / "outputs" / runtime_name / "runtime.log",
            "db_path": tmp_path / "outputs" / runtime_name / "runtime.sqlite3",
            "operator_control_path": tmp_path / "outputs" / runtime_name / "control.jsonl",
        },
    )

    futures_policy_path = service._production_link_service.config.snapshot_path.with_name("futures_pilot_policy_snapshot.json")
    futures_status_path = service._production_link_service.config.snapshot_path.with_name("futures_pilot_status.json")

    assert service.operator_artifact_file("production-link-futures-pilot-policy")[0] == futures_policy_path
    assert service.operator_artifact_file("production-link-futures-pilot-status")[0] == futures_status_path


def test_run_production_action_blocks_without_active_local_operator_auth(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=False)
    run_action_called = False

    def _unexpected_run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        nonlocal run_action_called
        run_action_called = True
        return {"ok": True, "message": "unexpected"}

    service._production_link_service.run_action = _unexpected_run_action  # type: ignore[method-assign]
    service._production_link_service.snapshot = lambda: {"status": "ready", "label": "CONNECTED"}  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("submit-order", {"symbol": "ABBV"})

    assert result["ok"] is False
    assert "Production live action blocked because" in result["message"]
    assert result["auth"]["auth_session_active"] is False
    assert result["auth"]["entry_allowed"] is False
    assert result["auth"]["flatten_allowed"] is True
    assert result["auth"]["cancel_allowed"] is True
    assert "expired" in result["auth"]["detail"].lower()
    assert run_action_called is False


def test_run_production_action_uses_current_local_operator_auth_for_live_submit(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)
    captured: dict[str, object] = {}

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {
            "ok": True,
            "message": "submitted",
            "production_link": {"status": "ready", "label": "CONNECTED"},
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("submit-order", {"symbol": "ABBV"})

    assert result["ok"] is True
    assert captured["action"] == "submit-order"
    submitted_payload = captured["payload"]
    assert submitted_payload["symbol"] == "ABBV"
    assert submitted_payload["operator_authenticated"] is True
    assert submitted_payload["local_operator_identity"] == "pilot_operator"
    assert submitted_payload["auth_method"] == "TOUCH_ID"
    assert str(submitted_payload["authenticated_at"]).endswith("+00:00")
    assert submitted_payload["auth_session_id"] == "auth-session-1"
    assert submitted_payload["operator_label"] == "pilot_operator"
    assert result["auth"]["next_action_label"] == "Ready"
    assert result["auth"]["time_remaining_seconds"] > 0
    assert result["auth"]["entry_allowed"] is True
    assert result["auth"]["flatten_allowed"] is True
    assert result["auth"]["cancel_allowed"] is True


def test_run_production_action_surfaces_broker_http_error_in_result_payload(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)

    def _raise_broker_error(action: str, payload: dict[str, object]) -> dict[str, object]:
        raise operator_dashboard_module.SchwabBrokerHttpError(
            "Schwab trader HTTP error 400 for POST /accounts/hash-123/orders: Invalid request data"
        )

    service._production_link_service.run_action = _raise_broker_error  # type: ignore[method-assign]
    service._production_link_service.snapshot = lambda: {"status": "ready", "label": "CONNECTED"}  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("submit-order", {"symbol": "MGC", "asset_class": "FUTURE"})

    assert result["ok"] is False
    assert result["message"] == "Production-link action submit-order failed."
    assert "Invalid request data" in result["output"]
    assert result["production_link"]["status"] == "ready"
    assert result["auth"]["entry_allowed"] is True


def test_run_production_action_uses_current_local_operator_auth_for_preview_when_available(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)
    captured: dict[str, object] = {}

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {
            "ok": True,
            "message": "previewed",
            "production_link": {"status": "ready", "label": "CONNECTED"},
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("preview-order", {"symbol": "MGC", "asset_class": "FUTURE"})

    assert result["ok"] is True
    assert captured["action"] == "preview-order"
    preview_payload = captured["payload"]
    assert preview_payload["symbol"] == "MGC"
    assert preview_payload["asset_class"] == "FUTURE"
    assert preview_payload["operator_authenticated"] is True
    assert preview_payload["local_operator_identity"] == "pilot_operator"
    assert preview_payload["auth_method"] == "TOUCH_ID"
    assert str(preview_payload["authenticated_at"]).endswith("+00:00")
    assert preview_payload["auth_session_id"] == "auth-session-1"
    assert preview_payload["operator_label"] == "pilot_operator"
    assert result["auth"]["next_action_label"] == "Ready"
    assert result["auth"]["time_remaining_seconds"] > 0


def test_run_production_action_logs_decimal_bearing_production_link_payloads(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        return {
            "ok": True,
            "message": "previewed",
            "output": "previewed",
            "production_link": {
                "status": "ready",
                "label": "CONNECTED",
                "quotes": [{"last_price": Decimal("2500.25")}],
            },
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("preview-order", {"symbol": "MGC", "asset_class": "FUTURE"})

    assert result["ok"] is True
    action_rows = [
        json.loads(line)
        for line in service._action_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert action_rows
    assert action_rows[-1]["production_link"]["quotes"][0]["last_price"] == "2500.25"


def test_run_production_action_uses_current_local_operator_auth_for_flatten_when_available(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)
    captured: dict[str, object] = {}

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {
            "ok": True,
            "message": "flattened",
            "production_link": {"status": "ready", "label": "CONNECTED"},
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("flatten-position", {"symbol": "ABBV"})

    assert result["ok"] is True
    assert captured["action"] == "flatten-position"
    flatten_payload = captured["payload"]
    assert flatten_payload["symbol"] == "ABBV"
    assert flatten_payload["operator_authenticated"] is True
    assert flatten_payload["local_operator_identity"] == "pilot_operator"
    assert flatten_payload["auth_method"] == "TOUCH_ID"
    assert flatten_payload["auth_session_id"] == "auth-session-1"
    assert flatten_payload["operator_label"] == "pilot_operator"
    assert result["auth"]["next_action_label"] == "Ready"
    assert result["auth"]["time_remaining_seconds"] > 0


def test_run_production_action_allows_reduce_only_flatten_with_expired_session(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=False)
    captured: dict[str, object] = {}

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {
            "ok": True,
            "message": "flatten submitted",
            "production_link": {"status": "ready", "label": "CONNECTED"},
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action(
        "flatten-position",
        {"symbol": "ABBV", "account_hash": "hash-123", "asset_class": "STOCK", "quantity": "1", "side": "LONG"},
    )

    assert result["ok"] is True
    assert captured["action"] == "flatten-position"
    flatten_payload = captured["payload"]
    assert flatten_payload["operator_authenticated"] is False
    assert flatten_payload["operator_reduce_only_authorized"] is True
    assert flatten_payload["operator_auth_policy"] == "REDUCE_ONLY_POLICY"
    assert flatten_payload["operator_auth_risk_bucket"] == "REDUCE_RISK"
    assert result["auth"]["authorized_for_action"] is True
    assert result["auth"]["authorization_policy"] == "REDUCE_ONLY_POLICY"
    assert result["auth"]["entry_allowed"] is False
    assert result["auth"]["flatten_allowed"] is True


def test_run_production_action_allows_reduce_only_cancel_with_expired_session(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=False)
    captured: dict[str, object] = {}

    def _run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        captured["action"] = action
        captured["payload"] = dict(payload)
        return {
            "ok": True,
            "message": "cancel submitted",
            "production_link": {"status": "ready", "label": "CONNECTED"},
        }

    service._production_link_service.run_action = _run_action  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("cancel-order", {"account_hash": "hash-123", "broker_order_id": "broker-1"})

    assert result["ok"] is True
    assert captured["action"] == "cancel-order"
    cancel_payload = captured["payload"]
    assert cancel_payload["operator_authenticated"] is False
    assert cancel_payload["operator_reduce_only_authorized"] is True
    assert cancel_payload["operator_auth_policy"] == "REDUCE_ONLY_POLICY"
    assert cancel_payload["operator_auth_risk_bucket"] == "REDUCE_RISK"
    assert result["auth"]["authorized_for_action"] is True
    assert result["auth"]["authorization_policy"] == "REDUCE_ONLY_POLICY"
    assert result["auth"]["cancel_allowed"] is True


def test_run_production_action_blocks_new_risk_when_touch_id_is_unavailable(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=False, auth_available=False, touch_id_available=False)
    run_action_called = False

    def _unexpected_run_action(action: str, payload: dict[str, object]) -> dict[str, object]:
        nonlocal run_action_called
        run_action_called = True
        return {"ok": True, "message": "unexpected"}

    service._production_link_service.run_action = _unexpected_run_action  # type: ignore[method-assign]
    service._production_link_service.snapshot = lambda: {"status": "ready", "label": "CONNECTED"}  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("submit-order", {"symbol": "ABBV"})

    assert result["ok"] is False
    assert "Production live action blocked because" in result["message"]
    assert result["auth"]["entry_allowed"] is False
    assert result["auth"]["flatten_allowed"] is True
    assert result["auth"]["cancel_allowed"] is True
    assert "unavailable" in str(result["auth"]["detail"]).lower()
    assert run_action_called is False


def test_run_production_action_prime_local_auth_reuses_existing_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=True)

    def _unexpected_run(*args, **kwargs):
        raise AssertionError("prime-local-auth should not spawn the helper when the session is already active")

    monkeypatch.setattr(operator_dashboard_module.subprocess, "run", _unexpected_run)
    service._production_link_service.snapshot = lambda: {"status": "ready", "label": "CONNECTED"}  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("prime-local-auth", {})

    assert result["ok"] is True
    assert result["message"] == "Local operator auth session already active."
    assert result["auth"]["operator_authenticated"] is True
    assert result["auth"]["next_action_label"] == "Ready"
    assert result["auth"]["time_remaining_seconds"] > 0


def test_run_production_action_prime_local_auth_front_loads_touch_id_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_dashboard_local_operator_auth_state(tmp_path, active=False)
    executed: dict[str, object] = {}

    def _fake_run(command, cwd, text, capture_output, check):
        executed["command"] = command
        executed["cwd"] = cwd
        _write_dashboard_local_operator_auth_state(tmp_path, active=True)
        return subprocess.CompletedProcess(command, 0, stdout="Touch ID authenticated", stderr="")

    monkeypatch.setattr(operator_dashboard_module.subprocess, "run", _fake_run)
    service._production_link_service.snapshot = lambda: {"status": "ready", "label": "CONNECTED"}  # type: ignore[method-assign]
    service.snapshot = lambda: {"production_link": {"status": "ready"}}  # type: ignore[method-assign]

    result = service.run_production_action("prime-local-auth", {})

    assert executed["command"] == ["bash", "scripts/run_local_operator_auth.sh"]
    assert executed["cwd"] == tmp_path
    assert result["ok"] is True
    assert result["message"] == "Local operator auth session primed for the live-pilot workflow."
    assert result["auth"]["operator_authenticated"] is True
    assert result["auth"]["next_action_label"] == "Ready"
    assert result["auth"]["time_remaining_seconds"] > 0


def test_same_underlying_conflict_acknowledgement_persists_by_instrument(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_ack.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_ack.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-acknowledge",
        {"instrument": "GC", "operator_label": "desk-op", "note": "Reviewed pre-open overlap."},
    )

    assert result["ok"] is True
    snapshot = result["snapshot"]
    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["acknowledged"] is True
    assert conflict["acknowledged_by"] == "desk-op"
    assert conflict["acknowledgement_note"] == "Reviewed pre-open overlap."
    assert conflict["review_state_status"] == "ACKNOWLEDGED"

    persisted = json.loads(
        (tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_state.json").read_text(encoding="utf-8")
    )
    assert persisted["records"]["GC"]["acknowledged"] is True
    assert persisted["records"]["GC"]["acknowledged_by"] == "desk-op"


def test_same_underlying_conflict_actions_persist_local_auth_metadata(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_auth.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_auth.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-acknowledge",
        {
            "instrument": "GC",
            "operator_label": "manual operator",
            "requested_operator_label": "desk-op-note",
            "local_operator_identity": "local_touch_id_operator",
            "auth_method": "TOUCH_ID",
            "authenticated_at": "2026-03-23T15:10:00+00:00",
            "auth_session_id": "auth-session-1",
            "note": "Reviewed with local auth.",
        },
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["acknowledged_by"] == "local_touch_id_operator"
    assert conflict["last_local_operator_identity"] == "local_touch_id_operator"
    assert conflict["last_auth_method"] == "TOUCH_ID"
    assert conflict["last_authenticated_at"] == "2026-03-23T15:10:00+00:00"
    assert conflict["last_auth_session_id"] == "auth-session-1"
    assert conflict["last_operator_authenticated"] is True
    assert conflict["last_requested_operator_label"] == "desk-op-note"

    latest_event = result["snapshot"]["same_underlying_conflicts"]["events"]["latest_event"]
    assert latest_event["event_type"] == "conflict_acknowledged"
    assert latest_event["local_operator_identity"] == "local_touch_id_operator"
    assert latest_event["auth_method"] == "TOUCH_ID"
    assert latest_event["authenticated_at"] == "2026-03-23T15:10:00+00:00"
    assert latest_event["auth_session_id"] == "auth-session-1"
    assert latest_event["operator_authenticated"] is True
    assert latest_event["requested_operator_label"] == "desk-op-note"

    history_rows = (
        tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_history.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    latest_history = json.loads(history_rows[-1])
    assert latest_history["local_operator_identity"] == "local_touch_id_operator"
    assert latest_history["auth_method"] == "TOUCH_ID"
    assert latest_history["authenticated_at"] == "2026-03-23T15:10:00+00:00"


def test_same_underlying_conflict_hold_updates_payloads_and_blocks_state(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_hold.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_hold.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-hold-entries",
        {"instrument": "GC", "operator_label": "desk-op", "reason": "Do not open fresh GC overlap until reviewed."},
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["hold_new_entries"] is True
    assert conflict["entry_hold_effective"] is True
    assert conflict["hold_reason"] == "Do not open fresh GC overlap until reviewed."
    assert conflict["review_state_status"] == "HOLDING"
    audit_rows = [row for row in result["snapshot"]["paper"]["signal_intent_fill_audit"]["rows"] if row["instrument"] == "GC"]
    assert all(row["same_underlying_hold_new_entries"] is True for row in audit_rows)
    assert all(row["same_underlying_entry_block_effective"] is True for row in audit_rows)
    events = result["snapshot"]["same_underlying_conflicts"]["events"]["rows"]
    assert events[0]["event_type"] == "conflict_hold_set"


def test_same_underlying_conflict_hold_expiry_is_enforced_and_preserved(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_expire.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_expire.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    service.run_action(
        "same-underlying-hold-entries",
        {
            "instrument": "GC",
            "operator_label": "desk-op",
            "reason": "Temporary hold through review window.",
            "hold_expires_at": "2026-03-20T12:00:00+00:00",
        },
    )
    snapshot = service.snapshot()

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["hold_new_entries"] is False
    assert conflict["hold_expired"] is True
    assert conflict["hold_expiry_enforced"] is True
    assert conflict["entry_hold_effective"] is False
    assert conflict["review_state_status"] == "HOLD_EXPIRED"
    assert "expired" in str(conflict["hold_state_reason"]).lower()
    assert snapshot["same_underlying_conflicts"]["summary"]["hold_expired_count"] == 1
    assert snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_hold_expired"

    persisted = json.loads(
        (tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_state.json").read_text(encoding="utf-8")
    )
    assert persisted["records"]["GC"]["hold_expired"] is True


def test_same_underlying_conflict_material_change_auto_reopens_review(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_reopen.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_reopen.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})

    _append_dashboard_intent(
        gc_bull_db,
        order_intent_id="gc-bull-intent-overlap",
        bar_id="gc-bull-bar-overlap",
        symbol="GC",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T10:05:00-04:00",
        reason_code="bullSnap",
        broker_order_id="gc-bull-broker-overlap",
        order_status="WORKING",
    )
    _append_dashboard_intent(
        gc_bear_db,
        order_intent_id="gc-bear-intent-overlap",
        bar_id="gc-bear-bar-overlap",
        symbol="GC",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T10:06:00-04:00",
        reason_code="bearSnap",
        broker_order_id="gc-bear-broker-overlap",
        order_status="WORKING",
    )

    snapshot = service.snapshot()
    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["review_state_status"] == "STALE"
    assert conflict["auto_reopen_required"] is True
    assert "pending-order overlap" in str(conflict["reopened_reason"])
    assert snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_auto_reopened"


def test_same_underlying_conflict_strategy_identity_churn_stays_acknowledged(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_identity.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_identity.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    baseline_snapshot = service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})["snapshot"]
    baseline_ids = list(baseline_snapshot["same_underlying_conflicts"]["rows"][0]["standalone_strategy_ids"])
    for path in (tmp_path / "shadow.sqlite3", tmp_path / "paper.sqlite3"):
        if path.exists():
            path.unlink()

    churned_snapshot = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapRefined"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    ).snapshot()

    conflict = churned_snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["standalone_strategy_ids"] != baseline_ids
    assert conflict["review_state_status"] == "ACKNOWLEDGED"
    assert conflict["auto_reopen_required"] is False
    assert conflict["reopened_reason"] is None
    assert churned_snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_acknowledged"


def test_same_underlying_conflict_kind_churn_without_exposure_stays_acknowledged(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_kind.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_kind.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})
    for path in (tmp_path / "shadow.sqlite3", tmp_path / "paper.sqlite3"):
        if path.exists():
            path.unlink()

    changed_snapshot = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    ).snapshot()

    conflict = changed_snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "multiple_eligible_same_instrument"
    assert conflict["severity"] == "INFO"
    assert conflict["review_state_status"] == "ACKNOWLEDGED"
    assert conflict["auto_reopen_required"] is False
    assert conflict["reopened_reason"] is None
    assert changed_snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_acknowledged"


def test_same_underlying_conflict_observational_override_is_surfaced(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_override.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_override.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-mark-observational",
        {"instrument": "GC", "operator_label": "desk-op", "override_reason": "Shared GC watch only; no exposure yet."},
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["override_observational_only"] is True
    assert conflict["override_reason"] == "Shared GC watch only; no exposure yet."
    assert conflict["review_state_status"] == "OVERRIDDEN"
    assert result["snapshot"]["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_marked_observational_only"


def test_same_underlying_conflict_runtime_entry_block_event_is_surfaced(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_block.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_block.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    paper_controls_path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_controls.jsonl"
    paper_controls_path.write_text(
        json.dumps(
            {
                "event_type": "entry_blocked_by_same_underlying_hold",
                "action": "same_underlying_entry_hold_blocked",
                "occurred_at": "2026-03-23T15:05:00+00:00",
                "instrument": "GC",
                "standalone_strategy_id": "gc_bull_lane__GC",
                "blocked_standalone_strategy_id": "gc_bull_lane__GC",
                "blocked_reason": "New entries held by operator for same-underlying conflict review on GC.",
                "entry_hold_effective": True,
                "review_state_status": "HOLDING",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot = service.snapshot()

    latest_block = snapshot["same_underlying_conflicts"]["events"]["latest_entry_blocked_event"]
    assert latest_block["event_type"] == "entry_blocked_by_same_underlying_hold"
    assert latest_block["blocked_standalone_strategy_id"] == "gc_bull_lane__GC"


def test_same_underlying_conflict_review_state_not_created_without_conflict(tmp_path: Path) -> None:
    gc_db = tmp_path / "gc_only.sqlite3"
    _init_empty_dashboard_db(gc_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_lane",
                "display_name": "GC Only",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_db}",
            }
        ],
    )

    with pytest.raises(ValueError):
        service.run_action("same-underlying-acknowledge", {"instrument": "GC"})


def test_dashboard_health_payload_reports_ready(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-03-21T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._record_dashboard_probe(  # type: ignore[attr-defined]
        snapshot={
            "generated_at": "2026-03-21T12:00:05+00:00",
            "operator_surface": {"readiness": {"title": "Runtime / Readiness"}},
        },
        error=None,
    )
    with service._dashboard_probe_lock:  # type: ignore[attr-defined]
        service._dashboard_probe.update(  # type: ignore[attr-defined]
            {
                "state": "ready",
                "ready": True,
                "stable_ready": True,
                "stable_ready_since": "2026-03-21T12:00:05+00:00",
                "consecutive_ready_samples": 2,
            }
        )

    payload = service.health_payload()

    assert payload["status"] == "ok"
    assert payload["ready"] is True
    assert payload["build_stamp"] == service._build_stamp
    assert payload["pid"] == 12345
    assert payload["checks"]["operator_surface_loadable"]["ok"] is True
    assert payload["checks"]["api_dashboard_responding"]["ok"] is True
    assert payload["endpoints"]["dashboard"] == "/api/dashboard"


def test_dashboard_health_payload_reports_degraded_when_snapshot_fails(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("snapshot failed"))  # type: ignore[attr-defined]

    payload = service.health_payload()

    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert "snapshot failed" in payload["error"]
    assert payload["checks"]["operator_surface_loadable"]["ok"] is False
    assert payload["checks"]["api_dashboard_responding"]["ok"] is False


def test_research_daily_capture_payload_surfaces_latest_run_and_symbol_failures(tmp_path: Path) -> None:
    repo_root = tmp_path
    latest_dir = repo_root / "outputs" / "research" / "daily_capture"
    latest_dir.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    capture_started_at = (now_utc.replace(second=0) if now_utc.second else now_utc).isoformat()
    capture_completed_at = now_utc.isoformat()
    last_bar_end_ts = now_utc.isoformat()
    (latest_dir / "latest.json").write_text(
        json.dumps(
            {
                    "status": "partial_failure",
                    "capture_started_at": capture_started_at,
                    "capture_completed_at": capture_completed_at,
                "attempted_symbols": ["MGC", "MES"],
                "succeeded_symbols": ["MGC"],
                "failed_symbols": [
                    {
                        "symbol": "MES",
                        "capture_class": "watched",
                        "timeframe": "5m",
                        "failure_code": "ValueError",
                        "failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    }
                ],
                "target_rows": [
                        {
                            "symbol": "MGC",
                                "capture_class": "research_universe",
                                "timeframe": "5m",
                                "status": "success",
                                "last_captured_bar_end_ts": last_bar_end_ts,
                            "failure_code": None,
                            "failure_detail": None,
                        },
                    {
                        "symbol": "MES",
                        "capture_class": "watched",
                        "timeframe": "5m",
                        "status": "failure",
                        "last_captured_bar_end_ts": None,
                        "failure_code": "ValueError",
                        "failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    },
                ],
                "target_count": 2,
                "success_count": 1,
                "failure_count": 1,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    engine = build_engine(f"sqlite:///{repo_root / 'mgc_v05l.replay.sqlite3'}")
    create_schema(engine)
    with engine.begin() as connection:
        connection.execute(
            research_capture_status_table.insert(),
            [
                {
                    "symbol": "MGC",
                    "timeframe": "5m",
                    "capture_class": "research_universe",
                    "data_source": "schwab_history",
                    "last_attempted_at": capture_completed_at,
                    "last_succeeded_at": capture_completed_at,
                    "last_bar_end_ts": last_bar_end_ts,
                    "last_status": "success",
                    "last_failure_code": None,
                    "last_failure_detail": None,
                    "last_capture_run_id": 1,
                },
                {
                    "symbol": "MES",
                    "timeframe": "5m",
                    "capture_class": "watched",
                    "data_source": "schwab_history",
                    "last_attempted_at": capture_completed_at,
                    "last_succeeded_at": None,
                    "last_bar_end_ts": None,
                    "last_status": "failure",
                    "last_failure_code": "ValueError",
                    "last_failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    "last_capture_run_id": 2,
                },
            ],
        )

    service = OperatorDashboardService(repo_root)

    payload = service._research_daily_capture_payload(generated_at=now_utc.isoformat())  # type: ignore[attr-defined]

    assert payload["run_status"] == "partial_failure"
    assert payload["freshness_state"] == "current"
    assert payload["attempted_symbols"] == ["MGC", "MES"]
    assert payload["succeeded_symbols"] == ["MGC"]
    assert payload["failed_symbols"][0]["symbol"] == "MES"
    assert {row["symbol"] for row in payload["status_rows"]} == {"MGC", "MES"}


def test_dashboard_bind_reports_conflicting_listener(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = 8790
    service = OperatorDashboardService(tmp_path)
    handler = _build_handler(service)

    def _raise_bind_error(*args: object, **kwargs: object) -> object:
        raise OSError("Address already in use")

    monkeypatch.setattr(operator_dashboard_module, "DashboardHTTPServer", _raise_bind_error)
    monkeypatch.setattr(
        operator_dashboard_module,
        "_listening_process_details",
        lambda requested_port: {"pid": "4242", "command": "python", "listener": f"TCP 127.0.0.1:{requested_port}"},
    )

    with pytest.raises(OSError) as excinfo:
        _bind_dashboard_server("127.0.0.1", port, handler, allow_port_fallback=False)

    message = str(excinfo.value)
    assert f"127.0.0.1:{port}" in message
    assert "already in use" in message
    assert "PID" in message


def test_dashboard_bind_reports_permission_denied_truthfully(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    handler = _build_handler(service)

    def _raise_permission_error(*args: object, **kwargs: object) -> object:
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(operator_dashboard_module, "DashboardHTTPServer", _raise_permission_error)
    monkeypatch.setattr(operator_dashboard_module, "_listening_process_details", lambda _requested_port: None)

    with pytest.raises(OSError) as excinfo:
        _bind_dashboard_server("127.0.0.1", 8790, handler, allow_port_fallback=False)

    assert "permission was denied" in str(excinfo.value)


def test_api_dashboard_serves_degraded_cached_snapshot_before_inline_live_generation(tmp_path: Path) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
            {
                "payload_version": 2,
                "generated_at": generated_at,
                "dashboard_meta": {"server_instance_id": "instance-stale"},
                "operator_surface": {"ok": True},
                "startup_control_plane": {"overall_state": "READY", "counts": {"ready": 1}},
                "supervised_paper_operability": {"app_usable_for_supervised_paper": True},
            }
        ),
        encoding="utf-8",
    )
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("api dashboard still warming"))  # noqa: SLF001

    def _unexpected_live_snapshot() -> dict[str, object]:
        raise AssertionError("inline regeneration must not run on the /api/dashboard hot path")

    service.dashboard_snapshot = _unexpected_live_snapshot  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert payload["dashboard_payload_mode"] == "degraded"
    assert payload["cold_snapshot_skipped_for_latency"] is True
    assert payload["dashboard_meta"]["snapshot_instance_stale"] is True


def test_api_dashboard_returns_degraded_cache_instead_of_inline_regeneration(tmp_path: Path) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
            {
                "payload_version": 2,
                "generated_at": generated_at,
                "dashboard_meta": {"server_instance_id": "instance-stale"},
                "operator_surface": {"ok": True},
                "startup_control_plane": {"overall_state": "READY", "counts": {"ready": 1}},
                "supervised_paper_operability": {"app_usable_for_supervised_paper": True},
            }
        ),
        encoding="utf-8",
    )
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("api dashboard still warming"))  # noqa: SLF001

    service.dashboard_snapshot = lambda: (_ for _ in ()).throw(AssertionError("unexpected cold snapshot"))  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert payload["dashboard_payload_mode"] == "degraded"
    assert payload["cold_snapshot_skipped_for_latency"] is True
    assert payload["dashboard_meta"]["snapshot_instance_stale"] is True


def test_api_dashboard_degraded_cache_does_not_require_inline_regeneration(tmp_path: Path) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
            {
                "payload_version": 2,
                "generated_at": generated_at,
                "dashboard_meta": {"server_instance_id": "instance-stale"},
                "operator_surface": {"ok": True},
                "startup_control_plane": {"overall_state": "READY", "counts": {"ready": 1}},
                "supervised_paper_operability": {"app_usable_for_supervised_paper": True},
            }
        ),
        encoding="utf-8",
    )
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("api dashboard still warming"))  # noqa: SLF001

    service.dashboard_snapshot = lambda: (_ for _ in ()).throw(AssertionError("unexpected cold snapshot"))  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert payload["dashboard_payload_mode"] == "degraded"
    assert payload["cold_snapshot_skipped_for_latency"] is True
    assert payload["dashboard_meta"]["snapshot_instance_stale"] is True


def test_api_dashboard_returns_minimal_degraded_payload_without_cache_or_cold_snapshot(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    (service._action_log_path.parent).mkdir(parents=True, exist_ok=True)  # noqa: SLF001
    service._action_log_path.write_text("x" * 1_000_000, encoding="utf-8")  # noqa: SLF001
    service.dashboard_snapshot = lambda: (_ for _ in ()).throw(AssertionError("unexpected cold snapshot"))  # type: ignore[method-assign]
    service._write_desktop_dashboard_cache_mirror = lambda _payload: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("mirror write must not run on the /api/dashboard hot path")
    )
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert payload["dashboard_payload_mode"] == "degraded"
    assert payload["cold_snapshot_skipped_for_latency"] is True
    assert payload["action_log"] == []
    assert payload["track_b_operator_status"]["track_b_live_feed_freshness_diagnostic"]["available"] is False


def test_operator_action_log_below_threshold_does_not_rotate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_MAX_BYTES", "1000")
    service = OperatorDashboardService(tmp_path)
    service._action_log_path.write_text('{"existing":true}\n', encoding="utf-8")  # noqa: SLF001

    service._log_action({"action": "unit-test", "ok": True})  # noqa: SLF001

    assert service._action_log_path.exists()  # noqa: SLF001
    assert len(service._rotated_action_log_paths()) == 0  # noqa: SLF001
    rows = operator_dashboard_module._tail_jsonl(service._action_log_path, 5)  # noqa: SLF001
    assert rows[-1]["action"] == "unit-test"


def test_operator_action_log_rotates_and_compresses_above_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_MAX_BYTES", "20")
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_COMPRESS_ROTATED", "true")
    service = OperatorDashboardService(tmp_path)
    service._action_log_path.write_text('{"old":"payload"}\n' * 3, encoding="utf-8")  # noqa: SLF001

    service._log_action({"action": "fresh-write", "ok": True})  # noqa: SLF001

    rotated = service._rotated_action_log_paths()  # noqa: SLF001
    assert service._action_log_path.exists()  # noqa: SLF001
    assert len(rotated) == 1
    assert rotated[0].suffix == ".gz"
    with gzip.open(rotated[0], "rt", encoding="utf-8") as handle:
        assert '{"old":"payload"}' in handle.read()
    rows = operator_dashboard_module._tail_jsonl(service._action_log_path, 5)  # noqa: SLF001
    assert rows == [{"action": "fresh-write", "ok": True}]


def test_operator_action_log_rotation_enforces_local_retention_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_MAX_BYTES", "20")
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_ROTATED_KEEP", "2")
    monkeypatch.setenv("MGC_OPERATOR_ACTION_LOG_COMPRESS_ROTATED", "true")
    outside_file = tmp_path / "action_log.19990101-000000.jsonl.gz"
    outside_file.write_text("outside", encoding="utf-8")
    service = OperatorDashboardService(tmp_path)

    for index in range(5):
        service._log_action({"action": "large", "index": index, "payload": "x" * 80})  # noqa: SLF001

    rotated = service._rotated_action_log_paths()  # noqa: SLF001
    assert len(rotated) == 2
    assert service._action_log_path.exists()  # noqa: SLF001
    assert outside_file.exists()
    status = service.action_log_rotation_status()
    assert status["rotated_file_count"] == 2
    assert status["archive_enabled"] is False
    assert status["would_delete_by_retention"] == []


def test_operator_action_log_archive_root_is_not_dashboard_hot_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive_root = tmp_path / "cold_archive"
    archive_root.mkdir()
    (archive_root / "action_log.20260101-000000.jsonl").write_text("not-json\n" * 1000, encoding="utf-8")
    monkeypatch.setenv("MGC_OPERATOR_ARCHIVE_ROOT", str(archive_root))
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service.dashboard_snapshot = lambda: (_ for _ in ()).throw(AssertionError("unexpected cold snapshot"))  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes[0][0] == HTTPStatus.OK
    assert writes[0][1]["dashboard_payload_mode"] == "degraded"
    assert service.action_log_rotation_status()["archive_root"] == str(archive_root)


def test_api_dashboard_reports_stale_runtime_config_paths_without_chasing_them(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    override_file = service._paper_runtime_config_paths_override_path  # noqa: SLF001
    override_file.parent.mkdir(parents=True, exist_ok=True)
    override_file.write_text(
        "/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml\n",
        encoding="utf-8",
    )
    service.dashboard_snapshot = lambda: (_ for _ in ()).throw(AssertionError("unexpected cold snapshot"))  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert payload["stale_runtime_config_paths_detected"] is True
    assert payload["stale_runtime_config_paths_ignored"] == [
        "/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml"
    ]
    assert payload["dashboard_meta"]["stale_runtime_config_paths_detected"] is True


def test_market_data_semantics_do_not_report_dead_when_runtime_is_stopped_but_feed_is_available() -> None:
    assert _market_data_semantics(running=False, market_data_ok=True, freshness="IDLE") == "READY"
    assert _market_data_semantics(running=False, market_data_ok=False, freshness="IDLE") == "UNKNOWN"
    assert _market_data_semantics(running=True, market_data_ok=False, freshness="FRESH") == "DEAD"


def test_api_dashboard_serves_current_same_instance_cache_while_runtime_artifacts_advance(tmp_path: Path) -> None:
    generated_at = (datetime.now(timezone.utc) - timedelta(seconds=35)).isoformat()
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-04-09T12:00:00+00:00",
        build_stamp="abc123def456",
        instance_id="instance-current",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
            {
                "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
                "generated_at": generated_at,
                "dashboard_meta": {"server_instance_id": "instance-current"},
                "operator_surface": {"ok": True},
            }
        ),
        encoding="utf-8",
    )
    source_path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text('{"updated_at":"2026-04-17T12:47:37+00:00"}', encoding="utf-8")
    track_b_status_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "operator_status"
        / "latest_operator_status_summary.json"
    )
    track_b_status_path.parent.mkdir(parents=True, exist_ok=True)
    track_b_status_path.write_text(
        json.dumps(
            {
                "status_verdict": "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW",
                "multi_strategy_runtime_cycle_verdict": "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
                "multi_strategy_submit_attempted": False,
                "live_money_readiness": False,
            }
        ),
        encoding="utf-8",
    )

    inline_generation_attempted = False

    def _unexpected_live_snapshot() -> dict[str, object]:
        nonlocal inline_generation_attempted
        inline_generation_attempted = True
        raise AssertionError("current-age same-instance cache should be served before inline regeneration")

    service.dashboard_snapshot = _unexpected_live_snapshot  # type: ignore[method-assign]
    handler_cls = _build_handler(service)
    handler = handler_cls.__new__(handler_cls)
    writes: list[tuple[HTTPStatus, dict[str, object]]] = []
    handler.path = "/api/dashboard"
    handler._write_json = lambda status, payload: writes.append((status, payload))  # type: ignore[method-assign]
    handler._serve_index_html = lambda: (_ for _ in ()).throw(AssertionError("unexpected html request"))  # type: ignore[method-assign]
    handler._serve_asset = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected asset request"))  # type: ignore[method-assign]

    handler.do_GET()

    assert writes
    status, payload = writes[0]
    assert status == HTTPStatus.OK
    assert inline_generation_attempted is False
    assert payload["dashboard_meta"]["server_instance_id"] == "instance-current"
    assert payload["generated_at"] == generated_at
    assert payload["track_b_operator_status"]["multi_strategy_runtime_cycle_verdict"] == (
        "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION"
    )
    assert payload["track_b_operator_status"]["multi_strategy_submit_attempted"] is False
    assert payload["track_b_operator_status"]["live_money_readiness"] is False


def test_track_b_operator_status_overlay_reads_live_monitor_artifacts(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    ledger_dir = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    diagnostic_dir = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    ledger_dir.mkdir(parents=True)
    diagnostic_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_operator_status_v1",
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
                "shadow_monitor_mode": "NOT_PROVIDED",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_shadow_monitor_v2",
                "monitor_mode": "PAPER",
                "mode": "PAPER",
                "monitor_verdict": "TRACK_B_SHADOW_MONITOR_LIVE_FEED_WARMING_UP",
                "pid": 80971,
                "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
                "paper_trading_enabled": True,
                "paper_on_signal": True,
                "paper_trades_attempted_count": 0,
                "latest_paper_lifecycle_report_path": {},
                "latest_broker_state_classification": {},
                "submit_allowed": False,
                "submit_attempted": False,
                "paper_proof_invoked": False,
                "broker_state_mutated": False,
                "live_money_readiness": False,
                "instrument_reports": [
                    {
                        "instrument_family": "MGC",
                        "runtime_chain_wired": True,
                        "enabled_strategies": ["ASIAN_DRIFT_V1", "US_LATE_PAUSE_RESUME_LONG_V1"],
                        "live_feed_pid": 80972,
                        "live_feed_connected": True,
                        "live_feed_status": "LIVE_FEED_WARMING_UP",
                        "live_feed_subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                        "live_feed_heartbeat_age_seconds": 13.4,
                        "live_feed_transport_connected": True,
                        "live_feed_raw_messages_fresh": True,
                        "live_feed_completed_1m_fresh": False,
                        "live_feed_completed_5m_fresh": True,
                        "live_feed_execution_fresh": False,
                        "live_feed_latest_1m_age_seconds": 151.2,
                        "live_feed_latest_completed_5m_age_seconds": 260.0,
                        "live_feed_execution_freshness_blocker": "latest 1m candle age 151.2s exceeds max 120s",
                        "live_feed_strategy_ready": False,
                        "live_feed_warmup_1m_count": 28,
                        "live_feed_warmup_completed_5m_count": 4,
                        "live_feed_required_1m_count": 40,
                        "live_feed_required_completed_5m_count": 8,
                        "live_feed_blocker": "Databento Live feed is warming up.",
                    },
                    {
                        "instrument_family": "MNQ",
                        "runtime_chain_wired": True,
                        "enabled_strategies": ["MNQ_US_DERIVATIVE_BEAR_TURN_V1"],
                        "live_feed_pid": 80973,
                        "live_feed_connected": True,
                        "live_feed_status": "LIVE_FEED_WARMING_UP",
                        "live_feed_subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                        "live_feed_heartbeat_age_seconds": 12.9,
                        "live_feed_strategy_ready": False,
                        "live_feed_warmup_1m_count": 27,
                        "live_feed_warmup_completed_5m_count": 4,
                        "live_feed_required_1m_count": 40,
                        "live_feed_required_completed_5m_count": 8,
                        "live_feed_blocker": "Databento Live feed is warming up.",
                    },
                    {
                        "instrument_family": "ES",
                        "runtime_chain_wired": False,
                        "enabled_strategies": [],
                        "primary_blocker": "ES has no enabled Track B strategies configured.",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_heartbeat.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_shadow_monitor_heartbeat_v2",
                "monitor_running": True,
                "pid": 80971,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (ledger_dir / "track_b_paper_trade_ledger.jsonl").write_text("", encoding="utf-8")
    (ledger_dir / "latest_track_b_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "paper_trades_attempted_count": 0,
                "open_position_count": 0,
                "review_required_count": 0,
                "latest_trade_ledger_path": "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl",
                "latest_live_position_status_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
                "latest_pnl_summary_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_pnl_summary.json",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_live_position_status.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "open_position_count": 0,
                "open_order_count": 0,
                "positions_by_instrument": {},
                "positions_by_strategy": {},
                "broker_truth_warning": "Artifact-derived status is not broker truth until source=BROKER_RECONCILED.",
                "latest_live_position_status_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "total_realized_pnl_today": "0",
                "total_realized_pnl_session": "0",
                "total_realized_pnl_week": "0",
                "total_unrealized_pnl": "0",
                "last_trade_strategy": None,
                "last_trade_pnl": None,
                "review_required_count": 0,
                "by_strategy": {},
                "by_instrument": {},
                "latest_pnl_summary_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_pnl_summary.json",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (diagnostic_dir / "latest_track_b_live_feed_freshness_diagnostic.json").write_text(
        json.dumps(
            {
                "diagnosis_classification": "STALE_LIVE_FEED",
                "primary_blocker": "MGC: latest 1m candle age 151.2s exceeds max 120s",
                "stale_instruments": ["MGC"],
                "fresh_instruments": ["MNQ"],
                "http_backfill_can_satisfy_execution_freshness": False,
                "instrument_reports": [
                    {
                        "instrument_family": "MGC",
                        "transport_connected": True,
                        "completed_1m_fresh": False,
                        "completed_5m_fresh": True,
                        "execution_fresh": False,
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    assert payload["shadow_monitor_mode"] == "PAPER"
    assert payload["shadow_monitor_launchd_label"] == "com.mgc.trackb.paper-monitor"
    assert payload["shadow_monitor_pid"] == 80971
    assert payload["shadow_monitor_live_feed_pid"] == 80972
    assert payload["shadow_monitor_runtime_decision_source"] == "DATABENTO_LIVE_ARTIFACT"
    assert payload["shadow_monitor_live_feed_connected"] is True
    assert payload["shadow_monitor_live_feed_execution_fresh"] is False
    assert payload["shadow_monitor_live_feed_completed_1m_fresh"] is False
    assert payload["shadow_monitor_live_feed_latest_1m_age_seconds"] == 151.2
    assert "latest 1m candle age" in payload["shadow_monitor_live_feed_execution_freshness_blocker"]
    assert payload["shadow_monitor_live_feed_strategy_ready"] is False
    assert payload["shadow_monitor_live_feed_warmup_1m_count"] == 28
    assert payload["shadow_monitor_live_feed_warmup_completed_5m_count"] == 4
    assert payload["shadow_monitor_paper_trades_attempted_count"] == 0
    assert payload["shadow_monitor_submit_allowed"] is False
    assert payload["shadow_monitor_submit_attempted"] is False
    assert payload["shadow_monitor_paper_proof_invoked"] is False
    assert payload["shadow_monitor_broker_state_mutated"] is False
    assert payload["shadow_monitor_live_money_readiness"] is False
    assert len(payload["shadow_monitor_instrument_reports"]) == 3
    assert payload["shadow_monitor_instrument_families"] == ["MGC", "MNQ", "ES"]
    assert payload["shadow_monitor_wired_instrument_count"] == 2
    assert payload["shadow_monitor_instrument_reports"][1]["instrument_family"] == "MNQ"
    assert payload["shadow_monitor_instrument_reports"][1]["enabled_strategies"] == ["MNQ_US_DERIVATIVE_BEAR_TURN_V1"]
    assert "MGC" not in str(payload["shadow_monitor_instrument_reports"][1].get("primary_blocker") or "")
    assert payload["track_b_paper_results_source"] == "TRACK_B_LIFECYCLE_ARTIFACTS"
    assert payload["track_b_paper_results_broker_reconciled"] is False
    assert payload["paper_trades_attempted_count"] == 0
    assert payload["open_position_count"] == 0
    assert payload["realized_pnl_today"] == "0"
    assert payload["realized_pnl_session"] == "0"
    assert payload["realized_pnl_week"] == "0"
    assert payload["unrealized_pnl"] == "0"
    assert payload["review_required_count"] == 0
    assert payload["track_b_positions_by_instrument"] == {}
    assert payload["latest_trade_ledger_path"] == "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl"
    assert payload["track_b_live_feed_freshness_diagnostic"]["diagnosis_classification"] == "STALE_LIVE_FEED"
    assert payload["track_b_live_feed_freshness_diagnostic"]["stale_instruments"] == ["MGC"]


def test_track_b_dashboard_keeps_startup_blockers_instrument_scoped(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics"
    diagnostics_dir.mkdir(parents=True)
    (diagnostics_dir / "latest_track_b_startup_readiness_diagnostic.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_startup_readiness_diagnostic_v1",
                "generated_at": "2026-05-06T07:10:00+00:00",
                "diagnosis_classification": "FEATURE_CONTEXT_NOT_READY",
                "instruments": {
                    "MGC": {
                        "classification": "FEATURE_CONTEXT_NOT_READY",
                        "context_ready": False,
                        "live_execution_approved": True,
                        "paper_evaluation_allowed": False,
                        "blocked_reason": "Runtime MGC 1m candle context has 1 detected gaps.",
                    },
                    "MNQ": {
                        "classification": "READY_WITH_BACKFILL_SEEDED_CONTEXT",
                        "context_ready": True,
                        "live_execution_approved": True,
                        "paper_evaluation_allowed": True,
                        "blocked_reason": None,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    diagnostic = payload["startup_readiness_diagnostic"]
    assert diagnostic["diagnosis_classification"] == "FEATURE_CONTEXT_NOT_READY"
    assert diagnostic["instruments"]["MGC"]["paper_evaluation_allowed"] is False
    assert diagnostic["instruments"]["MNQ"]["paper_evaluation_allowed"] is True
    assert diagnostic["instruments"]["MNQ"]["blocked_reason"] is None


def test_track_b_operator_status_overlay_does_not_scan_full_paper_ledger(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    ledger_dir = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    ledger_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps(
            {
                "monitor_mode": "PAPER",
                "pid": 123,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "live_money_readiness": False,
                "instrument_reports": [],
            }
        ),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_heartbeat.json").write_text(
        json.dumps({"monitor_running": True, "pid": 123}),
        encoding="utf-8",
    )
    (ledger_dir / "track_b_paper_trade_ledger.jsonl").write_text("{not valid jsonl and must not be read}\n", encoding="utf-8")
    (ledger_dir / "latest_track_b_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "paper_trades_attempted_count": 0,
                "latest_trade_ledger_path": "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl",
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_live_position_status.json").write_text(
        json.dumps({"source": "TRACK_B_LIFECYCLE_ARTIFACTS", "broker_reconciled": False, "open_position_count": 0}),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "total_realized_pnl_today": "0",
                "total_realized_pnl_session": "0",
                "total_realized_pnl_week": "0",
                "total_unrealized_pnl": "0",
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    assert payload["paper_trades_attempted_count"] == 0
    assert payload["latest_trade_ledger_path"].endswith("track_b_paper_trade_ledger.jsonl")
    assert payload["track_b_paper_results_broker_reconciled"] is False



def test_self_healing_health_compact_is_advisory_only() -> None:
    compact = operator_dashboard_module._compact_track_b_self_healing_health(
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "AUTO_RESTART_ELIGIBLE",
            "auto_restart_allowed": True,
            "restart_candidates": ["broker_truth_refresher"],
            "blockers": [],
            "warnings": ["broker_truth_refresher_not_running"],
            "live_money_eligible": False,
            "paper_recovery_policy_classification": "AUTONOMOUS_RETRY_ELIGIBLE",
            "paper_recovery_policy_severity": "INFO",
            "paper_recovery_diagnostic": "BOUNDED_AUTONOMOUS_RETRY",
            "paper_action_policy": "AUTONOMOUS_RETRY_ELIGIBLE",
            "autonomous_recovery_allowed": True,
            "requires_operator_ack_for_paper": False,
            "operator_ack_advisory_only_for_paper": True,
            "bounded_recovery_budget": {
                "max_attempts_per_target": 1,
                "max_attempts_per_window": 2,
                "cooldown_seconds": 300,
                "budget_exhausted": False,
            },
            "live_action_policy": "REQUIRE_ACK",
            "agents": {
                "broker_truth_refresher": {
                    "display_name": "Broker truth refresher",
                    "health_state": "UNHEALTHY",
                    "process_running": False,
                    "restart_eligible": True,
                    "restart_candidate": True,
                    "restart_blockers": ["unknown_open_orders"],
                }
            },
        },
        Path("outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json"),
    )

    assert compact["classification"] == "AUTO_RESTART_ELIGIBLE"
    assert compact["advisory_only"] is True
    assert compact["canonical_readiness_authority"] is False
    assert compact["submit_authority"] is False
    assert compact["paper_proof_invoked"] is False
    assert compact["live_money_eligible"] is False
    assert compact["restart_candidates"] == ["broker_truth_refresher"]
    assert compact["agents"][0]["restart_candidate"] is True
    assert compact["paper_recovery_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert compact["paper_recovery_diagnostic"] == "BOUNDED_AUTONOMOUS_RETRY"
    assert compact["autonomous_recovery_allowed"] is True
    assert compact["requires_operator_ack_for_paper"] is False
    assert compact["operator_ack_advisory_only_for_paper"] is True
    assert compact["bounded_recovery_budget"]["budget_exhausted"] is False


def test_operator_readiness_refresh_status_marks_stale_ready_as_stale() -> None:
    compact = operator_dashboard_module._compact_track_b_operator_readiness_refresh_status(
        {
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
            "generated_at": "2000-01-01T00:00:00+00:00",
            "last_success": True,
            "refresh_seconds": 60.0,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
        Path("outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json"),
    )

    assert compact["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_STALE"
    assert compact["source_classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert compact["fresh"] is False
    assert compact["last_success"] is True


def test_operator_readiness_refresh_status_marks_missing_service_loudly(tmp_path: Path) -> None:
    compact = operator_dashboard_module._compact_track_b_operator_readiness_refresh_status(
        {
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
            "generated_at": "2999-01-01T00:00:00+00:00",
            "last_success": True,
            "refresh_seconds": 60.0,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
        tmp_path / "latest_track_b_operator_readiness_refresher_status.json",
        heartbeat_payload={
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
            "generated_at": "2999-01-01T00:00:00+00:00",
            "refresh_seconds": 60.0,
        },
        heartbeat_path=tmp_path / "heartbeat.json",
        service_pid_path=tmp_path / "missing_service.pid",
        child_pid_path=tmp_path / "missing_child.pid",
    )

    assert compact["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_SERVICE_NOT_RUNNING"
    assert compact["service_running"] is False
    assert compact["heartbeat_fresh"] is True
    assert compact["last_success"] is True

def test_track_b_paper_trading_payload_reads_compact_summaries_without_full_ledger_scan(tmp_path: Path) -> None:
    ledger_dir = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    broker_truth_dir = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    readiness_refresh_dir = tmp_path / "outputs" / "reports" / "track_b_operator_readiness_refresher"
    runtime_dir = tmp_path / "outputs" / "operator_dashboard" / "runtime"
    ledger_dir.mkdir(parents=True)
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    broker_truth_dir.mkdir(parents=True)
    readiness_refresh_dir.mkdir(parents=True)
    runtime_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps({"monitor_mode": "PAPER", "live_money_readiness": False, "instrument_reports": []}),
        encoding="utf-8",
    )
    (ledger_dir / "track_b_paper_trade_ledger.jsonl").write_text("{invalid full ledger that must not be scanned}\n", encoding="utf-8")
    (ledger_dir / "latest_track_b_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "paper_trades_attempted_count": 1,
                "completed_trade_count": 1,
                "managed_strategy_trade_count": 0,
                "meaningful_strategy_trade_count": 0,
                "proof_canary_trade_count": 1,
                "archived_manual_flat_count": 0,
                "proof_canary_excluded_from_meaningful_strategy_counts": True,
                "recent_trades": [
                    {
                        "trade_id": "trade-1",
                        "time": "2026-05-06T13:35:00+00:00",
                        "instrument_family": "MNQ",
                        "contract_key": "MNQ-202606",
                        "local_symbol": "MNQM6",
                        "strategy_id": "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
                        "side": "SHORT",
                        "quantity": "1",
                        "entry_price": "18799.5",
                        "exit_price": "18795.25",
                        "realized_pnl": "8.5",
                        "paper_lifecycle_classification": "PROOF_COMPLETE_FLAT",
                        "broker_reconciled": False,
                        "review_required": False,
                    }
                ],
                "latest_trade_ledger_path": "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl",
                "latest_trade_summary_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_live_position_status.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "open_position_count": 1,
                "positions_by_instrument": {
                    "MNQ-202606": {
                        "lifecycle_id": "life-1",
                        "strategy_id": "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
                        "instrument_family": "MNQ",
                        "contract_key": "MNQ-202606",
                        "local_symbol": "MNQM6",
                        "side": "SHORT",
                        "quantity": "1",
                        "avg_entry_price": "18799.5",
                        "latest_mark_price": "18798.0",
                        "unrealized_pnl": "3.0",
                        "entry_timestamp": "2026-05-06T13:30:00+00:00",
                        "status": "OPEN",
                        "review_required": False,
                    }
                },
                "positions_by_strategy": {},
                "broker_truth_warning": "Artifact-derived status is not broker truth until source=BROKER_RECONCILED.",
                "latest_live_position_status_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "total_realized_pnl_today": "8.5",
                "total_realized_pnl_session": "8.5",
                "total_realized_pnl_week": "8.5",
                "total_realized_pnl_month": "8.5",
                "total_realized_pnl_ytd": "8.5",
                "total_unrealized_pnl": "3.0",
                "last_trade_strategy": "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
                "last_trade_pnl": "8.5",
                "review_required_count": 0,
                "by_strategy": {
                    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": {
                        "trades": 1,
                        "open_position_count": 1,
                        "instrument": "MNQ",
                        "realized_pnl_today": "8.5",
                        "realized_pnl_week": "8.5",
                        "realized_pnl_ytd": "8.5",
                        "last_trade_time": "2026-05-06T13:35:00+00:00",
                        "review_required_count": 0,
                    }
                },
                "by_instrument": {
                    "MNQ-202606": {
                        "trades": 1,
                        "open_position_count": 1,
                        "instrument": "MNQ",
                        "realized_pnl_today": "8.5",
                        "unrealized_pnl": "3.0",
                        "review_required_count": 0,
                    }
                },
                "latest_pnl_summary_path": "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_pnl_summary.json",
            }
        ),
        encoding="utf-8",
    )
    (broker_truth_dir / "ibkr_broker_truth_refresh_status.json").write_text(
        json.dumps(
            {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "generated_at": "2999-01-01T00:00:00+00:00",
                "latest_refresh_time": "2999-01-01T00:00:00+00:00",
                "last_success_at": "2999-01-01T00:00:00+00:00",
                "last_success": True,
                "account": "DUM882026",
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9077,
                "refresh_seconds": 60,
                "positions_complete": True,
                "open_orders_complete": True,
                "position_count": 1,
                "open_order_count": 0,
                "positions_snapshot_path": str(broker_truth_dir / "ibkr_positions_snapshot.json"),
                "open_orders_snapshot_path": str(broker_truth_dir / "ibkr_open_orders_snapshot.json"),
                "submit_authority": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        ),
        encoding="utf-8",
    )
    (readiness_refresh_dir / "latest_track_b_operator_readiness_refresher_status.json").write_text(
        json.dumps(
            {
                "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
                "generated_at": "2999-01-01T00:00:00+00:00",
                "last_success": True,
                "last_success_at": "2999-01-01T00:00:00+00:00",
                "refresh_seconds": 60,
                "preflight_mode": "monday-live",
                "refreshed_artifacts": {
                    "track_b_paper_preflight": "outputs/reports/track_b_paper_preflight/latest_track_b_paper_preflight.json"
                },
                "submit_authority": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        ),
        encoding="utf-8",
    )
    (runtime_dir / "latest_track_b_self_healing_health.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "SELF_HEALING_READY",
                "auto_restart_allowed": False,
                "restart_candidates": [],
                "operator_required_agents": [],
                "blockers": [],
                "warnings": [],
                "live_money_eligible": False,
                "agents": {
                    "paper_runtime": {
                        "display_name": "Track B PAPER runtime",
                        "health_state": "HEALTHY",
                        "process_running": True,
                        "restart_eligible": False,
                        "restart_candidate": False,
                        "restart_blockers": ["runtime_restart_requires_explicit_operator_approval"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    assert payload["source"] == "TRACK_B_LIFECYCLE_ARTIFACTS"
    assert payload["broker_reconciled"] is False
    assert "not broker truth" in payload["broker_truth_warning"]
    assert payload["paper_trades_attempted_count"] == 1
    assert payload["completed_trade_count"] == 1
    assert payload["managed_strategy_trade_count"] == 0
    assert payload["meaningful_strategy_trade_count"] == 0
    assert payload["proof_canary_trade_count"] == 1
    assert payload["archived_manual_flat_count"] == 0
    assert payload["proof_canary_excluded_from_meaningful_strategy_counts"] is True
    assert payload["open_position_count"] == 1
    assert payload["realized_pnl_today"] == "8.5"
    assert payload["realized_pnl_month"] == "8.5"
    assert payload["realized_pnl_ytd"] == "8.5"
    assert payload["unrealized_pnl"] == "3.0"
    assert payload["positions"][0]["contract_key"] == "MNQ-202606"
    assert payload["broker_truth_refresh_status"]["classification"] == "BROKER_TRUTH_REFRESH_FRESH"
    assert payload["broker_truth_refresh_status"]["fresh"] is True
    assert payload["broker_truth_refresh_status"]["account"] == "DUM882026"
    assert payload["broker_truth_refresh_status"]["submit_authority"] is False
    assert payload["broker_truth_refresh_status"]["live_money_eligible"] is False
    assert payload["operator_readiness_refresh_status"]["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_SERVICE_NOT_RUNNING"
    assert payload["operator_readiness_refresh_status"]["source_classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert payload["operator_readiness_refresh_status"]["fresh"] is True
    assert payload["operator_readiness_refresh_status"]["service_running"] is False

    assert payload["operator_readiness_refresh_status"]["last_success"] is True
    assert payload["operator_readiness_refresh_status"]["submit_authority"] is False
    assert payload["operator_readiness_refresh_status"]["paper_proof_invoked"] is False
    assert payload["operator_readiness_refresh_status"]["live_money_eligible"] is False
    assert payload["self_healing_health"]["classification"] == "SELF_HEALING_READY"
    assert payload["self_healing_health"]["advisory_only"] is True
    assert payload["self_healing_health"]["canonical_readiness_authority"] is False
    assert payload["self_healing_health"]["auto_restart_allowed"] is False
    assert payload["self_healing_health"]["submit_authority"] is False
    assert payload["self_healing_health"]["live_money_eligible"] is False
    assert payload["recent_trades"][0]["strategy_id"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert payload["strategy_performance"][0]["strategy"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert payload["instrument_performance"][0]["instrument"] == "MNQ-202606"
    managed_readiness = {row["strategy_id"]: row for row in payload["managed_paper_lifecycle_readiness"]}
    assert payload["managed_exit_readiness"] == payload["managed_paper_lifecycle_readiness"]
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["managed_paper_ready"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["managed_entry_ready"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["managed_exit_ready"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["side_action_explicit"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["signal_to_intent_bridge_can_create_intent"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["strategy_managed_route_available"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["paper_proof_fallback_allowed_for_real_signals"] is False
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["exit_policy_can_generate_close_intent"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["close_leg_can_be_tracked"] is True
    assert managed_readiness["MNQ_FIRST_BEAR_SNAP_TURN_V1"]["final_state_classification_supported"] is True
    assert managed_readiness["asian_drift_v1"]["managed_paper_ready"] is True
    assert managed_readiness["asian_drift_v1"]["managed_entry_ready"] is True
    assert managed_readiness["asian_drift_v1"]["managed_exit_ready"] is True
    assert managed_readiness["asian_drift_v1"]["side_action_explicit"] is True
    assert managed_readiness["asian_drift_v1"]["side"] == "RUNTIME_EXPLICIT_LONG_OR_SHORT"
    assert managed_readiness["asian_drift_v1"]["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert managed_readiness["mgc_ema_momentum_reclaim_long_v1"]["managed_paper_ready"] is False
    assert managed_readiness["mgc_ema_momentum_reclaim_long_v1"]["managed_entry_ready"] is False
    assert managed_readiness["mgc_ema_momentum_reclaim_long_v1"]["signal_to_intent_bridge_can_create_intent"] is False
    assert managed_readiness["mgc_ema_momentum_reclaim_long_v1"]["managed_exit_ready"] is False
    assert managed_readiness["mgc_ema_momentum_reclaim_long_v1"]["managed_paper_blocker"] == "managed exit policy missing"


def test_track_b_paper_trading_payload_prefers_fresh_broker_reconciled_overlay(tmp_path: Path) -> None:
    ledger_dir = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    reconciliation_dir = tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation"
    ledger_dir.mkdir(parents=True)
    reconciliation_dir.mkdir(parents=True)
    (ledger_dir / "latest_track_b_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "paper_trades_attempted_count": 2,
                "completed_trade_count": 2,
                "open_position_count": 0,
                "review_required_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_live_position_status.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "open_position_count": 0,
                "open_order_count": 0,
                "positions_by_instrument": {},
                "positions_by_strategy": {},
                "broker_truth_warning": "Artifact-derived status is not broker truth until source=BROKER_RECONCILED.",
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "total_realized_pnl_today": "-93.0",
                "total_realized_pnl_session": "-93.0",
                "total_realized_pnl_week": "-94.0",
                "total_unrealized_pnl": "0",
                "review_required_count": 0,
                "by_strategy": {},
                "by_instrument": {},
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_broker_reconciled_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "BROKER_RECONCILED",
                "broker_reconciled": True,
                "paper_trades_attempted_count": 2,
                "completed_trade_count": 2,
                "open_position_count": 0,
                "review_required_count": 0,
                "latest_trade_summary_path": str(
                    ledger_dir / "latest_track_b_broker_reconciled_paper_trade_summary.json"
                ),
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_broker_reconciled_live_position_status.json").write_text(
        json.dumps(
            {
                "source": "BROKER_RECONCILED",
                "broker_reconciled": True,
                "open_position_count": 0,
                "open_order_count": 0,
                "positions_by_instrument": {},
                "positions_by_strategy": {},
                "broker_truth_warning": "Broker read-only truth agrees with Track B lifecycle flat state.",
                "latest_live_position_status_path": str(
                    ledger_dir / "latest_track_b_broker_reconciled_live_position_status.json"
                ),
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_broker_reconciled_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "BROKER_RECONCILED",
                "broker_reconciled": True,
                "total_realized_pnl_today": "-93.0",
                "total_realized_pnl_session": "-93.0",
                "total_realized_pnl_week": "-94.0",
                "total_unrealized_pnl": "0",
                "review_required_count": 0,
                "by_strategy": {},
                "by_instrument": {},
                "latest_pnl_summary_path": str(ledger_dir / "latest_track_b_broker_reconciled_pnl_summary.json"),
            }
        ),
        encoding="utf-8",
    )
    (reconciliation_dir / "latest_track_b_paper_broker_reconciliation.json").write_text(
        json.dumps(
            {
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "generated_at": "2999-01-01T00:00:00+00:00",
                "broker_reconciled": True,
                "max_age_seconds": 120,
                "track_b_broker_position_count": 0,
                "track_b_broker_open_order_count": 0,
                "lifecycle_open_position_count": 0,
                "lifecycle_open_order_count": 0,
                "review_required_count": 0,
                "blockers": [],
                "submit_authority": False,
                "paper_proof_invoked": False,
                "live_money_eligible": False,
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    assert payload["source"] == "BROKER_RECONCILED"
    assert payload["broker_reconciled"] is True
    assert payload["broker_reconciliation_applied"] is True
    assert payload["broker_reconciliation_status"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert payload["broker_reconciliation_status"]["broker_reconciled"] is True
    assert payload["latest_live_position_status_path"].endswith(
        "latest_track_b_broker_reconciled_live_position_status.json"
    )
    assert payload["broker_truth_warning"] == "Broker-reconciled PAPER lifecycle view."
    assert payload["open_position_count"] == 0
    assert payload["review_required_count"] == 0


def test_track_b_paper_trading_payload_surfaces_fresh_broker_position_over_lifecycle_flat(
    tmp_path: Path,
) -> None:
    ledger_dir = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    reconciliation_dir = tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation"
    ledger_dir.mkdir(parents=True)
    reconciliation_dir.mkdir(parents=True)
    (ledger_dir / "latest_track_b_paper_trade_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "paper_trades_attempted_count": 2,
                "completed_trade_count": 2,
                "open_position_count": 0,
                "review_required_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_live_position_status.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "open_position_count": 0,
                "open_order_count": 0,
                "positions_by_instrument": {},
                "positions_by_strategy": {},
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "latest_track_b_pnl_summary.json").write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "total_unrealized_pnl": "0",
                "review_required_count": 0,
                "by_strategy": {},
                "by_instrument": {},
            }
        ),
        encoding="utf-8",
    )
    (reconciliation_dir / "latest_track_b_paper_broker_reconciliation.json").write_text(
        json.dumps(
            {
                "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
                "generated_at": "2999-01-01T00:00:00+00:00",
                "broker_reconciled": False,
                "max_age_seconds": 120,
                "track_b_broker_position_count": 1,
                "track_b_broker_open_order_count": 0,
                "lifecycle_open_position_count": 0,
                "lifecycle_open_order_count": 0,
                "review_required_count": 0,
                "blockers": [
                    {
                        "code": "TRACK_B_BROKER_POSITION_PRESENT",
                        "detail": "IBKR broker truth reports one or more Track B futures positions.",
                        "positions": [
                            {
                                "account_id": "DUM882026",
                                "local_symbol": "MNQM6",
                                "symbol": "MNQ",
                                "quantity": "1.0",
                                "average_cost": "57963.12",
                            }
                        ],
                    }
                ],
                "submit_authority": False,
                "paper_proof_invoked": False,
                "live_money_eligible": False,
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    assert payload["broker_reconciled"] is False
    assert payload["broker_reconciliation_applied"] is False
    assert payload["broker_reconciliation_status"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert payload["open_position_count"] == 1
    assert payload["display_open_position_source"] == "BROKER_TRUTH_RECONCILIATION"
    assert payload["broker_truth_open_position_count"] == 1
    assert payload["broker_truth_open_order_count"] == 0
    assert payload["lifecycle_open_position_count"] == 0
    assert payload["broker_truth_open_positions"][0]["local_symbol"] == "MNQM6"
    assert payload["broker_reconciliation_status"]["broker_truth_open_positions"][0]["local_symbol"] == "MNQM6"
    assert "IBKR broker truth reports 1 Track B futures position" in payload["broker_truth_warning"]


def test_track_b_paper_trading_payload_includes_compact_zero_activity_diagnostic(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics"
    diagnostics_dir.mkdir(parents=True)
    (diagnostics_dir / "latest_track_b_zero_activity_diagnostic.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_zero_activity_diagnostic_v1",
                "generated_at": "2026-05-06T06:30:00+00:00",
                "latest_monitor_completed_at": "2026-05-06T06:29:45+00:00",
                "latest_monitor_verdict": "TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT",
                "diagnosis_classification": "STALE_LIVE_FEED",
                "dominant_blocker": "Execution freshness failing for: MGC",
                "recommended_next_action": "Inspect live ohlcv latency.",
                "cycle_summary": {
                    "recent_monitor_cycle_count": 20,
                    "recent_evaluation_cycle_count": 2,
                    "recent_strategy_evaluation_count": 18,
                    "recent_candidate_signal_count": 0,
                    "recent_suppressed_signal_count": 0,
                },
                "journal_summary": {
                    "latest_tier_counts": {"TIER_1_NO_SETUP_AGGREGATE": 8},
                    "tier3_without_recent_candidate_signal_warning": False,
                },
                "completed_decision_bar_audit": {
                    "schema_version": "track_b_completed_decision_bar_evaluation_audit_v1",
                    "generated_at": "2026-05-06T06:30:00+00:00",
                    "classification": "EVALUATING_EACH_COMPLETED_BAR",
                    "instruments": {
                        "MGC": {
                            "classification": "EVALUATING_EACH_COMPLETED_BAR",
                            "completed_live_5m_bars_observed": 1,
                            "decision_bars_eligible_for_evaluation": 1,
                            "decision_bars_actually_evaluated": 1,
                            "decision_bars_skipped": 0,
                            "latest_evaluated_decision_bar_timestamp": "2026-05-06T06:25:00+00:00",
                            "latest_completed_live_5m_bar_timestamp": "2026-05-06T06:25:00+00:00",
                            "monitor_caught_up_to_latest_completed_bar": True,
                            "no_signal_count": 9,
                            "signal_count": 0,
                            "suppressed_signal_count": 0,
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (diagnostics_dir / "latest_track_b_no_signal_attribution_rollup.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_no_signal_attribution_rollup_v1",
                "generated_at": "2026-05-06T06:30:00+00:00",
                "classification": "NO_SIGNAL_WITH_ATTRIBUTION",
                "completed_decision_bars_observed": 1,
                "eligible_decision_bars": 1,
                "evaluated_decision_bars": 1,
                "total_strategy_evaluations": 1,
                "total_no_signals": 1,
                "total_signals": 0,
                "total_suppressed": 0,
                "total_handoffs": 0,
                "attribution_complete": True,
                "top_failed_predicates": [{"reason": "vwap_location_ok", "count": 1}],
                "closest_near_misses": [],
                "strategies": [
                    {
                        "strategy_id": "TEST_STRATEGY_V1",
                        "instrument": "MGC",
                        "evaluated_bars": 1,
                        "no_signal_count": 1,
                        "signal_count": 0,
                        "suppressed_count": 0,
                        "top_failed_predicates": [{"reason": "vwap_location_ok", "count": 1}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    diagnostic = payload["zero_activity_diagnostic"]
    assert diagnostic["available"] is True
    assert diagnostic["diagnosis_classification"] == "STALE_LIVE_FEED"
    assert diagnostic["recent_cycles"] == 20
    assert diagnostic["strategies_evaluated"] == 18
    assert diagnostic["signals_seen"] == 0
    assert diagnostic["completed_decision_bar_audit"]["classification"] == "EVALUATING_EACH_COMPLETED_BAR"
    assert diagnostic["completed_decision_bar_audit"]["instruments"][0]["decision_bars_actually_evaluated"] == 1
    assert payload["no_signal_attribution_rollup"]["classification"] == "NO_SIGNAL_WITH_ATTRIBUTION"
    assert payload["no_signal_attribution_rollup"]["top_failed_predicates"][0]["reason"] == "vwap_location_ok"


def test_track_b_paper_trading_payload_marks_stale_zero_activity_diagnostic(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics"
    diagnostics_dir.mkdir(parents=True)
    (diagnostics_dir / "latest_track_b_zero_activity_diagnostic.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_zero_activity_diagnostic_v1",
                "generated_at": "2026-05-06T06:30:00+00:00",
                "latest_monitor_completed_at": "2026-05-06T06:29:45+00:00",
                "latest_monitor_verdict": "TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT",
                "diagnosis_classification": "NORMAL_NO_SIGNAL",
                "cycle_summary": {
                    "recent_monitor_cycle_count": 20,
                    "recent_evaluation_cycle_count": 1,
                    "recent_strategy_evaluation_count": 9,
                    "recent_candidate_signal_count": 0,
                    "recent_suppressed_signal_count": 0,
                },
                "journal_summary": {},
            }
        ),
        encoding="utf-8",
    )
    (diagnostics_dir / "latest_track_b_startup_readiness_diagnostic.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_startup_readiness_diagnostic_v1",
                "generated_at": "2026-05-06T13:34:29+00:00",
                "diagnosis_classification": "READY_WITH_LIVE_ONLY_CONTEXT",
                "instruments": {},
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    diagnostic = payload["zero_activity_diagnostic"]
    assert diagnostic["diagnosis_classification"] == "STALE_DIAGNOSTIC"
    assert diagnostic["source_diagnosis_classification"] == "NORMAL_NO_SIGNAL"
    assert diagnostic["stale"] is True
    assert "older than" in diagnostic["stale_reason"]


def test_track_b_paper_trading_payload_includes_startup_readiness_diagnostic(tmp_path: Path) -> None:
    diagnostics_dir = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics"
    diagnostics_dir.mkdir(parents=True)
    (diagnostics_dir / "latest_track_b_startup_readiness_diagnostic.json").write_text(
        json.dumps(
            {
                "schema_version": "track_b_startup_readiness_diagnostic_v1",
                "generated_at": "2026-05-06T07:10:00+00:00",
                "diagnosis_classification": "READY_WITH_BACKFILL_SEEDED_CONTEXT",
                "instruments": {
                    "MGC": {
                        "classification": "READY_WITH_BACKFILL_SEEDED_CONTEXT",
                        "required_1m_context_bars": 40,
                        "available_1m_context_bars": 40,
                        "required_5m_context_bars": 8,
                        "available_5m_context_bars": 8,
                        "live_1m_bars": 3,
                        "required_live_1m_bars": 3,
                        "live_completed_5m_bars": 1,
                        "required_live_completed_5m_bars": 1,
                        "backfill_gap_detected": True,
                        "backfill_gap_filled": True,
                        "backfill_source": "DATABENTO_HTTP_BACKFILL",
                        "context_ready": True,
                        "live_execution_approved": True,
                        "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
                        "paper_evaluation_allowed": True,
                        "blocked_reason": None,
                        "context_continuity_verdict": "CONTEXT_CONTINUITY_READY",
                        "gap_count": 1,
                        "gap_start": "2026-05-06T07:03:00+00:00",
                        "gap_end": "2026-05-06T07:03:00+00:00",
                        "missing_expected_bars": 1,
                        "gap_classification": "REPAIRED_BACKFILL_GAP",
                        "gap_repair_attempted": True,
                        "gap_repair_succeeded": True,
                        "gap_repair_source": "DATABENTO_HTTP_BACKFILL",
                        "remaining_blocker": None,
                        "feature_context_ready_after_repair": True,
                        "paper_evaluation_allowed_after_repair": True,
                        "gaps": [
                            {
                                "classification": "REPAIRED_BACKFILL_GAP",
                                "start_timestamp": "2026-05-06T07:03:00+00:00",
                                "end_timestamp": "2026-05-06T07:03:00+00:00",
                                "within_required_window": True,
                                "repair_attempted": True,
                                "repair_succeeded": True,
                            }
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    diagnostic = payload["startup_readiness_diagnostic"]
    assert diagnostic["available"] is True
    assert diagnostic["diagnosis_classification"] == "READY_WITH_BACKFILL_SEEDED_CONTEXT"
    assert diagnostic["instruments"]["MGC"]["context_ready"] is True
    assert diagnostic["instruments"]["MGC"]["live_execution_approved"] is True
    assert diagnostic["instruments"]["MGC"]["live_1m_bars"] == 3
    assert diagnostic["instruments"]["MGC"]["required_live_1m_bars"] == 3
    assert diagnostic["instruments"]["MGC"]["live_completed_5m_bars"] == 1
    assert diagnostic["instruments"]["MGC"]["required_live_completed_5m_bars"] == 1
    assert diagnostic["instruments"]["MGC"]["latest_decision_bar_source"] == "DATABENTO_LIVE_ARTIFACT"
    assert diagnostic["instruments"]["MGC"]["paper_evaluation_allowed"] is True
    assert diagnostic["instruments"]["MGC"]["gap_start"] == "2026-05-06T07:03:00+00:00"
    assert diagnostic["instruments"]["MGC"]["gap_classification"] == "REPAIRED_BACKFILL_GAP"
    assert diagnostic["instruments"]["MGC"]["gap_repair_succeeded"] is True
    assert diagnostic["instruments"]["MGC"]["paper_evaluation_allowed_after_repair"] is True
    assert diagnostic["instruments"]["MGC"]["context_continuity_verdict"] == "CONTEXT_CONTINUITY_READY"
    assert diagnostic["instruments"]["MGC"]["gaps"][0]["classification"] == "REPAIRED_BACKFILL_GAP"


def test_track_b_paper_trading_payload_degrades_with_missing_compact_summaries(tmp_path: Path) -> None:
    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    assert payload["available"] is False
    assert payload["source"] == "NOT_PROVIDED"
    assert payload["paper_trades_attempted_count"] == 0
    assert payload["open_position_count"] == 0
    assert payload["realized_pnl_today"] == "0"
    assert payload["unrealized_pnl"] == "0"
    assert payload["summary_artifacts_missing"]
    assert payload["zero_activity_diagnostic"]["available"] is False


def test_track_b_paper_trading_payload_marks_live_money_critical(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps({"monitor_mode": "PAPER", "live_money_readiness": True, "instrument_reports": []}),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._track_b_paper_trading_results_payload()  # noqa: SLF001

    assert payload["critical"] is True
    assert "live_money_readiness=true" in payload["critical_warnings"][0]


def test_track_b_operator_status_overlay_degrades_when_compact_paper_summaries_are_missing(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps(
            {
                "monitor_mode": "PAPER",
                "pid": 123,
                "monitor_verdict": "TRACK_B_SHADOW_MONITOR_LIVE_FEED_WARMING_UP",
                "paper_trades_attempted_count": 0,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "live_money_readiness": False,
                "instrument_reports": [],
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    assert payload["shadow_monitor_mode"] == "PAPER"
    assert payload["shadow_monitor_pid"] == 123
    assert payload["paper_trades_attempted_count"] == 0
    assert payload["track_b_safety_critical"] is False


def test_track_b_operator_status_overlay_marks_live_money_readiness_as_critical(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps(
            {
                "monitor_mode": "PAPER",
                "live_money_readiness": True,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "instrument_reports": [],
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    assert payload["track_b_safety_critical"] is True
    assert "live_money_readiness=true" in payload["track_b_safety_primary_warning"]


def test_track_b_operator_status_overlay_marks_unproven_mutation_review_required(tmp_path: Path) -> None:
    operator_status_dir = tmp_path / "outputs" / "track_b_execution_core" / "operator_status"
    monitor_dir = tmp_path / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    operator_status_dir.mkdir(parents=True)
    monitor_dir.mkdir(parents=True)
    (operator_status_dir / "latest_operator_status_summary.json").write_text(
        json.dumps({"schema_version": "track_b_operator_status_v1", "live_money_readiness": False}),
        encoding="utf-8",
    )
    (monitor_dir / "latest_track_b_shadow_monitor_report.json").write_text(
        json.dumps(
            {
                "monitor_mode": "PAPER",
                "submit_attempted": True,
                "broker_state_mutated": True,
                "latest_paper_lifecycle_report_path": {},
                "live_money_readiness": False,
                "instrument_reports": [],
            }
        ),
        encoding="utf-8",
    )

    payload = OperatorDashboardService(tmp_path)._latest_track_b_operator_status_payload()  # noqa: SLF001

    assert payload is not None
    assert payload["track_b_safety_critical"] is True
    assert payload["track_b_safety_review_required"] is True
    assert "guarded lifecycle provenance" in payload["track_b_safety_primary_warning"]


def test_dashboard_assets_use_operator_first_surface_and_preserve_legacy_surfaces() -> None:
    html = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.html").read_text(encoding="utf-8")
    js = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.js").read_text(encoding="utf-8")
    css = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.css").read_text(encoding="utf-8")

    assert 'data-lane-section="' not in html
    assert "<h2>Execution Truth</h2>" in html
    assert "<h2>Portfolio P&amp;L / Risk</h2>" in html
    assert "<h2>Instrument Rollup</h2>" in html
    assert "<h2>Current Active Positions</h2>" in html
    assert "<h2>Active Lanes / Instruments</h2>" in html
    assert "<h2>Experimental Paper / Diagnostics</h2>" in html
    assert "<h2>Unified Active Lane Table</h2>" not in html
    assert "<h2>Secondary Market Context</h2>" in html
    assert "<h2>Diagnostics / Evidence</h2>" in html
    assert 'class="panel diagnostics-shell secondary-panel"' in html
    assert 'class="diagnostics-toggle"' in html
    assert 'class="diagnostics-stack"' in html
    assert 'id="operator-readiness-cards"' in html
    assert "Self-Healing" in js
    assert "self_healing_classification" in js
    assert "Optional Degradation" in js
    assert "Non-blocking readiness warnings" in js
    assert "Diagnostic-only stale surfaces" in js
    assert 'id="operator-canary-cards"' in html
    assert 'id="temporary-paper-strategies-table"' in html
    assert 'data-action="start-atp-companion-paper"' not in html
    assert 'data-action="atp-companion-paper-flatten-and-halt"' not in html
    assert "renderOperatorCanarySummary" in js
    assert "Broker truth lease:" in js
    assert "renderTemporaryPaperStrategies" in js
    assert "tracked-paper-start" not in js
    assert ".operator-canary-panel" in css
    assert 'id="operator-risk-cards"' in html
    assert 'id="operator-risk-notes"' in html
    assert 'id="operator-instrument-table"' in html
    assert 'id="operator-active-positions-table"' in html
    assert 'id="operator-universe-cards"' in html
    assert 'id="operator-lane-grid-summary"' in html
    assert 'id="operator-lane-grid-table"' in html
    assert 'id="operator-context-items"' in html
    assert html.count("operator-flow-table-wrap") == 3
    assert 'id="market-value-djia"' not in html
    assert 'id="treasury-summary-10y"' not in html
    assert "renderLaneRegistrySections(dashboard.lane_registry || {});" in js
    assert "renderOperatorSurface(dashboard.operator_surface || {});" in js
    assert "renderRuntimeBuildInfo(dashboard);" in js
    assert "function renderOperatorLaneGrid(rows)" in js
    assert "function renderOperatorInstrumentRollup(payload)" in js
    assert "function renderOperatorActivePositions(payload)" in js
    assert "function renderOperatorContext(payload)" in js
    assert '"Lanes Loaded"' in js
    assert '"Route-Ready Lanes"' in js
    assert '"Session Eligible"' in js
    assert '"Waiting For Bar"' in js
    assert '"Actionable Signals"' in js
    assert '"Current Blockers"' in js
    assert '"Stale Market Data"' in js
    assert "function contextStatusLevel(status)" in js
    assert "function horizonAvailable(horizon)" in js
    assert "operator-context-value" in js
    assert "operator-context-note" in js
    assert '"diagnostics diagnostics diagnostics diagnostics diagnostics"' in css
    assert ".diagnostics-shell { grid-area: diagnostics; }" in css
    assert ".diagnostics-stack {" in css
    assert ".operator-context-gap-list {" in css
    assert ".operator-flow-table-wrap {" in css
    assert "max-height: none;" in css
    assert ".operator-context-items {" in css
    assert "grid-template-columns: repeat(4, minmax(0, 1fr));" in css
    assert ".operator-context-value {" in css
    assert ".operator-context-reference {" in css
    assert 'id="runtime-build-stamp"' in html
    assert 'id="runtime-server-pid"' in html
    assert 'id="runtime-started-at"' in html
    assert 'id="runtime-snapshot-generated"' in html
    assert 'id="runtime-approved-quant-count"' in html
    assert 'id="runtime-admitted-paper-count"' in html
    assert 'id="runtime-temporary-paper-count"' in html
    assert 'id="runtime-registry-line"' in html
    assert 'id="performance-realized"' in html
    assert 'id="history-vs-prior"' in html
    assert 'id="branch-performance-table"' in html
    assert 'id="run-start-history-link"' in html
    assert "<h2>Historical Playback Test</h2>" in html
    assert 'id="historical-playback-table"' in html
    assert 'id="historical-playback-filter"' in html
    assert 'id="paper-ready-lane-eligibility-note"' in html
    assert 'id="paper-ready-lane-eligibility-table"' in html
    assert 'renderHistoricalPlayback(historical_playback || {});' in js
    assert 'function renderHistoricalPlayback(payload)' in js
    assert 'historical-playback-trigger-json-link' in html
    assert 'paper-ready-lane-eligibility-table' in js
    assert 'Current runtime session:' in js


def test_dashboard_snapshot_reads_real_artifacts(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    admitted_lanes = [
        {
            "lane_id": "mgc_us_late_pause_resume_long",
            "display_name": "MGC / usLatePauseResumeLongTurn",
            "symbol": "MGC",
            "session_restriction": "US_LATE",
            "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-500",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "symbol": "MGC",
            "session_restriction": "ASIA_EARLY",
            "approved_long_entry_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
            "entries_enabled": False,
            "operator_halt": True,
            "risk_state": "HALTED_DEGRADATION",
            "halt_reason": "lane_realized_loser_limit_per_session",
            "unblock_action": "Clear Risk Halts, then Resume Entries",
            "realized_losing_trades": 2,
            "catastrophic_open_loss_threshold": "-500",
            "position_side": "LONG",
            "broker_position_qty": 1,
            "internal_position_qty": 1,
            "entry_price": "100.0",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "mgc_asia_early_pause_resume_short",
            "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
            "symbol": "MGC",
            "session_restriction": "ASIA_EARLY",
            "approved_short_entry_sources": ["asiaEarlyPauseResumeShortTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-500",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "pl_us_late_pause_resume_long",
            "display_name": "PL / usLatePauseResumeLongTurn",
            "symbol": "PL",
            "session_restriction": "US_LATE",
            "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-1000",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "symbol": "GC",
            "session_restriction": "ASIA_EARLY",
            "approved_long_entry_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-750",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
    ]
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-18T14:05:00-04:00",
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": False,
                "operator_halt": True,
                "approved_long_entry_sources": [
                    "asiaEarlyNormalBreakoutRetestHoldTurn",
                    "usLatePauseResumeLongTurn",
                ],
                "approved_short_entry_sources": [
                    "asiaEarlyPauseResumeShortTurn",
                ],
                "desk_risk_state": "HALT_NEW_ENTRIES",
                "desk_risk_reason": "desk_halt_new_entries_loss",
                "desk_unblock_action": "Clear Risk Halts, then Resume Entries",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 1,
                    "broker_average_price": "100.0",
                },
                "lanes": admitted_lanes,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "branch_sources.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T14:00:00-04:00",
                        "source": "asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "decision": "allowed",
                    }
                ),
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T13:55:00-04:00",
                        "source": "usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "decision": "blocked",
                        "block_reason": "probationary_long_source_not_allowlisted",
                    }
                ),
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T13:50:00-04:00",
                        "source": "asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "decision": "allowed",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "alerts.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "rule_blocks.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-18T13:55:00-04:00",
                "source": "usLatePauseResumeLongTurn",
                "symbol": "MGC",
                "lane_id": "mgc_us_late_pause_resume_long",
                "block_reason": "daily_pause_condition",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "operator_controls.jsonl").write_text(
        json.dumps(
            {
                "requested_at": "2026-03-18T14:09:00-04:00",
                "applied_at": "2026-03-18T14:09:05-04:00",
                "action": "halt_entries",
                "status": "applied",
                "message": "entries halted for paper runtime",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-18T14:10:00-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "25.0",
                "session_date": "2026-03-18",
                "closed_trade_count": 1,
                "fill_count": 1,
                "order_intent_count": 1,
                "allowed_branch_decisions_by_source": {"asiaEarlyNormalBreakoutRetestHoldTurn": 3},
                "blocked_branch_decisions_by_source": {"usLatePauseResumeLongTurn": 1},
                "fills_by_intent_type": {"BUY_TO_OPEN": 1, "SELL_TO_CLOSE": 1},
                "processed_bars_session": 55,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-18T14:05:00-04:00,2026-03-18T14:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,100.5,5.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_desk_risk_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "session_date": "2026-03-18",
                "desk_risk_state": "HALT_NEW_ENTRIES",
                "session_realized_pnl": "-1600",
                "session_unrealized_pnl": "5",
                "session_total_pnl": "-1595",
                "desk_halt_new_entries_loss": "-1500",
                "desk_flatten_and_halt_loss": "-2500",
                "trigger_reason": "desk_halt_new_entries_loss",
                "unblock_action_required": "Clear Risk Halts, then Resume Entries",
                "reconciliation_clean": True,
                "faulted": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "session_date": "2026-03-18",
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "HALTED_DEGRADATION",
                        "halt_reason": "lane_realized_loser_limit_per_session",
                        "unblock_action": "Clear Risk Halts, then Resume Entries",
                        "realized_losing_trades": 2,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "-40",
                        "session_unrealized_pnl": "5",
                        "session_total_pnl": "-35",
                    },
                    {
                        "lane_id": "mgc_asia_early_pause_resume_short",
                        "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "pl_us_late_pause_resume_long",
                        "display_name": "PL / usLatePauseResumeLongTurn",
                        "symbol": "PL",
                        "session_restriction": "US_LATE",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-1000",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-750",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "desk_halt_new_entries_loss": "-1500",
                "desk_flatten_and_halt_loss": "-2500",
                "lane_realized_loser_limit_per_session": 2,
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "mgc_asia_early_pause_resume_short",
                        "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "short_sources": ["asiaEarlyPauseResumeShortTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "pl_us_late_pause_resume_long",
                        "display_name": "PL / usLatePauseResumeLongTurn",
                        "symbol": "PL",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "catastrophic_open_loss": "-1000",
                    },
                    {
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "session_restriction": "ASIA_EARLY",
                        "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                        "catastrophic_open_loss": "-750",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "paper_risk_events.jsonl").write_text(
        json.dumps(
            {
                "logged_at": "2026-03-18T14:10:00-04:00",
                "lane_id": "DESK",
                "symbol": "DESK",
                "severity": "WATCH",
                "event_code": "DESK_HALT_NEW_ENTRIES_LOSS",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_source": "Direct Schwab /quotes cash-index symbols.",
        "feed_state": "LIVE",
        "feed_label": "INDEX FEED LIVE",
        "updated_at": "2026-03-18T14:10:00-04:00",
        "age_seconds": 0,
        "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        "note": "Direct quote fetch.",
        "diagnostics": {"fetch_state": "SUCCESS", "symbols": []},
        "symbols": [
            {
                "label": "DJIA",
                "name": "Dow Jones",
                "external_symbol": "$DJI",
                "display_symbol": "$DJI",
                "source_type": "cash_index",
                "current_value": "39000.0",
                "absolute_change": "100.0",
                "percent_change": "0.26%",
                "bid": None,
                "ask": None,
                "state": "LIVE",
                "value_state": "LIVE",
                "bid_ask_state": "UNAVAILABLE",
                "bid_state": "UNAVAILABLE",
                "ask_state": "UNAVAILABLE",
                "field_states": {},
                "diagnostic_codes": ["BID_UNAVAILABLE", "ASK_UNAVAILABLE"],
                "note": "Bid/ask unavailable from current payload.",
            },
            {
                "label": "SPX",
                "name": "S&P 500",
                "external_symbol": "$SPX",
                "display_symbol": "$SPX",
                "source_type": "cash_index",
                "current_value": "5100.0",
                "absolute_change": "-10.0",
                "percent_change": "-0.20%",
                "bid": None,
                "ask": None,
                "state": "LIVE",
                "value_state": "LIVE",
                "bid_ask_state": "UNAVAILABLE",
                "bid_state": "UNAVAILABLE",
                "ask_state": "UNAVAILABLE",
                "field_states": {},
                "diagnostic_codes": ["BID_UNAVAILABLE", "ASK_UNAVAILABLE"],
                "note": "Bid/ask unavailable from current payload.",
            },
        ],
    }

    history_dir = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews"
    history_dir.mkdir(parents=True, exist_ok=True)
    prior_close_review = {
        "generated_at": "2026-03-17T21:00:00+00:00",
        "session_date": "2026-03-17",
        "desk_close_verdict": "CLEAN_WITH_ACTIVITY",
        "review_required_lanes": [
            "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        ],
        "rows": [
            {
                "branch": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "evidence_chain_status": "PARTIAL",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "FILLED_AND_FLAT",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_LOW",
                "attribution_gap_reason": [
                    "FAMILY_TAGGED_BLOTTER_ONLY",
                    "MULTI_LANE_SAME_FAMILY_AMBIGUITY",
                ],
            },
            {
                "branch": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "evidence_chain_status": "BROKEN",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "SIGNAL_NO_FILL",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": ["INSUFFICIENT_PERSISTED_EVIDENCE"],
            },
            {
                "branch": "PL / usLatePauseResumeLongTurn",
                "evidence_chain_status": "COMPLETE",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "IDLE",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": [],
            },
            {
                "branch": "MGC / asiaEarlyPauseResumeShortTurn",
                "evidence_chain_status": "COMPLETE",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "IDLE",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": [],
            },
        ],
    }
    (history_dir / "2026-03-17_2026-03-17T21-00-00p00-00.json").write_text(
        json.dumps(prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17_2026-03-17T21-00-00p00-00.md").write_text(
        "# prior close review\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17.json").write_text(
        json.dumps(prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17.md").write_text(
        "# prior close review canonical\n",
        encoding="utf-8",
    )
    duplicated_prior_close_review = dict(prior_close_review)
    duplicated_prior_close_review["generated_at"] = "2026-03-17T22:00:00+00:00"
    (history_dir / "2026-03-17_2026-03-17T22-00-00p00-00.json").write_text(
        json.dumps(duplicated_prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17_2026-03-17T22-00-00p00-00.md").write_text(
        "# duplicate prior close review\n",
        encoding="utf-8",
    )
    historical_playback_dir = repo_root / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_test.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_test.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_test.trigger_report.md"
    historical_strategy_study_path = historical_playback_dir / "historical_playback_mgc_test.strategy_study.json"
    historical_strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_test.strategy_study.md"
    historical_summary_path.write_text(
        json.dumps(
            {
                "symbol": "MGC",
                "processed_bars": 1429,
                "run_stamp": "test",
                "primary_standalone_strategy_id": "legacy_runtime__MGC",
                "per_strategy_summaries": [
                    {
                        "standalone_strategy_id": "legacy_runtime__MGC",
                        "strategy_family": "legacy_runtime",
                        "instrument": "MGC",
                        "processed_bars": 1429,
                        "order_intents": 2,
                        "fills": 2,
                        "entries": 1,
                        "exits": 1,
                        "long_entries": 1,
                        "short_entries": 0,
                        "final_position_side": "FLAT",
                        "final_strategy_status": "READY",
                        "realized_pnl": "25.0",
                        "unrealized_pnl": "0",
                        "cumulative_pnl": "25.0",
                        "pnl_unavailable_reason": None,
                    }
                ],
                "aggregate_portfolio_summary": {
                    "standalone_strategy_count": 1,
                    "strategy_count": 1,
                    "standalone_strategy_ids": ["legacy_runtime__MGC"],
                    "processed_bars": 1429,
                    "order_intents": 2,
                    "fills": 2,
                    "entries": 1,
                    "exits": 1,
                    "long_entries": 1,
                    "short_entries": 0,
                    "realized_pnl": "25.0",
                    "unrealized_pnl": "0",
                    "cumulative_pnl": "25.0",
                    "pnl_unavailable_reason": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "side": "LONG",
                    "bars_processed": 1429,
                    "signals_seen": 2,
                    "intents_created": 2,
                    "fills_created": 2,
                    "first_trigger_timestamp": "2026-03-10T16:45:00-04:00",
                    "first_fill_timestamp": "2026-03-10T16:45:00-04:00",
                    "block_or_fault_reason": None,
                },
                {
                    "symbol": "MGC",
                    "lane_family": "asiaEarlyPauseResumeShortTurn",
                    "side": "SHORT",
                    "bars_processed": 1429,
                    "signals_seen": 0,
                    "intents_created": 0,
                    "fills_created": 0,
                    "first_trigger_timestamp": None,
                    "first_fill_timestamp": None,
                    "block_or_fault_reason": "no_trigger_seen",
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text(
        "# Historical Playback\n",
        encoding="utf-8",
    )
    historical_strategy_study_path.write_text(
        json.dumps(
            {
                "contract_version": "strategy_study_v1",
                "symbol": "MGC",
                "timeframe": "5m",
                "standalone_strategy_id": "legacy_runtime__MGC",
                "strategy_family": "legacy_runtime",
                "rows": [
                    {
                        "bar_id": "MGC|5m|2026-03-10T16:45:00-04:00",
                        "timestamp": "2026-03-10T16:45:00-04:00",
                        "start_timestamp": "2026-03-10T16:40:00-04:00",
                        "end_timestamp": "2026-03-10T16:45:00-04:00",
                        "open": "100.0",
                        "high": "101.0",
                        "low": "99.5",
                        "close": "100.5",
                        "session_vwap": "100.2",
                        "atr": "0.8",
                        "position_side": "FLAT",
                        "position_qty": 0,
                        "position_phase": None,
                        "strategy_status": "READY",
                        "transition_label": "bar_close",
                        "entry_marker": True,
                        "exit_marker": False,
                        "fill_marker": False,
                        "entry_markers": [{"kind": "intent", "reason_code": "usLatePauseResumeLongTurn"}],
                        "exit_markers": [],
                        "fill_markers": [],
                        "realized_pnl": "0",
                        "unrealized_pnl": "0",
                        "cumulative_realized_pnl": "0",
                        "cumulative_total_pnl": "0",
                        "current_bias_state": "LONG_BIAS",
                        "current_pullback_state": "NORMAL_PULLBACK",
                        "pullback_envelope_band": "STANDARD",
                        "pullback_depth_score": 0.82,
                        "pullback_violence_score": 0.44,
                        "entry_eligible": True,
                        "entry_blocked": False,
                        "blocker_code": None,
                        "legacy_entry_eligible": True,
                        "legacy_entry_blocked": False,
                        "legacy_blocker_code": None,
                        "latest_signal_side": "LONG",
                        "latest_signal_source": "usLatePauseResumeLongTurn",
                        "latest_signal_state": "LONG_INTENT_CREATED",
                        "legacy_latest_signal_side": "LONG",
                        "legacy_latest_signal_source": "usLatePauseResumeLongTurn",
                        "legacy_latest_signal_state": "LONG_INTENT_CREATED",
                        "continuation_state": "CONTINUATION_TRIGGER_CONFIRMED",
                        "atp_entry_state": "ENTRY_ELIGIBLE",
                        "atp_entry_ready": True,
                        "atp_entry_blocked": False,
                        "atp_entry_blocker_code": None,
                        "atp_timing_state": "ATP_TIMING_CONFIRMED",
                        "atp_timing_confirmed": True,
                        "atp_timing_executable": True,
                        "atp_timing_blocker_code": None,
                        "atp_blocker_code": None,
                        "atp_timing_bar_timestamp": "2026-03-10T16:45:00-04:00",
                        "vwap_entry_quality_state": "VWAP_FAVORABLE",
                        "entry_source_family": "usLatePauseResumeLongTurn",
                    },
                    {
                        "bar_id": "MGC|5m|2026-03-10T16:50:00-04:00",
                        "timestamp": "2026-03-10T16:50:00-04:00",
                        "start_timestamp": "2026-03-10T16:45:00-04:00",
                        "end_timestamp": "2026-03-10T16:50:00-04:00",
                        "open": "100.5",
                        "high": "101.5",
                        "low": "100.2",
                        "close": "101.1",
                        "session_vwap": "100.6",
                        "atr": "0.8",
                        "position_side": "LONG",
                        "position_qty": 1,
                        "position_phase": None,
                        "strategy_status": "READY",
                        "transition_label": "bar_close",
                        "entry_marker": False,
                        "exit_marker": False,
                        "fill_marker": True,
                        "entry_markers": [],
                        "exit_markers": [],
                        "fill_markers": [{"kind": "fill", "is_entry": True, "is_exit": False}],
                        "realized_pnl": "0",
                        "unrealized_pnl": "6.0",
                        "cumulative_realized_pnl": "0",
                        "cumulative_total_pnl": "6.0",
                        "current_bias_state": "LONG_BIAS",
                        "current_pullback_state": "NO_PULLBACK",
                        "pullback_envelope_band": "SHALLOW",
                        "pullback_depth_score": 0.0,
                        "pullback_violence_score": 0.0,
                        "entry_eligible": False,
                        "entry_blocked": False,
                        "blocker_code": None,
                        "legacy_entry_eligible": False,
                        "legacy_entry_blocked": False,
                        "legacy_blocker_code": None,
                        "latest_signal_side": None,
                        "latest_signal_source": None,
                        "latest_signal_state": "NO_SIGNAL",
                        "legacy_latest_signal_side": None,
                        "legacy_latest_signal_source": None,
                        "legacy_latest_signal_state": "NO_SIGNAL",
                        "continuation_state": "CONTINUATION_TRIGGER_UNAVAILABLE",
                        "atp_entry_state": "ENTRY_BLOCKED",
                        "atp_entry_ready": False,
                        "atp_entry_blocked": True,
                        "atp_entry_blocker_code": "ATP_NO_PULLBACK",
                        "atp_timing_state": None,
                        "atp_timing_confirmed": None,
                        "atp_timing_executable": None,
                        "atp_timing_blocker_code": None,
                        "atp_blocker_code": "ATP_NO_PULLBACK",
                        "atp_timing_bar_timestamp": None,
                        "vwap_entry_quality_state": None,
                        "entry_source_family": "usLatePauseResumeLongTurn",
                    },
                ],
                "summary": {
                    "bar_count": 2,
                    "total_trades": 1,
                    "long_trades": 1,
                    "short_trades": 0,
                    "winners": 1,
                    "losers": 0,
                    "cumulative_realized_pnl": "25.0",
                    "cumulative_total_pnl": "25.0",
                    "max_run_up": "25.0",
                    "max_drawdown": "0",
                    "most_common_blocker_codes": [],
                    "most_common_legacy_blocker_codes": [],
                    "no_trade_regions": [],
                    "session_level_behavior": [],
                    "atp_summary": {
                        "available": True,
                        "timing_available": True,
                        "bar_count": 2,
                        "ready_bar_count": 1,
                        "bias_state_percent": {"LONG_BIAS": 100.0},
                        "pullback_state_percent": {"NORMAL_PULLBACK": 50.0, "NO_PULLBACK": 50.0},
                        "entry_state_percent": {"ENTRY_BLOCKED": 50.0, "ENTRY_ELIGIBLE": 50.0},
                        "continuation_state_percent": {"CONTINUATION_TRIGGER_CONFIRMED": 50.0, "CONTINUATION_TRIGGER_UNAVAILABLE": 50.0},
                        "timing_state_percent": {"ATP_TIMING_CONFIRMED": 100.0},
                        "vwap_entry_quality_state_percent": {"VWAP_FAVORABLE": 100.0},
                        "ready_to_timing_confirmed_percent": 100.0,
                        "timing_confirmed_to_executed_percent": 100.0,
                        "ready_to_executed_percent": 100.0,
                        "top_atp_blocker_codes": [{"code": "ATP_NO_PULLBACK", "count": 1}],
                        "top_no_trade_reasons": [{"code": "ATP_NO_PULLBACK", "count": 1}],
                    },
                    "pnl_supportable": True,
                    "pnl_unavailable_reason": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_strategy_study_md_path.write_text(
        "# Strategy Study\n",
        encoding="utf-8",
    )
    (historical_playback_dir / "historical_playback_test.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "test",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 1429,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                        "strategy_study_json_path": str(historical_strategy_study_path),
                        "strategy_study_markdown_path": str(historical_strategy_study_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot = service.snapshot()

    assert snapshot["global"]["mode"] == "IDLE"
    assert snapshot["paper"]["status"]["health_status"] == "HEALTHY"
    assert snapshot["paper"]["position"]["average_price"] == "100.0"
    assert snapshot["paper"]["position"]["realized_pnl"] == "25.0"
    assert snapshot["paper"]["latest_blotter_rows"][0]["setup_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["summary_available"] is True
    assert snapshot["paper"]["readiness"]["runtime_running"] is False
    assert snapshot["paper"]["readiness"]["runtime_phase"] == "STOPPED"
    assert snapshot["paper"]["readiness"]["entries_enabled"] is False
    assert snapshot["paper"]["readiness"]["approved_models_active"] == 5
    assert snapshot["paper"]["readiness"]["approved_models_total"] == 5
    assert "PL / usLatePauseResumeLongTurn / US_LATE" in snapshot["paper"]["readiness"]["instrument_scope"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn / ASIA_EARLY" in snapshot["paper"]["readiness"]["instrument_scope"]
    assert snapshot["paper"]["readiness"]["desk_risk_state"] == "HALT_NEW_ENTRIES"
    assert snapshot["paper"]["readiness"]["desk_risk_reason"] == "desk_halt_new_entries_loss"
    assert snapshot["paper"]["readiness"]["desk_unblock_action"] == "Clear Risk Halts, then Resume Entries"
    assert snapshot["paper"]["readiness"]["session_total_pnl"] == "-1595"
    lane_risk_rows = {
        row["lane_id"]: row
        for row in snapshot["paper"]["readiness"]["lane_risk_rows"]
    }
    assert lane_risk_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["risk_state"] == "HALTED_DEGRADATION"
    assert lane_risk_rows["pl_us_late_pause_resume_long"]["risk_state"] == "OK"
    assert lane_risk_rows["gc_asia_early_normal_breakout_retest_hold_long"]["risk_state"] == "OK"
    assert snapshot["paper"]["readiness"]["latest_paper_fill_timestamp"] == "2026-03-18T14:05:00-04:00"
    approved_rows = {
        row["branch"]: row
        for row in snapshot["paper"]["approved_models"]["rows"]
    }
    assert snapshot["paper"]["approved_models"]["enabled_count"] == 5
    assert snapshot["paper"]["approved_models"]["total_count"] == 5
    assert snapshot["paper"]["approved_models"]["instrument_scope"] == "5 shared paper lanes / multi-lane paper mode"
    assert set(approved_rows) == {
        "MGC / usLatePauseResumeLongTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyPauseResumeShortTurn",
        "PL / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    assert approved_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["last_intent"] == "2026-03-18T14:00:00-04:00"
    assert approved_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "FILLED_OPEN"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["state"] == "ENABLED"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["chain_state"] == "BLOCKED"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["decision_count"] == 1
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["state"] == "ENABLED"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["instrument"] == "PL"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["session_restriction"] == "US_LATE"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["chain_state"] == "NO_SIGNAL"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["state"] == "ENABLED"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["instrument"] == "GC"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_restriction"] == "ASIA_EARLY"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "DECISION_WITHOUT_INTENT"
    assert approved_rows["MGC / asiaEarlyPauseResumeShortTurn"]["state"] == "ENABLED"
    assert snapshot["paper"]["approved_models"]["default_branch"] == "MGC / asiaEarlyNormalBreakoutRetestHoldTurn"
    asia_detail = snapshot["paper"]["approved_models"]["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert asia_detail["open_position"] is True
    assert asia_detail["chain_state"] == "FILLED_OPEN"
    assert asia_detail["unrealized_pnl"] == "5"
    assert asia_detail["latest_activity_type"] == "FILL"
    assert asia_detail["event_trail"][0]["category"] in {"position", "control", "trade", "fill", "intent", "signal"}
    blocked_detail = snapshot["paper"]["approved_models"]["details_by_branch"]["MGC / usLatePauseResumeLongTurn"]
    assert blocked_detail["chain_state"] == "BLOCKED"
    assert blocked_detail["latest_activity_type"] == "BLOCK"
    assert blocked_detail["latest_blocked_reason"] in {"daily_pause_condition", "probationary_long_source_not_allowlisted"}
    assert blocked_detail["latest_eligible_timestamp"] is None
    assert blocked_detail["top_blockers"][0]["code"] in {"daily_pause_condition", "probationary_long_source_not_allowlisted"}
    assert snapshot["paper"]["approved_models"]["details_by_branch"]["PL / usLatePauseResumeLongTurn"]["chain_state"] == "NO_SIGNAL"
    gc_detail = snapshot["paper"]["approved_models"]["details_by_branch"]["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert gc_detail["chain_state"] == "DECISION_WITHOUT_INTENT"
    assert gc_detail["latest_eligible_timestamp"] == "2026-03-18T13:50:00-04:00"
    assert gc_detail["atp_timing_state"] is None
    assert snapshot["paper"]["approved_models"]["out_of_scope_blocked_count"] == 0
    assert snapshot["paper"]["activity_proof"]["verdict"] == "PAPER DESK NOT ACTUALLY RUNNING / NOT POLLING"
    assert snapshot["paper"]["activity_proof"]["session_summary"]["approved_models_seen_count"] == 3
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_signals_count"] == 2
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_blocked_count"] == 1
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_decisions_count"] == 3
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_intents_count"] == 1
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_fills_count"] == 1
    lane_activity_rows = {
        row["branch"]: row
        for row in snapshot["paper"]["lane_activity"]["rows"]
    }
    operator_surface_rows = snapshot["operator_surface"]["lane_rows"]
    assert any(row["classification_tag"] == "admitted_paper" for row in operator_surface_rows)
    assert snapshot["operator_surface"]["readiness"]["title"] == "Runtime / Readiness"
    assert snapshot["operator_surface"]["daily_risk"]["title"] == "Daily Risk / Performance"
    assert snapshot["operator_surface"]["lane_universe"]["title"] == "Unified Active Lane / Instrument Surface"
    assert snapshot["operator_surface"]["context"]["title"] == "Secondary Context"
    assert snapshot["paper"]["lane_activity"]["summary"]["any_activity_today"] is True
    assert snapshot["paper"]["lane_activity"]["summary"]["idle_only_count"] == 2
    assert snapshot["paper"]["lane_activity"]["summary"]["blocked_count"] == 1
    assert snapshot["paper"]["lane_activity"]["summary"]["filled_count"] == 1
    assert snapshot["paper"]["lane_activity"]["summary"]["open_now_count"] == 1
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["verdict"] == "HALTED_BY_RISK"
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["filled"] is True
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert lane_activity_rows["MGC / usLatePauseResumeLongTurn"]["verdict"] == "BLOCKED"
    assert lane_activity_rows["MGC / usLatePauseResumeLongTurn"]["blocked"] is True
    assert lane_activity_rows["MGC / usLatePauseResumeLongTurn"]["top_blockers"][0]["code"] in {"daily_pause_condition", "probationary_long_source_not_allowlisted"}
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["verdict"] == "NO_ACTIVITY_YET"
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["filled"] is False
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["blocked"] is False
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["verdict"] == "SIGNAL_ONLY"
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["atp_timing_state"] is None
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["filled"] is False
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["has_signal_or_decision"] is True
    assert "branch_sources.jsonl" in lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["used_sources"]
    assert "fills" not in lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["used_sources"]
    assert snapshot["paper"]["exceptions"]["session_verdict"] == "NEEDS_OPERATOR_REVIEW"
    assert snapshot["paper"]["exceptions"]["summary"]["owning_model"] == "MGC / asiaEarlyNormalBreakoutRetestHoldTurn"
    assert {row["code"] for row in snapshot["paper"]["exceptions"]["exceptions"]} >= {
        "OPEN_EXPOSURE_AFTER_RESTART_REQUIRES_REVIEW",
        "OPEN_EXPOSURE_WHILE_ENTRIES_HALTED",
    }
    assert snapshot["paper"]["entry_eligibility"]["verdict"] == "NOT ELIGIBLE: OPEN-RISK / REVIEW REQUIRED"
    assert snapshot["paper"]["entry_eligibility"]["clear_action"] == "Clear Risk Halts, then Resume Entries"
    assert snapshot["paper"]["entry_eligibility"]["approved_models_eligible_now"] is False
    assert any(
        row["label"] == "Runtime phase" and row["value"] == "STOPPED"
        for row in snapshot["paper"]["entry_eligibility"]["reasons"]
    )
    assert snapshot["paper"]["soak_session"]["models_signaled"] == [
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / usLatePauseResumeLongTurn",
    ]
    assert snapshot["paper"]["soak_session"]["models_filled"] == ["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert snapshot["paper"]["soak_session"]["models_open_now"] == ["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert snapshot["paper"]["soak_session"]["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    close_review = snapshot["paper_session_close_review"]
    assert close_review["desk_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert close_review["admitted_lanes_count"] == 5
    assert close_review["active_lanes_count"] == 3
    assert close_review["blocked_lanes_count"] == 1
    assert close_review["filled_lanes_count"] == 1
    assert close_review["open_lanes_count"] == 1
    assert close_review["total_attributable_realized_pnl"] == "0"
    assert close_review["realized_attribution_coverage"] == "0/5 lanes exact"
    assert close_review["desk_attribution_summary"]["exact_realized_attribution_count"] == 0
    assert close_review["desk_attribution_summary"]["partial_realized_attribution_count"] == 0
    assert close_review["desk_attribution_summary"]["unattributable_realized_attribution_count"] == 5
    assert close_review["desk_attribution_summary"]["exact_open_risk_ownership_count"] == 1
    assert close_review["desk_attribution_summary"]["ambiguous_open_risk_ownership_count"] == 0
    assert close_review["desk_attribution_summary"]["unattributed_realized_pnl_present"] is True
    assert close_review["desk_attribution_summary"]["desk_review_confidence"] == "LOW"
    assert close_review["desk_attribution_summary"]["desk_pnl_completeness"] == "PARTIAL"
    assert close_review["desk_attribution_summary"]["reliable_pnl_judgment_lanes"] == []
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["manual_pnl_inspection_lanes"]
    assert "PL / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert "MGC / asiaEarlyPauseResumeShortTurn" in close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["partial_evidence_chain_lanes"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["broken_evidence_chain_lanes"]
    assert "MGC / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["broken_evidence_chain_lanes"]
    assert close_review["desk_attribution_summary"]["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert close_review["desk_attribution_summary"]["desk_history_confidence"] == "MEDIUM"
    assert close_review["desk_attribution_summary"]["history_threshold_note"] == "Clean history judgment requires at least 3 prior archived close reviews per lane."
    assert "PL / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["lanes_with_insufficient_history"]
    assert close_review["desk_attribution_summary"]["lanes_with_sufficient_history"] == []
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_partial_chain_lanes"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_broken_chain_lanes"]
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_unattributable_realized_lanes"]
    assert close_review["desk_attribution_summary"]["top_attribution_gap_reasons"][0]["reason"] in {
        "FAMILY_TAGGED_BLOTTER_ONLY",
        "INSUFFICIENT_PERSISTED_EVIDENCE",
        "MULTI_LANE_SAME_FAMILY_AMBIGUITY",
    }
    assert close_review["history_summary"]["prior_reviews_count"] == 1
    assert close_review["history_summary"]["desk_history_confidence"] == "MEDIUM"
    assert close_review["history_summary"]["history_threshold_note"] == "Clean history judgment requires at least 3 prior archived close reviews per lane."
    assert "PL / usLatePauseResumeLongTurn" in close_review["history_summary"]["lanes_with_insufficient_history"]
    close_rows = {row["branch"]: row for row in close_review["rows"]}
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_verdict"] == "HALTED_BY_RISK"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_realized_pnl"] is None
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unattributed_realized_pnl_present"] is True
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unrealized_pnl_attribution_status"] == "EXACT"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_unrealized_pnl"] == "5"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_confidence"] == "LOW"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["review_confidence"] == "REVIEW_TRUST_LOW"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_chain_status"] == "PARTIAL"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_intents"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_fills"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_position_rows"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["ambiguous_family_rows"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_first_recommendation"]["label"] == "Position"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sessions_found"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["minimum_history_threshold_for_clean_judgment"] == 3
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["partial_chain_close_count"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_verdict"] == "WATCH_REPEAT_PARTIAL"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_confidence"] == "MEDIUM"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["last_partial_close_ts"] == "2026-03-17T21:00:00+00:00"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_links"]["blotter"] == "/api/operator-artifact/paper-latest-blotter"
    assert "persisted current position" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unrealized_attribution_evidence_summary"]
    assert "FAMILY_TAGGED_BLOTTER_ONLY" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_gap_reason"]
    assert "MULTI_LANE_SAME_FAMILY_AMBIGUITY" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_gap_reason"]
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["session_verdict"] == "BLOCKED_ONLY"
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["evidence_chain_status"] == "BROKEN"
    assert "INSUFFICIENT_PERSISTED_EVIDENCE" in close_rows["MGC / usLatePauseResumeLongTurn"]["attribution_gap_reason"]
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["open_first_recommendation"]["label"] == "Decisions"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_verdict"] == "SIGNAL_NO_FILL"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_realized_pnl"] is None
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_chain_status"] == "BROKEN"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["missing_lane_links"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_first_recommendation"]["label"] == "Decisions"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sessions_found"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["broken_chain_close_count"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_verdict"] == "WATCH_REPEAT_BROKEN"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_confidence"] == "MEDIUM"
    assert "No realized P/L is attributable" in close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_attribution_evidence_summary"]
    assert close_rows["PL / usLatePauseResumeLongTurn"]["session_verdict"] == "IDLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["unrealized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["evidence_chain_status"] == "COMPLETE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_sessions_found"] == 1
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["PL / usLatePauseResumeLongTurn"]["repeat_review_verdict"] == "NO_REPEAT_ISSUE_SEEN"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["repeat_review_confidence"] == "LOW"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_note"] == "No repeat issue seen yet, but history is still sparse (1/3 archived close reviews)."
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["fill_count"] == 0
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["review_required_lanes"]
    assert snapshot["paper"]["performance"]["realized_pnl"] == "25.0"
    assert snapshot["paper"]["performance"]["fill_count"] == 1
    assert snapshot["paper"]["performance"]["session_metrics"]["processed_bars"] == 55
    assert snapshot["paper"]["performance"]["session_metrics"]["signals_generated"] == 3
    assert snapshot["paper"]["performance"]["branch_performance"][0]["branch"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["performance"]["recent_trades"][0]["source"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["session_shape"]["session_start"] == "2026-03-18T14:00:00-04:00"
    assert snapshot["paper"]["session_shape"]["intraday_high_pnl"] == "10.0"
    assert snapshot["paper"]["session_shape"]["intraday_low_pnl"] == "0"
    assert snapshot["paper"]["session_shape"]["shape_label"] == "Steady up"
    assert snapshot["paper"]["session_shape"]["close_location"] == "Closed near highs"
    assert snapshot["paper"]["session_shape"]["current_or_latest_pnl"] == "10.0"
    assert snapshot["paper"]["session_shape"]["path_points"][-1]["kind"] == "current_open_estimate"
    assert snapshot["paper"]["branch_session_contribution"]["top_contributor"]["branch"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["branch_session_contribution"]["rows"][0]["total_contribution"] == "10.0"
    assert snapshot["paper"]["branch_session_contribution"]["rows"][0]["timing_hint"] == "Late contributor"
    assert any(event["title"] == "Session Start" for event in snapshot["paper"]["session_event_timeline"]["events"])
    assert any(event["category"] == "branch" for event in snapshot["paper"]["session_event_timeline"]["events"])
    assert snapshot["market_context"]["feed_state"] == "LIVE"
    assert snapshot["market_context"]["symbols"][0]["label"] == "DJIA"
    assert snapshot["review"]["paper"]["links"]["json"] == "/api/summary/paper/json"
    assert snapshot["manual_controls"]["controls"][3]["action"] == "paper-halt-entries"
    assert snapshot["paper_operator_state"]["entries_enabled"] is False
    assert snapshot["paper_operator_state"]["flatten_state"] == "idle"
    assert snapshot["paper_closeout"]["summary_generated"] is True
    assert snapshot["paper_closeout"]["position_flat"] is False
    assert snapshot["paper_closeout"]["sign_off_available"] is False
    assert snapshot["paper_carry_forward"]["active"] is False
    assert snapshot["paper_pre_session_review"]["ready_for_run"] is True
    assert snapshot["paper_continuity"]["entries"][0]["kind"] == "prior_close"
    assert snapshot["action_log"] == []
    session_shape_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_shape_snapshot.json"
    assert session_shape_path.exists()
    written_shape = json.loads(session_shape_path.read_text(encoding="utf-8"))
    assert written_shape["shape_label"] == snapshot["paper"]["session_shape"]["shape_label"]
    assert service.operator_artifact_file("paper-session-shape")[0] == session_shape_path
    branch_contrib_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_branch_contribution_snapshot.json"
    assert branch_contrib_path.exists()
    written_branch_contrib = json.loads(branch_contrib_path.read_text(encoding="utf-8"))
    assert written_branch_contrib["top_contributor"]["branch"] == snapshot["paper"]["branch_session_contribution"]["top_contributor"]["branch"]
    assert service.operator_artifact_file("paper-session-branch-contribution")[0] == branch_contrib_path
    session_timeline_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_event_timeline_snapshot.json"
    assert session_timeline_path.exists()
    written_timeline = json.loads(session_timeline_path.read_text(encoding="utf-8"))
    assert any(event["title"] == "Session Start" for event in written_timeline["events"])
    assert service.operator_artifact_file("paper-session-event-timeline")[0] == session_timeline_path
    market_index_path = repo_root / "outputs" / "operator_dashboard" / "market_index_strip_snapshot.json"
    assert market_index_path.exists()
    written_market_index = json.loads(market_index_path.read_text(encoding="utf-8"))
    assert written_market_index["feed_state"] == "LIVE"
    assert service.operator_artifact_file("market-index-strip")[0] == market_index_path
    market_index_diag_path = repo_root / "outputs" / "operator_dashboard" / "market_index_strip_diagnostics.json"
    assert market_index_diag_path.exists()
    assert service.operator_artifact_file("market-index-strip-diagnostics")[0] == market_index_diag_path
    paper_readiness_path = repo_root / "outputs" / "operator_dashboard" / "paper_readiness_snapshot.json"
    assert paper_readiness_path.exists()
    assert service.operator_artifact_file("paper-readiness")[0] == paper_readiness_path
    paper_approved_models_path = repo_root / "outputs" / "operator_dashboard" / "paper_approved_models_snapshot.json"
    assert paper_approved_models_path.exists()
    written_approved_models = json.loads(paper_approved_models_path.read_text(encoding="utf-8"))
    assert written_approved_models["enabled_count"] == 5
    assert written_approved_models["instrument_scope"] == "5 shared paper lanes / multi-lane paper mode"
    assert written_approved_models["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "FILLED_OPEN"
    assert "PL / usLatePauseResumeLongTurn" in written_approved_models["details_by_branch"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in written_approved_models["details_by_branch"]
    assert service.operator_artifact_file("paper-approved-models")[0] == paper_approved_models_path
    paper_lane_activity_path = repo_root / "outputs" / "operator_dashboard" / "paper_lane_activity_snapshot.json"
    assert paper_lane_activity_path.exists()
    written_lane_activity = json.loads(paper_lane_activity_path.read_text(encoding="utf-8"))
    assert written_lane_activity["summary"]["blocked_count"] == 1
    assert written_lane_activity["summary"]["filled_count"] == 1
    assert service.operator_artifact_file("paper-lane-activity")[0] == paper_lane_activity_path
    paper_tracked_strategies_path = repo_root / "outputs" / "operator_dashboard" / "paper_tracked_strategies_snapshot.json"
    assert paper_tracked_strategies_path.exists()
    written_tracked = json.loads(paper_tracked_strategies_path.read_text(encoding="utf-8"))
    assert written_tracked["rows"][0]["strategy_id"] == "atp_companion_v1_asia_us"
    assert written_tracked["rows"][0]["internal_label"] == "ATP_COMPANION_V1_ASIA_US"
    assert service.operator_artifact_file("paper-tracked-strategies")[0] == paper_tracked_strategies_path
    paper_tracked_details_path = repo_root / "outputs" / "operator_dashboard" / "paper_tracked_strategy_details_snapshot.json"
    assert paper_tracked_details_path.exists()
    assert service.operator_artifact_file("paper-tracked-strategy-details")[0] == paper_tracked_details_path
    paper_exceptions_path = repo_root / "outputs" / "operator_dashboard" / "paper_exceptions_snapshot.json"
    assert paper_exceptions_path.exists()
    written_exceptions = json.loads(paper_exceptions_path.read_text(encoding="utf-8"))
    assert written_exceptions["session_verdict"] == "NEEDS_OPERATOR_REVIEW"
    assert service.operator_artifact_file("paper-exceptions")[0] == paper_exceptions_path
    paper_soak_session_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_session_snapshot.json"
    assert paper_soak_session_path.exists()
    written_soak_session = json.loads(paper_soak_session_path.read_text(encoding="utf-8"))
    assert written_soak_session["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    assert service.operator_artifact_file("paper-soak-session")[0] == paper_soak_session_path
    paper_close_review_latest_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_review_latest.json"
    paper_close_review_latest_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_review_latest.md"
    paper_close_review_archive_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "2026-03-18.json"
    paper_close_review_archive_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "2026-03-18.md"
    paper_close_review_history_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "history_index.json"
    paper_close_review_history_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "history_index.md"
    timestamped_close_review_archives = sorted((repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews").glob("2026-03-18_*.json"))
    assert paper_close_review_latest_json.exists()
    assert paper_close_review_latest_md.exists()
    assert paper_close_review_archive_json.exists()
    assert paper_close_review_archive_md.exists()
    assert paper_close_review_history_json.exists()
    assert paper_close_review_history_md.exists()
    assert timestamped_close_review_archives
    written_close_review = json.loads(paper_close_review_latest_json.read_text(encoding="utf-8"))
    assert written_close_review["desk_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert written_close_review["desk_attribution_summary"]["desk_review_confidence"] == "LOW"
    assert written_close_review["desk_attribution_summary"]["desk_pnl_completeness"] == "PARTIAL"
    assert "PL / usLatePauseResumeLongTurn" in written_close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert written_close_review["desk_attribution_summary"]["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert written_close_review["desk_attribution_summary"]["desk_history_confidence"] == "MEDIUM"
    assert written_close_review["rows"][0]["branch"] in {
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    written_close_review_md = paper_close_review_latest_md.read_text(encoding="utf-8")
    assert "Desk review confidence: LOW" in written_close_review_md
    assert "Desk history confidence: MEDIUM" in written_close_review_md
    assert "history_sufficiency=HISTORY_SPARSE" in written_close_review_md
    assert "repeat_review_confidence=LOW" in written_close_review_md
    assert "Complete evidence chains:" in written_close_review_md
    assert "Broken evidence chains:" in written_close_review_md
    assert "gap_reason=FAMILY_TAGGED_BLOTTER_ONLY, MULTI_LANE_SAME_FAMILY_AMBIGUITY" in written_close_review_md
    assert "evidence_chain=PARTIAL" in written_close_review_md
    assert "open_first=Decisions" in written_close_review_md
    written_history = json.loads(paper_close_review_history_json.read_text(encoding="utf-8"))
    assert written_history["prior_reviews_count"] == 1
    assert written_history["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert service.operator_artifact_file("paper-session-close-review")[0] == paper_close_review_latest_json
    assert service.operator_artifact_file("paper-session-close-review-md")[0] == paper_close_review_latest_md
    assert service.operator_artifact_file("paper-session-close-review-history")[0] == paper_close_review_history_json
    assert service.operator_artifact_file("paper-session-close-review-history-md")[0] == paper_close_review_history_md
    paper_latest_intents_path = repo_root / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json"
    assert paper_latest_intents_path.exists()
    assert service.operator_artifact_file("paper-latest-intents")[0] == paper_latest_intents_path
    paper_latest_blotter_path = repo_root / "outputs" / "operator_dashboard" / "paper_latest_blotter_snapshot.json"
    assert paper_latest_blotter_path.exists()
    assert service.operator_artifact_file("paper-latest-blotter")[0] == paper_latest_blotter_path
    paper_position_state_path = repo_root / "outputs" / "operator_dashboard" / "paper_position_state_snapshot.json"
    assert paper_position_state_path.exists()
    assert service.operator_artifact_file("paper-position-state")[0] == paper_position_state_path
    historical_snapshot_path = repo_root / "outputs" / "operator_dashboard" / "historical_playback_snapshot.json"
    assert historical_snapshot_path.exists()
    historical_payload = snapshot["historical_playback"]["latest_run"]
    assert historical_payload["run_stamp"] == "test"
    assert historical_payload["bars_processed"] == 1429
    assert historical_payload["signals_seen"] == 2
    assert historical_payload["fills_created"] == 2
    assert historical_payload["rows"][0]["lane_family"] == "usLatePauseResumeLongTurn"
    assert historical_payload["rows"][0]["result_status"] == "FIRED"
    assert historical_payload["rows"][1]["result_status"] == "NO FIRE"
    assert historical_payload["truth_label"] == "REPLAY"
    assert historical_payload["replay_summary_available"] is True
    assert historical_payload["primary_standalone_strategy_id"] == "legacy_runtime__MGC"
    assert historical_payload["aggregate_portfolio_summary"]["standalone_strategy_count"] == 1
    assert historical_payload["per_strategy_summaries"][0]["standalone_strategy_id"] == "legacy_runtime__MGC"
    assert historical_payload["strategy_study_available"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["label"] == "Replay Strategy Study"
    assert snapshot["historical_playback"]["strategy_study_status"]["hint"] == (
        "Available after a replay/historical playback run with strategy-study artifacts."
    )
    assert snapshot["historical_playback"]["strategy_study_status"]["run_loaded"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["artifact_found"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["base_timeframe"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["structural_signal_timeframe"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["execution_resolution"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["study_mode"] == "baseline_parity_mode"
    assert snapshot["historical_playback"]["strategy_study_status"]["atp_timing_available"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["mode"] == "ATP_ENHANCED"
    assert historical_payload["strategy_study_status"]["artifact_row_count"] == 2
    assert historical_payload["strategy_study"]["summary"]["bar_count"] == 2
    assert historical_payload["strategy_study"]["meta"]["timeframe_truth"]["artifact_timeframe"] == "5m"
    assert historical_payload["strategy_study"]["summary"]["atp_summary"]["available"] is True
    assert historical_payload["strategy_study"]["summary"]["atp_summary"]["top_atp_blocker_codes"][0]["code"] == "ATP_NO_PULLBACK"
    assert service.operator_artifact_file("historical-playback-snapshot")[0] == historical_snapshot_path
    assert service.operator_artifact_file("historical-playback-manifest")[0] == historical_playback_dir / "historical_playback_test.manifest.json"
    assert service.operator_artifact_file("historical-playback-summary")[0] == historical_summary_path
    assert service.operator_artifact_file("historical-playback-trigger-report")[0] == historical_trigger_report_path
    assert service.operator_artifact_file("historical-playback-trigger-report-md")[0] == historical_trigger_report_md_path
    assert service.operator_artifact_file("historical-playback-strategy-study")[0] == historical_strategy_study_path
    assert service.operator_artifact_file("historical-playback-strategy-study-md")[0] == historical_strategy_study_md_path

    result = service.run_action("capture-paper-soak-evidence")
    assert result["ok"] is True
    latest_json_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_evidence_latest.json"
    latest_md_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_evidence_latest.md"
    assert latest_json_path.exists()
    assert latest_md_path.exists()
    latest_bundle = json.loads(latest_json_path.read_text(encoding="utf-8"))
    assert latest_bundle["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    assert latest_bundle["approved_models_snapshot"]["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert service.operator_artifact_file("paper-soak-evidence-latest-json")[0] == latest_json_path

    service._risk_ack_path.write_text(  # type: ignore[attr-defined]
        json.dumps(
            {
                "risk_hash": snapshot["paper_risk_state"]["risk_hash"],
                "acknowledged_at": "2026-03-18T14:12:00-04:00",
                "reasons": snapshot["paper_risk_state"]["reasons"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    signoff_result = service.run_action("sign-off-paper-session")
    assert signoff_result["ok"] is True
    lane_history_dir = repo_root / "outputs" / "operator_dashboard" / "paper_session_lane_history"
    lane_history_paths = sorted(lane_history_dir.glob("2026-03-18_*.json"))
    assert len(lane_history_paths) == 1
    lane_history = json.loads(lane_history_paths[0].read_text(encoding="utf-8"))
    assert lane_history["session_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert lane_history["admitted_lane_count"] == 5
    assert lane_history["active_lane_count"] == 3
    assert lane_history["filled_lane_count"] == 1
    assert lane_history["open_risk_lane_count"] == 1
    assert lane_history["dirty_close_lane_count"] == 1
    assert lane_history["manual_review_lane_count"] >= 1
    archived_lanes = {row["lane_id"]: row for row in lane_history["lanes"]}
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "MGC"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["fill"] is True
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["open_risk_at_close"] is True
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["clean_vs_dirty_close"] == "DIRTY"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "GC"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["signal"] is True
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["fill"] is False
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["primary_gap_reason"] == "INSUFFICIENT_PERSISTED_EVIDENCE"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["lane_id"] != archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["lane_id"]
    assert len(
        [
            row
            for row in lane_history["lanes"]
            if row["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn" and row["instrument"] in {"MGC", "GC"}
        ]
    ) == 2
    service._archive_paper_session_lane_history(  # type: ignore[attr-defined]
        snapshot=snapshot,
        signoff_payload=json.loads(service._session_signoff_path.read_text(encoding="utf-8")),  # type: ignore[attr-defined]
    )
    lane_history_paths = sorted(lane_history_dir.glob("2026-03-18_*.json"))
    assert len(lane_history_paths) == 2
    assert lane_history_paths[0].name != lane_history_paths[1].name


def test_dashboard_historical_playback_strategy_study_status_marks_missing_artifacts(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    historical_playback_dir = tmp_path / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_missing.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_missing.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_missing.trigger_report.md"
    historical_summary_path.write_text(
        json.dumps({"symbol": "MGC", "processed_bars": 12, "run_stamp": "missing-study"}) + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "bars_processed": 12,
                    "signals_seen": 0,
                    "intents_created": 0,
                    "fills_created": 0,
                    "block_or_fault_reason": "no_trigger_seen",
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text("# Historical Playback\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_missing.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "missing-study",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 12,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._historical_playback_payload()

    assert payload["available"] is True
    assert payload["strategy_study_status"]["run_loaded"] is True
    assert payload["strategy_study_status"]["artifact_found"] is False
    assert payload["strategy_study_status"]["artifact_row_count"] == 0
    assert payload["strategy_study_status"]["base_timeframe"] is None
    assert payload["strategy_study_status"]["atp_timing_available"] is False
    assert payload["strategy_study_status"]["mode"] == "NO_DATA"
    assert payload["latest_run"]["strategy_study_available"] is False
    assert payload["latest_run"]["strategy_study_status"]["mode"] == "NO_DATA"


def test_dashboard_historical_playback_payload_backfills_legacy_strategy_study_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    historical_playback_dir = tmp_path / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_backfill.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_backfill.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_backfill.trigger_report.md"
    historical_strategy_study_path = historical_playback_dir / "historical_playback_mgc_backfill.strategy_study.json"
    historical_strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_backfill.strategy_study.md"
    historical_summary_path.write_text(
        json.dumps(
            {
                "symbol": "MGC",
                "processed_bars": 12,
                "run_stamp": "backfill-study",
                "config_paths": ["config/base.yaml"],
                "source_db_path": str(tmp_path / "source.sqlite3"),
                "replay_db_path": str(tmp_path / "replay.sqlite3"),
                "source_timeframe": "5m",
                "target_timeframe": "5m",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "bars_processed": 12,
                    "signals_seen": 1,
                    "intents_created": 1,
                    "fills_created": 1,
                    "block_or_fault_reason": None,
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text("# Historical Playback\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_backfill-study.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "backfill-study",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 12,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def _fake_backfill(*, summary_path: str | Path, summary_payload: dict[str, object] | None = None) -> tuple[Path, Path]:
        historical_strategy_study_path.write_text(
            json.dumps(
                {
                    "contract_version": "strategy_study_v2",
                    "symbol": "MGC",
                    "timeframe": "5m",
                    "rows": [
                        {
                            "bar_id": "bar-1",
                            "timestamp": "2026-03-18T14:05:00-04:00",
                            "entry_marker": True,
                        }
                    ],
                    "summary": {
                        "bar_count": 1,
                        "atp_summary": {"available": True, "timing_available": False},
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        historical_strategy_study_md_path.write_text("# Strategy Study\n", encoding="utf-8")
        return historical_strategy_study_path, historical_strategy_study_md_path

    monkeypatch.setattr(operator_dashboard_module, "ensure_strategy_study_artifacts", _fake_backfill)

    payload = service._historical_playback_payload()

    assert payload["latest_run"]["strategy_study_available"] is True
    assert payload["selected_study"]["contract_version"] == "strategy_study_v3"
    assert "rows" not in payload["selected_study"]
    assert payload["study_catalog"]["selected_study_key"]
    assert payload["study_catalog"]["items"][0]["contract_version"] == "strategy_study_v3"
    assert payload["study_catalog"]["facets"]["symbols"] == ["MGC"]
    assert payload["study_catalog"]["facets"]["study_modes"] == ["baseline_parity_mode"]
    assert payload["study_catalog"]["facets"]["entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["facets"]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["facets"]["pnl_truth_bases"] == ["BASELINE_FILL_TRUTH"]
    assert payload["study_catalog"]["facets"]["lifecycle_truth_classes"] == ["BASELINE_PARITY_ONLY"]
    assert payload["study_catalog"]["items"][0]["scope_label"] == "Legacy Benchmark"
    assert payload["study_catalog"]["items"][0]["entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert payload["study_catalog"]["items"][0]["active_entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert payload["study_catalog"]["items"][0]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["items"][0]["execution_truth_emitter"] == "baseline_parity_emitter"
    assert payload["study_catalog"]["items"][0]["entry_model_supported"] is True
    assert payload["study_catalog"]["items"][0]["intrabar_execution_authoritative"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_intrabar_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_entry_truth_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_exit_truth_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_trade_lifecycle_available"] is False
    assert payload["study_catalog"]["items"][0]["pnl_truth_basis"] == "BASELINE_FILL_TRUTH"
    assert payload["study_catalog"]["items"][0]["lifecycle_truth_class"] == "BASELINE_PARITY_ONLY"
    assert payload["study_catalog"]["items"][0]["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert payload["latest_run"]["artifact_paths"]["strategy_study_json"] == str(historical_strategy_study_path)
    assert payload["latest_run"]["artifact_paths"]["strategy_study_markdown"] == str(historical_strategy_study_md_path)
    assert payload["latest_run"]["strategy_study"]["summary"]["bar_count"] == 1
    assert payload["latest_run"]["strategy_study_status"]["artifact_found"] is True
    assert payload["latest_run"]["strategy_study_status"]["mode"] == "ATP_ENHANCED"


def test_dashboard_paper_readiness_surfaces_lane_eligibility_rows_and_stale_override(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "mgc_us_late_pause_resume_long",
                        "ibkr_bridge_submit_capable": True,
                        "intent_action": "NO_ACTION",
                    },
                    {
                        "strategy_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "ibkr_bridge_submit_capable": True,
                        "intent_action": "NO_ACTION",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {
            "lanes": [
                {"lane_id": "mgc_us_late_pause_resume_long", "risk_state": "OK"},
                {"lane_id": "mgc_asia_early_normal_breakout_retest_hold_long", "risk_state": "OK"},
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "mgc_us_late_pause_resume_long",
                    "display_name": "MGC / usLatePauseResumeLongTurn",
                    "symbol": "MGC",
                    "session_restriction": "US_LATE",
                },
                {
                    "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                    "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                    "symbol": "MGC",
                    "session_restriction": "ASIA_EARLY",
                },
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "ASIA_EARLY",
            "lanes": [
                {
                    "lane_id": "mgc_us_late_pause_resume_long",
                    "display_name": "MGC / usLatePauseResumeLongTurn",
                    "symbol": "MGC",
                    "session_restriction": "US_LATE",
                    "current_detected_session": "ASIA_EARLY",
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                },
                {
                    "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                    "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                    "symbol": "MGC",
                    "session_restriction": "ASIA_EARLY",
                    "current_detected_session": "ASIA_EARLY",
                    "eligible_now": True,
                    "eligibility_reason": None,
                },
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    rows = {row["lane_id"]: row for row in payload["lane_eligibility_rows"]}
    status_rows = {row["lane_id"]: row for row in payload["lane_status_rows"]}

    assert payload["current_detected_session"] == "ASIA_EARLY"
    assert payload["current_broad_trading_session"] in {"ASIA_EARLY", "UNCLASSIFIED", "UNKNOWN", "LONDON_LATE", "US_EARLY", "US_MIDDAY", "US_LATE"}
    assert rows["mgc_us_late_pause_resume_long"]["eligible_now"] is False
    assert rows["mgc_us_late_pause_resume_long"]["eligibility_reason"] == "wrong_session"
    assert rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is True
    assert rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] == ""
    assert status_rows["mgc_us_late_pause_resume_long"]["loaded_in_runtime"] is True
    assert status_rows["mgc_us_late_pause_resume_long"]["eligible_to_trade"] is False
    assert status_rows["mgc_us_late_pause_resume_long"]["tradability_status"] == "LOADED_NOT_ELIGIBLE"
    assert status_rows["mgc_us_late_pause_resume_long"]["runtime_presence"] == "ACTIVE_RUNTIME"
    assert status_rows["mgc_us_late_pause_resume_long"]["runtime_presence_label"] == "Active Runtime"
    assert status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_to_trade"] is False
    assert status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["tradability_status"] == "LOADED_NOT_ELIGIBLE"
    assert payload["lane_status_summary"]["loaded_in_runtime_count"] == 2
    assert payload["lane_status_summary"]["eligible_to_trade_count"] == 0
    assert payload["lane_status_summary"]["runtime_presence_counts"]["ACTIVE_RUNTIME"] == 2

    paper["status"]["stale"] = True
    stale_payload = service._paper_readiness_payload(paper)
    stale_rows = {row["lane_id"]: row for row in stale_payload["lane_eligibility_rows"]}
    stale_status_rows = {row["lane_id"]: row for row in stale_payload["lane_status_rows"]}

    assert stale_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is False
    assert stale_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] == "stale_runtime"
    assert stale_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["tradability_status"] == "LOADED_NOT_ELIGIBLE"

    paper["status"]["stale"] = False
    paper["raw_operator_status"]["lanes"][1].update(
        {
            "quarantined": True,
            "quarantine_state": "QUARANTINED",
            "quarantine_reason": "Lane startup reconciliation remained unresolved; lane is quarantined fail-closed.",
            "quarantine_reason_code": "paper_startup_reconciliation_failed",
            "quarantine_first_failure_at": "2026-05-18T10:55:00+00:00",
            "quarantine_retry_count": 1,
            "quarantine_last_retry_at": "2026-05-18T10:55:00+00:00",
            "quarantine_operator_action_required": True,
            "startup_reconciliation_classification": "QUARANTINED",
        }
    )
    quarantined_payload = service._paper_readiness_payload(paper)
    quarantined_rows = {row["lane_id"]: row for row in quarantined_payload["lane_eligibility_rows"]}
    quarantined_status_rows = {row["lane_id"]: row for row in quarantined_payload["lane_status_rows"]}

    assert quarantined_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is False
    assert quarantined_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] == "lane_quarantined"
    assert quarantined_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_to_trade"] is False
    assert quarantined_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["quarantined"] is True
    assert quarantined_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["quarantine_reason_code"] == (
        "paper_startup_reconciliation_failed"
    )
    assert quarantined_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["quarantine_operator_action_required"] is True
    assert quarantined_payload["lane_status_summary"]["eligible_to_trade_count"] == 0


def test_decision_bar_seconds_prefers_execution_timeframe_over_context() -> None:
    row = {
        "execution_timeframe": "1m",
        "context_timeframes": ["3m"],
        "primary_context_timeframe": "3m",
    }

    assert operator_dashboard_module._decision_bar_seconds_for_row(row) == 60  # noqa: SLF001


def test_dashboard_paper_readiness_preserves_execution_cadence_metadata_from_approved_models(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_execution_cadence.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-30T03:33:00-04:00",
        processed_bar_end_ts="2026-04-30T03:33:00-04:00",
        feature_bar_ts="2026-04-30T03:33:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_1x_all_lanes__london_early_long",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {
            "rows": [
                {
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "branch": "GC / LONDON_EARLY_LONG / x1",
                    "enabled": True,
                    "instrument": "GC",
                    "execution_timeframe": "1m",
                    "context_timeframes": ["3m"],
                    "session_restriction": "LONDON_EARLY",
                    "runtime_presence": "ACTIVE_RUNTIME",
                    "strategy_status": "READY",
                }
            ]
        },
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_1x_all_lanes__london_early_long", "risk_state": "OK"}]},
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "display_name": "GC / LONDON_EARLY_LONG / x1",
                    "symbol": "GC",
                    "session_restriction": "LONDON_EARLY",
                    "database_url": f"sqlite:///{db_path}",
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "LONDON_EARLY",
            "lanes": [
                {
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "display_name": "GC / LONDON_EARLY_LONG / x1",
                    "symbol": "GC",
                    "current_detected_session": "LONDON_EARLY",
                    "eligible_now": False,
                    "eligibility_reason": "waiting_for_bar_close",
                    "execution_timeframe": None,
                    "context_timeframes": None,
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 30, 3, 34, 5, tzinfo=operator_dashboard_module.NEW_YORK_TZ),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["execution_timeframe"] == "1m"
    assert row["resolved_execution_timeframe"] == "1m"
    assert row["context_timeframes"] == ["3m"]
    assert row["resolved_context_timeframes"] == ["3m"]
    assert row["primary_context_timeframe"] == "3m"
    assert row["decision_bar_seconds"] == 60
    assert row["next_expected_decision_bar_ts"] == "2026-04-30T03:35:00-04:00"
    assert row["waiting_for_completed_bar"] is True
    assert row["bar_state"] == "WAITING_FOR_BAR_CLOSE"


def test_dashboard_paper_readiness_classifies_waiting_for_completed_bar_without_marking_lane_blocked(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "mes_waiting.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="MES",
        observed_completed_bar_end_ts="2026-04-29T10:33:00-04:00",
        processed_bar_end_ts="2026-04-29T10:33:00-04:00",
        feature_bar_ts="2026-04-29T10:33:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "mes_1x_ny_early_core__us_early_long",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mes_1x_ny_early_core__us_early_long", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "mes_1x_ny_early_core__us_early_long",
                    "display_name": "MES / usEarlyLong",
                    "symbol": "MES",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "latest_completed_bar_end_ts": "2026-04-29T10:33:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 5, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["session_eligible"] is True
    assert row["waiting_for_completed_bar"] is True
    assert row["bar_received_not_processed_yet"] is False
    assert row["market_data_stale"] is False
    assert row["bar_state"] == "WAITING_FOR_BAR_CLOSE"
    assert row["eligibility_reason"] == "waiting_for_bar_close"
    assert row["expected_completed_bar_end_ts"] == "2026-04-29T10:34:00-04:00"
    assert row["observed_completed_bar_end_ts"] == "2026-04-29T10:33:00-04:00"
    assert row["blocked_lane"] is False
    assert row["live_capable"] is True
    assert row["fireability_classification"] == "FIREABLE_WAITING_FOR_BAR"
    assert row["tradability_status"] == "WAITING_FOR_NEXT_DECISION_BAR"
    assert payload["lane_status_summary"]["session_eligible_lanes_count"] == 1
    assert payload["lane_status_summary"]["live_capable_count"] == 1
    assert payload["lane_status_summary"]["waiting_for_completed_bar_count"] == 1
    assert payload["bar_received_not_processed_yet_count"] == 0
    assert payload["market_data_stale_count"] == 0
    assert payload["paper_trade_allowed"] is True
    assert payload["paper_trade_block_reason"] is None
    assert payload["lane_status_summary"]["blocked_lanes_count"] == 0
    assert payload["next_expected_decision_bar_ts"] is not None


def test_dashboard_paper_readiness_softens_stale_runtime_when_runtime_is_healthy_and_on_cadence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    db_path = tmp_path / "mnq_waiting.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="MNQ",
        observed_completed_bar_end_ts="2026-04-29T10:33:00-04:00",
        processed_bar_end_ts="2026-04-29T10:33:00-04:00",
        feature_bar_ts="2026-04-29T10:33:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "mnq_1x_ny_early_core__us_early_long",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MNQ", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mnq_1x_ny_early_core__us_early_long", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "mnq_1x_ny_early_core__us_early_long",
                    "display_name": "MNQ / usEarlyLong",
                    "symbol": "MNQ",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "latest_completed_bar_end_ts": "2026-04-29T10:33:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": True},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 5, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["runtime_stale_observed"] is True
    assert row["runtime_stale_suppressed"] is True
    assert row["runtime_stale_effective"] is False
    assert row["eligibility_reason"] == "waiting_for_bar_close"
    assert row["bar_state"] == "WAITING_FOR_BAR_CLOSE"
    assert row["fireability_classification"] == "FIREABLE_WAITING_FOR_BAR"
    assert payload["lane_status_summary"]["stale_runtime_blocked_count"] == 0


def test_dashboard_paper_readiness_classifies_bar_received_not_processed_yet(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_processing_lag.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:33:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_processing_lag",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_processing_lag", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_processing_lag",
                    "display_name": "GC / processingLag",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_state"] == "BAR_RECEIVED_NOT_PROCESSED_YET"
    assert row["eligibility_reason"] == "bar_received_not_processed_yet"
    assert row["waiting_for_completed_bar"] is False
    assert row["bar_received_not_processed_yet"] is True
    assert row["market_data_stale"] is False
    assert row["processing_lag_seconds"] == 60.0
    assert row["fireability_classification"] == "FIREABLE_BLOCKED_PROCESSING_LAG"
    assert row["tradability_status"] == "BAR_RECEIVED_NOT_PROCESSED_YET"
    assert payload["bar_received_not_processed_yet_count"] == 1


def test_dashboard_paper_readiness_classifies_market_data_stale_beyond_grace(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_market_data_stale.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:32:00-04:00",
        processed_bar_end_ts="2026-04-29T10:32:00-04:00",
        feature_bar_ts="2026-04-29T10:32:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_market_data_stale",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_market_data_stale", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_market_data_stale",
                    "display_name": "GC / marketDataStale",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                    "market_data_recovery": {
                        "market_data_recovery_state": "FAILED",
                        "last_recovery_attempt_at": "2026-04-29T14:34:15+00:00",
                        "recovery_attempt_count": 2,
                        "recovery_action": "provider_resubscribe",
                        "recovery_result": "NO_FRESH_BAR_AFTER_RECOVERY",
                        "recovery_root_cause": "SUBSCRIPTION_DROPPED",
                        "affected_symbols": ["GC"],
                        "affected_lanes": ["gc_market_data_stale"],
                        "latest_observed_bar_after_recovery": "2026-04-29T14:32:00+00:00",
                        "recovered": False,
                    },
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_state"] == "MARKET_DATA_STALE"
    assert row["eligibility_reason"] == "market_data_stale"
    assert row["waiting_for_completed_bar"] is False
    assert row["bar_received_not_processed_yet"] is False
    assert row["market_data_stale"] is True
    assert row["market_data_lag_seconds"] == 120.0
    assert row["live_capable"] is False
    assert row["fireability_classification"] == "FIREABLE_BLOCKED_MARKET_DATA"
    assert row["tradability_status"] == "MARKET_DATA_STALE"
    assert row["market_data_recovery_state"] == "FAILED"
    assert row["recovery_action"] == "provider_resubscribe"
    assert row["recovery_root_cause"] == "SUBSCRIPTION_DROPPED"
    assert row["affected_symbols"] == ["GC"]
    assert row["affected_lanes"] == ["gc_market_data_stale"]
    assert row["recovered"] is False
    assert payload["market_data_stale_count"] == 1
    assert payload["paper_trade_allowed"] is False
    assert payload["paper_trade_block_reason"] == "paper_market_data_stale_or_unavailable"
    assert payload["lane_status_summary"]["live_capable_count"] == 0


def test_dashboard_paper_readiness_classifies_bar_authority_unavailable_as_explicit_blocker(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_bar_authority_unavailable",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_bar_authority_unavailable", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_bar_authority_unavailable",
                    "display_name": "GC / barAuthorityUnavailable",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                        "allowed_session_match": True,
                        "eligible_now": True,
                        "eligibility_reason": "no_new_completed_bar",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "execution_timeframe": "1m",
                        "primary_context_timeframe": "3m",
                    }
                ],
            },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["session_eligible"] is True
    assert row["bar_authority_available"] is False
    assert row["bar_authority_unavailable"] is True
    assert row["market_data_stale"] is False
    assert row["live_capable"] is False
    assert row["fireability_classification"] == "FIREABLE_BLOCKED_BAR_AUTHORITY"
    assert row["tradability_status"] == "BAR_AUTHORITY_UNAVAILABLE"
    assert row["first_true_blocker"] == "BAR_AUTHORITY_UNAVAILABLE"
    assert row["blocked_lane"] is True
    assert payload["market_data_stale_count"] == 0
    assert payload["bar_authority_unavailable_count"] == 1
    assert payload["lane_status_summary"]["live_capable_count"] == 0


def test_dashboard_sqlite_path_resolution_falls_back_to_single_duplicate_numbered_lane_db(tmp_path: Path) -> None:
    expected_path = tmp_path / "gc_expected.sqlite3"
    duplicate_path = tmp_path / "gc_expected 2.sqlite3"
    _write_lane_bar_authority_db(
        duplicate_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:34:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
    )

    resolved = operator_dashboard_module._sqlite_path_from_database_url(f"sqlite:///{expected_path}")  # noqa: SLF001

    assert resolved == duplicate_path.resolve()


def test_dashboard_paper_readiness_recovers_bar_authority_from_single_duplicate_numbered_lane_db(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    expected_db_path = tmp_path / "gc_expected.sqlite3"
    duplicate_db_path = tmp_path / "gc_expected 2.sqlite3"
    _write_lane_bar_authority_db(
        duplicate_db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:34:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_duplicate_lane_db",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_duplicate_lane_db", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [{"lane_id": "gc_duplicate_lane_db", "audit_verdict": "NO_SETUP_OBSERVED"}]
        },
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_duplicate_lane_db",
                    "display_name": "GC / duplicateLaneDb",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{expected_db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_authority_available"] is True
    assert row["bar_authority_unavailable"] is False
    assert row["bar_state"] == "READY_NO_SETUP"
    assert row["market_data_stale"] is False
    assert row["live_capable"] is True
    assert row["first_true_blocker"] == "no_setup_observed"
    assert payload["bar_authority_unavailable_count"] == 0
    assert payload["lane_status_summary"]["live_capable_count"] == 1


def test_dashboard_paper_readiness_treats_recently_published_one_bar_behind_feed_as_waiting(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_recent_publication.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:52:00-04:00",
        observed_bar_created_at="2026-04-29T10:52:59-04:00",
        processed_bar_end_ts="2026-04-29T10:52:00-04:00",
        feature_bar_ts="2026-04-29T10:52:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_recent_publication",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_recent_publication", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_recent_publication",
                    "display_name": "GC / recentPublication",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 53, 51, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_state"] == "WAITING_FOR_BAR_CLOSE"
    assert row["eligibility_reason"] == "waiting_for_bar_close"
    assert row["market_data_stale"] is False
    assert row["waiting_for_completed_bar"] is True
    assert row["observed_completed_bar_recorded_at"] == "2026-04-29T10:52:59-04:00"


def test_dashboard_paper_readiness_uses_utc_comparison_anchor_for_lane_db_freshness(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "es_utc_fresh.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="ES",
        execution_timeframe="1m",
        context_timeframe="3m",
        observed_completed_bar_end_ts="2026-05-01T12:54:00+00:00",
        observed_bar_created_at="2026-05-01T12:54:19+00:00",
        observed_data_source="databento_live",
        processed_bar_end_ts="2026-05-01T12:54:00+00:00",
        feature_bar_ts="2026-05-01T12:54:00+00:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "es_utc_fresh",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "es_utc_fresh", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "es_utc_fresh",
                    "display_name": "ES / utcFresh",
                    "symbol": "ES",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 5, 1, 8, 54, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["observed_completed_bar_end_ts"] == "2026-05-01T12:54:00+00:00"
    assert row["feature_bar_ts"] == "2026-05-01T12:54:00+00:00"
    assert row["last_processed_bar_end_ts"] == "2026-05-01T12:54:00+00:00"
    assert row["market_data_stale"] is False
    assert row["bar_state"] == "BAR_PROCESSED_CURRENT"
    assert row["live_capable"] is True


def test_dashboard_paper_readiness_classifies_ready_no_setup_when_latest_observed_bar_is_processed(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_ready_no_setup.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:34:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_ready_no_setup",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_ready_no_setup", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [{"lane_id": "gc_ready_no_setup", "audit_verdict": "NO_SETUP_OBSERVED"}]
        },
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_ready_no_setup",
                    "display_name": "GC / readyNoSetup",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_state"] == "READY_NO_SETUP"
    assert row["eligibility_reason"] == ""
    assert row["no_setup_present"] is True
    assert row["waiting_for_completed_bar"] is False
    assert row["bar_received_not_processed_yet"] is False
    assert row["market_data_stale"] is False
    assert row["live_capable"] is True
    assert row["fireability_classification"] == "FIREABLE_SESSION_ELIGIBLE_NO_SETUP"
    assert row["tradability_status"] == "SESSION_ELIGIBLE_NO_SETUP"
    assert row["latest_fault_or_blocker"] == "no_setup_observed"
    assert payload["lane_status_summary"]["live_capable_count"] == 1


def test_dashboard_paper_readiness_classifies_actionable_when_latest_observed_bar_is_processed_with_signal(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_actionable.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:34:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
        signal_bar_ts="2026-04-29T10:34:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_actionable",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "BUY",
                        "intent_action": "BUY",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_actionable", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [
                {
                    "lane_id": "gc_actionable",
                    "audit_verdict": "ENTRY_READY",
                    "last_signal_timestamp": "2026-04-29T10:34:00-04:00",
                    "last_actionable_signal_timestamp": "2026-04-29T10:34:00-04:00",
                    "last_long_entry": True,
                    "current_bar_signal_id": "signal-gc-actionable-1034",
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_actionable",
                    "display_name": "GC / actionable",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["bar_state"] == "ACTIONABLE"
    assert row["eligibility_reason"] == ""
    assert row["actionable_now"] is True
    assert row["executable_actionable_this_bar"] is True
    assert row["display_candidate_this_bar"] is True
    assert row["current_bar_signal_id"] == "signal-gc-actionable-1034"
    assert row["current_bar_order_intent_id"] is None
    assert row["waiting_for_completed_bar"] is False
    assert row["bar_received_not_processed_yet"] is False
    assert row["market_data_stale"] is False
    assert row["fireability_classification"] == "FIREABLE_ACTIONABLE"
    assert row["tradability_status"] == "ACTIONABLE_NOW"


def test_dashboard_paper_readiness_does_not_treat_display_only_route_summary_as_actionable(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "nq_display_only.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="NQ",
        observed_completed_bar_end_ts="2026-05-01T13:09:00-04:00",
        processed_bar_end_ts="2026-05-01T13:09:00-04:00",
        feature_bar_ts="2026-05-01T13:09:00-04:00",
        signal_bar_ts="2026-05-01T13:09:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "nq_display_only",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "BUY",
                        "intent_action": "BUY",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "NQ", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "nq_display_only", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [
                {
                    "lane_id": "nq_display_only",
                    "audit_verdict": "SURFACING_MISMATCH_SUSPECTED",
                    "last_signal_timestamp": "2026-05-01T13:09:00-04:00",
                    "last_actionable_signal_timestamp": "2026-05-01T12:56:00-04:00",
                    "last_long_entry": False,
                    "last_order_intent_id": None,
                    "last_intent_timestamp": None,
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "US_MIDDAY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "nq_display_only",
                    "display_name": "NQ / displayOnly",
                    "symbol": "NQ",
                    "session_restriction": "US_MIDDAY",
                    "allowed_sessions": ["US_MIDDAY"],
                    "current_detected_session": "US_MIDDAY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 5, 1, 13, 9, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["actionable_now"] is False
    assert row["executable_actionable_this_bar"] is False
    assert row["display_candidate_this_bar"] is True
    assert row["surfacing_mismatch_suspected"] is True
    assert row["current_bar_order_intent_id"] is None
    assert row["current_bar_submit_attempt_id"] is None
    assert row["bar_state"] == "DISPLAY_CANDIDATE_ONLY"
    assert row["fireability_classification"] == "FIREABLE_CANDIDATE_DISPLAY_ONLY"
    assert row["tradability_status"] == "SURFACING_MISMATCH_SUSPECTED"
    assert payload["lane_status_summary"]["actionable_now_count"] == 0
    assert payload["lane_status_summary"]["candidate_signal_count"] == 1


def test_dashboard_paper_readiness_treats_current_bar_order_intent_as_executable_actionable(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "mes_intent.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="MES",
        observed_completed_bar_end_ts="2026-05-01T13:16:00-04:00",
        processed_bar_end_ts="2026-05-01T13:16:00-04:00",
        feature_bar_ts="2026-05-01T13:16:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "mes_intent_backed",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "SELL",
                        "intent_action": "SELL",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mes_intent_backed", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [
                {
                    "lane_id": "mes_intent_backed",
                    "audit_verdict": "INTENT_NO_FILL_YET",
                    "last_order_intent_id": "MES|1m|2026-05-01T17:16:00Z|SELL_TO_OPEN",
                    "last_intent_timestamp": "2026-05-01T13:16:00-04:00",
                    "latest_intent_summary": {
                        "submit_attempt_id": "MES|1m|2026-05-01T17:16:00Z|SELL_TO_OPEN|submit|2026-05-01T17:16:00+00:00"
                    },
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "US_MIDDAY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "mes_intent_backed",
                    "display_name": "MES / intentBacked",
                    "symbol": "MES",
                    "session_restriction": "US_MIDDAY",
                    "allowed_sessions": ["US_MIDDAY"],
                    "current_detected_session": "US_MIDDAY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 5, 1, 13, 16, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["actionable_now"] is True
    assert row["executable_actionable_this_bar"] is True
    assert row["order_intent_minted"] is True
    assert row["route_preflight_attempted"] is True
    assert row["current_bar_order_intent_id"] == "MES|1m|2026-05-01T17:16:00Z|SELL_TO_OPEN"
    assert row["current_bar_submit_attempt_id"] == "MES|1m|2026-05-01T17:16:00Z|SELL_TO_OPEN|submit|2026-05-01T17:16:00+00:00"
    assert row["tradability_status"] == "ACTIONABLE_NOW"


def test_dashboard_paper_readiness_does_not_zero_unrelated_live_capable_lanes_when_one_lane_lacks_bar_authority(
    tmp_path: Path,
) -> None:
    service = OperatorDashboardService(tmp_path)
    ready_db = tmp_path / "gc_ready.sqlite3"
    missing_db = tmp_path / "es_missing.sqlite3"
    _write_lane_bar_authority_db(
        ready_db,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        processed_bar_end_ts="2026-04-29T10:34:00-04:00",
        feature_bar_ts="2026-04-29T10:34:00-04:00",
    )
    _init_empty_dashboard_db(missing_db)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_ready_no_setup",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    },
                    {
                        "strategy_id": "es_bar_authority_unavailable",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {
            "lanes": [
                {"lane_id": "gc_ready_no_setup", "risk_state": "OK"},
                {"lane_id": "es_bar_authority_unavailable", "risk_state": "OK"},
            ]
        },
        "signal_intent_fill_audit": {
            "rows": [{"lane_id": "gc_ready_no_setup", "audit_verdict": "NO_SETUP_OBSERVED"}]
        },
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_ready_no_setup",
                    "display_name": "GC / readyNoSetup",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{ready_db}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                },
                {
                    "lane_id": "es_bar_authority_unavailable",
                    "display_name": "ES / barAuthorityUnavailable",
                    "symbol": "ES",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{missing_db}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                },
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    rows = {row["lane_id"]: row for row in payload["lane_eligibility_rows"]}

    assert rows["gc_ready_no_setup"]["live_capable"] is True
    assert rows["gc_ready_no_setup"]["blocked_lane"] is False
    assert rows["es_bar_authority_unavailable"]["live_capable"] is False
    assert rows["es_bar_authority_unavailable"]["first_true_blocker"] == "BAR_AUTHORITY_UNAVAILABLE"
    assert payload["lane_status_summary"]["live_capable_count"] == 1
    assert payload["lane_status_summary"]["blocked_lanes_count"] == 1


def test_dashboard_bar_authority_prefers_current_databento_bar_over_older_schwab_row(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_databento_authority.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:34:00-04:00",
        observed_bar_created_at="2026-04-29T10:34:02-04:00",
        observed_data_source="schwab_live_poll",
        processed_bar_end_ts="2026-04-29T10:40:00-04:00",
        feature_bar_ts="2026-04-29T10:40:00-04:00",
        signal_bar_ts="2026-04-29T10:40:00-04:00",
    )
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "GC-exec-bar-databento",
                "databento_live",
                "GC",
                "GC",
                "1m",
                "2026-04-29T10:40:00-04:00",
                "2026-04-29T10:40:00-04:00",
                "2026-04-29T10:40:00-04:00",
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                "2026-04-29T10:40:03-04:00",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    paper = {
        "approved_models": {
            "rows": [
                {
                    "lane_id": "gc_lane",
                    "branch": "GC lane",
                    "instrument": "GC",
                    "execution_timeframe": "1m",
                    "context_timeframes": ["3m"],
                    "runtime_presence": "ACTIVE_RUNTIME",
                    "strategy_status": "READY",
                }
            ]
        },
        "raw_operator_status": {
            "active_lane_ids": ["gc_lane"],
            "lanes": [
                {
                    "lane_id": "gc_lane",
                    "display_name": "GC lane",
                    "symbol": "GC",
                    "database_url": f"sqlite:///{db_path}",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "eligibility_reason": "",
                    "eligibility_detail": "",
                    "last_processed_bar_end_ts": "2026-04-29T10:40:00-04:00",
                    "current_detected_session": "US_EARLY",
                    "strategy_status": "READY",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 40, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["observed_completed_bar_source"] == "databento_live"
    assert row["observed_completed_bar_end_ts"] == "2026-04-29T10:40:00-04:00"
    assert row["market_data_stale"] is False


def test_dashboard_paper_readiness_ignores_future_bars_when_replaying_snapshot_time(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "gc_replay_bound.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="GC",
        observed_completed_bar_end_ts="2026-04-29T10:40:00-04:00",
        processed_bar_end_ts="2026-04-29T10:40:00-04:00",
        feature_bar_ts="2026-04-29T10:40:00-04:00",
        signal_bar_ts="2026-04-29T10:40:00-04:00",
    )
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "GC-exec-bar-earlier",
                "schwab_live_poll",
                "GC",
                "GC",
                "1m",
                "2026-04-29T10:34:00-04:00",
                "2026-04-29T10:34:00-04:00",
                "2026-04-29T10:34:00-04:00",
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                "2026-04-29T10:34:00-04:00",
            ),
        )
        connection.execute(
            "insert into processed_bars values (?, ?)",
            ("GC-processed-bar-earlier", "2026-04-29T10:34:00-04:00"),
        )
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "GC-context-bar-earlier",
                "internal",
                "GC",
                "GC",
                "3m",
                "2026-04-29T10:34:00-04:00",
                "2026-04-29T10:34:00-04:00",
                "2026-04-29T10:34:00-04:00",
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                "2026-04-29T10:34:00-04:00",
            ),
        )
        connection.execute(
            "insert into features values (?, ?, ?)",
            ("GC-context-bar-earlier", json.dumps({"feature": "value"}), "2026-04-29T10:34:00-04:00"),
        )
        connection.execute(
            "insert into signals values (?, ?, ?)",
            ("GC-signal-bar-earlier", json.dumps({"signal": "BUY"}), "2026-04-29T10:34:00-04:00"),
        )
        connection.commit()
    finally:
        connection.close()
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_replay_bound",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "BUY",
                        "intent_action": "BUY",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_replay_bound", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [
                {
                    "lane_id": "gc_replay_bound",
                    "audit_verdict": "ENTRY_READY",
                    "last_signal_timestamp": "2026-04-29T10:34:00-04:00",
                    "last_actionable_signal_timestamp": "2026-04-29T10:34:00-04:00",
                    "last_long_entry": True,
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "US_EARLY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "gc_replay_bound",
                    "display_name": "GC / replayBound",
                    "symbol": "GC",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "US_EARLY",
                    "allowed_session_match": True,
                    "eligible_now": True,
                    "eligibility_reason": "no_new_completed_bar",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["observed_completed_bar_end_ts"] == "2026-04-29T10:34:00-04:00"
    assert row["last_processed_bar_end_ts"] == "2026-04-29T10:34:00-04:00"
    assert row["last_strategy_evaluated_bar_ts"] == "2026-04-29T10:34:00-04:00"
    assert row["bar_state"] == "ACTIONABLE"


def test_dashboard_paper_readiness_keeps_unclassified_phase_gap_from_blocking_broad_session_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: "US_EARLY")
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    db_path = tmp_path / "es_phase_gap.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="ES",
        observed_completed_bar_end_ts="2026-04-29T10:33:00-04:00",
        processed_bar_end_ts="2026-04-29T10:33:00-04:00",
        feature_bar_ts="2026-04-29T10:33:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "es_1x_ny_early_core__us_early_long",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "es_1x_ny_early_core__us_early_long", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "UNCLASSIFIED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "es_1x_ny_early_core__us_early_long",
                    "display_name": "ES / usEarlyLong",
                    "symbol": "ES",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "UNCLASSIFIED",
                    "allowed_session_match": True,
                    "eligible_now": False,
                    "eligibility_reason": "no_new_completed_bar",
                    "latest_completed_bar_end_ts": "2026-04-29T10:33:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 29, 10, 34, 5, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert payload["current_broad_trading_session"] == "US_EARLY"
    assert row["detected_phase_label"] == "UNCLASSIFIED"
    assert row["broad_trading_session"] == "US_EARLY"
    assert row["session_label_gap_active"] is True
    assert row["session_label_gap_blocking"] is False
    assert row["session_eligible"] is True
    assert row["fireability_classification"] == "FIREABLE_WAITING_FOR_BAR"


def test_dashboard_paper_readiness_does_not_mark_out_of_session_dormant_lane_as_market_data_stale(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    db_path = tmp_path / "es_out_of_session.sqlite3"
    _write_lane_bar_authority_db(
        db_path,
        symbol="ES",
        observed_completed_bar_end_ts="2026-04-29T10:59:00-04:00",
        processed_bar_end_ts="2026-04-29T10:59:00-04:00",
        feature_bar_ts="2026-04-29T10:59:00-04:00",
    )
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "es_out_of_session",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "es_out_of_session", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "LONDON_OPEN",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "es_out_of_session",
                    "display_name": "ES / outOfSession",
                    "symbol": "ES",
                    "session_restriction": "US_EARLY",
                    "allowed_sessions": ["US_EARLY"],
                    "current_detected_session": "LONDON_OPEN",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "database_url": f"sqlite:///{db_path}",
                    "execution_timeframe": "1m",
                    "primary_context_timeframe": "3m",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(
        paper,
        evaluation_timestamp=datetime(2026, 4, 30, 4, 50, 20, tzinfo=ZoneInfo("America/New_York")),
    )
    row = payload["lane_eligibility_rows"][0]

    assert row["session_eligible"] is False
    assert row["eligibility_reason"] == "wrong_session"
    assert row["market_data_stale"] is False
    assert row["bar_state"] == "OUT_OF_SESSION_DORMANT"
    assert row["blocked_lane"] is False
    assert row["live_capable"] is False
    assert row["fireability_classification"] == "FIREABLE_OUT_OF_SESSION"
    assert row["latest_fault_or_blocker"] == "wrong_session"
    assert payload["market_data_stale_count"] == 0
    assert payload["lane_status_summary"]["blocked_lanes_count"] == 0


@pytest.mark.parametrize(
    ("broad_session", "session_restriction"),
    [
        ("US_MIDDAY", "US_MIDDAY"),
        ("US_LATE", "US_LATE"),
    ],
)
def test_dashboard_paper_readiness_recovers_midday_and_late_from_stale_wrong_session_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    broad_session: str,
    session_restriction: str,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: broad_session)
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": f"lane_{session_restriction.lower()}",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": f"lane_{session_restriction.lower()}", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "UNCLASSIFIED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": f"lane_{session_restriction.lower()}",
                    "display_name": f"ES / {session_restriction}",
                    "symbol": "ES",
                    "session_restriction": session_restriction,
                    "allowed_sessions": ["US"],
                    "current_detected_session": "UNCLASSIFIED",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "latest_completed_bar_end_ts": "2026-04-29T11:03:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_eligibility_rows"][0]

    assert row["broad_trading_session"] == broad_session
    assert row["timestamp_session_match"] is True
    assert row["runtime_allowed_session_match"] is False
    assert row["allowed_session_match"] is True
    assert row["runtime_eligibility_reason"] == "wrong_session"
    assert row["eligibility_reason"] == ""
    assert row["broad_session_matches_lane"] is True
    assert row["session_label_gap_blocking"] is False
    assert row["session_eligible"] is True
    assert row["fireability_classification"] != "FIREABLE_OUT_OF_SESSION"
    assert "outside the lane's allowed session" not in str(row["tradability_reason"])


def test_dashboard_paper_readiness_keeps_out_of_window_lanes_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: "UNCLASSIFIED")
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: False)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "midday_lane",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "midday_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "UNCLASSIFIED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "midday_lane",
                    "display_name": "ES / US_MIDDAY",
                    "symbol": "ES",
                    "session_restriction": "US_MIDDAY",
                    "allowed_sessions": ["US"],
                    "current_detected_session": "UNCLASSIFIED",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "latest_completed_bar_end_ts": "2026-04-29T14:03:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_eligibility_rows"][0]

    assert row["timestamp_session_match"] is False
    assert row["allowed_session_match"] is False
    assert row["session_eligible"] is False
    assert row["eligibility_reason"] == "wrong_session"
    assert row["fireability_classification"] == "FIREABLE_OUT_OF_SESSION"


def test_dashboard_paper_readiness_keeps_entries_disabled_lanes_blocked_even_when_session_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: "US_MIDDAY")
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "disabled_midday_lane",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "disabled_midday_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_MIDDAY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "disabled_midday_lane",
                    "display_name": "ES / disabledMidday",
                    "symbol": "ES",
                    "session_restriction": "US_MIDDAY",
                    "allowed_sessions": ["US"],
                    "current_detected_session": "US_MIDDAY",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "entries_disabled",
                    "entries_enabled": False,
                    "operator_halt": False,
                    "latest_completed_bar_end_ts": "2026-04-29T11:03:00-04:00",
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": False, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_eligibility_rows"][0]

    assert row["timestamp_session_match"] is True
    assert row["allowed_session_match"] is True
    assert row["governance_allowed"] is False
    assert row["session_eligible"] is False
    assert row["eligibility_reason"] == "entries_disabled"
    assert row["tradability_reason"] == "Loaded in runtime, but entries are currently disabled."


def test_dashboard_paper_readiness_keeps_route_unready_lanes_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: "US_MIDDAY")
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps({"active_rows": []}),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "unsupported_midday_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_MIDDAY",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "unsupported_midday_lane",
                    "display_name": "ES / unsupportedMidday",
                    "symbol": "ES",
                    "session_restriction": "US_MIDDAY",
                    "allowed_sessions": ["US"],
                    "current_detected_session": "US_MIDDAY",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "latest_completed_bar_end_ts": "2026-04-29T11:03:00-04:00",
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_eligibility_rows"][0]

    assert row["timestamp_session_match"] is True
    assert row["allowed_session_match"] is True
    assert row["route_ready"] is False
    assert row["session_eligible"] is False
    assert row["fireability_classification"] == "FIREABLE_BLOCKED_ROUTE"


@pytest.mark.parametrize(
    ("evaluation_timestamp", "expected_broad_session"),
    [
        (datetime(2026, 4, 29, 11, 30, tzinfo=ZoneInfo("America/New_York")), "US_MIDDAY"),
        (datetime(2026, 4, 29, 14, 0, tzinfo=ZoneInfo("America/New_York")), "US_LATE"),
    ],
)
def test_dashboard_paper_readiness_uses_evaluation_timestamp_to_recover_generic_us_scope_from_stale_wrong_session(
    tmp_path: Path,
    evaluation_timestamp: datetime,
    expected_broad_session: str,
) -> None:
    service = OperatorDashboardService(tmp_path)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "generic_us_lane",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "ES", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "generic_us_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "UNCLASSIFIED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "generic_us_lane",
                    "display_name": "ES / genericUS",
                    "symbol": "ES",
                    "session_restriction": expected_broad_session,
                    "allowed_sessions": ["US"],
                    "current_detected_session": "UNCLASSIFIED",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "latest_completed_bar_end_ts": evaluation_timestamp.isoformat(),
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper, evaluation_timestamp=evaluation_timestamp)
    row = payload["lane_eligibility_rows"][0]

    assert row["broad_trading_session"] == expected_broad_session
    assert row["allowed_sessions"] == ["US"]
    assert row["session_restriction"] == expected_broad_session
    assert row["timestamp_session_match"] is True
    assert row["runtime_eligibility_reason"] == "wrong_session"
    assert row["effective_readiness_eligibility_reason"] in {"", None}
    assert row["session_eligible"] is True
    assert row["latest_fault_or_blocker"] != "wrong_session"
    assert row["first_true_blocker"] != "wrong_session"
    assert payload["lane_status_summary"]["session_eligible_lanes_count"] == 1


def test_dashboard_paper_readiness_does_not_surface_wrong_session_as_latest_blocker_when_in_session_no_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(operator_dashboard_module, "_broad_trading_session_for_timestamp", lambda _: "ASIA_EARLY")
    monkeypatch.setattr(operator_dashboard_module, "_lane_allowed_session_match_at_timestamp", lambda *_args, **_kwargs: True)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "asia_lane",
                        "current_routing_mode": "IBKR_ROUTED",
                        "ibkr_bridge_submit_capable": True,
                        "current_signal_state": "NO_ACTION",
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "asia_lane", "risk_state": "OK"}]},
        "signal_intent_fill_audit": {
            "rows": [
                {
                    "lane_id": "asia_lane",
                    "audit_verdict": "NO_SETUP_OBSERVED",
                }
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "UNCLASSIFIED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "health": {"health_status": "HEALTHY"},
            "lanes": [
                {
                    "lane_id": "asia_lane",
                    "display_name": "MGC / asia",
                    "symbol": "MGC",
                    "session_restriction": "ASIA_EARLY",
                    "allowed_sessions": ["ASIA_EARLY"],
                    "current_detected_session": "UNCLASSIFIED",
                    "allowed_session_match": False,
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                    "latest_completed_bar_end_ts": "2026-04-29T19:12:00-04:00",
                    "entries_enabled": True,
                    "operator_halt": False,
                    "context_timeframes": ["3m"],
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_eligibility_rows"][0]

    assert row["session_eligible"] is True
    assert row["effective_readiness_eligibility_reason"] in {"", None}
    assert row["latest_fault_or_blocker"] == "no_setup_observed"
    assert row["first_true_blocker"] == "no_setup_observed"
    assert row["latest_fault_or_blocker"] != "wrong_session"


def test_dashboard_paper_readiness_treats_harmless_same_underlying_coexistence_as_informational_only(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    (tmp_path / "var").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var" / "strategy_probation_dashboard.json").write_text(
        json.dumps(
            {
                "active_rows": [
                    {
                        "strategy_id": "gc_lane_a",
                        "ibkr_bridge_submit_capable": True,
                        "intent_action": "NO_ACTION",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_lane_a", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "gc_lane_a",
                        "display_name": "GC Lane A",
                        "symbol": "GC",
                        "session_restriction": "US_LATE",
                        "current_detected_session": "US_LATE",
                        "allowed_session_match": True,
                        "eligible_now": True,
                        "same_underlying_ambiguity": True,
                        "position_side": "FLAT",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_status_rows"][0]

    assert row["loaded_in_runtime"] is True
    assert row["eligible_to_trade"] is False
    assert row["informational_degradation_only"] is True
    assert row["tradability_status"] == "INFORMATIONAL_ONLY"
    assert row["manual_action_required"] is False


def test_dashboard_paper_readiness_surfaces_heartbeat_reconciliation_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "heartbeat_reconciliation": {
                        "status": "RECONCILING",
                        "classification": "unsafe_ambiguity",
                        "last_attempted_at": "2026-03-26T10:15:00+00:00",
                        "reason": "broker_position_quantity_mismatch",
                        "recommended_action": "Inspect reconciliation and wait for a clean/safe-repair result.",
                        "active_issue": True,
                        "cadence_seconds": 60,
                    },
                }
            ],
        },
        "status": {"entries_enabled": False, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["heartbeat_reconciliation_summary"]
    row = payload["lane_status_rows"][0]

    assert row["heartbeat_reconciliation_status"] == "RECONCILING"
    assert row["heartbeat_reconciliation_active_issue"] is True
    assert summary["last_status"] == "RECONCILING"
    assert summary["cadence_seconds"] == 60
    assert summary["active_issue_count"] == 1
    assert summary["active_issue_rows"][0]["lane_id"] == "mgc_lane"


def test_dashboard_paper_readiness_surfaces_order_timeout_watchdog_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "order_timeout_watchdog": {
                        "status": "ACTIVE_TIMEOUTS",
                        "last_checked_at": "2026-03-26T10:20:00+00:00",
                        "overdue_ack_count": 1,
                        "overdue_fill_count": 2,
                        "reason": "Pending-order timeouts are active.",
                        "recommended_action": "Wait for broker progression or reconciliation.",
                        "active_issue_count": 1,
                    },
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["order_timeout_watchdog_summary"]
    row = payload["lane_status_rows"][0]

    assert row["order_timeout_watchdog_status"] == "ACTIVE_TIMEOUTS"
    assert row["overdue_ack_count"] == 1
    assert row["overdue_fill_count"] == 2
    assert summary["last_status"] == "ACTIVE_TIMEOUTS"
    assert summary["overdue_ack_count"] == 1
    assert summary["overdue_fill_count"] == 2
    assert summary["active_issue_count"] == 1
    assert summary["active_issue_rows"][0]["reason"] == "Pending-order timeouts are active."


def test_dashboard_paper_readiness_surfaces_restore_validation_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "startup_restore_validation": {
                        "restore_result": "SAFE_CLEANUP_READY",
                        "restore_completed_at": "2026-03-26T10:30:00+00:00",
                        "safe_cleanup_applied": True,
                        "safe_cleanup_actions": ["clear_stale_open_order_markers"],
                        "unresolved_restore_issue": False,
                        "recommended_action": "No action needed; safe cleanup was applied automatically.",
                        "duplicate_action_prevention_held": True,
                    },
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["restore_validation_summary"]
    row = payload["lane_status_rows"][0]

    assert row["restore_result"] == "SAFE_CLEANUP_READY"
    assert row["restore_safe_cleanup_applied"] is True
    assert row["restore_unresolved_issue"] is False
    assert row["duplicate_action_prevention_held"] is True
    assert summary["last_restore_result"] == "SAFE_CLEANUP_READY"
    assert summary["safe_cleanup_count"] == 1
    assert summary["unresolved_issue_count"] == 0
    assert summary["duplicate_action_prevention_held"] is True
    assert summary["recommended_action"] == "No action needed; safe cleanup was applied automatically."


def test_dashboard_paper_soak_validation_surfaces_latest_validation_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_validation").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_validation" / "paper_soak_validation_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-26T19:30:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "scenario_count": 10,
                    "passed_count": 10,
                    "failed_count": 0,
                    "runtime_phase": "READY",
                    "strategy_state": "READY",
                    "position_state": {"side": "FLAT"},
                    "market_data_health": {"market_data_ok": True},
                },
                "scenarios": [
                    {"scenario_id": "clean_entry_exit_cycle", "status": "PASS", "detail": "ok", "summary": {"runtime_phase": "READY", "strategy_state": "READY"}},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_validation_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["passed_count"] == 10
    assert payload["summary"]["runtime_phase"] == "READY"
    assert payload["scenario_rows"][0]["scenario_id"] == "clean_entry_exit_cycle"
    assert "10/10 scenarios passed" in payload["summary_line"]


def test_dashboard_paper_live_timing_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "live_timing_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:00:00+00:00",
                "runtime_phase": "RECONCILING",
                "strategy_state": "READY",
                "position_state": {"side": "FLAT", "internal_qty": 0, "broker_qty": 0},
                "evaluated_bar_id": "MGC|5m|2026-03-27T14:55:00+00:00",
                "intent_created_at": "2026-03-27T14:55:01+00:00",
                "submit_attempted_at": "2026-03-27T14:55:01+00:00",
                "broker_ack_at": "2026-03-27T14:55:02+00:00",
                "broker_fill_at": None,
                "pending_since": "2026-03-27T14:55:02+00:00",
                "pending_reason": "fill_timeout_escalated",
                "pending_stage": "RECONCILING",
                "reconcile_trigger_source": "fill_timeout",
                "entries_disabled_blocker": "fill_timeout_escalated",
                "broker_truth": {
                    "decision_order": ["direct_order_status", "open_orders", "position_truth", "fill_truth"],
                    "direct_order_status": "ACKNOWLEDGED",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_live_timing_summary_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["runtime_phase"] == "RECONCILING"
    assert payload["pending_stage"] == "RECONCILING"
    assert payload["broker_truth"]["direct_order_status"] == "ACKNOWLEDGED"
    assert "stage=RECONCILING" in payload["summary_line"]


def test_dashboard_paper_broker_truth_shadow_validation_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    service = OperatorDashboardService(repo_root)
    artifact_path = service._production_link_service.config.snapshot_path.with_name("broker_truth_schema_validation_latest.json")  # type: ignore[attr-defined]
    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    artifact_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:05:00+00:00",
                "operator_path": "mgc-v05l probationary-broker-truth-shadow-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "READ_ONLY_LIVE_SHADOW"},
                "selected_account_hash": "hash-123",
                "schemas": {
                    "order_status": {"required_fields": ["broker_order_id", "status"], "optional_fields": ["symbol"]},
                    "open_orders": {"required_fields": ["broker_order_id", "symbol", "status", "instruction", "quantity"], "optional_fields": []},
                    "position": {"required_fields": ["symbol", "side", "quantity"], "optional_fields": []},
                    "account_health": {"required_fields": ["status", "broker_reachable", "auth_ready", "account_selected"], "optional_fields": []},
                },
                "validations": {
                    "order_status": {"classification": "partial_but_usable_truth", "issues": ["representative_order_unavailable"]},
                    "open_orders": {"classification": "sufficient_broker_truth", "issues": []},
                    "position": {"classification": "sufficient_broker_truth", "issues": []},
                    "account_health": {"classification": "sufficient_broker_truth", "issues": []},
                },
                "summary": {
                    "result": "WARN",
                    "overall_classification": "partial_but_usable_truth",
                    "representative_broker_order_id": None,
                    "missing_or_ambiguous_fields": [{"schema_name": "order_status", "issues": ["representative_order_unavailable"]}],
                    "summary_line": "WARN | classification=partial_but_usable_truth | order_status=partial_but_usable_truth | open_orders=sufficient_broker_truth | position=sufficient_broker_truth | account_health=sufficient_broker_truth",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_broker_truth_shadow_validation_payload({"artifacts_dir": str(repo_root / "outputs" / "probationary_pattern_engine" / "paper_session")})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "WARN"
    assert payload["selected_account_hash"] == "hash-123"
    assert payload["validations"]["order_status"]["classification"] == "partial_but_usable_truth"
    assert "classification=partial_but_usable_truth" in payload["summary_line"]


def test_dashboard_shadow_live_shadow_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (shadow_artifacts / "live_shadow_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:08:00+00:00",
                "operator_path": "mgc-v05l probationary-live-shadow",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "LIVE_SHADOW_NO_SUBMIT"},
                "current_runtime_phase": "RECONCILING",
                "strategy_state": "READY",
                "last_finalized_live_bar_id": "MGC|5m|2026-03-27T15:05:00+00:00",
                "session_classification": "US_MIDDAY",
                "latest_signal_summary": {"long_entry": True, "long_entry_source": "usLatePauseResumeLongTurn"},
                "latest_shadow_intent": {"intent_type": "BUY_TO_OPEN", "reason_code": "usLatePauseResumeLongTurn"},
                "submit_would_be_allowed_if_shadow_disabled": False,
                "entries_disabled_blocker": "broker_reconciliation_not_clear",
                "pending_stage": "SHADOW_INTENT_SUPPRESSED",
                "pending_reason": "shadow_submit_suppressed",
                "reconcile_trigger_source": "broker_reconciliation",
                "broker_truth_summary": {
                    "classification": "INSUFFICIENT_TRUTH_RECONCILE",
                    "reconciliation_status": "blocked",
                },
                "summary_line": "phase=RECONCILING | last_bar=MGC|5m|2026-03-27T15:05:00+00:00 | submit=BLOCKED | blocker=broker_reconciliation_not_clear",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._shadow_live_shadow_summary_payload({"artifacts_dir": str(shadow_artifacts)})

    assert payload["available"] is True
    assert payload["current_runtime_phase"] == "RECONCILING"
    assert payload["latest_shadow_intent"]["intent_type"] == "BUY_TO_OPEN"
    assert payload["entries_disabled_blocker"] == "broker_reconciliation_not_clear"
    assert payload["broker_truth_summary"]["classification"] == "INSUFFICIENT_TRUTH_RECONCILE"
    assert "submit=BLOCKED" in payload["summary_line"]


def test_dashboard_shadow_live_strategy_pilot_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (shadow_artifacts / "live_strategy_pilot_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:18:00+00:00",
                "operator_path": "mgc-v05l probationary-live-strategy-pilot",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "LIVE_STRATEGY_PILOT"},
                "live_strategy_pilot_enabled": True,
                "live_strategy_submit_enabled": True,
                "live_strategy_single_cycle_mode": True,
                "pilot_armed": False,
                "pilot_rearm_required": True,
                "submit_currently_enabled": False,
                "cycle_status": "completed",
                "remaining_allowed_live_submits": 0,
                "current_runtime_phase": "READY",
                "strategy_state": "READY",
                "current_strategy_readiness": False,
                "latest_evaluated_bar": {"bar_id": "MGC|5m|2026-03-27T15:15:00+00:00"},
                "latest_live_strategy_intent": {"intent_type": "BUY_TO_OPEN", "submit_attempted": True},
                "submit_attempted_at": "2026-03-27T15:15:01+00:00",
                "broker_ack_at": "2026-03-27T15:15:02+00:00",
                "broker_fill_at": None,
                "broker_order_id": "broker-123",
                "pending_stage": "AWAITING_FILL",
                "pending_reason": "awaiting_broker_fill",
                "reconcile_trigger_source": None,
                "entries_disabled_blocker": "pending_unresolved_order",
                "submit_gate": {"blocker": "pending_unresolved_order", "submit_eligible": False},
                "pilot_cycle": {
                    "pilot_armed": False,
                    "rearm_required": True,
                    "cycle_status": "completed",
                    "remaining_allowed_live_submits": 0,
                    "entry": {"intent_type": "BUY_TO_OPEN"},
                    "exit": {"intent_type": "SELL_TO_CLOSE"},
                    "final_result": "completed",
                    "rearm_action": "rearm_live_strategy_pilot",
                },
                "broker_truth_summary": {"classification": "SUFFICIENT_BROKER_TRUTH"},
                "position_state": {"side": "FLAT", "internal_qty": 0},
                "signal_observability": {
                    "available": True,
                    "why_no_trade_so_far": "No final entries yet. Raw long candidates: 2 -> final long entries: 0. Raw short candidates: 1 -> final short entries: 0.",
                    "session_counts": {
                        "bull_snap_turn_candidate": 3,
                        "firstBullSnapTurn": 0,
                        "asia_reclaim_bar_raw": 1,
                        "asia_hold_bar_ok": 0,
                        "asia_acceptance_bar_ok": 0,
                        "asiaVWAPLongSignal": 0,
                        "bear_snap_turn_candidate": 1,
                        "firstBearSnapTurn": 0,
                        "longEntryRaw": 2,
                        "shortEntryRaw": 1,
                        "longEntry": 0,
                        "shortEntry": 0,
                    },
                    "top_failed_predicates": {
                        "bullSnapLong": [{"predicate": "bull_snap_turn_candidate", "count": 5}],
                        "asiaVWAPLong": [{"predicate": "asia_reclaim_bar_raw", "count": 4}],
                        "bearSnapShort": [{"predicate": "bear_snap_turn_candidate", "count": 5}],
                    },
                    "per_bar_rows": [
                        {
                            "bar_id": "MGC|5m|2026-03-27T15:15:00+00:00",
                            "why_no_trade": "bullSnapLong stalled at bull_snap_turn_candidate",
                            "recentLongSetup": False,
                            "recentShortSetup": False,
                            "barsSinceLongSetup": None,
                            "barsSinceShortSetup": None,
                        }
                    ],
                },
                "summary_line": "pilot=ENABLED | phase=READY | submit=BLOCKED | bar=MGC|5m|2026-03-27T15:15:00+00:00 | blocker=pending_unresolved_order",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._shadow_live_strategy_pilot_summary_payload({"artifacts_dir": str(shadow_artifacts)})

    assert payload["available"] is True
    assert payload["live_strategy_pilot_enabled"] is True
    assert payload["live_strategy_submit_enabled"] is True
    assert payload["pilot_armed"] is False
    assert payload["cycle_status"] == "completed"
    assert payload["remaining_allowed_live_submits"] == 0
    assert payload["pending_stage"] == "AWAITING_FILL"
    assert payload["entries_disabled_blocker"] == "pending_unresolved_order"
    assert payload["latest_live_strategy_intent"]["intent_type"] == "BUY_TO_OPEN"
    assert dict(payload["pilot_cycle"])["rearm_action"] == "rearm_live_strategy_pilot"
    assert dict(payload["signal_observability"])["session_counts"]["longEntryRaw"] == 2
    assert dict(payload["signal_observability"])["top_failed_predicates"]["bullSnapLong"][0]["predicate"] == "bull_snap_turn_candidate"


def test_dashboard_paper_live_timing_validation_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_live_timing_validation").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_live_timing_validation" / "paper_live_timing_validation_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:10:00+00:00",
                "operator_path": "mgc-v05l probationary-live-timing-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER_RUNTIME_WITH_LIVE_TIMING_BOUNDARY"},
                "contract": {
                    "broker_truth_decision_order": ["direct_order_status", "open_orders", "position_truth", "fill_truth"],
                    "acknowledgement_window_seconds": 30,
                    "fill_confirmation_window_seconds": 60,
                },
                "summary": {
                    "result": "PASS",
                    "scenario_count": 8,
                    "passed_count": 8,
                    "final_runtime_phase": "FILLED",
                    "final_strategy_state": "READY",
                    "final_pending_stage": "FILLED",
                    "final_blocker": None,
                },
                "scenarios": [
                    {"scenario_id": "submit_after_completed_bar_close", "status": "PASS", "detail": "ok", "summary": {"pending_stage": "AWAITING_FILL", "runtime_phase": "READY"}},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_live_timing_validation_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["passed_count"] == 8
    assert payload["contract"]["broker_truth_decision_order"] == ["direct_order_status", "open_orders", "position_truth", "fill_truth"]
    assert payload["scenario_rows"][0]["scenario_id"] == "submit_after_completed_bar_close"
    assert "8/8 scenarios passed" in payload["summary_line"]


def test_dashboard_paper_soak_extended_surfaces_latest_extended_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_extended").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_extended" / "paper_soak_extended_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T14:30:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-extended",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "bars_processed": 24,
                    "restart_count": 5,
                    "drift_detected": False,
                    "final_runtime_phase": "RECONCILING",
                    "final_strategy_state": "READY",
                    "final_position_state": {"side": "FLAT"},
                    "final_entry_blocker": "fill_timeout_escalated",
                },
                "checkpoint_rows": [
                    {"checkpoint_id": "pending_acknowledged_order", "trigger_state": "PENDING_ORDER", "drift_detected": False},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_extended_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["bars_processed"] == 24
    assert payload["summary"]["restart_count"] == 5
    assert payload["checkpoint_rows"][0]["checkpoint_id"] == "pending_acknowledged_order"
    assert "bars=24" in payload["summary_line"]


def test_dashboard_signal_selectivity_analysis_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    artifact_dir = repo_root / "outputs" / "probationary_pattern_engine" / "signal_selectivity_analysis"
    artifact_dir.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (artifact_dir / "signal_selectivity_analysis_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T19:45:00+00:00",
                "dataset_count": 4,
                "summary_line": "raw long 3 -> final 0, raw short 1 -> final 0; top blockers: bullSnapLong -> range, asiaVWAPLong -> reclaim color, bearSnapShort -> location",
                "key_findings": [
                    "Live pilot: long raw 0 -> final 0, short raw 0 -> final 0.",
                    "Bear Snap location comparison: short raw 2 -> 5, short final 0 -> 1, location primary blocks 7 -> 1.",
                ],
                "live_pilot_focus": {
                    "why_no_trade_so_far": "No trade so far because raw long 0 -> final 0 and raw short 0 -> final 0.",
                    "top_failed_predicates": {
                        "bullSnapLong": [{"predicate": "range", "count": 12}],
                        "asiaVWAPLong": [{"predicate": "reclaim color", "count": 9}],
                        "bearSnapShort": [{"predicate": "location", "count": 7}],
                    },
                    "raw_candidates_vs_final_entries": {
                        "long": {"raw_candidates": 3, "final_entries": 0},
                        "short": {"raw_candidates": 1, "final_entries": 0},
                    },
                    "anti_churn": {
                        "suppression_by_family": {
                            "bullSnapLong": {"suppressed_count": 1},
                            "asiaVWAPLong": {"suppressed_count": 0},
                            "bearSnapShort": {"suppressed_count": 0},
                        }
                    },
                },
                "before_after_bear_snap_location": {
                    "available": True,
                    "summary_line": "short raw 2 -> 5, short final 0 -> 1, location primary blocks 7 -> 1",
                    "materially_improved_short_opportunity_rate": True,
                },
                "bear_snap_up_stretch_ladder": {
                    "available": True,
                    "recommended_value": "0.90",
                    "range_becomes_next_dominant_blocker": True,
                    "summary_line": "1.00 -> 0.90: short raw 21 -> 22, short final 21 -> 22, short/100 1.471 -> 1.541, top blocker upside stretch -> range",
                },
                "bear_snap_range_ladder": {
                    "available": True,
                    "recommended_value": "0.80",
                    "next_dominant_blocker_after_recommended": "upside stretch",
                    "summary_line": "0.90 -> 0.80: short raw 22 -> 25, short final 22 -> 25, short/100 1.541 -> 1.751, top blocker range -> upside stretch",
                },
                "regime_comparison": {
                    "red_day_down_tape": {
                        "short_raw_candidates_per_100_bars": 1.2,
                    }
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._signal_selectivity_analysis_payload({})

    assert payload["available"] is True
    assert payload["dataset_count"] == 4
    assert payload["live_pilot_focus"]["top_failed_predicates"]["bearSnapShort"][0]["predicate"] == "location"
    assert payload["before_after_bear_snap_location"]["materially_improved_short_opportunity_rate"] is True
    assert payload["bear_snap_range_ladder"]["recommended_value"] == "0.80"
    assert payload["bear_snap_up_stretch_ladder"]["recommended_value"] == "0.90"
    assert service._signal_selectivity_analysis_path == repo_root / "outputs" / "operator_dashboard" / "signal_selectivity_analysis_snapshot.json"


def test_dashboard_paper_soak_unattended_surfaces_latest_unattended_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_unattended").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_unattended" / "paper_soak_unattended_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T18:00:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-unattended",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "bars_processed": 60,
                    "runtime_duration_minutes": 295,
                    "restart_count": 7,
                    "drift_detected": False,
                    "final_runtime_phase": "RECONCILING",
                    "final_strategy_state": "READY",
                    "final_position_state": {"side": "FLAT"},
                    "final_entry_blocker": "fill_timeout_escalated",
                },
                "checkpoint_rows": [
                    {
                        "checkpoint_id": "heartbeat_reconcile_restart",
                        "trigger_state": "HEARTBEAT_RECONCILE",
                        "drift_detected": False,
                        "summary_alignment_held": True,
                    },
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_unattended_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["bars_processed"] == 60
    assert payload["summary"]["restart_count"] == 7
    assert payload["checkpoint_rows"][0]["checkpoint_id"] == "heartbeat_reconcile_restart"
    assert "duration=295m" in payload["summary_line"]


def test_dashboard_paper_exit_parity_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "exit_parity_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T19:00:00+00:00",
                "position_side": "LONG",
                "current_position_family": "VWAP",
                "latest_exit_decision": {
                    "primary_reason": "VWAP_LOSS",
                    "all_true_reasons": ["VWAP_LOSS", "VWAP_WEAK_FOLLOWTHROUGH"],
                },
                "stop_refs": {"active_long_stop_ref": "100.0"},
                "break_even": {"long_break_even_armed": True, "short_break_even_armed": False},
                "latest_restore_result": "READY",
                "exit_fill_pending": True,
                "exit_fill_confirmed": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_exit_parity_summary_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["current_position_family"] == "VWAP"
    assert payload["latest_exit_decision"]["primary_reason"] == "VWAP_LOSS"
    assert payload["break_even"]["long_break_even_armed"] is True
    assert "family=VWAP" in payload["summary_line"]


def test_dashboard_falls_back_to_configured_paper_lanes_when_runtime_lane_artifacts_are_missing(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True, exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:00:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T08:55:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": [
                    "asiaEarlyNormalBreakoutRetestHoldTurn",
                    "usLatePauseResumeLongTurn",
                ],
                "approved_short_entry_sources": ["asiaEarlyPauseResumeShortTurn"],
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_source": "test",
        "feed_state": "LIVE",
        "feed_label": "INDEX FEED LIVE",
        "updated_at": "2026-03-19T09:00:00-04:00",
        "age_seconds": 0,
        "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        "note": "test",
        "diagnostics": {"fetch_state": "SUCCESS", "symbols": []},
        "symbols": [],
    }
    service._paper_config_in_force_fallback = lambda artifacts_dir, db_path: {  # type: ignore[method-assign]
        "loss_halts_disabled": True,
        "desk_halt_new_entries_loss": "-1500",
        "desk_flatten_and_halt_loss": "-2500",
        "lane_realized_loser_limit_per_session": 2,
        "lanes": [
            {
                "lane_id": "mgc_us_late_pause_resume_long",
                "display_name": "MGC / usLatePauseResumeLongTurn",
                "symbol": "MGC",
                "long_sources": ["usLatePauseResumeLongTurn"],
                "short_sources": [],
                "session_restriction": "US_LATE",
            },
            {
                "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "MGC",
                "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                "short_sources": [],
                "session_restriction": "ASIA_EARLY",
            },
            {
                "lane_id": "mgc_asia_early_pause_resume_short",
                "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                "symbol": "MGC",
                "long_sources": [],
                "short_sources": ["asiaEarlyPauseResumeShortTurn"],
                "session_restriction": "ASIA_EARLY",
            },
            {
                "lane_id": "pl_us_late_pause_resume_long",
                "display_name": "PL / usLatePauseResumeLongTurn",
                "symbol": "PL",
                "long_sources": ["usLatePauseResumeLongTurn"],
                "short_sources": [],
                "session_restriction": "US_LATE",
            },
            {
                "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "GC",
                "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                "short_sources": [],
                "session_restriction": "ASIA_EARLY",
            },
        ],
    }  # type: ignore[method-assign]

    snapshot = service.snapshot()

    assert snapshot["paper"]["raw_operator_status"]["paper_lane_count"] == 5
    assert len(snapshot["paper"]["raw_operator_status"]["lanes"]) == 5
    assert snapshot["paper"]["approved_models"]["enabled_count"] == 5
    assert snapshot["paper"]["approved_models"]["total_count"] == 5
    assert snapshot["paper"]["approved_models"]["instrument_scope"] == "5 shared paper lanes / multi-lane paper mode"
    assert {row["branch"] for row in snapshot["paper"]["approved_models"]["rows"]} == {
        "MGC / usLatePauseResumeLongTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyPauseResumeShortTurn",
        "PL / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    assert snapshot["paper"]["readiness"]["approved_models_active"] == 5
    assert snapshot["paper"]["entry_eligibility"]["verdict"] == "NOT ELIGIBLE: RUNTIME STOPPED"
    assert snapshot["paper"]["entry_eligibility"]["primary_blocking_reason"] == "RUNTIME_STOPPED"


def test_market_index_rows_keep_primary_quote_fields_when_bid_ask_missing() -> None:
    raw_payload = {
        "$SPX": {
            "assetMainType": "INDEX",
            "quote": {
                "lastPrice": 6624.7,
                "netChange": -91.39,
                "netPercentChange": -1.36076199,
                "tradeTime": 1773864761067,
                "securityStatus": "Closed",
            },
            "realtime": True,
            "reference": {
                "description": "S&P 500 INDEX",
                "exchangeName": "Index",
            },
            "symbol": "$SPX",
        }
    }

    rows, diagnostics = _market_index_rows(
        raw_payload,
        [
            {"label": "SPX", "name": "S&P 500", "external_symbol": "$SPX", "source_type": "cash_index"},
        ],
    )

    assert rows[0]["state"] == "LIVE"
    assert rows[0]["value_state"] == "LIVE"
    assert rows[0]["current_value"] == "6624.7"
    assert rows[0]["absolute_change"] == "-91.39"
    assert rows[0]["percent_change"] == "-1.36%"
    assert rows[0]["bid"] is None
    assert rows[0]["ask"] is None
    assert rows[0]["bid_state"] == "UNAVAILABLE"
    assert rows[0]["ask_state"] == "UNAVAILABLE"
    assert "BID_UNAVAILABLE" in rows[0]["diagnostic_codes"]
    assert diagnostics[0]["payload_present"] is True
    assert diagnostics[0]["field_states"]["current_value"]["available"] is True
    assert diagnostics[0]["field_states"]["bid"]["available"] is False
    assert diagnostics[0]["matched_symbol"] == "$SPX"


def test_market_index_rows_match_future_root_via_reference_product() -> None:
    raw_payload = {
        "/GCJ26": {
            "assetMainType": "FUTURE",
            "quote": {
                "lastPrice": 4823.9,
                "netChange": -184.3,
                "futurePercentChange": -3.67996486,
                "bidPrice": 4810.0,
                "askPrice": 4832.2,
            },
            "realtime": True,
            "reference": {
                "description": "Gold Futures,Apr-2026, ETH",
                "product": "/GC",
            },
            "symbol": "/GCJ26",
        }
    }

    rows, diagnostics = _market_index_rows(
        raw_payload,
        [
            {"label": "GOLD", "name": "Gold Futures", "external_symbol": "/GC", "source_type": "future"},
        ],
    )

    assert rows[0]["state"] == "LIVE"
    assert rows[0]["current_value"] == "4823.9"
    assert rows[0]["absolute_change"] == "-184.3"
    assert rows[0]["matched_symbol"] == "/GC"
    assert rows[0]["matched_via"] == "reference.product"
    assert diagnostics[0]["matched_via"] == "reference.product"
    assert diagnostics[0]["field_states"]["bid"]["available"] is True
    assert diagnostics[0]["field_states"]["ask"]["available"] is True


def test_treasury_curve_rows_scale_verified_yield_indices_and_keep_missing_tenors_explicit() -> None:
    raw_payload = {
        "$IRX": {
            "assetMainType": "INDEX",
            "quote": {"lastPrice": 36.1, "closePrice": 36.05, "netChange": 0.05},
            "reference": {"description": "CBOE INT RATE 13 WK T BILL     13 WK T BILL"},
            "symbol": "$IRX",
        },
        "$FVX": {
            "assetMainType": "INDEX",
            "quote": {"lastPrice": 38.62, "closePrice": 37.86, "netChange": 0.76},
            "reference": {"description": "CBOE INT RATE 5 YEAR T NOTE    5 YEAR T NOTE"},
            "symbol": "$FVX",
        },
        "errors": {"invalidSymbols": ["$UST2Y"]},
    }

    rows, diagnostics = _treasury_curve_rows(
        raw_payload,
        [
            {"tenor": "3M", "name": "3M", "external_symbol": "$IRX", "source_type": "cash_treasury_yield", "source_note": "13-week source"},
            {"tenor": "5Y", "name": "5Y", "external_symbol": "$FVX", "source_type": "cash_treasury_yield", "source_note": "5-year source"},
            {"tenor": "2Y", "name": "2Y", "external_symbol": "$UST2Y", "source_type": "cash_treasury_yield", "source_note": "2-year source"},
        ],
    )

    assert rows[0]["current_yield"] == "3.610"
    assert rows[0]["prior_yield"] == "3.605"
    assert rows[0]["day_change_bp"] == "0.5"
    assert rows[0]["render_classification"] == "LIVE_WITH_COMPARISON"
    assert rows[1]["current_yield"] == "3.862"
    assert rows[1]["prior_yield"] == "3.786"
    assert rows[1]["day_change_bp"] == "7.6"
    assert rows[2]["render_classification"] == "UNAVAILABLE_UNSUPPORTED_SYMBOL"
    assert diagnostics[2]["diagnostic_codes"] == ["INVALID_SYMBOL"]


def test_dashboard_snapshot_surfaces_prior_session_carry_forward_risk(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T09:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-18",
                "realized_net_pnl": "10.0",
                "flat_at_end": False,
                "reconciliation_clean": False,
                "unresolved_open_intents": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    assert snapshot["global"]["desk_clean"] is False
    assert snapshot["global"]["desk_clean_label"] == "DESK GUARDED"
    assert snapshot["global"]["paper_run_ready"] is False
    assert snapshot["paper_carry_forward"]["active"] is True
    assert snapshot["paper_carry_forward"]["session_date"] == "2026-03-18"
    assert snapshot["paper_carry_forward"]["not_flat_at_close"] is True
    assert snapshot["paper_carry_forward"]["reconciliation_dirty"] is True
    assert snapshot["paper_carry_forward"]["unresolved_open_intents"] == 2
    assert snapshot["paper_pre_session_review"]["required"] is True
    assert snapshot["paper_pre_session_review"]["completed"] is False
    assert snapshot["paper_continuity"]["entries"][2]["kind"] == "carry_forward"


def test_dashboard_snapshot_reads_paper_run_start_artifacts(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T09:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.summary.json").write_text(
        json.dumps({"realized_net_pnl": "0.0", "session_date": "2026-03-19"}) + "\n",
        encoding="utf-8",
    )

    dashboard_dir = repo_root / "outputs" / "operator_dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    (dashboard_dir / "paper_current_run_start.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-03-19T09:30:00-04:00",
                "run_start_id": "paper-run-1",
                "desk_state_at_start": "GUARDED",
                "started_after_guarded_review": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (dashboard_dir / "paper_run_start_blocks.jsonl").write_text(
        json.dumps({"timestamp": "2026-03-19T09:00:00-04:00", "blocked_reason": "Inherited risk review pending."}) + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    assert snapshot["paper_run_start"]["current"]["run_start_id"] == "paper-run-1"
    assert snapshot["paper_run_start"]["current"]["desk_state_at_start"] == "GUARDED"
    assert snapshot["paper_run_start"]["blocked_history"][0]["blocked_reason"] == "Inherited risk review pending."
    assert snapshot["paper_continuity"]["entries"][-1]["kind"] == "run_start"


def test_paper_runtime_recovery_auto_starts_stopped_runtime_when_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    pre_paper = {
        "running": False,
        "readiness": {"runtime_phase": "STOPPED"},
        "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
        "operator_state": {},
        "status": {"session_date": "2026-03-26"},
        "non_approved_lanes": {"rows": []},
    }
    post_paper = {
        "running": True,
        "status": {"session_date": "2026-03-26"},
    }

    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="started", stderr=""),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: post_paper)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper=pre_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUCCEEDED"
    assert payload["manual_action_required"] is False
    assert refreshed_paper == post_paper
    assert result is not None
    assert result["action"] == "auto-start-paper"


def _write_track_b_paper_reconciliation(
    repo_root: Path,
    *,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    broker_positions: int = 0,
    broker_orders: int = 0,
    lifecycle_positions: int = 0,
    lifecycle_orders: int = 0,
    review_required: int = 0,
    live_money_eligible: bool = False,
    submit_authority: bool = False,
    paper_proof_invoked: bool = False,
) -> Path:
    path = (
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "max_age_seconds": 120,
                "classification": classification,
                "broker_reconciled": broker_reconciled,
                "track_b_broker_position_count": broker_positions,
                "track_b_broker_open_order_count": broker_orders,
                "lifecycle_open_position_count": lifecycle_positions,
                "lifecycle_open_order_count": lifecycle_orders,
                "review_required_count": review_required,
                "live_money_eligible": live_money_eligible,
                "submit_authority": submit_authority,
                "paper_proof_invoked": paper_proof_invoked,
                "blockers": [],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def test_paper_runtime_recovery_auto_starts_after_clean_supervised_flatten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_track_b_paper_reconciliation(tmp_path)
    pre_paper = {
        "running": False,
        "readiness": {"runtime_phase": "STOPPED"},
        "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
        "operator_state": {
            "last_control_action": "flatten_and_halt",
            "last_control_status": "applied",
            "flatten_state": "complete",
        },
        "status": {"session_date": "2026-03-26"},
        "non_approved_lanes": {"rows": []},
    }
    post_paper = {"running": True, "status": {"session_date": "2026-03-26"}}

    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="started", stderr=""),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: post_paper)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper=pre_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUCCEEDED"
    assert payload["manual_action_required"] is False
    assert payload["post_flatten_recovery"]["phase"] == "FLAT_RECONCILED"
    assert payload["post_flatten_recovery"]["live_money_eligible"] is False
    assert payload["post_flatten_recovery"]["paper_proof_invoked"] is False
    assert refreshed_paper == post_paper
    assert result is not None
    assert result["action"] == "auto-start-paper"
    assert "paper_proof" not in " ".join(result["command"])


def test_paper_runtime_recovery_keeps_emergency_halt_sticky(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("emergency halt must not auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {"last_control_action": "EMERGENCY_HALT_OPERATOR_LOCKOUT"},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "EMERGENCY_HALT_OPERATOR_LOCKOUT"
    assert payload["reason_code"] == "EMERGENCY_HALT_OPERATOR_LOCKOUT"
    assert payload["manual_action_required"] is True
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_waits_when_post_flatten_broker_state_is_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("unknown post-flatten broker state must fail closed")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {
                "last_control_action": "flatten_and_halt",
                "last_control_status": "applied",
                "flatten_state": "complete",
            },
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "VERIFYING_FLAT"
    assert payload["manual_action_required"] is False
    assert payload["auto_restart_eligible"] is True
    assert payload["auto_restart_allowed"] is False
    assert payload["post_flatten_recovery"]["reason_code"] == "BROKER_RECONCILIATION_MISSING"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_blocks_after_rejected_supervised_flatten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    _write_track_b_paper_reconciliation(tmp_path)

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("rejected supervised flatten must not auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {
                "last_control_action": "flatten_and_halt",
                "last_control_status": "rejected",
                "flatten_state": "rejected_open_order_uncertainty",
            },
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "FLATTEN_FAILED_REVIEW_REQUIRED"
    assert payload["manual_action_required"] is True
    assert payload["post_flatten_recovery"]["phase"] == "FLAT_RECONCILED"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_requires_manual_action_when_stopped_runtime_is_not_safe(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {
                "primary_blocking_reason": "RECONCILIATION_DIRTY",
                "state_note": "Persisted reconciliation is dirty.",
                "clear_action": "Manual inspection required",
            },
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert payload["manual_action_required"] is True
    assert payload["next_action"] == "Manual inspection required"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_does_not_block_on_schwab_auth_for_ibkr_databento_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(
        service,
        "_schwab_sidecar_requirement",
        lambda: {
            "explicitly_required": False,
            "required_for_current_route": False,
            "provider_roles": {
                "market_data_primary": "databento",
                "market_data_fallback": "schwab",
                "broker_truth_provider": "ibkr",
                "execution_provider": "ibkr",
            },
        },
    )
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (None, {"unresolved_lane_ids": ["lane-a"]}),
    )

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": False},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["reason_code"] == "TEMP_PAPER_STARTUP_MAPPING_MISSING"
    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_still_blocks_on_schwab_auth_when_route_requires_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(
        service,
        "_schwab_sidecar_requirement",
        lambda: {
            "explicitly_required": True,
            "required_for_current_route": True,
            "provider_roles": {
                "market_data_primary": "schwab",
                "market_data_fallback": "none",
                "broker_truth_provider": "schwab",
                "execution_provider": "schwab",
            },
        },
    )

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": False},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["reason_code"] == "AUTH_NOT_READY"
    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_respects_restart_backoff_and_does_not_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_BACKOFF",
            "attempted_at": now.isoformat(),
            "failed_at": now.isoformat(),
            "last_restart_result": "FAILED",
            "restart_backoff_until": (now.replace(microsecond=0) + operator_dashboard_module.timedelta(seconds=120)).isoformat(),
            "restart_attempt_history": [
                {
                    "attempted_at": now.isoformat(),
                    "result": "FAILED",
                }
            ],
            "last_runtime_stop_detected_at": now.isoformat(),
        }
    )

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("auto-restart should not run while backoff is active")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_BACKOFF"
    assert payload["manual_action_required"] is False
    assert payload["auto_restart_allowed"] is False
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_suppresses_after_budget_exhaustion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(
        service,
        "_paper_runtime_supervisor_policy",
        lambda: {
            "restart_window_seconds": 900,
            "max_auto_restarts_per_window": 2,
            "restart_backoff_seconds": 60,
            "restart_suppression_seconds": 600,
            "failure_cooldown_seconds": 120,
        },
    )
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_BACKOFF",
            "attempted_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
            "failed_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
            "last_restart_result": "FAILED",
            "restart_attempt_history": [
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
                    "result": "FAILED",
                }
            ],
            "last_runtime_stop_detected_at": (now - operator_dashboard_module.timedelta(minutes=4)).isoformat(),
        }
    )
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 1, stdout="", stderr="boom"),
    )

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUPPRESSED"
    assert payload["manual_action_required"] is True
    assert payload["restart_suppressed"] is True
    assert payload["restart_attempts_in_window"] == 2
    assert refreshed_paper is None
    assert result is not None
    supervisor_events = operator_dashboard_module._tail_jsonl(
        tmp_path / "outputs" / "operator_dashboard" / "paper_runtime_supervisor_events.jsonl",
        10,
    )
    assert any(row["event_type"] == "restart_failed" for row in supervisor_events)
    assert any(row["event_type"] == "restart_suppressed" for row in supervisor_events)


def test_paper_runtime_recovery_suppression_blocks_further_attempts_without_duplicate_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    suppressed_until = (now + operator_dashboard_module.timedelta(minutes=10)).isoformat()
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_SUPPRESSED",
            "attempted_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
            "failed_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
            "last_restart_result": "FAILED",
            "restart_suppressed_until": suppressed_until,
            "restart_attempt_history": [
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=2)).isoformat(),
                    "result": "FAILED",
                },
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
                    "result": "FAILED",
                },
            ],
            "last_runtime_stop_detected_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
        }
    )
    event_path = tmp_path / "outputs" / "operator_dashboard" / "paper_runtime_supervisor_events.jsonl"
    operator_dashboard_module._append_jsonl(
        event_path,
        {
            "event_type": "restart_suppressed",
            "occurred_at": now.isoformat(),
            "supervisor_status": "AUTO_RESTART_SUPPRESSED",
            "message": "Automatic restart has been suppressed because the rolling restart budget was exhausted.",
        },
    )

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("suppressed runtime should not auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUPPRESSED"
    assert payload["manual_action_required"] is True
    assert refreshed_paper is None
    assert result is None
    supervisor_events = operator_dashboard_module._tail_jsonl(event_path, 10)
    assert [row["event_type"] for row in supervisor_events].count("restart_suppressed") == 1


def test_paper_runtime_recovery_respects_explicit_operator_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("explicit operator stop should block auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {"last_control_action": "stop-paper"},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert payload["reason_code"] == "OPERATOR_STOP"
    assert payload["manual_action_required"] is True
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_success_clears_stopped_runtime_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    pre_paper = {
        "running": False,
        "readiness": {"runtime_phase": "STOPPED"},
        "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
        "operator_state": {},
        "status": {"session_date": "2026-03-26"},
        "non_approved_lanes": {"rows": []},
    }
    post_paper = {
        "running": True,
        "status": {"session_date": "2026-03-26"},
    }

    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="started", stderr=""),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: post_paper)

    first_payload, _, _ = service._paper_runtime_recovery_payload(
        paper=pre_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )
    second_payload, _, _ = service._paper_runtime_recovery_payload(
        paper=post_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert first_payload["status"] == "AUTO_RESTART_SUCCEEDED"
    assert second_payload["status"] in {"AUTO_RESTART_SUCCEEDED", "RUNNING"}
    assert second_payload["last_runtime_stop_detected_at"] is None
    assert second_payload["restart_suppressed"] is False


def test_paper_runtime_recovery_running_state_prunes_stale_command_output(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    stale_output = (
        "Launching probationary paper soak with repo bootstrap.\n"
        "Schwab config: /Users/patrick/Documents/MGC-v05l-automation/config/schwab.local.json\n"
        "PID file: /Users/patrick/Documents/MGC-v05l-automation/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
    )
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_SUPPRESSED",
            "attempted_at": "2026-05-21T05:29:11+00:00",
            "succeeded_at": "2026-05-21T05:29:11+00:00",
            "last_restart_result": "SUPPRESSED",
            "output": stale_output,
        }
    )

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": True,
            "readiness": {"runtime_phase": "RUNNING"},
            "entry_eligibility": {},
            "operator_state": {},
            "status": {"session_date": "2026-05-21"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    persisted = json.loads(service._paper_runtime_recovery_path.read_text(encoding="utf-8"))
    assert service._paper_runtime_recovery_path.name == "paper_runtime_recovery.json"
    assert payload["status"] == "RUNNING"
    assert payload["operator_message"] == "Paper runtime is active."
    assert payload["manual_action_required"] is False
    assert refreshed_paper is None
    assert result is None
    assert "output" not in payload
    assert "latest_command_output" not in payload
    assert "output" not in persisted
    assert "latest_command_output" not in persisted
    assert "/Users/patrick/Documents/" not in json.dumps(persisted)


def test_paper_runtime_recovery_ignores_numbered_legacy_copies(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._write_paper_runtime_recovery_state(
        {
            "status": "RUNNING",
            "operator_message": "Paper runtime is active.",
            "manual_action_required": False,
        }
    )
    legacy_copy = service._dashboard_artifacts_dir / "paper_runtime_recovery 2.json"
    legacy_copy.write_text(
        json.dumps(
            {
                "status": "STOPPED_MANUAL_REQUIRED",
                "operator_message": "Legacy copy should not be authoritative.",
                "manual_action_required": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_runtime_recovery_state()

    assert service._paper_runtime_recovery_path == service._dashboard_artifacts_dir / "paper_runtime_recovery.json"
    assert payload["status"] == "RUNNING"
    assert payload["manual_action_required"] is False


def test_restart_paper_with_temp_paper_ignores_missing_pid_and_surfaces_auth_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    paper_state = {
        "running": False,
        "status": {"fault_state": "CLEAR", "operator_halt": False},
        "operator_state": {"flatten_pending": False, "stop_after_cycle_requested": False},
        "desk_risk": {"desk_risk_state": "OK"},
        "lane_risk": {"lanes": []},
    }
    snapshot = {
        "paper": {
            "running": False,
            "temporary_paper_runtime_integrity": {
                "missing_lane_ids": [],
                "unresolved_start_lane_ids": [],
                "summary_line": "clear",
            },
        }
    }
    snapshots = [snapshot, snapshot]
    monkeypatch.setattr(service, "snapshot", lambda: snapshots.pop(0) if snapshots else snapshot)
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda current_snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: paper_state if runtime_name == "paper" else {"running": False})
    monkeypatch.setattr(service, "_review_payload", lambda paper, scope: {})
    monkeypatch.setattr(service, "_paper_carry_forward_state", lambda paper, review: {"active": False})
    monkeypatch.setattr(service, "_paper_pre_session_review_state", lambda carry: {"ready_for_run": True})
    monkeypatch.setattr(service, "_launch_gate_auth_status", lambda: {"runtime_ready": False, "next_action": "Auth Gate Check"})
    monkeypatch.setattr(
        service,
        "_prechecked_action_result",
        lambda action: (
            {
                **service._result_record(
                    action="start-paper",
                    ok=False,
                    command=None,
                    output="Paper runtime start is blocked because broker/auth readiness is not green yet.",
                ),
                "reason_code": "AUTH_NOT_READY",
                "next_action": "Auth Gate Check",
            }
            if action == "start-paper"
            else None
        ),
    )

    def _fake_run(command, **kwargs):
        if command == ["bash", "scripts/stop_probationary_paper_soak.sh"]:
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=f"No probationary paper PID file found at {tmp_path / 'probationary_paper.pid'}.",
                stderr="",
            )
        raise AssertionError("start command should not run when auth is not ready")

    monkeypatch.setattr(operator_dashboard_module.subprocess, "run", _fake_run)

    result = service.run_action("restart-paper-with-temp-paper")

    assert result["ok"] is False
    assert result["reason_code"] == "AUTH_NOT_READY"
    assert result["next_action"] == "Auth Gate Check"
    assert "AUTH_NOT_READY" in str(result["message"])
    assert "Auth Gate Check" in str(result["message"])
    assert str(result["detail"]) == "AUTH_NOT_READY | Next action: Auth Gate Check"
    assert "auth readiness is not green" in str(result["output"]).lower()
    assert "No probationary paper PID file found" not in str(result["output"])
    assert "No probationary paper PID file found" in str(result["stop_output"])


def test_launch_gate_auth_status_refreshes_when_cached_status_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())

    calls: list[bool] = []

    def _fake_load_or_refresh_auth_gate_result(*, run_if_missing: bool) -> dict[str, object]:
        calls.append(run_if_missing)
        return {"runtime_ready": False, "source": "missing"}

    monkeypatch.setattr(service, "_load_or_refresh_auth_gate_result", _fake_load_or_refresh_auth_gate_result)
    monkeypatch.setattr(
        service,
        "_run_auth_gate_result",
        lambda: {"runtime_ready": True, "source": "fresh_auth_gate", "next_action": "None"},
    )

    result = service._launch_gate_auth_status()  # noqa: SLF001

    assert calls == [False]
    assert result["runtime_ready"] is True
    assert result["source"] == "fresh_auth_gate"


def test_load_or_refresh_auth_gate_result_rechecks_stale_unready_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    stale_checked_at = "2026-04-10T11:00:00+00:00"
    service._auth_cache_path.write_text(  # noqa: SLF001
        json.dumps(
            {
                "runtime_ready": False,
                "refresh_checked_at": stale_checked_at,
                "detail": "refresh_token_authentication_error",
                "next_action": "Auth Gate Check",
            }
        ),
        encoding="utf-8",
    )
    fresh_payload = {
        "runtime_ready": False,
        "refresh_checked_at": "2026-04-10T11:01:00+00:00",
        "detail": "refresh_token_authentication_error",
        "next_action": "Auth Gate Check",
        "source": "fresh_auth_gate",
    }
    monkeypatch.setattr(service, "_run_auth_gate_result", lambda: fresh_payload)

    result = service._load_or_refresh_auth_gate_result(run_if_missing=True)  # noqa: SLF001

    assert result["source"] == "fresh_auth_gate"
    assert result["refresh_checked_at"] == "2026-04-10T11:01:00+00:00"


def test_snapshot_exposes_dashboard_recovery_metadata_when_auth_is_not_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    monkeypatch.setattr(
        service,
        "_load_or_refresh_auth_gate_result",
        lambda run_if_missing: {
            "runtime_ready": False,
            "refresh_checked_at": "2026-04-10T11:00:00+00:00",
            "detail": "refresh_token_authentication_error",
            "next_action": "Auth Gate Check",
            "source": "test_fixture",
        },
    )

    snapshot = service.snapshot()

    recovery = snapshot["dashboard_recovery"]
    assert recovery["state"] == "MANUAL_ACTION_REQUIRED"
    assert recovery["active"] is False
    assert recovery["recommended_action"] == "Manual inspection required"
    assert recovery["next_recovery_attempt_at"] == "2026-04-10T11:00:30+00:00"
    assert recovery["primary_target"] == "paper_runtime"
    auth_target = next(target for target in recovery["targets"] if target["target"] == "auth_gate")
    assert auth_target["state"] == "RECOVERING"
    assert auth_target["recommended_action"] == "Wait for recovery"
    assert snapshot["dashboard_meta"]["recovery"]["state"] == "MANUAL_ACTION_REQUIRED"


def test_snapshot_marks_auth_dependency_as_warming_when_auto_recovery_is_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())
    monkeypatch.setattr(
        service,
        "_load_or_refresh_auth_gate_result",
        lambda run_if_missing: {
            "runtime_ready": False,
            "refresh_checked_at": "2026-04-10T11:00:00+00:00",
            "detail": "refresh_token_authentication_error",
            "next_action": "Auth Gate Check",
            "source": "test_fixture",
        },
    )
    monkeypatch.setattr(
        service,
        "_normalize_auth_status_for_attached_runtime",
        lambda auth_status, *, paper: auth_status,
    )

    snapshot = service.snapshot()
    auth_row = next(
        row for row in snapshot["startup_control_plane"]["dependencies"] if row["key"] == "schwab_connectivity"
    )

    assert auth_row["state"] == "WARMING"
    assert auth_row["action_required_now"] is False
    assert auth_row["clears_automatically"] is True
    assert auth_row["next_action_label"] == "Wait for recovery"
    assert auth_row["next_recovery_attempt_at"] == "2026-04-10T11:00:30+00:00"


def test_startup_control_plane_does_not_block_ibkr_databento_route_on_schwab_sidecar_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())
    service._server_info = SimpleNamespace(
        pid=1,
        url="http://127.0.0.1:8790/",
        info_file="dashboard.json",
        instance_id="test-instance",
    )
    with service._dashboard_probe_lock:
        service._dashboard_probe["api_dashboard_responding"] = True
        service._dashboard_probe["operator_surface_loadable"] = True

    monkeypatch.setattr(
        service,
        "_auth_recovery_state",
        lambda auth_status: {
            "runtime_ready": False,
            "reason": "refresh_token_authentication_error",
            "auto_recovery_active": True,
            "recommended_action": "Wait for recovery",
            "next_recovery_attempt_at": "2026-04-10T11:00:30+00:00",
            "manual_action_required": False,
        },
    )
    monkeypatch.setattr(
        service,
        "_schwab_sidecar_requirement",
        lambda: {
            "explicitly_required": False,
            "required_for_current_route": False,
            "provider_roles": {
                "market_data_primary": "databento",
                "market_data_fallback": "schwab",
                "broker_truth_provider": "ibkr",
                "execution_provider": "ibkr",
            },
        },
    )

    payload = service._startup_control_plane_payload(
        generated_at="2026-04-10T11:00:00+00:00",
        auth_status={"source": "test_fixture"},
        market_context={
            "feed_state": "UNAVAILABLE",
            "note": "Market-index fetch failed.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": True,
            "runtime_recovery": {},
            "readiness": {
                "runtime_running": True,
                "heartbeat_reconciliation_summary": {},
                "order_timeout_watchdog_summary": {},
                "restore_validation_summary": {},
            },
            "status": {
                "entries_enabled": True,
                "operator_halt": False,
                "reconciliation_semantics": "CLEAR",
            },
            "entry_eligibility": {},
        },
    )

    auth_row = next(row for row in payload["dependencies"] if row["key"] == "schwab_connectivity")
    market_row = next(row for row in payload["dependencies"] if row["key"] == "market_data_connectivity")

    assert auth_row["state"] == "WARMING"
    assert auth_row["reason_code"] == "schwab_auth_sidecar_unavailable"
    assert auth_row["launch_blocking"] is False
    assert market_row["state"] == "READY"
    assert market_row["reason_code"] == "market_data_runtime_attached"
    assert payload["launch_allowed"] is True


def test_startup_control_plane_distinguishes_post_flatten_recovery_from_emergency_lockout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())
    service._server_info = SimpleNamespace(
        pid=1,
        url="http://127.0.0.1:8790/",
        info_file="dashboard.json",
        instance_id="test-instance",
    )
    with service._dashboard_probe_lock:
        service._dashboard_probe["api_dashboard_responding"] = True
        service._dashboard_probe["operator_surface_loadable"] = True
    monkeypatch.setattr(
        service,
        "_auth_recovery_state",
        lambda auth_status: {
            "runtime_ready": True,
            "reason": "Auth ready.",
            "auto_recovery_active": False,
            "recommended_action": "No action needed",
            "manual_action_required": False,
        },
    )

    base_paper = {
        "running": False,
        "readiness": {
            "runtime_running": False,
            "heartbeat_reconciliation_summary": {},
            "order_timeout_watchdog_summary": {},
            "restore_validation_summary": {},
        },
        "status": {
            "entries_enabled": True,
            "operator_halt": False,
            "reconciliation_semantics": "CLEAR",
        },
        "entry_eligibility": {},
    }
    recovery_payload = service._startup_control_plane_payload(
        generated_at="2026-04-10T11:00:00+00:00",
        auth_status={"source": "test_fixture"},
        market_context={"feed_state": "LIVE", "note": "Live."},
        paper={
            **base_paper,
            "runtime_recovery": {
                "status": "VERIFYING_FLAT",
                "auto_restart_eligible": True,
                "operator_message": "Paper runtime stopped; verifying flat post-flatten reconciliation.",
                "next_action": "Wait for broker reconciliation refresh",
            },
        },
    )
    emergency_payload = service._startup_control_plane_payload(
        generated_at="2026-04-10T11:00:00+00:00",
        auth_status={"source": "test_fixture"},
        market_context={"feed_state": "LIVE", "note": "Live."},
        paper={
            **base_paper,
            "runtime_recovery": {
                "status": "EMERGENCY_HALT_OPERATOR_LOCKOUT",
                "manual_action_required": True,
                "operator_message": "Paper runtime stopped after an explicit emergency halt/operator lockout.",
                "next_action": "Start Runtime",
            },
        },
    )

    recovery_row = next(row for row in recovery_payload["dependencies"] if row["key"] == "paper_runtime")
    emergency_row = next(row for row in emergency_payload["dependencies"] if row["key"] == "paper_runtime")
    assert recovery_row["state"] == "WARMING"
    assert recovery_row["manual_intervention_required"] is False
    assert recovery_row["clears_automatically"] is True
    assert emergency_row["state"] == "BLOCKED"
    assert emergency_row["manual_intervention_required"] is True
    assert emergency_row["reason_code"] == "paper_runtime_emergency_halt_operator_lockout"


def test_start_paper_precheck_does_not_require_schwab_auth_for_ibkr_databento_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())
    monkeypatch.setattr(
        service,
        "_schwab_sidecar_requirement",
        lambda: {
            "explicitly_required": False,
            "required_for_current_route": False,
            "provider_roles": {
                "market_data_primary": "databento",
                "market_data_fallback": "schwab",
                "broker_truth_provider": "ibkr",
                "execution_provider": "ibkr",
            },
        },
    )
    monkeypatch.setattr(service, "_launch_gate_auth_status", lambda: {"runtime_ready": False, "next_action": "Auth Gate Check"})
    monkeypatch.setattr(
        service,
        "_runtime_snapshot",
        lambda runtime_name: (
            {
                "running": False,
                "status": {"fault_state": "OK", "operator_halt": False},
                "desk_risk": {"desk_risk_state": "OK"},
                "lane_risk": {"lanes": []},
            }
            if runtime_name == "paper"
            else {"running": False}
        ),
    )
    monkeypatch.setattr(service, "_review_payload", lambda paper, scope: {})
    monkeypatch.setattr(service, "_paper_carry_forward_state", lambda paper, review: {"active": False})
    monkeypatch.setattr(service, "_paper_pre_session_review_state", lambda carry: {"ready_for_run": True})

    result = service._prechecked_action_result("start-paper")

    assert result is None


def test_snapshot_dashboard_recovery_includes_paper_runtime_auto_restart_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(Path.cwd())
    monkeypatch.setattr(
        service,
        "_load_or_refresh_auth_gate_result",
        lambda run_if_missing: {
            "runtime_ready": True,
            "refresh_checked_at": "2026-04-10T11:00:00+00:00",
            "detail": "Token is runtime-ready.",
            "next_action": "No action needed",
            "source": "test_fixture",
        },
    )
    monkeypatch.setattr(
        service,
        "_paper_runtime_recovery_payload",
        lambda **kwargs: (
            {
                "status": "AUTO_RESTART_BACKOFF",
                "auto_restart_eligible": True,
                "manual_action_required": False,
                "restart_backoff_until": "2026-04-10T11:02:00+00:00",
                "next_action": "Wait for the next readiness refresh",
                "operator_message": "Paper runtime stopped; auto-restart backoff is active.",
            },
            None,
            None,
        ),
    )

    snapshot = service.snapshot()
    recovery = snapshot["dashboard_recovery"]
    paper_target = next(target for target in recovery["targets"] if target["target"] == "paper_runtime")

    assert recovery["state"] == "RECOVERING"
    assert paper_target["state"] == "RECOVERING"
    assert paper_target["active"] is True
    assert paper_target["next_recovery_attempt_at"] == "2026-04-10T11:02:00+00:00"


def test_paper_latest_operator_control_prefers_operator_status_and_ignores_legacy_file(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    runtime_dir = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "operator_control.json").write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "status": "pending",
                "requested_at": "2026-04-10T22:51:20+00:00",
                "lane_id": "lane_a",
            }
        ),
        encoding="utf-8",
    )
    (runtime_dir / "atp_companion_v1_operator_control.json").write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "status": "applied",
                "requested_at": "2026-04-10T22:51:20+00:00",
                "applied_at": "2026-04-10T22:52:07+00:00",
                "lane_id": "lane_a",
                "control_path": str(runtime_dir / "atp_companion_v1_operator_control.json"),
            }
        ),
        encoding="utf-8",
    )

    result = service._paper_latest_operator_control(  # noqa: SLF001
        runtime_artifacts_dir=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        operator_status={
            "latest_operator_control": {
                "status": "applied",
                "requested_at": "2026-04-10T22:51:20+00:00",
                "applied_at": "2026-04-10T22:52:12+00:00",
                "lane_id": "lane_a",
                "control_path": str(runtime_dir / "operator_control.json"),
            }
        },
    )

    assert result["status"] == "applied"
    assert result["applied_at"] == "2026-04-10T22:52:12+00:00"


def test_paper_lane_fallback_status_treats_clean_lane_artifacts_as_restartable_when_top_level_status_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_artifacts / "runtime"
    lane_dir = paper_artifacts / "lanes" / "mgc_us_late_pause_resume_long"
    runtime_dir.mkdir(parents=True)
    lane_dir.mkdir(parents=True)

    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "database_url": f"sqlite:///{repo_root / 'paper__mgc_us_late_pause_resume_long.sqlite3'}",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "mgc_us_late_pause_resume_long",
                "display_name": "MGC / usLatePauseResumeLongTurn",
                "symbol": "MGC",
                "entries_enabled": True,
                "operator_halt": False,
                "position_side": "FLAT",
                "strategy_status": "READY",
                "processed_bars": 12,
                "new_bars_last_cycle": 1,
                "updated_at": "2026-04-02T12:31:29.376148+00:00",
                "last_processed_bar_end_ts": "2026-04-02T12:30:00+00:00",
                "reconciliation": {
                    "clean": True,
                    "reconcile_required": False,
                    "broker_snapshot": {"connected": True},
                },
                "heartbeat_reconciliation": {
                    "status": "CLEAN",
                    "last_completed_at": "2026-04-02T12:31:29.375513+00:00",
                },
                "startup_restore_validation": {
                    "restore_result": "READY",
                    "clean": True,
                    "reconcile_required": False,
                    "unresolved_restore_issue": False,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    monkeypatch.setattr(
        service,
        "_runtime_paths",
        lambda runtime_name: {
            "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
            "pid_file": repo_root / f"{runtime_name}.pid",
            "log_file": repo_root / f"{runtime_name}.log",
            "db_path": None,
        },
    )

    paper = service._runtime_snapshot("paper")
    paper["readiness"] = service._paper_readiness_payload(paper)
    paper["entry_eligibility"] = service._paper_entry_eligibility_payload(
        paper,
        {"required": False, "completed": True},
    )

    assert paper["running"] is False
    assert paper["status"]["reconciliation_clean"] is True
    assert paper["status"]["entries_enabled"] is True
    assert paper["status"]["strategy_status"] == "READY"
    assert paper["entry_eligibility"]["primary_blocking_reason"] != "RECONCILIATION_DIRTY"


def test_runtime_snapshot_prefers_current_paper_config_lanes_over_stale_operator_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_artifacts / "runtime"
    current_lane_id = "gc_1x_all_lanes__ny_late_short"
    legacy_lane_id = "atp_companion_v1_asia_us"
    current_lane_dir = paper_artifacts / "lanes" / current_lane_id
    current_lane_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    legacy_db = repo_root / "paper__legacy.sqlite3"
    current_db = repo_root / "paper__current.sqlite3"
    _init_empty_dashboard_db(legacy_db)
    _init_empty_dashboard_db(current_db)

    with sqlite3.connect(legacy_db) as connection:
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            ("legacy-fill", "BUY_TO_CLOSE", "FILLED", "2026-04-21T11:34:00-04:00", "999.9", "legacy-broker-order"),
        )
        connection.commit()

    with sqlite3.connect(current_db) as connection:
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            ("current-fill", "BUY_TO_CLOSE", "FILLED", "2026-04-21T11:18:00-04:00", "4770.3", "current-broker-order"),
        )
        connection.commit()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "active_lane_ids": [legacy_lane_id],
                "entries_enabled": True,
                "lanes": [
                    {
                        "lane_id": legacy_lane_id,
                        "display_name": "Legacy ATP lane",
                        "symbol": "MGC",
                        "database_url": f"sqlite:///{legacy_db}",
                    }
                ],
                "position_side": "FLAT",
                "strategy_status": "RUNNING",
                "updated_at": "2026-04-21T10:34:00-04:00",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-21T11:19:00-04:00",
                "lanes": [
                    {
                        "lane_id": current_lane_id,
                        "display_name": "GC 1x All Lanes / NY Late Short",
                        "symbol": "GC",
                        "session_restriction": "NY_LATE",
                        "artifacts_dir": str(current_lane_dir),
                        "database_url": f"sqlite:///{current_db}",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_lane_risk_status.json").write_text(json.dumps({"lanes": []}), encoding="utf-8")
    (current_lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "display_name": "GC 1x All Lanes / NY Late Short",
                "symbol": "GC",
                "entries_enabled": True,
                "operator_halt": False,
                "position_side": "FLAT",
                "strategy_status": "READY",
                "processed_bars": 8,
                "new_bars_last_cycle": 1,
                "updated_at": "2026-04-21T11:18:30-04:00",
                "last_processed_bar_end_ts": "2026-04-21T11:18:00-04:00",
                "reconciliation": {
                    "clean": True,
                    "reconcile_required": False,
                    "broker_snapshot": {"connected": True},
                },
                "heartbeat_reconciliation": {
                    "status": "CLEAN",
                    "last_completed_at": "2026-04-21T11:18:30-04:00",
                },
                "startup_restore_validation": {
                    "restore_result": "READY",
                    "clean": True,
                    "reconcile_required": False,
                    "unresolved_restore_issue": False,
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    monkeypatch.setattr(
        service,
        "_runtime_paths",
        lambda runtime_name: {
            "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
            "pid_file": repo_root / f"{runtime_name}.pid",
            "log_file": repo_root / f"{runtime_name}.log",
            "db_path": None,
        },
    )

    paper = service._runtime_snapshot("paper")

    assert paper["raw_operator_status"]["active_lane_ids"] == [current_lane_id]
    assert [row["lane_id"] for row in paper["raw_operator_status"]["lanes"]] == [current_lane_id]
    assert paper["latest_fills"][0]["broker_order_id"] == "current-broker-order"
    assert paper["latest_fills"][0]["fill_price"] == "4770.3"


def test_runtime_snapshot_prefers_newer_operator_status_lane_universe_when_config_in_force_is_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_artifacts / "runtime"
    current_lane_id = "gc_1x_all_lanes__ny_late_short"
    legacy_lane_id = "atp_companion_v1_asia_us"
    current_lane_dir = paper_artifacts / "lanes" / current_lane_id
    current_lane_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    current_db = repo_root / "paper__current.sqlite3"
    legacy_db = repo_root / "paper__legacy.sqlite3"
    _init_empty_dashboard_db(current_db)
    _init_empty_dashboard_db(legacy_db)

    with sqlite3.connect(current_db) as connection:
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            ("current-fill", "BUY_TO_CLOSE", "FILLED", "2026-04-21T11:18:00-04:00", "4770.3", "current-broker-order"),
        )
        connection.commit()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "active_lane_ids": [current_lane_id],
                "entries_enabled": True,
                "lanes": [
                    {
                        "lane_id": current_lane_id,
                        "display_name": "GC 1x All Lanes / NY Late Short",
                        "symbol": "GC",
                        "database_url": f"sqlite:///{current_db}",
                    }
                ],
                "position_side": "FLAT",
                "strategy_status": "RUNNING",
                "updated_at": "2026-04-21T11:19:00-04:00",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-21T11:10:00-04:00",
                "lanes": [
                    {
                        "lane_id": legacy_lane_id,
                        "display_name": "Legacy ATP lane",
                        "symbol": "MGC",
                        "session_restriction": "ASIA/US",
                        "artifacts_dir": str(paper_artifacts / "lanes" / legacy_lane_id),
                        "database_url": f"sqlite:///{legacy_db}",
                    }
                ]
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "paper_lane_risk_status.json").write_text(json.dumps({"lanes": []}), encoding="utf-8")

    service = OperatorDashboardService(repo_root)
    monkeypatch.setattr(
        service,
        "_runtime_paths",
        lambda runtime_name: {
            "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
            "pid_file": repo_root / f"{runtime_name}.pid",
            "log_file": repo_root / f"{runtime_name}.log",
            "db_path": None,
        },
    )

    paper = service._runtime_snapshot("paper")

    assert paper["raw_operator_status"]["active_lane_ids"] == [current_lane_id]
    assert [row["lane_id"] for row in paper["raw_operator_status"]["lanes"]] == [current_lane_id]
    assert paper["latest_fills"][0]["broker_order_id"] == "current-broker-order"


def test_restart_paper_with_temp_paper_ignores_missing_pid_and_surfaces_temp_paper_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    paper_state = {
        "running": False,
        "status": {"fault_state": "CLEAR", "operator_halt": False},
        "operator_state": {"flatten_pending": False, "stop_after_cycle_requested": False},
        "desk_risk": {"desk_risk_state": "OK"},
        "lane_risk": {"lanes": []},
    }
    pre_snapshot = {
        "paper": {
            "running": False,
            "temporary_paper_runtime_integrity": {
                "missing_lane_ids": [],
                "unresolved_start_lane_ids": [],
                "summary_line": "clear",
            },
        }
    }
    post_snapshot = {
        "paper": {
            "running": True,
            "temporary_paper_runtime_integrity": {
                "missing_lane_ids": ["temp_lane_gc"],
                "unresolved_start_lane_ids": [],
                "summary_line": "Enabled in app: 1 | loaded in runtime: 0",
            },
        }
    }
    snapshots = [pre_snapshot, post_snapshot, post_snapshot]
    monkeypatch.setattr(service, "snapshot", lambda: snapshots.pop(0) if snapshots else post_snapshot)
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda current_snapshot: (
            ["bash", "scripts/run_probationary_paper_soak.sh", "--background", "--include-temp-lane-gc"],
            {"unresolved_lane_ids": [], "requested_start_flags": ["--include-temp-lane-gc"]},
        ),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: paper_state if runtime_name == "paper" else {"running": False})
    monkeypatch.setattr(service, "_review_payload", lambda paper, scope: {})
    monkeypatch.setattr(service, "_paper_carry_forward_state", lambda paper, review: {"active": False})
    monkeypatch.setattr(service, "_paper_pre_session_review_state", lambda carry: {"ready_for_run": True})
    monkeypatch.setattr(service, "_load_or_refresh_auth_gate_result", lambda run_if_missing: {"runtime_ready": True})
    monkeypatch.setattr(service, "_prechecked_action_result", lambda action: None if action == "start-paper" else None)

    def _fake_run(command, **kwargs):
        if command == ["bash", "scripts/stop_probationary_paper_soak.sh"]:
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=f"No probationary paper PID file found at {tmp_path / 'probationary_paper.pid'}.",
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="paper started", stderr="")

    monkeypatch.setattr(operator_dashboard_module.subprocess, "run", _fake_run)

    result = service.run_action("restart-paper-with-temp-paper")

    assert result["ok"] is False
    assert result["reason_code"] == "TEMP_PAPER_RUNTIME_MISMATCH"
    assert result["next_action"] == "Restart Runtime + Temp Paper"
    assert "TEMP_PAPER_RUNTIME_MISMATCH" in str(result["message"])
    assert str(result["detail"]) == "TEMP_PAPER_RUNTIME_MISMATCH | Next action: Restart Runtime + Temp Paper"
    assert "enabled temporary paper lanes were not loaded" in str(result["output"]).lower()
    assert "temp_lane_gc" in str(result["output"])
    assert "No probationary paper PID file found" not in str(result["output"])
    assert result["snapshot"] == post_snapshot


def test_restart_paper_with_temp_paper_restarts_cleanly_when_runtime_is_already_stopped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    paper_state = {
        "running": False,
        "status": {"fault_state": "CLEAR", "operator_halt": False},
        "operator_state": {"flatten_pending": False, "stop_after_cycle_requested": False},
        "desk_risk": {"desk_risk_state": "OK"},
        "lane_risk": {"lanes": []},
    }
    pre_snapshot = {
        "paper": {
            "running": False,
            "temporary_paper_runtime_integrity": {
                "missing_lane_ids": [],
                "unresolved_start_lane_ids": [],
                "summary_line": "clear",
            },
        }
    }
    post_snapshot = {
        "paper": {
            "running": True,
            "temporary_paper_runtime_integrity": {
                "missing_lane_ids": [],
                "unresolved_start_lane_ids": [],
                "summary_line": "clear",
            },
        }
    }
    snapshots = [pre_snapshot, post_snapshot, post_snapshot]
    monkeypatch.setattr(service, "snapshot", lambda: snapshots.pop(0) if snapshots else post_snapshot)
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda current_snapshot: (
            ["bash", "scripts/run_probationary_paper_soak.sh", "--background"],
            {"unresolved_lane_ids": [], "requested_start_flags": []},
        ),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: paper_state if runtime_name == "paper" else {"running": False})
    monkeypatch.setattr(service, "_review_payload", lambda paper, scope: {})
    monkeypatch.setattr(service, "_paper_carry_forward_state", lambda paper, review: {"active": False})
    monkeypatch.setattr(service, "_paper_pre_session_review_state", lambda carry: {"ready_for_run": True})
    monkeypatch.setattr(service, "_load_or_refresh_auth_gate_result", lambda run_if_missing: {"runtime_ready": True})
    monkeypatch.setattr(service, "_prechecked_action_result", lambda action: None if action == "start-paper" else None)

    def _fake_run(command, **kwargs):
        if command == ["bash", "scripts/stop_probationary_paper_soak.sh"]:
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=f"No probationary paper PID file found at {tmp_path / 'probationary_paper.pid'}.",
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="paper started", stderr="")

    monkeypatch.setattr(operator_dashboard_module.subprocess, "run", _fake_run)

    result = service.run_action("restart-paper-with-temp-paper")

    assert result["ok"] is True
    assert "paper started" in str(result["output"]).lower()
    assert "No probationary paper PID file found" not in str(result["output"])
    assert result["snapshot"] == post_snapshot
    assert "No probationary paper PID file found" in str(result["stop_output"])


def test_dashboard_snapshot_writes_paper_performance_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-18T14:05:00-04:00",
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 1,
                    "broker_average_price": "100.0",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps({"realized_net_pnl": "25.0", "session_date": "2026-03-18", "closed_trade_count": 1}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-18T14:05:00-04:00,2026-03-18T14:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,100.5,5.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()
    performance_path = repo_root / "outputs" / "operator_dashboard" / "paper_performance_snapshot.json"

    assert performance_path.exists()
    written = json.loads(performance_path.read_text(encoding="utf-8"))
    assert written["realized_pnl"] == snapshot["paper"]["performance"]["realized_pnl"]
    assert service.operator_artifact_file("paper-performance")[0] == performance_path


def test_dashboard_snapshot_surfaces_strategy_performance_by_lane_and_instrument(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    mgc_lane_db = repo_root / "paper__mgc_bull.sqlite3"
    gc_lane_db = repo_root / "paper__gc_bear.sqlite3"
    _init_strategy_lane_dashboard_db(
        mgc_lane_db,
        symbol="MGC",
        entry_reason="bullSnap",
        closed_trade_pnl=None,
    )
    _init_strategy_lane_dashboard_db(
        gc_lane_db,
        symbol="GC",
        entry_reason="asiaVwapReclaim",
        closed_trade_pnl="25.0",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "mgc_bull",
                        "display_name": "MGC Bull",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "LONG",
                        "strategy_status": "IN_LONG_K",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "12.5",
                        "session_total_pnl": "12.5",
                        "entry_timestamp": "2026-03-22T13:35:00-04:00",
                        "entry_price": "100.0",
                        "last_mark": "101.25",
                        "point_value": "10",
                        "database_url": f"sqlite:///{mgc_lane_db}",
                    },
                    {
                        "lane_id": "gc_vwap",
                        "display_name": "GC VWAP",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "25.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "25.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{gc_lane_db}",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    strategy_rows = {row["lane_id"]: row for row in snapshot["paper"]["strategy_performance"]["rows"]}
    assert set(strategy_rows) == {"mgc_bull", "gc_vwap"}
    assert strategy_rows["mgc_bull"]["instrument"] == "MGC"
    assert strategy_rows["mgc_bull"]["standalone_strategy_id"] == "bull_snap__MGC"
    assert strategy_rows["mgc_bull"]["status"] == "OPEN_LONG"
    assert strategy_rows["mgc_bull"]["unrealized_pnl"] == "12.5"
    assert strategy_rows["gc_vwap"]["instrument"] == "GC"
    assert strategy_rows["gc_vwap"]["standalone_strategy_id"] == "asia_vwap_reclaim__GC"
    assert strategy_rows["gc_vwap"]["realized_pnl"] == "25.0"
    assert strategy_rows["gc_vwap"]["day_pnl"] == "25.0"
    assert strategy_rows["gc_vwap"]["trade_count"] == 1
    assert strategy_rows["gc_vwap"]["entry_count"] == 1
    assert strategy_rows["gc_vwap"]["expected_fire_cadence"] == "insufficient history"

    trade_log = snapshot["paper"]["strategy_performance"]["trade_log"]
    assert len(trade_log) == 1
    assert trade_log[0]["lane_id"] == "gc_vwap"
    assert trade_log[0]["instrument"] == "GC"
    assert trade_log[0]["standalone_strategy_id"] == "asia_vwap_reclaim__GC"
    assert trade_log[0]["signal_family_label"] == "VWAP reclaim"

    attribution_rows = {
        row["family_label"]: row for row in snapshot["paper"]["strategy_performance"]["attribution"]["rows"]
    }
    assert attribution_rows["VWAP reclaim"]["realized_pnl"] == "25.0"
    assert attribution_rows["VWAP reclaim"]["standalone_strategy_ids"] == ["asia_vwap_reclaim__GC"]

    portfolio_snapshot = snapshot["paper"]["strategy_performance"]["portfolio_snapshot"]
    assert portfolio_snapshot["total_realized_pnl"] == "25.0"
    assert portfolio_snapshot["total_unrealized_pnl"] == "12.5"
    assert portfolio_snapshot["total_day_pnl"] == "37.5"
    assert portfolio_snapshot["active_strategy_count"] == 2
    assert portfolio_snapshot["active_instrument_count"] == 2

    runtime_summary = snapshot["paper"]["strategy_runtime_summary"]
    assert runtime_summary["configured_standalone_strategies"] == 2
    assert runtime_summary["runtime_instances_present"] == 2
    assert runtime_summary["runtime_states_loaded"] == 0
    assert runtime_summary["can_process_bars"] == 2
    assert runtime_summary["in_position_strategies"] == 1


def test_dashboard_snapshot_rebuilds_empty_fresh_paper_strategy_performance_cache_when_runtime_has_lanes(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    lane_db = repo_root / "paper__mes_late.sqlite3"
    _init_strategy_lane_dashboard_db(
        lane_db,
        symbol="MES",
        entry_reason="indexNyLateLongV5",
        closed_trade_pnl=None,
    )

    operator_status = {
        "updated_at": "2026-03-22T13:50:00-04:00",
        "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
        "position_side": "LONG",
        "strategy_status": "RUNNING_MULTI_LANE",
        "entries_enabled": True,
        "operator_halt": False,
        "current_detected_session": "US_LATE",
        "active_lane_ids": ["mes_us_late_long"],
        "lanes": [
            {
                "lane_id": "mes_us_late_long",
                "display_name": "MES / US_LATE_LONG / x1",
                "symbol": "MES",
                "approved_long_entry_sources": ["indexNyLateLongV5"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "risk_state": "OK",
                "session_realized_pnl": "0",
                "session_unrealized_pnl": "10.0",
                "session_total_pnl": "10.0",
                "entry_timestamp": "2026-03-22T13:35:00-04:00",
                "entry_price": "100.0",
                "last_mark": "101.0",
                "point_value": "10",
                "database_url": f"sqlite:///{lane_db}",
            }
        ],
    }
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(operator_status) + "\n",
        encoding="utf-8",
    )

    strategy_performance_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    strategy_performance_path.parent.mkdir(parents=True, exist_ok=True)
    strategy_performance_path.write_text(
        json.dumps(
            {
                "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "session_date": "2026-03-22",
                "rows": [],
                "trade_log": [],
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    strategy_rows = snapshot["paper"]["strategy_performance"]["rows"]
    assert len(strategy_rows) == 1
    assert strategy_rows[0]["lane_id"] == "mes_us_late_long"

    cached = json.loads(strategy_performance_path.read_text(encoding="utf-8"))
    assert len(cached["rows"]) == 1
    assert cached["rows"][0]["lane_id"] == "mes_us_late_long"


def test_paper_strategy_performance_preserves_live_lane_pnl_when_status_rows_are_synthesized(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "mes_us_late_long"
    lane_dir.mkdir(parents=True, exist_ok=True)
    lane_db = repo_root / "paper__mes_late.sqlite3"
    _init_empty_dashboard_db(lane_db)

    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "mes_us_late_long",
                "display_name": "MES / US_LATE_LONG / x1",
                "symbol": "MES",
                "position_side": "LONG",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "reconciliation": {
                    "broker_position_quantity": 1,
                    "broker_average_price": "100.0",
                    "strategy_open_broker_order_id": "paper-order-1",
                },
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    merged_status = service._paper_operator_status_with_lane_fallback(
        {
            "updated_at": "2026-03-22T13:50:00-04:00",
            "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
            "position_side": "LONG",
            "strategy_status": "RUNNING_MULTI_LANE",
            "entries_enabled": True,
            "operator_halt": False,
            "current_detected_session": "US_LATE",
            "active_lane_ids": ["mes_us_late_long"],
            "lanes": [],
        },
        {
            "generated_at": "2026-03-22T13:49:00-04:00",
            "lanes": [
                {
                    "lane_id": "mes_us_late_long",
                    "display_name": "MES / US_LATE_LONG / x1",
                    "symbol": "MES",
                    "session_restriction": "US_LATE",
                    "long_sources": ["indexUsLateLongV5"],
                    "short_sources": [],
                    "artifacts_dir": str(lane_dir),
                    "database_url": f"sqlite:///{lane_db}",
                }
            ],
        },
        {
            "lanes": [
                {
                    "lane_id": "mes_us_late_long",
                    "risk_state": "OK",
                    "halt_reason": None,
                    "unblock_action": None,
                    "realized_losing_trades": 0,
                    "session_realized_pnl": "2.5",
                    "session_unrealized_pnl": "10.0",
                    "session_total_pnl": "12.5",
                }
            ]
        },
        paper_artifacts,
        lane_db,
    )

    payload = service._paper_strategy_performance_payload(
        paper={
            "raw_operator_status": merged_status,
            "status": {"strategy_status": "RUNNING"},
            "runtime_registry": {"rows": []},
        },
        session_date="2026-03-22",
        root_db_path=lane_db,
        approved_quant_baselines={"rows": []},
    )

    row = payload["rows"][0]
    assert row["lane_id"] == "mes_us_late_long"
    assert row["position_side"] == "LONG"
    assert row["entry_price"] == "100.0"
    assert row["unrealized_pnl"] == "10.0"
    assert row["day_pnl"] == "12.5"
    assert payload["portfolio_snapshot"]["total_unrealized_pnl"] == "10.0"
    assert payload["portfolio_snapshot"]["total_day_pnl"] == "12.5"


def test_dashboard_snapshot_builds_unified_strategy_analysis_surface(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)
    historical_playback_dir = repo_root / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    lane_db = repo_root / "paper__mgc_bull.sqlite3"
    _init_strategy_lane_dashboard_db(
        lane_db,
        symbol="MGC",
        entry_reason="bullSnap",
        closed_trade_pnl="25.0",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "mgc_bull",
                        "display_name": "MGC Bull",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "25.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "25.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary_path = historical_playback_dir / "historical_playback_mgc_unified.summary.json"
    trigger_report_path = historical_playback_dir / "historical_playback_mgc_unified.trigger_report.json"
    trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_unified.trigger_report.md"
    strategy_study_path = historical_playback_dir / "historical_playback_mgc_unified.strategy_study.json"
    strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_unified.strategy_study.md"

    summary_path.write_text(
        json.dumps(
            {
                "processed_bars": 2,
                "aggregate_portfolio_summary": {
                    "standalone_strategy_ids": ["bull_snap__MGC"],
                    "standalone_strategy_count": 1,
                    "realized_pnl": "30.0",
                    "unrealized_pnl": "0",
                    "cumulative_pnl": "30.0",
                },
                "per_strategy_summaries": [
                    {
                        "standalone_strategy_id": "bull_snap__MGC",
                        "strategy_family": "bullSnap",
                        "instrument": "MGC",
                        "processed_bars": 2,
                        "order_intents": 2,
                        "fills": 2,
                        "entries": 1,
                        "exits": 1,
                        "realized_pnl": "30.0",
                    }
                ],
                "primary_standalone_strategy_id": "bull_snap__MGC",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "bullSnap",
                    "bars_processed": 2,
                    "signals_seen": 1,
                    "intents_created": 2,
                    "fills_created": 2,
                    "block_or_fault_reason": "",
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    trigger_report_md_path.write_text("# Trigger Report\n", encoding="utf-8")
    strategy_study_path.write_text(
        json.dumps(
            {
                "contract_version": "strategy_study_v3",
                "symbol": "MGC",
                "standalone_strategy_id": "bull_snap__MGC",
                "strategy_family": "bullSnap",
                "timeframe": "5m",
                "meta": {
                    "study_id": "bull-snap-replay-study",
                    "strategy_id": "bull_snap__MGC",
                    "strategy_family": "bullSnap",
                    "study_mode": "baseline_parity_mode",
                    "entry_model": "BASELINE_NEXT_BAR_OPEN",
                    "pnl_truth_basis": "BASELINE_FILL_TRUTH",
                    "coverage_start": "2026-03-22T13:35:00-04:00",
                    "coverage_end": "2026-03-22T13:45:00-04:00",
                    "timeframe_truth": {
                        "structural_signal_timeframe": "5m",
                        "execution_timeframe": "5m",
                        "artifact_timeframe": "5m",
                        "execution_timeframe_role": "matches_signal_evaluation",
                    },
                },
                "summary": {
                    "bar_count": 2,
                    "total_trades": 1,
                    "long_trades": 1,
                    "short_trades": 0,
                    "winners": 1,
                    "losers": 0,
                    "cumulative_realized_pnl": "30.0",
                    "cumulative_total_pnl": "30.0",
                    "max_drawdown": "5.0",
                    "session_level_behavior": [{"session_phase": "US", "bar_count": 2}],
                    "atp_summary": {"available": False},
                },
                "bars": [
                    {"bar_id": "bar-1", "timestamp": "2026-03-22T13:35:00-04:00", "strategy_status": "READY"},
                    {"bar_id": "bar-2", "timestamp": "2026-03-22T13:40:00-04:00", "strategy_status": "READY"},
                ],
                "trade_events": [
                    {"event_type": "ENTRY_FILL", "event_timestamp": "2026-03-22T13:35:00-04:00", "family": "bullSnap", "side": "LONG"},
                    {"event_type": "EXIT_FILL", "event_timestamp": "2026-03-22T13:40:00-04:00", "family": "bullSnap", "side": "LONG"},
                ],
                "pnl_points": [{"timestamp": "2026-03-22T13:40:00-04:00", "realized": "30.0", "open_pnl": "0", "total": "30.0"}],
                "execution_slices": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    strategy_study_md_path.write_text("# Strategy Study\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_unified.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "historical_playback_unified",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "summary_path": str(summary_path),
                        "trigger_report_json_path": str(trigger_report_path),
                        "trigger_report_markdown_path": str(trigger_report_md_path),
                        "strategy_study_json_path": str(strategy_study_path),
                        "strategy_study_markdown_path": str(strategy_study_md_path),
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()
    service._refresh_strategy_analysis_snapshot(
        historical_playback=dict(snapshot["historical_playback"]),
        paper=dict(snapshot["paper"]),
        runtime_registry={},
        lane_registry={},
    )

    strategy_analysis_path = repo_root / "outputs" / "operator_dashboard" / "strategy_analysis_snapshot.json"
    assert strategy_analysis_path.exists()
    strategy_analysis = json.loads(strategy_analysis_path.read_text(encoding="utf-8"))
    assert strategy_analysis["available"] is True
    unified_monitor = strategy_analysis["unified_monitor"]
    assert unified_monitor["available"] is True
    assert unified_monitor["grouping"]["default_group_keys"] == ["strategy_class", "instrument", "family"]
    assert unified_monitor["selection_contract"]["default_selection_behavior"]["mode"] == "select_all_visible_lanes"
    assert unified_monitor["leaderboard_views"]["rankings"]["realized_pnl"]
    detail = strategy_analysis["details_by_strategy_key"]["bull_snap__MGC"]
    lane_types = {row["lane_type"] for row in detail["lanes"]}
    assert lane_types == {"benchmark_replay", "paper_runtime"}
    benchmark_lane = next(row for row in detail["lanes"] if row["lane_type"] == "benchmark_replay")
    paper_lane = next(row for row in detail["lanes"] if row["lane_type"] == "paper_runtime")
    assert benchmark_lane["source_of_truth"]["primary_artifact"] == "strategy_study_v3"
    assert paper_lane["source_of_truth"]["primary_artifact"] == "paper_strategy_performance_snapshot"
    assert benchmark_lane["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert paper_lane["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    assert detail["comparison_presets"][0]["comparison_type"] == "benchmark_vs_paper_runtime"
    assert detail["comparison_presets"][0]["left_lane"]["lane_type"] == "benchmark_replay"
    assert detail["comparison_presets"][0]["right_lane"]["lane_type"] == "paper_runtime"
    assert detail["comparison_presets"][0]["left_lane"]["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert detail["comparison_presets"][0]["right_lane"]["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    comparison_rows = unified_monitor["comparison_rows"]
    paper_row = next(row for row in comparison_rows if row["evidence_lane_type"] == "paper_runtime")
    assert unified_monitor["chart_series"]["series_by_lane_id"][paper_row["lane_id"]]["support"]["cumulative_realized_pnl"] is True

    assert service.operator_artifact_file("strategy-analysis")[0] == strategy_analysis_path


def test_record_snapshot_warning_accepts_section_message_signature() -> None:
    token = operator_dashboard_module._SNAPSHOT_WARNINGS.set([])
    try:
        operator_dashboard_module._record_snapshot_warning(
            section="strategy_analysis",
            message="lane-local sqlite evidence is temporarily unavailable",
            severity="warning",
        )
        warnings = operator_dashboard_module._SNAPSHOT_WARNINGS.get()
    finally:
        operator_dashboard_module._SNAPSHOT_WARNINGS.reset(token)

    assert warnings == [
        {
            "section": "strategy_analysis",
            "reader": "warning",
            "path": None,
            "detail": "lane-local sqlite evidence is temporarily unavailable",
        }
    ]


def test_dashboard_strategy_performance_tags_temporary_paper_metrics_bucket(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_db = repo_root / "atpe_lane.sqlite3"
    _init_empty_dashboard_db(lane_db)

    service = OperatorDashboardService(repo_root)
    payload = service._paper_strategy_performance_payload(
        paper={
            "raw_operator_status": {
                "current_detected_session": "US",
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary",
                        "display_name": "ATPE Long Medium+High Canary",
                        "symbol": "MES",
                        "source_family": "trend_participation.pullback_continuation.long.conservative",
                        "position_side": "FLAT",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{lane_db}",
                        "experimental_status": "experimental_canary",
                        "paper_only": True,
                        "non_approved": True,
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "side": "LONG",
                    }
                ],
            },
            "status": {"strategy_status": "RUNNING"},
            "runtime_registry": {"rows": []},
        },
        session_date="2026-03-23",
        root_db_path=lane_db,
        approved_quant_baselines={"rows": []},
    )

    row = payload["rows"][0]
    assert payload["payload_version"] == DASHBOARD_PAYLOAD_SCHEMA_VERSION
    assert row["lane_id"] == "atpe_long_medium_high_canary"
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["metrics_bucket"] == "experimental_temporary_paper"
    assert row["paper_only"] is True
    assert row["non_approved"] is True
    assert payload["metrics_buckets"]["experimental_temporary_paper"]["active_strategy_count"] == 1


def test_dashboard_strategy_performance_excludes_canary_rows_marked_non_performance(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_db = repo_root / "canary_lane.sqlite3"
    _init_empty_dashboard_db(lane_db)

    service = OperatorDashboardService(repo_root)
    payload = service._paper_strategy_performance_payload(
        paper={
            "raw_operator_status": {
                "current_detected_session": "US_LATE",
                "lanes": [
                    {
                        "lane_id": "ibkr_paper_route_canary",
                        "display_name": "PAPER_ROUTE_CANARY",
                        "symbol": "MNQ",
                        "position_side": "FLAT",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{lane_db}",
                        "experimental_status": "paper_route_canary",
                        "paper_only": True,
                        "non_approved": True,
                        "exclude_from_strategy_performance": True,
                    }
                ],
            },
            "status": {"strategy_status": "RUNNING"},
            "runtime_registry": {"rows": []},
        },
        session_date="2026-03-23",
        root_db_path=lane_db,
        approved_quant_baselines={"rows": []},
    )

    assert payload["rows"] == []
    assert payload["trade_log"] == []


def test_dashboard_snapshot_includes_strategy_execution_likelihood_statistics(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    lane_db = repo_root / "paper__gc_signal.sqlite3"
    _init_empty_dashboard_db(lane_db)
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade1",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-17T18:05:00-04:00",
        entry_fill_at="2026-03-17T18:10:00-04:00",
        exit_created_at="2026-03-17T18:15:00-04:00",
        exit_fill_at="2026-03-17T18:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-1",
    )
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade2",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-18T18:05:00-04:00",
        entry_fill_at="2026-03-18T18:10:00-04:00",
        exit_created_at="2026-03-18T18:15:00-04:00",
        exit_fill_at="2026-03-18T18:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-2",
    )
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade3",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-20T03:05:00-04:00",
        entry_fill_at="2026-03-20T03:10:00-04:00",
        exit_created_at="2026-03-20T03:15:00-04:00",
        exit_fill_at="2026-03-20T03:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-3",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "gc_signal",
                        "display_name": "GC Signal",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "30.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "30.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    likelihood_rows = snapshot["paper"]["strategy_performance"]["execution_likelihood"]["rows"]
    assert len(likelihood_rows) == 1
    row = likelihood_rows[0]
    assert row["entry_count"] == 3
    assert row["entries_by_session_bucket"]["SESSION_OPEN"] == 2
    assert row["entries_by_session_bucket"]["LONDON_OPEN"] == 1
    assert row["most_common_session_bucket"] == "SESSION_OPEN"
    assert row["expected_fire_cadence"] in {"frequent", "occasional", "rare"}
    assert "SESSION_OPEN" in row["most_likely_next_window"]
    assert row["operator_interpretation_state"] == "outside_usual_window"

    strategy_performance_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    strategy_trade_log_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json"
    strategy_attribution_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_attribution_snapshot.json"
    assert strategy_performance_path.exists()
    assert strategy_trade_log_path.exists()
    assert strategy_attribution_path.exists()
    assert service.operator_artifact_file("paper-strategy-performance")[0] == strategy_performance_path


def test_dashboard_snapshot_builds_signal_intent_fill_audit_verdicts(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    no_setup_db = repo_root / "paper__no_setup.sqlite3"
    gated_db = repo_root / "paper__gated.sqlite3"
    intent_db = repo_root / "paper__intent.sqlite3"
    filled_db = repo_root / "paper__filled.sqlite3"
    mismatch_db = repo_root / "paper__mismatch.sqlite3"
    sparse_db = repo_root / "paper__sparse.sqlite3"
    for path in (no_setup_db, gated_db, intent_db, filled_db, mismatch_db, sparse_db):
        _init_empty_dashboard_db(path)

    _append_dashboard_bar(
        no_setup_db,
        bar_id="no-setup-bar-1",
        symbol="MGC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )

    _append_dashboard_bar(
        gated_db,
        bar_id="gated-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:35:00-04:00",
        end_ts="2026-03-23T09:40:00-04:00",
    )
    _append_dashboard_signal(
        gated_db,
        bar_id="gated-bar-1",
        created_at="2026-03-23T09:40:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "bullSnap",
            "short_entry_source": None,
        },
    )

    _append_dashboard_bar(
        intent_db,
        bar_id="intent-bar-1",
        symbol="SI",
        start_ts="2026-03-23T09:40:00-04:00",
        end_ts="2026-03-23T09:45:00-04:00",
    )
    _append_dashboard_signal(
        intent_db,
        bar_id="intent-bar-1",
        created_at="2026-03-23T09:45:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "asiaVwapReclaim",
            "short_entry_source": None,
        },
    )
    _append_dashboard_intent(
        intent_db,
        order_intent_id="intent-only-1",
        bar_id="intent-bar-1",
        symbol="SI",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:45:05-04:00",
        reason_code="asiaVwapReclaim",
        broker_order_id="intent-only-broker",
        order_status="SUBMITTED",
    )

    _append_dashboard_bar(
        filled_db,
        bar_id="filled-bar-1",
        symbol="CL",
        start_ts="2026-03-23T09:45:00-04:00",
        end_ts="2026-03-23T09:50:00-04:00",
    )
    _append_dashboard_signal(
        filled_db,
        bar_id="filled-bar-1",
        created_at="2026-03-23T09:50:00-04:00",
        payload={
            "long_entry_raw": False,
            "short_entry_raw": True,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": False,
            "short_entry": True,
            "long_entry_source": None,
            "short_entry_source": "bearSnap",
        },
    )
    _append_dashboard_intent(
        filled_db,
        order_intent_id="filled-1",
        bar_id="filled-bar-1",
        symbol="CL",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T09:50:05-04:00",
        reason_code="bearSnap",
        broker_order_id="filled-broker-1",
        order_status="FILLED",
    )
    _append_dashboard_fill(
        filled_db,
        order_intent_id="filled-1",
        intent_type="SELL_TO_OPEN",
        order_status="FILLED",
        fill_timestamp="2026-03-23T09:55:00-04:00",
        fill_price="100.0",
        broker_order_id="filled-broker-1",
    )

    _append_dashboard_bar(
        mismatch_db,
        bar_id="mismatch-bar-1",
        symbol="PL",
        start_ts="2026-03-23T09:50:00-04:00",
        end_ts="2026-03-23T09:55:00-04:00",
    )
    _append_dashboard_signal(
        mismatch_db,
        bar_id="mismatch-bar-1",
        created_at="2026-03-23T09:55:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "bullSnap",
            "short_entry_source": None,
        },
    )
    _append_dashboard_intent(
        mismatch_db,
        order_intent_id="mismatch-1",
        bar_id="mismatch-bar-1",
        symbol="PL",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:55:05-04:00",
        reason_code="bullSnap",
        broker_order_id="mismatch-broker-1",
        order_status="FILLED",
    )
    _append_dashboard_fill(
        mismatch_db,
        order_intent_id="mismatch-1",
        intent_type="BUY_TO_OPEN",
        order_status="FILLED",
        fill_timestamp="2026-03-23T10:00:00-04:00",
        fill_price="101.0",
        broker_order_id="mismatch-broker-1",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T10:05:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T10:00:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "no_setup",
                        "display_name": "MGC Quiet",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{no_setup_db}",
                    },
                    {
                        "lane_id": "gated_lane",
                        "display_name": "GC Gated",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": False,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "eligibility_reason": "entries_disabled",
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{gated_db}",
                    },
                    {
                        "lane_id": "intent_lane",
                        "display_name": "SI Intent",
                        "symbol": "SI",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{intent_db}",
                    },
                    {
                        "lane_id": "filled_lane",
                        "display_name": "CL Filled",
                        "symbol": "CL",
                        "approved_long_entry_sources": [],
                        "approved_short_entry_sources": ["bearSnap"],
                        "position_side": "SHORT",
                        "strategy_status": "IN_SHORT_K",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "session_unrealized_pnl": "0",
                        "database_url": f"sqlite:///{filled_db}",
                    },
                    {
                        "lane_id": "mismatch_lane",
                        "display_name": "PL Surfacing Gap",
                        "symbol": "PL",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{mismatch_db}",
                    },
                    {
                        "lane_id": "sparse_lane",
                        "display_name": "HG Sparse",
                        "symbol": "HG",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{sparse_db}",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    audit_rows = {row["lane_id"]: row for row in snapshot["paper"]["signal_intent_fill_audit"]["rows"]}
    assert audit_rows["no_setup"]["audit_verdict"] == "NO_SETUP_OBSERVED"
    assert audit_rows["no_setup"]["standalone_strategy_id"] == "bull_snap__MGC"
    assert audit_rows["gated_lane"]["audit_verdict"] == "SETUP_GATED"
    assert audit_rows["intent_lane"]["audit_verdict"] == "INTENT_NO_FILL_YET"
    assert audit_rows["filled_lane"]["audit_verdict"] == "FILLED"
    assert audit_rows["mismatch_lane"]["audit_verdict"] == "SURFACING_MISMATCH_SUSPECTED"
    assert audit_rows["sparse_lane"]["audit_verdict"] == "INSUFFICIENT_HISTORY"
    assert "entries were disabled" in audit_rows["gated_lane"]["audit_reason"]
    assert audit_rows["intent_lane"]["last_order_intent_id"] == "intent-only-1"
    assert audit_rows["filled_lane"]["last_fill_broker_order_id"] == "filled-broker-1"
    assert audit_rows["mismatch_lane"]["trade_log_rows_exist"] is False
    assert audit_rows["mismatch_lane"]["strategy_performance_row_exists"] is True
    assert audit_rows["no_setup"]["processed_bar_count"] == 1

    audit_summary = snapshot["paper"]["signal_intent_fill_audit"]["summary"]["verdict_counts"]
    assert audit_summary["NO_SETUP_OBSERVED"] == 1
    assert audit_summary["SETUP_GATED"] == 1
    assert audit_summary["INTENT_NO_FILL_YET"] == 1
    assert audit_summary["FILLED"] == 1
    assert audit_summary["SURFACING_MISMATCH_SUSPECTED"] == 1
    assert audit_summary["INSUFFICIENT_HISTORY"] == 1

    audit_path = repo_root / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    assert audit_path.exists()
    assert service.operator_artifact_file("paper-signal-intent-fill-audit")[0] == audit_path


def test_signal_intent_audit_uses_signal_time_session_and_historical_rejection(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)

    lane_db = repo_root / "paper__midday.sqlite3"
    _init_empty_dashboard_db(lane_db)
    _append_dashboard_bar(
        lane_db,
        bar_id="midday-bar-1",
        symbol="NQ",
        start_ts="2026-04-29T11:00:00-04:00",
        end_ts="2026-04-29T11:05:00-04:00",
    )
    _append_dashboard_signal(
        lane_db,
        bar_id="midday-bar-1",
        created_at="2026-04-29T11:05:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": True,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "nyEarlyBreakout",
            "short_entry_source": None,
        },
    )
    (paper_artifacts / "alerts.jsonl").write_text(
        json.dumps(
            {
                "category": "order_rejection",
                "lane_id": "midday_lane",
                "message": "Order intent for NQ was rejected before broker submission.",
                "logged_at": "2026-04-29T11:05:01-04:00",
                "occurred_at": "2026-04-29T11:05:01-04:00",
                "detail": {
                    "lane_id": "midday_lane",
                    "bar_end_ts": "2026-04-29T11:05:00-04:00",
                    "broker_stage_message": "BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    payload = service._paper_signal_intent_fill_audit_payload(
        paper={
            "status": {"session_date": "2026-04-29"},
            "raw_operator_status": {
                "current_detected_session": "UNCLASSIFIED",
                "lanes": [
                    {
                        "lane_id": "midday_lane",
                        "display_name": "NQ Midday",
                        "symbol": "NQ",
                        "approved_long_entry_sources": ["nyEarlyBreakout"],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "eligibility_reason": "wrong_session",
                        "current_detected_session": "UNCLASSIFIED",
                        "session_restriction": "US_MIDDAY",
                        "allowed_sessions": ["US"],
                        "database_url": f"sqlite:///{lane_db}",
                    }
                ],
            },
            "config_in_force": {
                "lanes": [
                    {
                        "lane_id": "midday_lane",
                        "display_name": "NQ Midday",
                        "symbol": "NQ",
                        "session_restriction": "US_MIDDAY",
                        "allowed_sessions": ["US"],
                        "long_sources": ["nyEarlyBreakout"],
                    }
                ]
            },
            "strategy_performance": {"rows": [], "trade_log": []},
        },
        session_date="2026-04-29",
        root_db_path=None,
    )

    row = payload["rows"][0]
    assert row["audit_verdict"] == "SETUP_GATED"
    assert "supervised paper route was rejected before broker submission" in row["audit_reason"]
    assert "wrong_session" not in row["audit_reason"]
    assert row["signal_time_session_label"] == "US_MIDDAY"
    assert row["signal_time_allowed_session_match"] is True
    assert "non-manual caller path" in str(row["historical_order_rejection_reason"])


def test_dashboard_snapshot_builds_recent_paper_history(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-20T14:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "30.0",
                "session_date": "2026-03-20",
                "closed_trade_count": 2,
                "fill_count": 4,
                "allowed_branch_decisions_by_source": {
                    "asiaEarlyNormalBreakoutRetestHoldTurn": 4,
                    "usLatePauseResumeLongTurn": 2,
                },
                "blocked_branch_decisions_by_source": {"asiaEarlyPauseResumeShortTurn": 1},
                "flat_at_end": True,
                "reconciliation_clean": True,
                "unresolved_open_intents": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T10:00:00-04:00,2026-03-20T10:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,101.0,10.0,LONG_TIME_EXIT\n"
        "2026-03-20T11:00:00-04:00,2026-03-20T11:10:00-04:00,LONG,usLatePauseResumeLongTurn,101.0,103.0,20.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "-10.0",
                "session_date": "2026-03-19",
                "closed_trade_count": 2,
                "fill_count": 4,
                "allowed_branch_decisions_by_source": {"asiaEarlyPauseResumeShortTurn": 3},
                "blocked_branch_decisions_by_source": {"usLatePauseResumeLongTurn": 2},
                "flat_at_end": True,
                "reconciliation_clean": True,
                "unresolved_open_intents": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-19T10:00:00-04:00,2026-03-19T10:10:00-04:00,SHORT,asiaEarlyPauseResumeShortTurn,100.0,99.0,10.0,SHORT_TIME_EXIT\n"
        "2026-03-19T11:00:00-04:00,2026-03-19T11:10:00-04:00,LONG,usLatePauseResumeLongTurn,99.0,97.0,-20.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    history = snapshot["paper"]["history"]
    assert history["recent_sessions"][0]["session_date"] == "2026-03-20"
    assert history["recent_sessions"][1]["session_date"] == "2026-03-19"
    assert history["comparison"]["latest_vs_prior_realized"] == "40.0"
    assert history["comparison"]["trend"] == "IMPROVING / LOW SAMPLE"
    assert history["comparison"]["recent_win_rate"] == "75.0%"
    assert history["distribution"]["best_session"] == "30.0"
    assert history["distribution"]["worst_session"] == "-10.0"
    assert history["drawdown"]["worst_drawdown"] == "10.0"
    assert any(row["branch"] == "usLatePauseResumeLongTurn" for row in history["branch_history"])
    assert all("stability" in row for row in history["branch_history"])
    history_path = repo_root / "outputs" / "operator_dashboard" / "paper_history_snapshot.json"
    assert history_path.exists()
    assert service.operator_artifact_file("paper-history")[0] == history_path


def test_dashboard_exposes_paper_canary_separately_from_approved_lanes(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_artifacts = paper_artifacts / "lanes" / "canary_gc_us_early_execution_once"
    (paper_artifacts / "daily").mkdir(parents=True)
    lane_artifacts.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    canary_db = repo_root / "paper__canary.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(canary_db)

    connection = sqlite3.connect(canary_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "canary-entry-intent",
                "bar-1",
                "GC",
                "BUY_TO_OPEN",
                1,
                "2026-03-20T09:35:30-04:00",
                "paperExecutionCanaryEntry",
                "paper-entry-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "canary-exit-intent",
                "bar-2",
                "GC",
                "SELL_TO_CLOSE",
                1,
                "2026-03-20T09:40:30-04:00",
                "paperExecutionCanaryExitNextBar",
                "paper-exit-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "canary-entry-intent",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-20T09:40:00-04:00",
                "3050.0",
                "paper-entry-1",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "canary-exit-intent",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-20T09:45:00-04:00",
                "3051.5",
                "paper-exit-1",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T09:45:05-04:00",
                "last_processed_bar_end_ts": "2026-03-20T09:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                "approved_short_entry_sources": [],
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                        "entries_enabled": True,
                        "database_url": f"sqlite:///{paper_db}",
                        "artifacts_dir": str(paper_artifacts / "lanes" / "mgc_us_late_pause_resume_long"),
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "entries_enabled": True,
                        "position_side": "FLAT",
                        "database_url": f"sqlite:///{canary_db}",
                        "artifacts_dir": str(lane_artifacts),
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "canary_gc_us_early_execution_once",
                "updated_at": "2026-03-20T09:45:05-04:00",
                "position_side": "FLAT",
                "last_processed_bar_end_ts": "2026-03-20T09:45:00-04:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-20T09:35:00-04:00",
                "source": "paperExecutionCanary",
                "lane_id": "canary_gc_us_early_execution_once",
                "symbol": "GC",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-20T09:45:01-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "risk_state": "OK",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,instrument,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T09:40:00-04:00,2026-03-20T09:45:00-04:00,LONG,paperExecutionCanaryEntry,GC,3050.0,3051.5,1.5,paperExecutionCanaryExitNextBar\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
        "status": {"session_date": "2026-03-20"},
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((paper_artifacts / "runtime" / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((paper_artifacts / "runtime" / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    approved_payload = service._paper_approved_models_payload(paper)
    canary_payload = service._paper_non_approved_lanes_payload(paper)

    assert approved_payload["total_count"] == 1
    assert approved_payload["rows"][0]["lane_id"] == "mgc_us_late_pause_resume_long"
    assert canary_payload["total_count"] == 1
    assert canary_payload["canary_count"] == 1
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "canary_gc_us_early_execution_once"
    assert canary_row["is_canary"] is True
    assert canary_row["non_approved"] is True
    assert canary_row["paper_only"] is True
    assert canary_row["instrument"] == "GC"
    assert canary_row["session_restriction"] == "US_EARLY_OBSERVATION"
    assert canary_row["fired"] is True
    assert canary_row["entry_completed"] is True
    assert canary_row["exit_completed"] is True
    assert canary_row["entry_state"] == "COMPLETE"
    assert canary_row["exit_state"] == "COMPLETE"
    assert canary_row["lifecycle_state"] == "ENTRY_AND_EXIT_COMPLETE"
    assert canary_row["latest_signal_label"].startswith("2026-03-20T09:35:00-04:00")
    assert canary_row["latest_fill_label"].startswith("2026-03-20T09:45:00-04:00")
    assert canary_payload["artifacts"]["snapshot"] == "/api/operator-artifact/paper-non-approved-lanes"


def test_dashboard_non_approved_canary_uses_config_in_force_lane_universe_when_operator_status_lags(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_artifacts = paper_artifacts / "lanes" / "canary_gc_us_early_execution_once"
    (paper_artifacts / "daily").mkdir(parents=True)
    lane_artifacts.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    canary_db = repo_root / "paper__canary.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(canary_db)

    connection = sqlite3.connect(canary_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "force-entry-intent",
                "bar-10",
                "GC",
                "BUY_TO_OPEN",
                1,
                "2026-03-20T12:45:00-04:00",
                "paperExecutionCanaryForceFireOnceEntry:proof",
                "paper-force-entry-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "force-exit-intent",
                "bar-11",
                "GC",
                "SELL_TO_CLOSE",
                1,
                "2026-03-20T12:50:00-04:00",
                "paperExecutionCanaryForceFireOnceExitNextBar:proof",
                "paper-force-exit-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "force-entry-intent",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-20T12:50:00-04:00",
                "3048.0",
                "paper-force-entry-1",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "force-exit-intent",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-20T12:55:00-04:00",
                "3049.0",
                "paper-force-exit-1",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T12:55:05-04:00",
                "last_processed_bar_end_ts": "2026-03-20T12:55:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                "approved_short_entry_sources": [],
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                        "entries_enabled": True,
                        "database_url": f"sqlite:///{paper_db}",
                        "artifacts_dir": str(paper_artifacts / "lanes" / "mgc_us_late_pause_resume_long"),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "canary_gc_us_early_execution_once",
                "updated_at": "2026-03-20T12:55:05-04:00",
                "position_side": "FLAT",
                "last_processed_bar_end_ts": "2026-03-20T12:55:00-04:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-20T12:45:00-04:00",
                "source": "paperExecutionCanaryForceFireOnce",
                "lane_id": "canary_gc_us_early_execution_once",
                "symbol": "GC",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-20T12:55:01-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "canary_force_fire_once_token": "proof",
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                        "database_url": f"sqlite:///{canary_db}",
                        "artifacts_dir": str(lane_artifacts),
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "risk_state": "OK",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,instrument,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T12:50:00-04:00,2026-03-20T12:55:00-04:00,LONG,paperExecutionCanaryForceFireOnceEntry:proof,GC,3048.0,3049.0,100.0,paperExecutionCanaryForceFireOnceExitNextBar:proof\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
        "status": {"session_date": "2026-03-20"},
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((paper_artifacts / "runtime" / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((paper_artifacts / "runtime" / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    canary_payload = service._paper_non_approved_lanes_payload(paper)

    assert canary_payload["total_count"] == 1
    assert canary_payload["canary_count"] == 1
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "canary_gc_us_early_execution_once"
    assert canary_row["fired"] is True
    assert canary_row["entry_completed"] is True
    assert canary_row["exit_completed"] is True
    assert canary_row["latest_fill_label"].startswith("2026-03-20T12:55:00-04:00")


def test_dashboard_auto_clears_stale_atpe_decision_without_intent_when_overnight_fill_evidence_exists(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_artifacts / "runtime"
    paper_artifacts.mkdir(parents=True)
    runtime_dir.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    atpe_db = repo_root / "paper__atpe_long_medium_high_canary__MES.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(atpe_db)

    connection = sqlite3.connect(atpe_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "MES|1m|2026-03-26T03:36:00Z",
                "MES",
                "BUY_TO_OPEN",
                1,
                "2026-03-25T23:36:00-04:00",
                "trend_participation.pullback_continuation.long.conservative",
                "paper-MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-25T23:36:00-04:00",
                "6642.75",
                "paper-MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "operator-control|1774497240000|SELL_TO_CLOSE",
                "operator-control|1774497240000",
                "MES",
                "SELL_TO_CLOSE",
                1,
                "2026-03-25T23:54:00-04:00",
                "atpe_time_stop",
                "paper-operator-control|1774497240000|SELL_TO_CLOSE",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "operator-control|1774497240000|SELL_TO_CLOSE",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-25T23:54:00-04:00",
                "6638.75",
                "paper-operator-control|1774497240000|SELL_TO_CLOSE",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-26T03:36:04.286065+00:00",
                "logged_at": "2026-03-26T03:36:04.286065+00:00",
                "source": "trend_participation.pullback_continuation.long.conservative",
                "lane_id": "atpe_long_medium_high_canary__MES",
                "symbol": "MES",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "rule_blocks.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "reconciliation_events.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "operator_controls.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-26T04:09:25.170322-04:00",
                "last_processed_bar_end_ts": "2026-03-26T04:09:00-04:00",
                "position_side": "FLAT",
                "entries_enabled": True,
                "operator_halt": False,
                "strategy_status": "RUNNING_MULTI_LANE",
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "display_name": "ATPE Long Medium+High Canary / MES",
                        "symbol": "MES",
                        "approved_long_entry_sources": ["trend_participation.pullback_continuation.long.conservative"],
                        "entries_enabled": True,
                        "position_side": "FLAT",
                        "internal_position_qty": 0,
                        "broker_position_qty": 0,
                        "open_order_count": 0,
                        "fill_count": 2,
                        "intent_count": 2,
                        "database_url": f"sqlite:///{atpe_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "display_name": "ATPE Long Medium+High Canary / MES",
                        "symbol": "MES",
                        "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                        "runtime_kind": "atpe_canary_observer",
                        "database_url": f"sqlite:///{atpe_db}",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "risk_state": "HALTED_DEGRADATION",
                        "halt_reason": "lane_realized_loser_limit_per_session",
                        "unblock_action": "Next session reset required",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
            "status": {
                "session_date": "2026-03-25",
                "last_update_ts": "2026-03-26T08:30:11.670560+00:00",
            "reconciliation_clean": True,
            "entries_enabled": True,
        },
        "running": True,
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((runtime_dir / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((runtime_dir / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": operator_dashboard_module._latest_table_rows_across_paths([atpe_db], "fills", "fill_timestamp", 25),
        "latest_intents": operator_dashboard_module._latest_table_rows_across_paths([atpe_db], "order_intents", "created_at", 25),
        "daily_summary": None,
        "position": {"side": "FLAT", "quantity": 0},
        "operator_state": {"operator_halt": False},
        "performance": {"branch_performance": []},
    }

    approved_payload = service._paper_approved_models_payload(paper)
    detail = approved_payload["details_by_branch"]["ATPE Long Medium+High Canary / MES"]
    assert detail["intent_count"] == 1
    assert detail["fill_count"] == 1
    assert detail["latest_intent_timestamp"] == "2026-03-25T23:36:00-04:00"
    assert detail["latest_fill_timestamp"] == "2026-03-25T23:36:00-04:00"
    assert detail["chain_state"] == "FILLED_CLOSED"

    paper_with_models = {**paper, "approved_models": approved_payload}
    exceptions_payload = service._paper_exceptions_payload(paper_with_models, {"links": {}})
    assert not {
        row["code"]
        for row in exceptions_payload["exceptions"]
        if row["code"] in {"DECISION_WITHOUT_INTENT", "MODEL_SIGNAL_SEEN_BUT_NEVER_PROGRESSING"}
    }
    assert exceptions_payload["session_verdict"] == "RUNNING_CLEAN"


def test_dashboard_non_approved_payload_merges_experimental_canary_snapshot(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_experimental_canary_snapshot(repo_root)
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-23"},
        "raw_operator_status": {"lanes": []},
        "config_in_force": {"lanes": []},
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": load_experimental_canaries_snapshot(
            repo_root / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine" / "experimental_canaries_snapshot.json"
        ),
    }

    canary_payload = service._paper_non_approved_lanes_payload(paper)
    temporary_payload = service._paper_temporary_paper_strategies_payload(canary_payload)

    assert canary_payload["total_count"] == 1
    assert canary_payload["experimental_count"] == 1
    assert canary_payload["temporary_paper_count"] == 1
    assert canary_payload["enabled_count"] == 1
    assert canary_payload["disabled_count"] == 0
    assert canary_payload["kill_switch_active"] is False
    assert canary_payload["recent_signal_count"] == 2
    assert canary_payload["recent_event_count"] == 1
    assert canary_payload["operator_state_label"] == "ENABLED (PAPER ONLY)"
    assert canary_payload["artifacts"]["experimental_snapshot"] == "/api/operator-artifact/experimental-canaries"
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "atpe_long_medium_high_canary"
    assert canary_row["experimental_status"] == "experimental_canary"
    assert canary_row["instrument"] == "MES/MNQ"
    assert canary_row["quality_bucket_policy"] == "MEDIUM_HIGH_ONLY"
    assert canary_row["recent_signal_count"] == 2
    assert canary_row["recent_event_count"] == 1
    assert canary_row["state"] == "ENABLED"
    assert canary_row["temporary_paper_strategy"] is True
    assert canary_row["paper_strategy_class"] == "temporary_paper_strategy"
    assert canary_row["metrics_bucket"] == "experimental_temporary_paper"
    assert canary_row["runtime_instance_present"] is False
    assert canary_row["runtime_state_loaded"] is False
    assert canary_row["snapshot_only"] is True
    assert canary_row["runtime_presence"] == "HISTORICAL_SNAPSHOT_ONLY"
    assert canary_row["runtime_presence_label"] == "Historical / Snapshot"
    assert canary_row["allow_block_override_summary"]["label"] == "allowed=1 blocked=1 override=paper_only_experimental_canary"
    assert canary_row["atp_bias_state"] == "LONG_BIAS"
    assert canary_row["atp_pullback_state"] == "NORMAL_PULLBACK"
    assert canary_row["latest_atp_state"]["pullback_depth_score"] == 0.82
    assert canary_row["atp_entry_state"] == "ENTRY_ELIGIBLE"
    assert canary_row["atp_primary_blocker"] is None
    assert canary_row["atp_continuation_trigger_state"] == "CONTINUATION_TRIGGER_CONFIRMED"
    assert canary_row["atp_timing_state"] == "ATP_TIMING_CONFIRMED"
    assert canary_row["atp_vwap_price_quality_state"] == "VWAP_FAVORABLE"
    assert "bias=LONG_BIAS" in canary_row["operator_status_line"]
    assert "entry=ENTRY_ELIGIBLE" in canary_row["operator_status_line"]
    assert "timing=ATP_TIMING_CONFIRMED" in canary_row["operator_status_line"]
    assert canary_row["note"].startswith("Experimental Paper Strategy | Paper Only")
    assert temporary_payload["total_count"] == 1
    assert temporary_payload["enabled_count"] == 1
    assert temporary_payload["metrics_bucket"] == "experimental_temporary_paper"
    assert temporary_payload["rows"][0]["lane_id"] == "atpe_long_medium_high_canary"
    assert temporary_payload["runtime_presence_counts"]["HISTORICAL_SNAPSHOT_ONLY"] == 1

    integrity_payload = service._paper_temporary_paper_runtime_integrity_payload(
        {
            **paper,
            "non_approved_lanes": canary_payload,
            "temporary_paper_strategies": temporary_payload,
            "runtime_registry": {"rows": []},
        }
    )
    assert integrity_payload["enabled_in_app_count"] == 1
    assert integrity_payload["loaded_in_runtime_count"] == 0
    assert integrity_payload["snapshot_only_count"] == 1
    assert integrity_payload["temp_paper_blocked"] is True
    assert integrity_payload["block_reason_code"] == "enabled_lane_missing_from_runtime"
    assert "not loaded in the running paper runtime" in str(integrity_payload["block_reason"]).lower()
    assert integrity_payload["mismatch_status"] == "MISMATCH"
    assert integrity_payload["missing_lane_ids"] == ["atpe_long_medium_high_canary"]
    assert integrity_payload["start_flags"] == ["--include-atpe-canary"]
    assert integrity_payload["rows"][0]["runtime_presence"] == "HISTORICAL_SNAPSHOT_ONLY"


def test_temp_paper_runtime_integrity_keeps_loaded_unmapped_lane_as_warning_not_blocker(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)

    integrity_payload = service._paper_temporary_paper_runtime_integrity_payload(  # noqa: SLF001
        {
            "temporary_paper_strategies": {
                "rows": [
                    {
                        "lane_id": "atp_companion_v1_gc_asia_us_production_track",
                        "display_name": "ATP GC production track",
                        "state": "ENABLED",
                        "experimental_status": "experimental_temp_paper",
                        "runtime_instance_present": True,
                        "runtime_state_loaded": True,
                        "runtime_kind": "paper",
                    }
                ]
            },
            "runtime_registry": {
                "rows": [
                    {
                        "lane_id": "atp_companion_v1_gc_asia_us_production_track",
                        "runtime_instance_present": True,
                        "runtime_state_loaded": True,
                        "runtime_kind": "paper",
                    }
                ]
            },
        }
    )

    assert integrity_payload["temp_paper_blocked"] is False
    assert integrity_payload["mismatch_status"] == "CLEAR"
    assert integrity_payload["block_reason_code"] == "unresolved_temp_paper_overlay_mapping"
    assert integrity_payload["restart_overlay_mapping_ready"] is False


def test_atp_production_track_lane_is_not_classified_as_temporary_paper(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    lane_dir = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_gc_asia_us_production_track"
    lane_dir.mkdir(parents=True)

    payload = service._paper_non_approved_lanes_payload(
        {
            "artifacts_dir": str(tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session"),
            "raw_operator_status": {
                "lanes": [
                    {
                        "lane_id": "atp_companion_v1_gc_asia_us_production_track",
                        "display_name": "ATP Companion Production-Track Candidate v1 — GC / Asia + US / US_LATE Safeguard / Halt-Only 3000",
                        "symbol": "GC",
                        "runtime_kind": "atp_companion_benchmark_paper",
                        "experimental_status": "production_track_candidate",
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "observer_side": "LONG",
                        "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "position_side": "FLAT",
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{tmp_path / 'gc_prod.sqlite3'}",
                        "paper_only": True,
                        "non_approved": False,
                        "artifacts_dir": str(lane_dir),
                    }
                ]
            },
            "status": {"strategy_status": "RUNNING"},
            "runtime_registry": {"rows": []},
            "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
            "latest_fills": [],
            "latest_intents": [],
            "daily_summary": None,
            "position": {"side": "FLAT"},
            "operator_state": {},
            "performance": {"branch_performance": []},
            "experimental_canaries": {"rows": [], "generated_at": "2026-04-11T06:00:00+00:00", "kill_switch": {"active": False}},
        }
    )

    row = payload["rows"][0]
    assert row["temporary_paper_strategy"] is False
    assert row["paper_strategy_class"] == "paper_only_non_approved"
    assert row["metrics_bucket"] == "paper_only_non_approved"


def test_dashboard_approved_models_surface_includes_atp_as_shared_paper_lane(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    lane_dir = paper_artifacts / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True)

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-23"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "symbol": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "strategy_status": "RUNNING_ATP_COMPANION_BENCHMARK_PAPER",
                    "scope_label": "ATP Companion Benchmark / Paper Only / London Diagnostic-Only",
                    "benchmark_designation": "CURRENT_ATP_COMPANION_BENCHMARK",
                    "tracked_strategy_id": "atp_companion_v1_asia_us",
                    "participation_policy": "SINGLE_ENTRY_ONLY",
                    "execution_timeframe": "1m",
                    "structural_signal_timeframe": "5m",
                    "context_timeframes": ["5m"],
                    "last_execution_bar_evaluated_at": "2026-03-23T14:32:00-04:00",
                    "last_completed_context_bars_at": {},
                    "open_entry_leg_count": 0,
                    "open_add_count": 0,
                    "additional_entry_allowed": False,
                    "runtime_attached": True,
                    "operator_halt": False,
                    "entries_enabled": True,
                    "position_side": "FLAT",
                    "session_restriction": "ASIA/US",
                    "approved_long_entry_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "database_url": f"sqlite:///{repo_root / 'paper__atp.sqlite3'}",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "symbol": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "session_restriction": "ASIA/US",
                    "execution_timeframe": "1m",
                    "structural_signal_timeframe": "5m",
                    "context_timeframes": ["5m"],
                    "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    payload = service._paper_approved_models_payload(paper)

    assert payload["temporary_paper_count"] == 0
    assert payload["scope_label"] == "Shared paper lane operator detail"
    row = payload["rows"][0]
    assert row["lane_id"] == "atp_companion_v1_asia_us"
    assert row["temporary_paper_strategy"] is False
    assert row["paper_strategy_class"] == "approved_or_admitted_paper_strategy"
    assert row["lane_class"] == "benchmark_lane"
    assert row["lane_class_label"] == "Benchmark Lane"
    assert row["participation_policy"] == "SINGLE_ENTRY_ONLY"
    detail = payload["details_by_branch"][row["branch"]]
    assert detail["temporary_paper_strategy"] is False
    assert detail["paper_strategy_class"] == "approved_or_admitted_paper_strategy"
    assert detail["benchmark_designation"] == "CURRENT_ATP_COMPANION_BENCHMARK"
    assert detail["lane_class"] == "benchmark_lane"
    assert detail["designation_label"] == "CURRENT ATP COMPANION BENCHMARK"
    assert detail["staged_capable"] is False
    assert detail["execution_timeframe"] == "1m"
    assert detail["context_timeframes"] == ["5m"]
    assert detail["last_execution_bar_evaluated_at"] == "2026-03-23T14:32:00-04:00"
    assert detail["last_completed_context_bars_at"] == {}
    assert detail["control_availability"]["resume_entries"] == "NOT NEEDED: ENTRIES ALREADY LIVE"
    assert detail["surface_role"] == "PRIMARY_SHARED_LANE_OPERATOR_SURFACE"


def test_dashboard_approved_models_surface_marks_atp_candidate_as_staged_candidate_lane(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    lane_dir = paper_artifacts / "lanes" / "atp_companion_v1_gc_asia_us"
    lane_dir.mkdir(parents=True)

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "running": True,
        "status": {"session_date": "2026-03-23"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atp_companion_v1_gc_asia_us",
                    "display_name": "ATP Companion GC Candidate v1 — Asia + US Paper",
                    "symbol": "GC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "strategy_status": "RUNNING_ATP_COMPANION_CANDIDATE_STAGED_PAPER",
                    "scope_label": "ATP Companion Candidate / Paper Only / London Diagnostic-Only / Staged",
                    "tracked_strategy_id": "atp_companion_v1__paper_gc_asia_us",
                    "participation_policy": "STAGED_SAME_DIRECTION",
                    "execution_timeframe": "1m",
                    "structural_signal_timeframe": "5m",
                    "context_timeframes": ["5m"],
                    "entries_enabled": True,
                    "operator_halt": True,
                    "position_side": "LONG",
                    "broker_position_qty": 2,
                    "internal_position_qty": 2,
                    "open_entry_leg_count": 2,
                    "open_add_count": 1,
                    "additional_entry_allowed": False,
                    "last_execution_bar_evaluated_at": "2026-03-23T14:41:00-04:00",
                    "last_completed_context_bars_at": {"5m": "2026-03-23T14:40:00-04:00"},
                    "runtime_attached": True,
                    "session_restriction": "ASIA/US",
                    "approved_long_entry_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "database_url": f"sqlite:///{repo_root / 'paper__atp_gc.sqlite3'}",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atp_companion_v1_gc_asia_us",
                    "display_name": "ATP Companion GC Candidate v1 — Asia + US Paper",
                    "symbol": "GC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "session_restriction": "ASIA/US",
                    "execution_timeframe": "1m",
                    "structural_signal_timeframe": "5m",
                    "context_timeframes": ["5m"],
                    "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "LONG"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    payload = service._paper_approved_models_payload(paper)

    row = payload["rows"][0]
    detail = payload["details_by_branch"][row["branch"]]
    assert row["lane_class"] == "candidate_staged_lane"
    assert row["lane_class_label"] == "Candidate Staged Lane"
    assert row["candidate_designation"] == "ATP_COMPANION_CANDIDATE_STAGED"
    assert row["participation_policy"] == "STAGED_SAME_DIRECTION"
    assert row["open_entry_leg_count"] == 2
    assert row["open_add_count"] == 1
    assert row["additional_entry_allowed"] is False
    assert detail["lane_class"] == "candidate_staged_lane"
    assert detail["designation_label"] == "ATP candidate / staged paper lane"
    assert detail["staged_capable"] is True
    assert detail["execution_timeframe"] == "1m"
    assert detail["context_timeframes"] == ["5m"]
    assert detail["last_execution_bar_evaluated_at"] == "2026-03-23T14:41:00-04:00"
    assert detail["last_completed_context_bars_at"] == {"5m": "2026-03-23T14:40:00-04:00"}
    assert detail["total_quantity"] == 2
    assert detail["control_availability"]["halt_entries"] == "ALREADY ACTIVE: OPERATOR HALT IS SET"
    assert detail["control_availability"]["resume_entries"] == "AVAILABLE: CLEAR OPERATOR HALT"
    assert detail["control_availability"]["stop_after_cycle"] == "DISABLED: UNSAFE WHILE STAGED EXPOSURE IS OPEN"


def test_tracked_paper_strategy_payload_registers_live_attached_atp_benchmark_from_persisted_runtime_truth(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
            {"bar_id": "bar-2", "symbol": "MGC", "end_ts": "2026-03-23T14:40:00-04:00", "close": "101.25"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:40:00-04:00",
                "duplicate_bar_suppression_count": 1,
                "entry_model": "CURRENT_CANDLE_VWAP",
                "active_entry_model": "CURRENT_CANDLE_VWAP",
                "supported_entry_models": ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"],
                "entry_model_supported": True,
                "execution_truth_emitter": "atp_phase3_timing_emitter",
                "intrabar_execution_authoritative": True,
                "authoritative_intrabar_available": True,
                "authoritative_entry_truth_available": True,
                "authoritative_exit_truth_available": True,
                "authoritative_trade_lifecycle_available": True,
                "lifecycle_records": [
                    {
                        "trade_id": "atp-trade-1",
                        "decision_id": "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00",
                        "decision_ts": "2026-03-23T14:31:00-04:00",
                        "entry_ts": "2026-03-23T14:31:10-04:00",
                        "exit_ts": "2026-03-23T14:41:10-04:00",
                        "entry_price": "100.25",
                        "exit_price": "101.25",
                        "primary_exit_reason": "atpe_target",
                        "exit_reason": "atpe_target",
                        "setup_signature": "benchmark-setup",
                        "setup_state_signature": "benchmark-state",
                        "family": "atp_v1_long_pullback_continuation",
                        "entry_source_family": "atp_v1_long_pullback_continuation",
                        "side": "LONG",
                        "decision_context_linkage_available": True,
                        "decision_context_linkage_status": "AVAILABLE",
                        "entry_model": "CURRENT_CANDLE_VWAP",
                        "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                        "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                        "truth_provenance": {
                            "runtime_context": "PAPER",
                            "run_lane": "PAPER_RUNTIME",
                        },
                    }
                ],
                "authoritative_trade_lifecycle_records": [
                    {
                        "trade_id": "atp-trade-1",
                        "decision_id": "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00",
                        "decision_ts": "2026-03-23T14:31:00-04:00",
                        "entry_ts": "2026-03-23T14:31:10-04:00",
                        "exit_ts": "2026-03-23T14:41:10-04:00",
                        "entry_price": "100.25",
                        "exit_price": "101.25",
                        "primary_exit_reason": "atpe_target",
                        "exit_reason": "atpe_target",
                        "setup_signature": "benchmark-setup",
                        "setup_state_signature": "benchmark-state",
                        "family": "atp_v1_long_pullback_continuation",
                        "entry_source_family": "atp_v1_long_pullback_continuation",
                        "side": "LONG",
                        "decision_context_linkage_available": True,
                        "decision_context_linkage_status": "AVAILABLE",
                        "entry_model": "CURRENT_CANDLE_VWAP",
                        "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                        "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                        "truth_provenance": {
                            "runtime_context": "PAPER",
                            "run_lane": "PAPER_RUNTIME",
                        },
                    }
                ],
                "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                "unsupported_reason": None,
                "truth_provenance": {
                    "runtime_context": "PAPER",
                    "run_lane": "PAPER_RUNTIME",
                    "artifact_context": "ATP_COMPANION_PAPER_RUNTIME_STATUS",
                    "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                    "study_mode": "paper_runtime",
                    "artifact_rebuilt": False,
                },
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
                "latest_atp_entry_state": {"entry_state": "ENTRY_ELIGIBLE", "primary_blocker": None},
                "latest_atp_timing_state": {"timing_state": "ATP_TIMING_CONFIRMED", "primary_blocker": None},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_jsonl_rows(
        lane_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "atp-entry-1",
                "created_at": "2026-03-23T14:31:00-04:00",
                "intent_type": "BUY_TO_OPEN",
                "reason_code": "atp_v1_long_pullback_continuation",
                "order_status": "FILLED",
            }
        ],
    )
    _write_jsonl_rows(
        lane_dir / "fills.jsonl",
        [
            {
                "order_intent_id": "atp-entry-1",
                "fill_timestamp": "2026-03-23T14:31:10-04:00",
                "intent_type": "BUY_TO_OPEN",
                "fill_price": "100.25",
                "order_status": "FILLED",
            }
        ],
    )
    _write_jsonl_rows(
        lane_dir / "trades.jsonl",
        [
            {
                "trade_id": "atp-trade-1",
                "symbol": "MGC",
                "direction": "LONG",
                "entry_timestamp": "2026-03-23T14:31:10-04:00",
                "exit_timestamp": "2026-03-23T14:41:10-04:00",
                "entry_price": "100.25",
                "exit_price": "101.25",
                "realized_pnl": "10.0",
                "exit_reason": "atpe_target",
                "setup_family": "atp_v1_long_pullback_continuation",
            }
        ],
    )

    db_path = repo_root / "mgc_v05l.probationary.paper__atp_companion_v1_asia_us.sqlite3"
    _init_empty_dashboard_db(db_path)
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": True,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "approved_models": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "paper_strategy_class": "approved_or_admitted_paper_strategy",
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "database_url": f"sqlite:///{db_path}",
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                        "order_intents": str((lane_dir / "order_intents.jsonl").resolve()),
                        "fills": str((lane_dir / "fills.jsonl").resolve()),
                        "trades": str((lane_dir / "trades.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {
            "trade_log": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "side": "LONG",
                    "entry_timestamp": "2026-03-23T14:31:10-04:00",
                    "exit_timestamp": "2026-03-23T14:41:10-04:00",
                    "exit_reason": "atpe_target",
                    "net_pnl": "10.0",
                    "signal_family": "active_trend_participation_engine",
                    "signal_family_label": "ATP Companion",
                    "entry_session_phase": "US_LATE",
                }
            ]
        },
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    assert tracked_payload["total_count"] == 1
    assert "secondary audit/read-model surfaces" in tracked_payload["note"]
    assert "primary operating surface" in tracked_payload["note"]
    row = tracked_payload["rows"][0]
    detail = tracked_payload["details_by_strategy_id"]["atp_companion_v1_asia_us"]
    assert row["display_name"] == "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only"
    assert row["internal_label"] == "ATP_COMPANION_V1_ASIA_US"
    assert row["environment"] == "paper"
    assert row["benchmark_designation"] == "CURRENT_ATP_COMPANION_BENCHMARK"
    assert row["status"] == "READY"
    assert row["entries_enabled"] is True
    assert row["session_allowed"] is True
    assert row["runtime_attached"] is True
    assert row["data_stale"] is False
    assert row["latest_processed_bar_timestamp"] == "2026-03-23T14:40:00-04:00"
    assert row["realized_pnl"] == "10.0"
    assert row["current_day_pnl"] == "10.0"
    assert row["profit_factor"] == "999"
    assert row["trade_family_breakdown"][0]["family"] == "ATP Companion"
    assert row["session_breakdown"][0]["session"] == "US_LATE"
    assert row["last_trade_summary"]["family"] == "ATP Companion"
    assert row["lane_count"] == 1
    assert row["observed_instruments"] == ["MGC"]
    assert row["health_flags"]["duplicate_bar_suppression_count"] == 1
    assert row["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert row["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert row["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert row["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert row["authoritative_intrabar_available"] is True
    assert row["authoritative_entry_truth_available"] is True
    assert row["authoritative_exit_truth_available"] is True
    assert row["authoritative_trade_lifecycle_available"] is True
    assert row["authoritative_trade_lifecycle_records"][0]["trade_id"] == "atp-trade-1"
    assert row["authoritative_trade_lifecycle_records"][0]["decision_id"] == "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00"
    assert row["authoritative_trade_lifecycle_records"][0]["decision_context_linkage_status"] == "AVAILABLE"
    assert row["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"
    assert row["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert row["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert detail["authoritative_trade_lifecycle_records"][0]["primary_exit_reason"] == "atpe_target"
    assert detail["recent_trades"][0]["decision_ts"] == "2026-03-23T14:31:00-04:00"
    assert detail["recent_bars"][0]["bar_id"] == "bar-2"
    assert detail["recent_order_intents"][0]["order_intent_id"] == "atp-entry-1"
    assert detail["recent_fills"][0]["fill_price"] == "100.25"
    assert detail["recent_trades"][0]["primary_exit_reason"] == "atpe_target"
    assert [lane["lane_id"] for lane in detail["constituent_lanes"]] == ["atp_companion_v1_asia_us"]
    assert detail["config_identity"]["config_source"].endswith("probationary_pattern_engine_paper.yaml")
    assert detail["config_identity"]["allowed_sessions"] == ["ASIA", "US"]
    assert detail["config_identity"]["diagnostic_only_sessions"] == ["LONDON"]


def test_tracked_paper_strategy_payload_marks_atp_benchmark_reconciling_when_runtime_detached_or_stale(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:30:00-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:30:00-04:00",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "data_stale": True,
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "approved_models": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "paper_strategy_class": "approved_or_admitted_paper_strategy",
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": False,
                    "runtime_state_loaded": True,
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["status"] == "RECONCILING"
    assert row["runtime_attached"] is False
    assert row["data_stale"] is True
    assert "reattach" in str(row["status_reason"]).lower()


def test_tracked_paper_strategy_payload_marks_open_pnl_unavailable_without_trusted_mark(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "approved_models": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "paper_strategy_class": "approved_or_admitted_paper_strategy",
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "position_side": "LONG",
                    "entry_price": "100.0",
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {},
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["open_pnl"] is None
    assert row["open_pnl_supported"] is False
    assert row["open_pnl_unavailable_reason"] == (
        "Tracked paper strategy does not currently have a trusted latest mark/reference price for the open position."
    )


def test_tracked_paper_strategy_payload_treats_lane_local_runtime_instance_as_attached_even_without_global_running_flag(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "approved_models": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "paper_strategy_class": "approved_or_admitted_paper_strategy",
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["runtime_attached"] is True
    assert row["status"] == "READY"
    assert row["data_stale"] is False


def test_tracked_paper_strategy_payload_prefers_operator_status_entries_enabled_truth(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    operator_payload = {
        "updated_at": "2026-03-23T19:44:30-04:00",
        "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
        "runtime_attached": True,
        "entries_enabled": False,
        "operator_halt": True,
        "warmup_complete": True,
        "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
        "latest_atp_state": {"bias_state": "LONG_BIAS"},
    }
    (lane_dir / "operator_status.json").write_text(
        json.dumps(operator_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "approved_models": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "paper_strategy_class": "approved_or_admitted_paper_strategy",
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "operator_status_payload": operator_payload,
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["entries_enabled"] is False
    assert row["enabled"] is False
    assert row["operator_halt"] is True
    assert row["runtime_attached"] is True


def test_tracked_paper_strategy_payload_falls_back_to_lane_artifacts_when_dashboard_rows_are_missing(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    _write_jsonl_rows(
        lane_dir / "signals.jsonl",
        [
            {"signal_timestamp": "2026-03-23T14:35:00-04:00", "decision": "blocked"},
        ],
    )
    _write_jsonl_rows(
        lane_dir / "trades.jsonl",
        [
            {
                "trade_id": "atp-trade-1",
                "symbol": "MGC",
                "direction": "LONG",
                "entry_timestamp": "2026-03-23T14:31:10-04:00",
                "exit_timestamp": "2026-03-23T14:41:10-04:00",
                "entry_price": "100.25",
                "exit_price": "101.25",
                "realized_pnl": "10.0",
                "exit_reason": "atp_companion_target",
                "strategy_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                "status": "CLOSED",
            }
        ],
    )
    (lane_dir / "runtime_state.json").write_text(json.dumps({"duplicate_bar_suppression_count": 0}), encoding="utf-8")
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "duplicate_bar_suppression_count": 0,
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    db_path = repo_root / "mgc_v05l.probationary.paper__atp_companion_v1_asia_us.sqlite3"
    _init_empty_dashboard_db(db_path)
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {"rows": []},
        "non_approved_lanes": {"rows": []},
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    detail = tracked_payload["details_by_strategy_id"]["atp_companion_v1_asia_us"]
    assert row["runtime_attached"] is True
    assert row["entries_enabled"] is True
    assert row["lane_count"] == 1
    assert row["latest_processed_bar_timestamp"] == "2026-03-23T14:35:00-04:00"
    assert detail["recent_bars"][0]["bar_id"] == "bar-1"
    assert detail["recent_trades"][0]["exit_reason"] == "atp_companion_target"


def test_tracked_paper_strategy_payload_does_not_fallback_to_atp_when_other_live_paper_lanes_are_loaded(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": True,
        "raw_operator_status": {
            "active_lane_ids": [
                "gc_1x_all_lanes__asia_early_short",
                "gc_1x_all_lanes__ny_early_short",
            ],
            "lanes": [
                {"lane_id": "gc_1x_all_lanes__asia_early_short", "display_name": "GC / ASIA_EARLY_SHORT / x1"},
                {"lane_id": "gc_1x_all_lanes__ny_early_short", "display_name": "GC / NY_EARLY_SHORT / x1"},
            ],
        },
        "status": {"session_date": "2026-04-21", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {"rows": []},
        "non_approved_lanes": {"rows": []},
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-04-21T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert tracked_payload["active_count"] == 0
    assert row["lane_count"] == 0
    assert row["runtime_attached"] is False


def test_dashboard_non_approved_payload_marks_gc_mgc_temp_paper_runtime_rows_as_temporary_strategy(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "gc_mgc_london_open_acceptance_continuation_long__GC"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps({"display_name": "GC/MGC London-Open Acceptance Continuation Long / GC", "position_side": "FLAT"}),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                    "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                    "symbol": "GC",
                    "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    "entries_enabled": True,
                    "position_side": "FLAT",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                    "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                    "symbol": "GC",
                    "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    "long_sources": ["gc_mgc_london_open_acceptance_continuation_long"],
                    "experimental_status": "experimental_temp_paper",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "LONG",
                    "observer_variant_id": "gc_mgc_london_open_acceptance_continuation_long",
                    "session_restriction": "LONDON_OPEN",
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": {"rows": [], "generated_at": "2026-03-24T07:30:00+00:00", "kill_switch": {"active": False}},
    }

    payload = service._paper_non_approved_lanes_payload(paper)

    assert payload["total_count"] == 1
    row = payload["rows"][0]
    assert row["lane_id"] == "gc_mgc_london_open_acceptance_continuation_long__GC"
    assert row["temporary_paper_strategy"] is True
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["metrics_bucket"] == "experimental_temporary_paper"
    assert row["experimental_status"] == "experimental_temp_paper"
    assert row["display_name"] == "GC/MGC London-Open Acceptance Continuation Long / GC"


def test_dashboard_non_approved_payload_uses_live_temp_paper_runtime_counts(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "atpe_long_medium_high_canary__MES"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "atpe_long_medium_high_canary__MES",
                "display_name": "ATPE Long Medium+High Canary / MES",
                "updated_at": "2026-03-24T15:55:33-04:00",
                "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                "signal_count": 23,
                "intent_count": 30,
                "fill_count": 30,
                "closed_trades": 15,
                "session_realized_pnl": "-133.750",
                "position_side": "FLAT",
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atpe_long_medium_high_canary__MES",
                    "display_name": "ATPE Long Medium+High Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "entries_enabled": True,
                    "position_side": "FLAT",
                    "recent_signal_count": 23,
                    "intent_count": 30,
                    "fill_count": 30,
                    "closed_trades": 15,
                    "session_realized_pnl": "-133.750",
                    "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atpe_long_medium_high_canary__MES",
                    "display_name": "ATPE Long Medium+High Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "experimental_status": "experimental_canary",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "LONG",
                    "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
                    "session_restriction": "ASIA/LONDON/US",
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": {"rows": [], "generated_at": "2026-03-24T19:55:40+00:00", "kill_switch": {"active": False}},
    }

    payload = service._paper_non_approved_lanes_payload(paper)

    assert payload["total_count"] == 1
    row = payload["rows"][0]
    assert row["lane_id"] == "atpe_long_medium_high_canary__MES"
    assert row["intent_count"] == 30
    assert row["fill_count"] == 30
    assert row["trade_count"] == 15
    assert row["entry_completed"] is True
    assert row["exit_completed"] is True
    assert row["lifecycle_state"] == "ENTRY_AND_EXIT_COMPLETE"
    assert row["can_process_bars"] is True
    assert row["realized_pnl"] == "-133.750"


def test_dashboard_signal_intent_fill_audit_uses_lane_id_identity_for_temp_paper(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "atpe_short_high_only_canary__MES"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "atpe_short_high_only_canary__MES",
                "display_name": "ATPE Short High-Only Canary / MES",
                "updated_at": "2026-03-24T15:55:34-04:00",
                "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                "position_side": "FLAT",
            }
        ),
        encoding="utf-8",
    )
    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atpe_short_high_only_canary__MES",
                    "display_name": "ATPE Short High-Only Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "strategy_status": "RUNNING_PAPER_ONLY_EXPERIMENTAL_CANARY",
                    "entries_enabled": True,
                    "paper_only": True,
                    "non_approved": True,
                    "experimental_status": "experimental_canary",
                    "position_side": "FLAT",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atpe_short_high_only_canary__MES",
                    "display_name": "ATPE Short High-Only Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "short_sources": ["trend_participation.failed_countertrend_resumption.short.active"],
                    "experimental_status": "experimental_canary",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "SHORT",
                }
            ]
        },
        "strategy_performance": {"rows": [], "trade_log": []},
    }

    payload = service._paper_signal_intent_fill_audit_payload(
        paper=paper,
        session_date="2026-03-24",
        root_db_path=None,
    )
    assert payload["payload_version"] == DASHBOARD_PAYLOAD_SCHEMA_VERSION
    assert payload["session_date"] == "2026-03-24"
    row = payload["rows"][0]
    assert row["lane_id"] == "atpe_short_high_only_canary__MES"
    assert row["standalone_strategy_id"] == "atpe_short_high_only_canary__MES"
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["temporary_paper_strategy"] is True


def test_start_paper_command_auto_includes_enabled_temp_paper_overlays(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_experimental_canary_snapshot(repo_root)
    service = OperatorDashboardService(repo_root)
    snapshot = {
        "paper": {
            "non_approved_lanes": {
                "rows": [
                    {
                        "lane_id": "atpe_long_medium_high_canary",
                        "display_name": "ATPE Long Medium+High Canary",
                        "temporary_paper_strategy": True,
                        "paper_strategy_class": "temporary_paper_strategy",
                        "state": "ENABLED",
                        "runtime_kind": "atpe_canary_observer",
                    },
                    {
                        "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                        "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                        "temporary_paper_strategy": True,
                        "paper_strategy_class": "temporary_paper_strategy",
                        "state": "ENABLED",
                        "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    },
                ]
            }
        }
    }

    command, metadata = service._paper_start_command_with_enabled_temp_paper(snapshot)

    assert command is not None
    assert command[:2] == ["bash", "scripts/run_probationary_paper_soak.sh"]
    assert "--config" in command
    assert "--include-atpe-canary" in command
    assert "--include-gc-mgc-acceptance" in command
    assert command[-1] == "--background"
    assert metadata["enabled_lane_ids"] == [
        "atpe_long_medium_high_canary",
        "gc_mgc_london_open_acceptance_continuation_long__GC",
    ]
    assert metadata["requested_flags"] == ["--include-atpe-canary", "--include-gc-mgc-acceptance"]
    assert metadata["unresolved_lane_ids"] == []


def test_default_paper_runtime_config_paths_include_atp_companion_overlays(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)

    config_paths = [str(path) for path in service._paper_runtime_config_paths()]

    assert str(tmp_path / "config" / "probationary_pattern_engine_paper.yaml") in config_paths
    assert str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml") in config_paths
    assert str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml") in config_paths
    assert str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml") in config_paths
    assert (
        str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml")
        in config_paths
    )
    assert str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_shared_runtime.yaml") in config_paths
    assert config_paths[-1] == str(tmp_path / "config" / "probationary_pattern_engine_paper_atp_companion_shared_runtime.yaml")


def test_paper_runtime_config_paths_use_persisted_override_file(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "outputs" / "reports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "base.yaml").write_text("", encoding="utf-8")
    (tmp_path / "outputs" / "reports" / "gc_1x_all_lanes.paper_package.yaml").write_text("", encoding="utf-8")
    override_file = (
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_runtime_config_paths.txt"
    )
    override_file.parent.mkdir(parents=True, exist_ok=True)
    override_file.write_text(
        "\n".join(
            [
                str(tmp_path / "config" / "base.yaml"),
                str(tmp_path / "outputs" / "reports" / "gc_1x_all_lanes.paper_package.yaml"),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config_paths = [str(path) for path in service._paper_runtime_config_paths()]

    assert config_paths == [
        str((tmp_path / "config" / "base.yaml").resolve()),
        str((tmp_path / "outputs" / "reports" / "gc_1x_all_lanes.paper_package.yaml").resolve()),
    ]


def test_paper_runtime_config_paths_ignore_stale_override_paths_outside_repo(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    override_file = (
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_runtime_config_paths.txt"
    )
    override_file.parent.mkdir(parents=True, exist_ok=True)
    override_file.write_text(
        "\n".join(
            [
                "/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml",
                "/Users/patrick/Documents/MGC-v05l-automation/outputs/probationary_pattern_engine/paper_session/runtime/paper_route_canary_force_once.yaml",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config_paths = [str(path) for path in service._paper_runtime_config_paths()]

    assert str(tmp_path / "config" / "probationary_pattern_engine_paper.yaml") in config_paths
    assert all("/Users/patrick/Documents/MGC-v05l-automation" not in path for path in config_paths)
    assert {warning["code"] for warning in service._paper_runtime_config_path_warnings} == {
        "paper_runtime_config_override_outside_repo"
    }


def test_dashboard_snapshot_includes_approved_quant_baselines_snapshot(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    paper_artifacts = shadow_artifacts / "paper_session"
    (shadow_artifacts / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_quant_baselines").mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(paper_db)

    breakout_weekly_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / "phase2c.breakout.metals_only.us_unknown.baseline" / "weekly"
    breakout_weekly_dir.mkdir(parents=True)
    (breakout_weekly_dir / "2026-W12.json").write_text(
        json.dumps(
            {
                "week_id": "2026-W12",
                "symbol_attribution": [
                    {"symbol": "GC", "trade_count": 3, "net_r_020_total": 0.51},
                    {"symbol": "HG", "trade_count": 2, "net_r_020_total": 0.24},
                ],
                "session_attribution": [
                    {"session_label": "US", "trade_count": 4, "net_r_020_total": 0.62},
                    {"session_label": "UNKNOWN", "trade_count": 1, "net_r_020_total": 0.13},
                ],
                "warning_flags": ["unknown_session_labeling_watch"],
            }
        ) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.json").write_text(
        json.dumps({"freeze_mode": "logic_frozen_monitoring_only"}) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.md").write_text(
        "# Current Active Baseline Status\n",
        encoding="utf-8",
    )

    approved_quant_snapshot = {
        "generated_at": "2026-03-20T23:18:11+00:00",
        "status": "available",
        "rows": [
            {
                "lane_id": "phase2c.breakout.metals_only.us_unknown.baseline",
                "lane_name": "breakout_metals_us_unknown_continuation",
                "probation_status": "watch",
                "baseline_status": "operator_baseline_candidate",
                "approved_scope": {
                    "symbols": ["GC", "MGC", "HG", "PL"],
                    "allowed_sessions": ["US"],
                    "excluded_sessions": ["ASIA", "LONDON"],
                    "permanent_exclusions": ["6J", "LONDON", "broad_fx_metals_breakout", "cross_universe_breakout"],
                    "hold_bars": 24,
                    "stop_r": 1.0,
                    "target_r": None,
                    "exit_style": "time_stop_only",
                    "structural_invalidation_r": None,
                },
                "active_exit_logic": {
                    "exit_style": "time_stop_only",
                    "hold_bars": 24,
                    "stop_r": 1.0,
                    "target_r": None,
                    "structural_invalidation_r": None,
                },
                "artifacts": {
                    "weekly_dir": str(breakout_weekly_dir),
                },
            }
        ],
        "summary_line": "breakout_metals_us_unknown_continuation=watch/operator_baseline_candidate",
    }
    (repo_root / "outputs" / "probationary_quant_baselines" / "approved_quant_baselines_snapshot.json").write_text(
        json.dumps(approved_quant_snapshot) + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_state": "TEST",
        "feed_label": "INDEX FEED TEST",
        "symbols": [],
        "diagnostics": {},
    }
    service._treasury_curve_payload = lambda: {  # type: ignore[method-assign]
        "curve_state": "TEST",
        "rows": [],
        "diagnostics": {},
    }
    snapshot = service.snapshot()

    assert snapshot["approved_quant_baselines"]["status"] == "available"
    assert snapshot["approved_quant_baselines"]["rows"][0]["lane_id"] == "phase2c.breakout.metals_only.us_unknown.baseline"
    assert snapshot["approved_quant_baselines"]["rows"][0]["lane_classification"] == "approved_baseline_lane"
    assert snapshot["approved_quant_baselines"]["rows"][0]["promotion_state"] == "operator_baseline_candidate"
    assert snapshot["approved_quant_baselines"]["rows"][0]["post_cost_monitoring_read"]["label"] == "stable_positive_post_cost"
    assert snapshot["approved_quant_baselines"]["rows"][0]["approved_exit_label"] == "time_stop_only.h24"
    assert snapshot["approved_quant_baselines"]["rows"][0]["symbol_attribution_summary"][0].startswith("GC")
    assert snapshot["approved_quant_baselines"]["rows"][0]["session_attribution_summary"][0].startswith("US")
    assert snapshot["approved_quant_baselines"]["artifacts"]["snapshot"] == "/api/operator-artifact/approved-quant-baselines"
    assert snapshot["approved_quant_baselines"]["artifacts"]["current_status_json"] == "/api/operator-artifact/approved-quant-baselines-current-status"
    assert "APPROVED BASELINE" in snapshot["approved_quant_baselines"]["rows"][0]["operator_status_line"]
    operator_surface = snapshot["operator_surface"]
    assert operator_surface["readiness"]["provenance"] == "operator_critical"
    assert operator_surface["daily_risk"]["provenance"] == "operator_critical"
    assert operator_surface["context"]["provenance"] == "informational_only"
    lane_rows = operator_surface["lane_rows"]
    assert any(row["classification_tag"] == "approved_quant" for row in lane_rows)
    assert any(row["display_name"] == "breakout_metals_us_unknown_continuation" and row["instrument"] == "GC" for row in lane_rows)
    summary_cards = {row["label"]: row["value"] for row in operator_surface["lane_universe"]["cards"]}
    approved_quant_lane_ids = {
        row["lane_id"]
        for row in lane_rows
        if row["classification_tag"] == "approved_quant"
    }
    assert summary_cards["Approved Quant"] == str(len(approved_quant_lane_ids))
    assert operator_surface["lane_universe"]["title"] == "Unified Active Lane / Instrument Surface"
    assert operator_surface["daily_risk"]["cards"][0]["label"] == "Daily Realized"
    assert operator_surface["readiness"]["cards"][0]["label"] == "System Health"
    assert [section["key"] for section in snapshot["lane_registry"]["sections"]] == [
        "approved_quant",
        "admitted_paper",
        "canary",
    ]
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["display_name"] == "breakout_metals_us_unknown_continuation"
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["standalone_strategy_id"] == "breakout_metals_us_unknown_continuation__GC"
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["active_exit"] == "time_stop_only.h24"
    assert snapshot["lane_registry"]["diagnostics"]["approved_quant"]["source_row_count"] == 1
    assert snapshot["lane_registry"]["diagnostics"]["approved_quant"]["registry_row_count"] == 4
    assert snapshot["lane_registry"]["diagnostics"]["admitted_paper"]["registry_row_count"] == len(snapshot["paper"]["approved_models"]["rows"])
    assert snapshot["paper"]["approved_models"]["surface_alignment"]["aligned"] is True
    assert snapshot["lane_registry"]["diagnostics"]["canary"]["registry_row_count"] == len(snapshot["paper"]["non_approved_lanes"]["rows"])
    assert snapshot["paper"]["non_approved_lanes"]["surface_alignment"]["aligned"] is True
    assert snapshot["market_context"]["feed_state"] == "TEST"


def test_dashboard_snapshot_extends_signal_intent_fill_audit_to_quant_rows(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    paper_artifacts = shadow_artifacts / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_quant_baselines").mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(paper_db)
    paper_lane_db = repo_root / "paper__legacy_lane.sqlite3"
    _init_empty_dashboard_db(paper_lane_db)
    _append_dashboard_bar(
        paper_lane_db,
        bar_id="legacy-bar-1",
        symbol="MGC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )

    lane_one = "phase2c.breakout.metals_only.us_unknown.baseline"
    lane_two = "phase2c.failed.core4_plus_qc.no_us.baseline"
    lane_one_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_one
    lane_two_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_two
    (lane_one_dir / "daily").mkdir(parents=True)
    (lane_two_dir / "daily").mkdir(parents=True)

    (lane_one_dir / "daily" / "2026-03-23.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-23",
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "lane_classification": "approved_baseline_lane",
                "signal_count": 3,
                "trade_count": 1,
            }
        ) + "\n",
        encoding="utf-8",
    )
    (lane_two_dir / "daily" / "2026-03-23.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-23",
                "lane_id": lane_two,
                "lane_name": "failed_move_no_us_reversal_short",
                "lane_classification": "approved_baseline_lane",
                "signal_count": 1,
                "trade_count": 0,
            }
        ) + "\n",
        encoding="utf-8",
    )

    _write_jsonl_rows(
        lane_one_dir / "processed_bars.jsonl",
        [
            {"bar_id": "gc-bar-1", "symbol": "GC", "end_ts": "2026-03-23T09:35:00-04:00"},
            {"bar_id": "mgc-bar-1", "symbol": "MGC", "end_ts": "2026-03-23T09:40:00-04:00"},
            {"bar_id": "hg-bar-1", "symbol": "HG", "end_ts": "2026-03-23T09:45:00-04:00"},
            {"bar_id": "pl-bar-1", "symbol": "PL", "end_ts": "2026-03-23T09:50:00-04:00"},
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "signals.jsonl",
        [
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "HG",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:45:00-04:00",
                "entry_timestamp_planned": "2026-03-23T09:50:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            },
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "PL",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:50:00-04:00",
                "entry_timestamp_planned": "2026-03-23T09:55:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            },
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "mgc-intent-1",
                "symbol": "MGC",
                "created_at": "2026-03-23T09:40:05-04:00",
                "intent_type": "BUY_TO_OPEN",
                "reason_code": "breakout_continuation",
                "broker_order_id": "mgc-broker-1",
            }
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "trades.jsonl",
        [
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "HG",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:45:00-04:00",
                "entry_timestamp": "2026-03-23T09:50:00-04:00",
                "entry_price": 100.0,
                "exit_timestamp": "2026-03-23T10:00:00-04:00",
                "exit_price": 101.0,
            }
        ],
    )

    _write_jsonl_rows(
        lane_two_dir / "processed_bars.jsonl",
        [
            {"bar_id": "cl-bar-1", "symbol": "CL", "end_ts": "2026-03-23T09:55:00-04:00"},
        ],
    )
    _write_jsonl_rows(
        lane_two_dir / "signals.jsonl",
        [
            {
                "variant_id": lane_two,
                "lane_id": lane_two,
                "lane_name": "failed_move_no_us_reversal_short",
                "symbol": "CL",
                "direction": "SHORT",
                "signal_timestamp": "2026-03-23T09:55:00-04:00",
                "entry_timestamp_planned": "2026-03-23T10:00:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            }
        ],
    )

    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.json").write_text(
        json.dumps({"freeze_mode": "logic_frozen_monitoring_only"}) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.md").write_text(
        "# Current Active Baseline Status\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "approved_quant_baselines_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-23T14:00:00+00:00",
                "status": "available",
                "rows": [
                    {
                        "lane_id": lane_one,
                        "lane_name": "breakout_metals_us_unknown_continuation",
                        "probation_status": "watch",
                        "baseline_status": "operator_baseline_candidate",
                        "classification_tag": "approved_quant",
                        "approved_scope": {
                            "symbols": ["GC", "MGC", "HG", "PL"],
                            "allowed_sessions": ["US"],
                            "excluded_sessions": ["ASIA", "LONDON"],
                            "direction": "LONG",
                            "family": "breakout_continuation",
                        },
                    },
                    {
                        "lane_id": lane_two,
                        "lane_name": "failed_move_no_us_reversal_short",
                        "probation_status": "review",
                        "baseline_status": "operator_baseline_candidate",
                        "classification_tag": "approved_quant",
                        "approved_scope": {
                            "symbols": ["CL", "SI", "NG", "ZN", "ZB"],
                            "allowed_sessions": ["ASIA", "LONDON"],
                            "excluded_sessions": ["US"],
                            "direction": "SHORT",
                            "family": "failed_move_reversal",
                        },
                    },
                ],
            }
        ) + "\n",
        encoding="utf-8",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T10:05:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T10:00:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "legacy_lane",
                        "display_name": "Legacy Paper Lane",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{paper_lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {"feed_state": "TEST", "feed_label": "INDEX FEED TEST", "symbols": [], "diagnostics": {}}  # type: ignore[method-assign]
    service._treasury_curve_payload = lambda: {"curve_state": "TEST", "rows": [], "diagnostics": {}}  # type: ignore[method-assign]

    snapshot = service.snapshot()

    audit_rows = snapshot["paper"]["signal_intent_fill_audit"]["rows"]
    quant_rows = [row for row in audit_rows if row.get("lane_id") in {lane_one, lane_two}]
    assert len(quant_rows) == 9
    keyed_rows = {(row["lane_id"], row["instrument"]): row for row in quant_rows}
    assert keyed_rows[(lane_one, "GC")]["audit_verdict"] == "NO_SETUP_OBSERVED"
    assert keyed_rows[(lane_one, "GC")]["standalone_strategy_id"] == "breakout_metals_us_unknown_continuation__GC"
    assert keyed_rows[(lane_one, "MGC")]["audit_verdict"] == "INTENT_NO_FILL_YET"
    assert keyed_rows[(lane_one, "HG")]["audit_verdict"] == "FILLED"
    assert keyed_rows[(lane_two, "CL")]["audit_verdict"] == "SETUP_GATED"
    assert keyed_rows[(lane_one, "PL")]["performance_row_present"] is True
    assert keyed_rows[(lane_one, "PL")]["auditable_now"] is True
    assert keyed_rows[(lane_one, "PL")]["eligible_now"] is True
    assert keyed_rows[(lane_two, "CL")]["eligible_now"] is False
    assert keyed_rows[(lane_two, "CL")]["trade_log_present"] is False
    strategy_rows = {
        row["standalone_strategy_id"]: row
        for row in snapshot["paper"]["strategy_performance"]["rows"]
        if row["lane_id"] in {lane_one, lane_two}
    }
    assert len(strategy_rows) == 9
    assert "breakout_metals_us_unknown_continuation__GC" in strategy_rows
    assert "failed_move_no_us_reversal_short__CL" in strategy_rows
    assert any(row["lane_id"] == "legacy_lane" for row in audit_rows)

    assert snapshot["treasury_curve"]["curve_state"] == "TEST"
    assert "performance" in snapshot["paper"]
    assert "history" in snapshot["paper"]


def test_track_b_phase1_gc_preflight_compact_payload_exposes_panel_checks(tmp_path: Path) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "monday-live",
        "monday_live_preflight": "WARN",
        "blocking_reasons": ["paper_trade_allowed_true: paper_trade_allowed=False"],
        "warnings": [],
        "checks": [
            {
                "name": "gc_phase1_paper_candidate_visible_no_submit",
                "status": "PASS",
                "detail": (
                    "strategy=gc_1x_asia_london_participation__asia_london_long_v5; "
                    "candidate_evaluation_ready=True; "
                    "paper_candidate_approved=True; "
                    "paper_watch_ready=False; "
                    "can_submit=False; "
                    "live_money_eligible=False; "
                    "realtime_feed_confirmed=True"
                ),
            },
            {"name": "paper_trade_allowed_true", "status": "FAIL", "detail": "paper_trade_allowed=False"},
            {"name": "market_data_not_stale", "status": "PASS", "detail": "market_data_stale_count=0"},
            {"name": "monitor_healthy", "status": "PASS", "detail": "health_classification=HEALTHY"},
            {"name": "monitor_not_stale", "status": "PASS", "detail": "stale=False"},
            {"name": "monitor_submit_allowed", "status": "PASS", "detail": "submit_allowed=True"},
            {"name": "bridge_allowed", "status": "PASS", "detail": "bridge_allowed=True"},
            {"name": "broker_gc_flat_if_connected", "status": "PASS", "detail": "GC quantities=[0.0]"},
            {"name": "broker_gc_open_orders_zero_if_connected", "status": "PASS", "detail": "GC open_orders=0"},
            {"name": "no_current_review_required", "status": "PASS", "detail": "no current review_required flag found"},
            {
                "name": "phase1_runtime_candles_ready_for_strategy_approved_symbols",
                "status": "PASS",
                "detail": "strategy_required_symbols=['GC']; required_not_ready=[]",
            },
            {"name": "phase1_runtime_data_readiness_static", "status": "PASS", "detail": "ready_ticker_count=0"},
        ],
    }

    compact = operator_dashboard_module._compact_track_b_phase1_gc_preflight_readiness(
        payload,
        tmp_path / "latest_track_b_paper_preflight.json",
    )

    assert compact["classification"] == "GC_PHASE1_PREFLIGHT_BLOCKED"
    assert compact["panel_label"] == "Legacy GC Phase-1 diagnostic"
    assert compact["authority_scope"] == "DIAGNOSTIC_ONLY"
    assert compact["runtime_authority"] == "canonical_readiness_and_phase1_runtime_artifacts"
    assert compact["diagnostic_only"] is True
    assert compact["not_routing_authority"] is True
    assert compact["superseded_by"] == (
        "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json"
    )
    assert compact["legacy_lifecycle_diagnostic_authoritative"] is False
    assert compact["paper_trade_allowed"] is False
    assert compact["market_data_not_stale"] is True
    assert compact["runtime_candles_ready"] is True
    assert compact["derived_features_ready"] is True
    assert compact["monitor_healthy"] is True
    assert compact["monitor_not_stale"] is True
    assert compact["monitor_submit_allowed"] is True
    assert compact["bridge_allowed"] is True
    assert compact["broker_gc_flat"] is True
    assert compact["broker_gc_open_orders_zero"] is True
    assert compact["no_current_review_required"] is True
    assert compact["safety_checks_pass"] is False


def test_archived_paper_trade_log_recovers_closed_trades_from_alerts_when_trade_files_are_missing(tmp_path: Path) -> None:
    lane_dir = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "nq_1x_ny_early_core__ny_early_long"
    lane_dir.mkdir(parents=True)
    alerts = [
        {
            "event_type": "alert_event",
            "category": "entry_created",
            "detail": {
                "instrument": "NQ",
                "display_name": "NQ / NY_EARLY_LONG / x1",
                "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__nq_1x_ny_early_core__ny_early_long",
                "order_intent_id": "NQ|1m|2026-04-22T12:39:00Z|BUY_TO_OPEN",
                "intent_type": "BUY_TO_OPEN",
                "quantity": 1,
                "reason_code": "indexNyEarlyLongV5",
                "occurred_at": "2026-04-22T08:39:00-04:00",
            },
        },
        {
            "event_type": "alert_event",
            "category": "entry_filled",
            "detail": {
                "instrument": "NQ",
                "display_name": "NQ / NY_EARLY_LONG / x1",
                "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__nq_1x_ny_early_core__ny_early_long",
                "order_intent_id": "NQ|1m|2026-04-22T12:39:00Z|BUY_TO_OPEN",
                "intent_type": "BUY_TO_OPEN",
                "quantity": 1,
                "fill_price": "26848.75",
                "fill_timestamp": "2026-04-22T08:39:00-04:00",
                "broker_order_id": "paper-NQ|1m|2026-04-22T12:39:00Z|BUY_TO_OPEN",
            },
        },
        {
            "event_type": "alert_event",
            "category": "exit_created",
            "detail": {
                "instrument": "NQ",
                "display_name": "NQ / NY_EARLY_LONG / x1",
                "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__nq_1x_ny_early_core__ny_early_long",
                "order_intent_id": "NQ|1m|2026-04-22T12:40:00Z|SELL_TO_CLOSE",
                "intent_type": "SELL_TO_CLOSE",
                "quantity": 1,
                "reason_code": "forced_session_initial_stop",
                "occurred_at": "2026-04-22T08:40:00-04:00",
            },
        },
        {
            "event_type": "alert_event",
            "category": "exit_filled",
            "detail": {
                "instrument": "NQ",
                "display_name": "NQ / NY_EARLY_LONG / x1",
                "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__nq_1x_ny_early_core__ny_early_long",
                "order_intent_id": "NQ|1m|2026-04-22T12:40:00Z|SELL_TO_CLOSE",
                "intent_type": "SELL_TO_CLOSE",
                "quantity": 1,
                "fill_price": "26850.25",
                "fill_timestamp": "2026-04-22T08:40:00-04:00",
                "broker_order_id": "paper-NQ|1m|2026-04-22T12:40:00Z|SELL_TO_CLOSE",
            },
        },
    ]
    (lane_dir / "alerts.jsonl").write_text("\n".join(json.dumps(row) for row in alerts) + "\n", encoding="utf-8")

    rows = _archived_paper_trade_log_rows(repo_root=tmp_path)

    assert len(rows) == 1
    row = rows[0]
    assert row["lane_id"] == "nq_1x_ny_early_core__ny_early_long"
    assert row["strategy_key"] == "index_futures_ny_intraday_forced_core_v2__nq_1x_ny_early_core__ny_early_long"
    assert row["entry_timestamp"] == "2026-04-22T08:39:00-04:00"
    assert row["exit_timestamp"] == "2026-04-22T08:40:00-04:00"
    assert row["realized_pnl"] == "30.00"
