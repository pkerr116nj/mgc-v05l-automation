from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core import track_b_shared_truth_refresh_cli as shared_truth_module
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    DEFAULT_RECONCILIATION_ARTIFACT,
    TrackBSharedTruthRefreshConfig,
    build_runtime_start_preflight_summary,
    main,
    refresh_track_b_shared_truth,
)


NOW = datetime(2026, 5, 22, 18, 0, tzinfo=UTC)
OLD = "2026-05-22T17:00:00+00:00"


def test_refresh_clean_flat_stack(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["refresh_phase"] == "pre_supervisor_refresh"
    assert result["refresh_generation_id"] == "track-b-shared-truth-20260522T180000000000Z"
    assert result["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"
    assert result["classifications"]["Managed Order Registry"] == "NO_MANAGED_ORDERS"
    assert result["classifications"]["Position Truth"] == "CLEAN_FLAT_READY"
    assert result["classifications"]["Runtime Environment Truth"] == "RUNTIME_DOWN_CLEAN"
    assert result["classifications"]["Managed Position Registry"] == "NO_MANAGED_POSITIONS"
    assert result["classifications"]["Reconciliation"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert result["classifications"]["Broker Position Guardian"] == "BROKER_POSITION_GUARDIAN_READY"
    assert result["classifications"]["PAPER Recovery Policy"] == "REFRESH_EVIDENCE"
    assert result["classifications"]["PAPER Autonomous Recovery Planner"] == "PLAN_BLOCKED_STALE_EVIDENCE"
    assert result["paper_recovery_policy"] == "REFRESH_EVIDENCE"
    assert result["autonomous_recovery_plan_classification"] == "PLAN_BLOCKED_STALE_EVIDENCE"
    assert result["autonomous_recovery_next_action"] == "REFRESH_EVIDENCE"
    assert result["autonomous_recovery_execution_enabled"] is False
    assert result["bounded_current_scope_fast_path"]["used"] is True
    assert result["bounded_current_scope_fast_path"]["skipped_full_registry_reduction"] is True
    assert result["bounded_current_scope_fast_path"]["skipped_manifest_directory_scan"] is True
    assert result["unsafe_blockers"] == []
    assert _read(tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")[
        "classification"
    ] == "NO_OPEN_ORDERS"
    shared_truth_path = tmp_path / "outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json"
    shared_truth = _read(shared_truth_path)
    assert shared_truth["refresh_generation_id"] == result["refresh_generation_id"]
    assert shared_truth["authority_generation_id"] == result["refresh_generation_id"]
    assert shared_truth["authority_cycle_generated_at"] == result["generated_at"]
    assert shared_truth["refresh_phase"] == "pre_supervisor_refresh"
    position_truth = _read(tmp_path / "outputs/track_b_execution_core/position_truth/latest_position_truth.json")
    runtime_truth = _read(tmp_path / "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json")
    assert position_truth["authority_generation_id"] == result["refresh_generation_id"]
    assert runtime_truth["authority_generation_id"] == result["refresh_generation_id"]
    assert runtime_truth["source_generation_references"]["position_truth_authority_generation_id"] == result[
        "refresh_generation_id"
    ]
    assert runtime_truth["source_generation_references"]["position_truth_generated_at"] == position_truth["generated_at"]
    assert result["source_refresh_artifact_path"] == str(shared_truth_path)
    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    assert preflight["blockers"] == []
    assert preflight["refresh_generation_id"] == result["refresh_generation_id"]


def test_refresh_writes_autonomous_recovery_planner_artifact(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)
    planner_path = tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json"

    planner = _read(planner_path)
    assert result["artifact_paths"]["PAPER Autonomous Recovery Planner"] == str(planner_path)
    assert planner["schema_version"] == "track_b_paper_autonomous_recovery_plan_v1"
    assert planner["execution_enabled"] is False
    assert planner["classification"] == result["autonomous_recovery_plan_classification"]
    shared_truth = _read(tmp_path / "outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json")
    assert "Runtime Supervisor Authority" not in {row["service"] for row in shared_truth["services"]}


def test_refresh_replaces_stale_upstream_authority_artifact(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    stale_path = tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"
    _write(stale_path, {"generated_at": OLD, "classification": "ORDER_TRUTH_STALE"})

    result = _refresh(tmp_path)

    refreshed = _read(stale_path)
    assert result["exit_code"] == 0
    assert refreshed["classification"] == "NO_OPEN_ORDERS"
    assert refreshed["generated_at"] == NOW.isoformat()


def test_refresh_consumes_published_broker_lease_without_overwriting_it(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    lease_path = tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"
    published_lease = {
        "schema_version": "track_b_broker_truth_lease_v1",
        "generated_at": OLD,
        "lease_state": "ACTIVE",
        "connection_mode": "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
        "publisher": "broker_truth_refresher",
    }
    _write(lease_path, published_lease)

    result = _refresh(tmp_path)

    disk = _read(lease_path)
    row = next(row for row in result["services"] if row["service"] == "Broker Truth Lease")
    assert disk == published_lease
    assert row["classification"] == "ACTIVE"
    assert row["generated_at"] == OLD


def test_refresh_service_rows_match_written_authority_files(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    original_write = shared_truth_module.write_track_b_open_order_truth

    def skew_open_order_generated_at(**kwargs):
        path, events = original_write(**kwargs)
        payload = _read(path)
        payload["generated_at"] = OLD
        _write(path, payload)
        return path, events

    monkeypatch.setattr(shared_truth_module, "write_track_b_open_order_truth", skew_open_order_generated_at)

    result = _refresh(tmp_path)
    row = next(row for row in result["services"] if row["service"] == "Open Order Truth")
    disk = _read(tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")

    assert row["classification"] == "NO_OPEN_ORDERS"
    assert row["generated_at"] == disk["generated_at"] == OLD
    assert result["classifications"]["Open Order Truth"] == disk["classification"]


def test_refresh_replaces_stale_autonomous_recovery_plan(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    stale_path = tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json"
    _write(stale_path, {"generated_at": OLD, "classification": "PLAN_RUNTIME_RETRY", "execution_enabled": False})

    result = _refresh(tmp_path)

    refreshed = _read(stale_path)
    assert refreshed["generated_at"] == NOW.isoformat()
    assert refreshed["classification"] == result["autonomous_recovery_plan_classification"]
    assert refreshed["execution_enabled"] is False


def test_refresh_clean_flat_fast_path_avoids_historical_builders(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    calls: list[str] = []

    def count_call(name: str) -> None:
        original = getattr(shared_truth_module, name)

        def counted(*args, **kwargs):
            calls.append(name)
            return original(*args, **kwargs)

        monkeypatch.setattr(shared_truth_module, name, counted)

    for name in [
        "build_track_b_open_order_truth",
        "build_track_b_managed_order_registry",
        "build_track_b_position_truth",
        "build_track_b_runtime_environment_truth",
        "build_track_b_managed_position_registry",
    ]:
        count_call(name)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["bounded_current_scope_fast_path"]["used"] is True
    assert calls.count("build_track_b_open_order_truth") == 0
    assert calls.count("build_track_b_managed_order_registry") == 0
    assert calls.count("build_track_b_position_truth") == 0
    assert calls.count("build_track_b_runtime_environment_truth") == 1
    assert calls.count("build_track_b_managed_position_registry") == 0


def test_refresh_current_scope_blocker_disables_fast_path(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_registry_diagnostics(
        tmp_path,
        classification="TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE",
        current_scope_review_required_count=1,
        current_scope_trade_states=[{"trade_id": "trade_review", "current_state": "OPEN_MANAGED"}],
    )
    calls: list[str] = []
    original = shared_truth_module.build_track_b_open_order_truth

    def counted(*args, **kwargs):
        calls.append("build_track_b_open_order_truth")
        return original(*args, **kwargs)

    monkeypatch.setattr(shared_truth_module, "build_track_b_open_order_truth", counted)

    result = _refresh(tmp_path)

    assert result["bounded_current_scope_fast_path"]["used"] is False
    assert "registry_diagnostics_current_scope_clean" in result["bounded_current_scope_fast_path"]["disabled_reasons"]
    assert calls == ["build_track_b_open_order_truth"]


def test_refresh_non_flat_broker_exposure_disables_fast_path(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_reconciliation(
        tmp_path,
        broker_positions=[
            {
                "account": "DUM882026",
                "symbol": "MES",
                "localSymbol": "MESM6",
                "conId": 770561194,
                "position": "-1",
            }
        ],
    )
    calls: list[str] = []
    original = shared_truth_module.build_track_b_open_order_truth

    def counted(*args, **kwargs):
        calls.append("build_track_b_open_order_truth")
        return original(*args, **kwargs)

    monkeypatch.setattr(shared_truth_module, "build_track_b_open_order_truth", counted)

    result = _refresh(tmp_path)

    assert result["bounded_current_scope_fast_path"]["used"] is False
    assert "broker_positions_empty" in result["bounded_current_scope_fast_path"]["disabled_reasons"]
    assert calls == ["build_track_b_open_order_truth"]


def test_refresh_historical_registry_debris_does_not_disable_fast_path(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_registry_diagnostics(
        tmp_path,
        classification="TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED",
        current_scope_review_required_count=0,
        diagnostic_only=True,
    )

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["bounded_current_scope_fast_path"]["used"] is True
    assert result["bounded_current_scope_fast_path"]["registry_diagnostics_classification"] == (
        "TRACK_B_DIAGNOSTICS_HISTORICAL_REVIEW_REQUIRED"
    )


def test_refresh_market_closed_autonomous_plan_waits(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_recovery_control_plane(
        tmp_path,
        proof_classification="MARKET_CLOSED_NO_FRESH_BARS",
        supervisor_classification="SUPERVISOR_WAIT_MARKET_CLOSED",
        supervisor_mode="MARKET_CLOSED_WAIT",
        resume_classification="RESUME_BLOCKED_MARKET_CLOSED",
        self_recover_recommendation="WAIT_MARKET_CLOSED",
    )

    result = _refresh(tmp_path)

    assert result["paper_recovery_policy"] == "OBSERVE"
    assert result["autonomous_recovery_plan_classification"] == "WAIT_MARKET_CLOSED"
    assert result["autonomous_recovery_next_action"] == "WAIT_MARKET_CLOSED"
    assert result["autonomous_recovery_execution_enabled"] is False


def test_refresh_surfaces_planner_staleness_as_advisory_warning(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["autonomous_recovery_plan_classification"] == "PLAN_BLOCKED_STALE_EVIDENCE"
    assert any(warning["code"] == "paper_autonomous_recovery_plan_advisory_stale" for warning in result["warnings"])
    assert not any(blocker["code"] == "broker_lease_invalidated" for blocker in result["unsafe_blockers"])


def test_refresh_broker_exposure_produces_attention_required(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_reconciliation(
        tmp_path,
        classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        broker_reconciled=False,
        broker_positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )
    _write_broker_status(
        tmp_path,
        positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )

    result = _refresh(tmp_path)

    assert result["exit_code"] == 2
    assert result["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"
    assert result["classifications"]["Position Truth"] == "ATTENTION_REQUIRED"
    assert result["classifications"]["Runtime Environment Truth"] == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
    assert any(blocker["code"] == "position_truth_attention_required" for blocker in result["unsafe_blockers"])
    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_BLOCKED"
    assert any(blocker["code"] == "position_truth_not_clean_for_runtime_start" for blocker in preflight["blockers"])


def test_refresh_allows_reconciled_managed_timed_hold_pending(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    broker_position = {"symbol": "MNQ", "local_symbol": "MNQM6", "con_id": 770561201, "quantity": "1", "account": "DUM882026"}
    lifecycle_position = {
        "instrument_family": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "current_managed_mnq",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "bars_since_fill": 1,
    }
    _write_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILED",
        broker_reconciled=True,
        broker_positions=[broker_position],
        lifecycle_positions=[lifecycle_position],
    )
    _write_broker_status(tmp_path, positions=[broker_position])
    _write_live_position_status(tmp_path, open_positions=[lifecycle_position])
    _write_lifecycle_report(tmp_path, lifecycle_position)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"
    assert result["classifications"]["Managed Order Registry"] == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
    assert result["classifications"]["Position Truth"] == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
    assert result["classifications"]["Managed Position Registry"] == "OPEN_MANAGED_MATCHED"
    assert result["unsafe_blockers"] == []
    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    assert preflight["active_hold_managed_timed_exit_pending"] is True


def test_refresh_allows_reconciled_managed_exit_due_without_close_order(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    broker_position = {"symbol": "MES", "local_symbol": "MESM6", "con_id": 770561194, "quantity": "1", "account": "DUM882026"}
    lifecycle_position = {
        "instrument_family": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "current_managed_mes",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "bars_since_fill": 12,
    }
    _write_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILED",
        broker_reconciled=True,
        broker_positions=[broker_position],
        lifecycle_positions=[lifecycle_position],
    )
    _write_broker_status(tmp_path, positions=[broker_position])
    _write_live_position_status(tmp_path, open_positions=[lifecycle_position])
    _write_lifecycle_report(tmp_path, lifecycle_position)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"
    assert result["classifications"]["Managed Order Registry"] == "POSITION_WITHOUT_CLOSE_ORDER"
    assert result["classifications"]["Position Truth"] == "ATTENTION_REQUIRED"
    assert result["classifications"]["Managed Position Registry"] == "OPEN_MANAGED_EXIT_DUE"
    assert result["unsafe_blockers"] == []
    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    assert preflight["active_managed_exit_due"] is True


def test_runtime_start_preflight_blocks_open_order(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_reconciliation(
        tmp_path,
        classification="BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
        broker_reconciled=False,
        open_orders=[{"symbol": "MNQ", "local_symbol": "MNQM6", "action": "SELL", "order_id": 27}],
    )
    _write_broker_status(
        tmp_path,
        open_orders=[{"symbol": "MNQ", "local_symbol": "MNQM6", "action": "SELL", "order_id": 27}],
    )

    result = _refresh(tmp_path)

    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_BLOCKED"
    assert any(blocker["code"] == "open_order_truth_not_clean_for_runtime_start" for blocker in preflight["blockers"])


def test_runtime_start_preflight_blocks_managed_position(tmp_path: Path) -> None:
    result = {
        "generated_at": NOW.isoformat(),
        "live_money_eligible": False,
        "unsafe_blockers": [],
        "artifact_paths": {},
        "warnings": [],
        "classifications": {
            "Open Order Truth": "NO_OPEN_ORDERS",
            "Managed Order Registry": "NO_MANAGED_ORDERS",
            "Position Truth": "CLEAN_FLAT_READY",
            "Runtime Environment Truth": "RUNTIME_DOWN_CLEAN",
            "Managed Position Registry": "OPEN_MANAGED_MATCHED",
            "Reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
            "Broker Truth Lease": "ACTIVE",
        },
    }

    preflight = build_runtime_start_preflight_summary(result)
    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_BLOCKED"
    assert any(
        blocker["code"] == "managed_position_registry_not_clean_for_runtime_start"
        for blocker in preflight["blockers"]
    )


def test_runtime_start_preflight_allows_active_degraded_broker_lease_within_valid_window(tmp_path: Path) -> None:
    result = {
        "generated_at": NOW.isoformat(),
        "live_money_eligible": False,
        "unsafe_blockers": [],
        "artifact_paths": {},
        "warnings": [],
        "classifications": {
            "Open Order Truth": "NO_OPEN_ORDERS",
            "Managed Order Registry": "NO_MANAGED_ORDERS",
            "Position Truth": "CLEAN_FLAT_READY",
            "Runtime Environment Truth": "RUNTIME_DOWN_CLEAN",
                "Managed Position Registry": "NO_MANAGED_POSITIONS",
                "Reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
                "Broker Truth Lease": "ACTIVE_DEGRADED_REFRESH_FAILING",
                "Broker Position Guardian": "BROKER_POSITION_GUARDIAN_READY",
            },
        }

    preflight = build_runtime_start_preflight_summary(result)

    assert preflight["classification"] == "SHARED_TRUTH_PREFLIGHT_CLEAN"
    assert preflight["blockers"] == []


def test_dashboard_projections_are_not_consumed_or_written(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    projection_paths = [
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_position_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_runtime_environment_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_managed_positions.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_paper_recovery_policy.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_paper_autonomous_recovery_plan.json",
    ]
    assert all(not path.exists() for path in projection_paths)
    for path in result["artifact_paths"].values():
        assert "outputs/operator_dashboard/runtime/latest_track_b_" not in str(path)


def test_main_runtime_down_clean_exits_zero(tmp_path: Path, capsys) -> None:
    _seed_clean_stack(tmp_path, now=datetime.now(UTC))

    exit_code = main(["--repo-root", str(tmp_path), "--no-broker-lease-history"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Runtime Environment Truth" in output
    assert "RUNTIME_DOWN_CLEAN" in output


def test_main_runtime_start_preflight_blocks_attention_required(tmp_path: Path, capsys) -> None:
    _seed_clean_stack(tmp_path)
    _write_reconciliation(
        tmp_path,
        classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        broker_reconciled=False,
        broker_positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )
    _write_broker_status(
        tmp_path,
        positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )

    exit_code = main(["--repo-root", str(tmp_path), "--no-broker-lease-history", "--runtime-start-preflight"])

    output = capsys.readouterr().out
    assert exit_code == 2
    assert "Runtime start preflight: SHARED_TRUTH_PREFLIGHT_BLOCKED" in output
    assert "Position Truth is ATTENTION_REQUIRED" in output


def _refresh(root: Path) -> dict:
    return refresh_track_b_shared_truth(
        config=TrackBSharedTruthRefreshConfig(repo_root=root, broker_lease_history_path=None),
        now=NOW,
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )


def _seed_clean_stack(root: Path, *, now: datetime = NOW) -> None:
    _write_reconciliation(root, now=now)
    _write_registry_diagnostics(root, now=now)
    _write_broker_status(root, now=now)
    _write_live_position_status(root, now=now)
    _write_trade_summary(root, now=now)
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {
            "generated_at": now.isoformat(),
            "summary": {
                "overall_classification": "CLEAN_FLAT_READY",
                "broker_exposure_present": False,
            },
            "broker_positions": [],
            "open_broker_orders": [],
        },
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": now.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "summary": {"managed_position_count": 0},
        },
    )


def _seed_recovery_control_plane(
    root: Path,
    *,
    now: datetime = NOW,
    proof_classification: str = "READY_FOR_PROOF",
    supervisor_classification: str = "SUPERVISOR_RUNTIME_START_ALLOWED",
    supervisor_mode: str = "READY_FOR_OPERATOR_START",
    resume_classification: str = "RESUME_ALLOWED_CLEAN",
    self_recover_recommendation: str = "RESTART_RUNTIME_ALLOWED",
) -> None:
    _write(
        root / "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json",
        {
            "generated_at": now.isoformat(),
            "classification": proof_classification,
            "phase1_session_reason": proof_classification,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": now.isoformat(),
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "recommended_next_command": "wait for market reopen",
            "safe_to_start_runtime": supervisor_classification == "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": now.isoformat(),
            "classification": resume_classification,
            "previous_broker_safe_at_stop": True,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json",
        {
            "generated_at": now.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json",
        {
            "generated_at": now.isoformat(),
            "classification": "NO_CRASH_LOOP",
            "restart_blocked": False,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "generated_at": now.isoformat(),
            "classification": "AGENT_HEALTH_READY",
            "agents": [
                {
                    "agent_id": "phase1_databento_live_candles",
                    "status": "HEALTHY",
                    "reason": proof_classification,
                }
            ],
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
        {
            "generated_at": now.isoformat(),
            "classification": "RUNTIME_DOWN_CLEAN",
            "live_money_eligible": False,
        },
    )


def _write_reconciliation(
    root: Path,
    *,
    now: datetime = NOW,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    broker_positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
    lifecycle_positions: list[dict] | None = None,
) -> None:
    positions = broker_positions or []
    orders = open_orders or []
    lifecycle_rows = lifecycle_positions or []
    _write(
        root / DEFAULT_RECONCILIATION_ARTIFACT,
        {
            "generated_at": now.isoformat(),
            "classification": classification,
            "broker_reconciled": broker_reconciled,
            "live_money_eligible": False,
            "track_b_broker_positions": positions,
            "track_b_broker_open_orders": orders,
            "track_b_lifecycle_positions": lifecycle_rows,
            "unknown_broker_open_orders": orders if orders else [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": len(positions),
            "track_b_broker_open_order_count": len(orders),
            "unknown_broker_open_order_count": len(orders),
            "current_scope_review_required_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": len(lifecycle_rows),
            "lifecycle_open_order_count": len(orders),
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": broker_reconciled},
            "blockers": [] if broker_reconciled else [{"code": "broker_position_without_lifecycle"}],
        },
    )


def _write_registry_diagnostics(
    root: Path,
    *,
    now: datetime = NOW,
    classification: str = "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
    current_scope_review_required_count: int = 0,
    current_scope_trade_states: list[dict] | None = None,
    diagnostic_only: bool = False,
) -> None:
    _write(
        root / "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics.json",
        {
            "generated_at": now.isoformat(),
            "classification": classification,
            "current_scope_review_required_count": current_scope_review_required_count,
            "current_scope_trade_states": current_scope_trade_states or [],
            "current_blockers": [],
            "diagnostic_only": diagnostic_only,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )


def _write_lifecycle_report(root: Path, lifecycle_position: dict) -> None:
    lifecycle_id = str(lifecycle_position["lifecycle_id"])
    _write(
        root
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "generated_at": NOW.isoformat(),
            "lifecycle_id": lifecycle_id,
            "instrument_family": lifecycle_position.get("instrument_family"),
            "local_symbol": lifecycle_position.get("local_symbol"),
            "con_id": lifecycle_position.get("con_id"),
            "final_position_status": "OPEN_MANAGED",
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
            "managed_exit_policy_id": lifecycle_position.get("managed_exit_policy_id"),
            "managed_exit_policy_max_completed_5m_bars": 3,
            "bars_since_fill": lifecycle_position.get("bars_since_fill"),
            "entry_intent": {"side": lifecycle_position.get("side"), "quantity": lifecycle_position.get("quantity")},
            "entry_fill": {"price": "29976.81", "filled_at": "2026-05-25T11:13:29+00:00"},
        },
    )


def _write_broker_status(
    root: Path,
    *,
    now: datetime = NOW,
    positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
) -> None:
    payload = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": now.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": positions or [],
        "open_orders": open_orders or [],
        "live_money_eligible": False,
    }
    _write(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            **payload,
            "last_successful_broker_truth": payload,
            "latest_attempt_status": payload,
        },
    )
    _write(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", payload)


def _write_live_position_status(root: Path, *, now: datetime = NOW, open_positions: list[dict] | None = None) -> None:
    positions = open_positions or []
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": now.isoformat(),
            "open_position_count": len(positions),
            "open_order_count": 0,
            "open_positions": positions,
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )


def _write_trade_summary(root: Path, *, now: datetime = NOW) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": now.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
