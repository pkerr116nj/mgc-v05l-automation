from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_operational_certification import (
    aggregate_operational_status,
    build_operational_certification,
    main,
)


NOW = datetime(2026, 6, 30, 12, 0, tzinfo=UTC)


def test_aggregate_all_green_certified() -> None:
    domains = {"runtime": {"status": "PASS", "checks": [{"code": "ok", "status": "PASS", "severity": "critical"}]}}

    summary = aggregate_operational_status(domains)

    assert summary["classification"] == "PLATFORM_CERTIFIED"
    assert summary["critical_failure_count"] == 0
    assert summary["warning_count"] == 0


def test_aggregate_live_pass_om_pending_certified_with_warnings() -> None:
    domains = {
        "runtime": {"status": "PASS", "checks": [{"code": "runtime_ok", "status": "PASS", "severity": "critical"}]},
        "wmc": {"status": "WARN", "checks": [{"code": "strategy_dashboard_pending", "status": "FAIL", "severity": "warning"}]},
    }

    summary = aggregate_operational_status(domains)

    assert summary["classification"] == "PLATFORM_CERTIFIED_WITH_WARNINGS"
    assert summary["critical_failure_count"] == 0
    assert "strategy_dashboard_pending" in summary["warnings"]


def test_aggregate_broker_unsafe_not_certified() -> None:
    domains = {
        "broker_orders": {
            "status": "FAIL",
            "checks": [{"code": "unknown_orders_zero", "status": "FAIL", "severity": "critical"}],
        }
    }

    summary = aggregate_operational_status(domains)

    assert summary["classification"] == "PLATFORM_NOT_CERTIFIED"
    assert "unknown_orders_zero" in summary["critical_failures"]


def test_aggregate_safe_state_hard_hold_not_certified() -> None:
    domains = {
        "safe_state_guardian": {
            "status": "FAIL",
            "checks": [{"code": "safe_state_normal", "status": "FAIL", "severity": "critical"}],
        }
    }

    summary = aggregate_operational_status(domains)

    assert summary["classification"] == "PLATFORM_NOT_CERTIFIED"
    assert "safe_state_normal" in summary["critical_failures"]


def test_missing_optional_om_report_is_warning(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path, include_o1=False)

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=True)

    assert result.report["classification"] == "PLATFORM_CERTIFIED_WITH_WARNINGS"
    storage = result.report["domains"]["storage_resources"]
    assert storage["status"] == "WARN"
    assert any(check["code"] == "o1_report_available" and check["status"] == "FAIL" for check in storage["checks"])
    assert result.json_path.exists()
    assert result.md_path.exists()


def test_stale_runtime_identity_with_fresh_progress_is_warning_not_failure(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path, runtime_identity_at=NOW - timedelta(minutes=30))

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=False)

    runtime = result.report["domains"]["runtime"]
    assert runtime["status"] == "WARN"
    assert any(check["code"] == "runtime_heartbeat_fresh" and check["status"] == "PASS" for check in runtime["checks"])
    assert any(check["code"] == "runtime_identity_fresh" and check["status"] == "FAIL" for check in runtime["checks"])
    assert result.report["classification"] == "PLATFORM_CERTIFIED_WITH_WARNINGS"


def test_dead_runtime_pid_is_not_certified(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path, runtime_pid=99_999_999)

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=False)

    runtime = result.report["domains"]["runtime"]
    assert any(check["code"] == "runtime_process_alive" and check["status"] == "FAIL" for check in runtime["checks"])
    assert result.report["classification"] == "PLATFORM_NOT_CERTIFIED"


def test_runtime_alive_without_fresh_progress_is_not_certified(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path, progress_at=NOW - timedelta(minutes=30))

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=False)

    runtime = result.report["domains"]["runtime"]
    assert any(check["code"] == "runtime_heartbeat_fresh" and check["status"] == "FAIL" for check in runtime["checks"])
    assert result.report["classification"] == "PLATFORM_NOT_CERTIFIED"


def test_missing_optional_progress_artifact_passes_with_another_fresh_source(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path, include_blocked_intent_progress=False)

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=False)

    runtime = result.report["domains"]["runtime"]
    assert any(check["code"] == "runtime_heartbeat_fresh" and check["status"] == "PASS" for check in runtime["checks"])
    assert result.report["classification"] in {"PLATFORM_CERTIFIED", "PLATFORM_CERTIFIED_WITH_WARNINGS"}


def test_cli_report_generation(tmp_path: Path) -> None:
    _seed_live_artifacts(tmp_path)

    exit_code = main(["--repo-root", str(tmp_path), "--expected-lane-count", "71", "--freshness-seconds", "600"])

    assert exit_code == 0
    report_path = tmp_path / "outputs/track_b_execution_core/operations_maintenance/operational_certification/latest_operational_certification.json"
    md_path = tmp_path / "outputs/track_b_execution_core/operations_maintenance/operational_certification/latest_operational_certification.md"
    assert report_path.exists()
    assert md_path.exists()
    report = json.loads(report_path.read_text())
    assert report["classification"] in {"PLATFORM_CERTIFIED", "PLATFORM_CERTIFIED_WITH_WARNINGS"}
    assert report["read_only_contract"]["broker_actions"] is False


def _seed_live_artifacts(
    root: Path,
    *,
    include_o1: bool = True,
    runtime_identity_at: datetime | None = None,
    progress_at: datetime | None = None,
    runtime_pid: int | None = None,
    include_blocked_intent_progress: bool = True,
) -> None:
    fresh = (NOW - timedelta(seconds=30)).isoformat()
    identity = (runtime_identity_at or NOW - timedelta(seconds=30)).isoformat()
    progress = (progress_at or NOW - timedelta(seconds=30)).isoformat()
    runtime_started = (NOW - timedelta(minutes=3)).isoformat()
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json",
        {
            "generated_at": identity,
            "runtime_started_at": runtime_started,
            "producer_pid": os.getpid() if runtime_pid is None else runtime_pid,
            "source_commit": "test-commit",
            "lane_count": 71,
        },
    )
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_detached_child_status.json",
        {"generated_at": fresh, "events": [{"state": "TRADING_LOOP_ENTERED"}]},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json",
        {"generated_at": fresh, "pid": os.getpid(), "classification": "NO_ELIGIBLE_EXITS", "exit_due_count": 0, "eligible_count": 0},
    )
    _write(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": fresh,
            "classification": "NO_OPEN_ORDERS",
            "canonical_refresh_scope": "GLOBAL_COMPLETE",
            "open_orders": [],
            "unknown_order_count": 0,
            "duplicate_close_order_groups": [],
        },
    )
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"generated_at": fresh, "positions": [], "live_money_eligible": False, "paper_proof_invoked": False},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": fresh,
            "classification": "NO_MANAGED_ORDERS",
            "managed_order_count": 0,
            "review_required_count": 0,
            "dmc_metadata": {"artifact_family": "latest_managed_orders"},
            "current_truth_invalidation": {"invalidated_orders": []},
        },
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": fresh,
            "classification": "NO_MANAGED_POSITIONS",
            "managed_position_count": 0,
            "exit_due_count": 0,
            "review_required_count": 0,
            "dmc_metadata": {"artifact_family": "latest_managed_positions"},
            "current_truth_invalidation": {"invalidated_positions": []},
        },
    )
    _write(
        root / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "generated_at": fresh,
            "classification": "SAFE_STATE_NORMAL",
            "submit_allowed": True,
            "managed_close_mutation_allowed": True,
            "tripped_limits": [],
        },
    )
    _write(
        root / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": fresh,
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "hard_classifications": [],
            "dmc_metadata": {"artifact_family": "latest_broker_position_guardian"},
            "current_truth_invalidation": {"invalidated_findings": []},
        },
    )
    _write(
        root / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "generated_at": fresh,
            "lease_state": "BROKER_TRUTH_LEASE_VALID",
            "dmc_metadata": {"artifact_family": "latest_broker_truth_lease"},
            "current_truth_invalidation": {"invalidated_lease": None},
        },
    )
    _write(root / "outputs/probationary_pattern_engine/paper_session/operator_status.json", {"generated_at": fresh, "ok": True})
    _write(root / "outputs/probationary_pattern_engine/paper_session/live_timing_summary_latest.json", {"generated_at": progress})
    if include_blocked_intent_progress:
        _write(
            root / "outputs/probationary_pattern_engine/paper_session/blocked_strategy_intent_latest.json",
            {"generated_at": progress, "artifact_type": "blocked_strategy_intent"},
        )
    _write(
        root / "outputs/reports/ibkr_runtime_route_dispatch/test_lane/ibkr_paper_strategy_bridge_report.json",
        {"generated_at": fresh, "_bounded_snapshot": {"degraded": False}},
    )
    _write(
        root / "outputs/reports/ibkr_strategy_governance/strategy_probation_dashboard.json",
        {"generated_at": fresh, "classification": "PAPER_STRATEGY_GOVERNANCE_READY"},
    )
    _write(
        root / "outputs/track_b_execution_core/operations_maintenance/o5a_archive_retention_contract/archive_retention_contract.json",
        {
            "artifact_families": [
                {"artifact_family": "operator_status.json", "certification_status": "certified", "current_archive_readiness": "candidate_after_final_consumer_check"},
                {"artifact_family": "strategy_probation_dashboard.json", "certification_status": "awaiting_natural_certification", "current_archive_readiness": "not_eligible_yet"},
            ]
        },
    )
    if include_o1:
        _write(
            root / "outputs/track_b_execution_core/operations_maintenance/phase_o1_hot_path_storage_audit/operations_maintenance_hot_path_audit.json",
            {"sizes": {"outputs": "1 MiB"}},
        )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
