from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    CONTROL_PLANE_SNAPSHOT_READY,
    CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED,
    TrackBControlPlaneSnapshotConfig,
    build_dashboard_control_plane_snapshot_projection,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_snapshot_ties_supervisor_to_shared_truth_generation(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["shared_truth_refresh_generation_id"] == "track-b-shared-truth-20260523T120000000000Z"
    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["shared_truth_generation_matches_supervisor"] is True
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert payload["supervisor_mode"] == "READY_FOR_OPERATOR_START"
    assert payload["safe_to_start_runtime"] is True
    assert payload["broker_order_position_summary"]["open_order_truth"] == "NO_OPEN_ORDERS"
    assert payload["source_artifact_paths"]["shared_truth_refresh"].endswith(
        "outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json"
    )


def test_market_closed_snapshot_waits_without_alarm(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(
        tmp_path,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        supervisor_classification="SUPERVISOR_WAIT_MARKET_CLOSED",
        supervisor_mode="MARKET_CLOSED_WAIT",
        resume_classification="RESUME_BLOCKED_MARKET_CLOSED",
        self_recover_recommendation="WAIT_MARKET_CLOSED",
    )

    payload = _snapshot(tmp_path)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_WAIT_MARKET_CLOSED"
    assert payload["supervisor_mode"] == "MARKET_CLOSED_WAIT"
    assert payload["proof_window_status"] == "market_closed"
    assert payload["recommended_next_command"] == "wait for market reopen; rerun proof readiness before any runtime start"


def test_stale_mixed_generation_blocks_snapshot(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def corrupt_open_order_truth(_shared_truth: dict) -> None:
        _write(
            tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "UNKNOWN_OPEN_ORDER",
                "live_money_eligible": False,
            },
        )

    payload = _snapshot(tmp_path, post_hook=corrupt_open_order_truth)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_SHARED_TRUTH_STALE"
    assert payload["shared_truth_coherence_status"] == "STALE_OR_MIXED"
    assert any(source["service"] == "Open Order Truth" for source in payload["stale_or_mixed_sources"])


def test_snapshot_includes_recovery_and_planner_fields(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["paper_recovery_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["autonomous_recovery_plan_classification"]
    assert payload["autonomous_recovery_next_action"]
    assert payload["autonomous_recovery_execution_enabled"] is False


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    config = TrackBControlPlaneSnapshotConfig(repo_root=tmp_path)
    payload = build_track_b_control_plane_snapshot(
        config=config,
        now=NOW,
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )

    authority_path = write_track_b_control_plane_snapshot(config=config, payload=payload)
    projection = json.loads(config.resolve(config.dashboard_projection_path).read_text(encoding="utf-8"))  # type: ignore[arg-type]
    direct_projection = build_dashboard_control_plane_snapshot_projection(
        authority_payload=payload,
        authority_path=authority_path,
    )

    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def test_dashboard_projection_not_consumed(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    projection_path = tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json"
    _write(
        projection_path,
        {
            "projection_only": True,
            "not_routing_authority": True,
            "classification": "POISONED_DASHBOARD_PROJECTION",
        },
    )

    payload = _snapshot(tmp_path)

    assert payload["classification"] != "POISONED_DASHBOARD_PROJECTION"
    assert payload["source_artifact_paths"]["control_plane_snapshot"].endswith(
        "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
    )


def _snapshot(
    root: Path,
    *,
    post_hook=None,
) -> dict:
    return build_track_b_control_plane_snapshot(
        config=TrackBControlPlaneSnapshotConfig(repo_root=root),
        now=NOW,
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
        post_shared_truth_refresh_hook=post_hook,
    )


def _seed_clean_stack(root: Path) -> None:
    _write_reconciliation(root)
    _write_broker_status(root)
    _write_live_position_status(root)
    _write_trade_summary(root)


def _seed_control_plane(
    root: Path,
    *,
    proof_classification: str = "READY_FOR_PROOF",
    supervisor_classification: str = "SUPERVISOR_RUNTIME_START_ALLOWED",
    supervisor_mode: str = "READY_FOR_OPERATOR_START",
    resume_classification: str = "RESUME_ALLOWED_CLEAN",
    self_recover_recommendation: str = "RESTART_RUNTIME_ALLOWED",
) -> None:
    _write(
        root / "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "phase1_session_reason": proof_classification,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "safe_to_start_runtime": supervisor_classification == "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": resume_classification,
            "allowed": resume_classification == "RESUME_ALLOWED_CLEAN",
            "safe_to_start_runtime": resume_classification == "RESUME_ALLOWED_CLEAN",
            "previous_broker_safe_at_stop": True,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_CRASH_LOOP",
            "restart_blocked": False,
            "operator_ack_required": False,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "AGENT_HEALTH_READY",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/agent_registry/latest_agent_registry.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_REGISTRY_READY"},
    )
    _write(
        root / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        {"generated_at": NOW.isoformat(), "canonical_readiness": "READY_SUBMIT_CAPABLE"},
    )
    _write(
        root / "outputs/reports/phase1_runtime_data_readiness/latest_phase1_runtime_data_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "market_session": {"classification": proof_classification},
            "rows": [],
        },
    )
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/latest_runtime_stop_provenance.json",
        {
            "generated_at": NOW.isoformat(),
            "stop_source": "launcher",
            "stop_reason": "expected_clean_down",
            "live_money_eligible": False,
        },
    )


def _write_reconciliation(root: Path) -> None:
    _write(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "live_money_eligible": False,
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": True},
            "blockers": [],
        },
    )


def _write_broker_status(root: Path) -> None:
    payload = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": NOW.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": [],
        "open_orders": [],
        "live_money_eligible": False,
    }
    _write(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {**payload, "last_successful_broker_truth": payload, "latest_attempt_status": payload},
    )
    _write(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", payload)


def _write_live_position_status(root: Path) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": NOW.isoformat(),
            "open_position_count": 0,
            "open_order_count": 0,
            "open_positions": [],
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )


def _write_trade_summary(root: Path) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": NOW.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
