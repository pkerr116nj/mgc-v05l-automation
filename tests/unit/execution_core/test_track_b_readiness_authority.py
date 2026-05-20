from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.app.track_b_paper_leak_test import (
    LeakTestLanePlan,
    LeakTestSafetySnapshot,
    _pre_apply_readiness_check,
)
from mgc_v05l.execution_core.track_b_readiness_authority import (
    CANONICAL_READINESS_SUMMARY_ARTIFACT,
    PHASE1_RECONCILIATION_ARTIFACT,
    build_track_b_readiness_authority,
)
from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT,
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
)


NOW = datetime(2026, 5, 20, 17, 0, tzinfo=timezone.utc)
RECONCILED = "TRACK_B_PAPER_BROKER_RECONCILED"


def test_stale_presentation_snapshots_do_not_block_fresh_canonical_authority(tmp_path: Path) -> None:
    _write_authority_artifacts(tmp_path)
    stale_at = (NOW - timedelta(hours=6)).isoformat()
    _write_json(
        tmp_path / "outputs/operator_dashboard/paper_readiness_snapshot.json",
        {"generated_at": stale_at, "paper_trade_allowed": False, "runtime_running": False},
    )
    _write_json(
        tmp_path / "outputs/operator_dashboard/startup_control_plane_snapshot.json",
        {"generated_at": stale_at, "overall_state": "NOT_READY"},
    )
    _write_json(
        tmp_path / "outputs/operator_dashboard/supervised_paper_operability_snapshot.json",
        {"generated_at": stale_at, "app_usable_for_supervised_paper": False},
    )

    result = _pre_apply_readiness_check(
        repo_root=tmp_path,
        lane=_lane(),
        safety=_safety(tmp_path),
        now=NOW,
    )

    assert result["classification"] == "LEAK_TEST_PRECHECK_READY"
    assert result["ready"] is True
    assert "backend_readiness_artifact_stale" not in result["blockers"]
    assert result["warnings"] == ("presentation_readiness_snapshot_stale_diagnostic_only",)
    assert result["readiness_authority"]["classification"] == "TRACK_B_READINESS_AUTHORITY_READY"
    assert result["paper_trade_allowed"] is True


def test_stale_canonical_readiness_blocks(tmp_path: Path) -> None:
    _write_authority_artifacts(tmp_path, generated_at=NOW - timedelta(minutes=10))

    result = build_track_b_readiness_authority(repo_root=tmp_path, expected_root=tmp_path, now=NOW)

    assert result["ready"] is False
    assert "canonical_readiness_stale" in result["blockers"]


def test_runtime_down_blocks(tmp_path: Path) -> None:
    _write_authority_artifacts(tmp_path, runtime_running=False)

    result = build_track_b_readiness_authority(repo_root=tmp_path, expected_root=tmp_path, now=NOW)

    assert result["ready"] is False
    assert "runtime_not_running" in result["blockers"]


def test_broker_or_reconciliation_unsafe_blocks(tmp_path: Path) -> None:
    _write_authority_artifacts(tmp_path, broker_lease_state="INVALIDATED_UNKNOWN_OPEN_ORDERS")
    broker_result = build_track_b_readiness_authority(repo_root=tmp_path, expected_root=tmp_path, now=NOW)
    assert broker_result["ready"] is False
    assert "broker_truth_lease_not_active" in broker_result["blockers"]

    _write_authority_artifacts(tmp_path, reconciliation_state="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED")
    recon_result = build_track_b_readiness_authority(repo_root=tmp_path, expected_root=tmp_path, now=NOW)
    assert recon_result["ready"] is False
    assert "broker_reconciliation_not_clean" in recon_result["blockers"]


def test_live_money_eligible_blocks(tmp_path: Path) -> None:
    _write_authority_artifacts(tmp_path, live_money_eligible=True)

    result = build_track_b_readiness_authority(repo_root=tmp_path, expected_root=tmp_path, now=NOW)

    assert result["ready"] is False
    assert "live_money_eligible_true" in result["blockers"]


def _write_authority_artifacts(
    repo_root: Path,
    *,
    generated_at: datetime = NOW,
    canonical_state: str = "READY_SUBMIT_CAPABLE",
    runtime_running: bool = True,
    broker_lease_state: str = "ACTIVE",
    reconciliation_state: str = RECONCILED,
    live_money_eligible: bool = False,
) -> None:
    broker_lease = {
        "generated_at": generated_at.isoformat(),
        "available": True,
        "lease_state": broker_lease_state,
        "submit_entry_allowed": True,
        "submit_exit_allowed": True,
        "operator_action_required": False,
        "live_money_eligible": live_money_eligible,
    }
    reconciliation = {
        "generated_at": generated_at.isoformat(),
        "available": True,
        "fresh": True,
        "classification": reconciliation_state,
        "broker_reconciled": reconciliation_state == RECONCILED,
        "review_required_count": 0,
        "lifecycle_open_position_count": 0,
        "track_b_broker_open_order_count": 0,
        "live_money_eligible": live_money_eligible,
    }
    canonical = {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": generated_at.isoformat(),
        "canonical_readiness": canonical_state,
        "state": canonical_state,
        "live_money_eligible": live_money_eligible,
        "root_guard_summary": {
            "root_match": True,
            "processes": [
                {
                    "name": "paper_runtime",
                    "pid": 12345 if runtime_running else None,
                    "running": runtime_running,
                    "cwd": str(repo_root),
                    "detected_root": str(repo_root),
                    "root_match": True,
                }
            ],
        },
        "runtime": {
            "running": runtime_running,
            "healthy": runtime_running,
            "runtime_ingestion_fresh": runtime_running,
            "loaded_lane_count": 15,
            "eligible_lane_count": 15,
        },
        "broker_truth_lease": broker_lease,
        "phase1_reconciliation": reconciliation,
        "readiness_blockers": [],
        "readiness_warnings": [],
    }
    summary = {
        "generated_at": generated_at.isoformat(),
        "classification": canonical_state,
        "broker_truth_lease_state": broker_lease_state,
        "reconciliation_state": reconciliation_state,
    }
    _write_json(repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT, canonical)
    _write_json(repo_root / CANONICAL_READINESS_SUMMARY_ARTIFACT, summary)
    _write_json(repo_root / DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT, broker_lease)
    _write_json(repo_root / PHASE1_RECONCILIATION_ARTIFACT, reconciliation)


def _lane() -> LeakTestLanePlan:
    return LeakTestLanePlan(
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        lane_id="mnq_1x_ny_early_core__us_early_long",
        display_name="MNQ early long",
        symbol="MNQ",
        localSymbol="MNQM6",
        expiry="202606",
        conId=123,
        entry_execution_intent="BUY_TO_OPEN",
        entry_price_source="guarded_leak_test",
        exit_plan_classification="LEAK_TEST_CONTROLLED_EXIT",
        exit_plan_source="test",
        exit_plan_blockers=(),
        expected_route="IBKR_PAPER",
        expected_contract={"symbol": "MNQ", "localSymbol": "MNQM6"},
        safe_to_test=True,
        blockers=(),
        warnings=(),
        safe_for_isolated_test=True,
        isolated_blockers=(),
        safe_for_concurrent_test=False,
        concurrent_blockers=(),
        max_qty=1,
        live_money_eligible=False,
        runtime_pid=12345,
        runtime_cwd=None,
    )


def _safety(repo_root: Path) -> LeakTestSafetySnapshot:
    return LeakTestSafetySnapshot(
        account_id="DUM882026",
        classification=RECONCILED,
        broker_reconciled=True,
        review_required_count=0,
        open_order_count=0,
        broker_position_count=0,
        lifecycle_position_count=0,
        live_money_eligible=False,
        paper_proof_invoked=False,
        runtime_pid=12345,
        runtime_pid_active=True,
        runtime_cwd=str(repo_root),
        runtime_command="python -m mgc_v05l.app.main",
        runtime_from_dev_root=True,
        runtime_from_documents_or_icloud=False,
        duplicate_runtime_submitter_count=0,
        active_leak_test_lane_id=None,
        existing_positions=(),
        existing_open_orders=(),
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
