from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_lifecycle_local_repair_guard import (
    LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE,
    LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION,
    LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING,
    LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE,
    LIFECYCLE_LOCAL_REPAIR_VALID,
    TrackBLifecycleLocalRepairGuardConfig,
    validate_lifecycle_local_artifact_repair,
)


NOW = datetime(2026, 5, 23, 7, 0, tzinfo=UTC)


def test_unknown_lifecycle_state_is_rejected(tmp_path: Path) -> None:
    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="OLD_LOCAL_MAGIC",
        target_state="CLOSED_FLAT",
        evidence=_closed_flat_evidence(),
        apply=False,
        active_state_affecting=False,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE
    assert result["dashboard_projection_consumed"] is False


def test_invalid_matrix_transition_is_rejected(tmp_path: Path) -> None:
    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="CLOSED_FLAT",
        target_state="OPEN_MANAGED",
        evidence=_open_managed_evidence(),
        apply=False,
        active_state_affecting=False,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION


def test_closed_flat_without_fill_or_broker_flat_proof_is_rejected(tmp_path: Path) -> None:
    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="OPEN_MANAGED",
        target_state="CLOSED_FLAT",
        evidence={"requested_lifecycle_status": "CLOSED_FLAT"},
        apply=False,
        active_state_affecting=False,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE
    assert "close_fill_or_broker_flat_proof" in result["blockers"]


def test_active_apply_requires_control_plane_snapshot(tmp_path: Path) -> None:
    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="OPEN_MANAGED",
        target_state="CLOSED_FLAT",
        evidence=_closed_flat_evidence(),
        apply=True,
        active_state_affecting=True,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING
    assert result["control_plane_snapshot_required"] is True


def test_historical_matrix_aligned_repair_is_valid_in_dry_run(tmp_path: Path) -> None:
    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="OPEN_MANAGED",
        target_state="MANUAL_OR_MALFORMED_CLEANUP",
        evidence={
            "requested_lifecycle_status": "MANUAL_OR_MALFORMED_CLEANUP",
            "operator_review_or_malformed_artifact_evidence": True,
            "malformed_artifact_evidence": True,
        },
        apply=False,
        active_state_affecting=True,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_VALID
    assert result["clean_trade_stats_allowed"] is False
    assert result["managed_position_registry_allowed"] is False


def test_fresh_coherent_snapshot_allows_active_apply_validation(tmp_path: Path) -> None:
    _write_snapshot(tmp_path)

    result = validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(repo_root=tmp_path),
        current_state="OPEN_MANAGED",
        target_state="CLOSED_FLAT",
        evidence=_closed_flat_evidence(),
        apply=True,
        active_state_affecting=True,
        now=NOW,
    )

    assert result["classification"] == LIFECYCLE_LOCAL_REPAIR_VALID
    assert result["control_plane_snapshot_id"] == "snapshot-test"
    assert result["shared_truth_refresh_generation_id"] == "generation-test"


def _closed_flat_evidence() -> dict[str, object]:
    return {
        "requested_lifecycle_status": "CLOSED_FLAT",
        "intent_type": "SELL_TO_CLOSE",
        "broker_order_id": "32",
        "perm_id": "347094119",
        "fill_price": "4511.3",
        "fill_timestamp": "2026-05-23T06:59:00+00:00",
    }


def _open_managed_evidence() -> dict[str, object]:
    return {
        "requested_lifecycle_status": "OPEN_MANAGED",
        "entry_intent_id": "MGC|1m|2026-05-23T06:55:00Z|BUY_TO_OPEN",
        "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
        "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "side": "LONG",
        "quantity": "1",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "lifecycle_id": "bridge_fill_MGC|1m|2026-05-23T06:55:00Z|BUY_TO_OPEN",
        "broker_order_id": "10",
        "perm_id": "347094120",
        "fill_price": "4510.0",
        "fill_timestamp": "2026-05-23T06:56:00+00:00",
    }


def _write_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "control_plane_snapshot_id": "snapshot-test",
                "shared_truth_refresh_generation_id": "generation-test",
                "shared_truth_coherence_status": "COHERENT",
                "generated_at": NOW.isoformat(),
                "live_money_eligible": False,
                "duplicate_writer_count": 0,
            }
        ),
        encoding="utf-8",
    )
