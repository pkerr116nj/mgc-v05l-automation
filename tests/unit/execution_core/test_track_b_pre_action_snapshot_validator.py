from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_BLOCKED_HARD_INVARIANT,
    PRE_ACTION_BLOCKED_PLAN_MISMATCH,
    PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT,
    PRE_ACTION_BLOCKED_SNAPSHOT_MISSING,
    PRE_ACTION_BLOCKED_SNAPSHOT_STALE,
    PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH,
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
TARGET = {"symbol": "MGC", "contract": "MGCM6", "quantity": 1, "action": "SELL"}


def test_coherent_fresh_snapshot_and_matching_plan_are_valid(tmp_path: Path) -> None:
    _seed_valid(tmp_path)

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        max_snapshot_age_seconds=300,
        expected_snapshot_id="snapshot-1",
        expected_shared_truth_generation_id="generation-1",
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert result["valid"] is True
    assert result["control_plane_snapshot_id"] == "snapshot-1"
    assert result["shared_truth_refresh_generation_id"] == "generation-1"
    assert result["snapshot_coherence_status"] == "COHERENT"
    assert result["planner_action_type"] == "RUNTIME_RETRY"
    assert "operator_dashboard" not in json.dumps(result["source_artifact_paths"])


def test_missing_snapshot_blocks_even_if_dashboard_projection_exists(tmp_path: Path) -> None:
    _seed_valid(tmp_path, include_snapshot=False)
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json",
        {"control_plane_snapshot_id": "projection-only", "shared_truth_coherence_status": "COHERENT"},
    )

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_SNAPSHOT_MISSING
    assert result["valid"] is False


def test_stale_snapshot_blocks(tmp_path: Path) -> None:
    _seed_valid(tmp_path, snapshot_generated_at=datetime(2026, 5, 23, 11, 0, tzinfo=UTC))

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        max_snapshot_age_seconds=60,
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_SNAPSHOT_STALE
    assert result["snapshot_age_seconds"] == 3600.0


def test_incoherent_snapshot_blocks(tmp_path: Path) -> None:
    _seed_valid(tmp_path, coherence="STALE_OR_MIXED")

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT
    assert result["snapshot_coherence_status"] == "STALE_OR_MIXED"


def test_planner_references_different_snapshot_blocks(tmp_path: Path) -> None:
    _seed_valid(tmp_path, planner_snapshot_id="snapshot-other")

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_PLAN_MISMATCH
    assert result["reason"] == "Planner does not reference the active snapshot id."


def test_live_money_hard_invariant_blocks(tmp_path: Path) -> None:
    _seed_valid(tmp_path, live_money_eligible=True)

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        expected_action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_HARD_INVARIANT
    assert result["reason"] == "live_money_eligible=true is a hard invariant block."


def test_target_identity_mismatch_blocks(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        action_type="SCOPED_POSITION_CLEANUP",
        target_identity=TARGET,
    )

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        expected_action_type="SCOPED_POSITION_CLEANUP",
        expected_target_identity={**TARGET, "contract": "MNQM6"},
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH
    assert result["planner_target_identity"]["contract"] == "MGCM6"


def test_matching_target_identity_is_valid(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        action_type="SCOPED_POSITION_CLEANUP",
        target_identity=TARGET,
    )

    result = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        expected_action_type="SCOPED_POSITION_CLEANUP",
        expected_target_identity=TARGET,
        now=NOW,
    )

    assert result["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert result["planner_target_identity"] == {key: str(value) for key, value in TARGET.items()}


def _seed_valid(
    root: Path,
    *,
    include_snapshot: bool = True,
    snapshot_generated_at: datetime = NOW,
    coherence: str = "COHERENT",
    planner_snapshot_id: str = "snapshot-1",
    plan_classification: str = "PLAN_RUNTIME_RETRY",
    action_type: str = "RUNTIME_RETRY",
    target_identity: dict | None = None,
    live_money_eligible: bool = False,
) -> None:
    if include_snapshot:
        _write_json(
            root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
            {
                "generated_at": snapshot_generated_at.isoformat(),
                "control_plane_snapshot_id": "snapshot-1",
                "shared_truth_refresh_generation_id": "generation-1",
                "shared_truth_coherence_status": coherence,
                "runtime_supervisor_decision_id": "supervisor-1",
                "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
                "live_money_eligible": live_money_eligible,
            },
        )
    _write_json(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "supervisor_decision_id": "supervisor-1",
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": plan_classification,
            "control_plane_snapshot_id": planner_snapshot_id,
            "shared_truth_refresh_generation_id": "generation-1",
            "execution_enabled": False,
            "live_money_eligible": live_money_eligible,
            "proposed_actions": [
                {
                    "action_id": action_type.lower(),
                    "action_type": action_type,
                    "target_identity": target_identity or {},
                    "execution_enabled": False,
                }
            ],
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
