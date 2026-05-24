from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_recovery_attempt_history import (
    TrackBRecoveryAttemptHistoryConfig,
    build_track_b_recovery_attempt_history,
    write_track_b_recovery_attempt_history,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_empty_history_reports_no_attempts(tmp_path: Path) -> None:
    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["latest_recovery_attempt_id"] == ""
    assert payload["latest_action_type"] == ""
    assert payload["latest_classification"] == ""
    assert payload["recent_attempts"] == []
    assert payload["active_budget_reservations"] == []
    assert payload["stale_or_incomplete_attempts"] == []
    assert payload["operator_explanation"] == "No PAPER autonomous recovery attempts have been recorded."
    assert payload["dashboard_projection_consumed"] is False


def test_one_dry_run_runtime_retry_rolls_up_self_recover_fields(tmp_path: Path) -> None:
    _seed_attempt_history(tmp_path)

    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        now=NOW,
    )
    authority_path = write_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        payload=payload,
    )
    written = json.loads(authority_path.read_text(encoding="utf-8"))

    assert payload["latest_recovery_attempt_id"] == "attempt-1"
    assert payload["latest_action_type"] == "RUNTIME_RETRY"
    assert payload["latest_classification"] == "EXECUTOR_DRY_RUN_READY"
    assert payload["recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert payload["paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert payload["recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert payload["attempts_remaining"] == 1
    assert payload["quarantine_required"] is False
    assert payload["recent_attempts"][0]["recovery_attempt_id"] == "attempt-1"
    assert written["latest_recovery_attempt_id"] == "attempt-1"


def test_budget_exhausted_quarantine_is_visible(tmp_path: Path) -> None:
    _seed_attempt_history(
        tmp_path,
        classification="EXECUTOR_BLOCKED_BUDGET_EXHAUSTED",
        recommended_recovery_action="QUARANTINE_OBSERVE_ONLY",
        paper_action_policy="QUARANTINE_OBSERVE_ONLY",
        attempts_remaining=0,
        quarantine_required=True,
    )

    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["latest_classification"] == "EXECUTOR_BLOCKED_BUDGET_EXHAUSTED"
    assert payload["recommended_recovery_action"] == "QUARANTINE_OBSERVE_ONLY"
    assert payload["attempts_remaining"] == 0
    assert payload["quarantine_required"] is True
    assert payload["operator_explanation"] == "PAPER should quarantine and observe without routine operator ack."


def test_incomplete_attempt_and_active_budget_reservation_are_visible(tmp_path: Path) -> None:
    _write_jsonl(
        tmp_path
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/paper_autonomous_recovery_executor_events.jsonl",
        [
            {
                "generated_at": NOW.isoformat(),
                "recovery_attempt_id": "attempt-incomplete",
                "action_type": "RUNTIME_RETRY",
                "classification": "",
                "control_plane_snapshot_id": "",
            }
        ],
    )
    _write_jsonl(
        tmp_path / "outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl",
        [
            {
                "event_type": "BUDGET_ATTEMPT_RESERVED",
                "reservation_id": "reservation-1",
                "recovery_attempt_id": "attempt-incomplete",
                "action_type": "RUNTIME_RETRY",
                "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
                "created_at": (NOW - timedelta(minutes=20)).isoformat(),
                "control_plane_snapshot_id": "snapshot-1",
                "shared_truth_generation_id": "generation-1",
            }
        ],
    )

    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path, incomplete_attempt_stale_seconds=300),
        now=NOW,
    )

    assert payload["active_budget_reservations"][0]["reservation_id"] == "reservation-1"
    reasons = {row["reason"] for row in payload["stale_or_incomplete_attempts"]}
    assert "executor_event_row_incomplete" in reasons
    assert "active_budget_reservation_stale_or_missing_timestamp" in reasons


def test_budget_outcomes_are_rolled_up(tmp_path: Path) -> None:
    _seed_attempt_history(tmp_path)
    _write_jsonl(
        tmp_path / "outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl",
        [
            {
                "event_type": "BUDGET_ATTEMPT_CONSUMED_SUCCESS",
                "reservation_id": "reservation-1",
                "recovery_attempt_id": "attempt-1",
                "consumed_at": "2026-05-23T12:01:00+00:00",
            },
            {
                "event_type": "BUDGET_ATTEMPT_CONSUMED_FAILURE",
                "reservation_id": "reservation-2",
                "recovery_attempt_id": "attempt-2",
                "consumed_at": "2026-05-23T12:02:00+00:00",
            },
        ],
    )

    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["last_success_at"] == "2026-05-23T12:01:00+00:00"
    assert payload["last_failure_at"] == "2026-05-23T12:02:00+00:00"


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_recovery_attempt_history.json",
        {
            "latest_recovery_attempt_id": "dashboard-only",
            "recommended_recovery_action": "DO_NOT_USE",
        },
    )

    payload = build_track_b_recovery_attempt_history(
        config=TrackBRecoveryAttemptHistoryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["latest_recovery_attempt_id"] == ""
    assert payload["recommended_recovery_action"] == ""
    assert "operator_dashboard" not in json.dumps(payload["source_artifact_paths"])
    assert payload["dashboard_projection_consumed"] is False


def _seed_attempt_history(
    root: Path,
    *,
    classification: str = "EXECUTOR_DRY_RUN_READY",
    recommended_recovery_action: str = "RUNTIME_RETRY_DRY_RUN",
    paper_action_policy: str = "AUTONOMOUS_RETRY_ELIGIBLE",
    attempts_remaining: int = 1,
    quarantine_required: bool = False,
) -> None:
    attempt = {
        "generated_at": NOW.isoformat(),
        "recovery_attempt_id": "attempt-1",
        "action_type": "RUNTIME_RETRY",
        "classification": classification,
        "control_plane_snapshot_id": "snapshot-1",
        "shared_truth_generation_id": "generation-1",
        "recommended_recovery_action": recommended_recovery_action,
        "self_recover_paper_action_policy": paper_action_policy,
        "autonomous_recovery_plan_classification": "PLAN_RUNTIME_RETRY",
        "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
        "self_recover_attempts_remaining": attempts_remaining,
        "self_recover_cooldown_until": None,
        "quarantine_required": quarantine_required,
        "operator_explanation": "PAPER should quarantine and observe without routine operator ack."
        if quarantine_required
        else "Runtime retry dry-run remains disabled and auditable.",
        "execution_enabled": False,
    }
    _write_json(
        root
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/latest_paper_autonomous_recovery_executor_attempt.json",
        attempt,
    )
    _write_jsonl(
        root
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/paper_autonomous_recovery_executor_events.jsonl",
        [attempt],
    )
    _write_json(
        root / "outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json",
        {
            "classification": "RECOVERY_BUDGET_EXHAUSTED" if quarantine_required else "RECOVERY_BUDGET_AVAILABLE",
            "quarantine_required": quarantine_required,
            "summary": {
                "minimum_attempts_remaining": attempts_remaining,
                "quarantine_required": quarantine_required,
            },
            "entries": [
                {
                    "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
                    "attempts_remaining": attempts_remaining,
                    "quarantine_required": quarantine_required,
                }
            ],
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "recommended_recovery_action": recommended_recovery_action,
            "paper_action_policy": paper_action_policy,
            "self_recover_autonomous_recovery_plan_classification": "PLAN_RUNTIME_RETRY",
            "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": attempts_remaining,
            "quarantine_required": quarantine_required,
            "operator_explanation": "snapshot explanation",
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
