from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    BUDGET_ATTEMPT_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_RELEASED,
    BUDGET_ATTEMPT_RESERVED,
    BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION,
    BUDGET_EVENT_INVALID_MISSING_RESERVATION,
    BUDGET_EVENT_INVALID_MISSING_SNAPSHOT,
    BUDGET_EVENT_VALID,
    DEFAULT_ACTION_TYPE,
    DEFAULT_AGENT_ID,
    RECOVERY_BUDGET_AVAILABLE,
    RECOVERY_BUDGET_EXHAUSTED,
    TrackBRecoveryBudgetLedgerConfig,
    append_recovery_budget_event,
    build_budget_consumed_event,
    build_budget_release_event,
    build_budget_reservation_event,
    build_recovery_budget_event,
    build_track_b_recovery_budget_ledger,
    target_identity_hash,
    validate_budget_event,
    write_track_b_recovery_budget_ledger,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
TARGET = {"symbol": "MGC", "contract": "MGCM6", "action": "SELL", "quantity": 1}


def test_no_history_has_full_budget_available(tmp_path: Path) -> None:
    payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=2),
        now=NOW,
    )

    assert payload["classification"] == RECOVERY_BUDGET_AVAILABLE
    assert payload["budget_exhausted"] is False
    assert payload["entries"][0]["agent_id"] == DEFAULT_AGENT_ID
    assert payload["entries"][0]["action_type"] == DEFAULT_ACTION_TYPE
    assert payload["entries"][0]["attempts_used"] == 0
    assert payload["entries"][0]["attempts_remaining"] == 2


def test_one_failed_runtime_retry_decrements_remaining_budget(tmp_path: Path) -> None:
    event = build_recovery_budget_event(
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        stop_reason="runtime_exited_after_preflight",
        failure_classification="runtime_exited_after_preflight",
        occurred_at=NOW - timedelta(minutes=5),
    )
    payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=2),
        now=NOW,
        events=[event],
        budget_requests=[
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": TARGET,
                "failure_classification": "runtime_exited_after_preflight",
            }
        ],
    )

    entry = payload["entries"][0]
    assert payload["classification"] == RECOVERY_BUDGET_AVAILABLE
    assert entry["target_identity_hash"] == target_identity_hash(TARGET)
    assert entry["attempts_used"] == 1
    assert entry["attempts_remaining"] == 1
    assert entry["budget_exhausted"] is False


def test_repeated_same_stop_reason_exhausts_budget_and_quarantines(tmp_path: Path) -> None:
    events = [
        build_recovery_budget_event(
            action_type="RUNTIME_RETRY",
            stop_reason="runtime_exited_after_preflight",
            failure_classification="runtime_exited_after_preflight",
            occurred_at=NOW - timedelta(minutes=4),
        ),
        build_recovery_budget_event(
            action_type="RUNTIME_RETRY",
            stop_reason="runtime_exited_after_preflight",
            failure_classification="runtime_exited_after_preflight",
            occurred_at=NOW - timedelta(minutes=1),
        ),
    ]
    payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=2),
        now=NOW,
        events=events,
    )

    entry = payload["entries"][0]
    assert payload["classification"] == RECOVERY_BUDGET_EXHAUSTED
    assert payload["budget_exhausted"] is True
    assert payload["quarantine_required"] is True
    assert entry["attempts_used"] == 2
    assert entry["attempts_remaining"] == 0
    assert entry["cooldown_until"] is not None


def test_write_authority_and_append_event_log_are_accounting_only(tmp_path: Path) -> None:
    config = TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path)
    event = build_recovery_budget_event(action_type="RUNTIME_RETRY", dry_run=True, occurred_at=NOW)
    event_path = append_recovery_budget_event(config=config, event=event)
    payload = build_track_b_recovery_budget_ledger(config=config, now=NOW)
    authority_path = write_track_b_recovery_budget_ledger(config=config, payload=payload)

    assert authority_path == tmp_path / "outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json"
    assert event_path == tmp_path / "outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl"
    assert json.loads(authority_path.read_text(encoding="utf-8"))["read_only"] is True
    assert json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])["broker_mutation"] is False


def test_valid_reservation_event_schema() -> None:
    event = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW,
    )

    validation = validate_budget_event(event)

    assert event["event_type"] == BUDGET_ATTEMPT_RESERVED
    assert event["recovery_attempt_id"] == "attempt-1"
    assert event["control_plane_snapshot_id"] == "snapshot-1"
    assert event["shared_truth_generation_id"] == "generation-1"
    assert event["target_identity_hash"] == target_identity_hash(TARGET)
    assert validation["classification"] == BUDGET_EVENT_VALID
    assert validation["valid"] is True


def test_invalid_reservation_missing_snapshot_id_rejected() -> None:
    event = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        created_at=NOW,
    )

    validation = validate_budget_event(event)

    assert validation["classification"] == BUDGET_EVENT_INVALID_MISSING_SNAPSHOT
    assert validation["valid"] is False


def test_duplicate_active_reservation_rejected() -> None:
    first = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        created_at=NOW,
    )
    duplicate = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-2",
        shared_truth_generation_id="generation-2",
        action_type="RUNTIME_RETRY",
        created_at=NOW,
    )

    validation = validate_budget_event(duplicate, existing_events=[first])

    assert validation["classification"] == BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION


def test_consumed_event_without_reservation_rejected() -> None:
    consumed = build_budget_consumed_event(
        reservation_id="missing-reservation",
        recovery_attempt_id="attempt-1",
        success=False,
        failure_classification="runtime_exited_after_preflight",
        consumed_at=NOW,
    )

    validation = validate_budget_event(consumed, existing_events=[])

    assert consumed["event_type"] == BUDGET_ATTEMPT_CONSUMED_FAILURE
    assert validation["classification"] == BUDGET_EVENT_INVALID_MISSING_RESERVATION


def test_ledger_counts_active_reservation_and_release_removes_it(tmp_path: Path) -> None:
    reservation = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW - timedelta(minutes=1),
    )
    released = build_budget_release_event(
        reservation_id=reservation["reservation_id"],
        recovery_attempt_id="attempt-1",
        released_at=NOW,
    )
    active_payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=1),
        now=NOW,
        events=[reservation],
        budget_requests=[
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": TARGET,
            }
        ],
    )
    released_payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=1),
        now=NOW,
        events=[reservation, released],
        budget_requests=[
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": TARGET,
            }
        ],
    )

    assert active_payload["entries"][0]["attempts_used"] == 1
    assert active_payload["entries"][0]["budget_exhausted"] is True
    assert released["event_type"] == BUDGET_ATTEMPT_RELEASED
    assert released_payload["entries"][0]["attempts_used"] == 0
    assert released_payload["entries"][0]["budget_exhausted"] is False


def test_ledger_counts_consumed_reservation_once(tmp_path: Path) -> None:
    reservation = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW - timedelta(minutes=1),
    )
    consumed = build_budget_consumed_event(
        reservation_id=reservation["reservation_id"],
        recovery_attempt_id="attempt-1",
        success=False,
        failure_classification="runtime_exited_after_preflight",
        consumed_at=NOW,
    )

    validation = validate_budget_event(consumed, existing_events=[reservation])
    payload = build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(repo_root=tmp_path, max_attempts_per_target=2),
        now=NOW,
        events=[reservation, consumed],
        budget_requests=[
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": TARGET,
            }
        ],
    )

    assert validation["classification"] == BUDGET_EVENT_VALID
    assert payload["entries"][0]["attempts_used"] == 1
    assert payload["entries"][0]["attempts_remaining"] == 1
