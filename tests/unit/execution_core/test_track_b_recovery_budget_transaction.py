from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    BUDGET_ATTEMPT_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_CONSUMED_SUCCESS,
    BUDGET_ATTEMPT_RELEASED,
    DEFAULT_AGENT_ID,
    TrackBRecoveryBudgetLedgerConfig,
    build_budget_consumed_event,
    build_budget_release_event,
    build_budget_reservation_event,
    build_track_b_recovery_budget_ledger,
    target_identity_hash,
)
from mgc_v05l.execution_core.track_b_recovery_budget_transaction import (
    TRANSACTION_BLOCKED_BUDGET_EXHAUSTED,
    TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION,
    TRANSACTION_BLOCKED_INVALID_ORDERING,
    TRANSACTION_BLOCKED_NO_RESERVATION,
    TRANSACTION_SIMULATED_CONSUMED_FAILURE,
    TRANSACTION_SIMULATED_CONSUMED_SUCCESS,
    TRANSACTION_SIMULATED_RELEASED,
    simulate_recovery_budget_followup,
    simulate_recovery_budget_transaction,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
TARGET = {"symbol": "MGC", "contract": "MGCM6", "action": "SELL", "quantity": 1}


def test_reserve_then_consume_success_valid() -> None:
    payload = simulate_recovery_budget_transaction(
        ledger=_ledger(attempts_remaining=1),
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        followup_event_type=BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_SIMULATED_CONSUMED_SUCCESS
    assert payload["would_append"] is False
    assert payload["append_enabled"] is False
    assert payload["reservation_event_preview"]["event_type"] == "BUDGET_ATTEMPT_RESERVED"
    assert payload["followup_event_preview"]["event_type"] == BUDGET_ATTEMPT_CONSUMED_SUCCESS


def test_reserve_then_consume_failure_valid() -> None:
    payload = simulate_recovery_budget_transaction(
        ledger=_ledger(attempts_remaining=1),
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        followup_event_type=BUDGET_ATTEMPT_CONSUMED_FAILURE,
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_SIMULATED_CONSUMED_FAILURE
    assert payload["followup_event_preview"]["event_type"] == BUDGET_ATTEMPT_CONSUMED_FAILURE
    assert payload["followup_event_preview"]["event"]["success"] is False


def test_reserve_then_release_valid() -> None:
    payload = simulate_recovery_budget_transaction(
        ledger=_ledger(attempts_remaining=1),
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        followup_event_type=BUDGET_ATTEMPT_RELEASED,
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_SIMULATED_RELEASED
    assert payload["followup_event_preview"]["event_type"] == BUDGET_ATTEMPT_RELEASED


def test_consume_without_reserve_blocked() -> None:
    payload = simulate_recovery_budget_followup(
        recovery_attempt_id="attempt-1",
        reservation_id="budget-reservation-attempt-1",
        followup_event_type=BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        existing_events=[],
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_BLOCKED_NO_RESERVATION


def test_duplicate_active_reservation_blocked() -> None:
    reservation = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW,
    )

    payload = simulate_recovery_budget_transaction(
        ledger=_ledger(attempts_remaining=1),
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-2",
        shared_truth_generation_id="generation-2",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        existing_events=[reservation],
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION


def test_consume_after_release_blocked() -> None:
    reservation = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW,
    )
    release = build_budget_release_event(
        reservation_id=reservation["reservation_id"],
        recovery_attempt_id="attempt-1",
        released_at=NOW,
    )

    payload = simulate_recovery_budget_followup(
        recovery_attempt_id="attempt-1",
        reservation_id=reservation["reservation_id"],
        followup_event_type=BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        existing_events=[reservation, release],
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_BLOCKED_INVALID_ORDERING


def test_consume_after_consume_blocked() -> None:
    reservation = build_budget_reservation_event(
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        created_at=NOW,
    )
    consumed = build_budget_consumed_event(
        reservation_id=reservation["reservation_id"],
        recovery_attempt_id="attempt-1",
        success=False,
        failure_classification="runtime_exited_after_preflight",
        consumed_at=NOW,
    )

    payload = simulate_recovery_budget_followup(
        recovery_attempt_id="attempt-1",
        reservation_id=reservation["reservation_id"],
        followup_event_type=BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        existing_events=[reservation, consumed],
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_BLOCKED_INVALID_ORDERING


def test_exhausted_budget_blocks_reservation() -> None:
    payload = simulate_recovery_budget_transaction(
        ledger=_ledger(attempts_remaining=0, budget_exhausted=True),
        recovery_attempt_id="attempt-1",
        control_plane_snapshot_id="snapshot-1",
        shared_truth_generation_id="generation-1",
        action_type="RUNTIME_RETRY",
        target_identity=TARGET,
        now=NOW,
    )

    assert payload["transaction_classification"] == TRANSACTION_BLOCKED_BUDGET_EXHAUSTED
    assert payload["reservation_event_preview"]["event"] is None


def _ledger(*, attempts_remaining: int, budget_exhausted: bool = False) -> dict:
    return build_track_b_recovery_budget_ledger(
        config=TrackBRecoveryBudgetLedgerConfig(max_attempts_per_target=2),
        now=NOW,
        events=[],
        budget_requests=[
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": TARGET,
            }
        ],
    ) | {
        "classification": "RECOVERY_BUDGET_EXHAUSTED" if budget_exhausted else "RECOVERY_BUDGET_AVAILABLE",
        "budget_exhausted": budget_exhausted,
        "entries": [
            {
                "agent_id": DEFAULT_AGENT_ID,
                "action_type": "RUNTIME_RETRY",
                "target_identity": {str(key): str(value) for key, value in TARGET.items()},
                "target_identity_hash": target_identity_hash(TARGET),
                "attempts_used": 2 - attempts_remaining,
                "attempts_remaining": attempts_remaining,
                "budget_exhausted": budget_exhausted,
                "quarantine_required": budget_exhausted,
                "cooldown_until": None,
            }
        ],
    }
