"""Dry-run Recovery Budget reservation transaction simulator.

The simulator validates the future reserve/release/consume/expire ordering for
PAPER autonomous recovery attempts. It previews event payloads only; it never
appends to the Recovery Budget Ledger event log and never executes recovery.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    BUDGET_ATTEMPT_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_CONSUMED_SUCCESS,
    BUDGET_ATTEMPT_EXPIRED,
    BUDGET_ATTEMPT_RELEASED,
    BUDGET_ATTEMPT_RESERVED,
    BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION,
    BUDGET_EVENT_INVALID_MISSING_RESERVATION,
    BUDGET_EVENT_VALID,
    DEFAULT_ACTION_TYPE,
    DEFAULT_AGENT_ID,
    build_budget_consumed_event,
    build_budget_expired_event,
    build_budget_release_event,
    build_budget_reservation_event,
    target_identity_hash,
    validate_budget_event,
)


TRANSACTION_DRY_RUN_READY = "TRANSACTION_DRY_RUN_READY"
TRANSACTION_BLOCKED_NO_RESERVATION = "TRANSACTION_BLOCKED_NO_RESERVATION"
TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION = "TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION"
TRANSACTION_BLOCKED_INVALID_ORDERING = "TRANSACTION_BLOCKED_INVALID_ORDERING"
TRANSACTION_BLOCKED_BUDGET_EXHAUSTED = "TRANSACTION_BLOCKED_BUDGET_EXHAUSTED"
TRANSACTION_SIMULATED_RELEASED = "TRANSACTION_SIMULATED_RELEASED"
TRANSACTION_SIMULATED_CONSUMED_SUCCESS = "TRANSACTION_SIMULATED_CONSUMED_SUCCESS"
TRANSACTION_SIMULATED_CONSUMED_FAILURE = "TRANSACTION_SIMULATED_CONSUMED_FAILURE"
TRANSACTION_SIMULATED_EXPIRED = "TRANSACTION_SIMULATED_EXPIRED"

FOLLOWUP_CLASSIFICATIONS = {
    BUDGET_ATTEMPT_CONSUMED_SUCCESS: TRANSACTION_SIMULATED_CONSUMED_SUCCESS,
    BUDGET_ATTEMPT_CONSUMED_FAILURE: TRANSACTION_SIMULATED_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_RELEASED: TRANSACTION_SIMULATED_RELEASED,
    BUDGET_ATTEMPT_EXPIRED: TRANSACTION_SIMULATED_EXPIRED,
}


def simulate_recovery_budget_transaction(
    *,
    ledger: Mapping[str, Any],
    recovery_attempt_id: str,
    control_plane_snapshot_id: str,
    shared_truth_generation_id: str,
    action_type: str = DEFAULT_ACTION_TYPE,
    budget_key: str | None = None,
    agent_id: str = DEFAULT_AGENT_ID,
    target_identity: Mapping[str, Any] | None = None,
    runtime_generation_id: str | None = None,
    followup_event_type: str | None = BUDGET_ATTEMPT_CONSUMED_SUCCESS,
    existing_events: Sequence[Mapping[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Preview reserve plus optional follow-up event without appending it."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    normalized_target = _normalize_identity(target_identity or {})
    budget_gate = _budget_gate(
        ledger=ledger,
        agent_id=agent_id,
        action_type=action_type,
        target_identity=normalized_target,
        now=actual_now,
    )
    if budget_gate["blocked"]:
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_BLOCKED_BUDGET_EXHAUSTED,
            reason=budget_gate["reason"],
            reservation_event=None,
            reservation_validation=None,
            followup_event=None,
            followup_validation=None,
        )

    reservation = build_budget_reservation_event(
        recovery_attempt_id=recovery_attempt_id,
        control_plane_snapshot_id=control_plane_snapshot_id,
        shared_truth_generation_id=shared_truth_generation_id,
        action_type=action_type,
        budget_key=budget_key,
        agent_id=agent_id,
        target_identity=normalized_target,
        runtime_generation_id=runtime_generation_id,
        created_at=actual_now,
    )
    reservation_validation = validate_budget_event(reservation, existing_events=existing_events or [])
    if reservation_validation["classification"] == BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION:
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_BLOCKED_DUPLICATE_ACTIVE_RESERVATION,
            reason=str(reservation_validation["reason"]),
            reservation_event=reservation,
            reservation_validation=reservation_validation,
            followup_event=None,
            followup_validation=None,
        )
    if reservation_validation["classification"] != BUDGET_EVENT_VALID:
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_BLOCKED_INVALID_ORDERING,
            reason=str(reservation_validation["reason"]),
            reservation_event=reservation,
            reservation_validation=reservation_validation,
            followup_event=None,
            followup_validation=None,
        )
    if not followup_event_type:
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_DRY_RUN_READY,
            reason="Reservation transaction preview is valid; no follow-up was requested.",
            reservation_event=reservation,
            reservation_validation=reservation_validation,
            followup_event=None,
            followup_validation=None,
        )

    return simulate_recovery_budget_followup(
        recovery_attempt_id=recovery_attempt_id,
        reservation_id=str(reservation["reservation_id"]),
        followup_event_type=followup_event_type,
        existing_events=[*(existing_events or []), reservation],
        reservation_event_preview=reservation,
        reservation_validation=reservation_validation,
        now=actual_now,
    )


def simulate_recovery_budget_followup(
    *,
    recovery_attempt_id: str,
    reservation_id: str,
    followup_event_type: str,
    existing_events: Sequence[Mapping[str, Any]] | None = None,
    reservation_event_preview: Mapping[str, Any] | None = None,
    reservation_validation: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Preview a release/consume/expire event against existing reservation events."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    existing = [dict(row) for row in (existing_events or [])]
    if _reservation_by_id(reservation_id, existing) is None:
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_BLOCKED_NO_RESERVATION,
            reason="Follow-up event has no known reservation.",
            reservation_event=reservation_event_preview,
            reservation_validation=reservation_validation,
            followup_event=_build_followup_event(
                followup_event_type=followup_event_type,
                reservation_id=reservation_id,
                recovery_attempt_id=recovery_attempt_id,
                now=actual_now,
            ),
            followup_validation=None,
        )
    if _reservation_terminal(reservation_id, existing):
        return _transaction_report(
            recovery_attempt_id=recovery_attempt_id,
            classification=TRANSACTION_BLOCKED_INVALID_ORDERING,
            reason="Reservation is already released, consumed, or expired.",
            reservation_event=reservation_event_preview,
            reservation_validation=reservation_validation,
            followup_event=_build_followup_event(
                followup_event_type=followup_event_type,
                reservation_id=reservation_id,
                recovery_attempt_id=recovery_attempt_id,
                now=actual_now,
            ),
            followup_validation=None,
        )

    followup = _build_followup_event(
        followup_event_type=followup_event_type,
        reservation_id=reservation_id,
        recovery_attempt_id=recovery_attempt_id,
        now=actual_now,
    )
    validation = validate_budget_event(followup, existing_events=existing)
    if validation["classification"] == BUDGET_EVENT_INVALID_MISSING_RESERVATION:
        classification = TRANSACTION_BLOCKED_NO_RESERVATION
    elif validation["classification"] != BUDGET_EVENT_VALID:
        classification = TRANSACTION_BLOCKED_INVALID_ORDERING
    else:
        classification = FOLLOWUP_CLASSIFICATIONS.get(followup_event_type, TRANSACTION_BLOCKED_INVALID_ORDERING)
    return _transaction_report(
        recovery_attempt_id=recovery_attempt_id,
        classification=classification,
        reason=str(validation["reason"]),
        reservation_event=reservation_event_preview,
        reservation_validation=reservation_validation,
        followup_event=followup,
        followup_validation=validation,
    )


def _build_followup_event(
    *,
    followup_event_type: str,
    reservation_id: str,
    recovery_attempt_id: str,
    now: datetime,
) -> dict[str, Any]:
    if followup_event_type == BUDGET_ATTEMPT_CONSUMED_SUCCESS:
        return build_budget_consumed_event(
            reservation_id=reservation_id,
            recovery_attempt_id=recovery_attempt_id,
            success=True,
            consumed_at=now,
        )
    if followup_event_type == BUDGET_ATTEMPT_CONSUMED_FAILURE:
        return build_budget_consumed_event(
            reservation_id=reservation_id,
            recovery_attempt_id=recovery_attempt_id,
            success=False,
            failure_classification="runtime_retry_failed_after_apply",
            stop_reason="runtime_retry_failed_after_apply",
            consumed_at=now,
        )
    if followup_event_type == BUDGET_ATTEMPT_RELEASED:
        return build_budget_release_event(
            reservation_id=reservation_id,
            recovery_attempt_id=recovery_attempt_id,
            released_at=now,
        )
    if followup_event_type == BUDGET_ATTEMPT_EXPIRED:
        return build_budget_expired_event(
            reservation_id=reservation_id,
            recovery_attempt_id=recovery_attempt_id,
            expired_at=now,
        )
    return {
        "event_type": followup_event_type,
        "reservation_id": reservation_id,
        "recovery_attempt_id": recovery_attempt_id,
        "generated_at": now.isoformat(),
        "occurred_at": now.isoformat(),
    }


def _transaction_report(
    *,
    recovery_attempt_id: str,
    classification: str,
    reason: str,
    reservation_event: Mapping[str, Any] | None,
    reservation_validation: Mapping[str, Any] | None,
    followup_event: Mapping[str, Any] | None,
    followup_validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_recovery_budget_transaction_simulation_v1",
        "recovery_attempt_id": recovery_attempt_id,
        "reservation_event_preview": _event_preview(reservation_event, reservation_validation),
        "followup_event_preview": _event_preview(followup_event, followup_validation),
        "transaction_classification": classification,
        "valid": classification
        in {
            TRANSACTION_DRY_RUN_READY,
            TRANSACTION_SIMULATED_RELEASED,
            TRANSACTION_SIMULATED_CONSUMED_SUCCESS,
            TRANSACTION_SIMULATED_CONSUMED_FAILURE,
            TRANSACTION_SIMULATED_EXPIRED,
        },
        "reason": reason,
        "would_append": False,
        "append_enabled": False,
        "broker_mutation": False,
        "runtime_restart_executed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _event_preview(
    event: Mapping[str, Any] | None,
    validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "event_type": None if event is None else event.get("event_type"),
        "event": None if event is None else dict(event),
        "validation": None if validation is None else dict(validation),
        "would_append": False,
        "append_enabled": False,
    }


def _budget_gate(
    *,
    ledger: Mapping[str, Any],
    agent_id: str,
    action_type: str,
    target_identity: Mapping[str, str],
    now: datetime,
) -> dict[str, Any]:
    if not ledger or not ledger.get("classification"):
        return {"blocked": True, "reason": "Recovery Budget Ledger authority artifact is missing."}
    entry = _ledger_entry_for_action(
        ledger=ledger,
        agent_id=agent_id,
        action_type=action_type,
        target_identity=target_identity,
    )
    if not entry:
        return {"blocked": True, "reason": "Recovery Budget Ledger has no matching budget entry."}
    if entry.get("quarantine_required") is True:
        return {"blocked": True, "reason": "Recovery Budget Ledger requires quarantine."}
    cooldown_until = _parse_datetime(entry.get("cooldown_until"))
    if cooldown_until is not None and cooldown_until > now:
        return {"blocked": True, "reason": "Recovery Budget Ledger cooldown is active."}
    if entry.get("budget_exhausted") is True:
        return {"blocked": True, "reason": "Recovery Budget Ledger budget is exhausted."}
    try:
        attempts_remaining = int(entry.get("attempts_remaining"))
    except (TypeError, ValueError):
        return {"blocked": True, "reason": "Recovery Budget Ledger attempts_remaining is missing or invalid."}
    if attempts_remaining <= 0:
        return {"blocked": True, "reason": "Recovery Budget Ledger has no attempts remaining."}
    return {"blocked": False, "reason": "Recovery Budget Ledger permits reservation simulation."}


def _ledger_entry_for_action(
    *,
    ledger: Mapping[str, Any],
    agent_id: str,
    action_type: str,
    target_identity: Mapping[str, str],
) -> Mapping[str, Any]:
    wanted_hash = target_identity_hash(target_identity)
    for entry in _list(ledger.get("entries")):
        row = _mapping(entry)
        if (
            row.get("agent_id") == agent_id
            and row.get("action_type") == action_type
            and row.get("target_identity_hash") == wanted_hash
        ):
            return row
    return {}


def _reservation_by_id(reservation_id: str, events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for event in events:
        if event.get("event_type") == BUDGET_ATTEMPT_RESERVED and event.get("reservation_id") == reservation_id:
            return event
    return None


def _reservation_terminal(reservation_id: str, events: Sequence[Mapping[str, Any]]) -> bool:
    terminal = False
    for event in events:
        if event.get("reservation_id") != reservation_id:
            continue
        event_type = event.get("event_type")
        if event_type == BUDGET_ATTEMPT_RESERVED:
            terminal = False
        elif event_type in {
            BUDGET_ATTEMPT_RELEASED,
            BUDGET_ATTEMPT_CONSUMED_SUCCESS,
            BUDGET_ATTEMPT_CONSUMED_FAILURE,
            BUDGET_ATTEMPT_EXPIRED,
        }:
            terminal = True
    return terminal


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in sorted(identity.items()) if value not in (None, "")}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
