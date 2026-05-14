"""Track B PAPER exit safety state helpers.

This module is read-only plumbing. It classifies stale market/broker inputs so
entry gating can tighten before open-position exits become emergency actions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Mapping


RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME = {
    "1m": 180.0,
    "3m": 360.0,
    "5m": 600.0,
}

DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS = 180.0
DEFAULT_HARD_EXIT_MAX_NOT_FILLED_CANCELLED = 3

_EXIT_CLOSE_INTENT_TYPES = {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}
_EXIT_NOT_FILLED_CANCELLED_CLASSIFICATIONS = {
    "PAPER_CLOSE_NOT_FILLED_CANCELLED",
    "PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED",
}


class TrackBStaleDataState(str, Enum):
    FRESH = "FRESH"
    MICRO_STALE_WARNING = "MICRO_STALE_WARNING"
    DEGRADED_BLOCK_NEW_ENTRIES = "DEGRADED_BLOCK_NEW_ENTRIES"
    STALE_RESTRICT_DISCRETIONARY_EXITS = "STALE_RESTRICT_DISCRETIONARY_EXITS"
    SEVERE_STALE_EMERGENCY_REVIEW = "SEVERE_STALE_EMERGENCY_REVIEW"


@dataclass(frozen=True)
class StaleDataClassification:
    state: TrackBStaleDataState
    age_seconds: float | None
    fresh_threshold_seconds: float
    block_new_entries: bool
    suppress_discretionary_exits: bool
    emergency_review: bool
    reason: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "age_seconds": None if self.age_seconds is None else round(float(self.age_seconds), 3),
            "fresh_threshold_seconds": float(self.fresh_threshold_seconds),
            "block_new_entries": self.block_new_entries,
            "suppress_discretionary_exits": self.suppress_discretionary_exits,
            "emergency_review": self.emergency_review,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class BridgeTerminalEventGrace:
    state: TrackBStaleDataState
    applied: bool
    expired: bool
    event_age_seconds: float | None
    ttl_seconds: float
    reason: str
    event: dict[str, Any] | None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "applied": self.applied,
            "expired": self.expired,
            "event_age_seconds": None if self.event_age_seconds is None else round(float(self.event_age_seconds), 3),
            "ttl_seconds": float(self.ttl_seconds),
            "reason": self.reason,
            "event": self.event,
        }


@dataclass(frozen=True)
class ExitAttemptPolicy:
    lifecycle_id: str | None
    exit_attempt_count: int
    not_filled_cancelled_count: int
    execution_policy: str
    escalation_level: int
    hard_exit: bool
    discretionary_exit: bool
    limit_offset_ticks: float
    fill_timeout_seconds: float
    final_close_status: str | None
    block_submit: bool
    block_reason: str | None
    broker_flat: bool
    broker_lifecycle_mismatch: bool
    working_exit_order_present: bool

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "lifecycle_id": self.lifecycle_id,
            "exit_attempt_count": int(self.exit_attempt_count),
            "not_filled_cancelled_count": int(self.not_filled_cancelled_count),
            "execution_policy": self.execution_policy,
            "escalation_level": int(self.escalation_level),
            "hard_exit": self.hard_exit,
            "discretionary_exit": self.discretionary_exit,
            "limit_offset_ticks": float(self.limit_offset_ticks),
            "fill_timeout_seconds": float(self.fill_timeout_seconds),
            "final_close_status": self.final_close_status,
            "block_submit": self.block_submit,
            "block_reason": self.block_reason,
            "broker_flat": self.broker_flat,
            "broker_lifecycle_mismatch": self.broker_lifecycle_mismatch,
            "working_exit_order_present": self.working_exit_order_present,
        }


def classify_exit_attempt_policy(
    *,
    history_events: list[Mapping[str, Any]],
    lifecycle_id: str | None,
    intent_type: str | None,
    action: str | None,
    hard_exit: bool,
    discretionary_exit: bool | None = None,
    broker_position_quantity: float | int | None = None,
    broker_reconciled: bool = True,
    open_order_count: int = 0,
    max_hard_not_filled_cancelled: int = DEFAULT_HARD_EXIT_MAX_NOT_FILLED_CANCELLED,
) -> ExitAttemptPolicy:
    normalized_intent_type = str(intent_type or "").strip().upper()
    normalized_action = str(action or "").strip().upper()
    is_close = normalized_intent_type in _EXIT_CLOSE_INTENT_TYPES or normalized_action == "EXIT"
    if not is_close:
        return ExitAttemptPolicy(
            lifecycle_id=_clean_text(lifecycle_id),
            exit_attempt_count=0,
            not_filled_cancelled_count=0,
            execution_policy="NOT_AN_EXIT",
            escalation_level=0,
            hard_exit=False,
            discretionary_exit=False,
            limit_offset_ticks=1.0,
            fill_timeout_seconds=60.0,
            final_close_status=None,
            block_submit=False,
            block_reason=None,
            broker_flat=False,
            broker_lifecycle_mismatch=False,
            working_exit_order_present=False,
        )

    lifecycle_key = _clean_text(lifecycle_id)
    matching_events = [
        event
        for event in history_events
        if _event_matches_lifecycle(event, lifecycle_key) and _event_is_exit_attempt(event)
    ]
    exit_attempt_count = len(matching_events)
    not_filled_cancelled_count = sum(1 for event in matching_events if _event_is_not_filled_cancelled(event))
    final_close_status = _final_close_status(matching_events)
    broker_flat = _broker_flat_for_close(
        broker_position_quantity=broker_position_quantity,
        intent_type=normalized_intent_type,
        action=normalized_action,
    )
    working_exit_order_present = int(open_order_count or 0) > 0
    broker_lifecycle_mismatch = not bool(broker_reconciled)
    is_hard_exit = bool(hard_exit)
    is_discretionary_exit = bool(discretionary_exit) if discretionary_exit is not None else not is_hard_exit
    execution_policy, escalation_level, limit_offset_ticks, fill_timeout_seconds = _exit_execution_policy(
        not_filled_cancelled_count=not_filled_cancelled_count,
        hard_exit=is_hard_exit,
    )

    block_reason = None
    if broker_flat:
        block_reason = "broker_truth_exact_contract_flat"
    elif broker_lifecycle_mismatch:
        block_reason = "broker_lifecycle_reconciliation_not_clean"
    elif working_exit_order_present:
        block_reason = "working_exit_order_present"
    elif (
        is_hard_exit
        and int(max_hard_not_filled_cancelled) >= 0
        and not_filled_cancelled_count >= int(max_hard_not_filled_cancelled)
    ):
        block_reason = "hard_exit_repeated_not_filled_cancelled_operator_review_required"
        execution_policy = "OPERATOR_REVIEW_REQUIRED"
        escalation_level = max(escalation_level, 3)

    return ExitAttemptPolicy(
        lifecycle_id=lifecycle_key,
        exit_attempt_count=exit_attempt_count,
        not_filled_cancelled_count=not_filled_cancelled_count,
        execution_policy=execution_policy,
        escalation_level=escalation_level,
        hard_exit=is_hard_exit,
        discretionary_exit=is_discretionary_exit,
        limit_offset_ticks=limit_offset_ticks,
        fill_timeout_seconds=fill_timeout_seconds,
        final_close_status=final_close_status,
        block_submit=block_reason is not None,
        block_reason=block_reason,
        broker_flat=broker_flat,
        broker_lifecycle_mismatch=broker_lifecycle_mismatch,
        working_exit_order_present=working_exit_order_present,
    )


def classify_stale_data(
    *,
    age_seconds: float | int | None,
    fresh_threshold_seconds: float | int,
) -> StaleDataClassification:
    threshold = float(fresh_threshold_seconds)
    if threshold <= 0:
        raise ValueError("fresh_threshold_seconds must be positive.")
    if age_seconds is None:
        return StaleDataClassification(
            state=TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
            age_seconds=None,
            fresh_threshold_seconds=threshold,
            block_new_entries=True,
            suppress_discretionary_exits=True,
            emergency_review=True,
            reason="freshness age unavailable",
        )

    age = max(0.0, float(age_seconds))
    if age <= threshold:
        state = TrackBStaleDataState.FRESH
    elif age <= threshold * 1.5:
        state = TrackBStaleDataState.MICRO_STALE_WARNING
    elif age <= threshold * 2.0:
        state = TrackBStaleDataState.DEGRADED_BLOCK_NEW_ENTRIES
    elif age <= threshold * 4.0:
        state = TrackBStaleDataState.STALE_RESTRICT_DISCRETIONARY_EXITS
    else:
        state = TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW

    return StaleDataClassification(
        state=state,
        age_seconds=age,
        fresh_threshold_seconds=threshold,
        block_new_entries=state
        in {
            TrackBStaleDataState.DEGRADED_BLOCK_NEW_ENTRIES,
            TrackBStaleDataState.STALE_RESTRICT_DISCRETIONARY_EXITS,
            TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
        },
        suppress_discretionary_exits=state
        in {
            TrackBStaleDataState.STALE_RESTRICT_DISCRETIONARY_EXITS,
            TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
        },
        emergency_review=state == TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
        reason=f"age_seconds={round(age, 3)}; fresh_threshold_seconds={threshold}",
    )


def classify_market_data_freshness(
    *,
    latest_1m_age_seconds: float | int | None,
    latest_completed_5m_age_seconds: float | int | None,
    latest_1m_threshold_seconds: float | int = RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME["1m"],
    completed_5m_threshold_seconds: float | int = RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME["5m"],
) -> StaleDataClassification:
    one_minute = classify_stale_data(
        age_seconds=latest_1m_age_seconds,
        fresh_threshold_seconds=latest_1m_threshold_seconds,
    )
    five_minute = classify_stale_data(
        age_seconds=latest_completed_5m_age_seconds,
        fresh_threshold_seconds=completed_5m_threshold_seconds,
    )
    worst = _worst_classification(one_minute, five_minute)
    return StaleDataClassification(
        state=worst.state,
        age_seconds=worst.age_seconds,
        fresh_threshold_seconds=worst.fresh_threshold_seconds,
        block_new_entries=one_minute.block_new_entries or five_minute.block_new_entries,
        suppress_discretionary_exits=one_minute.suppress_discretionary_exits or five_minute.suppress_discretionary_exits,
        emergency_review=one_minute.emergency_review or five_minute.emergency_review,
        reason=(
            f"latest_1m={one_minute.state.value} ({one_minute.reason}); "
            f"completed_5m={five_minute.state.value} ({five_minute.reason})"
        ),
    )


def classify_broker_truth_freshness(
    *,
    age_seconds: float | int | None,
    max_age_seconds: float | int = 120.0,
) -> StaleDataClassification:
    return classify_stale_data(age_seconds=age_seconds, fresh_threshold_seconds=max_age_seconds)


def bridge_terminal_event_grace_state(
    *,
    event: Mapping[str, Any] | None,
    now: datetime,
    account_id: str,
    contract_key: str,
    local_symbol: str,
    con_id: int | None,
    ttl_seconds: float = DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS,
) -> BridgeTerminalEventGrace:
    if event is None:
        return BridgeTerminalEventGrace(
            state=TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
            applied=False,
            expired=False,
            event_age_seconds=None,
            ttl_seconds=float(ttl_seconds),
            reason="bridge terminal event unavailable",
            event=None,
        )
    matched = _bridge_event_matches_identity(
        event=event,
        account_id=account_id,
        contract_key=contract_key,
        local_symbol=local_symbol,
        con_id=con_id,
    )
    event_time = _first_time(
        event.get("filled_at"),
        event.get("timestamp"),
        event.get("submitted_at"),
        event.get("created_at"),
    )
    age = None if event_time is None else max(0.0, (now.astimezone(UTC) - event_time).total_seconds())
    expired = age is None or age > float(ttl_seconds)
    applied = bool(matched and not expired)
    if applied:
        reason = "exact bridge terminal event is inside broker-truth grace ttl"
        state = TrackBStaleDataState.MICRO_STALE_WARNING
    elif not matched:
        reason = "bridge terminal event identity does not match account/contract"
        state = TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW
    else:
        reason = "bridge terminal event grace ttl expired"
        state = TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW
    return BridgeTerminalEventGrace(
        state=state,
        applied=applied,
        expired=expired,
        event_age_seconds=age,
        ttl_seconds=float(ttl_seconds),
        reason=reason,
        event=dict(event),
    )


def _worst_classification(*classifications: StaleDataClassification) -> StaleDataClassification:
    order = {state: index for index, state in enumerate(TrackBStaleDataState)}
    return max(classifications, key=lambda item: order[item.state])


def _bridge_event_matches_identity(
    *,
    event: Mapping[str, Any],
    account_id: str,
    contract_key: str,
    local_symbol: str,
    con_id: int | None,
) -> bool:
    event_account = str(event.get("account_id") or account_id or "").strip()
    event_contract = str(event.get("contract_key") or contract_key or "").strip()
    event_local_symbol = str(event.get("local_symbol") or local_symbol or "").strip()
    event_con_id = _int_or_none(event.get("con_id") or event.get("conId"))
    return (
        event_account == str(account_id or "").strip()
        and event_contract == str(contract_key or "").strip()
        and event_local_symbol == str(local_symbol or "").strip()
        and (con_id is None or event_con_id is None or event_con_id == int(con_id))
    )


def _first_time(*values: object) -> datetime | None:
    for value in values:
        parsed = _parse_time(value)
        if parsed is not None:
            return parsed
    return None


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _event_matches_lifecycle(event: Mapping[str, Any], lifecycle_id: str | None) -> bool:
    if lifecycle_id is None:
        return True
    metadata = event.get("caller_metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    intent = event.get("intent")
    if not isinstance(intent, Mapping):
        intent = {}
    candidates = (
        event.get("lifecycle_id"),
        event.get("position_lifecycle_id"),
        metadata.get("lifecycle_id"),
        metadata.get("position_lifecycle_id"),
        intent.get("lifecycle_id"),
    )
    return lifecycle_id in {str(candidate or "").strip() for candidate in candidates}


def _event_is_exit_attempt(event: Mapping[str, Any]) -> bool:
    metadata = event.get("caller_metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    intent = event.get("intent")
    if not isinstance(intent, Mapping):
        intent = {}
    intent_type = str(
        event.get("intent_type")
        or metadata.get("intent_type")
        or intent.get("intent_type")
        or ""
    ).strip().upper()
    action = str(event.get("action") or intent.get("action") or "").strip().upper()
    event_type = str(event.get("event_type") or "").strip()
    delegated_classification = str(event.get("delegated_classification") or "").strip().upper()
    return (
        intent_type in _EXIT_CLOSE_INTENT_TYPES
        or action == "EXIT"
        or (
            event_type == "delegated_manual_harness_completed"
            and delegated_classification
            in _EXIT_NOT_FILLED_CANCELLED_CLASSIFICATIONS.union(
                {"PAPER_CLOSE_FILLED_FLAT", "PAPER_STRATEGY_ORDER_FILLED"}
            )
        )
    )


def _event_is_not_filled_cancelled(event: Mapping[str, Any]) -> bool:
    classification = str(
        event.get("delegated_classification")
        or event.get("classification")
        or event.get("bridge_classification")
        or ""
    ).strip().upper()
    return classification in _EXIT_NOT_FILLED_CANCELLED_CLASSIFICATIONS


def _final_close_status(events: list[Mapping[str, Any]]) -> str | None:
    for event in reversed(events):
        classification = str(
            event.get("delegated_classification")
            or event.get("classification")
            or event.get("bridge_classification")
            or ""
        ).strip().upper()
        if classification in {"PAPER_CLOSE_FILLED_FLAT", "PAPER_STRATEGY_ORDER_FILLED"}:
            return "FILLED_FLAT"
        if classification in _EXIT_NOT_FILLED_CANCELLED_CLASSIFICATIONS:
            return "NOT_FILLED_CANCELLED"
        if classification:
            return classification
    return None


def _broker_flat_for_close(
    *,
    broker_position_quantity: float | int | None,
    intent_type: str,
    action: str,
) -> bool:
    if broker_position_quantity is None:
        return False
    quantity = float(broker_position_quantity)
    if intent_type == "SELL_TO_CLOSE" or action == "SELL":
        return quantity <= 0.0
    if intent_type == "BUY_TO_CLOSE":
        return quantity >= 0.0
    return quantity == 0.0


def _exit_execution_policy(
    *,
    not_filled_cancelled_count: int,
    hard_exit: bool,
) -> tuple[str, int, float, float]:
    if not hard_exit:
        return "DISCRETIONARY_MARKETABLE_LIMIT_1T", 0, 1.0, 60.0
    if not_filled_cancelled_count <= 0:
        return "HARD_MARKETABLE_LIMIT_1T", 0, 1.0, 60.0
    if not_filled_cancelled_count == 1:
        return "HARD_AGGRESSIVE_LIMIT_4T", 1, 4.0, 120.0
    return "HARD_PROTECTIVE_LIMIT_8T", 2, 8.0, 180.0


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
