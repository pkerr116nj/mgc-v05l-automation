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
DEFAULT_MANAGED_HARD_EXIT_WORKING_TIMEOUT_SECONDS = 60.0
DEFAULT_MANAGED_DISCRETIONARY_EXIT_STALE_SECONDS = 900.0

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


@dataclass(frozen=True)
class ManagedExitWorkingOrderPolicy:
    classification: str
    order_age_seconds: float | None
    exit_urgency: str
    hard_exit: bool
    order_type: str | None
    action: str | None
    limit_price: float | None
    stop_price: float | None
    runtime_market_reference: float | None
    runtime_market_reference_source: str | None
    distance_from_market_points: float | None
    marketable_by_runtime_context: bool | None
    stale_by_policy: bool
    recommended_action: str
    reason: str

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "order_age_seconds": None if self.order_age_seconds is None else round(float(self.order_age_seconds), 3),
            "exit_urgency": self.exit_urgency,
            "hard_exit": self.hard_exit,
            "order_type": self.order_type,
            "action": self.action,
            "limit_price": self.limit_price,
            "stop_price": self.stop_price,
            "runtime_market_reference": self.runtime_market_reference,
            "runtime_market_reference_source": self.runtime_market_reference_source,
            "distance_from_market_points": None
            if self.distance_from_market_points is None
            else round(float(self.distance_from_market_points), 8),
            "marketable_by_runtime_context": self.marketable_by_runtime_context,
            "stale_by_policy": self.stale_by_policy,
            "recommended_action": self.recommended_action,
            "reason": self.reason,
        }


def classify_managed_exit_working_order(
    *,
    order: Mapping[str, Any],
    now: datetime,
    runtime_market_reference: float | int | None = None,
    runtime_market_reference_source: str | None = None,
    hard_exit_timeout_seconds: float = DEFAULT_MANAGED_HARD_EXIT_WORKING_TIMEOUT_SECONDS,
    discretionary_stale_seconds: float = DEFAULT_MANAGED_DISCRETIONARY_EXIT_STALE_SECONDS,
) -> ManagedExitWorkingOrderPolicy:
    """Classify an already-working managed exit order without mutating it."""

    status = str(order.get("status") or order.get("broker_order_status") or "").strip().upper()
    if status in {"FILLED", "EXECUTED"}:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_EXIT_ORDER_FILLED",
            order_age_seconds=_managed_order_age_seconds(order, now),
            exit_urgency=_managed_exit_urgency(order),
            hard_exit=_managed_exit_is_hard(order),
            order_type=_clean_text(order.get("order_type") or order.get("orderType")),
            action=_clean_text(order.get("action") or order.get("order_action")),
            limit_price=_float_or_none(order.get("limit_price") or order.get("lmtPrice") or order.get("order_limit_price")),
            stop_price=_float_or_none(order.get("stop_price") or order.get("auxPrice")),
            runtime_market_reference=_float_or_none(runtime_market_reference),
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=None,
            marketable_by_runtime_context=None,
            stale_by_policy=False,
            recommended_action="VERIFY_LIFECYCLE_CLOSE_AND_CLEAR_PENDING_ORDER",
            reason="Broker order status is filled.",
        )
    if status in {"CANCELLED", "CANCELED", "EXPIRED", "INACTIVE"}:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_EXIT_ORDER_CANCELLED_OR_EXPIRED",
            order_age_seconds=_managed_order_age_seconds(order, now),
            exit_urgency=_managed_exit_urgency(order),
            hard_exit=_managed_exit_is_hard(order),
            order_type=_clean_text(order.get("order_type") or order.get("orderType")),
            action=_clean_text(order.get("action") or order.get("order_action")),
            limit_price=_float_or_none(order.get("limit_price") or order.get("lmtPrice") or order.get("order_limit_price")),
            stop_price=_float_or_none(order.get("stop_price") or order.get("auxPrice")),
            runtime_market_reference=_float_or_none(runtime_market_reference),
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=None,
            marketable_by_runtime_context=None,
            stale_by_policy=False,
            recommended_action="CLEAR_OR_CLASSIFY_PENDING_ORDER_STATE",
            reason="Broker order status is terminal without a fill.",
        )

    action = _upper_text(order.get("action") or order.get("order_action"))
    order_type = _upper_text(order.get("order_type") or order.get("orderType"))
    limit_price = _float_or_none(order.get("limit_price") or order.get("lmtPrice") or order.get("order_limit_price"))
    stop_price = _float_or_none(order.get("stop_price") or order.get("auxPrice"))
    market_reference = _float_or_none(runtime_market_reference)
    age_seconds = _managed_order_age_seconds(order, now)
    hard_exit = _managed_exit_is_hard(order)
    exit_urgency = _managed_exit_urgency(order)
    stale_threshold = float(hard_exit_timeout_seconds if hard_exit else discretionary_stale_seconds)
    stale_by_policy = bool(age_seconds is not None and age_seconds > stale_threshold)
    marketable = _managed_exit_marketable_by_runtime_context(
        action=action,
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        runtime_market_reference=market_reference,
    )
    distance = _managed_exit_distance_from_market(action=action, limit_price=limit_price, runtime_market_reference=market_reference)

    if order_type == "LMT" and limit_price is None:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_EXIT_ORDER_STATE_GAP",
            order_age_seconds=age_seconds,
            exit_urgency=exit_urgency,
            hard_exit=hard_exit,
            order_type=order_type,
            action=action,
            limit_price=limit_price,
            stop_price=stop_price,
            runtime_market_reference=market_reference,
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=distance,
            marketable_by_runtime_context=marketable,
            stale_by_policy=stale_by_policy,
            recommended_action="PERSIST_ORDER_PRICE_OR_OPERATOR_REVIEW",
            reason="Known managed limit exit order is missing its limit price.",
        )
    if hard_exit and stale_by_policy and marketable is False:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
            order_age_seconds=age_seconds,
            exit_urgency=exit_urgency,
            hard_exit=hard_exit,
            order_type=order_type,
            action=action,
            limit_price=limit_price,
            stop_price=stop_price,
            runtime_market_reference=market_reference,
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=distance,
            marketable_by_runtime_context=marketable,
            stale_by_policy=True,
            recommended_action="PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER",
            reason="Hard/protective managed exit is stale and no longer marketable against runtime market context.",
        )
    if hard_exit and stale_by_policy:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_EXIT_ORDER_STALE_REVIEW",
            order_age_seconds=age_seconds,
            exit_urgency=exit_urgency,
            hard_exit=hard_exit,
            order_type=order_type,
            action=action,
            limit_price=limit_price,
            stop_price=stop_price,
            runtime_market_reference=market_reference,
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=distance,
            marketable_by_runtime_context=marketable,
            stale_by_policy=True,
            recommended_action="REVIEW_WORKING_HARD_EXIT_ORDER_STATUS",
            reason="Hard/protective managed exit exceeded its working timeout.",
        )
    if not hard_exit and stale_by_policy:
        return ManagedExitWorkingOrderPolicy(
            classification="KNOWN_MANAGED_EXIT_ORDER_STALE_REVIEW",
            order_age_seconds=age_seconds,
            exit_urgency=exit_urgency,
            hard_exit=False,
            order_type=order_type,
            action=action,
            limit_price=limit_price,
            stop_price=stop_price,
            runtime_market_reference=market_reference,
            runtime_market_reference_source=runtime_market_reference_source,
            distance_from_market_points=distance,
            marketable_by_runtime_context=marketable,
            stale_by_policy=True,
            recommended_action="REVIEW_DISCRETIONARY_WORKING_EXIT_ORDER",
            reason="Discretionary managed exit has worked beyond the discretionary stale threshold.",
        )
    return ManagedExitWorkingOrderPolicy(
        classification="KNOWN_MANAGED_EXIT_ORDER_WORKING_NORMAL",
        order_age_seconds=age_seconds,
        exit_urgency=exit_urgency,
        hard_exit=hard_exit,
        order_type=order_type,
        action=action,
        limit_price=limit_price,
        stop_price=stop_price,
        runtime_market_reference=market_reference,
        runtime_market_reference_source=runtime_market_reference_source,
        distance_from_market_points=distance,
        marketable_by_runtime_context=marketable,
        stale_by_policy=False,
        recommended_action="KEEP_OBSERVING_KNOWN_MANAGED_EXIT_ORDER",
        reason="Known managed exit order remains inside its working-order policy window.",
    )


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


def _upper_text(value: object) -> str | None:
    text = _clean_text(value)
    return text.upper() if text else None


def _float_or_none(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _managed_order_age_seconds(order: Mapping[str, Any], now: datetime) -> float | None:
    submitted_at = _first_time(
        order.get("submitted_at"),
        order.get("acknowledged_at"),
        order.get("created_at"),
        order.get("updated_at"),
    )
    if submitted_at is None:
        return None
    return max(0.0, (now.astimezone(UTC) - submitted_at).total_seconds())


def _managed_exit_is_hard(order: Mapping[str, Any]) -> bool:
    explicit = order.get("hard_exit")
    if isinstance(explicit, bool):
        return explicit
    text = str(explicit or "").strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    reason_text = " ".join(
        str(value or "")
        for value in (
            order.get("exit_reason"),
            order.get("reason"),
            order.get("reason_code"),
            order.get("exit_family"),
            order.get("risk_tags"),
            order.get("order_intent_id"),
        )
    ).upper()
    hard_tokens = (
        "FORCED_SESSION",
        "HARD",
        "PROTECTIVE",
        "STOP",
        "INTEGRITY_FAIL",
        "MAX_LOSS",
        "RISK_STOP",
    )
    return any(token in reason_text for token in hard_tokens)


def _managed_exit_urgency(order: Mapping[str, Any]) -> str:
    return "HARD_PROTECTIVE" if _managed_exit_is_hard(order) else "DISCRETIONARY"


def _managed_exit_marketable_by_runtime_context(
    *,
    action: str | None,
    order_type: str | None,
    limit_price: float | None,
    stop_price: float | None,
    runtime_market_reference: float | None,
) -> bool | None:
    if runtime_market_reference is None:
        return None
    normalized_action = str(action or "").strip().upper()
    normalized_order_type = str(order_type or "").strip().upper()
    if normalized_order_type == "MKT":
        return True
    if normalized_order_type == "LMT":
        if limit_price is None:
            return None
        if normalized_action == "SELL":
            return float(limit_price) <= float(runtime_market_reference)
        if normalized_action == "BUY":
            return float(limit_price) >= float(runtime_market_reference)
    if normalized_order_type in {"STP", "STOP"} and stop_price is not None:
        if normalized_action == "SELL":
            return float(runtime_market_reference) <= float(stop_price)
        if normalized_action == "BUY":
            return float(runtime_market_reference) >= float(stop_price)
    return None


def _managed_exit_distance_from_market(
    *,
    action: str | None,
    limit_price: float | None,
    runtime_market_reference: float | None,
) -> float | None:
    if limit_price is None or runtime_market_reference is None:
        return None
    normalized_action = str(action or "").strip().upper()
    if normalized_action == "SELL":
        return float(runtime_market_reference) - float(limit_price)
    if normalized_action == "BUY":
        return float(limit_price) - float(runtime_market_reference)
    return float(limit_price) - float(runtime_market_reference)


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
