"""Track B-local execution-core models.

These models deliberately avoid imports from the legacy Track A execution
stack. They are small dataclasses with explicit validation and JSON helpers so
the ledger can persist durable, replayable events.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


MILESTONE_ONE_QUANTITY = Decimal("1")
MILESTONE_ONE_ORDER_TYPE = "LMT"
MILESTONE_ONE_TIME_IN_FORCE = "DAY"


class TrackBModelError(ValueError):
    """Raised when a Track B model violates milestone-one invariants."""


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class IntentKind(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class PositionSource(str, Enum):
    BROKER = "BROKER"
    LEDGER = "LEDGER"


class ReconciliationStage(str, Enum):
    PRE_OPEN = "PRE_OPEN"
    POST_OPEN = "POST_OPEN"
    PRE_CLOSE = "PRE_CLOSE"
    POST_CLOSE = "POST_CLOSE"
    FINAL = "FINAL"


class ReconciliationStatus(str, Enum):
    CLEAN = "CLEAN"
    BLOCKED = "BLOCKED"
    AMBIGUOUS = "AMBIGUOUS"


class TerminalClassification(str, Enum):
    PASSED = "TRACK_B_PAPER_PROOF_PASSED"
    BLOCKED = "TRACK_B_PAPER_PROOF_BLOCKED"
    FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE = "TRACK_B_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED"


class RunState(str, Enum):
    CREATED = "CREATED"
    CONFIG_VALIDATED = "CONFIG_VALIDATED"
    BROKER_CONNECTED = "BROKER_CONNECTED"
    PRE_OPEN_RECONCILED = "PRE_OPEN_RECONCILED"
    OPEN_INTENT_CREATED = "OPEN_INTENT_CREATED"
    OPEN_GATE_PASSED = "OPEN_GATE_PASSED"
    OPEN_SUBMITTED = "OPEN_SUBMITTED"
    OPEN_FILLED = "OPEN_FILLED"
    POST_OPEN_RECONCILED = "POST_OPEN_RECONCILED"
    CLOSE_INTENT_CREATED = "CLOSE_INTENT_CREATED"
    CLOSE_GATE_PASSED = "CLOSE_GATE_PASSED"
    CLOSE_SUBMITTED = "CLOSE_SUBMITTED"
    CLOSE_FILLED = "CLOSE_FILLED"
    POST_CLOSE_RECONCILED = "POST_CLOSE_RECONCILED"
    FINAL_RECONCILED = "FINAL_RECONCILED"
    PROOF_PASSED = "PROOF_PASSED"
    BLOCKED = "BLOCKED"
    AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"


class SubmitAttemptState(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    BLOCKED = "BLOCKED"
    AMBIGUOUS = "AMBIGUOUS"


class BrokerOrderLifecycleStatus(str, Enum):
    HELD_OR_PRESUBMITTED = "HELD_OR_PRESUBMITTED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    AMBIGUOUS = "AMBIGUOUS"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"


def require_id(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise TrackBModelError(f"{field_name} is required.")
    return normalized


def require_aware_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TrackBModelError(f"{field_name} must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TrackBModelError(f"{field_name} must be timezone-aware.")
    return value


def normalize_decimal(value: Decimal | int | float | str, field_name: str) -> Decimal:
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TrackBModelError(f"{field_name} must be decimal-compatible.") from exc
    return normalized


def require_milestone_quantity(value: Decimal | int | float | str, field_name: str = "quantity") -> Decimal:
    normalized = normalize_decimal(value, field_name)
    if normalized != MILESTONE_ONE_QUANTITY:
        raise TrackBModelError(f"{field_name} must be exactly 1 for milestone one.")
    return normalized


def normalize_action(value: Action | str) -> Action:
    try:
        return value if isinstance(value, Action) else Action(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("action must be BUY or SELL.") from exc


def normalize_intent_kind(value: IntentKind | str) -> IntentKind:
    try:
        return value if isinstance(value, IntentKind) else IntentKind(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("intent_kind must be OPEN or CLOSE.") from exc


def normalize_position_source(value: PositionSource | str) -> PositionSource:
    try:
        return value if isinstance(value, PositionSource) else PositionSource(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("source must be BROKER or LEDGER.") from exc


def normalize_reconciliation_stage(value: ReconciliationStage | str) -> ReconciliationStage:
    try:
        return value if isinstance(value, ReconciliationStage) else ReconciliationStage(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("stage is not a valid reconciliation stage.") from exc


def normalize_reconciliation_status(value: ReconciliationStatus | str) -> ReconciliationStatus:
    try:
        return value if isinstance(value, ReconciliationStatus) else ReconciliationStatus(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("status must be CLEAN, BLOCKED, or AMBIGUOUS.") from exc


def normalize_submit_state(value: SubmitAttemptState | str) -> SubmitAttemptState:
    try:
        return value if isinstance(value, SubmitAttemptState) else SubmitAttemptState(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("state is not a valid submit attempt state.") from exc


def normalize_broker_order_lifecycle_status(value: BrokerOrderLifecycleStatus | str) -> BrokerOrderLifecycleStatus:
    try:
        return value if isinstance(value, BrokerOrderLifecycleStatus) else BrokerOrderLifecycleStatus(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("broker order lifecycle status is not valid.") from exc


def classify_broker_order_lifecycle(status: str, remaining_quantity: Decimal | int | float | str) -> BrokerOrderLifecycleStatus:
    normalized_status = str(status or "").replace("_", "").replace(" ", "").strip().upper()
    remaining = normalize_decimal(remaining_quantity, "remaining_quantity")
    if normalized_status in {"PENDINGCANCEL", "PENDCANCEL"}:
        return BrokerOrderLifecycleStatus.PENDING_CANCEL if remaining > 0 else BrokerOrderLifecycleStatus.CANCELLED
    if normalized_status in {"CANCELLED", "CANCELED"}:
        return BrokerOrderLifecycleStatus.CANCELLED
    if normalized_status == "FILLED":
        return BrokerOrderLifecycleStatus.FILLED
    if normalized_status in {"REJECTED", "INACTIVE"}:
        return BrokerOrderLifecycleStatus.REJECTED
    if normalized_status in {"PRESUBMITTED", "SUBMITTED", "PENDINGSUBMIT", "APIPENDING", "HELD"}:
        return BrokerOrderLifecycleStatus.HELD_OR_PRESUBMITTED if remaining > 0 else BrokerOrderLifecycleStatus.FILLED
    if normalized_status in {"UNKNOWN", "AMBIGUOUS", ""}:
        return BrokerOrderLifecycleStatus.AMBIGUOUS
    if remaining > 0:
        return BrokerOrderLifecycleStatus.MANUAL_REVIEW_REQUIRED
    return BrokerOrderLifecycleStatus.AMBIGUOUS


def broker_order_blocks_same_account_contract_submit(order: "BrokerOrder") -> bool:
    return classify_broker_order_lifecycle(order.status, order.remaining_quantity) in {
        BrokerOrderLifecycleStatus.HELD_OR_PRESUBMITTED,
        BrokerOrderLifecycleStatus.PENDING_CANCEL,
        BrokerOrderLifecycleStatus.AMBIGUOUS,
        BrokerOrderLifecycleStatus.MANUAL_REVIEW_REQUIRED,
    }


def ensure_lmt_day(order_type: str, time_in_force: str) -> None:
    if str(order_type or "").strip().upper() != MILESTONE_ONE_ORDER_TYPE:
        raise TrackBModelError("order_type must be LMT for milestone one.")
    if str(time_in_force or "").strip().upper() != MILESTONE_ONE_TIME_IN_FORCE:
        raise TrackBModelError("time_in_force must be DAY for milestone one.")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {key: to_jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


@dataclass(frozen=True)
class JsonSerializable:
    def to_json_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass(frozen=True)
class SignalEvent(JsonSerializable):
    signal_event_id: str
    run_id: str
    source_event_id: str
    bar_id: str
    strategy_id: str
    symbol: str
    contract_key: str
    decision: str
    side: Action | str
    quantity: Decimal | int | str
    reason: str
    occurred_at: datetime
    input_digest: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "signal_event_id", require_id(self.signal_event_id, "signal_event_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "source_event_id", require_id(self.source_event_id, "source_event_id"))
        object.__setattr__(self, "bar_id", require_id(self.bar_id, "bar_id"))
        object.__setattr__(self, "strategy_id", require_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "symbol", require_id(self.symbol, "symbol").upper())
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "decision", require_id(self.decision, "decision"))
        object.__setattr__(self, "side", normalize_action(self.side))
        object.__setattr__(self, "quantity", require_milestone_quantity(self.quantity))
        object.__setattr__(self, "reason", require_id(self.reason, "reason"))
        object.__setattr__(self, "occurred_at", require_aware_datetime(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "input_digest", require_id(self.input_digest, "input_digest"))


@dataclass(frozen=True)
class OrderIntent(JsonSerializable):
    order_intent_id: str
    signal_event_id: str
    run_id: str
    intent_kind: IntentKind | str
    account_id: str
    symbol: str
    contract_key: str
    action: Action | str
    quantity: Decimal | int | str
    order_type: str
    limit_price: Decimal | int | float | str
    time_in_force: str
    paper_only: bool
    created_at: datetime
    reason: str
    extra_fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_intent_id", require_id(self.order_intent_id, "order_intent_id"))
        object.__setattr__(self, "signal_event_id", require_id(self.signal_event_id, "signal_event_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "intent_kind", normalize_intent_kind(self.intent_kind))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "symbol", require_id(self.symbol, "symbol").upper())
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "action", normalize_action(self.action))
        object.__setattr__(self, "quantity", require_milestone_quantity(self.quantity))
        ensure_lmt_day(self.order_type, self.time_in_force)
        object.__setattr__(self, "order_type", MILESTONE_ONE_ORDER_TYPE)
        object.__setattr__(self, "limit_price", normalize_decimal(self.limit_price, "limit_price"))
        object.__setattr__(self, "time_in_force", MILESTONE_ONE_TIME_IN_FORCE)
        if not bool(self.paper_only):
            raise TrackBModelError("paper_only must be true for milestone one.")
        object.__setattr__(self, "created_at", require_aware_datetime(self.created_at, "created_at"))
        object.__setattr__(self, "reason", require_id(self.reason, "reason"))


@dataclass(frozen=True)
class SubmitAttempt(JsonSerializable):
    submit_attempt_id: str
    order_intent_id: str
    run_id: str
    account_id: str
    broker: str
    environment: dict[str, Any]
    pre_submit_reconciliation_id: str
    open_order_baseline_event_id: str
    request_digest: str
    state: SubmitAttemptState | str
    submitted_at: datetime
    broker_order_id: str | None = None
    perm_id: str | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "submit_attempt_id", require_id(self.submit_attempt_id, "submit_attempt_id"))
        object.__setattr__(self, "order_intent_id", require_id(self.order_intent_id, "order_intent_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker", require_id(self.broker, "broker"))
        object.__setattr__(
            self,
            "pre_submit_reconciliation_id",
            require_id(self.pre_submit_reconciliation_id, "pre_submit_reconciliation_id"),
        )
        object.__setattr__(
            self,
            "open_order_baseline_event_id",
            require_id(self.open_order_baseline_event_id, "open_order_baseline_event_id"),
        )
        object.__setattr__(self, "request_digest", require_id(self.request_digest, "request_digest"))
        object.__setattr__(self, "state", normalize_submit_state(self.state))
        object.__setattr__(self, "submitted_at", require_aware_datetime(self.submitted_at, "submitted_at"))


@dataclass(frozen=True)
class CancelAttempt(JsonSerializable):
    cancel_attempt_id: str
    run_id: str
    submit_attempt_id: str
    broker_order_id: str
    perm_id: str | None
    account_id: str
    contract_key: str
    requested_at: datetime
    observed_cancel_status: str | None = None
    confirmed_at: datetime | None = None
    failure_reason: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cancel_attempt_id", require_id(self.cancel_attempt_id, "cancel_attempt_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "submit_attempt_id", require_id(self.submit_attempt_id, "submit_attempt_id"))
        object.__setattr__(self, "broker_order_id", require_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "requested_at", require_aware_datetime(self.requested_at, "requested_at"))
        if self.confirmed_at is not None:
            object.__setattr__(self, "confirmed_at", require_aware_datetime(self.confirmed_at, "confirmed_at"))


@dataclass(frozen=True)
class BrokerOrder(JsonSerializable):
    broker_order_event_id: str
    run_id: str
    submit_attempt_id: str
    account_id: str
    broker_order_id: str
    perm_id: str | None
    client_id: int | None
    contract_key: str
    action: Action | str
    quantity: Decimal | int | str
    order_type: str
    limit_price: Decimal | int | float | str
    status: str
    filled_quantity: Decimal | int | str
    remaining_quantity: Decimal | int | str
    average_fill_price: Decimal | int | float | str | None
    observed_at: datetime
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker_order_event_id", require_id(self.broker_order_event_id, "broker_order_event_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "submit_attempt_id", require_id(self.submit_attempt_id, "submit_attempt_id"))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker_order_id", require_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "action", normalize_action(self.action))
        object.__setattr__(self, "quantity", require_milestone_quantity(self.quantity))
        ensure_lmt_day(self.order_type, MILESTONE_ONE_TIME_IN_FORCE)
        object.__setattr__(self, "order_type", MILESTONE_ONE_ORDER_TYPE)
        object.__setattr__(self, "limit_price", normalize_decimal(self.limit_price, "limit_price"))
        object.__setattr__(self, "status", require_id(self.status, "status"))
        object.__setattr__(self, "filled_quantity", normalize_decimal(self.filled_quantity, "filled_quantity"))
        object.__setattr__(self, "remaining_quantity", normalize_decimal(self.remaining_quantity, "remaining_quantity"))
        if self.average_fill_price is not None:
            object.__setattr__(self, "average_fill_price", normalize_decimal(self.average_fill_price, "average_fill_price"))
        object.__setattr__(self, "observed_at", require_aware_datetime(self.observed_at, "observed_at"))

    @property
    def lifecycle_status(self) -> BrokerOrderLifecycleStatus:
        return classify_broker_order_lifecycle(self.status, self.remaining_quantity)

    @property
    def blocks_same_account_contract_submit(self) -> bool:
        return broker_order_blocks_same_account_contract_submit(self)


@dataclass(frozen=True)
class FillEvent(JsonSerializable):
    fill_event_id: str
    run_id: str
    submit_attempt_id: str
    order_intent_id: str
    account_id: str
    broker_order_id: str
    perm_id: str | None
    execution_id: str
    contract_key: str
    action: Action | str
    quantity: Decimal | int | str
    price: Decimal | int | float | str
    filled_at: datetime
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "fill_event_id", require_id(self.fill_event_id, "fill_event_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "submit_attempt_id", require_id(self.submit_attempt_id, "submit_attempt_id"))
        object.__setattr__(self, "order_intent_id", require_id(self.order_intent_id, "order_intent_id"))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "broker_order_id", require_id(self.broker_order_id, "broker_order_id"))
        object.__setattr__(self, "execution_id", require_id(self.execution_id, "execution_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "action", normalize_action(self.action))
        object.__setattr__(self, "quantity", require_milestone_quantity(self.quantity))
        object.__setattr__(self, "price", normalize_decimal(self.price, "price"))
        object.__setattr__(self, "filled_at", require_aware_datetime(self.filled_at, "filled_at"))


@dataclass(frozen=True)
class PositionState(JsonSerializable):
    position_state_id: str
    run_id: str
    source: PositionSource | str
    account_id: str
    contract_key: str
    signed_quantity: int
    average_price: Decimal | int | float | str | None
    open_order_ids: tuple[str, ...]
    observed_at: datetime
    source_event_ids: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "position_state_id", require_id(self.position_state_id, "position_state_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "source", normalize_position_source(self.source))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        if abs(int(self.signed_quantity)) > 1:
            raise TrackBModelError("signed_quantity must be -1, 0, or 1 for milestone one.")
        object.__setattr__(self, "signed_quantity", int(self.signed_quantity))
        if self.average_price is not None:
            object.__setattr__(self, "average_price", normalize_decimal(self.average_price, "average_price"))
        object.__setattr__(self, "open_order_ids", tuple(str(item) for item in self.open_order_ids))
        object.__setattr__(self, "observed_at", require_aware_datetime(self.observed_at, "observed_at"))
        object.__setattr__(self, "source_event_ids", tuple(str(item) for item in self.source_event_ids))


@dataclass(frozen=True)
class ReconciliationResult(JsonSerializable):
    reconciliation_id: str
    run_id: str
    stage: ReconciliationStage | str
    status: ReconciliationStatus | str
    account_id: str
    contract_key: str
    expected_signed_quantity: int
    broker_position_state_id: str | None
    ledger_position_state_id: str | None
    broker_open_order_ids: tuple[str, ...]
    ledger_open_order_ids: tuple[str, ...]
    fill_event_ids: tuple[str, ...]
    issues: tuple[str, ...]
    required_action: str
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "reconciliation_id", require_id(self.reconciliation_id, "reconciliation_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "stage", normalize_reconciliation_stage(self.stage))
        object.__setattr__(self, "status", normalize_reconciliation_status(self.status))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "expected_signed_quantity", int(self.expected_signed_quantity))
        object.__setattr__(self, "broker_open_order_ids", tuple(str(item) for item in self.broker_open_order_ids))
        object.__setattr__(self, "ledger_open_order_ids", tuple(str(item) for item in self.ledger_open_order_ids))
        object.__setattr__(self, "fill_event_ids", tuple(str(item) for item in self.fill_event_ids))
        object.__setattr__(self, "issues", tuple(str(item) for item in self.issues))
        object.__setattr__(self, "required_action", require_id(self.required_action, "required_action"))
        object.__setattr__(self, "created_at", require_aware_datetime(self.created_at, "created_at"))

    @property
    def clean(self) -> bool:
        return self.status == ReconciliationStatus.CLEAN


@dataclass(frozen=True)
class GateDecision(JsonSerializable):
    gate_decision_id: str
    run_id: str
    passed: bool
    blocking_reason: str | None
    checks: tuple[dict[str, Any], ...]
    event_payload: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "gate_decision_id", require_id(self.gate_decision_id, "gate_decision_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "checks", tuple(dict(item) for item in self.checks))
        object.__setattr__(self, "created_at", require_aware_datetime(self.created_at, "created_at"))
        if not self.passed and not str(self.blocking_reason or "").strip():
            raise TrackBModelError("blocking_reason is required when a gate decision fails.")
