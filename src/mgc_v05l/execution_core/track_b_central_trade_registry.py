"""Pure Track B central trade registry model.

Phase 1 is model-only: append-only events, an in-memory reducer, and JSON
round-tripping. It does not read live artifacts, call a broker, or mutate
runtime/lifecycle/reconciliation state.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


class TradeRegistryModelError(ValueError):
    """Raised when a trade registry model payload is malformed."""


class TradeEventType(str, Enum):
    ENTRY_INTENT_CREATED = "ENTRY_INTENT_CREATED"
    ENTRY_ORDER_SUBMITTED = "ENTRY_ORDER_SUBMITTED"
    ENTRY_ORDER_CANCELLED = "ENTRY_ORDER_CANCELLED"
    ENTRY_FILL_BROKER_BACKED = "ENTRY_FILL_BROKER_BACKED"
    LIFECYCLE_OPEN_MANAGED = "LIFECYCLE_OPEN_MANAGED"
    EXIT_INTENT_CREATED = "EXIT_INTENT_CREATED"
    EXIT_ORDER_SUBMITTED = "EXIT_ORDER_SUBMITTED"
    EXIT_ORDER_CANCELLED = "EXIT_ORDER_CANCELLED"
    EXIT_FILL_BROKER_BACKED = "EXIT_FILL_BROKER_BACKED"
    RECONCILED_OPEN = "RECONCILED_OPEN"
    RECONCILED_FLAT = "RECONCILED_FLAT"
    RECONCILED_FLAT_HISTORICAL_CLEANUP = "RECONCILED_FLAT_HISTORICAL_CLEANUP"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    MANUAL_OPERATOR_CLOSE_RECORDED = "MANUAL_OPERATOR_CLOSE_RECORDED"
    RECOVERY_ADOPTION_RECORDED = "RECOVERY_ADOPTION_RECORDED"


class TradeCurrentState(str, Enum):
    PENDING_ENTRY = "PENDING_ENTRY"
    WORKING_ENTRY = "WORKING_ENTRY"
    OPEN_MANAGED = "OPEN_MANAGED"
    EXIT_DUE = "EXIT_DUE"
    WORKING_EXIT = "WORKING_EXIT"
    CLOSED_FLAT = "CLOSED_FLAT"
    CANCELLED = "CANCELLED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


BROKER_BACKED_EVENT_TYPES = {
    TradeEventType.ENTRY_FILL_BROKER_BACKED,
    TradeEventType.EXIT_FILL_BROKER_BACKED,
}
ORDER_EVENT_TYPES = {
    TradeEventType.ENTRY_ORDER_SUBMITTED,
    TradeEventType.ENTRY_ORDER_CANCELLED,
    TradeEventType.EXIT_ORDER_SUBMITTED,
    TradeEventType.EXIT_ORDER_CANCELLED,
}


@dataclass(frozen=True)
class TradeEvent:
    event_id: str
    event_type: TradeEventType
    generated_at: datetime
    trade_id: str
    lifecycle_id: str | None
    lane_id: str
    thesis_strategy_id: str
    account_id: str
    symbol: str
    con_id: int
    local_symbol: str
    expiry: str
    side: str
    action: str
    qty: Decimal
    source_artifact_path: str
    order_id: str | None = None
    client_id: str | None = None
    perm_id: str | None = None
    exec_id: str | None = None
    price: Decimal | None = None
    reason_codes: tuple[str, ...] = ()
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _require_id(self.event_id, "event_id")
        _require_id(self.trade_id, "trade_id")
        _require_id(self.lane_id, "lane_id")
        _require_id(self.thesis_strategy_id, "thesis_strategy_id")
        _require_id(self.account_id, "account_id")
        _require_id(self.symbol, "symbol")
        _require_id(self.local_symbol, "local_symbol")
        _require_id(self.expiry, "expiry")
        _require_id(self.side, "side")
        _require_id(self.action, "action")
        _require_id(self.source_artifact_path, "source_artifact_path")
        _ensure_datetime(self.generated_at, "generated_at")
        if int(self.con_id) <= 0:
            raise TradeRegistryModelError("con_id must be positive.")
        if self.qty <= 0:
            raise TradeRegistryModelError("qty must be positive.")

    @property
    def broker_backed(self) -> bool:
        if self.event_type in BROKER_BACKED_EVENT_TYPES:
            return bool(self.order_id and self.client_id and self.perm_id and self.exec_id)
        if self.event_type in ORDER_EVENT_TYPES:
            return bool(self.order_id and self.client_id)
        return False

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TradeEvent":
        return cls(
            event_id=str(payload.get("event_id") or ""),
            event_type=TradeEventType(str(payload.get("event_type") or "")),
            generated_at=_parse_datetime(payload.get("generated_at")),
            trade_id=str(payload.get("trade_id") or ""),
            lifecycle_id=_str_or_none(payload.get("lifecycle_id")),
            lane_id=str(payload.get("lane_id") or ""),
            thesis_strategy_id=str(payload.get("thesis_strategy_id") or ""),
            account_id=str(payload.get("account_id") or ""),
            symbol=str(payload.get("symbol") or ""),
            con_id=int(payload.get("con_id") or payload.get("conId") or 0),
            local_symbol=str(payload.get("local_symbol") or payload.get("localSymbol") or ""),
            expiry=str(payload.get("expiry") or ""),
            side=str(payload.get("side") or ""),
            action=str(payload.get("action") or ""),
            qty=_decimal(payload.get("qty") or 0),
            source_artifact_path=str(payload.get("source_artifact_path") or ""),
            order_id=_str_or_none(payload.get("order_id")),
            client_id=_str_or_none(payload.get("client_id")),
            perm_id=_str_or_none(payload.get("perm_id")),
            exec_id=_str_or_none(payload.get("exec_id")),
            price=_decimal_or_none(payload.get("price")),
            reason_codes=tuple(str(item) for item in payload.get("reason_codes") or ()),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class TradeOwnershipIdentity:
    lifecycle_id: str | None
    lane_id: str
    thesis_strategy_id: str
    account_id: str
    symbol: str
    con_id: int
    local_symbol: str
    expiry: str
    side: str
    qty: Decimal

    def conflicts_with(self, event: TradeEvent) -> tuple[str, ...]:
        conflicts: list[str] = []
        if self.lifecycle_id and event.lifecycle_id and self.lifecycle_id != event.lifecycle_id:
            conflicts.append("LIFECYCLE_ID_MISMATCH")
        if self.lane_id != event.lane_id:
            conflicts.append("LANE_ID_MISMATCH")
        if self.thesis_strategy_id != event.thesis_strategy_id:
            conflicts.append("THESIS_STRATEGY_ID_MISMATCH")
        if self.account_id != event.account_id:
            conflicts.append("ACCOUNT_ID_MISMATCH")
        if self.con_id != event.con_id:
            conflicts.append("CON_ID_MISMATCH")
        if self.local_symbol != event.local_symbol:
            conflicts.append("LOCAL_SYMBOL_MISMATCH")
        if self.qty != event.qty:
            conflicts.append("QTY_MISMATCH")
        return tuple(conflicts)


@dataclass(frozen=True)
class TradeRegistryRecord:
    trade_id: str
    current_state: TradeCurrentState
    broker_backed_entry: bool
    broker_backed_exit: bool
    open_qty: Decimal
    entry_price: Decimal | None
    exit_price: Decimal | None
    ownership_identity: TradeOwnershipIdentity | None
    latest_reason_codes: tuple[str, ...]
    event_chain: tuple[TradeEvent, ...]
    manual_operator_close_recorded: bool = False
    recovery_adoption_recorded: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = _jsonable(asdict(self))
        payload["event_chain"] = [event.to_dict() for event in self.event_chain]
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TradeRegistryRecord":
        ownership = payload.get("ownership_identity")
        return cls(
            trade_id=str(payload.get("trade_id") or ""),
            current_state=TradeCurrentState(str(payload.get("current_state") or "")),
            broker_backed_entry=payload.get("broker_backed_entry") is True,
            broker_backed_exit=payload.get("broker_backed_exit") is True,
            open_qty=_decimal(payload.get("open_qty") or 0),
            entry_price=_decimal_or_none(payload.get("entry_price")),
            exit_price=_decimal_or_none(payload.get("exit_price")),
            ownership_identity=_ownership_from_dict(ownership) if isinstance(ownership, Mapping) else None,
            latest_reason_codes=tuple(str(item) for item in payload.get("latest_reason_codes") or ()),
            event_chain=tuple(TradeEvent.from_dict(item) for item in payload.get("event_chain") or ()),
            manual_operator_close_recorded=payload.get("manual_operator_close_recorded") is True,
            recovery_adoption_recorded=payload.get("recovery_adoption_recorded") is True,
        )


def generate_trade_id(
    *,
    account_id: str,
    con_id: int,
    lane_id: str,
    entry_action: str,
    generated_at: datetime,
    entry_perm_id: str | None = None,
    order_id: str | None = None,
) -> str:
    identity = entry_perm_id or order_id
    if not identity:
        raise TradeRegistryModelError("entry_perm_id or order_id is required to generate trade_id.")
    _ensure_datetime(generated_at, "generated_at")
    raw = f"trade_{account_id}|{con_id}|{lane_id}|{entry_action}|{identity}|{generated_at.isoformat()}"
    return _slug(raw)


def reduce_trade_events(events: Sequence[TradeEvent]) -> TradeRegistryRecord:
    if not events:
        raise TradeRegistryModelError("at least one event is required.")
    ordered = tuple(sorted(events, key=lambda event: (event.generated_at, event.event_id)))
    trade_ids = {event.trade_id for event in ordered}
    if len(trade_ids) != 1:
        raise TradeRegistryModelError("all events must share one trade_id.")

    state = TradeCurrentState.PENDING_ENTRY
    broker_backed_entry = False
    broker_backed_exit = False
    open_qty = Decimal("0")
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    ownership: TradeOwnershipIdentity | None = None
    reason_codes: list[str] = []
    manual_operator_close_recorded = False
    recovery_adoption_recorded = False

    for event in ordered:
        reason_codes.extend(event.reason_codes)
        event_conflicts = _event_identity_conflicts(ownership, event)
        if event_conflicts:
            reason_codes.extend(event_conflicts)
            state = TradeCurrentState.REVIEW_REQUIRED
            continue
        malformed = _broker_backed_malformed_reason(event)
        if malformed:
            reason_codes.append(malformed)
            state = TradeCurrentState.REVIEW_REQUIRED
            continue

        if event.event_type == TradeEventType.ENTRY_INTENT_CREATED:
            state = TradeCurrentState.PENDING_ENTRY if state != TradeCurrentState.REVIEW_REQUIRED else state
            ownership = ownership or _identity_from_event(event)
        elif event.event_type == TradeEventType.ENTRY_ORDER_SUBMITTED:
            state = TradeCurrentState.WORKING_ENTRY
            ownership = ownership or _identity_from_event(event)
        elif event.event_type == TradeEventType.ENTRY_ORDER_CANCELLED:
            if not broker_backed_entry:
                state = TradeCurrentState.CANCELLED
                open_qty = Decimal("0")
        elif event.event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED:
            broker_backed_entry = True
            open_qty = event.qty
            entry_price = event.price
            state = TradeCurrentState.OPEN_MANAGED
            ownership = _identity_from_event(event)
        elif event.event_type == TradeEventType.RECOVERY_ADOPTION_RECORDED:
            recovery_adoption_recorded = True
            if _has_fill_broker_ids(event):
                broker_backed_entry = True
            open_qty = event.qty
            state = TradeCurrentState.OPEN_MANAGED
            ownership = _identity_from_event(event)
        elif event.event_type == TradeEventType.LIFECYCLE_OPEN_MANAGED:
            state = TradeCurrentState.OPEN_MANAGED
            ownership = _identity_from_event(event, lifecycle_required=True)
            open_qty = event.qty if open_qty == 0 else open_qty
        elif event.event_type == TradeEventType.RECONCILED_OPEN:
            state = TradeCurrentState.OPEN_MANAGED if state != TradeCurrentState.REVIEW_REQUIRED else state
        elif event.event_type == TradeEventType.EXIT_INTENT_CREATED:
            state = TradeCurrentState.EXIT_DUE
        elif event.event_type == TradeEventType.EXIT_ORDER_SUBMITTED:
            state = TradeCurrentState.WORKING_EXIT
        elif event.event_type == TradeEventType.EXIT_ORDER_CANCELLED:
            state = TradeCurrentState.EXIT_DUE if open_qty > 0 else state
        elif event.event_type == TradeEventType.EXIT_FILL_BROKER_BACKED:
            broker_backed_exit = True
            exit_price = event.price
            open_qty = max(Decimal("0"), open_qty - event.qty)
            state = TradeCurrentState.CLOSED_FLAT if open_qty == 0 else TradeCurrentState.REVIEW_REQUIRED
            if open_qty != 0:
                reason_codes.append("EXIT_FILL_QTY_DID_NOT_FLATTEN")
        elif event.event_type == TradeEventType.MANUAL_OPERATOR_CLOSE_RECORDED:
            manual_operator_close_recorded = True
            exit_price = event.price or exit_price
            open_qty = Decimal("0")
            state = TradeCurrentState.CLOSED_FLAT
            reason_codes.append("MANUAL_OPERATOR_CLOSE_RECORDED")
        elif event.event_type == TradeEventType.RECONCILED_FLAT:
            open_qty = Decimal("0")
            state = TradeCurrentState.CLOSED_FLAT if state != TradeCurrentState.REVIEW_REQUIRED else state
        elif event.event_type == TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP:
            open_qty = Decimal("0")
            state = TradeCurrentState.CLOSED_FLAT
            reason_codes.append("RECONCILED_FLAT_HISTORICAL_CLEANUP")
        elif event.event_type == TradeEventType.REVIEW_REQUIRED:
            state = TradeCurrentState.REVIEW_REQUIRED
            reason_codes.append("REVIEW_REQUIRED_EVENT")

    return TradeRegistryRecord(
        trade_id=ordered[0].trade_id,
        current_state=state,
        broker_backed_entry=broker_backed_entry,
        broker_backed_exit=broker_backed_exit,
        open_qty=open_qty,
        entry_price=entry_price,
        exit_price=exit_price,
        ownership_identity=ownership,
        latest_reason_codes=tuple(dict.fromkeys(reason_codes)),
        event_chain=ordered,
        manual_operator_close_recorded=manual_operator_close_recorded,
        recovery_adoption_recorded=recovery_adoption_recorded,
    )


def records_to_json(records: Sequence[TradeRegistryRecord]) -> str:
    return json.dumps([record.to_dict() for record in records], indent=2, sort_keys=True)


def records_from_json(payload: str) -> tuple[TradeRegistryRecord, ...]:
    loaded = json.loads(payload)
    if not isinstance(loaded, list):
        raise TradeRegistryModelError("registry JSON must contain a list of records.")
    return tuple(TradeRegistryRecord.from_dict(item) for item in loaded)


def events_to_json(events: Sequence[TradeEvent]) -> str:
    return json.dumps([event.to_dict() for event in events], indent=2, sort_keys=True)


def events_from_json(payload: str) -> tuple[TradeEvent, ...]:
    loaded = json.loads(payload)
    if not isinstance(loaded, list):
        raise TradeRegistryModelError("event JSON must contain a list of events.")
    return tuple(TradeEvent.from_dict(item) for item in loaded)


def write_registry_records(path: Path, records: Sequence[TradeRegistryRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(records_to_json(records) + "\n", encoding="utf-8")


def read_registry_records(path: Path) -> tuple[TradeRegistryRecord, ...]:
    return records_from_json(path.read_text(encoding="utf-8"))


def _identity_from_event(event: TradeEvent, *, lifecycle_required: bool = False) -> TradeOwnershipIdentity:
    if lifecycle_required and not event.lifecycle_id:
        raise TradeRegistryModelError("lifecycle_id is required for lifecycle ownership.")
    return TradeOwnershipIdentity(
        lifecycle_id=event.lifecycle_id,
        lane_id=event.lane_id,
        thesis_strategy_id=event.thesis_strategy_id,
        account_id=event.account_id,
        symbol=event.symbol,
        con_id=event.con_id,
        local_symbol=event.local_symbol,
        expiry=event.expiry,
        side=event.side,
        qty=event.qty,
    )


def _event_identity_conflicts(ownership: TradeOwnershipIdentity | None, event: TradeEvent) -> tuple[str, ...]:
    if ownership is None:
        return ()
    if event.event_type == TradeEventType.ENTRY_INTENT_CREATED:
        return ()
    return ownership.conflicts_with(event)


def _broker_backed_malformed_reason(event: TradeEvent) -> str | None:
    if event.event_type in BROKER_BACKED_EVENT_TYPES and not event.broker_backed:
        return "BROKER_BACKED_EVENT_MISSING_BROKER_IDS"
    if event.event_type in ORDER_EVENT_TYPES and event.event_type not in {
        TradeEventType.ENTRY_ORDER_CANCELLED,
        TradeEventType.EXIT_ORDER_CANCELLED,
    } and not event.broker_backed:
        return "ORDER_EVENT_MISSING_ORDER_IDS"
    return None


def _has_fill_broker_ids(event: TradeEvent) -> bool:
    return bool(event.order_id and event.client_id and event.perm_id and event.exec_id)


def _ownership_from_dict(payload: Mapping[str, Any]) -> TradeOwnershipIdentity:
    return TradeOwnershipIdentity(
        lifecycle_id=_str_or_none(payload.get("lifecycle_id")),
        lane_id=str(payload.get("lane_id") or ""),
        thesis_strategy_id=str(payload.get("thesis_strategy_id") or ""),
        account_id=str(payload.get("account_id") or ""),
        symbol=str(payload.get("symbol") or ""),
        con_id=int(payload.get("con_id") or payload.get("conId") or 0),
        local_symbol=str(payload.get("local_symbol") or payload.get("localSymbol") or ""),
        expiry=str(payload.get("expiry") or ""),
        side=str(payload.get("side") or ""),
        qty=_decimal(payload.get("qty") or 0),
    )


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def _require_id(value: str | None, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise TradeRegistryModelError(f"{field_name} is required.")
    return normalized


def _ensure_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TradeRegistryModelError(f"{field_name} must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TradeRegistryModelError(f"{field_name} must be timezone-aware.")
    return value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime:
    if not value:
        raise TradeRegistryModelError("datetime value is required.")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise TradeRegistryModelError("datetime value is invalid.") from exc
    return _ensure_datetime(parsed, "datetime")


def _decimal(value: Any) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TradeRegistryModelError("decimal value is invalid.") from exc


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _str_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(inner) for inner in value]
    return value
