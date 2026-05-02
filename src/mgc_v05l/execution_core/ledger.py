"""Append-only JSONL ledger for Track B execution-core events."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

from .models import (
    Action,
    CancelAttempt,
    FillEvent,
    PositionSource,
    PositionState,
    JsonSerializable,
    to_jsonable,
)


class LedgerError(RuntimeError):
    """Raised when the Track B ledger cannot append or replay events."""


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    run_id: str
    sequence: int
    event_type: str
    created_at: datetime
    payload_sha256: str
    payload: dict[str, Any]
    causation_id: str | None = None
    correlation_id: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "event_type": self.event_type,
            "created_at": self.created_at.isoformat(),
            "payload_sha256": self.payload_sha256,
            "payload": to_jsonable(self.payload),
        }
        if self.causation_id is not None:
            payload["causation_id"] = self.causation_id
        if self.correlation_id is not None:
            payload["correlation_id"] = self.correlation_id
        return payload


class JsonlLedger:
    """Small append-only JSONL ledger scoped to one events file."""

    def __init__(self, events_path: Path) -> None:
        self.events_path = Path(events_path)

    def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | JsonSerializable,
        causation_id: str | None = None,
        correlation_id: str | None = None,
        event_id: str | None = None,
        created_at: datetime | None = None,
    ) -> LedgerEvent:
        normalized_payload = _normalize_payload(payload)
        now = created_at or datetime.now(timezone.utc)
        if now.tzinfo is None or now.utcoffset() is None:
            raise LedgerError("created_at must be timezone-aware.")
        event = LedgerEvent(
            event_id=event_id or f"evt_{uuid.uuid4().hex}",
            run_id=_require_text(run_id, "run_id"),
            sequence=self._next_sequence(),
            event_type=_require_text(event_type, "event_type"),
            created_at=now,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload_sha256=_payload_digest(normalized_payload),
            payload=normalized_payload,
        )
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_json_dict(), sort_keys=True))
            handle.write("\n")
        return event

    def append_model_event(
        self,
        *,
        event_type: str,
        model: JsonSerializable,
        causation_id: str | None = None,
        correlation_id: str | None = None,
        event_id: str | None = None,
        created_at: datetime | None = None,
    ) -> LedgerEvent:
        payload = model.to_json_dict()
        run_id = str(payload.get("run_id") or "").strip()
        return self.append_event(
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            causation_id=causation_id,
            correlation_id=correlation_id,
            event_id=event_id,
            created_at=created_at,
        )

    def append_cancel_attempt(
        self,
        cancel_attempt: CancelAttempt,
        *,
        causation_id: str | None = None,
        correlation_id: str | None = None,
    ) -> LedgerEvent:
        return self.append_model_event(
            event_type="cancel_attempt_created",
            model=cancel_attempt,
            causation_id=causation_id,
            correlation_id=correlation_id or cancel_attempt.submit_attempt_id,
        )

    def read_events(self, *, run_id: str | None = None) -> tuple[LedgerEvent, ...]:
        if not self.events_path.exists():
            return ()
        rows: list[LedgerEvent] = []
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                    event = _event_from_json(payload)
                except Exception as exc:  # noqa: BLE001 - preserve ledger line context.
                    raise LedgerError(f"Invalid ledger event at line {line_number}: {exc}") from exc
                if run_id is None or event.run_id == run_id:
                    rows.append(event)
        return tuple(rows)

    def replay_position(
        self,
        *,
        run_id: str,
        account_id: str,
        contract_key: str,
        observed_at: datetime | None = None,
    ) -> PositionState:
        return replay_ledger_position(
            self.read_events(run_id=run_id),
            run_id=run_id,
            account_id=account_id,
            contract_key=contract_key,
            observed_at=observed_at,
        )

    def validate_identity_uniqueness(self, *, run_id: str) -> None:
        validate_identity_uniqueness(self.read_events(run_id=run_id), run_id=run_id)

    def _next_sequence(self) -> int:
        if not self.events_path.exists():
            return 1
        with self.events_path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip()) + 1


def replay_ledger_position(
    events: Iterable[LedgerEvent],
    *,
    run_id: str,
    account_id: str,
    contract_key: str,
    observed_at: datetime | None = None,
) -> PositionState:
    signed_quantity = Decimal("0")
    average_price: Decimal | None = None
    source_event_ids: list[str] = []
    latest_at = observed_at
    seen_execution_ids: set[str] = set()

    for event in sorted(events, key=lambda item: item.sequence):
        if event.run_id != run_id or event.event_type != "fill_event_created":
            continue
        fill_payload = _extract_fill_payload(event.payload)
        if str(fill_payload.get("account_id") or "").strip() != account_id:
            continue
        if str(fill_payload.get("contract_key") or "").strip() != contract_key:
            continue
        execution_id = str(fill_payload.get("execution_id") or "").strip()
        if execution_id:
            if execution_id in seen_execution_ids:
                raise LedgerError(f"Duplicate execution_id in ledger replay: {execution_id}")
            seen_execution_ids.add(execution_id)
        action = Action(str(fill_payload.get("action") or "").strip().upper())
        quantity = Decimal(str(fill_payload.get("quantity") or "0"))
        price = Decimal(str(fill_payload.get("price") or "0"))
        signed_delta = quantity if action == Action.BUY else -quantity
        next_quantity = signed_quantity + signed_delta
        average_price = _next_average_price(
            current_quantity=signed_quantity,
            current_average=average_price,
            signed_delta=signed_delta,
            fill_price=price,
            next_quantity=next_quantity,
        )
        signed_quantity = next_quantity
        source_event_ids.append(event.event_id)
        latest_at = event.created_at

    if signed_quantity != signed_quantity.to_integral_value():
        raise LedgerError("Ledger replay produced a fractional position quantity.")
    return PositionState(
        position_state_id=f"ledger_position_{run_id}_{contract_key}",
        run_id=run_id,
        source=PositionSource.LEDGER,
        account_id=account_id,
        contract_key=contract_key,
        signed_quantity=int(signed_quantity),
        average_price=average_price,
        open_order_ids=(),
        observed_at=latest_at or datetime.now(timezone.utc),
        source_event_ids=tuple(source_event_ids),
        raw={"source": "track_b_ledger_replay"},
    )


def validate_identity_uniqueness(events: Iterable[LedgerEvent], *, run_id: str) -> None:
    """Fail closed on duplicate lifecycle or broker correlation IDs."""

    seen_order_intents: dict[str, str] = {}
    seen_submit_attempts: dict[str, str] = {}
    seen_executions: dict[str, str] = {}
    submit_by_intent: dict[str, str] = {}

    for event in sorted(events, key=lambda item: item.sequence):
        if event.run_id != run_id:
            continue
        if event.event_type == "order_intent_created":
            order_intent_id = str(event.payload.get("order_intent_id") or "").strip()
            if order_intent_id:
                _remember_unique(seen_order_intents, order_intent_id, event.event_id, "order_intent_id")
        elif event.event_type == "submit_attempt_created":
            submit_attempt_id = str(event.payload.get("submit_attempt_id") or "").strip()
            if submit_attempt_id:
                _remember_unique(seen_submit_attempts, submit_attempt_id, event.event_id, "submit_attempt_id")
            order_intent_id = str(event.payload.get("order_intent_id") or "").strip()
            if order_intent_id:
                _remember_unique(submit_by_intent, order_intent_id, event.event_id, "active submit_attempt for order_intent_id")
        elif event.event_type == "fill_event_created":
            fill_payload = _extract_fill_payload(event.payload)
            execution_id = str(fill_payload.get("execution_id") or "").strip()
            if execution_id:
                _remember_unique(seen_executions, execution_id, event.event_id, "execution_id")


def _remember_unique(seen: dict[str, str], value: str, event_id: str, field_name: str) -> None:
    previous_event_id = seen.get(value)
    if previous_event_id is not None:
        raise LedgerError(
            f"Duplicate {field_name} in ledger: {value} "
            f"({previous_event_id}, {event_id})"
        )
    seen[value] = event_id


def _next_average_price(
    *,
    current_quantity: Decimal,
    current_average: Decimal | None,
    signed_delta: Decimal,
    fill_price: Decimal,
    next_quantity: Decimal,
) -> Decimal | None:
    if next_quantity == 0:
        return None
    if current_quantity == 0:
        return fill_price
    same_direction = (current_quantity > 0 and signed_delta > 0) or (current_quantity < 0 and signed_delta < 0)
    if same_direction:
        current_abs = abs(current_quantity)
        next_abs = abs(next_quantity)
        current_cost = (current_average or Decimal("0")) * current_abs
        next_cost = current_cost + fill_price * abs(signed_delta)
        return next_cost / next_abs
    if (current_quantity > 0 and next_quantity > 0) or (current_quantity < 0 and next_quantity < 0):
        return current_average
    return fill_price


def _extract_fill_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "fill_event" in payload and isinstance(payload["fill_event"], dict):
        return dict(payload["fill_event"])
    if "fill" in payload and isinstance(payload["fill"], dict):
        return dict(payload["fill"])
    return dict(payload)


def _event_from_json(payload: dict[str, Any]) -> LedgerEvent:
    created_at = datetime.fromisoformat(str(payload["created_at"]))
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise LedgerError("ledger event created_at must be timezone-aware")
    return LedgerEvent(
        event_id=str(payload["event_id"]),
        run_id=str(payload["run_id"]),
        sequence=int(payload["sequence"]),
        event_type=str(payload["event_type"]),
        created_at=created_at,
        causation_id=payload.get("causation_id"),
        correlation_id=payload.get("correlation_id"),
        payload_sha256=str(payload["payload_sha256"]),
        payload=dict(payload.get("payload") or {}),
    )


def _normalize_payload(payload: dict[str, Any] | JsonSerializable) -> dict[str, Any]:
    if isinstance(payload, JsonSerializable):
        return payload.to_json_dict()
    normalized = to_jsonable(payload)
    if not isinstance(normalized, dict):
        raise LedgerError("ledger payload must serialize to a JSON object.")
    return normalized


def _payload_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(to_jsonable(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _require_text(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise LedgerError(f"{field_name} is required.")
    return normalized
