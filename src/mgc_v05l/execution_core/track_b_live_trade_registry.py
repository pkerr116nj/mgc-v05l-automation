"""Append-only live Track B PAPER trade registry event writer.

This module is artifact plumbing only. It validates and appends central
``TradeEvent`` rows for the guarded PAPER lifecycle path; it does not query or
mutate broker state and it is not submit authority.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    TradeRegistryRecord,
    reduce_trade_events,
)


DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
)
DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_LATEST_EVENT_JSON = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "latest_live_trade_event.json"
)


def append_live_trade_registry_event(
    *,
    repo_root: Path,
    event: TradeEvent,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    latest_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_LATEST_EVENT_JSON,
) -> dict[str, Any]:
    """Append one validated central trade event to the live PAPER registry."""

    event_payload = event.to_dict()
    resolved_jsonl = _resolve(repo_root, jsonl_path)
    resolved_latest = _resolve(repo_root, latest_path)
    resolved_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with resolved_jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event_payload, sort_keys=True))
        handle.write("\n")
    latest_payload = {
        "schema_version": "track_b_live_trade_registry_latest_event_v1",
        "generated_at": _now().isoformat(),
        "append_only": True,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broker_mutation_allowed": False,
        "event": event_payload,
        "jsonl_path": str(resolved_jsonl),
    }
    resolved_latest.parent.mkdir(parents=True, exist_ok=True)
    resolved_latest.write_text(json.dumps(latest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "persisted": True,
        "event_type": event.event_type.value,
        "event_id": event.event_id,
        "trade_id": event.trade_id,
        "lifecycle_id": event.lifecycle_id,
        "jsonl_path": str(resolved_jsonl),
        "latest_path": str(resolved_latest),
    }


def load_live_trade_registry_record(
    *,
    repo_root: Path,
    trade_id: str,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> TradeRegistryRecord | None:
    """Load and reduce one live registry trade chain by ``trade_id``.

    This is read-only reconstruction from the append-only registry event log.
    Malformed records are ignored here and should be classified by callers as
    review-required rather than inferred into ownership authority.
    """

    requested_trade_id = str(trade_id or "").strip()
    if not requested_trade_id:
        return None
    events: list[TradeEvent] = []
    path = _resolve(repo_root, jsonl_path)
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in rows:
        if not line.strip():
            continue
        try:
            event = TradeEvent.from_dict(json.loads(line))
        except Exception:
            continue
        if event.trade_id == requested_trade_id:
            events.append(event)
    if not events:
        return None
    return reduce_trade_events(events)


def validate_registry_managed_exit_identity(
    *,
    repo_root: Path,
    trade_id: str | None,
    lifecycle_id: str | None,
    account_id: str | None,
    con_id: int | str | None,
    local_symbol: str | None,
    quantity: int | float | str | Decimal | None,
    action: str | None,
    phase1_reconciliation_gate: Mapping[str, Any],
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> dict[str, Any]:
    """Validate a managed close against the central registry owner identity.

    The registry record is the ownership source of first resort. Phase-1
    broker/lifecycle reconciliation is then used to prove the registry owner
    still matches exact live broker-backed lifecycle truth.
    """

    blockers: list[str] = []
    requested_trade_id = str(trade_id or "").strip()
    requested_lifecycle_id = str(lifecycle_id or "").strip()
    if not requested_trade_id:
        blockers.append("missing_trade_id")
    if not requested_lifecycle_id:
        blockers.append("missing_lifecycle_id")
    if not bool(phase1_reconciliation_gate.get("ready")):
        blockers.append("broker_lifecycle_reconciliation_not_clean")
    if blockers:
        return _managed_exit_validation_result(blockers=blockers)

    record = load_live_trade_registry_record(repo_root=repo_root, trade_id=requested_trade_id, jsonl_path=jsonl_path)
    if record is None:
        return _managed_exit_validation_result(blockers=["trade_registry_record_missing"])
    if record.current_state not in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE}:
        return _managed_exit_validation_result(
            blockers=["trade_registry_state_not_open_managed"],
            record=record,
        )
    if record.broker_backed_entry is not True:
        return _managed_exit_validation_result(blockers=["trade_registry_entry_not_broker_backed"], record=record)
    owner = record.ownership_identity
    if owner is None:
        return _managed_exit_validation_result(blockers=["trade_registry_owner_identity_missing"], record=record)
    if not owner.lifecycle_id:
        return _managed_exit_validation_result(blockers=["trade_registry_owner_lifecycle_id_missing"], record=record)
    if owner.lifecycle_id != requested_lifecycle_id:
        blockers.append("lifecycle_id_mismatch")

    lifecycle_row = _exact_lifecycle_row(phase1_reconciliation_gate, requested_lifecycle_id)
    if not lifecycle_row:
        blockers.append("lifecycle_identity_row_missing")
    broker_row = _matching_broker_position(lifecycle_row, phase1_reconciliation_gate) if lifecycle_row else {}

    requested_account = _valid_identity_text(account_id)
    owner_account = _valid_identity_text(owner.account_id)
    lifecycle_account = _valid_identity_text(lifecycle_row.get("account_id") if lifecycle_row else None)
    broker_account = _valid_identity_text(
        broker_row.get("account_id") or broker_row.get("account") if broker_row else None
    )
    exact_account = broker_account or lifecycle_account
    if requested_account and exact_account and requested_account != exact_account:
        blockers.append("account_id_mismatch")
    if owner_account and exact_account and owner_account != exact_account:
        blockers.append("registry_owner_account_id_mismatch")

    requested_con_id = str(con_id or "").strip()
    exact_con_id = str((lifecycle_row or {}).get("con_id") or (broker_row or {}).get("con_id") or (broker_row or {}).get("conId") or "").strip()
    if requested_con_id and exact_con_id and requested_con_id != exact_con_id:
        blockers.append("con_id_mismatch")
    if exact_con_id and str(owner.con_id) != exact_con_id:
        blockers.append("registry_owner_con_id_mismatch")

    requested_local = str(local_symbol or "").strip().upper()
    exact_local = str((lifecycle_row or {}).get("local_symbol") or (broker_row or {}).get("local_symbol") or (broker_row or {}).get("localSymbol") or "").strip().upper()
    if requested_local and exact_local and requested_local != exact_local:
        blockers.append("local_symbol_mismatch")
    if exact_local and owner.local_symbol.upper() != exact_local:
        blockers.append("registry_owner_local_symbol_mismatch")

    requested_qty = _decimal_or_none(quantity)
    exact_qty = _decimal_or_none((lifecycle_row or {}).get("quantity"))
    if requested_qty is not None and exact_qty is not None and requested_qty != exact_qty:
        blockers.append("quantity_mismatch")
    if exact_qty is not None and owner.qty != exact_qty:
        blockers.append("registry_owner_quantity_mismatch")

    expected_action = "SELL" if str(owner.side or "").upper() == "LONG" else "BUY"
    requested_action = str(action or "").strip().upper()
    if requested_action and requested_action != expected_action:
        blockers.append("close_action_mismatch")

    owner_payload = {
        "trade_id": record.trade_id,
        "current_state": record.current_state.value,
        "broker_backed_entry": record.broker_backed_entry,
        "lifecycle_id": owner.lifecycle_id,
        "lane_id": owner.lane_id,
        "strategy_id": owner.thesis_strategy_id,
        "account_id": exact_account or owner.account_id,
        "con_id": owner.con_id,
        "local_symbol": owner.local_symbol,
        "symbol": owner.symbol,
        "expiry": owner.expiry,
        "side": owner.side,
        "quantity": str(owner.qty),
        "entry_perm_id": _latest_event_value(record, "perm_id"),
        "entry_exec_id": _latest_event_value(record, "exec_id"),
    }
    return _managed_exit_validation_result(
        blockers=blockers,
        record=record,
        owner_identity=owner_payload,
        lifecycle_row=lifecycle_row,
        broker_position=broker_row,
    )


def make_live_trade_registry_event(
    *,
    event_type: TradeEventType,
    trade_id: str,
    lane_id: str,
    thesis_strategy_id: str,
    account_id: str,
    symbol: str,
    con_id: int | str | None,
    local_symbol: str,
    expiry: str,
    side: str,
    action: str,
    qty: int | float | str | Decimal | None,
    source_artifact_path: str,
    generated_at: datetime | None = None,
    lifecycle_id: str | None = None,
    order_id: str | int | None = None,
    client_id: str | int | None = None,
    perm_id: str | int | None = None,
    exec_id: str | None = None,
    price: int | float | str | Decimal | None = None,
    reason_codes: tuple[str, ...] = (),
    metadata: Mapping[str, Any] | None = None,
) -> TradeEvent:
    """Build a central trade event with strict identity validation."""

    return TradeEvent(
        event_id=f"live_{event_type.value.lower()}_{uuid.uuid4().hex}",
        event_type=event_type,
        generated_at=_ensure_utc(generated_at or _now()),
        trade_id=str(trade_id or "").strip(),
        lifecycle_id=_text_or_none(lifecycle_id),
        lane_id=str(lane_id or "").strip(),
        thesis_strategy_id=str(thesis_strategy_id or "").strip(),
        account_id=str(account_id or "").strip(),
        symbol=str(symbol or "").strip().upper(),
        con_id=int(con_id or 0),
        local_symbol=str(local_symbol or "").strip(),
        expiry=str(expiry or "").strip(),
        side=str(side or "").strip().upper(),
        action=str(action or "").strip().upper(),
        qty=_decimal(qty or 0),
        source_artifact_path=str(source_artifact_path or "").strip(),
        order_id=_text_or_none(order_id),
        client_id=_text_or_none(client_id),
        perm_id=_text_or_none(perm_id),
        exec_id=_text_or_none(exec_id),
        price=_decimal_or_none(price),
        reason_codes=tuple(str(item) for item in reason_codes if str(item or "").strip()),
        metadata=dict(metadata or {}),
    )


def trade_id_from_live_identity(
    *,
    explicit_trade_id: object = None,
    lifecycle_id: object = None,
    ownership_intent_id: object = None,
    order_intent_id: object = None,
    account_id: object = None,
    con_id: object = None,
    lane_id: object = None,
) -> str:
    """Return a stable live-path trade id from the best available identity."""

    for value in (explicit_trade_id,):
        text = str(value or "").strip()
        if text:
            return text
    lifecycle_text = str(lifecycle_id or "").strip()
    if lifecycle_text:
        return f"trade_{_slug(lifecycle_text)}"
    ownership_text = str(ownership_intent_id or "").strip()
    if ownership_text:
        return f"trade_{_slug(ownership_text)}"
    order_intent_text = str(order_intent_id or "").strip()
    if order_intent_text:
        return f"trade_{_slug(order_intent_text)}"
    identity = "|".join(
        str(value or "").strip()
        for value in (account_id, con_id, lane_id)
        if str(value or "").strip()
    )
    if identity:
        return f"trade_{_slug(identity)}"
    return ""


def broker_backed_fill_has_required_ids(*, perm_id: object = None, exec_id: object = None) -> bool:
    return bool(str(perm_id or "").strip() and str(exec_id or "").strip())


def _managed_exit_validation_result(
    *,
    blockers: list[str],
    record: TradeRegistryRecord | None = None,
    owner_identity: Mapping[str, Any] | None = None,
    lifecycle_row: Mapping[str, Any] | None = None,
    broker_position: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "classification": "REGISTRY_MANAGED_EXIT_ALLOWED" if not blockers else "REGISTRY_MANAGED_EXIT_BLOCKED",
        "allowed": not blockers,
        "block_reasons": list(dict.fromkeys(blockers)),
        "trade_id": None if record is None else record.trade_id,
        "registry_current_state": None if record is None else record.current_state.value,
        "broker_backed_entry": None if record is None else record.broker_backed_entry,
        "owner_identity": dict(owner_identity or {}),
        "lifecycle_row": dict(lifecycle_row or {}),
        "broker_position": dict(broker_position or {}),
    }


def _exact_lifecycle_row(phase1_reconciliation_gate: Mapping[str, Any], lifecycle_id: str) -> dict[str, Any]:
    for row in list(phase1_reconciliation_gate.get("track_b_lifecycle_positions") or []):
        if isinstance(row, Mapping) and str(row.get("lifecycle_id") or "").strip() == lifecycle_id:
            return dict(row)
    return {}


def _matching_broker_position(
    lifecycle_row: Mapping[str, Any],
    phase1_reconciliation_gate: Mapping[str, Any],
) -> dict[str, Any]:
    requested_con_id = str(lifecycle_row.get("con_id") or "").strip()
    requested_local = str(lifecycle_row.get("local_symbol") or "").strip().upper()
    requested_symbol = str(lifecycle_row.get("track_b_root") or lifecycle_row.get("instrument_family") or "").strip().upper()
    for row in list(phase1_reconciliation_gate.get("track_b_broker_positions") or []):
        if not isinstance(row, Mapping):
            continue
        broker_con_id = str(row.get("con_id") or row.get("conId") or "").strip()
        broker_local = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
        broker_symbol = str(row.get("track_b_root") or row.get("symbol") or "").strip().upper()
        if requested_con_id and broker_con_id and requested_con_id == broker_con_id:
            return dict(row)
        if requested_local and broker_local and requested_local == broker_local:
            return dict(row)
        if requested_symbol and broker_symbol and requested_symbol == broker_symbol and not requested_local and not requested_con_id:
            return dict(row)
    return {}


def _valid_identity_text(value: object) -> str:
    text = str(value or "").strip()
    if text.upper() in {"", "MULTIPLE", "MISSING", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def _latest_event_value(record: TradeRegistryRecord, field: str) -> str | None:
    for event in reversed(record.event_chain):
        value = getattr(event, field, None)
        if value not in (None, ""):
            return str(value)
    return None


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else Path(repo_root) / path


def _now() -> datetime:
    return datetime.now(UTC)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: object) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("trade registry quantity/price value is invalid") from exc


def _decimal_or_none(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _text_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", ".", "-"} else "_" for ch in value).strip("_")
