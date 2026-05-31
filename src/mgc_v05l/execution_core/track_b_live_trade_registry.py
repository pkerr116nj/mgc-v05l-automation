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

from .track_b_central_trade_registry import TradeEvent, TradeEventType


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
