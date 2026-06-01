"""Durable Track B PAPER submit-intent ownership artifacts.

This module is intentionally pure artifact plumbing. It does not import broker
adapters, does not connect to brokers, and does not submit/cancel orders.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_ROOT = (
    Path("outputs") / "track_b_execution_core" / "submit_intent_ownership"
)
DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL = (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_ROOT / "track_b_submit_intent_ownership.jsonl"
)
DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON = (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_ROOT / "latest_track_b_submit_intent_ownership.json"
)
SCHEMA_VERSION = "track_b_submit_intent_ownership_v1"
EXPECTED_PAPER_ACCOUNT_ID = "DUM882026"
UNRESOLVED_STATES = {
    "PRE_SUBMIT_INTENT_DURABLE",
    "SUBMIT_DELEGATED",
    "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED",
    "BROKER_ORDER_WORKING",
    "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED",
    "REVIEW_REQUIRED",
}
DIGEST_FIELDS = (
    "schema_version",
    "state",
    "mode",
    "account_id",
    "lane_id",
    "strategy_id",
    "lifecycle_id",
    "intent_type",
    "action",
    "symbol",
    "local_symbol",
    "expiry",
    "con_id",
    "qty",
    "order_type",
    "limit_price",
    "time_in_force",
    "repo_root",
    "git_head",
    "authorization_digest",
    "live_money_eligible",
    "paper_proof_invoked",
)


class SubmitIntentOwnershipError(ValueError):
    """Raised when a submit-intent ownership artifact is not PAPER-safe."""


class SubmitIntentOwnershipState(str, Enum):
    PRE_SUBMIT_INTENT_DURABLE = "PRE_SUBMIT_INTENT_DURABLE"
    SUBMIT_DELEGATED = "SUBMIT_DELEGATED"
    BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED = "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED"
    BROKER_ORDER_WORKING = "BROKER_ORDER_WORKING"
    BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED = "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED"
    LIFECYCLE_OPEN_PERSISTED = "LIFECYCLE_OPEN_PERSISTED"
    NO_BROKER_EFFECT_CONFIRMED = "NO_BROKER_EFFECT_CONFIRMED"
    NOT_FILLED_CANCELLED = "NOT_FILLED_CANCELLED"
    HISTORICAL_FLAT_RESOLVED = "HISTORICAL_FLAT_RESOLVED"
    REJECTED = "REJECTED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


@dataclass(frozen=True)
class SubmitIntentOwnershipRecord:
    mode: str
    account_id: str
    lane_id: str
    strategy_id: str
    intent_type: str
    action: str
    symbol: str
    local_symbol: str
    expiry: str
    con_id: int | str
    qty: int | float | str
    order_type: str
    limit_price: int | float | str
    time_in_force: str
    repo_root: str
    git_head: str
    created_at: datetime
    state: SubmitIntentOwnershipState | str = SubmitIntentOwnershipState.PRE_SUBMIT_INTENT_DURABLE
    ownership_intent_id: str | None = None
    lifecycle_id: str | None = None
    lifecycle_id_reserved_only: bool = True
    lifecycle_position_open: bool = False
    caller_path: str | None = None
    caller_type: str | None = None
    authorization_path: str | None = None
    authorization_digest: str | None = None
    runtime_pid: int | None = None
    runtime_cwd: str | None = None
    execution_price_source: str | None = None
    runtime_reference_price: str | None = None
    pre_submit_reconciliation_classification: str | None = None
    governance_classification: str | None = None
    exposure_classification: str | None = None
    open_order_count: int = 0
    unknown_open_order_count: int = 0
    review_required_count: int = 0
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False
    broker_order_id: str | None = None
    client_id: int | None = None
    perm_id: int | None = None
    exec_id: str | None = None
    source_artifact_paths: tuple[str, ...] = ()
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        created_at = require_aware_datetime(self.created_at, "created_at").astimezone(UTC)
        state = _state_value(self.state)
        lifecycle_id = self.lifecycle_id or reserve_lifecycle_id(
            lane_id=self.lane_id,
            intent_type=self.intent_type,
            created_at=created_at,
        )
        ownership_intent_id = self.ownership_intent_id or generate_ownership_intent_id(
            lane_id=self.lane_id,
            intent_type=self.intent_type,
            action=self.action,
            symbol=self.symbol,
            local_symbol=self.local_symbol,
            created_at=created_at,
        )
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "ownership_intent_id": ownership_intent_id,
            "state": state,
            "created_at": created_at.isoformat(),
            "updated_at": created_at.isoformat(),
            "mode": str(self.mode or "").strip().upper(),
            "account_id": str(self.account_id or "").strip(),
            "lane_id": str(self.lane_id or "").strip(),
            "strategy_id": str(self.strategy_id or "").strip(),
            "lifecycle_id": lifecycle_id,
            "lifecycle_id_reserved_only": bool(self.lifecycle_id_reserved_only),
            "lifecycle_position_open": bool(self.lifecycle_position_open),
            "intent_type": str(self.intent_type or "").strip().upper(),
            "action": str(self.action or "").strip().upper(),
            "symbol": str(self.symbol or "").strip().upper(),
            "local_symbol": str(self.local_symbol or "").strip(),
            "expiry": str(self.expiry or "").strip(),
            "con_id": _int_or_text(self.con_id),
            "qty": _quantity_value(self.qty),
            "order_type": str(self.order_type or "").strip().upper(),
            "limit_price": str(self.limit_price),
            "time_in_force": str(self.time_in_force or "").strip().upper(),
            "repo_root": str(self.repo_root or "").strip(),
            "git_head": str(self.git_head or "").strip(),
            "caller_path": self.caller_path,
            "caller_type": self.caller_type,
            "authorization_path": self.authorization_path,
            "authorization_digest": self.authorization_digest,
            "runtime_pid": self.runtime_pid,
            "runtime_cwd": self.runtime_cwd,
            "execution_price_source": self.execution_price_source,
            "runtime_reference_price": self.runtime_reference_price,
            "pre_submit_reconciliation_classification": self.pre_submit_reconciliation_classification,
            "governance_classification": self.governance_classification,
            "exposure_classification": self.exposure_classification,
            "open_order_count": int(self.open_order_count),
            "unknown_open_order_count": int(self.unknown_open_order_count),
            "review_required_count": int(self.review_required_count),
            "live_money_eligible": bool(self.live_money_eligible),
            "paper_proof_invoked": bool(self.paper_proof_invoked),
            "broker_order_id": self.broker_order_id,
            "client_id": self.client_id,
            "perm_id": self.perm_id,
            "exec_id": self.exec_id,
            "source_artifact_paths": list(self.source_artifact_paths),
            "extra": dict(self.extra or {}),
        }
        validate_submit_intent_ownership_payload(payload)
        payload["digest"] = submit_intent_ownership_digest(payload)
        return payload


@dataclass(frozen=True)
class SubmitIntentOwnershipStoreResult:
    jsonl_path: Path
    latest_path: Path
    record: dict[str, Any]
    latest_view: dict[str, Any]


def generate_ownership_intent_id(
    *,
    lane_id: str,
    intent_type: str,
    action: str,
    symbol: str,
    local_symbol: str,
    created_at: datetime,
) -> str:
    timestamp = require_aware_datetime(created_at, "created_at").astimezone(UTC).isoformat()
    seed = json.dumps(
        {
            "lane_id": str(lane_id or "").strip(),
            "intent_type": str(intent_type or "").strip().upper(),
            "action": str(action or "").strip().upper(),
            "symbol": str(symbol or "").strip().upper(),
            "local_symbol": str(local_symbol or "").strip(),
            "created_at": timestamp,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"submit_owner_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:24]}"


def reserve_lifecycle_id(*, lane_id: str, intent_type: str, created_at: datetime) -> str:
    timestamp = require_aware_datetime(created_at, "created_at").astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    suffix = uuid.uuid5(uuid.NAMESPACE_URL, f"{lane_id}:{intent_type}:{timestamp}").hex[:12]
    return f"reserved_submit_{_slug(lane_id)}_{timestamp}_{suffix}"


def submit_intent_ownership_digest(payload: Mapping[str, Any]) -> str:
    digest_payload = {field: payload.get(field) for field in DIGEST_FIELDS}
    encoded = json.dumps(digest_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_submit_intent_ownership_payload(payload: Mapping[str, Any]) -> None:
    required = (
        "schema_version",
        "ownership_intent_id",
        "state",
        "mode",
        "account_id",
        "lane_id",
        "strategy_id",
        "lifecycle_id",
        "intent_type",
        "action",
        "symbol",
        "local_symbol",
        "expiry",
        "con_id",
        "qty",
        "order_type",
        "limit_price",
        "time_in_force",
        "repo_root",
        "git_head",
    )
    missing = [field for field in required if payload.get(field) in {None, ""}]
    if missing:
        raise SubmitIntentOwnershipError(f"submit-intent ownership missing required fields: {', '.join(missing)}")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise SubmitIntentOwnershipError("submit-intent ownership schema_version is not supported")
    if str(payload.get("mode") or "").upper() != "PAPER":
        raise SubmitIntentOwnershipError("submit-intent ownership requires mode=PAPER")
    if str(payload.get("account_id") or "") != EXPECTED_PAPER_ACCOUNT_ID:
        raise SubmitIntentOwnershipError("submit-intent ownership requires PAPER account DUM882026")
    if payload.get("live_money_eligible") is not False:
        raise SubmitIntentOwnershipError("submit-intent ownership requires live_money_eligible=false")
    if payload.get("paper_proof_invoked") is not False:
        raise SubmitIntentOwnershipError("submit-intent ownership requires paper_proof_invoked=false")
    if _quantity_value(payload.get("qty")) != 1:
        raise SubmitIntentOwnershipError("submit-intent ownership requires qty=1")
    if str(payload.get("order_type") or "").upper() != "LMT":
        raise SubmitIntentOwnershipError("submit-intent ownership requires order_type=LMT")
    if str(payload.get("time_in_force") or "").upper() != "DAY":
        raise SubmitIntentOwnershipError("submit-intent ownership requires time_in_force=DAY")
    if str(payload.get("action") or "").upper() not in {"BUY", "SELL"}:
        raise SubmitIntentOwnershipError("submit-intent ownership action must be BUY or SELL")
    _state_value(payload.get("state"))


def append_submit_intent_ownership_record(
    record: SubmitIntentOwnershipRecord | Mapping[str, Any],
    *,
    jsonl_path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    latest_path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON,
) -> SubmitIntentOwnershipStoreResult:
    payload = record.to_payload() if isinstance(record, SubmitIntentOwnershipRecord) else dict(record)
    validate_submit_intent_ownership_payload(payload)
    payload = dict(payload)
    payload["digest"] = submit_intent_ownership_digest(payload)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")
    records = load_submit_intent_ownership_records(jsonl_path)
    latest_view = _latest_view(records=records, generated_at=payload["updated_at"])
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(to_jsonable(latest_view), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return SubmitIntentOwnershipStoreResult(
        jsonl_path=jsonl_path,
        latest_path=latest_path,
        record=payload,
        latest_view=latest_view,
    )


def load_submit_intent_ownership_records(path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def load_unresolved_submit_intent_ownership_records(
    path: Path = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
) -> list[dict[str, Any]]:
    latest_by_id: dict[str, dict[str, Any]] = {}
    for record in load_submit_intent_ownership_records(path):
        ownership_id = str(record.get("ownership_intent_id") or "")
        if ownership_id:
            latest_by_id[ownership_id] = record
    return [
        record
        for record in latest_by_id.values()
        if str(record.get("state") or "").upper() in UNRESOLVED_STATES
    ]


def _latest_view(*, records: list[dict[str, Any]], generated_at: str) -> dict[str, Any]:
    latest_by_id: dict[str, dict[str, Any]] = {}
    anonymous_records: list[dict[str, Any]] = []
    for record in records:
        ownership_id = str(record.get("ownership_intent_id") or "")
        if ownership_id:
            latest_by_id[ownership_id] = record
        else:
            anonymous_records.append(record)
    latest_records = [*latest_by_id.values(), *anonymous_records]
    unresolved = [
        record
        for record in latest_records
        if str(record.get("state") or "").upper() in UNRESOLVED_STATES
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "record_count": len(records),
        "latest_ownership_record_count": len(latest_records),
        "unresolved_count": len(unresolved),
        "unresolved_submit_intent_ownership": unresolved,
        "latest_record": records[-1] if records else None,
    }


def _state_value(value: SubmitIntentOwnershipState | str | object) -> str:
    try:
        return value.value if isinstance(value, SubmitIntentOwnershipState) else SubmitIntentOwnershipState(str(value).strip().upper()).value
    except ValueError as exc:
        raise SubmitIntentOwnershipError("submit-intent ownership state is not valid") from exc


def _quantity_value(value: object) -> int | float:
    try:
        numeric = float(str(value))
    except (TypeError, ValueError) as exc:
        raise SubmitIntentOwnershipError("submit-intent ownership qty must be numeric") from exc
    return int(numeric) if numeric.is_integer() else numeric


def _int_or_text(value: object) -> int | str:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return str(value or "").strip()


def _slug(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    return "_".join(part for part in text.split("_") if part)[:80] or "unknown"
