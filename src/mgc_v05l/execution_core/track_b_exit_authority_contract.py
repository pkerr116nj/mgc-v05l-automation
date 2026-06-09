"""Pure Track B managed-exit intent and authority contracts.

The contracts in this module do not submit, cancel, close, refresh broker
state, or mutate lifecycle state. They define the typed shape and invariant
checks that future managed-exit layers must consume.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from mgc_v05l.execution_core.models import (
    JsonSerializable,
    TrackBModelError,
    normalize_decimal,
    require_aware_datetime,
    require_id,
    require_positive_integral_quantity,
)


EXIT_INTENT_SCHEMA_VERSION = "track_b_exit_intent_v1"
EXIT_AUTHORITY_DECISION_SCHEMA_VERSION = "track_b_exit_authority_decision_v1"
EXIT_AUTHORITY_VALIDATOR_VERSION = "track_b_exit_authority_validator_v1"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class CloseAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ExitType(str, Enum):
    FULL_CLOSE = "FULL_CLOSE"
    PARTIAL_SCALE_OUT = "PARTIAL_SCALE_OUT"
    TIMEBOX_CLOSE = "TIMEBOX_CLOSE"
    PROTECTIVE_CLOSE = "PROTECTIVE_CLOSE"
    HARD_STOP = "HARD_STOP"
    TRAILING_STOP = "TRAILING_STOP"
    PROFIT_TARGET = "PROFIT_TARGET"
    VWAP_RECLAIM_LOSS = "VWAP_RECLAIM_LOSS"
    REVERSAL_EXIT = "REVERSAL_EXIT"
    OPERATOR_CLOSE = "OPERATOR_CLOSE"


class ExitUrgency(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    URGENT = "URGENT"


class ExitAuthorityDecisionValue(str, Enum):
    ALLOWED = "ALLOWED"
    BLOCKED = "BLOCKED"
    DEGRADED_ALLOWED = "DEGRADED_ALLOWED"
    DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"


@dataclass(frozen=True)
class SourceArtifactRef(JsonSerializable):
    name: str
    path: str
    generated_at: datetime | None = None
    authority_layer: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", require_id(self.name, "source_artifact_ref.name"))
        object.__setattr__(self, "path", require_id(self.path, "source_artifact_ref.path"))
        if self.generated_at is not None:
            object.__setattr__(
                self,
                "generated_at",
                require_aware_datetime(self.generated_at, "source_artifact_ref.generated_at"),
            )


@dataclass(frozen=True)
class ExitIntent(JsonSerializable):
    exit_intent_id: str
    lifecycle_id: str
    trade_id: str
    strategy_id: str
    lane_id: str
    account: str
    instrument: str
    local_symbol: str
    con_id: int
    position_side: PositionSide | str
    owned_qty: Decimal | int | str
    close_action: CloseAction | str
    close_qty: Decimal | int | str
    remaining_qty_after: Decimal | int | str
    exit_type: ExitType | str
    exit_reason: str
    priority: int
    urgency: ExitUrgency | str
    price_policy: Mapping[str, Any]
    idempotency_key: str
    allow_partial: bool
    allow_reverse: bool
    source_policy_id: str
    generated_at: datetime
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...] = ()
    partial_policy_supported: bool = False
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False
    broad_flatten_allowed: bool = False
    global_flatten_allowed: bool = False
    schema_version: str = EXIT_INTENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "exit_intent_id", require_id(self.exit_intent_id, "exit_intent_id"))
        object.__setattr__(self, "lifecycle_id", require_id(self.lifecycle_id, "lifecycle_id"))
        object.__setattr__(self, "trade_id", require_id(self.trade_id, "trade_id"))
        object.__setattr__(self, "strategy_id", require_id(self.strategy_id, "strategy_id"))
        object.__setattr__(self, "lane_id", require_id(self.lane_id, "lane_id"))
        object.__setattr__(self, "account", require_id(self.account, "account"))
        object.__setattr__(self, "instrument", require_id(self.instrument, "instrument").upper())
        object.__setattr__(self, "local_symbol", require_id(self.local_symbol, "local_symbol").upper())
        if int(self.con_id) <= 0:
            raise TrackBModelError("con_id must be positive.")
        object.__setattr__(self, "con_id", int(self.con_id))
        object.__setattr__(self, "position_side", _normalize_position_side(self.position_side))
        object.__setattr__(self, "owned_qty", require_positive_integral_quantity(self.owned_qty, "owned_qty"))
        object.__setattr__(self, "close_action", _normalize_close_action(self.close_action))
        object.__setattr__(self, "close_qty", require_positive_integral_quantity(self.close_qty, "close_qty"))
        object.__setattr__(
            self,
            "remaining_qty_after",
            _normalize_non_negative_integral_quantity(self.remaining_qty_after, "remaining_qty_after"),
        )
        object.__setattr__(self, "exit_type", _normalize_exit_type(self.exit_type))
        object.__setattr__(self, "exit_reason", require_id(self.exit_reason, "exit_reason"))
        if int(self.priority) < 0:
            raise TrackBModelError("priority must be non-negative.")
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "urgency", _normalize_urgency(self.urgency))
        if not isinstance(self.price_policy, Mapping) or not self.price_policy:
            raise TrackBModelError("price_policy must be a non-empty mapping.")
        object.__setattr__(self, "price_policy", dict(self.price_policy))
        object.__setattr__(self, "source_policy_id", require_id(self.source_policy_id, "source_policy_id"))
        object.__setattr__(self, "generated_at", require_aware_datetime(self.generated_at, "generated_at"))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_artifact_ref(row) for row in self.source_artifact_refs))
        _validate_exit_intent_invariants(self)
        expected_key = build_exit_intent_idempotency_key(self)
        if self.idempotency_key and self.idempotency_key != expected_key:
            raise TrackBModelError("idempotency_key must match deterministic exit intent fields.")
        object.__setattr__(self, "idempotency_key", expected_key)


@dataclass(frozen=True)
class ExitAuthorityDecision(JsonSerializable):
    exit_intent_id: str
    decision: ExitAuthorityDecisionValue | str
    block_reasons: tuple[str, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    hard_required_checks: Mapping[str, Any] = field(default_factory=dict)
    conditional_checks: Mapping[str, Any] = field(default_factory=dict)
    diagnostic_checks: Mapping[str, Any] = field(default_factory=dict)
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...] = ()
    validated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    validator_version: str = EXIT_AUTHORITY_VALIDATOR_VERSION
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False
    broad_flatten_allowed: bool = False
    global_flatten_allowed: bool = False
    schema_version: str = EXIT_AUTHORITY_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "exit_intent_id", require_id(self.exit_intent_id, "exit_intent_id"))
        object.__setattr__(self, "decision", _normalize_authority_decision(self.decision))
        object.__setattr__(self, "block_reasons", tuple(str(row) for row in self.block_reasons if str(row or "").strip()))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics))
        object.__setattr__(self, "hard_required_checks", dict(self.hard_required_checks))
        object.__setattr__(self, "conditional_checks", dict(self.conditional_checks))
        object.__setattr__(self, "diagnostic_checks", dict(self.diagnostic_checks))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_artifact_ref(row) for row in self.source_artifact_refs))
        object.__setattr__(self, "validated_at", require_aware_datetime(self.validated_at, "validated_at"))
        object.__setattr__(self, "validator_version", require_id(self.validator_version, "validator_version"))
        _validate_safety_booleans(self)
        if self.decision == ExitAuthorityDecisionValue.BLOCKED and not self.block_reasons:
            raise TrackBModelError("blocked authority decisions require block_reasons.")
        if self.decision in {ExitAuthorityDecisionValue.ALLOWED, ExitAuthorityDecisionValue.DEGRADED_ALLOWED} and self.block_reasons:
            raise TrackBModelError("allowed authority decisions must not carry block_reasons.")


def build_exit_intent_idempotency_key(intent: ExitIntent | Mapping[str, Any]) -> str:
    payload = intent.to_json_dict() if isinstance(intent, ExitIntent) else dict(intent)
    stable = {
        "account": str(payload.get("account") or ""),
        "close_action": str(_enum_value(payload.get("close_action"))),
        "close_qty": str(normalize_decimal(payload.get("close_qty") or "0", "close_qty")),
        "con_id": int(payload.get("con_id") or 0),
        "exit_type": str(_enum_value(payload.get("exit_type"))),
        "lifecycle_id": str(payload.get("lifecycle_id") or ""),
        "local_symbol": str(payload.get("local_symbol") or "").upper(),
        "source_policy_id": str(payload.get("source_policy_id") or ""),
        "trade_id": str(payload.get("trade_id") or ""),
    }
    digest = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]
    return f"track_b_exit_intent:{digest}"


def validate_exit_intent(intent: ExitIntent | Mapping[str, Any]) -> ExitAuthorityDecision:
    try:
        normalized = intent if isinstance(intent, ExitIntent) else ExitIntent(**dict(intent))
    except (TypeError, ValueError, TrackBModelError) as exc:
        return ExitAuthorityDecision(
            exit_intent_id=str(_mapping(intent).get("exit_intent_id") or "invalid_exit_intent"),
            decision=ExitAuthorityDecisionValue.BLOCKED,
            block_reasons=(str(exc),),
            hard_required_checks={"exit_intent_contract_valid": False},
            validated_at=_safe_now(),
        )
    return ExitAuthorityDecision(
        exit_intent_id=normalized.exit_intent_id,
        decision=ExitAuthorityDecisionValue.ALLOWED,
        hard_required_checks={
            "exit_intent_contract_valid": True,
            "risk_reducing_action": True,
            "close_qty_within_owned_qty": True,
            "no_reverse_or_flip": True,
            "paper_safety_flags": True,
        },
        conditional_checks={
            "partial_close_allowed": normalized.remaining_qty_after == 0
            or (normalized.allow_partial is True and normalized.partial_policy_supported is True),
        },
        source_artifact_refs=normalized.source_artifact_refs,
        validated_at=_safe_now(),
    )


def _validate_exit_intent_invariants(intent: ExitIntent) -> None:
    _validate_safety_booleans(intent)
    if intent.close_qty > intent.owned_qty:
        raise TrackBModelError("close_qty must be less than or equal to owned_qty.")
    expected_remaining = intent.owned_qty - intent.close_qty
    if intent.remaining_qty_after != expected_remaining:
        raise TrackBModelError("remaining_qty_after must equal owned_qty - close_qty.")
    if intent.position_side == PositionSide.LONG and intent.close_action != CloseAction.SELL:
        raise TrackBModelError("long positions require SELL close_action.")
    if intent.position_side == PositionSide.SHORT and intent.close_action != CloseAction.BUY:
        raise TrackBModelError("short positions require BUY close_action.")
    if intent.allow_reverse is True:
        raise TrackBModelError("reverse/flip exits must be modeled separately and are forbidden in ExitIntent.")
    is_partial = expected_remaining > 0
    if is_partial and intent.allow_partial is not True:
        raise TrackBModelError("partial closes require allow_partial=true.")
    if is_partial and intent.partial_policy_supported is not True:
        raise TrackBModelError("partial closes require partial_policy_supported=true.")
    if not is_partial and intent.exit_type == ExitType.PARTIAL_SCALE_OUT:
        raise TrackBModelError("PARTIAL_SCALE_OUT requires remaining_qty_after greater than zero.")
    if is_partial and intent.exit_type == ExitType.FULL_CLOSE:
        raise TrackBModelError("FULL_CLOSE requires remaining_qty_after equal to zero.")


def _validate_safety_booleans(value: Any) -> None:
    if getattr(value, "live_money_eligible") is not False:
        raise TrackBModelError("live_money_eligible must be explicit false.")
    if getattr(value, "paper_proof_invoked") is not False:
        raise TrackBModelError("paper_proof_invoked must be explicit false.")
    if getattr(value, "broad_flatten_allowed") is not False:
        raise TrackBModelError("broad_flatten_allowed must be explicit false.")
    if getattr(value, "global_flatten_allowed") is not False:
        raise TrackBModelError("global_flatten_allowed must be explicit false.")


def _normalize_position_side(value: PositionSide | str) -> PositionSide:
    try:
        return value if isinstance(value, PositionSide) else PositionSide(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("position_side must be LONG or SHORT.") from exc


def _normalize_close_action(value: CloseAction | str) -> CloseAction:
    try:
        return value if isinstance(value, CloseAction) else CloseAction(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("close_action must be BUY or SELL.") from exc


def _normalize_exit_type(value: ExitType | str) -> ExitType:
    try:
        return value if isinstance(value, ExitType) else ExitType(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("exit_type is not valid.") from exc


def _normalize_urgency(value: ExitUrgency | str) -> ExitUrgency:
    try:
        return value if isinstance(value, ExitUrgency) else ExitUrgency(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("urgency is not valid.") from exc


def _normalize_authority_decision(value: ExitAuthorityDecisionValue | str) -> ExitAuthorityDecisionValue:
    try:
        return (
            value
            if isinstance(value, ExitAuthorityDecisionValue)
            else ExitAuthorityDecisionValue(str(value).strip().upper())
        )
    except ValueError as exc:
        raise TrackBModelError("decision is not valid.") from exc


def _normalize_non_negative_integral_quantity(value: Decimal | int | str, field_name: str) -> Decimal:
    normalized = normalize_decimal(value, field_name)
    if normalized < 0 or normalized != normalized.to_integral_value():
        raise TrackBModelError(f"{field_name} must be a non-negative whole-number quantity.")
    return normalized


def _normalize_artifact_ref(value: SourceArtifactRef | Mapping[str, Any]) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    if not isinstance(value, Mapping):
        raise TrackBModelError("source_artifact_refs must contain mappings or SourceArtifactRef values.")
    return SourceArtifactRef(**dict(value))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _safe_now() -> datetime:
    return datetime.now(UTC)
