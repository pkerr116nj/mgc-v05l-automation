"""Pure Track B managed-exit intent and authority contracts.

The contracts in this module do not submit, cancel, close, refresh broker
state, or mutate lifecycle state. V1.1 separates risk-reduction authority from
strategy/lifecycle attribution: the validator answers only whether a proposed
action can safely reduce current broker exposure in the intended domain.
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


EXIT_INTENT_SCHEMA_VERSION = "track_b_exit_intent_v1_1"
EXIT_AUTHORITY_DECISION_SCHEMA_VERSION = "track_b_exit_authority_decision_v1_1"
EXIT_AUTHORITY_VALIDATOR_VERSION = "track_b_exit_authority_validator_v1_1"


class ExecutionDomain(str, Enum):
    TRACK_B_PAPER = "TRACK_B_PAPER"
    TRACK_B_LIVE = "TRACK_B_LIVE"


class AttributionStatus(str, Enum):
    ATTRIBUTED = "ATTRIBUTED"
    PARTIALLY_ATTRIBUTED = "PARTIALLY_ATTRIBUTED"
    UNATTRIBUTED = "UNATTRIBUTED"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class CloseAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class CloseQtySource(str, Enum):
    STRATEGY_POLICY = "STRATEGY_POLICY"
    RISK_POLICY = "RISK_POLICY"
    OPERATOR_INSTRUCTION = "OPERATOR_INSTRUCTION"
    PORTFOLIO_RISK_MANAGER = "PORTFOLIO_RISK_MANAGER"


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
    DEGRADED_ALLOWED = "DEGRADED_ALLOWED"
    BLOCKED = "BLOCKED"


class ExitAuthorityCheckCategory(str, Enum):
    HARD_REQUIRED = "HARD_REQUIRED"
    CONDITIONAL = "CONDITIONAL"
    DIAGNOSTIC = "DIAGNOSTIC"


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
class ExitAttribution(JsonSerializable):
    lifecycle_id: str | None = None
    trade_id: str | None = None
    strategy_id: str | None = None
    lane_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "lifecycle_id", _optional_id(self.lifecycle_id))
        object.__setattr__(self, "trade_id", _optional_id(self.trade_id))
        object.__setattr__(self, "strategy_id", _optional_id(self.strategy_id))
        object.__setattr__(self, "lane_id", _optional_id(self.lane_id))

    @property
    def status(self) -> AttributionStatus:
        present = [self.lifecycle_id, self.trade_id, self.strategy_id, self.lane_id]
        count = sum(1 for item in present if item)
        if count == len(present):
            return AttributionStatus.ATTRIBUTED
        if count:
            return AttributionStatus.PARTIALLY_ATTRIBUTED
        return AttributionStatus.UNATTRIBUTED


@dataclass(frozen=True)
class ExitIntent(JsonSerializable):
    exit_intent_id: str
    execution_domain: ExecutionDomain | str
    account_id: str
    instrument: str
    local_symbol: str
    con_id: int
    position_side: PositionSide | str
    owned_qty: Decimal | int | str
    close_action: CloseAction | str
    close_qty: Decimal | int | str
    remaining_qty_after: Decimal | int | str
    close_qty_source: CloseQtySource | str
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
    attribution: ExitAttribution | Mapping[str, Any] | None = None
    lifecycle_id: str | None = None
    trade_id: str | None = None
    strategy_id: str | None = None
    lane_id: str | None = None
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...] = ()
    partial_policy_supported: bool = False
    live_money_eligible: bool = False
    live_money_allowed: bool = False
    paper_proof_invoked: bool = False
    broad_flatten_allowed: bool = False
    global_flatten_allowed: bool = False
    schema_version: str = EXIT_INTENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "exit_intent_id", require_id(self.exit_intent_id, "exit_intent_id"))
        object.__setattr__(self, "execution_domain", _normalize_execution_domain(self.execution_domain))
        object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
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
        object.__setattr__(self, "close_qty_source", _normalize_close_qty_source(self.close_qty_source))
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
        object.__setattr__(self, "attribution", _normalize_attribution(self))
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
    attribution_status: AttributionStatus | str = AttributionStatus.UNATTRIBUTED
    attribution_diagnostics: Mapping[str, Any] = field(default_factory=dict)
    hard_required_checks: Mapping[str, Any] = field(default_factory=dict)
    conditional_risk_checks: Mapping[str, Any] = field(default_factory=dict)
    diagnostic_checks: Mapping[str, Any] = field(default_factory=dict)
    block_reasons: tuple[str, ...] = ()
    execution_domain: ExecutionDomain | str | None = None
    account_id: str | None = None
    validated_close_qty: Decimal | int | str | None = None
    validated_remaining_qty: Decimal | int | str | None = None
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...] = ()
    validated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    validator_version: str = EXIT_AUTHORITY_VALIDATOR_VERSION
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    conditional_checks: Mapping[str, Any] = field(default_factory=dict)
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False
    broad_flatten_allowed: bool = False
    global_flatten_allowed: bool = False
    schema_version: str = EXIT_AUTHORITY_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "exit_intent_id", require_id(self.exit_intent_id, "exit_intent_id"))
        object.__setattr__(self, "decision", _normalize_authority_decision(self.decision))
        object.__setattr__(self, "attribution_status", _normalize_attribution_status(self.attribution_status))
        object.__setattr__(self, "attribution_diagnostics", dict(self.attribution_diagnostics))
        object.__setattr__(self, "hard_required_checks", dict(self.hard_required_checks))
        risk_checks = dict(self.conditional_risk_checks or self.conditional_checks)
        object.__setattr__(self, "conditional_risk_checks", risk_checks)
        object.__setattr__(self, "conditional_checks", risk_checks)
        object.__setattr__(self, "diagnostic_checks", dict(self.diagnostic_checks))
        object.__setattr__(self, "block_reasons", tuple(str(row) for row in self.block_reasons if str(row or "").strip()))
        if self.execution_domain is not None:
            object.__setattr__(self, "execution_domain", _normalize_execution_domain(self.execution_domain))
        if self.account_id is not None:
            object.__setattr__(self, "account_id", require_id(self.account_id, "account_id"))
        if self.validated_close_qty is not None:
            object.__setattr__(
                self,
                "validated_close_qty",
                require_positive_integral_quantity(self.validated_close_qty, "validated_close_qty"),
            )
        if self.validated_remaining_qty is not None:
            object.__setattr__(
                self,
                "validated_remaining_qty",
                _normalize_non_negative_integral_quantity(self.validated_remaining_qty, "validated_remaining_qty"),
            )
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_artifact_ref(row) for row in self.source_artifact_refs))
        object.__setattr__(self, "validated_at", require_aware_datetime(self.validated_at, "validated_at"))
        object.__setattr__(self, "validator_version", require_id(self.validator_version, "validator_version"))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics))
        _validate_output_safety_booleans(self)
        if self.decision == ExitAuthorityDecisionValue.BLOCKED and not self.block_reasons:
            raise TrackBModelError("blocked authority decisions require block_reasons.")
        if self.decision in {ExitAuthorityDecisionValue.ALLOWED, ExitAuthorityDecisionValue.DEGRADED_ALLOWED} and self.block_reasons:
            raise TrackBModelError("allowed authority decisions must not carry block_reasons.")


@dataclass(frozen=True)
class ExitAuthorityCurrentState(JsonSerializable):
    execution_domain: ExecutionDomain | str
    known_position: bool
    broker_position_side: PositionSide | str
    broker_position_qty: Decimal | int | str
    account_id: str
    local_symbol: str
    con_id: int
    safe_state_hard_halt: bool = False
    same_contract_working_close_qty: Decimal | int | str = 0
    unrelated_unknown_order_count: int = 0
    same_contract_unknown_order_count: int = 0
    same_contract_unknown_order_could_over_close: bool = False
    same_contract_unknown_order_over_close_ruled_out: bool = False
    reconciliation_clean: bool | None = None
    safe_state_allows_managed_close: bool | None = None
    guardian_allows_exact_close: bool | None = None
    bsa_managed_risk_reducing_close: bool | None = None
    bsa_degraded_exact_close_ready: bool | None = None
    live_money_eligible: bool = False
    live_money_allowed: bool = False
    paper_proof_invoked: bool = False
    broad_flatten_allowed: bool = False
    global_flatten_allowed: bool = False
    attribution_status: AttributionStatus | str = AttributionStatus.UNATTRIBUTED
    attribution_diagnostics: Mapping[str, Any] = field(default_factory=dict)
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "execution_domain", _normalize_execution_domain(self.execution_domain))
        object.__setattr__(self, "broker_position_side", _normalize_position_side(self.broker_position_side))
        object.__setattr__(
            self,
            "broker_position_qty",
            require_positive_integral_quantity(self.broker_position_qty, "broker_position_qty"),
        )
        object.__setattr__(self, "account_id", require_id(self.account_id, "state.account_id"))
        object.__setattr__(self, "local_symbol", require_id(self.local_symbol, "state.local_symbol").upper())
        if int(self.con_id) <= 0:
            raise TrackBModelError("state.con_id must be positive.")
        object.__setattr__(self, "con_id", int(self.con_id))
        object.__setattr__(
            self,
            "same_contract_working_close_qty",
            _normalize_non_negative_integral_quantity(self.same_contract_working_close_qty, "same_contract_working_close_qty"),
        )
        object.__setattr__(
            self,
            "unrelated_unknown_order_count",
            _normalize_non_negative_int(self.unrelated_unknown_order_count, "unrelated_unknown_order_count"),
        )
        object.__setattr__(
            self,
            "same_contract_unknown_order_count",
            _normalize_non_negative_int(self.same_contract_unknown_order_count, "same_contract_unknown_order_count"),
        )
        object.__setattr__(self, "attribution_status", _normalize_attribution_status(self.attribution_status))
        object.__setattr__(self, "attribution_diagnostics", dict(self.attribution_diagnostics))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_artifact_ref(row) for row in self.source_artifact_refs))
        _validate_state_safety_flags(self)


class ExitAuthorityValidator:
    """Pure validator for managed-exit authority facts."""

    validator_version = EXIT_AUTHORITY_VALIDATOR_VERSION

    def validate(
        self,
        *,
        intent: ExitIntent | Mapping[str, Any],
        current_state: ExitAuthorityCurrentState | Mapping[str, Any],
        validated_at: datetime | None = None,
    ) -> ExitAuthorityDecision:
        actual_validated_at = require_aware_datetime(validated_at or _safe_now(), "validated_at")
        normalized_intent, intent_error = _coerce_exit_intent(intent)
        normalized_state, state_error = _coerce_current_state(current_state)
        if normalized_intent is None:
            return _blocked_decision(
                exit_intent_id=str(_mapping(intent).get("exit_intent_id") or "invalid_exit_intent"),
                reason=str(intent_error),
                check_name="exit_intent_contract_valid",
                validated_at=actual_validated_at,
            )
        if normalized_state is None:
            return _blocked_decision(
                exit_intent_id=normalized_intent.exit_intent_id,
                reason=str(state_error),
                check_name="current_state_contract_valid",
                validated_at=actual_validated_at,
                source_artifact_refs=normalized_intent.source_artifact_refs,
            )

        attribution_status = _combined_attribution_status(normalized_intent, normalized_state)
        hard_required_checks = _hard_required_checks(normalized_intent, normalized_state)
        conditional_risk_checks = _conditional_risk_checks(normalized_intent, normalized_state)
        diagnostic_checks = _diagnostic_checks(normalized_intent, normalized_state)
        block_reasons = tuple(
            check["code"]
            for checks in (hard_required_checks, conditional_risk_checks)
            for check in checks.values()
            if check.get("passed") is not True
        )
        degraded = (
            not block_reasons
            and (
                attribution_status != AttributionStatus.ATTRIBUTED
                or _same_contract_unknown_order_degraded(normalized_state)
            )
        )
        decision = (
            ExitAuthorityDecisionValue.BLOCKED
            if block_reasons
            else ExitAuthorityDecisionValue.DEGRADED_ALLOWED
            if degraded
            else ExitAuthorityDecisionValue.ALLOWED
        )
        return ExitAuthorityDecision(
            exit_intent_id=normalized_intent.exit_intent_id,
            decision=decision,
            attribution_status=attribution_status,
            attribution_diagnostics=_attribution_diagnostics(normalized_intent, normalized_state),
            hard_required_checks=hard_required_checks,
            conditional_risk_checks=conditional_risk_checks,
            diagnostic_checks=diagnostic_checks,
            block_reasons=block_reasons,
            execution_domain=normalized_intent.execution_domain,
            account_id=normalized_intent.account_id,
            validated_close_qty=normalized_intent.close_qty,
            validated_remaining_qty=normalized_intent.remaining_qty_after,
            source_artifact_refs=tuple(normalized_intent.source_artifact_refs) + tuple(normalized_state.source_artifact_refs),
            validated_at=actual_validated_at,
            validator_version=self.validator_version,
            diagnostics={
                "risk_reduction_only": True,
                "attribution_blocks_authority": False,
            },
        )


def build_exit_intent_idempotency_key(intent: ExitIntent | Mapping[str, Any]) -> str:
    payload = intent.to_json_dict() if isinstance(intent, ExitIntent) else dict(intent)
    stable = {
        "account_id": str(payload.get("account_id") or ""),
        "close_action": str(_enum_value(payload.get("close_action"))),
        "close_qty": str(normalize_decimal(payload.get("close_qty") or "0", "close_qty")),
        "close_qty_source": str(_enum_value(payload.get("close_qty_source"))),
        "con_id": int(payload.get("con_id") or 0),
        "execution_domain": str(_enum_value(payload.get("execution_domain"))),
        "exit_type": str(_enum_value(payload.get("exit_type"))),
        "local_symbol": str(payload.get("local_symbol") or "").upper(),
        "source_policy_id": str(payload.get("source_policy_id") or ""),
    }
    digest = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]
    return f"track_b_exit_intent:{digest}"


def validate_exit_authority(
    *,
    intent: ExitIntent | Mapping[str, Any],
    current_state: ExitAuthorityCurrentState | Mapping[str, Any],
    validated_at: datetime | None = None,
) -> ExitAuthorityDecision:
    return ExitAuthorityValidator().validate(intent=intent, current_state=current_state, validated_at=validated_at)


def validate_exit_intent(intent: ExitIntent | Mapping[str, Any]) -> ExitAuthorityDecision:
    try:
        normalized = intent if isinstance(intent, ExitIntent) else ExitIntent(**dict(intent))
    except (TypeError, ValueError, TrackBModelError) as exc:
        return ExitAuthorityDecision(
            exit_intent_id=str(_mapping(intent).get("exit_intent_id") or "invalid_exit_intent"),
            decision=ExitAuthorityDecisionValue.BLOCKED,
            block_reasons=(str(exc),),
            hard_required_checks={"exit_intent_contract_valid": _check(ExitAuthorityCheckCategory.HARD_REQUIRED, False, "exit_intent_contract_valid", str(exc))},
            validated_at=_safe_now(),
        )
    return ExitAuthorityDecision(
        exit_intent_id=normalized.exit_intent_id,
        decision=ExitAuthorityDecisionValue.ALLOWED,
        attribution_status=normalized.attribution.status,
        hard_required_checks={
            "exit_intent_contract_valid": _check(
                ExitAuthorityCheckCategory.HARD_REQUIRED,
                True,
                "exit_intent_contract_valid",
                "ExitIntent schema and quantity invariants are valid.",
            ),
        },
        conditional_risk_checks={
            "partial_close_policy": _check(
                ExitAuthorityCheckCategory.CONDITIONAL,
                True,
                "partial_close_policy",
                "Partial close quantity source is explicit when required.",
            ),
        },
        execution_domain=normalized.execution_domain,
        account_id=normalized.account_id,
        validated_close_qty=normalized.close_qty,
        validated_remaining_qty=normalized.remaining_qty_after,
        source_artifact_refs=normalized.source_artifact_refs,
        validated_at=_safe_now(),
    )


def _hard_required_checks(intent: ExitIntent, state: ExitAuthorityCurrentState) -> dict[str, dict[str, Any]]:
    return {
        "known_current_broker_position": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.known_position is True,
            "known_current_broker_position_missing",
            "Current state must identify a known broker position.",
        ),
        "execution_domain_matches": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.execution_domain == intent.execution_domain,
            "execution_domain_mismatch",
            "Current state must be in the ExitIntent execution domain.",
        ),
        "account_matches": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.account_id == intent.account_id,
            "account_mismatch",
            "Current broker account must match the ExitIntent account.",
        ),
        "contract_matches": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.local_symbol == intent.local_symbol and state.con_id == intent.con_id,
            "contract_mismatch",
            "Current broker contract must match the ExitIntent contract.",
        ),
        "position_side_matches": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.broker_position_side == intent.position_side,
            "position_side_mismatch",
            "Current broker position side must match the ExitIntent.",
        ),
        "close_qty_within_broker_position": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            Decimal("0") < intent.close_qty <= state.broker_position_qty,
            "close_qty_out_of_bounds",
            "Close quantity must be greater than zero and not exceed current broker position quantity.",
        ),
        "risk_reducing_action": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            _close_action_reduces_position(intent),
            "not_risk_reducing",
            "Close action must reduce absolute exposure.",
        ),
        "no_reverse_or_flip": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            intent.allow_reverse is False and intent.remaining_qty_after >= 0,
            "reverse_or_flip_forbidden",
            "Reverse/flip actions must be modeled separately.",
        ),
        "price_policy_present": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            bool(intent.price_policy),
            "price_policy_missing",
            "Executable price policy is required.",
        ),
        "same_contract_working_close_does_not_over_close": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.same_contract_working_close_qty + intent.close_qty <= state.broker_position_qty,
            "same_contract_working_close_over_close_risk",
            "Known same-contract working closes plus proposed close must not over-close.",
        ),
        "paper_proof_not_invoked": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            intent.paper_proof_invoked is False and state.paper_proof_invoked is False,
            "paper_proof_invoked",
            "paper_proof must not be invoked for exit authority.",
        ),
        "live_money_domain_allowed": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            _live_money_allowed(intent, state),
            "live_money_not_allowed",
            "live_money is only allowed when explicitly scoped to TRACK_B_LIVE.",
        ),
        "safe_state_no_hard_halt": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            state.safe_state_hard_halt is False,
            "safe_state_hard_halt",
            "Safe-State hard halt blocks exit authority.",
        ),
        "broad_or_global_flatten_not_requested": _check(
            ExitAuthorityCheckCategory.HARD_REQUIRED,
            intent.broad_flatten_allowed is False
            and intent.global_flatten_allowed is False
            and state.broad_flatten_allowed is False
            and state.global_flatten_allowed is False,
            "broad_or_global_flatten_requested",
            "Broad/global flatten semantics are outside ExitIntent.",
        ),
    }


def _conditional_risk_checks(intent: ExitIntent, state: ExitAuthorityCurrentState) -> dict[str, dict[str, Any]]:
    partial = intent.remaining_qty_after > 0
    same_contract_unknown_safe = (
        state.same_contract_unknown_order_count == 0
        or (
            state.same_contract_unknown_order_over_close_ruled_out is True
            and state.same_contract_unknown_order_could_over_close is False
        )
    )
    return {
        "partial_close_qty_source": _check(
            ExitAuthorityCheckCategory.CONDITIONAL,
            (not partial) or (intent.allow_partial is True and intent.partial_policy_supported is True),
            "partial_close_not_authorized",
            "Partial exits require explicit partial permission and policy/source support.",
        ),
        "same_contract_unknown_order_risk": _check(
            ExitAuthorityCheckCategory.CONDITIONAL,
            same_contract_unknown_safe,
            "same_contract_unknown_order_over_close_risk",
            "Same-contract unknown orders block when over-close risk cannot be ruled out.",
        ),
    }


def _diagnostic_checks(intent: ExitIntent, state: ExitAuthorityCurrentState) -> dict[str, dict[str, Any]]:
    checks = {
        "attribution_status": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            _combined_attribution_status(intent, state) == AttributionStatus.ATTRIBUTED,
            "attribution_incomplete",
            "Attribution is diagnostic unless it creates safety ambiguity.",
        ),
        "unrelated_unknown_orders": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            state.unrelated_unknown_order_count == 0,
            "unrelated_unknown_orders_present",
            "Unrelated unknown orders are diagnostic-only for this contract.",
        ),
        "reconciliation_clean": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            state.reconciliation_clean is True,
            "reconciliation_not_clean",
            "Strategy/lifecycle reconciliation cleanliness is diagnostic for risk-reducing exit authority.",
        ),
        "safe_state_allows_managed_close": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            state.safe_state_allows_managed_close is not False,
            "safe_state_managed_close_warning",
            "Only explicit Safe-State hard halt blocks this contract.",
        ),
        "guardian_allows_exact_close": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            state.guardian_allows_exact_close is not False,
            "guardian_exact_close_warning",
            "Guardian attribution is diagnostic unless current broker risk is ambiguous.",
        ),
        "bsa_close_authority": _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            state.bsa_managed_risk_reducing_close is not False or state.bsa_degraded_exact_close_ready is True,
            "bsa_close_authority_warning",
            "BSA is diagnostic in V1.1 unless separate broker/session safety facts are unsafe.",
        ),
    }
    for name, value in state.diagnostics.items():
        checks[str(name)] = _check(
            ExitAuthorityCheckCategory.DIAGNOSTIC,
            bool(value),
            f"diagnostic_{name}",
            "Diagnostic-only checks are surfaced but do not block exit authority.",
        )
    return checks


def _check(category: ExitAuthorityCheckCategory, passed: bool, code: str, detail: str) -> dict[str, Any]:
    return {
        "category": category.value,
        "passed": bool(passed),
        "code": code,
        "detail": detail,
    }


def _blocked_decision(
    *,
    exit_intent_id: str,
    reason: str,
    check_name: str,
    validated_at: datetime,
    source_artifact_refs: tuple[SourceArtifactRef, ...] = (),
) -> ExitAuthorityDecision:
    return ExitAuthorityDecision(
        exit_intent_id=exit_intent_id,
        decision=ExitAuthorityDecisionValue.BLOCKED,
        block_reasons=(reason,),
        hard_required_checks={
            check_name: _check(ExitAuthorityCheckCategory.HARD_REQUIRED, False, check_name, reason),
        },
        source_artifact_refs=source_artifact_refs,
        validated_at=validated_at,
    )


def _coerce_exit_intent(value: ExitIntent | Mapping[str, Any]) -> tuple[ExitIntent | None, Exception | None]:
    try:
        return (value if isinstance(value, ExitIntent) else ExitIntent(**dict(value)), None)
    except (TypeError, ValueError, TrackBModelError) as exc:
        return None, exc


def _coerce_current_state(
    value: ExitAuthorityCurrentState | Mapping[str, Any],
) -> tuple[ExitAuthorityCurrentState | None, Exception | None]:
    try:
        return (value if isinstance(value, ExitAuthorityCurrentState) else ExitAuthorityCurrentState(**dict(value)), None)
    except (TypeError, ValueError, TrackBModelError) as exc:
        return None, exc


def _validate_exit_intent_invariants(intent: ExitIntent) -> None:
    _validate_intent_safety_flags(intent)
    if intent.close_qty > intent.owned_qty:
        raise TrackBModelError("close_qty must be less than or equal to owned_qty.")
    expected_remaining = intent.owned_qty - intent.close_qty
    if intent.remaining_qty_after != expected_remaining:
        raise TrackBModelError("remaining_qty_after must equal owned_qty - close_qty.")
    if not _close_action_reduces_position(intent):
        side = "long" if intent.position_side == PositionSide.LONG else "short"
        action = "SELL" if intent.position_side == PositionSide.LONG else "BUY"
        raise TrackBModelError(f"{side} positions require {action} close_action.")
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


def _validate_intent_safety_flags(intent: ExitIntent) -> None:
    if intent.paper_proof_invoked is not False:
        raise TrackBModelError("paper_proof_invoked must be explicit false.")
    if intent.broad_flatten_allowed is not False:
        raise TrackBModelError("broad_flatten_allowed must be explicit false.")
    if intent.global_flatten_allowed is not False:
        raise TrackBModelError("global_flatten_allowed must be explicit false.")
    if intent.live_money_eligible is True and not (
        intent.execution_domain == ExecutionDomain.TRACK_B_LIVE and intent.live_money_allowed is True
    ):
        raise TrackBModelError("live_money_eligible requires TRACK_B_LIVE and live_money_allowed=true.")


def _validate_state_safety_flags(state: ExitAuthorityCurrentState) -> None:
    if state.paper_proof_invoked is not False:
        raise TrackBModelError("paper_proof_invoked must be explicit false.")
    if state.broad_flatten_allowed is not False:
        raise TrackBModelError("broad_flatten_allowed must be explicit false.")
    if state.global_flatten_allowed is not False:
        raise TrackBModelError("global_flatten_allowed must be explicit false.")
    if state.live_money_eligible is True and not (
        state.execution_domain == ExecutionDomain.TRACK_B_LIVE and state.live_money_allowed is True
    ):
        raise TrackBModelError("live_money_eligible requires TRACK_B_LIVE and live_money_allowed=true.")


def _validate_output_safety_booleans(value: ExitAuthorityDecision) -> None:
    if value.paper_proof_invoked is not False:
        raise TrackBModelError("paper_proof_invoked must be explicit false.")
    if value.broad_flatten_allowed is not False:
        raise TrackBModelError("broad_flatten_allowed must be explicit false.")
    if value.global_flatten_allowed is not False:
        raise TrackBModelError("global_flatten_allowed must be explicit false.")


def _combined_attribution_status(intent: ExitIntent, state: ExitAuthorityCurrentState) -> AttributionStatus:
    statuses = (intent.attribution.status, state.attribution_status)
    if AttributionStatus.ATTRIBUTED in statuses:
        return AttributionStatus.ATTRIBUTED
    if AttributionStatus.PARTIALLY_ATTRIBUTED in statuses:
        return AttributionStatus.PARTIALLY_ATTRIBUTED
    return AttributionStatus.UNATTRIBUTED


def _attribution_diagnostics(intent: ExitIntent, state: ExitAuthorityCurrentState) -> dict[str, Any]:
    return {
        "intent_attribution_status": intent.attribution.status.value,
        "state_attribution_status": state.attribution_status.value,
        "lifecycle_id": intent.attribution.lifecycle_id,
        "trade_id": intent.attribution.trade_id,
        "strategy_id": intent.attribution.strategy_id,
        "lane_id": intent.attribution.lane_id,
        "state": dict(state.attribution_diagnostics),
        "blocks_authority": False,
    }


def _same_contract_unknown_order_degraded(state: ExitAuthorityCurrentState) -> bool:
    return (
        state.same_contract_unknown_order_count > 0
        and state.same_contract_unknown_order_over_close_ruled_out is True
        and state.same_contract_unknown_order_could_over_close is False
    )


def _live_money_allowed(intent: ExitIntent, state: ExitAuthorityCurrentState) -> bool:
    if intent.paper_proof_invoked or state.paper_proof_invoked:
        return False
    live_requested = bool(intent.live_money_eligible or state.live_money_eligible)
    if not live_requested:
        return True
    return (
        intent.execution_domain == ExecutionDomain.TRACK_B_LIVE
        and state.execution_domain == ExecutionDomain.TRACK_B_LIVE
        and intent.live_money_allowed is True
        and state.live_money_allowed is True
    )


def _close_action_reduces_position(intent: ExitIntent) -> bool:
    return (
        intent.position_side == PositionSide.LONG
        and intent.close_action == CloseAction.SELL
        or intent.position_side == PositionSide.SHORT
        and intent.close_action == CloseAction.BUY
    )


def _normalize_execution_domain(value: ExecutionDomain | str) -> ExecutionDomain:
    try:
        return value if isinstance(value, ExecutionDomain) else ExecutionDomain(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("execution_domain must be TRACK_B_PAPER or TRACK_B_LIVE.") from exc


def _normalize_attribution_status(value: AttributionStatus | str) -> AttributionStatus:
    try:
        return value if isinstance(value, AttributionStatus) else AttributionStatus(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("attribution_status is not valid.") from exc


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


def _normalize_close_qty_source(value: CloseQtySource | str) -> CloseQtySource:
    try:
        return value if isinstance(value, CloseQtySource) else CloseQtySource(str(value).strip().upper())
    except ValueError as exc:
        raise TrackBModelError("close_qty_source is not valid.") from exc


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


def _normalize_non_negative_int(value: int, field_name: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise TrackBModelError(f"{field_name} must be non-negative.")
    return normalized


def _normalize_artifact_ref(value: SourceArtifactRef | Mapping[str, Any]) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    if not isinstance(value, Mapping):
        raise TrackBModelError("source_artifact_refs must contain mappings or SourceArtifactRef values.")
    return SourceArtifactRef(**dict(value))


def _normalize_attribution(intent: ExitIntent) -> ExitAttribution:
    if isinstance(intent.attribution, ExitAttribution):
        return intent.attribution
    if isinstance(intent.attribution, Mapping):
        base = dict(intent.attribution)
    else:
        base = {}
    base.setdefault("lifecycle_id", intent.lifecycle_id)
    base.setdefault("trade_id", intent.trade_id)
    base.setdefault("strategy_id", intent.strategy_id)
    base.setdefault("lane_id", intent.lane_id)
    return ExitAttribution(**base)


def _optional_id(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _safe_now() -> datetime:
    return datetime.now(UTC)
