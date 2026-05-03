"""Track B no-submit signal-to-intent proposal policy boundary."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .shadow_signal import (
    SignalDecisionStyle,
    ShadowSignal,
    ShadowSignalValidationConfig,
    ShadowSignalValidationResult,
    ShadowSignalValidationVerdict,
    validate_shadow_signal,
)
from .strategy_intent import StrategyTradeIntent


DEFAULT_SIGNAL_INTENT_PROPOSAL_OUTPUT_ROOT = Path("outputs/track_b_execution_core/signal_intent_proposals")


class IntentProposalVerdict(str, Enum):
    CREATED_FOR_REVIEW = "INTENT_PROPOSAL_CREATED_FOR_REVIEW"
    BLOCKED_INVALID_SIGNAL = "INTENT_PROPOSAL_BLOCKED_INVALID_SIGNAL"
    BLOCKED_BINARY_POLICY_MISSING_ORDER_FIELDS = "INTENT_PROPOSAL_BLOCKED_BINARY_POLICY_MISSING_ORDER_FIELDS"
    BLOCKED_STATIC_SCORE_BELOW_THRESHOLD = "INTENT_PROPOSAL_BLOCKED_STATIC_SCORE_BELOW_THRESHOLD"
    BLOCKED_SCORING_REQUIRED = "INTENT_PROPOSAL_BLOCKED_SCORING_REQUIRED"
    BLOCKED_DYNAMIC_SCORING_NOT_IMPLEMENTED = "INTENT_PROPOSAL_BLOCKED_DYNAMIC_SCORING_NOT_IMPLEMENTED"
    BLOCKED_HUMAN_REVIEW_ONLY = "INTENT_PROPOSAL_BLOCKED_HUMAN_REVIEW_ONLY"
    BLOCKED_LIVE_MODE = "INTENT_PROPOSAL_BLOCKED_LIVE_MODE"
    BLOCKED_SCHEMA_ERROR = "INTENT_PROPOSAL_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class SignalIntentProposalPolicy:
    quantity: Any = None
    order_type: str | None = None
    limit_price: Any = None
    limit_price_source: str | None = None
    time_in_force: str | None = None
    source: str = "track_b_signal_intent_proposal"
    min_signal_score: Any = None
    min_expected_value_r: Any = None
    allowed_confidence_levels: tuple[str, ...] = ()
    allow_dynamic_scoring_for_review_only: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "SignalIntentProposalPolicy":
        actual = payload or {}
        return cls(
            quantity=actual.get("quantity"),
            order_type=_optional_upper(actual.get("order_type")),
            limit_price=actual.get("limit_price"),
            limit_price_source=_optional_str(actual.get("limit_price_source")),
            time_in_force=_optional_upper(actual.get("time_in_force")),
            source=_optional_str(actual.get("source")) or "track_b_signal_intent_proposal",
            min_signal_score=actual.get("min_signal_score"),
            min_expected_value_r=actual.get("min_expected_value_r"),
            allowed_confidence_levels=_upper_tuple(actual.get("allowed_confidence_levels")),
            allow_dynamic_scoring_for_review_only=_bool(actual.get("allow_dynamic_scoring_for_review_only", False)),
        )


@dataclass(frozen=True)
class SignalIntentProposalConfig:
    expected_account_id: str | None = None
    policy: SignalIntentProposalPolicy | None = None
    output_root: Path = DEFAULT_SIGNAL_INTENT_PROPOSAL_OUTPUT_ROOT


@dataclass(frozen=True)
class SignalIntentProposalResult:
    verdict: IntentProposalVerdict
    report_json: Path
    report: dict[str, Any]
    proposed_intent: dict[str, Any] | None


def propose_intent_from_signal(
    *,
    signal_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any] | None = None,
    config: SignalIntentProposalConfig | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> SignalIntentProposalResult:
    actual_config = config or SignalIntentProposalConfig()
    policy = actual_config.policy or SignalIntentProposalPolicy.from_mapping(policy_payload)
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"signal_intent_proposal_{uuid.uuid4().hex}"
    report_json = Path(actual_config.output_root) / actual_run_id / "signal_intent_proposal_report.json"
    try:
        signal_result = validate_shadow_signal(
            payload=signal_payload,
            config=ShadowSignalValidationConfig(
                expected_account_id=actual_config.expected_account_id,
                output_root=Path(actual_config.output_root) / "signal_validation",
            ),
            run_id=f"{actual_run_id}_signal",
            now=actual_now,
        )
        verdict, proposed_intent, blocker, action, scoring_passed, secondary = _classify_and_propose(
            signal_result=signal_result,
            policy=policy,
            now=actual_now,
        )
    except (TypeError, ValueError) as exc:
        signal_result = None
        verdict = IntentProposalVerdict.BLOCKED_SCHEMA_ERROR
        proposed_intent = None
        blocker = str(exc)
        action = "Fix signal intent proposal policy or signal schema before review."
        scoring_passed = False
        secondary = ()

    report = _report(
        verdict=verdict,
        signal_result=signal_result,
        policy=policy,
        proposed_intent=proposed_intent,
        primary_blocker=blocker,
        required_next_action=action,
        scoring_passed_policy=scoring_passed,
        secondary_blockers=secondary,
        generated_at=actual_now,
        report_json=report_json,
    )
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return SignalIntentProposalResult(verdict=verdict, report_json=report_json, report=report, proposed_intent=proposed_intent)


def _classify_and_propose(
    *,
    signal_result: ShadowSignalValidationResult,
    policy: SignalIntentProposalPolicy,
    now: datetime,
) -> tuple[IntentProposalVerdict, dict[str, Any] | None, str | None, str, bool, tuple[str, ...]]:
    if signal_result.verdict == ShadowSignalValidationVerdict.BLOCKED_LIVE_MODE:
        return (
            IntentProposalVerdict.BLOCKED_LIVE_MODE,
            None,
            str(signal_result.report.get("primary_blocker") or "Signal uses unsupported live mode."),
            "Use PAPER mode before signal-to-intent proposal.",
            False,
            (),
        )
    if signal_result.verdict == ShadowSignalValidationVerdict.BLOCKED_SCORING_REQUIRED:
        return (
            IntentProposalVerdict.BLOCKED_SCORING_REQUIRED,
            None,
            str(signal_result.report.get("primary_blocker") or "Signal scoring is required for proposal policy."),
            str(signal_result.report.get("required_next_action") or "Add scoring metadata before proposal."),
            False,
            (),
        )
    if signal_result.verdict != ShadowSignalValidationVerdict.VALID_FOR_REVIEW or signal_result.signal is None:
        return (
            IntentProposalVerdict.BLOCKED_INVALID_SIGNAL,
            None,
            str(signal_result.report.get("primary_blocker") or "Shadow signal is invalid."),
            str(signal_result.report.get("required_next_action") or "Fix shadow signal before proposal."),
            False,
            (),
        )
    signal = signal_result.signal
    if signal.mode != "PAPER":
        return (
            IntentProposalVerdict.BLOCKED_LIVE_MODE,
            None,
            "Signal-to-intent proposal accepts PAPER mode only.",
            "Use PAPER mode. Live-money proposal is not supported.",
            False,
            (),
        )
    if signal.decision_style == SignalDecisionStyle.HUMAN_REVIEW.value:
        return (
            IntentProposalVerdict.BLOCKED_HUMAN_REVIEW_ONLY,
            None,
            "HUMAN_REVIEW signals are informational only and do not automatically propose intents.",
            "Operator may review the signal and create a separate Track B intent later.",
            False,
            (),
        )
    if signal.decision_style == SignalDecisionStyle.SCORED_DYNAMIC.value:
        return (
            IntentProposalVerdict.BLOCKED_DYNAMIC_SCORING_NOT_IMPLEMENTED,
            None,
            "SCORED_DYNAMIC signal-to-intent proposal is not implemented.",
            "Use BINARY or SCORED_STATIC policy for no-submit proposal review, or keep the signal review-only.",
            False,
            ("Policy marked dynamic review-only; automatic intent proposal remains blocked.",)
            if policy.allow_dynamic_scoring_for_review_only
            else (),
        )
    order_error = _order_defaults_error(policy)
    if order_error is not None:
        return (
            IntentProposalVerdict.BLOCKED_BINARY_POLICY_MISSING_ORDER_FIELDS,
            None,
            order_error,
            "Provide quantity, order_type, time_in_force, and LMT limit_price in the proposal policy.",
            False,
            (),
        )
    if signal.decision_style == SignalDecisionStyle.SCORED_STATIC.value:
        scoring_error = _static_scoring_policy_error(signal, policy)
        if scoring_error == "scoring_required":
            return (
                IntentProposalVerdict.BLOCKED_SCORING_REQUIRED,
                None,
                "SCORED_STATIC proposal requires signal scoring and explicit static score thresholds.",
                "Provide informational scoring and policy thresholds before proposal.",
                False,
                (),
            )
        if scoring_error is not None:
            return (
                IntentProposalVerdict.BLOCKED_STATIC_SCORE_BELOW_THRESHOLD,
                None,
                scoring_error,
                "Keep signal as review-only or adjust explicit static thresholds after governance review.",
                False,
                (),
            )
    proposed_intent = _proposed_intent(signal=signal, policy=policy, now=now)
    StrategyTradeIntent.from_mapping(proposed_intent)
    return (
        IntentProposalVerdict.CREATED_FOR_REVIEW,
        proposed_intent,
        None,
        "Proposed intent is ready for no-submit Track B review. Lane registry, order plan, readiness, broker, timing, and proof gates still apply.",
        signal.decision_style == SignalDecisionStyle.SCORED_STATIC.value,
        (),
    )


def _report(
    *,
    verdict: IntentProposalVerdict,
    signal_result: ShadowSignalValidationResult | None,
    policy: SignalIntentProposalPolicy,
    proposed_intent: dict[str, Any] | None,
    primary_blocker: str | None,
    required_next_action: str,
    scoring_passed_policy: bool,
    secondary_blockers: tuple[str, ...],
    generated_at: datetime,
    report_json: Path,
) -> dict[str, Any]:
    signal_report = {} if signal_result is None else signal_result.report
    report = {
        "schema_version": "track_b_signal_intent_proposal_v1",
        "generated_at": generated_at.isoformat(),
        "intent_proposal_verdict": verdict.value,
        "intent_proposal_created": proposed_intent is not None,
        "proposed_intent": proposed_intent,
        "signal_validation_verdict": None if signal_result is None else signal_result.verdict.value,
        "signal_validation_report_json": None if signal_result is None else str(signal_result.report_json),
        "decision_style": signal_report.get("decision_style"),
        "scoring_present": bool(signal_report.get("scoring_present", False)),
        "scoring_passed_policy": scoring_passed_policy,
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(secondary_blockers) + list(signal_report.get("secondary_blockers") or ()),
        "required_next_action": required_next_action,
        "signal_id": signal_report.get("signal_id"),
        "strategy_id": signal_report.get("strategy_id"),
        "lane_id": signal_report.get("lane_id"),
        "account_id": signal_report.get("account_id"),
        "local_execution_contract_key": signal_report.get("local_execution_contract_key"),
        "signal_direction": signal_report.get("signal_direction"),
        "quantity": policy.quantity,
        "order_type": policy.order_type,
        "limit_price": policy.limit_price,
        "limit_price_source": policy.limit_price_source,
        "time_in_force": policy.time_in_force,
        "min_signal_score": policy.min_signal_score,
        "min_expected_value_r": policy.min_expected_value_r,
        "allowed_confidence_levels": list(policy.allowed_confidence_levels),
        "live_money_readiness": False,
        "proposal_is_lane_authorization": False,
        "lane_authorized": False,
        "proposal_is_order_plan": False,
        "order_plan_created": False,
        "proposal_is_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "metadata_is_authoritative": False,
        "scoring_is_execution_authority": False,
        "local_execution_contract_key_is_execution_authority": True,
        "manifest_registry_readiness_gates_bypassed": False,
        "paper_proof_cli_remains_only_submit_path": True,
        "report_json_path": str(report_json),
    }
    return report


def _proposed_intent(*, signal: ShadowSignal, policy: SignalIntentProposalPolicy, now: datetime) -> dict[str, Any]:
    return {
        "strategy_id": signal.strategy_id,
        "lane_id": signal.lane_id,
        "mode": "PAPER",
        "account_id": signal.account_id,
        "instrument_family": signal.instrument_family or "",
        "local_execution_contract_key": signal.local_execution_contract_key,
        "side": _side_from_direction(signal.signal_direction),
        "quantity": policy.quantity,
        "order_type": policy.order_type,
        "limit_price": policy.limit_price,
        "time_in_force": policy.time_in_force,
        "signal_timestamp": signal.signal_timestamp.isoformat() if signal.signal_timestamp is not None else None,
        "decision_timestamp": now.isoformat(),
        "reason": f"Proposed from shadow signal {signal.signal_id or signal.signal_type}: {signal.reason}",
        "source": policy.source,
        "submit_requested": False,
        "live_money_readiness": False,
    }


def _side_from_direction(direction: str) -> str:
    if direction == "LONG":
        return "BUY"
    if direction == "SHORT":
        return "SELL"
    raise ValueError("Only LONG or SHORT signals can propose strategy intents.")


def _order_defaults_error(policy: SignalIntentProposalPolicy) -> str | None:
    if policy.quantity is None:
        return "Proposal policy quantity is required."
    if policy.order_type is None:
        return "Proposal policy order_type is required."
    if policy.time_in_force is None:
        return "Proposal policy time_in_force is required."
    if policy.order_type != "LMT":
        return "Proposal policy order_type must be LMT."
    if policy.time_in_force != "DAY":
        return "Proposal policy time_in_force must be DAY."
    if _decimal(policy.quantity, "quantity") <= 0:
        return "Proposal policy quantity must be positive."
    if policy.limit_price is None:
        return "Proposal policy limit_price is required for LMT proposal."
    if _decimal(policy.limit_price, "limit_price") <= 0:
        return "Proposal policy limit_price must be positive."
    return None


def _static_scoring_policy_error(signal: ShadowSignal, policy: SignalIntentProposalPolicy) -> str | None:
    if signal.scoring is None or (policy.min_signal_score is None and policy.min_expected_value_r is None and not policy.allowed_confidence_levels):
        return "scoring_required"
    if policy.min_signal_score is not None:
        if signal.scoring.signal_score is None:
            return "signal_score is required by static proposal policy."
        if _decimal(signal.scoring.signal_score, "signal_score") < _decimal(policy.min_signal_score, "min_signal_score"):
            return "signal_score is below the static proposal policy threshold."
    if policy.min_expected_value_r is not None:
        if signal.scoring.expected_value_r is None:
            return "expected_value_r is required by static proposal policy."
        if _decimal(signal.scoring.expected_value_r, "expected_value_r") < _decimal(policy.min_expected_value_r, "min_expected_value_r"):
            return "expected_value_r is below the static proposal policy threshold."
    if policy.allowed_confidence_levels and str(signal.scoring.confidence_level or "").upper() not in policy.allowed_confidence_levels:
        return "confidence_level is not allowed by static proposal policy."
    return None


def _decimal(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible.") from exc


def _optional_str(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _optional_upper(value: object) -> str | None:
    normalized = _optional_str(value)
    return None if normalized is None else normalized.upper()


def _upper_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        candidates: Sequence[object] = (value,)
    elif isinstance(value, Sequence):
        candidates = value
    else:
        raise ValueError("allowed_confidence_levels must be a string or list.")
    return tuple(str(item).strip().upper() for item in candidates if str(item or "").strip())


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
