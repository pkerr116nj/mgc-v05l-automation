"""Reusable Track B PAPER exit execution pricing policy."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
    managed_close_limit_from_reference,
)


EXIT_CLASS_ALPHA_SEEKING = "EXIT_CLASS_ALPHA_SEEKING"
EXIT_CLASS_RISK_REDUCING = "EXIT_CLASS_RISK_REDUCING"

PAPER_AGGRESSIVE_CLOSE_FALLBACK_MIN_TICKS = 400
PAPER_AGGRESSIVE_CLOSE_FALLBACK_PERCENT = Decimal("0.02")
PAPER_RISK_REDUCING_CLOSE_MAX_DEVIATION_PERCENT = Decimal("0.005")

_RISK_REDUCING_TOKENS = (
    "TIMEBOX",
    "STALE",
    "CLEANUP",
    "RECONCILIATION",
    "ADOPTION",
    "REPAIR",
    "MAINTENANCE",
    "RISK_REDUCING",
    "RISK-REDUCING",
)


def classify_exit_execution(
    *,
    execution_class: Any = None,
    exit_type: Any = None,
    reason: Any = None,
    policy_id: Any = None,
) -> str:
    normalized = str(execution_class or "").strip().upper()
    if normalized in {EXIT_CLASS_ALPHA_SEEKING, EXIT_CLASS_RISK_REDUCING}:
        return normalized
    haystack = " ".join(str(value or "").upper() for value in (exit_type, reason, policy_id))
    if any(token in haystack for token in _RISK_REDUCING_TOKENS):
        return EXIT_CLASS_RISK_REDUCING
    return EXIT_CLASS_ALPHA_SEEKING


def build_exit_limit_policy(
    *,
    reference: Mapping[str, Any],
    close_action: str,
    tick_size: str,
    stale_reference_seconds: int,
    execution_class: str = EXIT_CLASS_RISK_REDUCING,
    base_offset_ticks: int = ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
    max_slippage_ticks: int | None = ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
    reprice_attempts: int = 0,
    reprice_escalation_ticks: int = 0,
) -> dict[str, Any]:
    normalized_class = classify_exit_execution(execution_class=execution_class)
    reference_blocker = _reference_blocker(reference)
    if reference_blocker:
        return _pricing_block(
            blocker=reference_blocker,
            reference=reference,
            action=str(close_action or "").strip().upper(),
            age_seconds=_float_or_none(
                reference.get("reference_age_seconds")
                or reference.get("pricing_reference_age_seconds")
                or reference.get("age_seconds")
            ),
            tick_size=tick_size,
            reprice_attempts=reprice_attempts,
        )
    if normalized_class == EXIT_CLASS_RISK_REDUCING:
        return _risk_reducing_exit_limit_policy(
            reference=reference,
            close_action=close_action,
            tick_size=tick_size,
            stale_reference_seconds=stale_reference_seconds,
            base_offset_ticks=base_offset_ticks,
            max_slippage_ticks=max_slippage_ticks,
            reprice_attempts=reprice_attempts,
        )
    result = managed_close_limit_from_reference(
        reference_price=reference.get("reference_price"),
        close_action=str(close_action or "").strip().upper(),
        tick_size=tick_size,
        base_offset_ticks=base_offset_ticks,
        max_slippage_ticks=max_slippage_ticks,
        reprice_attempts=reprice_attempts,
        reprice_escalation_ticks=reprice_escalation_ticks,
        reference_age_seconds=_float_or_none(
            reference.get("reference_age_seconds")
            or reference.get("pricing_reference_age_seconds")
            or reference.get("age_seconds")
        ),
        stale_reference_seconds=stale_reference_seconds,
    )
    return {
        **result,
        "execution_class": EXIT_CLASS_ALPHA_SEEKING,
        "marketable_execution_required": False,
        "passive_execution_allowed": True,
        "reference_source": reference.get("reference_source"),
        "reference_source_type": reference.get("reference_source_type"),
        "pricing_source": reference.get("pricing_source"),
        "bar_end": reference.get("bar_end"),
        "generated_at": reference.get("generated_at"),
    }


def _risk_reducing_exit_limit_policy(
    *,
    reference: Mapping[str, Any],
    close_action: str,
    tick_size: str,
    stale_reference_seconds: int,
    base_offset_ticks: int,
    max_slippage_ticks: int | None,
    reprice_attempts: int,
) -> dict[str, Any]:
    age_seconds = _float_or_none(
        reference.get("reference_age_seconds")
        or reference.get("pricing_reference_age_seconds")
        or reference.get("age_seconds")
    )
    action = str(close_action or "").strip().upper()
    tick = _decimal_or_none(tick_size)
    if action not in {"BUY", "SELL"} or tick is None or tick <= Decimal("0"):
        return _pricing_block(
            blocker="MANAGED_CLOSE_ACTION_UNSUPPORTED",
            reference=reference,
            action=action,
            age_seconds=age_seconds,
            tick_size=tick_size,
            reprice_attempts=reprice_attempts,
        )
    if age_seconds is not None and age_seconds > float(stale_reference_seconds):
        return _pricing_block(
            blocker="MANAGED_CLOSE_REFERENCE_STALE",
            reference=reference,
            action=action,
            age_seconds=age_seconds,
            tick_size=tick_size,
            reprice_attempts=reprice_attempts,
        )

    preferred_key = "ask_price" if action == "BUY" else "bid_price"
    preferred = _decimal_or_none(reference.get(preferred_key))
    if preferred is not None:
        return _priced_risk_reducing_close(
            action=action,
            reference_price=preferred,
            reference_kind=preferred_key,
            tick=tick,
            offset_ticks=Decimal("0"),
            max_deviation_ticks=Decimal(max(int(max_slippage_ticks), 0)) if max_slippage_ticks is not None else Decimal("0"),
            age_seconds=age_seconds,
            reference=reference,
            reprice_attempts=reprice_attempts,
        )

    last_price = _decimal_or_none(reference.get("last_price"))
    close = _decimal_or_none(reference.get("close"))
    fallback = last_price or close
    if fallback is None:
        return _pricing_block(
            blocker="MANAGED_CLOSE_REFERENCE_MISSING",
            reference=reference,
            action=action,
            age_seconds=age_seconds,
            tick_size=tick_size,
            reprice_attempts=reprice_attempts,
        )
    offset_ticks = Decimal(max(int(base_offset_ticks), 0))
    if max_slippage_ticks is not None:
        offset_ticks = min(offset_ticks, Decimal(max(int(max_slippage_ticks), 0)))
    return _priced_risk_reducing_close(
        action=action,
        reference_price=fallback,
        reference_kind="last_price" if last_price is not None else "close",
        tick=tick,
        offset_ticks=offset_ticks,
        max_deviation_ticks=Decimal(max(int(max_slippage_ticks), 0)) if max_slippage_ticks is not None else offset_ticks,
        age_seconds=age_seconds,
        reference=reference,
        reprice_attempts=reprice_attempts,
    )


def _priced_risk_reducing_close(
    *,
    action: str,
    reference_price: Decimal,
    reference_kind: str,
    tick: Decimal,
    offset_ticks: Decimal,
    max_deviation_ticks: Decimal,
    age_seconds: float | None,
    reference: Mapping[str, Any],
    reprice_attempts: int,
) -> dict[str, Any]:
    offset = tick * offset_ticks
    raw = reference_price + offset if action == "BUY" else reference_price - offset
    rounded = (raw / tick).to_integral_value() * tick
    deviation = abs(rounded - reference_price)
    tick_tolerance = tick * max_deviation_ticks
    percent_tolerance = abs(reference_price) * PAPER_RISK_REDUCING_CLOSE_MAX_DEVIATION_PERCENT
    max_deviation = min(tick_tolerance, percent_tolerance) if percent_tolerance > 0 else tick_tolerance
    if deviation > max_deviation:
        return _pricing_block(
            blocker="MANAGED_CLOSE_PRICE_SANITY_DEVIATION",
            reference=reference,
            action=action,
            age_seconds=age_seconds,
            tick_size=str(tick),
            reprice_attempts=reprice_attempts,
        )
    return {
        "classification": "MANAGED_CLOSE_PRICED",
        "execution_class": EXIT_CLASS_RISK_REDUCING,
        "marketable_execution_required": True,
        "passive_execution_allowed": False,
        "limit_price": format(rounded.normalize(), "f"),
        "close_action": action,
        "reference_price": format(reference_price.normalize(), "f"),
        "reference_price_kind": reference_kind,
        "reference_age_seconds": age_seconds,
        "reference_source": reference.get("reference_source"),
        "reference_source_type": reference.get("reference_source_type"),
        "pricing_source": reference.get("pricing_source"),
        "timeframe": reference.get("timeframe"),
        "bar_end": reference.get("bar_end"),
        "generated_at": reference.get("generated_at"),
        "marketable_limit_offset_ticks": float(offset_ticks),
        "price_deviation_from_reference": format(deviation.normalize(), "f"),
        "max_price_deviation_from_reference": format(max_deviation.normalize(), "f"),
        "price_sanity_tolerance_ticks": float(max_deviation_ticks),
        "price_sanity_tolerance_percent": float(PAPER_RISK_REDUCING_CLOSE_MAX_DEVIATION_PERCENT),
        "aggressive_paper_fallback": reference_kind in {"last_price", "close"},
        "aggressive_paper_fallback_percent": float(PAPER_AGGRESSIVE_CLOSE_FALLBACK_PERCENT),
        "reprice_attempts": max(int(reprice_attempts), 0),
        "stale_reference_blocker": None,
    }


def _pricing_block(
    *,
    blocker: str,
    reference: Mapping[str, Any],
    action: str,
    age_seconds: float | None,
    tick_size: str,
    reprice_attempts: int,
) -> dict[str, Any]:
    return {
        "classification": "MANAGED_CLOSE_PRICING_BLOCKED",
        "execution_class": EXIT_CLASS_RISK_REDUCING,
        "marketable_execution_required": True,
        "passive_execution_allowed": False,
        "limit_price": None,
        "close_action": action,
        "reference_price": reference.get("reference_price"),
        "reference_age_seconds": age_seconds,
        "reference_source": reference.get("reference_source"),
        "reference_source_type": reference.get("reference_source_type"),
        "pricing_source": reference.get("pricing_source"),
        "timeframe": reference.get("timeframe"),
        "bar_end": reference.get("bar_end"),
        "generated_at": reference.get("generated_at"),
        "tick_size": tick_size,
        "marketable_limit_offset_ticks": None,
        "price_deviation_from_reference": None,
        "max_price_deviation_from_reference": None,
        "price_sanity_tolerance_ticks": None,
        "price_sanity_tolerance_percent": float(PAPER_RISK_REDUCING_CLOSE_MAX_DEVIATION_PERCENT),
        "aggressive_paper_fallback": False,
        "reprice_attempts": max(int(reprice_attempts), 0),
        "stale_reference_blocker": blocker,
        "block_reason": blocker,
    }


def _reference_blocker(reference: Mapping[str, Any]) -> str | None:
    classification = str(reference.get("classification") or "").strip().upper()
    if classification in {"RUNTIME_MARKET_REFERENCE_WRONG_SYMBOL", "RUNTIME_MARKET_REFERENCE_WRONG_CONTRACT"}:
        return "MANAGED_CLOSE_REFERENCE_WRONG_SYMBOL"
    if classification == "RUNTIME_MARKET_REFERENCE_STALE":
        return "MANAGED_CLOSE_REFERENCE_STALE"
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
