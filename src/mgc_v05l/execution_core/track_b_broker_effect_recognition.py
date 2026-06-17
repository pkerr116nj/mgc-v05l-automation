"""Shared broker-effect recognition for Track B managed closes.

This module is intentionally pure: callers must refresh broker/order truth
before invoking it. The result decides whether a retry would still reduce the
original exposure or would risk flipping an already-reduced position.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT = "BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT"
BROKER_EFFECT_OBSERVED = "BROKER_EFFECT_OBSERVED"
BROKER_EFFECT_BLOCKED = "BROKER_EFFECT_BLOCKED"


def recognize_managed_close_broker_effect(
    *,
    expected_signed_quantity: Any,
    observed_signed_quantity: Any,
    close_action: str,
    close_quantity: Any,
    broker_truth_complete: bool = True,
    identity_resolved: bool = True,
    conflicting_order_count: int = 0,
    unknown_order_count: int = 0,
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
) -> dict[str, Any]:
    expected = _decimal_or_none(expected_signed_quantity)
    observed = _decimal_or_none(observed_signed_quantity)
    close_qty = _decimal_or_none(close_quantity)
    action = str(close_action or "").upper()
    blockers: list[str] = []
    if broker_truth_complete is not True:
        blockers.append("broker_truth_incomplete")
    if identity_resolved is not True:
        blockers.append("broker_identity_unresolved")
    if unknown_order_count > 0:
        blockers.append("unknown_orders_present")
    if conflicting_order_count > 0:
        blockers.append("conflicting_orders_present")
    if live_money_eligible:
        blockers.append("live_money_eligible")
    if paper_proof_invoked:
        blockers.append("paper_proof_invoked")
    if expected is None or expected == 0:
        blockers.append("expected_position_quantity_invalid")
    if observed is None:
        blockers.append("observed_position_quantity_unavailable")
    if close_qty is None or close_qty <= 0:
        blockers.append("close_quantity_invalid")
    if action not in {"BUY", "SELL"}:
        blockers.append("close_action_invalid")
    if blockers:
        return _result(
            classification=BROKER_EFFECT_BLOCKED,
            effect_state="BLOCKED_HARD_INVARIANT",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=blockers,
        )

    assert expected is not None
    assert observed is not None
    assert close_qty is not None
    expected_side = Decimal("1") if expected > 0 else Decimal("-1")
    expected_action = "SELL" if expected_side > 0 else "BUY"
    if action != expected_action:
        return _result(
            classification=BROKER_EFFECT_BLOCKED,
            effect_state="CLOSE_ACTION_NOT_RISK_REDUCING",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=["close_action_not_risk_reducing"],
        )
    same_side = (observed > 0 and expected > 0) or (observed < 0 and expected < 0)
    if close_qty > abs(expected):
        return _result(
            classification=BROKER_EFFECT_BLOCKED,
            effect_state="CLOSE_QUANTITY_EXCEEDS_EXPECTED_EXPOSURE",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=["close_quantity_exceeds_expected_exposure"],
        )
    if observed == expected:
        return _result(
            classification=BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT,
            effect_state="ORIGINAL_EXPOSURE_PRESENT",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=[],
        )
    if observed == 0:
        return _result(
            classification=BROKER_EFFECT_OBSERVED,
            effect_state="EXPOSURE_FLAT",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=[],
        )
    if not same_side:
        return _result(
            classification=BROKER_EFFECT_OBSERVED,
            effect_state="EXPOSURE_OPPOSITE",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=[],
        )
    if close_qty > abs(observed):
        return _result(
            classification=BROKER_EFFECT_BLOCKED,
            effect_state="CLOSE_QUANTITY_EXCEEDS_OBSERVED_EXPOSURE",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=["close_quantity_exceeds_observed_exposure"],
        )
    if same_side and abs(observed) > abs(expected):
        return _result(
            classification=BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT,
            effect_state="SAME_SIDE_EXCESS_EXPOSURE_PRESENT",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=[],
        )
    if abs(observed) < abs(expected):
        return _result(
            classification=BROKER_EFFECT_OBSERVED,
            effect_state="EXPOSURE_REDUCED",
            expected=expected,
            observed=observed,
            close_qty=close_qty,
            close_action=action,
            blockers=[],
        )
    return _result(
        classification=BROKER_EFFECT_BLOCKED,
        effect_state="OBSERVED_EXPOSURE_EXCEEDS_EXPECTED",
        expected=expected,
        observed=observed,
        close_qty=close_qty,
        close_action=action,
        blockers=["observed_exposure_exceeds_expected"],
    )


def broker_effect_observed(payload: Mapping[str, Any] | None) -> bool:
    return isinstance(payload, Mapping) and payload.get("classification") == BROKER_EFFECT_OBSERVED


def _result(
    *,
    classification: str,
    effect_state: str,
    expected: Decimal | None,
    observed: Decimal | None,
    close_qty: Decimal | None,
    close_action: str,
    blockers: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_broker_effect_recognition_v1",
        "classification": classification,
        "effect_state": effect_state,
        "expected_signed_quantity": None if expected is None else str(expected),
        "observed_signed_quantity": None if observed is None else str(observed),
        "close_quantity": None if close_qty is None else str(close_qty),
        "close_action": close_action,
        "blockers": blockers,
        "retry_close_allowed": classification == BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT,
        "stop_without_submit": classification == BROKER_EFFECT_OBSERVED,
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
