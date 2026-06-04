"""Risk-reducing close authority for stale-runtime Track B PAPER states.

This module is deliberately read-only.  It does not submit, cancel, flatten, or
mutate lifecycle state.  It classifies whether a fresh broker-backed managed
position that is already exit-due still has an exact scoped close path when
normal runtime submit authority is blocked by stale/down runtime truth.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_fresh_truth_contract import (
    RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE,
)


RISK_REDUCING_CLOSE_NOT_REQUIRED = "RISK_REDUCING_CLOSE_NOT_REQUIRED"
RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE = (
    "RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE"
)
RISK_REDUCING_CLOSE_BLOCKED_GUARDIAN_NOT_READY = "RISK_REDUCING_CLOSE_BLOCKED_GUARDIAN_NOT_READY"
RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS = "RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS"
RISK_REDUCING_CLOSE_BLOCKED_CONFLICTING_OPEN_ORDER = "RISK_REDUCING_CLOSE_BLOCKED_CONFLICTING_OPEN_ORDER"
RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE = "RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE"
RISK_REDUCING_CLOSE_BLOCKED_NOT_BROKER_BACKED = "RISK_REDUCING_CLOSE_BLOCKED_NOT_BROKER_BACKED"
RISK_REDUCING_CLOSE_BLOCKED_WOULD_INCREASE_OR_FLIP_EXPOSURE = (
    "RISK_REDUCING_CLOSE_BLOCKED_WOULD_INCREASE_OR_FLIP_EXPOSURE"
)


def classify_runtime_stale_risk_reducing_close(
    *,
    control_plane_snapshot: Mapping[str, Any],
    guardian: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a central authority verdict for stale-runtime managed closes."""

    if not _runtime_stale_with_broker_exposure(control_plane_snapshot):
        return _decision(
            classification=RISK_REDUCING_CLOSE_NOT_REQUIRED,
            allowed=False,
            reason_codes=["RUNTIME_AUTHORITY_NOT_STALE_WITH_BROKER_EXPOSURE"],
            runtime_authority_stale_with_broker_exposure=False,
        )

    if _owner_ambiguous(guardian, managed_position_registry, managed_order_registry):
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS,
            allowed=False,
            reason_codes=["CANONICAL_OWNER_AMBIGUOUS"],
        )

    if _has_conflicting_open_order(open_order_truth, managed_order_registry):
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_CONFLICTING_OPEN_ORDER,
            allowed=False,
            reason_codes=["CONFLICTING_OPEN_ORDER"],
        )

    positions = _exit_due_positions(managed_position_registry)
    if not positions:
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE,
            allowed=False,
            reason_codes=["NO_OPEN_MANAGED_EXIT_DUE_POSITION"],
        )
    if len(positions) != 1:
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS,
            allowed=False,
            reason_codes=["MULTIPLE_EXIT_DUE_POSITIONS"],
            managed_positions=positions,
        )
    position = positions[0]
    if not _broker_backed(position):
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_NOT_BROKER_BACKED,
            allowed=False,
            reason_codes=["BROKER_BACKED_ENTRY_EVIDENCE_MISSING"],
            managed_positions=[position],
        )

    guardian_close = _mapping(guardian.get("managed_close_authority"))
    candidates = [_mapping(candidate) for candidate in _list(guardian_close.get("candidates"))]
    if guardian_close.get("allowed") is not True or not candidates:
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_GUARDIAN_NOT_READY,
            allowed=False,
            reason_codes=list(guardian_close.get("reason_codes") or ["GUARDIAN_CLOSE_NOT_ALLOWED"]),
            managed_positions=[position],
            guardian_close_candidates=candidates,
        )
    matching = [candidate for candidate in candidates if _same_owner_and_contract(position=position, candidate=candidate)]
    if len(matching) != 1:
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS,
            allowed=False,
            reason_codes=["GUARDIAN_CANDIDATE_OWNER_MATCH_AMBIGUOUS" if matching else "GUARDIAN_CANDIDATE_OWNER_NOT_FOUND"],
            managed_positions=[position],
            guardian_close_candidates=candidates,
        )
    candidate = matching[0]
    risk_reducing_reasons = _risk_reducing_mismatch_reasons(position=position, candidate=candidate)
    if risk_reducing_reasons:
        return _decision(
            classification=RISK_REDUCING_CLOSE_BLOCKED_WOULD_INCREASE_OR_FLIP_EXPOSURE,
            allowed=False,
            reason_codes=risk_reducing_reasons,
            managed_positions=[position],
            guardian_close_candidates=[candidate],
        )

    return _decision(
        classification=RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE,
        allowed=True,
        reason_codes=[],
        managed_positions=[position],
        guardian_close_candidates=[candidate],
        close_candidate=_close_candidate(candidate),
    )


def _decision(
    *,
    classification: str,
    allowed: bool,
    reason_codes: Sequence[str],
    runtime_authority_stale_with_broker_exposure: bool = True,
    managed_positions: Sequence[Mapping[str, Any]] | None = None,
    guardian_close_candidates: Sequence[Mapping[str, Any]] | None = None,
    close_candidate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "allowed": allowed,
        "runtime_authority_stale_with_broker_exposure": runtime_authority_stale_with_broker_exposure,
        "entry_submit_allowed": False,
        "normal_runtime_submit_authority_required": False,
        "risk_reducing_only": allowed,
        "requires_exact_scope": True,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "reason_codes": list(dict.fromkeys(str(item) for item in reason_codes if str(item or ""))),
        "managed_positions": [dict(item) for item in managed_positions or []],
        "guardian_close_candidates": [dict(item) for item in guardian_close_candidates or []],
        "close_candidate": dict(close_candidate or {}),
    }


def _runtime_stale_with_broker_exposure(snapshot: Mapping[str, Any]) -> bool:
    return (
        snapshot.get("runtime_authority_stale_with_broker_exposure") is True
        or str(snapshot.get("runtime_authority_exposure_classification") or "")
        == RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE
    )


def _owner_ambiguous(*artifacts: Mapping[str, Any]) -> bool:
    ambiguous_tokens = {"AMBIGUOUS_EXPOSURE_OWNERSHIP", "PROJECTION_AUTHORITY_DIVERGENCE"}
    for artifact in artifacts:
        current_values = [
            artifact.get("classification"),
            artifact.get("owner_resolution_classification"),
            artifact.get("current_exposure_owner_classification"),
            artifact.get("projection_authority_classification"),
            artifact.get("current_scope_classification"),
        ]
        current_values.extend(_list(artifact.get("reason_codes")))
        for row in _list(artifact.get("managed_positions")) + _list(artifact.get("positions")):
            mapped = _mapping(row)
            current_values.extend(
                [
                    mapped.get("classification"),
                    mapped.get("owner_resolution_classification"),
                    mapped.get("current_exposure_owner_classification"),
                    mapped.get("projection_authority_classification"),
                ]
            )
            current_values.extend(_list(mapped.get("reason_codes")))
        guardian_close = _mapping(artifact.get("managed_close_authority"))
        current_values.append(guardian_close.get("classification"))
        current_values.extend(_list(guardian_close.get("reason_codes")))
        if any(str(value or "") in ambiguous_tokens for value in current_values):
            return True
    return False


def _has_conflicting_open_order(
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
) -> bool:
    for row in _list(open_order_truth.get("open_orders")) + _list(open_order_truth.get("order_states")):
        if _mapping(row):
            return True
    for row in _list(managed_order_registry.get("managed_orders")):
        order = _mapping(row)
        if order.get("working") is True or order.get("broker_order_id") or order.get("client_id"):
            return True
    return False


def _exit_due_positions(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _list(registry.get("managed_positions")) or _list(registry.get("positions"))
    return [
        dict(row)
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("classification") or row.get("state") or "").upper() == "OPEN_MANAGED_EXIT_DUE"
    ]


def _broker_backed(position: Mapping[str, Any]) -> bool:
    if position.get("broker_backed_entry") is True:
        return True
    lifecycle = _mapping(position.get("lifecycle_position"))
    broker_identity = _mapping(lifecycle.get("entry_broker_identity"))
    units = [_mapping(unit) for unit in _list(lifecycle.get("lifecycle_units"))]
    entry_exec = _first_text(
        position.get("entry_exec_id"),
        lifecycle.get("entry_exec_id"),
        broker_identity.get("exec_id"),
        *[unit.get("entry_exec_id") for unit in units],
    )
    entry_perm = _first_text(
        position.get("entry_perm_id"),
        lifecycle.get("entry_perm_id"),
        broker_identity.get("perm_id"),
        *[unit.get("entry_perm_id") for unit in units],
    )
    return bool(entry_exec and entry_perm)


def _same_owner_and_contract(*, position: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    if _text(position.get("trade_id")) and _text(candidate.get("trade_id")):
        if _text(position.get("trade_id")) != _text(candidate.get("trade_id")):
            return False
    if _text(position.get("lifecycle_id")) and _text(candidate.get("lifecycle_id")):
        if _text(position.get("lifecycle_id")) != _text(candidate.get("lifecycle_id")):
            return False
    return _same_contract(position=position, candidate=candidate)


def _same_contract(*, position: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    pos_broker = _mapping(position.get("broker_position"))
    pos_account = _first_text(position.get("account_id"), pos_broker.get("account_id"))
    cand_account = _first_text(candidate.get("account_id"), candidate.get("account"))
    if pos_account and cand_account and pos_account != cand_account:
        return False
    pos_con = _first_text(position.get("con_id"), position.get("conId"), pos_broker.get("con_id"), pos_broker.get("conId"))
    cand_con = _first_text(candidate.get("con_id"), candidate.get("conId"))
    if pos_con and cand_con and pos_con != cand_con:
        return False
    pos_local = _first_text(position.get("local_symbol"), position.get("localSymbol"), pos_broker.get("local_symbol"))
    cand_local = _first_text(candidate.get("local_symbol"), candidate.get("localSymbol"))
    if pos_local and cand_local and pos_local != cand_local:
        return False
    return bool((pos_con and cand_con) or (pos_local and cand_local))


def _risk_reducing_mismatch_reasons(*, position: Mapping[str, Any], candidate: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    broker_position = _mapping(position.get("broker_position"))
    broker_qty = _decimal(
        broker_position.get("quantity")
        or position.get("signed_broker_qty")
        or position.get("aggregate_qty")
        or position.get("quantity")
    )
    candidate_qty = _decimal(candidate.get("quantity") or candidate.get("qty"))
    action = str(candidate.get("action") or "").upper()
    if broker_qty == Decimal("0"):
        reasons.append("BROKER_POSITION_FLAT")
    if candidate_qty <= Decimal("0"):
        reasons.append("CLOSE_QUANTITY_MISSING")
    if abs(candidate_qty) != abs(broker_qty):
        reasons.append("CLOSE_QUANTITY_NOT_EXACT_BROKER_QTY")
    expected_action = "BUY" if broker_qty < 0 else "SELL"
    if action != expected_action:
        reasons.append("CLOSE_ACTION_NOT_RISK_REDUCING")
    if not _same_contract(position=position, candidate=candidate):
        reasons.append("CLOSE_CONTRACT_SCOPE_MISMATCH")
    return reasons


def _close_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": candidate.get("trade_id"),
        "lifecycle_id": candidate.get("lifecycle_id"),
        "account_id": candidate.get("account_id") or candidate.get("account"),
        "symbol": candidate.get("symbol"),
        "local_symbol": candidate.get("local_symbol") or candidate.get("localSymbol"),
        "con_id": candidate.get("con_id") or candidate.get("conId"),
        "action": str(candidate.get("action") or "").upper(),
        "quantity": str(candidate.get("quantity") or candidate.get("qty") or ""),
        "classification": RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
