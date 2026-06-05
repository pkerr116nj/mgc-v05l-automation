"""Read-only Track B PAPER managed-exit recovery planner.

This planner is a backstop for broker-backed managed positions that are already
exit-due but have no working managed close order. It never submits, cancels, or
flattens; it only reports exact scoped close candidates that are already allowed
by Guardian/Safe-State authority.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_position_guardian import DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_session_authority import DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import DEFAULT_REPORT_PATH as DEFAULT_RECONCILIATION_REPORT_PATH
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANAGED_EXIT_RECOVERY_PLAN = (
    Path("outputs") / "track_b_execution_core" / "managed_exit_recovery" / "latest_managed_exit_recovery_plan.json"
)

NO_EXIT_DUE_POSITIONS = "NO_EXIT_DUE_POSITIONS"
EXIT_DUE_CLOSE_READY = "EXIT_DUE_CLOSE_READY"
EXIT_DUE_CLOSE_BLOCKED = "EXIT_DUE_CLOSE_BLOCKED"
EXIT_DUE_CLOSE_PARTIAL_READY = "EXIT_DUE_CLOSE_PARTIAL_READY"

READY_POSITION = "EXIT_DUE_POSITION_CLOSE_READY"
BLOCKED_POSITION = "EXIT_DUE_POSITION_CLOSE_BLOCKED"


@dataclass(frozen=True)
class TrackBManagedExitRecoveryConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_MANAGED_EXIT_RECOVERY_PLAN
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    broker_position_guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    safe_state_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_REPORT_PATH
    broker_truth_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_exit_recovery_plan(
    *,
    config: TrackBManagedExitRecoveryConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    inputs = _inputs(config=config, overrides=input_overrides or {})
    managed_positions = [_mapping(row) for row in _list(inputs["managed_positions"].get("managed_positions"))]
    exit_due_positions = [
        row
        for row in managed_positions
        if str(row.get("classification") or "") == "OPEN_MANAGED_EXIT_DUE" or row.get("exit_due") is True
    ]
    per_position = [
        _classify_position(position=position, positions=managed_positions, inputs=inputs)
        for position in exit_due_positions
    ]
    diagnostic_ready = [row for row in per_position if row["diagnostic_close_candidate_ready"] is True]
    ready = [row for row in per_position if row["apply_eligible"] is True]
    blocked = [row for row in per_position if row["apply_eligible"] is not True]

    if not per_position:
        classification = NO_EXIT_DUE_POSITIONS
    elif ready and not blocked:
        classification = EXIT_DUE_CLOSE_READY
    elif ready and blocked:
        classification = EXIT_DUE_CLOSE_PARTIAL_READY
    else:
        classification = EXIT_DUE_CLOSE_BLOCKED

    global_flags = _global_safety_flags(inputs)
    return {
        "schema_version": "track_b_managed_exit_recovery_plan_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "dry_run": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "paper_proof_invoked": global_flags["paper_proof_invoked"],
        "live_money_eligible": global_flags["live_money_eligible"],
        "classification": classification,
        "eligible_count": len(ready),
        "apply_eligible_count": len(ready),
        "blocked_count": len(blocked),
        "exit_due_count": len(per_position),
        "diagnostic_close_candidate_count": len(diagnostic_ready),
        "broker_session_authority_classification": inputs["broker_session_authority"].get("classification"),
        "broker_session_connection_mode": inputs["broker_session_authority"].get("connection_mode"),
        "broker_session_allowed_uses": _mapping(inputs["broker_session_authority"].get("allowed_uses")),
        "broker_session_authority_blockers": _list(inputs["broker_session_authority"].get("authority_blockers")),
        "callback_ownership_attribution": _mapping(inputs["broker_session_authority"].get("callback_ownership_attribution")),
        "managed_exit_recovery_plan": {
            "classification": classification,
            "eligible_positions": ready,
            "blocked_positions": blocked,
            "diagnostic_positions": diagnostic_ready,
            "diagnostic_close_candidates": [row["close_candidate"] for row in diagnostic_ready],
            "close_candidates": [row["close_candidate"] for row in diagnostic_ready],
        },
        "eligible_positions": ready,
        "blocked_positions": blocked,
        "diagnostic_positions": diagnostic_ready,
        "diagnostic_close_candidates": [row["close_candidate"] for row in diagnostic_ready],
        "global_safety_flags": global_flags,
        "source_classifications": {
            "managed_position_registry": inputs["managed_positions"].get("classification"),
            "managed_order_registry": inputs["managed_orders"].get("classification"),
            "open_order_truth": inputs["open_order_truth"].get("classification"),
            "broker_position_guardian": inputs["guardian"].get("classification"),
            "safe_state": inputs["safe_state"].get("classification"),
            "reconciliation": inputs["reconciliation"].get("classification"),
            "registry_reconciliation": _mapping(inputs["reconciliation"].get("registry_reconciliation")).get("classification"),
            "broker_truth_lease": inputs["broker_truth_lease"].get("lease_state"),
            "broker_session_authority": inputs["broker_session_authority"].get("classification"),
        },
        "source_artifact_paths": {
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "broker_position_guardian": str(config.resolve(config.broker_position_guardian_path)),
            "safe_state": str(config.resolve(config.safe_state_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "broker_truth_lease": str(config.resolve(config.broker_truth_lease_path)),
            "broker_session_authority": str(config.resolve(config.broker_session_authority_path)),
        },
    }


def write_track_b_managed_exit_recovery_plan(
    *,
    config: TrackBManagedExitRecoveryConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), dict(payload))


def run_track_b_managed_exit_recovery(
    *,
    config: TrackBManagedExitRecoveryConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_managed_exit_recovery_plan(config=config, now=now)
    if write:
        write_track_b_managed_exit_recovery_plan(config=config, payload=payload)
    return payload


def _classify_position(
    *,
    position: Mapping[str, Any],
    positions: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    broker_position = _mapping(position.get("broker_position"))
    lifecycle_position = _mapping(position.get("lifecycle_position"))
    trade_id = _text(position.get("trade_id") or lifecycle_position.get("trade_id"))
    lifecycle_id = _text(position.get("lifecycle_id") or lifecycle_position.get("lifecycle_id"))
    account_id = _text(position.get("account_id") or broker_position.get("account_id") or lifecycle_position.get("account_id"))
    local_symbol = _text(position.get("local_symbol") or broker_position.get("local_symbol") or lifecycle_position.get("local_symbol"))
    con_id = _text(position.get("con_id") or broker_position.get("con_id") or lifecycle_position.get("con_id"))
    broker_qty = _decimal(broker_position.get("quantity"))
    expected_action = "SELL" if broker_qty > 0 else "BUY" if broker_qty < 0 else ""
    expected_qty = _decimal_display(abs(broker_qty))

    blockers: list[str] = []
    if not broker_position or broker_qty == 0:
        blockers.append("BROKER_POSITION_MISSING")
    if not trade_id:
        blockers.append("TRADE_ID_MISSING")
    if not lifecycle_id:
        blockers.append("LIFECYCLE_ID_MISSING")
    if str(position.get("classification") or "") != "OPEN_MANAGED_EXIT_DUE" and position.get("exit_due") is not True:
        blockers.append("POSITION_NOT_EXIT_DUE")
    if position.get("exit_due") is not True:
        blockers.append("EXIT_DUE_FALSE_OR_MISSING")
    if str(lifecycle_position.get("source") or "") == "AMBIGUOUS":
        blockers.append("OWNERSHIP_AMBIGUOUS")
    if position.get("projection_authority_owner_confirmed") is not True:
        blockers.append("OWNER_PROJECTION_NOT_CONFIRMED")
    if position.get("review_required_position"):
        blockers.append("REVIEW_REQUIRED_POSITION_PRESENT")
    if position.get("duplicate_same_lane_exposure") is True or lifecycle_position.get("duplicate_same_lane_exposure") is True:
        blockers.append("DUPLICATE_OR_COMPETING_EXPOSURE")

    if _matching_position_count(positions=positions, account_id=account_id, local_symbol=local_symbol, con_id=con_id) != 1:
        blockers.append("COMPETING_MANAGED_POSITION_CANDIDATE")

    registry_record = _matching_registry_record(
        reconciliation=inputs["reconciliation"],
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id=account_id,
        local_symbol=local_symbol,
        con_id=con_id,
    )
    if not registry_record:
        blockers.append("REGISTRY_OPEN_MANAGED_RECORD_MISSING")
    elif str(registry_record.get("current_state") or "") != "OPEN_MANAGED":
        blockers.append("REGISTRY_CURRENT_STATE_NOT_OPEN_MANAGED")
    if _registry_competing_count(
        reconciliation=inputs["reconciliation"],
        account_id=account_id,
        local_symbol=local_symbol,
        con_id=con_id,
    ) != 1:
        blockers.append("COMPETING_REGISTRY_CANDIDATE")

    order_conflicts = _conflicting_orders(inputs=inputs, account_id=account_id, local_symbol=local_symbol, con_id=con_id, lifecycle_id=lifecycle_id)
    if order_conflicts:
        blockers.append("BROKER_OPEN_ORDER_CONFLICT")

    managed_order_state = _matching_managed_order_state(
        managed_orders=inputs["managed_orders"],
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id=account_id,
        local_symbol=local_symbol,
        con_id=con_id,
    )
    if not _close_required_state(managed_order_state=managed_order_state, managed_orders=inputs["managed_orders"]):
        blockers.append("MANAGED_ORDER_STATE_NOT_CLOSE_REQUIRED")

    guardian_candidate = _matching_guardian_candidate(
        guardian=inputs["guardian"],
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id=account_id,
        local_symbol=local_symbol,
        con_id=con_id,
        expected_action=expected_action,
        expected_qty=expected_qty,
    )
    if not guardian_candidate:
        blockers.append("GUARDIAN_EXACT_CLOSE_CANDIDATE_MISSING")
    if _mapping(inputs["guardian"].get("managed_close_authority")).get("allowed") is not True:
        blockers.append("GUARDIAN_CLOSE_AUTHORITY_NOT_ALLOWED")

    if not _safe_state_allows_candidate(
        safe_state=inputs["safe_state"],
        candidate=guardian_candidate,
        expected_action=expected_action,
        expected_qty=expected_qty,
    ):
        blockers.append("SAFE_STATE_CLOSE_NOT_ALLOWED")

    if _flag_true(inputs, "live_money_eligible"):
        blockers.append("LIVE_MONEY_ELIGIBLE_TRUE")
    if _flag_true(inputs, "paper_proof_invoked"):
        blockers.append("PAPER_PROOF_INVOKED_TRUE")
    if _bool_any(inputs["guardian"].get("broad_flatten_allowed"), inputs["safe_state"].get("broad_flatten_allowed")):
        blockers.append("BROAD_FLATTEN_AVAILABLE_UNSAFE")
    if _bool_any(inputs["guardian"].get("global_flatten_allowed"), inputs["safe_state"].get("global_flatten_allowed")):
        blockers.append("GLOBAL_FLATTEN_AVAILABLE_UNSAFE")

    candidate = {
        "account_id": account_id,
        "action": expected_action,
        "quantity": expected_qty,
        "local_symbol": local_symbol,
        "con_id": _int_or_text(con_id),
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": position.get("strategy_id") or lifecycle_position.get("strategy_id"),
        "lane_id": position.get("lane_id") or lifecycle_position.get("lane_id"),
        "broker_quantity": _decimal_display(broker_qty),
        "risk_reducing_only": True,
    }
    if guardian_candidate:
        candidate.update({key: guardian_candidate.get(key) for key in ("classification", "reason") if guardian_candidate.get(key) is not None})

    diagnostic_blockers = _dedupe(blockers)
    apply_blockers = _dedupe([*diagnostic_blockers, *_broker_session_apply_blockers(inputs=inputs)])
    diagnostic_ready = not diagnostic_blockers
    apply_eligible = diagnostic_ready and not apply_blockers
    return {
        "classification": READY_POSITION if apply_eligible else BLOCKED_POSITION,
        "eligible": apply_eligible,
        "diagnostic_close_candidate_ready": diagnostic_ready,
        "apply_eligible": apply_eligible,
        "blockers": apply_blockers,
        "diagnostic_blockers": diagnostic_blockers,
        "apply_blockers": apply_blockers,
        "close_candidate": candidate,
        "identity": {
            "account_id": account_id,
            "local_symbol": local_symbol,
            "con_id": _int_or_text(con_id),
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
        },
        "managed_position_classification": position.get("classification"),
        "managed_order_classification": inputs["managed_orders"].get("classification"),
        "bars_since_entry": position.get("bars_since_entry"),
        "exit_due": position.get("exit_due") is True,
        "working_close_qty": str(position.get("working_close_qty") or "0"),
        "broker_position": broker_position,
        "registry_current_state": registry_record.get("current_state") if registry_record else None,
        "order_conflicts": order_conflicts,
        "broker_session_authority_classification": inputs["broker_session_authority"].get("classification"),
        "broker_session_connection_mode": inputs["broker_session_authority"].get("connection_mode"),
        "broker_session_allowed_uses": _mapping(inputs["broker_session_authority"].get("allowed_uses")),
        "callback_ownership_attribution": _mapping(inputs["broker_session_authority"].get("callback_ownership_attribution")),
    }


def _inputs(
    *,
    config: TrackBManagedExitRecoveryConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    paths = {
        "managed_positions": config.managed_position_registry_path,
        "managed_orders": config.managed_order_registry_path,
        "open_order_truth": config.open_order_truth_path,
        "guardian": config.broker_position_guardian_path,
        "safe_state": config.safe_state_path,
        "reconciliation": config.reconciliation_path,
        "broker_truth_lease": config.broker_truth_lease_path,
        "broker_session_authority": config.broker_session_authority_path,
    }
    return {name: overrides[name] if name in overrides else _read_json(config.resolve(path)) for name, path in paths.items()}


def _matching_guardian_candidate(
    *,
    guardian: Mapping[str, Any],
    trade_id: str,
    lifecycle_id: str,
    account_id: str,
    local_symbol: str,
    con_id: str,
    expected_action: str,
    expected_qty: str,
) -> dict[str, Any]:
    close = _mapping(guardian.get("managed_close_authority"))
    for row in _list(close.get("candidates")):
        candidate = _mapping(row)
        if _candidate_matches(
            candidate=candidate,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            account_id=account_id,
            local_symbol=local_symbol,
            con_id=con_id,
            expected_action=expected_action,
            expected_qty=expected_qty,
        ):
            return candidate
    return {}


def _safe_state_allows_candidate(*, safe_state: Mapping[str, Any], candidate: Mapping[str, Any], expected_action: str, expected_qty: str) -> bool:
    if str(safe_state.get("classification") or "") == "SAFE_STATE_NORMAL":
        close = _mapping(safe_state.get("close_authority"))
        if not close:
            return True
        if close.get("allowed") is not True:
            return False
    else:
        close = _mapping(safe_state.get("close_authority"))
        if close.get("allowed") is not True:
            return False
    close = _mapping(safe_state.get("close_authority"))
    if not close:
        return True
    candidates = [_mapping(row) for row in _list(close.get("guardian_close_candidates"))]
    if not candidates:
        return True
    return any(
        _candidate_matches(
            candidate=row,
            trade_id=_text(candidate.get("trade_id")),
            lifecycle_id=_text(candidate.get("lifecycle_id")),
            account_id=_text(candidate.get("account_id")),
            local_symbol=_text(candidate.get("local_symbol")),
            con_id=_text(candidate.get("con_id")),
            expected_action=expected_action,
            expected_qty=expected_qty,
        )
        for row in candidates
    )


def _candidate_matches(
    *,
    candidate: Mapping[str, Any],
    trade_id: str,
    lifecycle_id: str,
    account_id: str,
    local_symbol: str,
    con_id: str,
    expected_action: str,
    expected_qty: str,
) -> bool:
    return (
        _text(candidate.get("trade_id")) == trade_id
        and _text(candidate.get("lifecycle_id")) == lifecycle_id
        and _text(candidate.get("account_id")) == account_id
        and _text(candidate.get("local_symbol")) == local_symbol
        and _text(candidate.get("con_id")) == _text(con_id)
        and str(candidate.get("action") or "").upper() == expected_action
        and _decimal(candidate.get("quantity")) == _decimal(expected_qty)
    )


def _matching_registry_record(
    *,
    reconciliation: Mapping[str, Any],
    trade_id: str,
    lifecycle_id: str,
    account_id: str,
    local_symbol: str,
    con_id: str,
) -> dict[str, Any]:
    for row in _registry_records(reconciliation):
        if (
            _text(row.get("trade_id")) == trade_id
            and _text(row.get("lifecycle_id")) == lifecycle_id
            and _text(row.get("account_id")) == account_id
            and _text(row.get("local_symbol")) == local_symbol
            and _text(row.get("con_id")) == _text(con_id)
        ):
            return row
    return {}


def _registry_competing_count(*, reconciliation: Mapping[str, Any], account_id: str, local_symbol: str, con_id: str) -> int:
    count = 0
    for row in _registry_records(reconciliation):
        if str(row.get("current_state") or "") != "OPEN_MANAGED":
            continue
        if _text(row.get("account_id")) == account_id and _text(row.get("local_symbol")) == local_symbol and _text(row.get("con_id")) == _text(con_id):
            count += 1
    return count


def _registry_records(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    registry = _mapping(reconciliation.get("registry_reconciliation"))
    return [_mapping(row) for row in _list(registry.get("mapped_records"))]


def _matching_position_count(*, positions: Sequence[Mapping[str, Any]], account_id: str, local_symbol: str, con_id: str) -> int:
    count = 0
    for row in positions:
        broker_position = _mapping(row.get("broker_position"))
        if (
            _text(row.get("account_id") or broker_position.get("account_id")) == account_id
            and _text(row.get("local_symbol") or broker_position.get("local_symbol")) == local_symbol
            and _text(row.get("con_id") or broker_position.get("con_id")) == _text(con_id)
        ):
            count += 1
    return count


def _matching_managed_order_state(
    *,
    managed_orders: Mapping[str, Any],
    trade_id: str,
    lifecycle_id: str,
    account_id: str,
    local_symbol: str,
    con_id: str,
) -> dict[str, Any]:
    for row in _list(managed_orders.get("managed_orders")):
        order = _mapping(row)
        if _text(order.get("trade_id")) == trade_id or _text(order.get("lifecycle_id")) == lifecycle_id:
            return order
        broker_position = _mapping(order.get("broker_position"))
        canonical = _mapping(broker_position.get("canonical_managed_position"))
        if (
            _text(order.get("account_id") or broker_position.get("account_id")) == account_id
            and _text(order.get("local_symbol") or order.get("contract") or broker_position.get("local_symbol")) == local_symbol
            and _text(order.get("con_id") or broker_position.get("con_id")) == _text(con_id)
            and (_text(canonical.get("trade_id")) == trade_id or _text(canonical.get("lifecycle_id")) == lifecycle_id)
        ):
            return order
    return {}


def _close_required_state(*, managed_order_state: Mapping[str, Any], managed_orders: Mapping[str, Any]) -> bool:
    if str(managed_orders.get("classification") or "") in {"POSITION_WITHOUT_CLOSE_ORDER", "BROKER_POSITION_WITHOUT_CLOSE_ORDER"}:
        return True
    if not managed_order_state:
        return False
    if str(managed_order_state.get("classification") or "") in {"POSITION_WITHOUT_CLOSE_ORDER", "BROKER_POSITION_WITHOUT_CLOSE_ORDER"}:
        return True
    return managed_order_state.get("close_order_required_now") is True


def _conflicting_orders(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    account_id: str,
    local_symbol: str,
    con_id: str,
    lifecycle_id: str,
) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    orders.extend(_list(inputs["open_order_truth"].get("broker_open_orders")))
    orders.extend(_list(inputs["open_order_truth"].get("open_orders")))
    orders.extend(_list(inputs["open_order_truth"].get("orders")))
    orders.extend(row for row in _list(inputs["managed_orders"].get("managed_orders")) if _mapping(row).get("working") is True)
    conflicts: list[dict[str, Any]] = []
    for raw in orders:
        order = _mapping(raw)
        if not order:
            continue
        order_account = _text(order.get("account_id") or order.get("account"))
        order_symbol = _text(order.get("local_symbol") or order.get("contract"))
        order_con_id = _text(order.get("con_id"))
        same_contract = (
            (order_symbol and order_symbol == local_symbol)
            or (order_con_id and order_con_id == _text(con_id))
            or (_text(order.get("lifecycle_id")) == lifecycle_id)
        )
        if same_contract and (not order_account or order_account == account_id):
            conflicts.append(dict(order))
    return conflicts


def _broker_session_apply_blockers(*, inputs: Mapping[str, Mapping[str, Any]]) -> list[str]:
    authority = _mapping(inputs.get("broker_session_authority"))
    if not authority:
        return ["BROKER_SESSION_AUTHORITY_MISSING"]
    blockers: list[str] = []
    connection_mode = str(authority.get("connection_mode") or "").strip().upper()
    classification = str(authority.get("classification") or "").strip().upper()
    allowed_uses = _mapping(authority.get("allowed_uses"))
    if connection_mode == "ORDER_STATUS_UNRELIABLE" or classification == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE":
        blockers.append("BROKER_SESSION_CLOSE_AUTHORITY_BLOCKED_ORDER_STATUS_UNRELIABLE")
    elif connection_mode == "POSITION_TRUTH_ONLY" or classification == "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY":
        blockers.append("BROKER_SESSION_CLOSE_AUTHORITY_BLOCKED_POSITION_TRUTH_ONLY")
    elif connection_mode in {"IBKR_CONNECTION_DOWN", ""} or classification in {
        "BROKER_SESSION_AUTHORITY_CONNECTION_DOWN",
        "BROKER_SESSION_AUTHORITY_OPERATOR_REQUIRED",
        "",
    }:
        blockers.append("BROKER_SESSION_CLOSE_AUTHORITY_BLOCKED_NOT_SUBMIT_CAPABLE")
    if allowed_uses.get("managed_risk_reducing_close") is not True:
        blockers.append("BROKER_SESSION_MANAGED_RISK_REDUCING_CLOSE_NOT_ALLOWED")
    if authority.get("live_money_eligible") is True:
        blockers.append("BROKER_SESSION_LIVE_MONEY_ELIGIBLE_TRUE")
    if authority.get("paper_proof_invoked") is True:
        blockers.append("BROKER_SESSION_PAPER_PROOF_INVOKED_TRUE")
    return _dedupe(blockers)


def _global_safety_flags(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "live_money_eligible": _flag_true(inputs, "live_money_eligible"),
        "paper_proof_invoked": _flag_true(inputs, "paper_proof_invoked"),
        "broad_flatten_allowed": _bool_any(inputs["guardian"].get("broad_flatten_allowed"), inputs["safe_state"].get("broad_flatten_allowed")),
        "global_flatten_allowed": _bool_any(inputs["guardian"].get("global_flatten_allowed"), inputs["safe_state"].get("global_flatten_allowed")),
        "guardian_classification": inputs["guardian"].get("classification"),
        "guardian_close_allowed": _mapping(inputs["guardian"].get("managed_close_authority")).get("allowed") is True,
        "safe_state_classification": inputs["safe_state"].get("classification"),
        "safe_state_close_allowed": _mapping(inputs["safe_state"].get("close_authority")).get("allowed") is True,
    }


def _flag_true(inputs: Mapping[str, Mapping[str, Any]], key: str) -> bool:
    return any(payload.get(key) is True for payload in inputs.values())


def _bool_any(*values: Any) -> bool:
    return any(value is True for value in values)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_display(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f")


def _int_or_text(value: str) -> int | str:
    try:
        return int(value)
    except ValueError:
        return value


def _dedupe(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_MANAGED_EXIT_RECOVERY_PLAN)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBManagedExitRecoveryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = run_track_b_managed_exit_recovery(config=config, write=not args.no_write)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"eligible_count={payload.get('eligible_count')}")
        print(f"blocked_count={payload.get('blocked_count')}")
    return 0 if payload.get("classification") in {NO_EXIT_DUE_POSITIONS, EXIT_DUE_CLOSE_READY, EXIT_DUE_CLOSE_PARTIAL_READY} else 2


if __name__ == "__main__":
    raise SystemExit(main())
