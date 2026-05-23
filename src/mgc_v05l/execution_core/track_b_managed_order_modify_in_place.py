"""Operator-authorized Track B PAPER managed close-order modify-in-place.

Managed order modification authority lives in execution_core. Dashboard
artifacts are projections only and must never be consumed as routing authority.
This v1 path can plan a limit-price adjustment for an existing managed close
order and can execute it only when the caller provides an explicit broker
adapter hook plus operator authorization. It never creates replacement orders.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_crash_loop_protection import DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_order_adjustment_planner import (
    DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT,
    MODIFY_IN_PLACE_ELIGIBLE,
)
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import DEFAULT_SELF_RECOVER_RULES_ARTIFACT
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT


MODIFY_IN_PLACE_DRY_RUN_READY = "MODIFY_IN_PLACE_DRY_RUN_READY"
MODIFY_IN_PLACE_APPLIED = "MODIFY_IN_PLACE_APPLIED"
MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH = "MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH"
MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH = "MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH"
MODIFY_IN_PLACE_BLOCKED_NOT_MANAGED_ORDER = "MODIFY_IN_PLACE_BLOCKED_NOT_MANAGED_ORDER"
MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER = "MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER"
MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER = "MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER"
MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK = "MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK"
MODIFY_IN_PLACE_BLOCKED_PRICE_INVALID = "MODIFY_IN_PLACE_BLOCKED_PRICE_INVALID"
MODIFY_IN_PLACE_BLOCKED_OPERATOR_AUTH_REQUIRED = "MODIFY_IN_PLACE_BLOCKED_OPERATOR_AUTH_REQUIRED"
MODIFY_IN_PLACE_VERIFICATION_FAILED = "MODIFY_IN_PLACE_VERIFICATION_FAILED"

DEFAULT_AUDIT_PATH = (
    Path("outputs")
    / "reports"
    / "track_b_managed_order_modify_in_place"
    / "latest_managed_order_modify_in_place.json"
)

PAPER_ACCOUNT = "DUM882026"
_TERMINAL_STATUSES = {"FILLED", "CANCELLED", "APICANCELLED", "INACTIVE"}
_SENTINEL_FILLED_QUANTITY = Decimal("1e100")


@dataclass(frozen=True)
class ManagedOrderModifyInPlaceConfig:
    repo_root: Path
    broker_order_id: str
    perm_id: str
    symbol: str
    contract: str
    action: str
    quantity: str
    current_known_limit: str
    new_limit: str
    account_id: str = PAPER_ACCOUNT
    con_id: str | None = None
    mode: str = "PAPER"
    apply: bool = False
    operator_authorized_modify: bool = False
    output_path: Path = DEFAULT_AUDIT_PATH
    require_shared_truth_evidence: bool = True
    shared_truth_max_age_seconds: float = 600.0
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    runtime_resume_semantics_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    crash_loop_protection_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


BrokerOrderRefresh = Callable[[ManagedOrderModifyInPlaceConfig], Mapping[str, Any] | Sequence[Mapping[str, Any]]]
BrokerOrderModify = Callable[[ManagedOrderModifyInPlaceConfig], Mapping[str, Any]]


def run_track_b_managed_order_modify_in_place(
    *,
    config: ManagedOrderModifyInPlaceConfig,
    now: datetime | None = None,
    pre_modify_open_order_refresh: BrokerOrderRefresh | None = None,
    modify_order_limit: BrokerOrderModify | None = None,
    post_modify_open_order_refresh: BrokerOrderRefresh | None = None,
) -> dict[str, Any]:
    """Plan or execute one exact managed close-order limit modification."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    shared = _shared_truth_evidence(config=config, now=actual_now)
    target = _target_evidence(config=config, shared=shared)
    readiness = _classify_readiness(config=config, shared=shared, target=target)
    report = _base_report(config=config, now=actual_now, shared=shared, target=target, readiness=readiness)

    if readiness["classification"] != MODIFY_IN_PLACE_DRY_RUN_READY:
        _write_report(config=config, report=report)
        return report
    if not config.apply:
        report["detail"] = "Modify-in-place dry-run is ready; apply=false so no broker mutation was attempted."
        _write_report(config=config, report=report)
        return report
    if not config.operator_authorized_modify:
        report["classification"] = MODIFY_IN_PLACE_BLOCKED_OPERATOR_AUTH_REQUIRED
        report["detail"] = "Actual modify requires --operator-authorized-modify plus exact order identifiers."
        report["broker_mutation_attempted"] = False
        _write_report(config=config, report=report)
        return report

    if pre_modify_open_order_refresh is None or modify_order_limit is None or post_modify_open_order_refresh is None:
        report["classification"] = MODIFY_IN_PLACE_VERIFICATION_FAILED
        report["detail"] = (
            "Apply mode requires broker open-order refresh, modify, and post-modify "
            "verification adapter hooks. No broker mutation was attempted."
        )
        report["broker_mutation_attempted"] = False
        _write_report(config=config, report=report)
        return report

    pre_refresh = _normalize_refresh(pre_modify_open_order_refresh(config))
    pre_order = _matching_order(config=config, rows=pre_refresh)
    if pre_order is None:
        report["classification"] = MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER
        report["detail"] = "Pre-modify broker refresh did not find the exact working order."
        report["pre_modify_refresh"] = pre_refresh
        _write_report(config=config, report=report)
        return report
    pre_mismatch = _identity_mismatch(config=config, row=pre_order, require_limit=True)
    if pre_mismatch:
        report["classification"] = MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH
        report["detail"] = pre_mismatch
        report["pre_modify_refresh"] = pre_refresh
        _write_report(config=config, report=report)
        return report
    if _is_terminal(pre_order) or _is_suspicious_order(pre_order):
        report["classification"] = (
            MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER
            if _is_suspicious_order(pre_order)
            else MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER
        )
        report["detail"] = "Pre-modify broker refresh shows the order is not clean and working."
        report["pre_modify_refresh"] = pre_refresh
        _write_report(config=config, report=report)
        return report

    try:
        modify_result = dict(modify_order_limit(config))
    except Exception as exc:  # noqa: BLE001 - adapter errors must become audit evidence.
        report["classification"] = MODIFY_IN_PLACE_VERIFICATION_FAILED
        report["detail"] = f"Broker modify adapter failed: {exc}"
        report["broker_mutation_attempted"] = True
        report["pre_modify_refresh"] = pre_refresh
        _write_report(config=config, report=report)
        return report

    post_refresh = _normalize_refresh(post_modify_open_order_refresh(config))
    post_order = _matching_order(config=config, rows=post_refresh, expected_limit=config.new_limit)
    if post_order is None:
        report["classification"] = MODIFY_IN_PLACE_VERIFICATION_FAILED
        report["detail"] = "Post-modify verification did not find the same order id/perm with the requested new limit."
        report["broker_mutation_attempted"] = True
        report["broker_mutation_performed"] = bool(modify_result.get("accepted", True))
        report["pre_modify_refresh"] = pre_refresh
        report["modify_result"] = _jsonable(modify_result)
        report["post_modify_refresh"] = post_refresh
        _write_report(config=config, report=report)
        return report

    report["classification"] = MODIFY_IN_PLACE_APPLIED
    report["detail"] = "Existing managed close order limit was modified in place and verified with the same order identity."
    report["broker_mutation_attempted"] = True
    report["broker_mutation_performed"] = True
    report["pre_modify_refresh"] = pre_refresh
    report["modify_result"] = _jsonable(modify_result)
    report["post_modify_refresh"] = post_refresh
    report["verified_order"] = _jsonable(post_order)
    _write_report(config=config, report=report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track B PAPER managed close-order modify-in-place boundary.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--broker-order-id", required=True)
    parser.add_argument("--perm-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--con-id")
    parser.add_argument("--action", required=True)
    parser.add_argument("--quantity", required=True)
    parser.add_argument("--current-known-limit", required=True)
    parser.add_argument("--new-limit", required=True)
    parser.add_argument("--account-id", default=PAPER_ACCOUNT)
    parser.add_argument("--apply", action="store_true", help="Attempt broker modify if an adapter hook is supplied by the caller.")
    parser.add_argument("--operator-authorized-modify", action="store_true")
    parser.add_argument("--output-path", type=Path, default=DEFAULT_AUDIT_PATH)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ManagedOrderModifyInPlaceConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        broker_order_id=str(args.broker_order_id),
        perm_id=str(args.perm_id),
        symbol=str(args.symbol).upper(),
        contract=str(args.contract).upper(),
        con_id=None if args.con_id is None else str(args.con_id),
        action=str(args.action).upper(),
        quantity=str(args.quantity),
        current_known_limit=str(args.current_known_limit),
        new_limit=str(args.new_limit),
        account_id=str(args.account_id),
        apply=bool(args.apply),
        operator_authorized_modify=bool(args.operator_authorized_modify),
        output_path=Path(args.output_path),
    )
    report = run_track_b_managed_order_modify_in_place(config=config)
    if bool(args.json):
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"classification={report.get('classification')}")
        print(f"detail={report.get('detail')}")
        print(f"artifact_path={report.get('artifact_path')}")
    return 0 if report.get("classification") in {MODIFY_IN_PLACE_DRY_RUN_READY, MODIFY_IN_PLACE_APPLIED} else 1


def _classify_readiness(
    *,
    config: ManagedOrderModifyInPlaceConfig,
    shared: Mapping[str, Any],
    target: Mapping[str, Any],
) -> dict[str, Any]:
    current_limit = _decimal(config.current_known_limit)
    new_limit = _decimal(config.new_limit)
    if config.mode.upper() != "PAPER" or config.account_id != PAPER_ACCOUNT:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH, "Modify-in-place is locked to local PAPER account DUM882026.")
    if current_limit is None or new_limit is None or new_limit <= 0:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_PRICE_INVALID, "Current and new limit prices must be positive decimals.")
    if current_limit == new_limit:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_PRICE_INVALID, "New limit must differ from current known limit.")
    if shared.get("blockers"):
        blocker_detail = "; ".join(str(item) for item in shared["blockers"])
        if any("DUPLICATE" in str(item).upper() for item in shared["blockers"]):
            return _blocked(MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK, blocker_detail)
        if any("SUSPICIOUS" in str(item).upper() or "REVIEW_REQUIRED" in str(item).upper() for item in shared["blockers"]):
            return _blocked(MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER, blocker_detail)
        return _blocked(MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH, blocker_detail)
    order = target.get("managed_order_match")
    if not isinstance(order, Mapping):
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_MANAGED_ORDER, "Managed Order Registry has no exact managed order match.")
    plan = target.get("order_adjustment_plan_match")
    if not isinstance(plan, Mapping):
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_MANAGED_ORDER, "Order Adjustment Planner has no exact order plan match.")
    missing_linkage = [
        name
        for name in ("lifecycle_id", "manifest_id", "ownership_id")
        if not (order.get(name) or plan.get(name))
    ]
    if missing_linkage:
        return _blocked(
            MODIFY_IN_PLACE_BLOCKED_NOT_MANAGED_ORDER,
            f"Managed order is missing lifecycle/manifest/ownership linkage: {', '.join(missing_linkage)}.",
        )
    mismatch = _identity_mismatch(config=config, row=order, require_limit=True)
    if mismatch:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH, mismatch)
    if _decimal(_value(order, "limit_price", "order_limit_price")) != current_limit:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH, "Current known limit does not match Managed Order Registry.")
    managed_class = str(order.get("classification") or "")
    plan_class = str(plan.get("classification") or "")
    if managed_class in {"CLOSE_ORDER_SUSPICIOUS", "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED"}:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER, f"Managed order is suspicious: {managed_class}.")
    if managed_class in {"DUPLICATE_CLOSE_ORDER_BLOCKED"}:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK, "Managed Order Registry reports duplicate close risk.")
    if managed_class in {"BROKER_FLAT_WITH_WORKING_CLOSE"} or plan_class == "BROKER_FLAT_NO_REPLACE":
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER, "Broker flat with working close order; do not modify.")
    if managed_class not in {"WORKING_CLOSE_ORDER", "CLOSE_ORDER_MODIFIABLE"}:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER, f"Managed order is not a working close order: {managed_class}.")
    if plan_class != MODIFY_IN_PLACE_ELIGIBLE:
        if plan_class == "DO_NOT_REPLACE_DUPLICATE_RISK":
            return _blocked(MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK, "Order Adjustment Planner reports duplicate risk.")
        if plan_class in {"REVIEW_REQUIRED_SUSPICIOUS_STATE", "TARGETED_CANCEL_REPLACE_REQUIRED"}:
            return _blocked(MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER, f"Planner does not allow modify-in-place: {plan_class}.")
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER, f"Planner is not modify-in-place eligible: {plan_class}.")
    if target.get("matching_position_count", 0) <= 0:
        return _blocked(MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER, "No matching active broker/managed position exists; do not modify close order.")
    return {
        "classification": MODIFY_IN_PLACE_DRY_RUN_READY,
        "detail": "Exact managed close order is eligible for operator-authorized modify-in-place.",
    }


def _shared_truth_evidence(*, config: ManagedOrderModifyInPlaceConfig, now: datetime) -> dict[str, Any]:
    paths = {
        "open_order_truth": config.open_order_truth_path,
        "managed_order_registry": config.managed_order_registry_path,
        "order_adjustment_plan": config.order_adjustment_plan_path,
        "position_truth": config.position_truth_path,
        "managed_position_registry": config.managed_position_registry_path,
        "runtime_supervisor_authority": config.runtime_supervisor_authority_path,
        "self_recover_rules": config.self_recover_rules_path,
        "runtime_resume_semantics": config.runtime_resume_semantics_path,
        "crash_loop_protection": config.crash_loop_protection_path,
        "broker_lease": config.broker_lease_path,
    }
    payloads = {name: _read_json(config.resolve(path)) for name, path in paths.items()}
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    classifications = {name: _classification(payload) for name, payload in payloads.items()}
    classifications["reconciliation"] = str(reconciliation.get("classification") or "")
    freshness = {
        name: _freshness(payload=payload, now=now, max_age_seconds=config.shared_truth_max_age_seconds)
        for name, payload in payloads.items()
    }
    blockers: list[str] = []
    if config.require_shared_truth_evidence:
        for name, payload in payloads.items():
            if not payload:
                blockers.append(f"Shared authority artifact missing: {name}.")
        if not reconciliation:
            blockers.append("Shared authority artifact missing: reconciliation.")
        for name, state in freshness.items():
            if state["stale_or_missing"]:
                blockers.append(f"Shared authority artifact stale/missing: {name}.")

    if classifications["open_order_truth"] in {
        "DUPLICATE_CLOSE_ORDER",
        "UNKNOWN_OPEN_ORDER",
        "SUSPICIOUS_ORDER_STATE",
        "BROKER_FLAT_WITH_OPEN_CLOSE_ORDER",
    }:
        blockers.append(f"Open Order Truth blocks modify-in-place: {classifications['open_order_truth']}.")
    if classifications["managed_order_registry"] in {
        "CLOSE_ORDER_SUSPICIOUS",
        "DUPLICATE_CLOSE_ORDER_BLOCKED",
        "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        "BROKER_FLAT_WITH_WORKING_CLOSE",
    }:
        blockers.append(f"Managed Order Registry blocks modify-in-place: {classifications['managed_order_registry']}.")
    if classifications["order_adjustment_plan"] in {
        "TARGETED_CANCEL_REPLACE_REQUIRED",
        "DO_NOT_REPLACE_DUPLICATE_RISK",
        "REVIEW_REQUIRED_SUSPICIOUS_STATE",
        "BROKER_FLAT_NO_REPLACE",
        "ORDER_NOT_FOUND",
    }:
        blockers.append(f"Order Adjustment Planner blocks modify-in-place: {classifications['order_adjustment_plan']}.")
    if classifications["runtime_supervisor_authority"] in {
        "SUPERVISOR_MANUAL_REVIEW_REQUIRED",
        "SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK",
        "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP",
        "SUPERVISOR_SHARED_TRUTH_STALE",
        "SUPERVISOR_UNKNOWN_REVIEW_REQUIRED",
    }:
        blockers.append(f"Runtime Supervisor Authority blocks modify-in-place: {classifications['runtime_supervisor_authority']}.")
    if classifications["self_recover_rules"] in {
        "OPERATOR_REVIEW_REQUIRED",
        "MANUAL_TWS_REVIEW_REQUIRED",
        "DO_NOT_RECOVER_UNSAFE_STATE",
        "REFRESH_SHARED_TRUTH",
    }:
        blockers.append(f"Self-Recover Rules block modify-in-place: {classifications['self_recover_rules']}.")
    if classifications["runtime_resume_semantics"] in {
        "RESUME_BLOCKED_CRASH_LOOP",
        "RESUME_BLOCKED_OPERATOR_ACK_REQUIRED",
        "RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE",
        "RESUME_UNKNOWN_REVIEW_REQUIRED",
    }:
        blockers.append(f"Runtime Resume Semantics blocks modify-in-place: {classifications['runtime_resume_semantics']}.")
    if classifications["crash_loop_protection"] in {
        "RESTART_COOLDOWN_ACTIVE",
        "REPEATED_RUNTIME_FAILURE",
        "REPEATED_MARKET_DATA_FAILURE",
        "REPEATED_BROKER_LEASE_FAILURE",
        "OPERATOR_ACK_REQUIRED",
    }:
        blockers.append(f"Crash Loop Protection blocks modify-in-place: {classifications['crash_loop_protection']}.")
    if classifications["broker_lease"] in {
        "INVALIDATED_CONTRADICTION",
        "INVALIDATED_UNKNOWN_OPEN_ORDERS",
        "INVALIDATED_MANUAL_BROKER_ACTION",
        "OPERATOR_REQUIRED",
    }:
        blockers.append(f"Broker Truth Lease is unsafe for modify-in-place: {classifications['broker_lease']}.")
    if classifications["reconciliation"] in {
        "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
        "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED_UNKNOWN_OPEN_ORDERS",
    }:
        blockers.append(f"Reconciliation is unsafe for modify-in-place: {classifications['reconciliation']}.")

    return {
        "source_authority": "execution_core_authority",
        "dashboard_projection_consumed": False,
        "required": config.require_shared_truth_evidence,
        "max_age_seconds": config.shared_truth_max_age_seconds,
        "artifact_paths": {name: str(config.resolve(path)) for name, path in paths.items()}
        | {"reconciliation": str(config.resolve(config.reconciliation_path))},
        "payloads": payloads | {"reconciliation": reconciliation},
        "classifications": classifications,
        "freshness": freshness,
        "blockers": blockers,
    }


def _target_evidence(*, config: ManagedOrderModifyInPlaceConfig, shared: Mapping[str, Any]) -> dict[str, Any]:
    payloads = _mapping(shared.get("payloads"))
    managed_order = _find_order(config=config, rows=_list(_mapping(payloads.get("managed_order_registry")).get("managed_orders")))
    plan = _find_order(config=config, rows=_list(_mapping(payloads.get("order_adjustment_plan")).get("plans")))
    position_rows = _position_rows(_mapping(payloads.get("position_truth"))) + _position_rows(
        _mapping(payloads.get("managed_position_registry"))
    )
    matching_positions = [row for row in position_rows if _position_matches(config=config, row=row)]
    conflicting_positions = [
        row
        for row in position_rows
        if _position_active(row) and not _position_matches(config=config, row=row)
    ]
    return {
        "managed_order_match": _jsonable(managed_order),
        "order_adjustment_plan_match": _jsonable(plan),
        "matching_position_count": len(matching_positions),
        "conflicting_position_count": len(conflicting_positions),
        "matching_positions": _jsonable(matching_positions),
        "conflicting_positions": _jsonable(conflicting_positions),
    }


def _base_report(
    *,
    config: ManagedOrderModifyInPlaceConfig,
    now: datetime,
    shared: Mapping[str, Any],
    target: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_order_modify_in_place_v1",
        "generated_at": now.isoformat(),
        "classification": readiness.get("classification"),
        "detail": readiness.get("detail"),
        "mode": "PAPER",
        "route": "TRACK_B_PAPER_MANAGED_CLOSE_MODIFY_IN_PLACE_ONLY",
        "dry_run": not config.apply,
        "apply": bool(config.apply),
        "operator_authorized_modify": bool(config.operator_authorized_modify),
        "operator_authorization_required": True,
        "broker_mutation_attempted": False,
        "broker_mutation_performed": False,
        "new_order_created": False,
        "forbidden_routes": ["cancel_replace", "broad_cancel", "flatten_all", "paper_proof", "live_money"],
        "requested_identity": {
            "account_id": config.account_id,
            "symbol": config.symbol,
            "contract": config.contract,
            "con_id": config.con_id,
            "broker_order_id": config.broker_order_id,
            "perm_id": config.perm_id,
            "action": config.action,
            "quantity": config.quantity,
            "current_known_limit": config.current_known_limit,
            "new_limit": config.new_limit,
        },
        "shared_truth_evidence": _redacted_shared(shared),
        "target_evidence": _jsonable(target),
        "readiness": _jsonable(readiness),
        "artifact_path": str(config.resolve(config.output_path)),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _blocked(classification: str, detail: str) -> dict[str, str]:
    return {"classification": classification, "detail": detail}


def _find_order(*, config: ManagedOrderModifyInPlaceConfig, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        if _order_identity_matches(config=config, row=row):
            return dict(row)
    return None


def _matching_order(
    *,
    config: ManagedOrderModifyInPlaceConfig,
    rows: Sequence[Mapping[str, Any]],
    expected_limit: str | None = None,
) -> dict[str, Any] | None:
    for row in rows:
        if _order_identity_matches(config=config, row=row):
            if expected_limit is not None and _decimal(_value(row, "limit_price", "order_limit_price", "lmt_price")) != _decimal(
                expected_limit
            ):
                continue
            return dict(row)
    return None


def _order_identity_matches(*, config: ManagedOrderModifyInPlaceConfig, row: Mapping[str, Any]) -> bool:
    identity = _mapping(row.get("identity"))
    source = _mapping(row.get("source_order"))
    broker_order_id = _value(row, "broker_order_id", "order_id") or _value(identity, "broker_order_id", "order_id") or _value(
        source, "broker_order_id", "order_id"
    )
    if str(broker_order_id or "") != str(config.broker_order_id):
        return False
    perm_id = _value(row, "perm_id") or _value(identity, "perm_id") or _value(source, "perm_id")
    if str(perm_id or "") != str(config.perm_id):
        return False
    action = _value(row, "action") or _value(identity, "action") or _value(source, "action")
    if str(action or "").upper() != config.action.upper():
        return False
    quantity = _decimal(_value(row, "quantity", "qty") or _value(identity, "quantity", "qty") or _value(source, "quantity", "qty"))
    if quantity != _decimal(config.quantity):
        return False
    contract = str(_value(row, "contract", "local_symbol") or _value(identity, "contract", "local_symbol") or _value(source, "contract", "local_symbol") or "").upper()
    if contract and contract != config.contract.upper():
        return False
    con_id = _value(row, "con_id", "conId") or _value(identity, "con_id", "conId") or _value(source, "con_id", "conId")
    if config.con_id is not None and con_id not in {None, ""} and str(con_id) != str(config.con_id):
        return False
    account = str(_value(row, "account_id", "account") or _value(identity, "account_id", "account") or _value(source, "account_id", "account") or "").strip()
    return not account or account == config.account_id


def _identity_mismatch(*, config: ManagedOrderModifyInPlaceConfig, row: Mapping[str, Any], require_limit: bool) -> str | None:
    if not _order_identity_matches(config=config, row=row):
        return "Order identity does not match the exact requested account/order/perm/contract/action/quantity."
    if require_limit:
        limit = _decimal(_value(row, "limit_price", "order_limit_price", "lmt_price"))
        if limit != _decimal(config.current_known_limit):
            return f"Limit price mismatch: expected current {config.current_known_limit}, saw {_value(row, 'limit_price', 'order_limit_price', 'lmt_price')}."
    return None


def _position_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("position_states", "broker_positions", "managed_positions"):
        for row in _list(payload.get(key)):
            if isinstance(row, Mapping):
                rows.append(dict(row))
    return rows


def _position_matches(*, config: ManagedOrderModifyInPlaceConfig, row: Mapping[str, Any]) -> bool:
    if not _position_active(row):
        return False
    symbol = str(_value(row, "symbol", "track_b_root", "instrument_family", "instrument") or "").upper()
    if symbol and symbol != config.symbol.upper():
        return False
    contract = str(_value(row, "contract", "local_symbol", "localSymbol") or "").upper()
    if contract and contract != config.contract.upper():
        return False
    con_id = _value(row, "con_id", "conId")
    if config.con_id is not None and con_id not in {None, ""} and str(con_id) != str(config.con_id):
        return False
    quantity = _decimal(_value(row, "signed_quantity", "broker_quantity", "quantity", "qty"))
    if quantity is None or quantity == 0:
        return False
    expected = _decimal(config.quantity)
    if expected is None:
        return False
    expected_signed = expected if config.action.upper() == "SELL" else -expected
    if quantity != expected_signed:
        return False
    account = str(_value(row, "account_id", "account") or "").strip()
    return not account or account == config.account_id


def _position_active(row: Mapping[str, Any]) -> bool:
    classification = str(row.get("classification") or "").upper()
    if classification in {"FLAT_CLEAN", "NO_MANAGED_POSITIONS", "CLOSED_FLAT"}:
        return False
    status = str(_value(row, "final_position_status", "lifecycle_status", "status") or "").upper()
    return status != "CLOSED_FLAT"


def _is_terminal(row: Mapping[str, Any]) -> bool:
    return str(_value(row, "broker_status", "status") or "").upper() in _TERMINAL_STATUSES


def _is_suspicious_order(row: Mapping[str, Any]) -> bool:
    reasons = _list(row.get("suspicious_reasons")) + _list(_mapping(row.get("source_order")).get("suspicious_reasons"))
    if reasons:
        return True
    filled = _decimal(_value(row, "filled_quantity", "filled") or _value(_mapping(row.get("source_order")), "filled_quantity", "filled"))
    remaining = _value(row, "remaining_quantity", "remaining") or _value(_mapping(row.get("source_order")), "remaining_quantity", "remaining")
    return (filled is not None and abs(filled) >= _SENTINEL_FILLED_QUANTITY) or remaining in {None, ""}


def _normalize_refresh(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("open_orders", "track_b_broker_open_orders", "order_states", "managed_orders"):
            rows = payload.get(key)
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
                return [dict(row) for row in rows if isinstance(row, Mapping)]
        return [dict(payload)]
    return [dict(row) for row in payload if isinstance(row, Mapping)]


def _redacted_shared(shared: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_authority": shared.get("source_authority"),
        "dashboard_projection_consumed": shared.get("dashboard_projection_consumed"),
        "required": shared.get("required"),
        "max_age_seconds": shared.get("max_age_seconds"),
        "artifact_paths": shared.get("artifact_paths"),
        "classifications": shared.get("classifications"),
        "freshness": shared.get("freshness"),
        "blockers": shared.get("blockers"),
    }


def _classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(
        payload.get("classification")
        or payload.get("recommendation")
        or payload.get("crash_loop_classification")
        or payload.get("broker_lease_classification")
        or summary.get("overall_classification")
        or summary.get("classification")
        or ""
    )


def _freshness(*, payload: Mapping[str, Any], now: datetime, max_age_seconds: float) -> dict[str, Any]:
    generated_at = payload.get("generated_at")
    parsed = _parse_datetime(generated_at)
    age = None if parsed is None else max(0.0, (now - parsed).total_seconds())
    return {
        "generated_at": generated_at,
        "age_seconds": age,
        "max_age_seconds": max_age_seconds,
        "stale_or_missing": parsed is None or age is None or age > max_age_seconds,
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _value(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in {None, ""}:
            return mapping[key]
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_report(*, config: ManagedOrderModifyInPlaceConfig, report: Mapping[str, Any]) -> None:
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
