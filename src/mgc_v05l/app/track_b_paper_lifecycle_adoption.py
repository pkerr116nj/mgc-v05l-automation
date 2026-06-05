"""Supervised PAPER-only lifecycle adoption for proven Track B broker fills.

This command is intentionally offline.  It reconstructs local lifecycle
artifacts only after existing bridge, intent, broker-truth, and route identity
artifacts all prove that the broker position belongs to the named Track B lane.
It never connects to a broker and dry-run is the default.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from mgc_v05l.execution_core.track_b_broker_session_authority import (
    DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_fill_evidence_resolver import (
    RESOLVED as BROKER_FILL_EVIDENCE_RESOLVED,
    BrokerFillEvidenceRequest,
    resolve_broker_backed_fill_evidence,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    NO_MANAGED_ORDERS,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT, NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    update_track_b_paper_trade_ledger_from_filled_bridge_result,
)
from mgc_v05l.execution_core.track_b_position_management_manifest import (
    DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    resolve_management_metadata,
)
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_lifecycle_local_repair_guard import (
    LIFECYCLE_LOCAL_REPAIR_VALID,
    TrackBLifecycleLocalRepairGuardConfig,
    validate_lifecycle_local_artifact_repair,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    append_submit_intent_ownership_record,
    load_submit_intent_ownership_records,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    load_live_trade_registry_record,
    make_live_trade_registry_event,
)

PAPER_ACCOUNT_ID = "DUM882026"
BROKER_POSITION_WITHOUT_CLOSE_ORDER = "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
POSITION_WITHOUT_CLOSE_ORDER = "POSITION_WITHOUT_CLOSE_ORDER"
BROKER_POSITION_REQUIRES_ADOPTION = "BROKER_POSITION_REQUIRES_ADOPTION"
BROKER_BACKED_ADOPTION_REQUIRED = "BROKER_BACKED_ADOPTION_REQUIRED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
DEFAULT_LANE_ID = "atp_companion_v1_pl_asia_us"
DEFAULT_SYMBOL = "PL"
DEFAULT_LOCAL_SYMBOL = "PLN6"
DEFAULT_EXPIRY = "20260729"
DEFAULT_QUANTITY = Decimal("1")
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "track_b_paper_lifecycle_adoption"
DEFAULT_LANE_ROOT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"
DEFAULT_BRIDGE_ROOT = Path("outputs") / "reports" / "ibkr_runtime_route_dispatch"
DEFAULT_BROKER_TRUTH_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_LEDGER_ROOT = Path("outputs") / "track_b_execution_core" / "paper_trade_ledger"
DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL


@dataclass(frozen=True)
class LifecycleAdoptionConfig:
    repo_root: Path
    lane_id: str = DEFAULT_LANE_ID
    order_intent_id: str | None = None
    account_id: str = PAPER_ACCOUNT_ID
    symbol: str = DEFAULT_SYMBOL
    local_symbol: str = DEFAULT_LOCAL_SYMBOL
    expiry: str = DEFAULT_EXPIRY
    quantity: Decimal = DEFAULT_QUANTITY
    apply: bool = False
    allow_leak_test_synthetic_intent: bool = False
    expected_broker_order_id: str | None = None
    expected_client_id: int | None = None
    expected_perm_id: int | None = None
    expected_exec_id: str | None = None
    expected_fill_price: Decimal | None = None
    output_root: Path = DEFAULT_OUTPUT_ROOT
    lane_root: Path = DEFAULT_LANE_ROOT
    bridge_root: Path = DEFAULT_BRIDGE_ROOT
    broker_truth_path: Path = DEFAULT_BROKER_TRUTH_PATH
    ledger_root: Path = DEFAULT_LEDGER_ROOT
    submit_intent_ownership_path: Path = DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH
    require_shared_truth_evidence: bool = True
    shared_truth_max_age_seconds: float = 600.0
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    require_control_plane_snapshot_for_apply: bool = True
    local_repair_snapshot_max_age_seconds: int = 300
    submit_intent_max_age_seconds: float = 7200.0


@dataclass(frozen=True)
class LifecycleAdoptionResult:
    classification: str
    report: dict[str, Any]
    audit_path: Path
    latest_audit_path: Path


def run_track_b_paper_lifecycle_adoption(
    *, config: LifecycleAdoptionConfig, now: datetime | None = None
) -> LifecycleAdoptionResult:
    actual_now = now or datetime.now(UTC)
    failures: list[str] = []
    repo_root = config.repo_root
    mode = "APPLY" if config.apply else "DRY_RUN"

    bridge_path = repo_root / config.bridge_root / config.lane_id / "ibkr_paper_strategy_bridge_report.json"
    broker_truth_path = repo_root / config.broker_truth_path
    lane_dir = repo_root / config.lane_root / config.lane_id
    order_intents_path = lane_dir / "order_intents.jsonl"
    fills_path = lane_dir / "fills.jsonl"
    trades_path = lane_dir / "trades.jsonl"
    submit_intent_ownership_path = repo_root / config.submit_intent_ownership_path

    broker_truth = _read_json(broker_truth_path)
    bridge_report = _read_json(bridge_path)
    intents = _read_jsonl(order_intents_path)
    submit_intent_ownership_records = _latest_submit_intent_ownership_records_for_adoption(
        load_submit_intent_ownership_records(submit_intent_ownership_path)
    )
    adapter = lane_submit_bridge_adapter(lane_id=config.lane_id)

    broker_position = _select_broker_position(
        broker_truth=broker_truth,
        account_id=config.account_id,
        symbol=config.symbol,
        local_symbol=config.local_symbol,
        expiry=config.expiry,
        quantity=config.quantity,
        failures=failures,
    )
    submit_intent_ownership = _select_submit_intent_ownership(
        records=submit_intent_ownership_records,
        account_id=config.account_id,
        lane_id=config.lane_id,
        symbol=config.symbol,
        local_symbol=config.local_symbol,
        expiry=config.expiry,
        quantity=config.quantity,
        expected_broker_order_id=config.expected_broker_order_id,
        expected_client_id=config.expected_client_id,
        expected_perm_id=config.expected_perm_id,
        failures=failures,
    )
    intent = _select_intent(
        intents=intents,
        bridge_report=bridge_report,
        submit_intent_ownership=submit_intent_ownership,
        lane_id=config.lane_id,
        order_intent_id=config.order_intent_id,
        symbol=config.symbol,
        quantity=config.quantity,
        allow_leak_test_synthetic_intent=config.allow_leak_test_synthetic_intent,
        failures=failures,
    )
    route_identity = _validate_route_identity(
        adapter=adapter,
        lane_id=config.lane_id,
        symbol=config.symbol,
        failures=failures,
    )
    broker_fill_evidence = _resolve_broker_fill_evidence_for_submit_intent(
        repo_root=repo_root,
        submit_intent_ownership=submit_intent_ownership,
        account_id=config.account_id,
        symbol=config.symbol,
        local_symbol=config.local_symbol,
        quantity=config.quantity,
        expected_broker_order_id=config.expected_broker_order_id,
        expected_client_id=config.expected_client_id,
        expected_perm_id=config.expected_perm_id,
        expected_exec_id=config.expected_exec_id,
    )
    resolved_broker_fill_evidence = (
        broker_fill_evidence
        if (broker_fill_evidence or {}).get("classification") == BROKER_FILL_EVIDENCE_RESOLVED
        and (broker_fill_evidence or {}).get("broker_backed_evidence_valid") is True
        else None
    )
    broker_average_price = _broker_average_price(broker_position)
    bridge_evidence = (
        _extract_submit_intent_ownership_evidence(
            submit_intent_ownership=submit_intent_ownership,
            bridge_report=bridge_report,
            broker_fill_evidence=resolved_broker_fill_evidence,
            broker_position=broker_position,
            broker_average_price=broker_average_price,
            account_id=config.account_id,
            symbol=config.symbol,
            local_symbol=config.local_symbol,
            expiry=config.expiry,
            quantity=config.quantity,
            expected_broker_order_id=config.expected_broker_order_id,
            expected_client_id=config.expected_client_id,
            expected_perm_id=config.expected_perm_id,
            expected_exec_id=config.expected_exec_id,
            expected_fill_price=config.expected_fill_price,
            failures=failures,
        )
        if submit_intent_ownership is not None
        else _extract_bridge_evidence(
            bridge_report=bridge_report,
            broker_position=broker_position,
            broker_average_price=broker_average_price,
            lane_id=config.lane_id,
            account_id=config.account_id,
            symbol=config.symbol,
            local_symbol=config.local_symbol,
            expiry=config.expiry,
            quantity=config.quantity,
            intent=intent,
            expected_broker_order_id=config.expected_broker_order_id,
            expected_client_id=config.expected_client_id,
            expected_perm_id=config.expected_perm_id,
            expected_exec_id=config.expected_exec_id,
            expected_fill_price=config.expected_fill_price,
            failures=failures,
        )
    )
    _validate_broker_observed_submit_ownership_adoption(
        config=config,
        submit_intent_ownership=submit_intent_ownership,
        broker_position=broker_position,
        bridge_evidence=bridge_evidence,
        now=actual_now,
        failures=failures,
    )

    fill_price = _decimal(bridge_evidence.get("fill_price")) if bridge_evidence else None
    cost_basis_adjustment = None
    if broker_average_price is not None and fill_price is not None:
        cost_basis_adjustment = _decimal_text(broker_average_price - fill_price)

    existing_fills = _read_jsonl(fills_path)
    existing_trades = _read_jsonl(trades_path)
    order_intent_id = str((intent or {}).get("order_intent_id") or config.order_intent_id or "")
    fill_payload = _build_fill_payload(
        config=config,
        intent=intent,
        bridge_evidence=bridge_evidence,
        broker_position=broker_position,
        broker_average_price=broker_average_price,
        broker_cost_basis_adjustment=cost_basis_adjustment,
        now=actual_now,
    )
    trade_payload = _build_trade_payload(fill_payload)
    filled_bridge_result = _build_filled_bridge_result(fill_payload)
    fill_already_present = _jsonl_has_identity(
        existing_fills,
        order_intent_id=order_intent_id,
        execution_id=str(fill_payload.get("execution_id") or ""),
    )
    trade_already_present = _jsonl_has_identity(
        existing_trades,
        order_intent_id=order_intent_id,
        execution_id=str(fill_payload.get("execution_id") or ""),
    )

    if str(config.account_id) != PAPER_ACCOUNT_ID:
        failures.append("PAPER account mismatch: adoption is restricted to DUM882026.")
    if fill_payload.get("live_money_eligible") is not False:
        failures.append("live_money_eligible must be false.")
    if not order_intent_id:
        failures.append("Missing order_intent_id after intent selection.")
    shared_truth_evidence = _shared_truth_adoption_evidence(
        config=config,
        broker_position=broker_position,
        submit_intent_ownership=submit_intent_ownership,
        order_intent_id=order_intent_id,
        now=actual_now,
    )
    failures.extend(shared_truth_evidence["blockers"])
    lifecycle_local_repair_guard = _lifecycle_local_repair_guard(
        config=config,
        trade_payload=trade_payload,
        fill_payload=fill_payload,
        now=actual_now,
    )
    if _control_plane_incoherence_allows_broker_backed_adoption(
        lifecycle_local_repair_guard=lifecycle_local_repair_guard,
        shared_truth_evidence=shared_truth_evidence,
    ):
        lifecycle_local_repair_guard = {
            **lifecycle_local_repair_guard,
            "valid": True,
            "classification": LIFECYCLE_LOCAL_REPAIR_VALID,
            "reason": "Control Plane Snapshot is incoherent only because this exact broker-backed lifecycle adoption is pending.",
            "blockers": [],
            "control_plane_circular_adoption_allowance": True,
        }
    if lifecycle_local_repair_guard["classification"] != LIFECYCLE_LOCAL_REPAIR_VALID:
        failures.append(f"Lifecycle State Matrix / Control Plane Snapshot guard blocked adoption: {lifecycle_local_repair_guard['classification']}.")

    valid = not failures
    classification = (
        "TRACK_B_PAPER_LIFECYCLE_ADOPTION_DRY_RUN_READY"
        if valid and not config.apply
        else "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
        if valid
        else "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    )
    write_plan = {
        "fills_jsonl": {
            "path": str(fills_path),
            "would_append": valid and not fill_already_present,
            "already_present": fill_already_present,
        },
        "trades_jsonl": {
            "path": str(trades_path),
            "would_append": valid and not trade_already_present,
            "already_present": trade_already_present,
        },
        "compact_ledger": {
            "path": str(repo_root / config.ledger_root),
            "would_update": valid,
            "already_present": None,
        },
        "submit_intent_ownership": {
            "path": str(submit_intent_ownership_path),
            "would_resolve": valid and submit_intent_ownership is not None,
            "already_present": submit_intent_ownership is None,
        },
    }
    report: dict[str, Any] = {
        "classification": classification,
        "adoption_input_classification": (bridge_evidence or {}).get("adoption_input_classification"),
        "mode": mode,
        "generated_at": actual_now.isoformat(),
        "paper_only": True,
        "live_money_eligible": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "place_order_attempted": False,
        "paper_proof_invoked": False,
        "broker_mutated": False,
        "account_id": config.account_id,
        "lane_id": config.lane_id,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "expiry": config.expiry,
        "quantity": _decimal_text(config.quantity),
        "order_intent_id": order_intent_id,
        "failures": failures,
        "route_identity": route_identity,
        "broker_truth": {
            "path": str(broker_truth_path),
            "snapshot_time": broker_truth.get("generated_at"),
            "position": broker_position,
        },
        "bridge_evidence": {
            "path": str(bridge_path),
            "selected": bridge_evidence,
            "broker_fill_evidence": broker_fill_evidence,
        },
        "submit_intent_ownership_evidence": {
            "path": str(submit_intent_ownership_path),
            "selected": submit_intent_ownership,
            "unresolved_record_count": len(submit_intent_ownership_records),
        },
        "shared_truth_evidence": shared_truth_evidence,
        "broker_session_authority_classification": shared_truth_evidence.get(
            "broker_session_authority_classification"
        ),
        "broker_session_connection_mode": shared_truth_evidence.get("broker_session_connection_mode"),
        "broker_session_allowed_uses": shared_truth_evidence.get("broker_session_allowed_uses"),
        "broker_session_authority_blockers": shared_truth_evidence.get("broker_session_authority_blockers"),
        "callback_ownership_attribution": shared_truth_evidence.get("callback_ownership_attribution"),
        "callback_missing_reason": shared_truth_evidence.get("callback_missing_reason"),
        "callback_missing_reasons": shared_truth_evidence.get("callback_missing_reasons"),
        "broker_observed_adoption_diagnosis_allowed": shared_truth_evidence.get(
            "broker_observed_adoption_diagnosis_allowed"
        ),
        "broker_observed_adoption_apply_allowed": shared_truth_evidence.get(
            "broker_observed_adoption_apply_allowed"
        ),
        "lifecycle_local_repair_guard": lifecycle_local_repair_guard,
        "control_plane_snapshot_id": lifecycle_local_repair_guard.get("control_plane_snapshot_id"),
        "shared_truth_refresh_generation_id": lifecycle_local_repair_guard.get("shared_truth_refresh_generation_id"),
        "intent_evidence": {
            "path": str(order_intents_path),
            "selected": intent,
        },
        "broker_cost_basis_adjustment": {
            "broker_average_price": _decimal_text(broker_average_price),
            "lifecycle_fill_price": _decimal_text(fill_price),
            "difference": cost_basis_adjustment,
            "classification": "BROKER_COST_BASIS_ADJUSTMENT_RECORDED_NOT_BLOCKING",
        },
        "write_plan": write_plan,
        "fill_payload": fill_payload if valid else None,
        "trade_payload": trade_payload if valid else None,
        "filled_bridge_result_payload": filled_bridge_result if valid else None,
    }

    audit_path = _write_audit(repo_root, config.output_root, report, actual_now=actual_now)
    report["audit_path"] = str(audit_path)
    latest_audit_path = _write_latest_audit(repo_root, config.output_root, report)

    if valid and config.apply:
        registry_fill_event = _append_adoption_entry_fill_registry_event(
            config=config,
            fill_payload=fill_payload,
            trade_payload=trade_payload,
            audit_path=audit_path,
            now=actual_now,
        )
        _append_jsonl_if_missing(
            fills_path,
            fill_payload,
            order_intent_id=order_intent_id,
            execution_id=str(fill_payload.get("execution_id") or ""),
        )
        _append_jsonl_if_missing(
            trades_path,
            trade_payload,
            order_intent_id=order_intent_id,
            execution_id=str(fill_payload.get("execution_id") or ""),
        )
        ledger_result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
            filled_bridge_result=filled_bridge_result,
            filled_bridge_result_json=audit_path,
            output_root=repo_root / config.ledger_root,
            position_management_manifest_root=repo_root / DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
            managed_lifecycle_output_root=repo_root / DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
            now=actual_now,
        )
        ownership_resolution = _resolve_submit_intent_ownership_after_adoption(
            submit_intent_ownership=submit_intent_ownership,
            fill_payload=fill_payload,
            trade_payload=trade_payload,
            audit_path=audit_path,
            jsonl_path=submit_intent_ownership_path,
            now=actual_now,
        )
        post_report = dict(report)
        post_report["classification"] = "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
        post_report["post_adoption"] = {
            "fill_written": not fill_already_present,
            "trade_written": not trade_already_present,
            "registry_entry_fill_event": registry_fill_event,
            "compact_ledger_trade_record_written": ledger_result.trade_record_written,
            "compact_ledger_live_position_status": ledger_result.live_position_status,
            "submit_intent_ownership_resolved": ownership_resolution is not None,
            "submit_intent_ownership_resolution": ownership_resolution,
        }
        post_report["audit_path"] = str(audit_path)
        audit_path = _write_audit(repo_root, config.output_root, post_report, actual_now=actual_now, suffix="post")
        latest_audit_path = _write_latest_audit(repo_root, config.output_root, post_report)
        report = post_report

    return LifecycleAdoptionResult(
        classification=classification,
        report=report,
        audit_path=audit_path,
        latest_audit_path=latest_audit_path,
    )


def _resolve_submit_intent_ownership_after_adoption(
    *,
    submit_intent_ownership: Mapping[str, Any] | None,
    fill_payload: Mapping[str, Any],
    trade_payload: Mapping[str, Any],
    audit_path: Path,
    jsonl_path: Path,
    now: datetime,
) -> dict[str, Any] | None:
    if submit_intent_ownership is None:
        return None
    lifecycle_id = str(trade_payload.get("lifecycle_id") or fill_payload.get("lifecycle_id") or "")
    if not lifecycle_id:
        return None
    resolved = dict(submit_intent_ownership)
    resolved["state"] = "LIFECYCLE_OPEN_PERSISTED"
    resolved["updated_at"] = now.isoformat()
    resolved["lifecycle_id"] = lifecycle_id
    resolved["lifecycle_id_reserved_only"] = False
    resolved["lifecycle_position_open"] = True
    resolved["open_order_count"] = 0
    resolved["unknown_open_order_count"] = 0
    resolved["review_required_count"] = 0
    resolved["live_money_eligible"] = False
    resolved["paper_proof_invoked"] = False
    resolved["broker_order_id"] = str(fill_payload.get("broker_order_id") or resolved.get("broker_order_id") or "")
    resolved["client_id"] = fill_payload.get("client_id") or resolved.get("client_id")
    resolved["perm_id"] = fill_payload.get("perm_id") or resolved.get("perm_id")
    resolved["exec_id"] = fill_payload.get("execution_id") or fill_payload.get("exec_id") or resolved.get("exec_id")
    source_paths = [str(path) for path in (resolved.get("source_artifact_paths") or [])]
    source_paths.append(str(audit_path))
    resolved["source_artifact_paths"] = tuple(dict.fromkeys(source_paths))
    extra = dict(resolved.get("extra") or {})
    extra.update(
        {
            "adoption_classification": "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED",
            "adoption_resolution": "BROKER_BACKED_ENTRY_ADOPTED_FROM_BROKER_POSITION",
            "adoption_audit_path": str(audit_path),
            "lifecycle_id": lifecycle_id,
        }
    )
    resolved["extra"] = extra
    latest_path = jsonl_path.parent / "latest_track_b_submit_intent_ownership.json"
    store_result = append_submit_intent_ownership_record(resolved, jsonl_path=jsonl_path, latest_path=latest_path)
    return {
        "classification": "SUBMIT_INTENT_OWNERSHIP_RESOLVED_LIFECYCLE_OPEN_PERSISTED",
        "ownership_intent_id": store_result.record.get("ownership_intent_id"),
        "state": store_result.record.get("state"),
        "lifecycle_id": store_result.record.get("lifecycle_id"),
        "jsonl_path": str(store_result.jsonl_path),
        "latest_path": str(store_result.latest_path),
    }


def _select_broker_position(
    *,
    broker_truth: Mapping[str, Any],
    account_id: str,
    symbol: str,
    local_symbol: str,
    expiry: str,
    quantity: Decimal,
    failures: list[str],
) -> dict[str, Any] | None:
    if not broker_truth:
        failures.append("Missing broker truth position snapshot.")
        return None
    if str(broker_truth.get("mode") or "").upper() != "PAPER":
        failures.append("Broker truth snapshot is not PAPER mode.")
    if str(broker_truth.get("account") or broker_truth.get("selected_account_id") or "") != account_id:
        failures.append("Broker truth snapshot account does not match expected PAPER account.")
    positions = broker_truth.get("positions") if isinstance(broker_truth.get("positions"), list) else []
    matches = [
        dict(item)
        for item in positions
        if str(item.get("account_id") or "") == account_id
        and str(item.get("symbol") or "").upper() == symbol.upper()
        and str(item.get("local_symbol") or "").upper() == local_symbol.upper()
        and str(item.get("expiry") or "") == str(expiry)
        and _decimal(item.get("quantity")) == quantity
    ]
    if len(matches) != 1:
        failures.append(f"Expected exactly one matching broker position, found {len(matches)}.")
        return matches[0] if matches else None
    return matches[0]


def _select_intent(
    *,
    intents: Sequence[Mapping[str, Any]],
    bridge_report: Mapping[str, Any],
    submit_intent_ownership: Mapping[str, Any] | None,
    lane_id: str,
    order_intent_id: str | None,
    symbol: str,
    quantity: Decimal,
    allow_leak_test_synthetic_intent: bool,
    failures: list[str],
) -> dict[str, Any] | None:
    rows = [
        dict(row)
        for row in intents
        if str(row.get("lane_id") or "") == lane_id
        and str(row.get("symbol") or "").upper() == symbol.upper()
        and _decimal(row.get("quantity")) == quantity
        and str(row.get("broker_order_status") or "").upper() == "FILLED"
        and str(row.get("intent_type") or "").upper() in {"BUY_TO_OPEN", "SELL_TO_OPEN"}
    ]
    if order_intent_id:
        rows = [row for row in rows if str(row.get("order_intent_id") or "") == order_intent_id]
    if len(rows) != 1 and submit_intent_ownership is not None:
        return _intent_from_submit_intent_ownership(
            submit_intent_ownership=submit_intent_ownership,
            lane_id=lane_id,
            order_intent_id=order_intent_id,
            symbol=symbol,
            quantity=quantity,
        )
    if len(rows) != 1 and allow_leak_test_synthetic_intent:
        synthetic = _synthetic_leak_test_intent_from_bridge(
            bridge_report=bridge_report,
            lane_id=lane_id,
            order_intent_id=order_intent_id,
            symbol=symbol,
            quantity=quantity,
            failures=failures,
        )
        if synthetic is not None:
            return synthetic
    if len(rows) != 1:
        failures.append(f"Expected exactly one filled matching order intent, found {len(rows)}.")
        return rows[0] if rows else None
    return rows[0]


def _intent_from_submit_intent_ownership(
    *,
    submit_intent_ownership: Mapping[str, Any],
    lane_id: str,
    order_intent_id: str | None,
    symbol: str,
    quantity: Decimal,
) -> dict[str, Any]:
    ownership_intent_id = str(submit_intent_ownership.get("ownership_intent_id") or "")
    intent_type = str(submit_intent_ownership.get("intent_type") or "BUY_TO_OPEN").upper()
    return {
        "order_intent_id": order_intent_id or ownership_intent_id,
        "lane_id": lane_id,
        "strategy_id": submit_intent_ownership.get("strategy_id") or lane_id,
        "standalone_strategy_id": submit_intent_ownership.get("strategy_id") or lane_id,
        "symbol": symbol,
        "instrument": symbol,
        "intent_type": intent_type,
        "quantity": _decimal_text(quantity),
        "broker_order_id": submit_intent_ownership.get("broker_order_id"),
        "broker_order_status": "FILLED",
        "reason_code": _nested(submit_intent_ownership, "extra", "reason") or "LEAK_TEST_ENTRY",
        "managed_exit_policy_id": submit_intent_ownership.get("managed_exit_policy_id")
        or _nested(submit_intent_ownership, "extra", "managed_exit_policy_id")
        or _nested(submit_intent_ownership, "extra", "caller_metadata", "managed_exit_policy_id"),
        "decision_bar_timestamp": _nested(submit_intent_ownership, "extra", "caller_metadata", "decision_bar_timestamp")
        or _nested(submit_intent_ownership, "extra", "caller_metadata", "runtime_candle_timestamp")
        or submit_intent_ownership.get("created_at"),
        "signal_timestamp": _nested(submit_intent_ownership, "extra", "caller_metadata", "signal_timestamp")
        or submit_intent_ownership.get("created_at"),
        "created_at": submit_intent_ownership.get("created_at"),
        "trade_id": _single_trade_id_from_sources(submit_intent_ownership),
        "ownership_intent_id": ownership_intent_id,
        "reserved_lifecycle_id": submit_intent_ownership.get("lifecycle_id"),
        "source": "TRACK_B_SUBMIT_INTENT_OWNERSHIP",
    }


def _synthetic_leak_test_intent_from_bridge(
    *,
    bridge_report: Mapping[str, Any],
    lane_id: str,
    order_intent_id: str | None,
    symbol: str,
    quantity: Decimal,
    failures: list[str],
) -> dict[str, Any] | None:
    intent_payload = bridge_report.get("intent") if isinstance(bridge_report.get("intent"), Mapping) else {}
    caller_metadata = bridge_report.get("caller_metadata") if isinstance(bridge_report.get("caller_metadata"), Mapping) else {}
    delegated = bridge_report.get("delegated_result") if isinstance(bridge_report.get("delegated_result"), Mapping) else {}
    if not intent_payload:
        return None
    if caller_metadata.get("leak_test") is not True and "TRACK_B_LEAK_TEST" not in set(intent_payload.get("risk_tags") or []):
        return None
    leak_test_fill_classes = {
        "PAPER_STRATEGY_ORDER_FILLED",
        "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
    }
    delegated_fill_classes = {
        "PAPER_ORDER_FILLED",
        "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
    }
    if str(bridge_report.get("classification") or "") not in leak_test_fill_classes:
        return None
    if str(delegated.get("classification") or "") not in delegated_fill_classes:
        return None
    if str(intent_payload.get("strategy_id") or "") != lane_id:
        failures.append("Synthetic leak-test intent bridge lane mismatch.")
        return None
    if str(intent_payload.get("symbol") or "").upper() != symbol.upper():
        failures.append("Synthetic leak-test intent symbol mismatch.")
        return None
    if _decimal(intent_payload.get("quantity")) != quantity:
        failures.append("Synthetic leak-test intent quantity mismatch.")
        return None
    bridge_intent_id = str(intent_payload.get("intent_id") or "")
    if order_intent_id and bridge_intent_id and order_intent_id != bridge_intent_id:
        failures.append("Synthetic leak-test intent id mismatch.")
        return None
    intent_type = str(caller_metadata.get("intent_type") or intent_payload.get("intent_type") or "BUY_TO_OPEN").upper()
    return {
        "order_intent_id": order_intent_id or bridge_intent_id,
        "lane_id": lane_id,
        "strategy_id": lane_id,
        "standalone_strategy_id": caller_metadata.get("strategy_id") or lane_id,
        "symbol": symbol,
        "instrument": symbol,
        "intent_type": intent_type,
        "quantity": _decimal_text(quantity),
        "broker_order_id": _nested(bridge_report, "delegated_result", "report", "submit_cancel_lifecycle", "submitted_order_id")
        or _nested(bridge_report, "delegated_result", "report", "submit_cancel_lifecycle", "latest_order_status", "order_id"),
        "broker_order_status": "FILLED",
        "reason_code": intent_payload.get("reason") or "LEAK_TEST_ENTRY",
        "managed_exit_policy_id": intent_payload.get("managed_exit_policy_id")
        or caller_metadata.get("managed_exit_policy_id"),
        "decision_bar_timestamp": intent_payload.get("timestamp"),
        "signal_timestamp": intent_payload.get("timestamp"),
        "synthetic_leak_test_intent": True,
        "source": "TRACK_B_PAPER_LEAK_TEST_BRIDGE_REPORT",
    }


def _select_submit_intent_ownership(
    *,
    records: Sequence[Mapping[str, Any]],
    account_id: str,
    lane_id: str,
    symbol: str,
    local_symbol: str,
    expiry: str,
    quantity: Decimal,
    expected_broker_order_id: str | None,
    expected_client_id: int | None,
    expected_perm_id: int | None,
    failures: list[str],
) -> dict[str, Any] | None:
    target_action = "BUY" if quantity > 0 else "SELL"
    target_contract_records = [
        dict(row)
        for row in records
        if str(row.get("symbol") or "").upper() == symbol.upper()
        and str(row.get("local_symbol") or "").upper() == local_symbol.upper()
        and str(row.get("expiry") or "") == str(expiry)
    ]
    target_lane_records = [
        row
        for row in target_contract_records
        if str(row.get("mode") or "").upper() == "PAPER"
        and str(row.get("account_id") or "") == account_id
        and str(row.get("lane_id") or "") == lane_id
    ]
    unsafe_records = [
        row
        for row in target_contract_records
        if row.get("live_money_eligible") is not False or row.get("paper_proof_invoked") is not False
    ]
    if unsafe_records:
        failures.append("Submit-intent ownership record has unsafe live_money_eligible/paper_proof flags.")
        return None

    matches = [
        row
        for row in target_lane_records
        if _decimal(row.get("qty")) == abs(quantity)
        and str(row.get("action") or "").upper() == target_action
        and str(row.get("intent_type") or "").upper() in {"BUY_TO_OPEN", "SELL_TO_OPEN", "ENTRY"}
    ]
    if expected_broker_order_id is not None:
        matches = [row for row in matches if str(row.get("broker_order_id") or "") == str(expected_broker_order_id)]
    if expected_client_id is not None:
        matches = [row for row in matches if _decimal(row.get("client_id")) == Decimal(expected_client_id)]
    if expected_perm_id is not None:
        matches = [row for row in matches if _decimal(row.get("perm_id")) == Decimal(expected_perm_id)]

    adoptable_states = {
        "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED",
        "BROKER_ORDER_WORKING",
        "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED",
        "LIFECYCLE_OPEN_PERSISTED",
    }
    matches = [row for row in matches if str(row.get("state") or "").upper() in adoptable_states]
    if len(matches) > 1:
        failures.append(
            "Competing unresolved submit-intent ownership records match the same account/contract/side adoption window."
        )
        return None
    if len(matches) == 1:
        return matches[0]
    if target_lane_records:
        failures.append("Submit-intent ownership record exists for target contract but account/qty/side/order identity did not match.")
    return None


def _latest_submit_intent_ownership_records_for_adoption(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    latest_by_id: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)
        ownership_id = str(row.get("ownership_intent_id") or "").strip()
        if ownership_id:
            latest_by_id[ownership_id] = row
        else:
            anonymous.append(row)
    return [*latest_by_id.values(), *anonymous]


def _resolve_broker_fill_evidence_for_submit_intent(
    *,
    repo_root: Path,
    submit_intent_ownership: Mapping[str, Any] | None,
    account_id: str,
    symbol: str,
    local_symbol: str,
    quantity: Decimal,
    expected_broker_order_id: str | None,
    expected_client_id: int | None,
    expected_perm_id: int | None,
    expected_exec_id: str | None,
) -> dict[str, Any] | None:
    if not isinstance(submit_intent_ownership, Mapping):
        return None
    trade_id = _single_trade_id_from_sources(submit_intent_ownership)
    if trade_id == "__AMBIGUOUS__":
        trade_id = ""
    action = "BUY" if quantity > 0 else "SELL"
    result = resolve_broker_backed_fill_evidence(
        repo_root=repo_root,
        request=BrokerFillEvidenceRequest(
            trade_id=trade_id or None,
            submit_intent_id=submit_intent_ownership.get("ownership_intent_id"),
            order_id=expected_broker_order_id or submit_intent_ownership.get("broker_order_id"),
            client_id=expected_client_id if expected_client_id is not None else submit_intent_ownership.get("client_id"),
            perm_id=expected_perm_id if expected_perm_id is not None else submit_intent_ownership.get("perm_id"),
            con_id=submit_intent_ownership.get("con_id"),
            local_symbol=local_symbol,
            account_id=account_id,
            action=action,
            qty=abs(quantity),
            symbol=symbol,
        ),
    )
    if result.classification != BROKER_FILL_EVIDENCE_RESOLVED or not result.evidence:
        return {
            "classification": result.classification,
            "broker_backed_evidence_valid": result.broker_backed_evidence_valid,
            "reason_codes": list(result.reason_codes),
            "match_count": len(result.matches),
            "searched_paths": list(result.searched_paths),
        }
    evidence = dict(result.evidence)
    evidence["classification"] = result.classification
    evidence["broker_backed_evidence_valid"] = result.broker_backed_evidence_valid
    evidence["reason_codes"] = list(result.reason_codes)
    evidence["match_count"] = len(result.matches)
    return evidence


def _extract_submit_intent_ownership_evidence(
    *,
    submit_intent_ownership: Mapping[str, Any],
    bridge_report: Mapping[str, Any],
    broker_fill_evidence: Mapping[str, Any] | None,
    broker_position: Mapping[str, Any] | None,
    broker_average_price: Decimal | None,
    account_id: str,
    symbol: str,
    local_symbol: str,
    expiry: str,
    quantity: Decimal,
    expected_broker_order_id: str | None,
    expected_client_id: int | None,
    expected_perm_id: int | None,
    expected_exec_id: str | None,
    expected_fill_price: Decimal | None,
    failures: list[str],
) -> dict[str, Any] | None:
    local_failures: list[str] = []
    matching_bridge_report = (
        bridge_report
        if _bridge_report_matches_submit_intent_ownership(
            bridge_report=bridge_report,
            submit_intent_ownership=submit_intent_ownership,
        )
        else {}
    )
    if broker_position is None:
        local_failures.append("Submit-intent adoption requires exact broker position truth.")
    if str(submit_intent_ownership.get("mode") or "").upper() != "PAPER":
        local_failures.append("Submit-intent ownership record is not PAPER mode.")
    if str(submit_intent_ownership.get("account_id") or "") != account_id:
        local_failures.append("Submit-intent ownership account does not match expected PAPER account.")
    if submit_intent_ownership.get("live_money_eligible") is not False:
        local_failures.append("Submit-intent ownership live_money_eligible must be false.")
    if submit_intent_ownership.get("paper_proof_invoked") is not False:
        local_failures.append("Submit-intent ownership paper_proof_invoked must be false.")
    if str(submit_intent_ownership.get("symbol") or "").upper() != symbol.upper():
        local_failures.append("Submit-intent ownership symbol mismatch.")
    if str(submit_intent_ownership.get("local_symbol") or "").upper() != local_symbol.upper():
        local_failures.append("Submit-intent ownership local_symbol mismatch.")
    if str(submit_intent_ownership.get("expiry") or "") != str(expiry):
        local_failures.append("Submit-intent ownership expiry mismatch.")
    if _decimal(submit_intent_ownership.get("qty")) != abs(quantity):
        local_failures.append("Submit-intent ownership quantity mismatch.")
    target_action = "BUY" if quantity > 0 else "SELL"
    if str(submit_intent_ownership.get("action") or "").upper() != target_action:
        local_failures.append("Submit-intent ownership action is not consistent with broker position side.")
    broker_order_id = str(submit_intent_ownership.get("broker_order_id") or "")
    if expected_broker_order_id is not None and broker_order_id != str(expected_broker_order_id):
        local_failures.append("Submit-intent ownership broker_order_id does not match expected broker_order_id.")
    client_id = submit_intent_ownership.get("client_id")
    if expected_client_id is not None and _decimal(client_id) != Decimal(expected_client_id):
        local_failures.append("Submit-intent ownership client_id does not match expected client_id.")
    perm_id = submit_intent_ownership.get("perm_id")
    bridge_lifecycle = _nested(matching_bridge_report, "delegated_result", "report", "submit_cancel_lifecycle")
    bridge_lifecycle = bridge_lifecycle if isinstance(bridge_lifecycle, Mapping) else {}
    bridge_latest_status = (
        bridge_lifecycle.get("latest_order_status")
        if isinstance(bridge_lifecycle.get("latest_order_status"), Mapping)
        else {}
    )
    bridge_execution = _select_exact_bridge_execution_for_submit_ownership(
        bridge_report=matching_bridge_report,
        account_id=account_id,
        symbol=symbol,
        quantity=quantity,
        expected_exec_id=expected_exec_id,
        failures=local_failures,
    )
    if expected_perm_id is not None and _decimal(perm_id) != Decimal(expected_perm_id):
        local_failures.append("Submit-intent ownership perm_id does not match expected perm_id.")
    original_trade_id = _single_trade_id_from_sources(
        submit_intent_ownership,
        None if broker_fill_evidence else matching_bridge_report,
    )
    if original_trade_id == "__AMBIGUOUS__":
        local_failures.append("Multiple distinct registry trade_id values match the broker-backed fill.")
        original_trade_id = ""
    execution_id = submit_intent_ownership.get("exec_id") or (
        (broker_fill_evidence or {}).get("exec_id")
        or (broker_fill_evidence or {}).get("execution_id")
        or (bridge_execution.get("execution_id") if bridge_execution is not None else None)
    )
    if expected_exec_id is not None and str(execution_id or "") != str(expected_exec_id):
        local_failures.append("Submit-intent ownership exec_id does not match expected exec_id.")
    expected_fill_decimal = _decimal(expected_fill_price)
    if (
        expected_fill_decimal is not None
        and broker_fill_evidence is None
        and broker_average_price is not None
        and broker_average_price != expected_fill_decimal
    ):
        local_failures.append("Broker average price does not match expected fill_price.")
    if local_failures:
        failures.extend(local_failures)
        return None

    execution_price = _decimal((bridge_execution or {}).get("price"))
    broker_execution_price = _decimal((broker_fill_evidence or {}).get("price"))
    fill_price = expected_fill_decimal or broker_execution_price or execution_price or broker_average_price
    bridge_fill_timestamp = (
        (broker_fill_evidence or {}).get("fill_timestamp")
        or (broker_fill_evidence or {}).get("executed_at")
        or (bridge_execution or {}).get("executed_at")
    )
    managed_exit_policy_id = (
        submit_intent_ownership.get("managed_exit_policy_id")
        or _nested(submit_intent_ownership, "extra", "managed_exit_policy_id")
        or _nested(submit_intent_ownership, "extra", "caller_metadata", "managed_exit_policy_id")
        or _nested(matching_bridge_report, "intent", "managed_exit_policy_id")
        or _nested(matching_bridge_report, "caller_metadata", "managed_exit_policy_id")
    )
    missing_fields = [
        field
        for field, value in (
            ("broker_order_id", broker_order_id),
            ("client_id", client_id),
            ("perm_id", perm_id),
            ("execution_id", execution_id),
            ("managed_exit_policy_id", managed_exit_policy_id),
        )
        if value in {None, ""}
    ]
    return {
        "account_id": account_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "contract_month": expiry[:6],
        "multiplier": str((broker_position or {}).get("multiplier") or ""),
        "con_id": submit_intent_ownership.get("con_id"),
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "client_id": client_id,
        "execution_id": execution_id,
        "runtime_generation_id": _find_first_by_key(submit_intent_ownership, "runtime_generation_id"),
        "trade_id": original_trade_id,
        "submit_intent_created_at": submit_intent_ownership.get("created_at"),
        "strategy_entry_bar_ts": _nested(submit_intent_ownership, "extra", "caller_metadata", "runtime_candle_timestamp")
        or _nested(submit_intent_ownership, "extra", "caller_metadata", "decision_bar_timestamp")
        or _nested(bridge_report, "entry_execution_pricing", "runtime_candle_timestamp"),
        "fill_price": _decimal_text(fill_price) or "",
        "fill_price_source": (
            "BROKER_EXECUTION_EVIDENCE"
            if broker_execution_price is not None
            else "BRIDGE_EXECUTION_EVIDENCE"
            if execution_price is not None
            else "BROKER_POSITION_AVERAGE_PRICE"
        ),
        "fill_timestamp": bridge_fill_timestamp or (broker_position or {}).get("updated_at") or submit_intent_ownership.get("updated_at"),
        "order_status_updated_at": bridge_latest_status.get("updated_at") or submit_intent_ownership.get("updated_at"),
        "latest_order_status": dict(bridge_latest_status),
        "selected_execution": dict(broker_fill_evidence or bridge_execution or {}),
        "entry_execution_intent": _nested(submit_intent_ownership, "extra", "entry_execution_intent")
        or _nested(submit_intent_ownership, "extra", "caller_metadata", "entry_execution_intent")
        or _nested(bridge_report, "entry_execution_pricing", "entry_execution_intent")
        or _nested(bridge_report, "caller_metadata", "entry_execution_intent"),
        "entry_price_source": submit_intent_ownership.get("execution_price_source")
        or _nested(bridge_report, "entry_execution_pricing", "execution_price_source"),
        "managed_exit_policy_id": managed_exit_policy_id,
        "leak_test": str(submit_intent_ownership.get("caller_path") or "") == "track_b_paper_leak_test_apply",
        "authorization_digest": submit_intent_ownership.get("authorization_digest")
        or _nested(bridge_report, "caller_metadata", "authorization_digest"),
        "ownership_intent_id": submit_intent_ownership.get("ownership_intent_id"),
        "reserved_lifecycle_id": submit_intent_ownership.get("lifecycle_id"),
        "submit_intent_state": submit_intent_ownership.get("state"),
        "evidence_classification": (
            "SUBMIT_INTENT_BROKER_EXECUTION_CONFIRMED"
            if broker_fill_evidence
            else "SUBMIT_INTENT_BROKER_POSITION_CONFIRMED_PARTIAL_IDENTITY"
        ),
        "adoption_input_classification": "SUBMIT_INTENT_BROKER_BACKED_ENTRY_REQUIRES_LIFECYCLE_ADOPTION",
        "broker_position_confirmed": True,
        "identity_completeness": "PARTIAL" if missing_fields else "COMPLETE",
        "missing_broker_identity_fields": missing_fields,
        "delegated_classification": _nested(submit_intent_ownership, "extra", "delegated_classification"),
        "bridge_classification": _nested(submit_intent_ownership, "extra", "bridge_classification"),
        "source_artifact_paths": submit_intent_ownership.get("source_artifact_paths") or [],
    }


def _validate_broker_observed_submit_ownership_adoption(
    *,
    config: LifecycleAdoptionConfig,
    submit_intent_ownership: Mapping[str, Any] | None,
    broker_position: Mapping[str, Any] | None,
    bridge_evidence: Mapping[str, Any] | None,
    now: datetime,
    failures: list[str],
) -> None:
    if not isinstance(submit_intent_ownership, Mapping):
        return
    if not isinstance(broker_position, Mapping):
        return

    lifecycle_id = str(submit_intent_ownership.get("lifecycle_id") or "").strip()
    if not lifecycle_id:
        failures.append("Broker-observed adoption requires a matching reserved lifecycle_id.")
    if (
        str(submit_intent_ownership.get("state") or "").upper() != "LIFECYCLE_OPEN_PERSISTED"
        and submit_intent_ownership.get("lifecycle_id_reserved_only") is not True
    ):
        failures.append("Broker-observed adoption requires reserved-only lifecycle ownership before promotion.")
    trade_id = _single_trade_id_from_sources(submit_intent_ownership)
    submit_state = str(submit_intent_ownership.get("state") or "").upper()
    if not trade_id and submit_state == "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED":
        failures.append("Broker-observed adoption requires a matching trade_id in submit ownership.")
    if trade_id == "__AMBIGUOUS__":
        failures.append("Broker-observed adoption requires one unambiguous trade_id in submit ownership.")

    age_seconds = _age_seconds(submit_intent_ownership.get("created_at"), now)
    if age_seconds is None:
        failures.append("Broker-observed adoption requires a parseable submit ownership created_at timestamp.")
    elif age_seconds > config.submit_intent_max_age_seconds:
        failures.append(
            "Broker-observed adoption refused stale submit ownership: "
            f"age_seconds={age_seconds:.3f} max_age_seconds={config.submit_intent_max_age_seconds:.3f}."
        )

    if _decimal(submit_intent_ownership.get("open_order_count")) not in {None, Decimal("0")}:
        failures.append("Broker-observed adoption requires submit ownership open_order_count=0.")
    if _decimal(submit_intent_ownership.get("unknown_open_order_count")) not in {None, Decimal("0")}:
        failures.append("Broker-observed adoption requires submit ownership unknown_open_order_count=0.")
    if _decimal(submit_intent_ownership.get("review_required_count")) not in {None, Decimal("0")}:
        failures.append("Broker-observed adoption requires submit ownership review_required_count=0.")

    for reason_field in ("governance_block_reasons", "exposure_block_reasons"):
        reasons = _nested(submit_intent_ownership, "extra", reason_field)
        if isinstance(reasons, list) and reasons:
            failures.append(f"Broker-observed adoption refuses submit ownership with {reason_field}.")

    broker_con_id = _decimal(broker_position.get("con_id") or broker_position.get("conId"))
    ownership_con_id = _decimal(submit_intent_ownership.get("con_id"))
    if broker_con_id is not None and ownership_con_id is not None and broker_con_id != ownership_con_id:
        failures.append("Broker-observed adoption broker con_id does not match submit ownership con_id.")
    if str(broker_position.get("account_id") or "") != config.account_id:
        failures.append("Broker-observed adoption broker account does not match expected account.")
    if str(broker_position.get("local_symbol") or "").upper() != config.local_symbol.upper():
        failures.append("Broker-observed adoption broker local_symbol does not match expected contract.")
    if _decimal(broker_position.get("quantity")) != _decimal(config.quantity):
        failures.append("Broker-observed adoption broker side/quantity does not match expected adoption quantity.")

    if bridge_evidence and bridge_evidence.get("broker_position_confirmed") is not True:
        failures.append("Broker-observed adoption requires broker_position_confirmed evidence.")


def _bridge_report_matches_submit_intent_ownership(
    *,
    bridge_report: Mapping[str, Any],
    submit_intent_ownership: Mapping[str, Any],
) -> bool:
    if not isinstance(bridge_report, Mapping) or not bridge_report:
        return False
    ownership_ids = {
        str(value).strip()
        for value in (
            submit_intent_ownership.get("ownership_intent_id"),
            _nested(submit_intent_ownership, "extra", "intent_id"),
            _nested(submit_intent_ownership, "extra", "trade_id"),
            _nested(submit_intent_ownership, "extra", "caller_metadata", "trade_id"),
        )
        if value not in {None, ""}
    }
    bridge_ids = {
        str(value).strip()
        for value in (
            _nested(bridge_report, "intent", "intent_id"),
            _nested(bridge_report, "caller_metadata", "trade_id"),
            _nested(bridge_report, "intent", "trade_id"),
        )
        if value not in {None, ""}
    }
    if ownership_ids & bridge_ids:
        return True

    ownership_order_id = str(submit_intent_ownership.get("broker_order_id") or "").strip()
    if not ownership_order_id:
        return False
    lifecycle = _nested(bridge_report, "delegated_result", "report", "submit_cancel_lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, Mapping) else {}
    latest_status = lifecycle.get("latest_order_status") if isinstance(lifecycle.get("latest_order_status"), Mapping) else {}
    delegated = bridge_report.get("delegated_result") if isinstance(bridge_report.get("delegated_result"), Mapping) else {}
    return ownership_order_id in _bridge_known_order_ids(
        lifecycle=lifecycle,
        latest_status=latest_status,
        delegated=delegated,
    )


def _select_exact_bridge_execution_for_submit_ownership(
    *,
    bridge_report: Mapping[str, Any],
    account_id: str,
    symbol: str,
    quantity: Decimal,
    expected_exec_id: str | None,
    failures: list[str],
) -> dict[str, Any] | None:
    lifecycle = _nested(bridge_report, "delegated_result", "report", "submit_cancel_lifecycle")
    if not isinstance(lifecycle, Mapping):
        return None
    executions = _bridge_execution_rows(lifecycle)
    matching: dict[str, dict[str, Any]] = {}
    for execution in executions:
        if str(execution.get("account_id") or "") != account_id:
            continue
        if str(execution.get("symbol") or "").upper() != symbol.upper():
            continue
        if _decimal(execution.get("quantity")) != quantity:
            continue
        execution_id = str(execution.get("execution_id") or "")
        if expected_exec_id is not None and execution_id != str(expected_exec_id):
            continue
        matching[execution_id or json.dumps(execution, sort_keys=True)] = dict(execution)
    if len(matching) == 1:
        return next(iter(matching.values()))
    if expected_exec_id is not None or matching:
        failures.append(
            f"Expected exactly one unique matching bridge execution for submit-intent ownership, found {len(matching)}."
        )
    return None


def _validate_route_identity(
    *,
    adapter: Mapping[str, Any] | None,
    lane_id: str,
    symbol: str,
    failures: list[str],
) -> dict[str, Any] | None:
    if not isinstance(adapter, Mapping):
        failures.append("Missing submit-capable route adapter for lane.")
        return None
    route_identity = dict(adapter)
    if str(route_identity.get("lane_id") or "") != lane_id:
        failures.append("Route adapter lane_id mismatch.")
    if str(route_identity.get("source_instrument") or "").upper() != symbol.upper():
        failures.append("Route adapter source instrument mismatch.")
    if str(route_identity.get("current_order_destination") or "") != "ibkr_paper_bridge_submit_capable":
        failures.append("Route adapter is not the approved IBKR PAPER bridge destination.")
    if "PL_SIGNAL_DIRECT_PHASE1" not in str(route_identity.get("bridge_proxy_mode") or "") and symbol.upper() == "PL":
        failures.append("Route adapter proxy mode is not PL_SIGNAL_DIRECT_PHASE1.")
    return route_identity


def _extract_bridge_evidence(
    *,
    bridge_report: Mapping[str, Any],
    broker_position: Mapping[str, Any] | None,
    broker_average_price: Decimal | None,
    lane_id: str,
    account_id: str,
    symbol: str,
    local_symbol: str,
    expiry: str,
    quantity: Decimal,
    intent: Mapping[str, Any] | None,
    expected_broker_order_id: str | None,
    expected_client_id: int | None,
    expected_perm_id: int | None,
    expected_exec_id: str | None,
    expected_fill_price: Decimal | None,
    failures: list[str],
) -> dict[str, Any] | None:
    if not bridge_report:
        failures.append("Missing bridge report evidence.")
        return None
    delegated = bridge_report.get("delegated_result") if isinstance(bridge_report.get("delegated_result"), Mapping) else {}
    delegated_report = delegated.get("report") if isinstance(delegated.get("report"), Mapping) else {}
    lifecycle = (
        delegated_report.get("submit_cancel_lifecycle")
        if isinstance(delegated_report.get("submit_cancel_lifecycle"), Mapping)
        else {}
    )
    latest_status = lifecycle.get("latest_order_status") if isinstance(lifecycle.get("latest_order_status"), Mapping) else {}
    executions = _bridge_execution_rows(lifecycle)
    if _is_partial_leak_test_broker_position_evidence(
        bridge_report=bridge_report,
        delegated=delegated,
        lifecycle=lifecycle,
        latest_status=latest_status,
        executions=executions,
    ):
        return _extract_partial_leak_test_broker_position_evidence(
            bridge_report=bridge_report,
            delegated=delegated,
            lifecycle=lifecycle,
            latest_status=latest_status,
            executions=executions,
            broker_position=broker_position,
            broker_average_price=broker_average_price,
            lane_id=lane_id,
            account_id=account_id,
            symbol=symbol,
            local_symbol=local_symbol,
            expiry=expiry,
            quantity=quantity,
            expected_broker_order_id=expected_broker_order_id,
            expected_client_id=expected_client_id,
            expected_perm_id=expected_perm_id,
            expected_exec_id=expected_exec_id,
            expected_fill_price=expected_fill_price,
            failures=failures,
        )
    if str(bridge_report.get("classification") or "") != "PAPER_STRATEGY_ORDER_FILLED":
        failures.append("Bridge report classification is not PAPER_STRATEGY_ORDER_FILLED.")
    environment = bridge_report.get("environment") if isinstance(bridge_report.get("environment"), Mapping) else {}
    if str(environment.get("mode") or "").upper() != "PAPER":
        failures.append("Bridge environment is not PAPER.")
    if str(bridge_report.get("selected_account_id") or "") != account_id:
        failures.append("Bridge selected account does not match expected PAPER account.")
    strategy_identity = (
        bridge_report.get("strategy_identity") if isinstance(bridge_report.get("strategy_identity"), Mapping) else {}
    )
    if str(strategy_identity.get("strategy_id") or "") != lane_id:
        failures.append("Bridge strategy identity does not match lane_id.")

    intent_payload = bridge_report.get("intent") if isinstance(bridge_report.get("intent"), Mapping) else {}
    if str(intent_payload.get("strategy_id") or "") != lane_id:
        failures.append("Bridge intent strategy_id does not match lane_id.")
    if str(intent_payload.get("symbol") or "").upper() != symbol.upper():
        failures.append("Bridge intent symbol mismatch.")
    if _decimal(intent_payload.get("quantity")) != quantity:
        failures.append("Bridge intent quantity mismatch.")

    exact_contract = _nested(bridge_report, "exact_contract_report", "exact_contract")
    qualified_contract = _nested(bridge_report, "qualified_contract_report", "qualified_contract")
    preview_contract = _nested(bridge_report, "delegated_result", "report", "preview_payload", "contract")
    contract_candidates = [item for item in (exact_contract, qualified_contract, preview_contract) if isinstance(item, Mapping)]
    if not any(_contract_matches(item, symbol=symbol, local_symbol=local_symbol, expiry=expiry) for item in contract_candidates):
        failures.append("Bridge contract evidence does not match symbol/localSymbol/expiry.")

    if str(delegated.get("classification") or "") != "PAPER_ORDER_FILLED":
        failures.append("Delegated bridge classification is not PAPER_ORDER_FILLED.")
    if str(latest_status.get("status") or "").upper() != "FILLED":
        failures.append("Bridge latest order status is not FILLED.")
    if _decimal(latest_status.get("filled")) != quantity:
        failures.append("Bridge latest order filled quantity mismatch.")

    broker_order_id = str((intent or {}).get("broker_order_id") or "")
    if broker_order_id and str(latest_status.get("order_id") or "") != broker_order_id:
        failures.append("Bridge latest order id does not match intent broker_order_id.")
    if expected_broker_order_id is not None and str(latest_status.get("order_id") or "") != str(expected_broker_order_id):
        failures.append("Bridge latest order id does not match expected broker_order_id.")
    if expected_client_id is not None and _decimal(latest_status.get("client_id")) != Decimal(expected_client_id):
        failures.append("Bridge latest client id does not match expected client_id.")
    if expected_perm_id is not None and _decimal(latest_status.get("perm_id")) != Decimal(expected_perm_id):
        failures.append("Bridge latest perm id does not match expected perm_id.")

    matching_executions: dict[str, dict[str, Any]] = {}
    for execution in executions:
        if str(execution.get("account_id") or "") != account_id:
            continue
        if str(execution.get("symbol") or "").upper() != symbol.upper():
            continue
        if _decimal(execution.get("quantity")) != quantity:
            continue
        execution_id = str(execution.get("execution_id") or "")
        matching_executions[execution_id or json.dumps(execution, sort_keys=True)] = dict(execution)
    if len(matching_executions) != 1:
        failures.append(
            f"Expected exactly one unique matching bridge execution after dedupe, found {len(matching_executions)}."
        )
        return None

    execution = next(iter(matching_executions.values()))
    if expected_exec_id is not None and str(execution.get("execution_id") or "") != str(expected_exec_id):
        failures.append("Bridge execution id does not match expected exec_id.")
    execution_price = _decimal(execution.get("price") or latest_status.get("avg_fill_price"))
    expected_fill_decimal = _decimal(expected_fill_price)
    if expected_fill_decimal is not None and execution_price != expected_fill_decimal:
        failures.append("Bridge execution fill price does not match expected fill_price.")
    contract = next(
        (dict(item) for item in contract_candidates if _contract_matches(item, symbol=symbol, local_symbol=local_symbol, expiry=expiry)),
        {},
    )
    con_id = contract.get("con_id") or contract.get("qualified_contract_identifier")
    original_trade_id = _single_trade_id_from_sources(bridge_report, intent or {})
    if original_trade_id == "__AMBIGUOUS__":
        failures.append("Multiple distinct registry trade_id values match the broker-backed bridge fill.")
        original_trade_id = ""
    return {
        "account_id": account_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "contract_month": str(preview_contract.get("expiry") or expiry[:6]) if isinstance(preview_contract, Mapping) else expiry[:6],
        "multiplier": str(contract.get("multiplier") or preview_contract.get("multiplier") or "") if isinstance(preview_contract, Mapping) else str(contract.get("multiplier") or ""),
        "con_id": con_id,
        "broker_order_id": str(latest_status.get("order_id") or execution.get("broker_order_id") or ""),
        "perm_id": latest_status.get("perm_id"),
        "client_id": latest_status.get("client_id") or _nested(bridge_report, "environment", "client_id"),
        "execution_id": execution.get("execution_id"),
        "trade_id": original_trade_id,
        "strategy_entry_bar_ts": _nested(bridge_report, "entry_execution_pricing", "runtime_candle_timestamp")
        or (intent or {}).get("decision_bar_timestamp"),
        "submit_intent_created_at": (intent or {}).get("created_at") or _nested(bridge_report, "intent", "timestamp"),
        "fill_price": str(execution.get("price") or latest_status.get("avg_fill_price") or ""),
        "fill_timestamp": execution.get("executed_at") or latest_status.get("updated_at"),
        "order_status_updated_at": latest_status.get("updated_at"),
        "latest_order_status": dict(latest_status),
        "selected_execution": execution,
        "entry_execution_intent": _nested(bridge_report, "entry_execution_pricing", "entry_execution_intent"),
        "entry_price_source": _nested(bridge_report, "entry_execution_pricing", "execution_price_source"),
        "leak_test": bool(_nested(bridge_report, "caller_metadata", "leak_test")),
        "authorization_digest": _nested(bridge_report, "caller_metadata", "authorization_digest"),
    }


def _is_partial_leak_test_broker_position_evidence(
    *,
    bridge_report: Mapping[str, Any],
    delegated: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    latest_status: Mapping[str, Any],
    executions: Sequence[Mapping[str, Any]],
) -> bool:
    intent_payload = bridge_report.get("intent") if isinstance(bridge_report.get("intent"), Mapping) else {}
    caller_metadata = bridge_report.get("caller_metadata") if isinstance(bridge_report.get("caller_metadata"), Mapping) else {}
    if caller_metadata.get("leak_test") is not True and "TRACK_B_LEAK_TEST" not in set(intent_payload.get("risk_tags") or []):
        return False
    if str(bridge_report.get("classification") or "") == "PAPER_STRATEGY_ORDER_FILLED" and str(delegated.get("classification") or "") == "PAPER_ORDER_FILLED":
        return False
    known_submit = any(_bridge_known_order_ids(lifecycle=lifecycle, latest_status=latest_status, delegated=delegated))
    return (
        str(delegated.get("classification") or "") == "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW"
        or str(bridge_report.get("classification") or "") == "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW"
        or known_submit
        or bool(executions)
    )


def _extract_partial_leak_test_broker_position_evidence(
    *,
    bridge_report: Mapping[str, Any],
    delegated: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    latest_status: Mapping[str, Any],
    executions: Sequence[Mapping[str, Any]],
    broker_position: Mapping[str, Any] | None,
    broker_average_price: Decimal | None,
    lane_id: str,
    account_id: str,
    symbol: str,
    local_symbol: str,
    expiry: str,
    quantity: Decimal,
    expected_broker_order_id: str | None,
    expected_client_id: int | None,
    expected_perm_id: int | None,
    expected_exec_id: str | None,
    expected_fill_price: Decimal | None,
    failures: list[str],
) -> dict[str, Any] | None:
    local_failures: list[str] = []
    if broker_position is None:
        local_failures.append("Partial leak-test adoption requires exact broker position truth.")
    if expected_broker_order_id is None:
        local_failures.append("Partial leak-test adoption requires expected broker_order_id evidence.")
    environment = bridge_report.get("environment") if isinstance(bridge_report.get("environment"), Mapping) else {}
    if str(environment.get("mode") or "").upper() != "PAPER":
        local_failures.append("Bridge environment is not PAPER.")
    if str(bridge_report.get("selected_account_id") or "") != account_id:
        local_failures.append("Bridge selected account does not match expected PAPER account.")
    strategy_identity = (
        bridge_report.get("strategy_identity") if isinstance(bridge_report.get("strategy_identity"), Mapping) else {}
    )
    if str(strategy_identity.get("strategy_id") or "") != lane_id:
        local_failures.append("Bridge strategy identity does not match lane_id.")
    intent_payload = bridge_report.get("intent") if isinstance(bridge_report.get("intent"), Mapping) else {}
    caller_metadata = bridge_report.get("caller_metadata") if isinstance(bridge_report.get("caller_metadata"), Mapping) else {}
    if caller_metadata.get("leak_test") is not True and "TRACK_B_LEAK_TEST" not in set(intent_payload.get("risk_tags") or []):
        local_failures.append("Partial adoption is restricted to Track B leak-test bridge evidence.")
    if str(intent_payload.get("strategy_id") or "") != lane_id:
        local_failures.append("Bridge intent strategy_id does not match lane_id.")
    if str(intent_payload.get("symbol") or "").upper() != symbol.upper():
        local_failures.append("Bridge intent symbol mismatch.")
    if _decimal(intent_payload.get("quantity")) != quantity:
        local_failures.append("Bridge intent quantity mismatch.")
    exact_contract = _nested(bridge_report, "exact_contract_report", "exact_contract")
    qualified_contract = _nested(bridge_report, "qualified_contract_report", "qualified_contract")
    preview_contract = _nested(bridge_report, "delegated_result", "report", "preview_payload", "contract")
    contract_candidates = [item for item in (exact_contract, qualified_contract, preview_contract) if isinstance(item, Mapping)]
    if not any(_contract_matches(item, symbol=symbol, local_symbol=local_symbol, expiry=expiry) for item in contract_candidates):
        local_failures.append("Bridge contract evidence does not match symbol/localSymbol/expiry.")
    known_order_ids = _bridge_known_order_ids(lifecycle=lifecycle, latest_status=latest_status, delegated=delegated)
    if expected_broker_order_id is not None and known_order_ids and str(expected_broker_order_id) not in known_order_ids:
        local_failures.append("Bridge submitted order id does not match expected broker_order_id.")
    known_client_id = _first_nonempty(
        latest_status.get("client_id"),
        lifecycle.get("client_id"),
        lifecycle.get("submitted_client_id"),
    )
    if expected_client_id is not None and known_client_id is not None and _decimal(known_client_id) != Decimal(expected_client_id):
        local_failures.append("Bridge latest client id does not match expected client_id.")
    known_perm_id = _first_nonempty(
        latest_status.get("perm_id"),
        lifecycle.get("perm_id"),
        lifecycle.get("submitted_perm_id"),
    )
    if expected_perm_id is not None and known_perm_id is not None and _decimal(known_perm_id) != Decimal(expected_perm_id):
        local_failures.append("Bridge latest perm id does not match expected perm_id.")
    matching_executions = [
        dict(row)
        for row in executions
        if str(row.get("account_id") or "") == account_id
        and str(row.get("symbol") or "").upper() == symbol.upper()
        and _decimal(row.get("quantity")) == quantity
    ]
    execution = matching_executions[-1] if matching_executions else {}
    known_exec_id = execution.get("execution_id")
    if expected_exec_id is not None and known_exec_id is not None and str(known_exec_id) != str(expected_exec_id):
        local_failures.append("Bridge execution id does not match expected exec_id.")
    expected_fill_decimal = _decimal(expected_fill_price)
    execution_price = _decimal(execution.get("price") or latest_status.get("avg_fill_price") or latest_status.get("last_fill_price"))
    if expected_fill_decimal is not None and execution_price is not None and execution_price != expected_fill_decimal:
        local_failures.append("Bridge execution fill price does not match expected fill_price.")
    if local_failures:
        failures.extend(local_failures)
        return None
    contract = next(
        (dict(item) for item in contract_candidates if _contract_matches(item, symbol=symbol, local_symbol=local_symbol, expiry=expiry)),
        {},
    )
    con_id = contract.get("con_id") or contract.get("qualified_contract_identifier")
    broker_order_id = str(expected_broker_order_id or next(iter(known_order_ids), "") or "")
    client_id = expected_client_id if expected_client_id is not None else known_client_id
    perm_id = expected_perm_id if expected_perm_id is not None else known_perm_id
    execution_id = expected_exec_id if expected_exec_id is not None else known_exec_id
    fill_price = expected_fill_decimal or execution_price or broker_average_price
    runtime_generation_id = _find_first_by_key(bridge_report, "runtime_generation_id")
    missing_fields = [
        field
        for field, value in (
            ("client_id", client_id),
            ("perm_id", perm_id),
            ("execution_id", execution_id),
        )
        if value in {None, ""}
    ]
    return {
        "account_id": account_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "contract_month": str(preview_contract.get("expiry") or expiry[:6]) if isinstance(preview_contract, Mapping) else expiry[:6],
        "multiplier": str(contract.get("multiplier") or broker_position.get("multiplier") or ""),
        "con_id": con_id,
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "client_id": client_id,
        "execution_id": execution_id,
        "runtime_generation_id": runtime_generation_id,
        "fill_price": _decimal_text(fill_price) or "",
        "fill_price_source": "BROKER_POSITION_AVERAGE_PRICE" if execution_price is None and expected_fill_decimal is None else "BRIDGE_OR_OPERATOR_EVIDENCE",
        "fill_timestamp": execution.get("executed_at") or latest_status.get("updated_at") or broker_position.get("updated_at"),
        "order_status_updated_at": latest_status.get("updated_at"),
        "latest_order_status": dict(latest_status),
        "selected_execution": dict(execution),
        "entry_execution_intent": _nested(bridge_report, "entry_execution_pricing", "entry_execution_intent"),
        "entry_price_source": _nested(bridge_report, "entry_execution_pricing", "execution_price_source"),
        "leak_test": True,
        "authorization_digest": _nested(bridge_report, "caller_metadata", "authorization_digest"),
        "evidence_classification": "LEAK_TEST_BROKER_POSITION_CONFIRMED_PARTIAL_IDENTITY",
        "adoption_input_classification": "LEAK_TEST_BROKER_BACKED_ENTRY_REQUIRES_LIFECYCLE_ADOPTION",
        "broker_position_confirmed": True,
        "identity_completeness": "PARTIAL" if missing_fields else "COMPLETE",
        "missing_broker_identity_fields": missing_fields,
        "delegated_classification": delegated.get("classification"),
        "bridge_classification": bridge_report.get("classification"),
    }


def _bridge_execution_rows(lifecycle: Mapping[str, Any]) -> list[dict[str, Any]]:
    executions: list[dict[str, Any]] = []
    for source in (
        lifecycle.get("executions_after_submit"),
        _nested(lifecycle, "fill_verification", "executions_after_submit"),
    ):
        if isinstance(source, list):
            executions.extend(dict(item) for item in source if isinstance(item, Mapping))
    return executions


def _bridge_known_order_ids(
    *,
    lifecycle: Mapping[str, Any],
    latest_status: Mapping[str, Any],
    delegated: Mapping[str, Any],
) -> set[str]:
    values = (
        lifecycle.get("submitted_order_id"),
        lifecycle.get("broker_order_id"),
        lifecycle.get("order_id"),
        latest_status.get("order_id"),
        delegated.get("submitted_order_id"),
        delegated.get("order_id"),
    )
    return {str(value) for value in values if value not in {None, ""}}


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in {None, ""}:
            return value
    return None


def _find_first_by_key(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        if value.get(key) not in {None, ""}:
            return value.get(key)
        for nested in value.values():
            found = _find_first_by_key(nested, key)
            if found not in {None, ""}:
                return found
    if isinstance(value, list):
        for nested in value:
            found = _find_first_by_key(nested, key)
            if found not in {None, ""}:
                return found
    return None


def _build_fill_payload(
    *,
    config: LifecycleAdoptionConfig,
    intent: Mapping[str, Any] | None,
    bridge_evidence: Mapping[str, Any] | None,
    broker_position: Mapping[str, Any] | None,
    broker_average_price: Decimal | None,
    broker_cost_basis_adjustment: str | None,
    now: datetime,
) -> dict[str, Any]:
    intent = dict(intent or {})
    bridge_evidence = dict(bridge_evidence or {})
    broker_position = dict(broker_position or {})
    order_intent_id = str(intent.get("order_intent_id") or config.order_intent_id or "")
    intent_type = str(intent.get("intent_type") or "BUY_TO_OPEN").upper()
    action = "BUY" if intent_type == "BUY_TO_OPEN" else "SELL"
    fill_price = str(bridge_evidence.get("fill_price") or _decimal_text(broker_average_price) or "")
    broker_execution_timestamp = str(
        bridge_evidence.get("fill_timestamp") or bridge_evidence.get("order_status_updated_at") or now.isoformat()
    )
    strategy_id = str(intent.get("standalone_strategy_id") or intent.get("strategy_id") or config.lane_id)
    original_trade_id = str(bridge_evidence.get("trade_id") or intent.get("trade_id") or "").strip()
    strategy_entry_bar_ts = str(
        bridge_evidence.get("strategy_entry_bar_ts")
        or intent.get("decision_bar_timestamp")
        or intent.get("signal_timestamp")
        or ""
    ).strip()
    submit_intent_created_at = str(
        bridge_evidence.get("submit_intent_created_at") or intent.get("created_at") or ""
    ).strip()
    managed_entry_time = _managed_entry_time(
        strategy_entry_bar_ts=strategy_entry_bar_ts,
        submit_intent_created_at=submit_intent_created_at,
        broker_execution_timestamp=broker_execution_timestamp,
        fallback=now.isoformat(),
    )
    management_metadata = resolve_management_metadata(
        source={
            **intent,
            **bridge_evidence,
            "trade_id": original_trade_id,
            "lane_id": config.lane_id,
            "strategy_id": strategy_id,
            "order_intent_id": order_intent_id,
            "symbol": config.symbol,
            "local_symbol": config.local_symbol,
            "quantity": _decimal_text(config.quantity),
            "action": action,
        },
        output_root=config.repo_root / DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    )
    return {
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "classification": "LEAK_TEST_FILL_LIFECYCLE_ADOPTION_RECONSTRUCTED"
        if bridge_evidence.get("leak_test")
        else "PAPER_FILL_LIFECYCLE_ADOPTION_RECONSTRUCTED",
        "leak_test": bool(bridge_evidence.get("leak_test")),
        "entry_source": "LEAK_TEST_ENTRY" if bridge_evidence.get("leak_test") else "SUPERVISED_ADOPTION",
        "adopted_at": now.isoformat(),
        "paper_only": True,
        "live_money_eligible": False,
        "account_id": config.account_id,
        "lane_id": config.lane_id,
        "strategy_id": strategy_id,
        "standalone_strategy_id": strategy_id,
        "runtime_generation_id": bridge_evidence.get("runtime_generation_id") or intent.get("runtime_generation_id"),
        "trade_id": original_trade_id,
        "repo_root": str(config.repo_root),
        "order_intent_id": order_intent_id,
        "ownership_intent_id": bridge_evidence.get("ownership_intent_id"),
        "reserved_lifecycle_id": bridge_evidence.get("reserved_lifecycle_id"),
        "lifecycle_id": bridge_evidence.get("reserved_lifecycle_id"),
        "submit_intent_state": bridge_evidence.get("submit_intent_state"),
        "intent_type": intent_type,
        "action": action,
        "symbol": config.symbol,
        "instrument": config.symbol,
        "local_symbol": config.local_symbol,
        "expiry": config.expiry,
        "contract_month": str(bridge_evidence.get("contract_month") or config.expiry[:6]),
        "con_id": bridge_evidence.get("con_id"),
        "multiplier": str(broker_position.get("multiplier") or bridge_evidence.get("multiplier") or ""),
        "quantity": _decimal_text(abs(_decimal(config.quantity) or Decimal("0"))),
        "fill_price": fill_price,
        "fill_timestamp": broker_execution_timestamp,
        "managed_entry_time": managed_entry_time,
        "strategy_entry_bar_ts": strategy_entry_bar_ts or None,
        "submit_intent_created_at": submit_intent_created_at or None,
        "broker_execution_timestamp": broker_execution_timestamp,
        "evidence_retrieved_at": now.isoformat(),
        "lifecycle_adopted_at": now.isoformat(),
        "broker_order_id": str(bridge_evidence.get("broker_order_id") or intent.get("broker_order_id") or ""),
        "perm_id": bridge_evidence.get("perm_id"),
        "client_id": bridge_evidence.get("client_id"),
        "execution_id": bridge_evidence.get("execution_id"),
        "exec_id": bridge_evidence.get("execution_id"),
        "entry_execution_intent": bridge_evidence.get("entry_execution_intent"),
        "entry_price_source": bridge_evidence.get("entry_price_source"),
        "execution_price_source": bridge_evidence.get("entry_price_source"),
        "managed_exit_policy_id": management_metadata.managed_exit_policy_id,
        "position_management_metadata_source": management_metadata.source,
        "position_management_metadata_classification": management_metadata.classification,
        "position_management_metadata_blockers": list(management_metadata.blockers),
        "fill_price_source": bridge_evidence.get("fill_price_source") or "BRIDGE_EXECUTION_EVIDENCE",
        "evidence_classification": bridge_evidence.get("evidence_classification"),
        "adoption_input_classification": bridge_evidence.get("adoption_input_classification"),
        "identity_completeness": bridge_evidence.get("identity_completeness") or "COMPLETE",
        "missing_broker_identity_fields": bridge_evidence.get("missing_broker_identity_fields") or [],
        "broker_position_confirmed": bool(bridge_evidence.get("broker_position_confirmed")),
        "authorization_digest": bridge_evidence.get("authorization_digest"),
        "submit_intent_ownership_evidence": bool(bridge_evidence.get("ownership_intent_id")),
        "broker_status": "FILLED",
        "broker_position_quantity": str(broker_position.get("quantity") or ""),
        "broker_average_cost": str(broker_position.get("average_cost") or ""),
        "broker_average_price": _decimal_text(broker_average_price),
        "broker_cost_basis_adjustment": broker_cost_basis_adjustment,
        "broker_position_snapshot_time": broker_position.get("updated_at"),
        "reason_code": intent.get("reason_code"),
        "decision_bar_timestamp": intent.get("decision_bar_timestamp") or intent.get("signal_timestamp"),
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "paper_proof_invoked": False,
        "broker_mutated_by_adoption": False,
        "review_required": False,
        "source_artifact_paths": [
            str(config.broker_truth_path),
            str(config.bridge_root / config.lane_id / "ibkr_paper_strategy_bridge_report.json"),
            *[str(path) for path in (bridge_evidence.get("source_artifact_paths") or [])],
        ],
    }


def _build_trade_payload(fill_payload: Mapping[str, Any]) -> dict[str, Any]:
    order_intent_id = str(fill_payload.get("order_intent_id") or "")
    strategy_id = str(fill_payload.get("strategy_id") or fill_payload.get("lane_id") or "UNKNOWN")
    lifecycle_id = str(
        fill_payload.get("lifecycle_id")
        or (f"bridge_fill_{order_intent_id}" if order_intent_id else f"bridge_fill_{fill_payload.get('broker_order_id')}")
    )
    side = "LONG" if str(fill_payload.get("intent_type") or "").upper() == "BUY_TO_OPEN" else "SHORT"
    return {
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "classification": "PAPER_TRADE_LIFECYCLE_ADOPTION_RECONSTRUCTED_OPEN",
        "trade_id": str(fill_payload.get("trade_id") or "").strip() or f"{strategy_id}:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "ownership_intent_id": fill_payload.get("ownership_intent_id"),
        "reserved_lifecycle_id": fill_payload.get("reserved_lifecycle_id"),
        "submit_intent_state": fill_payload.get("submit_intent_state"),
        "final_position_status": "OPEN_MANAGED",
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "leak_test": bool(fill_payload.get("leak_test")),
        "entry_source": fill_payload.get("entry_source"),
        "account_id": fill_payload.get("account_id"),
        "lane_id": fill_payload.get("lane_id"),
        "strategy_id": strategy_id,
        "runtime_generation_id": fill_payload.get("runtime_generation_id"),
        "order_intent_id": order_intent_id,
        "symbol": fill_payload.get("symbol"),
        "instrument": fill_payload.get("instrument"),
        "local_symbol": fill_payload.get("local_symbol"),
        "expiry": fill_payload.get("expiry"),
        "contract_month": fill_payload.get("contract_month"),
        "con_id": fill_payload.get("con_id"),
        "side": side,
        "quantity": fill_payload.get("quantity"),
        "entry_timestamp": fill_payload.get("managed_entry_time") or fill_payload.get("fill_timestamp"),
        "broker_execution_timestamp": fill_payload.get("broker_execution_timestamp"),
        "evidence_retrieved_at": fill_payload.get("evidence_retrieved_at"),
        "lifecycle_adopted_at": fill_payload.get("lifecycle_adopted_at"),
        "strategy_entry_bar_ts": fill_payload.get("strategy_entry_bar_ts"),
        "entry_price": fill_payload.get("fill_price"),
        "entry_fill_price": fill_payload.get("fill_price"),
        "exit_timestamp": None,
        "exit_price": None,
        "realized_pnl": None,
        "broker_order_id": fill_payload.get("broker_order_id"),
        "entry_order_id": fill_payload.get("broker_order_id"),
        "entry_perm_id": fill_payload.get("perm_id"),
        "entry_client_id": fill_payload.get("client_id"),
        "entry_exec_id": fill_payload.get("execution_id"),
        "entry_execution_intent": fill_payload.get("entry_execution_intent"),
        "entry_price_source": fill_payload.get("entry_price_source"),
        "managed_exit_policy_id": fill_payload.get("managed_exit_policy_id"),
        "position_management_manifest_path": fill_payload.get("position_management_manifest_path"),
        "submit_intent_ownership_evidence": fill_payload.get("submit_intent_ownership_evidence"),
        "broker_average_price": fill_payload.get("broker_average_price"),
        "broker_cost_basis_adjustment": fill_payload.get("broker_cost_basis_adjustment"),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broker_mutated_by_adoption": False,
        "source_artifact_paths": fill_payload.get("source_artifact_paths") or [],
    }


def _lifecycle_local_repair_guard(
    *,
    config: LifecycleAdoptionConfig,
    trade_payload: Mapping[str, Any],
    fill_payload: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    evidence = {
        **dict(fill_payload),
        **dict(trade_payload),
        "requested_lifecycle_status": "OPEN_MANAGED",
        "fill_price": fill_payload.get("fill_price") or trade_payload.get("entry_fill_price"),
        "fill_timestamp": fill_payload.get("fill_timestamp") or trade_payload.get("entry_timestamp"),
        "broker_order_id": fill_payload.get("broker_order_id") or trade_payload.get("broker_order_id"),
        "perm_id": fill_payload.get("perm_id") or trade_payload.get("entry_perm_id"),
        "lifecycle_id": trade_payload.get("lifecycle_id") or fill_payload.get("lifecycle_id"),
    }
    target_identity = {
        "account_id": config.account_id,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "expiry": config.expiry,
        "quantity": _decimal_text(config.quantity),
        "order_intent_id": trade_payload.get("order_intent_id") or fill_payload.get("order_intent_id"),
        "broker_order_id": fill_payload.get("broker_order_id") or trade_payload.get("broker_order_id"),
        "perm_id": fill_payload.get("perm_id") or trade_payload.get("entry_perm_id"),
        "lifecycle_id": trade_payload.get("lifecycle_id") or fill_payload.get("lifecycle_id"),
    }
    return validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(
            repo_root=config.repo_root,
            control_plane_snapshot_path=config.control_plane_snapshot_path,
            max_snapshot_age_seconds=config.local_repair_snapshot_max_age_seconds,
            require_snapshot_for_apply=config.require_control_plane_snapshot_for_apply,
        ),
        current_state="SUBMITTED_PENDING_FILL",
        target_state="OPEN_MANAGED",
        evidence=evidence,
        apply=config.apply,
        active_state_affecting=True,
        target_identity=target_identity,
        now=now,
    )


def _build_filled_bridge_result(fill_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": fill_payload.get("strategy_id"),
        "lane_id": fill_payload.get("lane_id"),
        "runtime_generation_id": fill_payload.get("runtime_generation_id"),
        "trade_id": fill_payload.get("trade_id"),
        "repo_root": fill_payload.get("repo_root"),
        "instrument": fill_payload.get("instrument"),
        "symbol": fill_payload.get("symbol"),
        "action": fill_payload.get("action"),
        "quantity": fill_payload.get("quantity"),
        "order_intent_id": fill_payload.get("order_intent_id"),
        "ownership_intent_id": fill_payload.get("ownership_intent_id"),
        "reserved_lifecycle_id": fill_payload.get("reserved_lifecycle_id"),
        "lifecycle_id": fill_payload.get("lifecycle_id"),
        "submit_intent_state": fill_payload.get("submit_intent_state"),
        "intent_type": fill_payload.get("intent_type"),
        "decision_bar_timestamp": fill_payload.get("decision_bar_timestamp"),
        "broker_order_id": fill_payload.get("broker_order_id"),
        "account_id": fill_payload.get("account_id"),
        "perm_id": fill_payload.get("perm_id"),
        "client_id": fill_payload.get("client_id"),
        "exec_id": fill_payload.get("execution_id"),
        "execution_id": fill_payload.get("execution_id"),
        "local_symbol": fill_payload.get("local_symbol"),
        "con_id": fill_payload.get("con_id"),
        "contract_month": fill_payload.get("contract_month"),
        "contract": {
            "symbol": fill_payload.get("symbol"),
            "local_symbol": fill_payload.get("local_symbol"),
            "expiry": fill_payload.get("expiry"),
            "multiplier": fill_payload.get("multiplier"),
            "qualified_contract_identifier": fill_payload.get("con_id"),
        },
        "fill_price": fill_payload.get("fill_price"),
        "fill_timestamp": fill_payload.get("managed_entry_time") or fill_payload.get("fill_timestamp"),
        "managed_entry_time": fill_payload.get("managed_entry_time"),
        "strategy_entry_bar_ts": fill_payload.get("strategy_entry_bar_ts"),
        "submit_intent_created_at": fill_payload.get("submit_intent_created_at"),
        "broker_execution_timestamp": fill_payload.get("broker_execution_timestamp"),
        "evidence_retrieved_at": fill_payload.get("evidence_retrieved_at"),
        "lifecycle_adopted_at": fill_payload.get("lifecycle_adopted_at"),
        "bridge_classification": fill_payload.get("bridge_classification"),
        "leak_test": bool(fill_payload.get("leak_test")),
        "entry_source": fill_payload.get("entry_source"),
        "entry_execution_intent": fill_payload.get("entry_execution_intent"),
        "entry_price_source": fill_payload.get("entry_price_source"),
        "managed_exit_policy_id": fill_payload.get("managed_exit_policy_id"),
        "position_management_metadata_source": fill_payload.get("position_management_metadata_source"),
        "position_management_metadata_classification": fill_payload.get("position_management_metadata_classification"),
        "position_management_metadata_blockers": fill_payload.get("position_management_metadata_blockers") or [],
        "submit_intent_ownership_evidence": fill_payload.get("submit_intent_ownership_evidence"),
        "source_artifact_paths": fill_payload.get("source_artifact_paths") or [],
        "route_destination": fill_payload.get("route_destination"),
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
        "broker_cost_basis_adjustment": fill_payload.get("broker_cost_basis_adjustment"),
    }


def _append_adoption_entry_fill_registry_event(
    *,
    config: LifecycleAdoptionConfig,
    fill_payload: Mapping[str, Any],
    trade_payload: Mapping[str, Any],
    audit_path: Path,
    now: datetime,
) -> dict[str, Any] | None:
    trade_id = str(trade_payload.get("trade_id") or fill_payload.get("trade_id") or "").strip()
    exec_id = str(fill_payload.get("execution_id") or fill_payload.get("exec_id") or "").strip()
    perm_id = str(fill_payload.get("perm_id") or "").strip()
    if not trade_id or not exec_id or not perm_id:
        return None
    existing = load_live_trade_registry_record(repo_root=config.repo_root, trade_id=trade_id)
    if existing is not None:
        for event in existing.event_chain:
            if (
                event.event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED
                and str(event.exec_id or "") == exec_id
                and str(event.perm_id or "") == perm_id
            ):
                return {
                    "persisted": False,
                    "classification": "ENTRY_FILL_BROKER_BACKED_ALREADY_PRESENT",
                    "trade_id": trade_id,
                    "exec_id": exec_id,
                    "perm_id": perm_id,
                }
    event = make_live_trade_registry_event(
        event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
        trade_id=trade_id,
        lifecycle_id=str(trade_payload.get("lifecycle_id") or fill_payload.get("lifecycle_id") or ""),
        lane_id=str(fill_payload.get("lane_id") or config.lane_id),
        thesis_strategy_id=str(fill_payload.get("strategy_id") or fill_payload.get("lane_id") or config.lane_id),
        account_id=str(fill_payload.get("account_id") or config.account_id),
        symbol=str(fill_payload.get("symbol") or config.symbol),
        con_id=fill_payload.get("con_id"),
        local_symbol=str(fill_payload.get("local_symbol") or config.local_symbol),
        expiry=str(fill_payload.get("expiry") or config.expiry),
        side=str(trade_payload.get("side") or ("LONG" if str(fill_payload.get("intent_type") or "").upper() == "BUY_TO_OPEN" else "SHORT")),
        action=str(fill_payload.get("action") or ""),
        qty=fill_payload.get("quantity"),
        source_artifact_path=str(audit_path),
        generated_at=now,
        order_id=fill_payload.get("broker_order_id"),
        client_id=fill_payload.get("client_id"),
        perm_id=perm_id,
        exec_id=exec_id,
        price=fill_payload.get("fill_price"),
        reason_codes=("BROKER_BACKED_ENTRY_FILL_ADOPTED",),
        metadata={
            "broker_execution_timestamp": fill_payload.get("broker_execution_timestamp"),
            "managed_entry_time": fill_payload.get("managed_entry_time"),
            "evidence_retrieved_at": fill_payload.get("evidence_retrieved_at"),
            "lifecycle_adopted_at": fill_payload.get("lifecycle_adopted_at"),
            "strategy_entry_bar_ts": fill_payload.get("strategy_entry_bar_ts"),
            "submit_intent_created_at": fill_payload.get("submit_intent_created_at"),
            "adoption_source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        },
    )
    return append_live_trade_registry_event(repo_root=config.repo_root, event=event)


def _single_trade_id_from_sources(*sources: Mapping[str, Any] | None) -> str:
    candidates: list[str] = []
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for value in (
            source.get("trade_id"),
            _nested(source, "extra", "trade_id"),
            _nested(source, "extra", "caller_metadata", "trade_id"),
            _nested(source, "caller_metadata", "trade_id"),
            _nested(source, "intent", "trade_id"),
        ):
            text = str(value or "").strip()
            if text:
                candidates.append(text)
    unique = tuple(dict.fromkeys(candidates))
    if len(unique) > 1:
        return "__AMBIGUOUS__"
    return unique[0] if unique else ""


def _managed_entry_time(
    *,
    strategy_entry_bar_ts: str,
    submit_intent_created_at: str,
    broker_execution_timestamp: str,
    fallback: str,
) -> str:
    for value in (submit_intent_created_at, strategy_entry_bar_ts, broker_execution_timestamp, fallback):
        text = str(value or "").strip()
        if text:
            return text
    return fallback


def _broker_average_price(position: Mapping[str, Any] | None) -> Decimal | None:
    if not position:
        return None
    average_cost = _decimal(position.get("average_cost"))
    multiplier = _decimal(position.get("multiplier"))
    if average_cost is None or multiplier in {None, Decimal("0")}:
        return None
    return average_cost / multiplier


def _contract_matches(contract: Mapping[str, Any], *, symbol: str, local_symbol: str, expiry: str) -> bool:
    contract_symbol = str(contract.get("broker_symbol") or contract.get("symbol") or contract.get("internal_symbol") or "").upper()
    contract_local = str(contract.get("local_symbol") or "").upper()
    contract_expiry = str(contract.get("expiry") or "")
    return (
        contract_symbol == symbol.upper()
        and contract_local == local_symbol.upper()
        and (contract_expiry == expiry or contract_expiry == expiry[:6])
    )


def _jsonl_has_identity(rows: Sequence[Mapping[str, Any]], *, order_intent_id: str, execution_id: str) -> bool:
    for row in rows:
        if order_intent_id and str(row.get("order_intent_id") or "") == order_intent_id:
            return True
        if execution_id and str(row.get("execution_id") or row.get("exec_id") or row.get("entry_exec_id") or "") == execution_id:
            return True
    return False


def _shared_truth_adoption_evidence(
    *,
    config: LifecycleAdoptionConfig,
    broker_position: Mapping[str, Any] | None,
    submit_intent_ownership: Mapping[str, Any] | None,
    order_intent_id: str,
    now: datetime,
) -> dict[str, Any]:
    paths = {
        "open_order_truth": config.open_order_truth_path,
        "managed_order_registry": config.managed_order_registry_path,
        "position_truth": config.position_truth_path,
        "managed_position_registry": config.managed_position_registry_path,
        "runtime_supervisor_authority": config.runtime_supervisor_authority_path,
        "reconciliation": config.reconciliation_path,
        "broker_lease": config.broker_lease_path,
        "broker_session_authority": config.broker_session_authority_path,
    }
    payloads = {name: _read_json(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()}
    classifications = {name: _shared_classification(name=name, payload=payload) for name, payload in payloads.items()}
    broker_backed_adoption_context = _broker_backed_adoption_context(
        submit_intent_ownership=submit_intent_ownership,
        broker_position=broker_position,
    )
    broker_session_adoption = _broker_session_authority_adoption_evidence(
        payload=payloads["broker_session_authority"],
        broker_backed_adoption_context=broker_backed_adoption_context,
    )
    circular_adoption_allowances: list[dict[str, str]] = []
    freshness = {
        name: _shared_freshness(payload=payload, now=now, max_age_seconds=config.shared_truth_max_age_seconds)
        for name, payload in payloads.items()
    }
    blockers: list[str] = []
    if config.require_shared_truth_evidence:
        for name, payload in payloads.items():
            if not payload:
                blockers.append(f"Shared truth authority artifact missing: {name}.")
        for name, state in freshness.items():
            if state["stale_or_missing"]:
                blockers.append(f"Shared truth authority artifact stale/missing: {name}.")

    if classifications["open_order_truth"] and classifications["open_order_truth"] != NO_OPEN_ORDERS:
        if broker_backed_adoption_context and _open_order_truth_allows_broker_backed_adoption(
            payloads["open_order_truth"]
        ):
            circular_adoption_allowances.append(
                {
                    "artifact": "open_order_truth",
                    "classification": classifications["open_order_truth"],
                    "reason": "Exact broker-backed entry adoption may proceed while the only open-order finding is the target broker position without a close order.",
                }
            )
        else:
            blockers.append(f"Open Order Truth is not safe for adoption: {classifications['open_order_truth']}.")
    if classifications["managed_order_registry"] and classifications["managed_order_registry"] != NO_MANAGED_ORDERS:
        if broker_backed_adoption_context and _managed_order_registry_allows_broker_backed_adoption(
            payloads["managed_order_registry"],
            config=config,
            broker_position=broker_position,
            submit_intent_ownership=submit_intent_ownership,
        ):
            circular_adoption_allowances.append(
                {
                    "artifact": "managed_order_registry",
                    "classification": classifications["managed_order_registry"],
                    "reason": "Exact broker-backed entry adoption may proceed while the only managed-order finding is the target position without a close order.",
                }
            )
        else:
            blockers.append(
                f"Managed Order Registry is not safe for adoption: {classifications['managed_order_registry']}."
            )
    if classifications["runtime_supervisor_authority"] in {
        "SUPERVISOR_MANUAL_REVIEW_REQUIRED",
        "SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK",
        "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP",
        "SUPERVISOR_SHARED_TRUTH_STALE",
        "SUPERVISOR_UNKNOWN_REVIEW_REQUIRED",
    }:
        if (
            classifications["runtime_supervisor_authority"] == "SUPERVISOR_SHARED_TRUTH_STALE"
            and broker_backed_adoption_context
            and not any(state["stale_or_missing"] for state in freshness.values())
        ):
            circular_adoption_allowances.append(
                {
                    "artifact": "runtime_supervisor_authority",
                    "classification": classifications["runtime_supervisor_authority"],
                    "reason": "Runtime Supervisor shared-truth stale posture is caused by the exact broker-backed lifecycle adoption being repaired.",
                }
            )
        else:
            blockers.append(
                f"Runtime Supervisor Authority blocks lifecycle adoption: {classifications['runtime_supervisor_authority']}."
            )
    if classifications["reconciliation"] in {
        "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
        "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED_UNKNOWN_OPEN_ORDERS",
    }:
        blockers.append(f"Reconciliation is unsafe for lifecycle adoption: {classifications['reconciliation']}.")
    if classifications["broker_lease"] in {
        "INVALIDATED_CONTRADICTION",
        "INVALIDATED_UNKNOWN_OPEN_ORDERS",
        "INVALIDATED_MANUAL_BROKER_ACTION",
        "OPERATOR_REQUIRED",
    }:
        if (
            classifications["broker_lease"] in {"OPERATOR_REQUIRED", "INVALIDATED_CONTRADICTION"}
            and broker_backed_adoption_context
            and _broker_lease_allows_broker_backed_adoption(payloads["broker_lease"])
        ):
            circular_adoption_allowances.append(
                {
                    "artifact": "broker_lease",
                    "classification": classifications["broker_lease"],
                    "reason": "Broker Truth Lease is blocked only because reconciliation is waiting for this exact broker-backed lifecycle adoption.",
                }
            )
        else:
            blockers.append(f"Broker Truth Lease is unsafe for lifecycle adoption: {classifications['broker_lease']}.")
    if broker_backed_adoption_context and not broker_session_adoption["broker_observed_adoption_apply_allowed"]:
        blockers.extend(broker_session_adoption["blockers"])

    target_agreement = {}
    for name, rows in {
        "position_truth": _list(payloads["position_truth"].get("position_states")),
        "managed_position_registry": _list(payloads["managed_position_registry"].get("managed_positions")),
    }.items():
        conflicting = [
            row
            for row in rows
            if _shared_adoption_row_is_active(row)
            and _shared_adoption_row_overlaps_target(
                config=config,
                row=row,
                broker_position=broker_position,
                submit_intent_ownership=submit_intent_ownership,
            )
            and not _shared_adoption_row_matches_target(
                config=config,
                row=row,
                broker_position=broker_position,
                submit_intent_ownership=submit_intent_ownership,
                order_intent_id=order_intent_id,
            )
        ]
        matching = [
            row
            for row in rows
            if _shared_adoption_row_overlaps_target(
                config=config,
                row=row,
                broker_position=broker_position,
                submit_intent_ownership=submit_intent_ownership,
            )
            if _shared_adoption_row_matches_target(
                config=config,
                row=row,
                broker_position=broker_position,
                submit_intent_ownership=submit_intent_ownership,
                order_intent_id=order_intent_id,
            )
        ]
        target_agreement[name] = {
            "row_count": len(rows),
            "matching_row_count": len(matching),
            "conflicting_active_row_count": len(conflicting),
            "non_target_active_row_count": len(
                [
                    row
                    for row in rows
                    if _shared_adoption_row_is_active(row)
                    and not _shared_adoption_row_overlaps_target(
                        config=config,
                        row=row,
                        broker_position=broker_position,
                        submit_intent_ownership=submit_intent_ownership,
                    )
                ]
            ),
        }
        if conflicting:
            blockers.append(f"{name} active rows conflict with requested adoption target.")

    return {
        "source_authority": "execution_core_authority",
        "dashboard_projection_consumed": False,
        "required": config.require_shared_truth_evidence,
        "broker_backed_adoption_context": broker_backed_adoption_context,
        "broker_session_authority_classification": broker_session_adoption[
            "broker_session_authority_classification"
        ],
        "broker_session_connection_mode": broker_session_adoption["broker_session_connection_mode"],
        "broker_session_allowed_uses": broker_session_adoption["broker_session_allowed_uses"],
        "broker_session_authority_blockers": broker_session_adoption["broker_session_authority_blockers"],
        "callback_ownership_attribution": broker_session_adoption["callback_ownership_attribution"],
        "callback_missing_reason": broker_session_adoption["callback_missing_reason"],
        "callback_missing_reasons": broker_session_adoption["callback_missing_reasons"],
        "broker_observed_adoption_diagnosis_allowed": broker_session_adoption[
            "broker_observed_adoption_diagnosis_allowed"
        ],
        "broker_observed_adoption_apply_allowed": broker_session_adoption[
            "broker_observed_adoption_apply_allowed"
        ],
        "circular_adoption_allowances": circular_adoption_allowances,
        "max_age_seconds": config.shared_truth_max_age_seconds,
        "artifact_paths": {name: str(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()},
        "classifications": classifications,
        "freshness": freshness,
        "target_agreement": target_agreement,
        "blockers": blockers,
    }


def _broker_backed_adoption_context(
    *,
    submit_intent_ownership: Mapping[str, Any] | None,
    broker_position: Mapping[str, Any] | None,
) -> bool:
    if not submit_intent_ownership or not broker_position:
        return False
    if submit_intent_ownership.get("live_money_eligible") is not False:
        return False
    if submit_intent_ownership.get("paper_proof_invoked") is not False:
        return False
    return str(submit_intent_ownership.get("state") or "").upper() in {
        "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED",
        "BROKER_ORDER_WORKING",
        "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED",
        "LIFECYCLE_OPEN_PERSISTED",
    }


def _open_order_truth_allows_broker_backed_adoption(payload: Mapping[str, Any]) -> bool:
    if _shared_classification(name="open_order_truth", payload=payload) != BROKER_POSITION_WITHOUT_CLOSE_ORDER:
        return False
    summary = _mapping_or_empty(payload.get("summary"))
    return (
        _decimal(summary.get("open_order_count")) == Decimal("0")
        and _decimal(summary.get("duplicate_close_order_group_count")) == Decimal("0")
        and _decimal(summary.get("suspicious_order_count")) == Decimal("0")
        and _decimal(summary.get("working_entry_order_count")) == Decimal("0")
    )


def _managed_order_registry_allows_broker_backed_adoption(
    payload: Mapping[str, Any],
    *,
    config: LifecycleAdoptionConfig,
    broker_position: Mapping[str, Any] | None,
    submit_intent_ownership: Mapping[str, Any] | None,
) -> bool:
    registry_classification = _shared_classification(name="managed_order_registry", payload=payload)
    if registry_classification not in {POSITION_WITHOUT_CLOSE_ORDER, "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"}:
        return False
    summary = _mapping_or_empty(payload.get("summary"))
    if (
        _decimal(summary.get("duplicate_close_order_count")) not in {None, Decimal("0")}
        or _decimal(summary.get("suspicious_order_count")) not in {None, Decimal("0")}
        or _decimal(summary.get("working_entry_order_count")) not in {None, Decimal("0")}
    ):
        return False
    for row in _list(payload.get("managed_orders")):
        classification = str(row.get("classification") or "")
        overlaps_target = _shared_adoption_row_overlaps_target(
            config=config,
            row=row,
            broker_position=broker_position,
            submit_intent_ownership=submit_intent_ownership,
        )
        if not overlaps_target:
            if classification == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING":
                continue
            if classification == POSITION_WITHOUT_CLOSE_ORDER and not (
                row.get("source_order") or row.get("broker_order_id") or row.get("perm_id")
            ):
                continue
            return False
        if classification == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING":
            continue
        if classification != POSITION_WITHOUT_CLOSE_ORDER:
            return False
        if row.get("source_order") or row.get("broker_order_id") or row.get("perm_id"):
            return False
    return True


def _broker_lease_allows_broker_backed_adoption(payload: Mapping[str, Any]) -> bool:
    blockers = _list(payload.get("blockers"))
    if not blockers:
        return False
    allowed_codes = {
        "reconciliation_not_clean",
        "lifecycle_broker_position_mismatch",
    }
    open_order_count = _decimal(payload.get("track_b_broker_open_order_count"))
    if open_order_count not in {None, Decimal("0")}:
        return False
    return all(str(blocker.get("code") or "") in allowed_codes for blocker in blockers)


def _broker_session_authority_adoption_evidence(
    *,
    payload: Mapping[str, Any],
    broker_backed_adoption_context: bool,
) -> dict[str, Any]:
    classification = _shared_classification(name="broker_session_authority", payload=payload)
    connection_mode = str(payload.get("connection_mode") or "").strip().upper()
    allowed_uses = dict(_mapping_or_empty(payload.get("allowed_uses")))
    authority_blockers = _list(payload.get("authority_blockers"))
    callback_attribution = _mapping_or_empty(payload.get("callback_ownership_attribution"))
    callback_missing_reasons = _callback_missing_reasons_from_authority(payload, callback_attribution)
    callback_missing_reason = (
        str(payload.get("callback_missing_reason") or callback_attribution.get("callback_missing_reason") or "")
        or None
    )
    if callback_missing_reason is None and callback_missing_reasons:
        first_reason = callback_missing_reasons[0]
        if isinstance(first_reason, Mapping):
            callback_missing_reason = str(first_reason.get("code") or "") or None

    blockers: list[str] = []
    if not payload:
        blockers.append("Broker Session Authority artifact missing for broker-observed adoption.")
    elif not broker_backed_adoption_context:
        blockers.append("Broker Session Authority adoption requires exact broker-backed adoption context.")
    elif not _broker_session_authority_supports_broker_observed_adoption(
        classification=classification,
        connection_mode=connection_mode,
        allowed_uses=allowed_uses,
    ):
        blockers.append(
            "Broker Session Authority does not allow broker-observed adoption: "
            f"classification={classification or 'UNKNOWN'} connection_mode={connection_mode or 'UNKNOWN'}."
        )

    allowed = bool(payload and broker_backed_adoption_context and not blockers)
    return {
        "broker_session_authority_classification": classification,
        "broker_session_connection_mode": connection_mode,
        "broker_session_allowed_uses": allowed_uses,
        "broker_session_authority_blockers": authority_blockers,
        "callback_ownership_attribution": dict(callback_attribution),
        "callback_missing_reason": callback_missing_reason,
        "callback_missing_reasons": callback_missing_reasons,
        "broker_observed_adoption_diagnosis_allowed": allowed,
        "broker_observed_adoption_apply_allowed": allowed,
        "blockers": blockers,
    }


def _broker_session_authority_supports_broker_observed_adoption(
    *,
    classification: str,
    connection_mode: str,
    allowed_uses: Mapping[str, Any],
) -> bool:
    if bool(allowed_uses.get("broker_observed_adoption_diagnosis")):
        return True
    supported_modes = {
        "ORDER_STATUS_UNRELIABLE",
        "POSITION_TRUTH_ONLY",
        "SUBMIT_CAPABLE",
        "FILL_CALLBACK_CAPABLE",
    }
    supported_classifications = {
        "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY",
        "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
        "BROKER_SESSION_AUTHORITY_FILL_CALLBACK_CAPABLE",
    }
    return connection_mode in supported_modes or classification in supported_classifications


def _callback_missing_reasons_from_authority(
    payload: Mapping[str, Any],
    callback_attribution: Mapping[str, Any],
) -> list[Any]:
    direct_reasons = _list(payload.get("callback_missing_reasons"))
    attribution_reasons = _list(callback_attribution.get("callback_missing_reasons"))
    reasons = direct_reasons or attribution_reasons
    reason = payload.get("callback_missing_reason") or callback_attribution.get("callback_missing_reason")
    if reason and not any(isinstance(row, Mapping) and row.get("code") == reason for row in reasons):
        return [{"code": str(reason), "detail": "Broker session authority reported missing callback evidence."}] + reasons
    return reasons


def _control_plane_incoherence_allows_broker_backed_adoption(
    *,
    lifecycle_local_repair_guard: Mapping[str, Any],
    shared_truth_evidence: Mapping[str, Any],
) -> bool:
    if lifecycle_local_repair_guard.get("classification") != "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT":
        return False
    if lifecycle_local_repair_guard.get("blockers") != ["control_plane_snapshot_incoherent"]:
        return False
    if shared_truth_evidence.get("blockers"):
        return False
    if shared_truth_evidence.get("broker_backed_adoption_context") is not True:
        return False
    allowance_artifacts = {
        str(row.get("artifact") or "")
        for row in _list(shared_truth_evidence.get("circular_adoption_allowances"))
    }
    return "broker_lease" in allowance_artifacts


def _shared_adoption_row_overlaps_target(
    *,
    config: LifecycleAdoptionConfig,
    row: Mapping[str, Any],
    broker_position: Mapping[str, Any] | None,
    submit_intent_ownership: Mapping[str, Any] | None,
) -> bool:
    symbol = str(row.get("symbol") or row.get("instrument_family") or row.get("instrument") or "").upper()
    if symbol and symbol != config.symbol.upper():
        return False
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").upper()
    if local_symbol and local_symbol != config.local_symbol.upper():
        return False
    con_id = _decimal(row.get("con_id") or row.get("conId"))
    broker_con_id = _decimal((broker_position or {}).get("con_id") or (submit_intent_ownership or {}).get("con_id"))
    if con_id is not None and broker_con_id is not None and con_id != broker_con_id:
        return False
    quantity = _decimal(row.get("quantity") or row.get("broker_quantity") or row.get("qty"))
    if quantity is not None and quantity not in {config.quantity, abs(config.quantity)}:
        return False
    return bool(symbol or local_symbol or con_id is not None)


def _shared_adoption_row_matches_target(
    *,
    config: LifecycleAdoptionConfig,
    row: Mapping[str, Any],
    broker_position: Mapping[str, Any] | None,
    submit_intent_ownership: Mapping[str, Any] | None,
    order_intent_id: str,
) -> bool:
    lifecycle_id = str(row.get("lifecycle_id") or row.get("entry_lifecycle_id") or "")
    reserved_lifecycle_id = str((submit_intent_ownership or {}).get("lifecycle_id") or "")
    if lifecycle_id and reserved_lifecycle_id and lifecycle_id == reserved_lifecycle_id:
        return True
    row_intent_id = str(row.get("order_intent_id") or row.get("entry_intent_id") or "")
    ownership_intent_id = str((submit_intent_ownership or {}).get("ownership_intent_id") or "")
    if row_intent_id and row_intent_id in {order_intent_id, ownership_intent_id}:
        return True
    symbol = str(row.get("symbol") or row.get("instrument_family") or row.get("instrument") or "").upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").upper()
    con_id = _decimal(row.get("con_id") or row.get("conId"))
    broker_con_id = _decimal((broker_position or {}).get("con_id") or (submit_intent_ownership or {}).get("con_id"))
    quantity = _decimal(row.get("quantity") or row.get("broker_quantity") or row.get("qty"))
    if symbol and symbol != config.symbol.upper():
        return False
    if local_symbol and local_symbol != config.local_symbol.upper():
        return False
    if con_id is not None and broker_con_id is not None and con_id != broker_con_id:
        return False
    return quantity in {None, config.quantity, abs(config.quantity)}


def _shared_adoption_row_is_active(row: Mapping[str, Any]) -> bool:
    classification = str(row.get("classification") or "")
    if classification in {"FLAT_CLEAN", "NO_MANAGED_POSITIONS"}:
        return False
    status = str(row.get("final_position_status") or row.get("lifecycle_status") or "").upper()
    return status != "CLOSED_FLAT"


def _append_jsonl_if_missing(path: Path, row: Mapping[str, Any], *, order_intent_id: str, execution_id: str) -> bool:
    existing = _read_jsonl(path)
    if _jsonl_has_identity(existing, order_intent_id=order_intent_id, execution_id=execution_id):
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_jsonable(dict(row)), sort_keys=True) + "\n")
    return True


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping_or_empty(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _shared_classification(*, name: str, payload: Mapping[str, Any]) -> str:
    if name == "position_truth":
        summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
        return str(payload.get("classification") or summary.get("overall_classification") or "")
    if name == "runtime_supervisor_authority":
        return str(payload.get("classification") or payload.get("supervisor_classification") or "")
    if name == "broker_lease":
        return str(payload.get("classification") or payload.get("lease_state") or "")
    return str(payload.get("classification") or "")


def _shared_freshness(*, payload: Mapping[str, Any], now: datetime, max_age_seconds: float) -> dict[str, Any]:
    generated_at = payload.get("generated_at") or payload.get("latest_refresh_time") or payload.get("last_success_at")
    age_seconds = _age_seconds(generated_at, now)
    return {
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "stale_or_missing": age_seconds is None or age_seconds > max_age_seconds,
    }


def _resolve(*, repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _age_seconds(value: object, now: datetime) -> float | None:
    if not value:
        return None
    try:
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0.0, (now - parsed.astimezone(UTC)).total_seconds())
    except (TypeError, ValueError):
        return None


def _write_audit(
    repo_root: Path,
    output_root: Path,
    report: Mapping[str, Any],
    *,
    actual_now: datetime,
    suffix: str | None = None,
) -> Path:
    root = repo_root / output_root
    root.mkdir(parents=True, exist_ok=True)
    stamp = actual_now.strftime("%Y%m%dT%H%M%S%fZ")
    suffix_text = f"_{suffix}" if suffix else ""
    path = root / f"track_b_paper_lifecycle_adoption_{stamp}{suffix_text}.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_latest_audit(repo_root: Path, output_root: Path, report: Mapping[str, Any]) -> Path:
    root = repo_root / output_root
    root.mkdir(parents=True, exist_ok=True)
    path = root / "latest_track_b_paper_lifecycle_adoption_audit.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    value: Any = payload
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Any) -> str | None:
    decimal_value = _decimal(value)
    if decimal_value is None:
        return None
    return format(decimal_value.normalize(), "f")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="track-b-paper-lifecycle-adoption",
        description="Supervised offline PAPER lifecycle adoption for proven Track B broker fills.",
    )
    parser.add_argument("--repo-root", default=".", help="Repository root containing Track B artifacts.")
    parser.add_argument("--lane-id", default=DEFAULT_LANE_ID)
    parser.add_argument("--order-intent-id", default=None)
    parser.add_argument("--account-id", default=PAPER_ACCOUNT_ID)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--local-symbol", default=DEFAULT_LOCAL_SYMBOL)
    parser.add_argument("--expiry", default=DEFAULT_EXPIRY)
    parser.add_argument("--quantity", default=str(DEFAULT_QUANTITY))
    parser.add_argument("--allow-leak-test-synthetic-intent", action="store_true")
    parser.add_argument("--expected-broker-order-id", default=None)
    parser.add_argument("--expected-client-id", type=int, default=None)
    parser.add_argument("--expected-perm-id", type=int, default=None)
    parser.add_argument("--expected-exec-id", default=None)
    parser.add_argument("--expected-fill-price", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--broker-truth-path", default=str(DEFAULT_BROKER_TRUTH_PATH))
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--bridge-root", default=str(DEFAULT_BRIDGE_ROOT))
    parser.add_argument("--ledger-root", default=str(DEFAULT_LEDGER_ROOT))
    parser.add_argument("--apply", action="store_true", help="Actually append lifecycle artifacts. Omit for dry-run.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    quantity = _decimal(args.quantity)
    if quantity is None:
        raise SystemExit("--quantity must be decimal")
    expected_fill_price = _decimal(args.expected_fill_price)
    if args.expected_fill_price is not None and expected_fill_price is None:
        raise SystemExit("--expected-fill-price must be decimal")
    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=Path(args.repo_root).expanduser().resolve(),
            lane_id=str(args.lane_id),
            order_intent_id=args.order_intent_id,
            account_id=str(args.account_id),
            symbol=str(args.symbol).upper(),
            local_symbol=str(args.local_symbol).upper(),
            expiry=str(args.expiry),
            quantity=quantity,
            apply=bool(args.apply),
            allow_leak_test_synthetic_intent=bool(args.allow_leak_test_synthetic_intent),
            expected_broker_order_id=args.expected_broker_order_id,
            expected_client_id=args.expected_client_id,
            expected_perm_id=args.expected_perm_id,
            expected_exec_id=args.expected_exec_id,
            expected_fill_price=expected_fill_price,
            output_root=Path(args.output_root),
            lane_root=Path(args.lane_root),
            bridge_root=Path(args.bridge_root),
            broker_truth_path=Path(args.broker_truth_path),
            ledger_root=Path(args.ledger_root),
        )
    )
    print(json.dumps({"classification": result.classification, "audit_path": str(result.audit_path)}, indent=2))
    return 0 if result.classification != "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
