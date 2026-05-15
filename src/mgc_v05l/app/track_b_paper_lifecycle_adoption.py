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
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    update_track_b_paper_trade_ledger_from_filled_bridge_result,
)

PAPER_ACCOUNT_ID = "DUM882026"
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

    broker_truth = _read_json(broker_truth_path)
    bridge_report = _read_json(bridge_path)
    intents = _read_jsonl(order_intents_path)
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
    intent = _select_intent(
        intents=intents,
        bridge_report=bridge_report,
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
    bridge_evidence = _extract_bridge_evidence(
        bridge_report=bridge_report,
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

    broker_average_price = _broker_average_price(broker_position)
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
    }
    report: dict[str, Any] = {
        "classification": classification,
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
        },
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
            now=actual_now,
        )
        post_report = dict(report)
        post_report["classification"] = "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
        post_report["post_adoption"] = {
            "fill_written": not fill_already_present,
            "trade_written": not trade_already_present,
            "compact_ledger_trade_record_written": ledger_result.trade_record_written,
            "compact_ledger_live_position_status": ledger_result.live_position_status,
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
    if str(bridge_report.get("classification") or "") != "PAPER_STRATEGY_ORDER_FILLED":
        return None
    if str(delegated.get("classification") or "") != "PAPER_ORDER_FILLED":
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
        "decision_bar_timestamp": intent_payload.get("timestamp"),
        "signal_timestamp": intent_payload.get("timestamp"),
        "synthetic_leak_test_intent": True,
        "source": "TRACK_B_PAPER_LEAK_TEST_BRIDGE_REPORT",
    }


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

    delegated = bridge_report.get("delegated_result") if isinstance(bridge_report.get("delegated_result"), Mapping) else {}
    if str(delegated.get("classification") or "") != "PAPER_ORDER_FILLED":
        failures.append("Delegated bridge classification is not PAPER_ORDER_FILLED.")
    delegated_report = delegated.get("report") if isinstance(delegated.get("report"), Mapping) else {}
    lifecycle = (
        delegated_report.get("submit_cancel_lifecycle")
        if isinstance(delegated_report.get("submit_cancel_lifecycle"), Mapping)
        else {}
    )
    latest_status = lifecycle.get("latest_order_status") if isinstance(lifecycle.get("latest_order_status"), Mapping) else {}
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

    executions = []
    for source in (
        lifecycle.get("executions_after_submit"),
        _nested(lifecycle, "fill_verification", "executions_after_submit"),
    ):
        if isinstance(source, list):
            executions.extend(dict(item) for item in source if isinstance(item, Mapping))
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
    fill_price = str(bridge_evidence.get("fill_price") or "")
    fill_timestamp = str(bridge_evidence.get("fill_timestamp") or bridge_evidence.get("order_status_updated_at") or now.isoformat())
    strategy_id = str(intent.get("standalone_strategy_id") or intent.get("strategy_id") or config.lane_id)
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
        "order_intent_id": order_intent_id,
        "intent_type": intent_type,
        "action": action,
        "symbol": config.symbol,
        "instrument": config.symbol,
        "local_symbol": config.local_symbol,
        "expiry": config.expiry,
        "contract_month": str(bridge_evidence.get("contract_month") or config.expiry[:6]),
        "con_id": bridge_evidence.get("con_id"),
        "multiplier": str(broker_position.get("multiplier") or bridge_evidence.get("multiplier") or ""),
        "quantity": _decimal_text(config.quantity),
        "fill_price": fill_price,
        "fill_timestamp": fill_timestamp,
        "broker_order_id": str(bridge_evidence.get("broker_order_id") or intent.get("broker_order_id") or ""),
        "perm_id": bridge_evidence.get("perm_id"),
        "client_id": bridge_evidence.get("client_id"),
        "execution_id": bridge_evidence.get("execution_id"),
        "exec_id": bridge_evidence.get("execution_id"),
        "entry_execution_intent": bridge_evidence.get("entry_execution_intent"),
        "entry_price_source": bridge_evidence.get("entry_price_source"),
        "execution_price_source": bridge_evidence.get("entry_price_source"),
        "authorization_digest": bridge_evidence.get("authorization_digest"),
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
        ],
    }


def _build_trade_payload(fill_payload: Mapping[str, Any]) -> dict[str, Any]:
    order_intent_id = str(fill_payload.get("order_intent_id") or "")
    strategy_id = str(fill_payload.get("strategy_id") or fill_payload.get("lane_id") or "UNKNOWN")
    lifecycle_id = f"bridge_fill_{order_intent_id}" if order_intent_id else f"bridge_fill_{fill_payload.get('broker_order_id')}"
    side = "LONG" if str(fill_payload.get("intent_type") or "").upper() == "BUY_TO_OPEN" else "SHORT"
    return {
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "classification": "PAPER_TRADE_LIFECYCLE_ADOPTION_RECONSTRUCTED_OPEN",
        "trade_id": f"{strategy_id}:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "final_position_status": "OPEN_MANAGED",
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "leak_test": bool(fill_payload.get("leak_test")),
        "entry_source": fill_payload.get("entry_source"),
        "account_id": fill_payload.get("account_id"),
        "lane_id": fill_payload.get("lane_id"),
        "strategy_id": strategy_id,
        "order_intent_id": order_intent_id,
        "symbol": fill_payload.get("symbol"),
        "instrument": fill_payload.get("instrument"),
        "local_symbol": fill_payload.get("local_symbol"),
        "expiry": fill_payload.get("expiry"),
        "contract_month": fill_payload.get("contract_month"),
        "con_id": fill_payload.get("con_id"),
        "side": side,
        "quantity": fill_payload.get("quantity"),
        "entry_timestamp": fill_payload.get("fill_timestamp"),
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
        "broker_average_price": fill_payload.get("broker_average_price"),
        "broker_cost_basis_adjustment": fill_payload.get("broker_cost_basis_adjustment"),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broker_mutated_by_adoption": False,
        "source_artifact_paths": fill_payload.get("source_artifact_paths") or [],
    }


def _build_filled_bridge_result(fill_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": fill_payload.get("strategy_id"),
        "lane_id": fill_payload.get("lane_id"),
        "instrument": fill_payload.get("instrument"),
        "symbol": fill_payload.get("symbol"),
        "action": fill_payload.get("action"),
        "quantity": fill_payload.get("quantity"),
        "order_intent_id": fill_payload.get("order_intent_id"),
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
        "fill_timestamp": fill_payload.get("fill_timestamp"),
        "bridge_classification": fill_payload.get("bridge_classification"),
        "leak_test": bool(fill_payload.get("leak_test")),
        "entry_source": fill_payload.get("entry_source"),
        "entry_execution_intent": fill_payload.get("entry_execution_intent"),
        "entry_price_source": fill_payload.get("entry_price_source"),
        "source_artifact_paths": fill_payload.get("source_artifact_paths") or [],
        "route_destination": fill_payload.get("route_destination"),
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
        "broker_cost_basis_adjustment": fill_payload.get("broker_cost_basis_adjustment"),
    }


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
