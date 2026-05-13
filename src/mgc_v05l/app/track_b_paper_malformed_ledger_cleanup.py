"""Supervised PAPER-only cleanup for one malformed stale compact-ledger row.

This command is intentionally artifact-only. It never connects to IBKR, never
submits/cancels/closes an order, and dry-run is the default. The only apply
action is appending an explicit ARTIFACT_RECONCILIATION record that voids one
exact malformed stale ledger row after already-written evidence proves it is
not a valid broker-backed open position.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    LEDGER_SCHEMA_VERSION,
    RECONCILIATION_SCHEMA_VERSION,
    VOID_MALFORMED_STALE_ARTIFACT,
    build_track_b_paper_trade_summaries,
)


PAPER_ACCOUNT_ID = "DUM882026"
DEFAULT_LANE_ID = "mnq_1x_ny_early_core__us_late_long"
DEFAULT_STRATEGY_ID = DEFAULT_LANE_ID
DEFAULT_SYMBOL = "MNQ"
DEFAULT_LOCAL_SYMBOL = "MNQM6"
DEFAULT_CON_ID = 770561201
DEFAULT_SIDE = "LONG"
DEFAULT_QUANTITY = Decimal("1")
DEFAULT_ENTRY_PRICE = Decimal("29307.75")
DEFAULT_ENTRY_TIMESTAMP = "2026-05-08T17:36:29.205805+00:00"
DEFAULT_LIFECYCLE_ID = "bridge_fill_MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN"
DEFAULT_SOURCE_INTENT_ID = "MNQ|1m|2026-05-08T17:36:00Z|BUY_TO_OPEN"
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "track_b_paper_malformed_ledger_cleanup"
DEFAULT_LANE_ROOT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"
DEFAULT_BROKER_TRUTH_ROOT = Path("outputs") / "reports" / "ibkr_read_only_verification"
DEFAULT_SESSION_CLOSE_ROOT = Path("outputs") / "operator_dashboard" / "paper_session_close_reviews"


@dataclass(frozen=True)
class MalformedLedgerCleanupConfig:
    repo_root: Path
    account_id: str = PAPER_ACCOUNT_ID
    lane_id: str = DEFAULT_LANE_ID
    strategy_id: str = DEFAULT_STRATEGY_ID
    symbol: str = DEFAULT_SYMBOL
    local_symbol: str = DEFAULT_LOCAL_SYMBOL
    con_id: int = DEFAULT_CON_ID
    side: str = DEFAULT_SIDE
    quantity: Decimal = DEFAULT_QUANTITY
    entry_price: Decimal = DEFAULT_ENTRY_PRICE
    entry_timestamp: str = DEFAULT_ENTRY_TIMESTAMP
    lifecycle_id: str = DEFAULT_LIFECYCLE_ID
    source_intent_id: str = DEFAULT_SOURCE_INTENT_ID
    apply: bool = False
    output_root: Path = DEFAULT_OUTPUT_ROOT
    lane_root: Path = DEFAULT_LANE_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    session_close_root: Path = DEFAULT_SESSION_CLOSE_ROOT
    ledger_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT


@dataclass(frozen=True)
class MalformedLedgerCleanupResult:
    classification: str
    report: dict[str, Any]
    audit_path: Path
    latest_audit_path: Path


def run_track_b_paper_malformed_ledger_cleanup(
    *, config: MalformedLedgerCleanupConfig, now: datetime | None = None
) -> MalformedLedgerCleanupResult:
    actual_now = now or datetime.now(UTC)
    if actual_now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    repo_root = config.repo_root
    ledger_jsonl = repo_root / config.ledger_root / "track_b_paper_trade_ledger.jsonl"
    lane_dir = repo_root / config.lane_root / config.lane_id
    blocked_intents_path = lane_dir / "blocked_strategy_intents.jsonl"
    filled_bridge_results_path = lane_dir / "filled_bridge_results.jsonl"
    trades_path = lane_dir / "trades.jsonl"
    broker_positions_path = repo_root / config.broker_truth_root / "ibkr_positions_snapshot.json"
    broker_orders_path = repo_root / config.broker_truth_root / "ibkr_open_orders_snapshot.json"

    failures: list[str] = []
    ledger_records = _read_jsonl(ledger_jsonl)
    blocked_intents = _read_jsonl(blocked_intents_path)
    filled_bridge_rows = _read_jsonl(filled_bridge_results_path)
    trade_rows = _read_jsonl(trades_path)
    broker_positions = _read_json(broker_positions_path)
    broker_orders = _read_json(broker_orders_path)

    target = _select_exact_target(config=config, rows=ledger_records, failures=failures)
    malformed_identity = _malformed_identity_evidence(config=config, target=target, failures=failures)
    blocked_source_intent = _blocked_source_intent_evidence(
        config=config,
        rows=blocked_intents,
        path=blocked_intents_path,
        failures=failures,
    )
    durable_fill_evidence = _durable_fill_evidence(
        config=config,
        filled_bridge_rows=filled_bridge_rows,
        trade_rows=trade_rows,
        filled_bridge_results_path=filled_bridge_results_path,
        trades_path=trades_path,
        failures=failures,
    )
    session_close_evidence = _session_close_no_fill_evidence(
        config=config,
        root=repo_root / config.session_close_root,
        failures=failures,
    )
    broker_flat_evidence = _broker_flat_evidence(
        config=config,
        positions_snapshot=broker_positions,
        orders_snapshot=broker_orders,
        positions_path=broker_positions_path,
        orders_path=broker_orders_path,
        failures=failures,
    )
    already_applied = _already_voided(config=config, rows=ledger_records)
    reconciliation_record = (
        _malformed_reconciliation_record(config=config, target=target, now=actual_now)
        if target is not None
        else None
    )
    simulated_records = list(ledger_records)
    if reconciliation_record is not None and not already_applied:
        simulated_records.append(reconciliation_record)
    summaries = _build_summaries(
        records=simulated_records,
        ledger_jsonl=ledger_jsonl,
        output_root=repo_root / config.ledger_root,
        now=actual_now,
    )
    reconciliation_prediction = _reconciliation_prediction(
        config=config,
        live_position_status=summaries["live_position_status"],
        positions_snapshot=broker_positions,
        orders_snapshot=broker_orders,
    )

    valid = not failures
    if valid and already_applied:
        classification = "TRACK_B_MALFORMED_LEDGER_CLEANUP_ALREADY_APPLIED"
    elif valid and config.apply:
        classification = "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
    elif valid:
        classification = "TRACK_B_MALFORMED_LEDGER_CLEANUP_DRY_RUN_READY"
    else:
        classification = "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"

    report: dict[str, Any] = {
        "classification": classification,
        "mode": "APPLY" if config.apply else "DRY_RUN",
        "generated_at": actual_now.isoformat(),
        "paper_only": True,
        "account_id": config.account_id,
        "live_money_eligible": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_order_attempted": False,
        "place_order_attempted": False,
        "paper_proof_invoked": False,
        "broker_mutated": False,
        "failures": failures,
        "expected_identity": _expected_identity(config),
        "before": {
            "ledger_path": str(ledger_jsonl),
            "target_row": target,
            "already_applied": already_applied,
        },
        "evidence": {
            "malformed_identity": malformed_identity,
            "blocked_source_intent": blocked_source_intent,
            "durable_fill_absence": durable_fill_evidence,
            "session_close_no_fill": session_close_evidence,
            "broker_flat": broker_flat_evidence,
        },
        "write_plan": {
            "would_append_reconciliation_record": valid and not already_applied,
            "would_update_compact_summaries": valid,
            "rows_that_would_change": [] if target is None else [_row_identity(target)],
            "action": VOID_MALFORMED_STALE_ARTIFACT,
            "preserve_original_row": True,
            "ledger_path": str(ledger_jsonl),
        },
        "reconciliation_record": reconciliation_record if valid else None,
        "after_prediction": {
            "trade_summary": _summary_compact(summaries["trade_summary"]),
            "live_position_status": _position_compact(summaries["live_position_status"]),
            "reconciliation_would_clear": reconciliation_prediction["would_clear"],
            "reconciliation_prediction": reconciliation_prediction,
        },
    }
    audit_path = _write_audit(repo_root, config.output_root, report, actual_now=actual_now)
    report["audit_path"] = str(audit_path)
    latest_audit_path = _write_latest_audit(repo_root, config.output_root, report)

    if valid and config.apply and reconciliation_record is not None and not already_applied:
        ledger_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with ledger_jsonl.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_jsonable(reconciliation_record), sort_keys=True) + "\n")
        _write_summaries(summaries=summaries, output_root=repo_root / config.ledger_root)
        post_report = dict(report)
        post_report["classification"] = "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
        post_report["after"] = {
            "reconciliation_record_written": True,
            "compact_summaries_updated": True,
            "reconciliation_would_clear": reconciliation_prediction["would_clear"],
        }
        post_report["audit_path"] = str(audit_path)
        audit_path = _write_audit(repo_root, config.output_root, post_report, actual_now=actual_now, suffix="post")
        latest_audit_path = _write_latest_audit(repo_root, config.output_root, post_report)
        report = post_report
    elif valid and config.apply and already_applied:
        _write_summaries(summaries=summaries, output_root=repo_root / config.ledger_root)

    return MalformedLedgerCleanupResult(
        classification=classification,
        report=report,
        audit_path=audit_path,
        latest_audit_path=latest_audit_path,
    )


def _select_exact_target(
    *,
    config: MalformedLedgerCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> dict[str, Any] | None:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("lifecycle_id") or "") == config.lifecycle_id
        and str(row.get("final_position_status") or "") == "OPEN_MANAGED"
    ]
    if len(matches) != 1:
        failures.append(f"Expected exactly one target OPEN_MANAGED malformed ledger row, found {len(matches)}.")
        return matches[0] if matches else None
    row = matches[0]
    checks = {
        "strategy_id": str(row.get("strategy_id") or "") == config.strategy_id,
        "instrument_family": str(row.get("instrument_family") or "").upper() == config.symbol,
        "local_symbol": str(row.get("local_symbol") or "").upper() == config.local_symbol,
        "con_id": _int(row.get("con_id")) == config.con_id,
        "side": str(row.get("side") or "").upper() == config.side,
        "quantity": _decimal(row.get("quantity")) == config.quantity,
        "entry_price": _decimal(row.get("entry_fill_price")) == config.entry_price,
        "entry_timestamp": _same_time(row.get("entry_timestamp"), config.entry_timestamp),
        "status": str(row.get("final_position_status") or "") == "OPEN_MANAGED",
        "no_exit": row.get("exit_fill_price") in {None, ""},
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        failures.append(f"Target malformed ledger row identity mismatch: {', '.join(failed)}.")
    return row


def _malformed_identity_evidence(
    *,
    config: MalformedLedgerCleanupConfig,
    target: Mapping[str, Any] | None,
    failures: list[str],
) -> dict[str, Any]:
    if target is None:
        return {"confirmed": False, "blocker": "TARGET_ROW_MISSING"}
    lifecycle_conflicts_with_contract = (
        str(target.get("lifecycle_id") or "").startswith("bridge_fill_MGC|5m|")
        and str(target.get("instrument_family") or "").upper() == config.symbol
        and str(target.get("local_symbol") or "").upper() == config.local_symbol
        and _int(target.get("con_id")) == config.con_id
    )
    if not lifecycle_conflicts_with_contract:
        failures.append("Malformed identity proof failed: lifecycle id does not conflict with MNQ contract identity.")
    return {
        "confirmed": lifecycle_conflicts_with_contract,
        "lifecycle_id": target.get("lifecycle_id"),
        "instrument_family": target.get("instrument_family"),
        "local_symbol": target.get("local_symbol"),
        "con_id": target.get("con_id"),
        "reason": "Lifecycle id carries stale MGC|5m identity while strategy/contract/conId are MNQ.",
    }


def _blocked_source_intent_evidence(
    *,
    config: MalformedLedgerCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    path: Path,
    failures: list[str],
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("order_intent_id") or "") == config.source_intent_id
        and str(row.get("lane_id") or row.get("strategy_id") or "").endswith(config.lane_id)
    ]
    if len(matches) != 1:
        failures.append(f"Expected exactly one blocked source intent for malformed row, found {len(matches)}.")
        return {"confirmed": False, "path": str(path), "match_count": len(matches), "matches": matches}
    row = matches[0]
    blocked = (
        row.get("submit_allowed") is False
        or str(row.get("blocker_classification") or "") == "PRE_SUBMIT_GATE_BLOCKED"
        or "PRE_SUBMIT_GATE_BLOCKED" in str(row.get("exact_blocker_reason") or "")
    )
    missing_broker_identity = not any(row.get(key) not in {None, ""} for key in ("broker_order_id", "order_id", "perm_id"))
    confirmed = blocked and missing_broker_identity
    if not confirmed:
        failures.append("Source intent is not both blocked and missing broker order identity.")
    return {
        "confirmed": confirmed,
        "path": str(path),
        "order_intent_id": row.get("order_intent_id"),
        "submit_allowed": row.get("submit_allowed"),
        "blocker_classification": row.get("blocker_classification"),
        "bridge_classification": row.get("bridge_classification"),
        "missing_broker_identity": missing_broker_identity,
        "exact_blocker_reason": row.get("exact_blocker_reason"),
    }


def _durable_fill_evidence(
    *,
    config: MalformedLedgerCleanupConfig,
    filled_bridge_rows: Sequence[Mapping[str, Any]],
    trade_rows: Sequence[Mapping[str, Any]],
    filled_bridge_results_path: Path,
    trades_path: Path,
    failures: list[str],
) -> dict[str, Any]:
    bridge_matches = [
        _row_identity(row)
        for row in filled_bridge_rows
        if _matches_malformed_entry_evidence(config, row)
    ]
    trade_matches = [
        _row_identity(row)
        for row in trade_rows
        if _matches_malformed_entry_evidence(config, row)
    ]
    if bridge_matches or trade_matches:
        failures.append("Durable May 8 MNQ fill/trade evidence exists; refusing malformed-artifact void.")
    return {
        "confirmed_absent": not bridge_matches and not trade_matches,
        "filled_bridge_results_path": str(filled_bridge_results_path),
        "trades_path": str(trades_path),
        "matching_filled_bridge_result_count": len(bridge_matches),
        "matching_trade_row_count": len(trade_matches),
        "matching_filled_bridge_results": bridge_matches,
        "matching_trade_rows": trade_matches,
    }


def _session_close_no_fill_evidence(
    *,
    config: MalformedLedgerCleanupConfig,
    root: Path,
    failures: list[str],
) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for path in sorted(root.glob("2026-05-08_*.json")) if root.exists() else []:
        payload = _read_json(path)
        if _contains_session_no_fill(payload, config):
            matches.append({"path": str(path), "classification": "SIGNAL_NO_FILL"})
    if not matches:
        failures.append("No May 8 session-close SIGNAL_NO_FILL evidence found for malformed MNQ lane.")
    return {
        "confirmed": bool(matches),
        "root": str(root),
        "match_count": len(matches),
        "matches": matches[:10],
    }


def _broker_flat_evidence(
    *,
    config: MalformedLedgerCleanupConfig,
    positions_snapshot: Mapping[str, Any],
    orders_snapshot: Mapping[str, Any],
    positions_path: Path,
    orders_path: Path,
    failures: list[str],
) -> dict[str, Any]:
    if not positions_snapshot:
        failures.append("Missing broker positions snapshot.")
    if not orders_snapshot:
        failures.append("Missing broker open-orders snapshot.")
    position_account = positions_snapshot.get("account") or positions_snapshot.get("selected_account_id")
    order_account = orders_snapshot.get("account") or orders_snapshot.get("selected_account_id")
    if str(position_account or "") != config.account_id or str(order_account or "") != config.account_id:
        failures.append("Broker truth account mismatch.")
    positions = positions_snapshot.get("positions") if isinstance(positions_snapshot.get("positions"), list) else []
    target_rows = [
        dict(row)
        for row in positions
        if str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("local_symbol") or row.get("localSymbol") or "").upper() == config.local_symbol
    ]
    nonflat_rows = [row for row in target_rows if (_decimal(row.get("quantity")) or Decimal("0")) != Decimal("0")]
    if nonflat_rows:
        failures.append("Broker truth reports non-flat MNQ quantity.")
    open_orders = orders_snapshot.get("open_orders")
    open_order_count = orders_snapshot.get("open_order_count")
    if open_order_count is None and isinstance(open_orders, list):
        open_order_count = len(open_orders)
    if _int(open_order_count) != 0:
        failures.append("Broker truth open orders are not zero.")
    return {
        "confirmed": not nonflat_rows and _int(open_order_count) == 0,
        "positions_path": str(positions_path),
        "open_orders_path": str(orders_path),
        "positions_generated_at": positions_snapshot.get("generated_at"),
        "open_orders_generated_at": orders_snapshot.get("generated_at"),
        "account": position_account,
        "matching_position_rows": target_rows,
        "nonflat_position_rows": nonflat_rows,
        "open_order_count": open_order_count,
    }


def _malformed_reconciliation_record(
    *,
    config: MalformedLedgerCleanupConfig,
    target: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    return {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "ARTIFACT_RECONCILIATION",
        "reconciliation_schema_version": RECONCILIATION_SCHEMA_VERSION,
        "trade_id": f"{target.get('trade_id')}:void_malformed_stale_artifact",
        "lifecycle_id": target.get("lifecycle_id"),
        "strategy_id": target.get("strategy_id"),
        "instrument_family": target.get("instrument_family"),
        "contract_key": target.get("contract_key"),
        "local_symbol": target.get("local_symbol"),
        "con_id": target.get("con_id"),
        "account_id": config.account_id,
        "prior_artifact_classification": target.get("paper_lifecycle_classification"),
        "prior_final_position_status": target.get("final_position_status"),
        "prior_review_required": target.get("review_required"),
        "reconciliation_action": VOID_MALFORMED_STALE_ARTIFACT,
        "new_artifact_classification": VOID_MALFORMED_STALE_ARTIFACT,
        "final_position_status": VOID_MALFORMED_STALE_ARTIFACT,
        "review_required": False,
        "broker_reconciled": False,
        "broker_backed_position_confirmed": False,
        "broker_flat_confirmed": True,
        "broker_open_order_count": 0,
        "source": "SUPERVISED_PAPER_ONLY_MALFORMED_STALE_LEDGER_CLEANUP",
        "reason_codes": [
            "MALFORMED_LIFECYCLE_ID_BUG",
            "SOURCE_INTENT_BLOCKED_OR_MISSING_BROKER_IDENTITY",
            "DURABLE_FILL_EVIDENCE_ABSENT",
            "SESSION_CLOSE_SIGNAL_NO_FILL",
            "BROKER_TRUTH_FLAT_OPEN_ORDERS_ZERO",
        ],
        "broker_mutation_attempted": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_order_attempted": False,
        "place_order_attempted": False,
        "paper_proof_cli_invoked": False,
        "live_money_eligible": False,
        "created_at": now.isoformat(),
    }


def _already_voided(config: MalformedLedgerCleanupConfig, rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        str(row.get("lifecycle_id") or "") == config.lifecycle_id
        and row.get("record_type") == "ARTIFACT_RECONCILIATION"
        and row.get("new_artifact_classification") == VOID_MALFORMED_STALE_ARTIFACT
        for row in rows
    )


def _matches_malformed_entry_evidence(config: MalformedLedgerCleanupConfig, row: Mapping[str, Any]) -> bool:
    order_or_lifecycle = str(row.get("order_intent_id") or row.get("signal_id") or row.get("lifecycle_id") or "")
    symbol = str(row.get("symbol") or row.get("instrument") or row.get("instrument_family") or "").upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or _nested(row, "contract", "local_symbol") or "").upper()
    con_id = _int(row.get("con_id") or _nested(row, "contract", "qualified_contract_identifier"))
    price = _decimal(row.get("fill_price") or row.get("entry_fill_price") or row.get("price"))
    timestamp = row.get("fill_timestamp") or row.get("entry_timestamp") or row.get("filled_at")
    return (
        (order_or_lifecycle in {config.source_intent_id, config.lifecycle_id} or "2026-05-08T17:36:00" in order_or_lifecycle)
        and symbol == config.symbol
        and local_symbol == config.local_symbol
        and con_id == config.con_id
        and price == config.entry_price
        and _same_time(timestamp, config.entry_timestamp)
    )


def _contains_session_no_fill(payload: Any, config: MalformedLedgerCleanupConfig) -> bool:
    if isinstance(payload, Mapping):
        text_values = {str(value) for value in payload.values() if isinstance(value, (str, int, float, bool))}
        has_lane = any(config.lane_id in value or config.strategy_id in value for value in text_values)
        has_event = any(config.source_intent_id in value or "2026-05-08T17:36:00" in value for value in text_values)
        has_no_fill = "SIGNAL_NO_FILL" in text_values or any("SIGNAL_NO_FILL" in value for value in text_values)
        if has_lane and has_no_fill and has_event:
            return True
        return any(_contains_session_no_fill(value, config) for value in payload.values())
    if isinstance(payload, list):
        return any(_contains_session_no_fill(value, config) for value in payload)
    return False


def _build_summaries(
    *,
    records: Sequence[Mapping[str, Any]],
    ledger_jsonl: Path,
    output_root: Path,
    now: datetime,
) -> dict[str, dict[str, Any]]:
    return build_track_b_paper_trade_summaries(
        ledger_records=records,
        ledger_jsonl=ledger_jsonl,
        trade_summary_json=output_root / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=output_root / "latest_track_b_live_position_status.json",
        pnl_summary_json=output_root / "latest_track_b_pnl_summary.json",
        now=now,
    )


def _write_summaries(*, summaries: Mapping[str, Mapping[str, Any]], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    for key, filename in (
        ("trade_summary", "latest_track_b_paper_trade_summary.json"),
        ("live_position_status", "latest_track_b_live_position_status.json"),
        ("pnl_summary", "latest_track_b_pnl_summary.json"),
    ):
        (output_root / filename).write_text(
            json.dumps(_jsonable(dict(summaries[key])), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _reconciliation_prediction(
    *,
    config: MalformedLedgerCleanupConfig,
    live_position_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    orders_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    lifecycle_positions = [
        dict(row)
        for row in (live_position_status.get("positions_by_instrument") or {}).values()
        if isinstance(row, Mapping) and str(row.get("instrument_family") or "").upper() == config.symbol
    ]
    broker_positions = [
        dict(row)
        for row in positions_snapshot.get("positions", [])
        if isinstance(row, Mapping)
        and str(row.get("symbol") or "").upper() == config.symbol
        and (_decimal(row.get("quantity")) or Decimal("0")) != Decimal("0")
    ]
    open_order_count = _int(orders_snapshot.get("open_order_count"))
    if open_order_count is None and isinstance(orders_snapshot.get("open_orders"), list):
        open_order_count = len(orders_snapshot.get("open_orders") or [])
    return {
        "would_clear": len(lifecycle_positions) == len(broker_positions) and open_order_count == 0,
        "lifecycle_open_position_count": len(lifecycle_positions),
        "broker_open_position_count": len(broker_positions),
        "broker_open_order_count": open_order_count,
        "lifecycle_positions": lifecycle_positions,
        "broker_positions": broker_positions,
        "note": "Count-level prediction only; normal Track B broker reconciliation remains authoritative after apply.",
    }


def _expected_identity(config: MalformedLedgerCleanupConfig) -> dict[str, Any]:
    return {
        "account_id": config.account_id,
        "lifecycle_id": config.lifecycle_id,
        "source_intent_id": config.source_intent_id,
        "strategy_id": config.strategy_id,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": config.side,
        "quantity": _decimal_text(config.quantity),
        "entry_price": _decimal_text(config.entry_price),
        "entry_timestamp": _canonical_time(config.entry_timestamp),
    }


def _row_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": row.get("trade_id"),
        "strategy_id": row.get("strategy_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "instrument_family": row.get("instrument_family"),
        "contract_key": row.get("contract_key"),
        "local_symbol": row.get("local_symbol"),
        "con_id": row.get("con_id"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "entry_timestamp": row.get("entry_timestamp"),
        "entry_fill_price": row.get("entry_fill_price"),
        "final_position_status": row.get("final_position_status"),
    }


def _summary_compact(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "open_position_count": summary.get("open_position_count"),
        "open_position_record_count": summary.get("open_position_record_count"),
        "completed_trade_count": summary.get("completed_trade_count"),
        "review_required_count": summary.get("review_required_count"),
    }


def _position_compact(status: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "open_position_count": status.get("open_position_count"),
        "open_order_count": status.get("open_order_count"),
        "positions_by_instrument": status.get("positions_by_instrument"),
        "positions_by_strategy_keys": sorted((status.get("positions_by_strategy") or {}).keys()),
    }


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
    path = root / f"track_b_paper_malformed_ledger_cleanup_{stamp}{suffix_text}.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_latest_audit(repo_root: Path, output_root: Path, report: Mapping[str, Any]) -> Path:
    root = repo_root / output_root
    root.mkdir(parents=True, exist_ok=True)
    path = root / "latest_track_b_paper_malformed_ledger_cleanup_audit.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    value: Any = payload
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _canonical_time(value: object) -> str:
    raw = str(value or "")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    return parsed.astimezone(UTC).isoformat()


def _same_time(left: object, right: object) -> bool:
    if not left or not right:
        return False
    try:
        return _canonical_time(left) == _canonical_time(right)
    except ValueError:
        return False


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Any) -> str | None:
    decimal_value = _decimal(value)
    if decimal_value is None:
        return None
    return format(decimal_value.normalize(), "f")


def _int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
        prog="track-b-paper-malformed-ledger-cleanup",
        description="Supervised offline PAPER cleanup for one malformed stale compact-ledger artifact.",
    )
    parser.add_argument("--repo-root", default=".", help="Repository root containing Track B artifacts.")
    parser.add_argument("--account-id", default=PAPER_ACCOUNT_ID)
    parser.add_argument("--lane-id", default=DEFAULT_LANE_ID)
    parser.add_argument("--strategy-id", default=DEFAULT_STRATEGY_ID)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--local-symbol", default=DEFAULT_LOCAL_SYMBOL)
    parser.add_argument("--con-id", type=int, default=DEFAULT_CON_ID)
    parser.add_argument("--side", default=DEFAULT_SIDE)
    parser.add_argument("--quantity", default=str(DEFAULT_QUANTITY))
    parser.add_argument("--entry-price", default=str(DEFAULT_ENTRY_PRICE))
    parser.add_argument("--entry-timestamp", default=DEFAULT_ENTRY_TIMESTAMP)
    parser.add_argument("--lifecycle-id", default=DEFAULT_LIFECYCLE_ID)
    parser.add_argument("--source-intent-id", default=DEFAULT_SOURCE_INTENT_ID)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--broker-truth-root", default=str(DEFAULT_BROKER_TRUTH_ROOT))
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--session-close-root", default=str(DEFAULT_SESSION_CLOSE_ROOT))
    parser.add_argument("--ledger-root", default=str(DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT))
    parser.add_argument("--apply", action="store_true", help="Append the reconciliation record. Omit for dry-run.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    quantity = _decimal(args.quantity)
    entry_price = _decimal(args.entry_price)
    if quantity is None:
        raise SystemExit("--quantity must be decimal")
    if entry_price is None:
        raise SystemExit("--entry-price must be decimal")
    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(
            repo_root=Path(args.repo_root).expanduser().resolve(),
            account_id=str(args.account_id),
            lane_id=str(args.lane_id),
            strategy_id=str(args.strategy_id),
            symbol=str(args.symbol).upper(),
            local_symbol=str(args.local_symbol).upper(),
            con_id=int(args.con_id),
            side=str(args.side).upper(),
            quantity=quantity,
            entry_price=entry_price,
            entry_timestamp=str(args.entry_timestamp),
            lifecycle_id=str(args.lifecycle_id),
            source_intent_id=str(args.source_intent_id),
            apply=bool(args.apply),
            output_root=Path(args.output_root),
            lane_root=Path(args.lane_root),
            broker_truth_root=Path(args.broker_truth_root),
            session_close_root=Path(args.session_close_root),
            ledger_root=Path(args.ledger_root),
        )
    )
    print(json.dumps({"classification": result.classification, "audit_path": str(result.audit_path)}, indent=2))
    return 0 if result.classification != "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
