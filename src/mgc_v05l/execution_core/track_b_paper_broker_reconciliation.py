"""Read-only Track B PAPER lifecycle-to-broker-truth reconciliation.

This module consumes already-written IBKR read-only position/open-order
snapshots and Track B lifecycle summaries. It never connects to IBKR and never
mutates broker state.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
DEFAULT_BROKER_TRUTH_ROOT = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"
DEFAULT_REPORT_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_BROKER_TRUTH_MAX_AGE_SECONDS", "120"))
PAPER_ACCOUNT = "DUM882026"


@dataclass(frozen=True)
class ReconciliationConfig:
    repo_root: Path = REPO_ROOT
    ledger_root: Path = DEFAULT_LEDGER_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    report_path: Path = DEFAULT_REPORT_PATH
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS
    account: str = PAPER_ACCOUNT
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER

    @property
    def trade_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_paper_trade_summary.json"

    @property
    def live_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_live_position_status.json"

    @property
    def pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_pnl_summary.json"

    @property
    def reconciled_trade_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_paper_trade_summary.json"

    @property
    def reconciled_live_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_live_position_status.json"

    @property
    def reconciled_pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_pnl_summary.json"

    @property
    def broker_status_path(self) -> Path:
        return self.broker_truth_root / "ibkr_broker_truth_refresh_status.json"


def reconcile_track_b_paper_broker_truth(
    *,
    config: ReconciliationConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    trade_summary = _load_json(config.trade_summary_path)
    live_position_status = _load_json(config.live_position_status_path)
    pnl_summary = _load_json(config.pnl_summary_path)
    broker_status = _load_json(config.broker_status_path)
    positions_path = _path_from_payload(
        broker_status.get("positions_snapshot_path"),
        default=config.broker_truth_root / "ibkr_positions_snapshot.json",
    )
    open_orders_path = _path_from_payload(
        broker_status.get("open_orders_snapshot_path"),
        default=config.broker_truth_root / "ibkr_open_orders_snapshot.json",
    )
    positions_snapshot = _load_json(positions_path)
    open_orders_snapshot = _load_json(open_orders_path)

    blockers: list[dict[str, Any]] = []
    _validate_broker_truth(
        broker_status=broker_status,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        positions_path=positions_path,
        open_orders_path=open_orders_path,
        config=config,
        now=actual_now,
        blockers=blockers,
    )
    lifecycle_blockers = _validate_lifecycle_read_model(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        pnl_summary=pnl_summary,
    )
    blockers.extend(lifecycle_blockers)
    track_b_positions = _track_b_broker_positions(positions_snapshot, config.symbols)
    track_b_open_orders = _track_b_broker_open_orders(open_orders_snapshot, config.symbols)
    lifecycle_positions = _track_b_lifecycle_positions(live_position_status, config.symbols)
    position_match_report = _broker_lifecycle_position_match(
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        symbols=config.symbols,
    )
    broker_cost_basis_adjustments = _broker_cost_basis_adjustments_from_match_report(position_match_report)
    if position_match_report["matched"] is not True:
        blockers.append(position_match_report["blocker"])
    if track_b_open_orders:
        blockers.append(
            {
                "code": "TRACK_B_BROKER_OPEN_ORDER_PRESENT",
                "detail": "IBKR broker truth reports one or more Track B futures open orders.",
                "open_orders": track_b_open_orders,
            }
        )

    reconciled = not blockers
    classification = "TRACK_B_PAPER_BROKER_RECONCILED" if reconciled else "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    report = {
        "schema_version": "track_b_paper_broker_reconciliation_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "broker_reconciled": reconciled,
        "source": "IBKR_READ_ONLY_BROKER_TRUTH",
        "account": config.account,
        "symbols": list(config.symbols),
        "max_age_seconds": config.max_age_seconds,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "base_artifacts": {
            "trade_summary": str(config.trade_summary_path),
            "live_position_status": str(config.live_position_status_path),
            "pnl_summary": str(config.pnl_summary_path),
        },
        "broker_truth_artifacts": {
            "status": str(config.broker_status_path),
            "positions_snapshot": str(positions_path),
            "open_orders_snapshot": str(open_orders_path),
        },
        "reconciled_artifacts": {
            "trade_summary": str(config.reconciled_trade_summary_path),
            "live_position_status": str(config.reconciled_live_position_status_path),
            "pnl_summary": str(config.reconciled_pnl_summary_path),
        },
        "track_b_broker_position_count": len(track_b_positions),
        "track_b_broker_open_order_count": len(track_b_open_orders),
        "track_b_broker_positions": track_b_positions,
        "track_b_broker_open_orders": track_b_open_orders,
        "track_b_lifecycle_positions": lifecycle_positions,
        "position_match_report": position_match_report,
        "broker_cost_basis_adjustments": broker_cost_basis_adjustments,
        "lifecycle_open_position_count": _int_value(live_position_status.get("open_position_count")),
        "lifecycle_open_order_count": _int_value(live_position_status.get("open_order_count")),
        "review_required_count": _max_int(
            pnl_summary.get("review_required_count"),
            trade_summary.get("review_required_count"),
            len(live_position_status.get("review_required_positions") or []),
        ),
        "blockers": blockers,
    }
    if reconciled:
        _write_reconciled_summaries(
            config=config,
            now=actual_now,
            trade_summary=trade_summary,
            live_position_status=live_position_status,
            pnl_summary=pnl_summary,
            report=report,
            positions_path=positions_path,
            open_orders_path=open_orders_path,
            broker_positions=track_b_positions,
        )
    _write_json_atomic(config.report_path, report)
    return report


def _validate_broker_truth(
    *,
    broker_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
    config: ReconciliationConfig,
    now: datetime,
    blockers: list[dict[str, Any]],
) -> None:
    if not broker_status:
        blockers.append({"code": "BROKER_TRUTH_STATUS_MISSING", "path": str(config.broker_status_path)})
        return
    status_generated_at = _parse_time(broker_status.get("generated_at") or broker_status.get("latest_refresh_time"))
    if status_generated_at is None:
        blockers.append({"code": "BROKER_TRUTH_STATUS_MISSING_GENERATED_AT", "path": str(config.broker_status_path)})
    else:
        age = max((now - status_generated_at).total_seconds(), 0.0)
        if age > config.max_age_seconds:
            blockers.append(
                {
                    "code": "BROKER_TRUTH_STATUS_STALE",
                    "path": str(config.broker_status_path),
                    "generated_at": status_generated_at.isoformat(),
                    "age_seconds": age,
                    "max_age_seconds": config.max_age_seconds,
                }
            )
    expected_status_flags = {
        "last_success": True,
        "read_only": True,
        "positions_complete": True,
        "open_orders_complete": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    for key, expected in expected_status_flags.items():
        if broker_status.get(key) is not expected:
            blockers.append({"code": "BROKER_TRUTH_STATUS_FLAG_MISMATCH", "field": key, "expected": expected, "actual": broker_status.get(key)})
    if str(broker_status.get("account") or "") != config.account:
        blockers.append({"code": "BROKER_TRUTH_ACCOUNT_MISMATCH", "expected": config.account, "actual": broker_status.get("account")})
    _validate_snapshot(
        payload=positions_snapshot,
        path=positions_path,
        complete_field="positions_complete",
        request_method="reqPositions",
        config=config,
        now=now,
        blockers=blockers,
    )
    _validate_snapshot(
        payload=open_orders_snapshot,
        path=open_orders_path,
        complete_field="open_orders_complete",
        request_method="reqAllOpenOrders",
        config=config,
        now=now,
        blockers=blockers,
    )
    if open_orders_snapshot.get("auto_open_orders_requested") is True:
        blockers.append({"code": "BROKER_TRUTH_UNSAFE_OPEN_ORDER_BINDING", "field": "auto_open_orders_requested", "path": str(open_orders_path)})
    if open_orders_snapshot.get("order_binding_requested") is True:
        blockers.append({"code": "BROKER_TRUTH_UNSAFE_OPEN_ORDER_BINDING", "field": "order_binding_requested", "path": str(open_orders_path)})


def _validate_snapshot(
    *,
    payload: Mapping[str, Any],
    path: Path,
    complete_field: str,
    request_method: str,
    config: ReconciliationConfig,
    now: datetime,
    blockers: list[dict[str, Any]],
) -> None:
    if not payload:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_MISSING", "path": str(path)})
        return
    generated_at = _parse_time(payload.get("generated_at"))
    if generated_at is None:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_MISSING_GENERATED_AT", "path": str(path)})
    else:
        age = max((now - generated_at).total_seconds(), 0.0)
        if age > config.max_age_seconds:
            blockers.append(
                {
                    "code": "BROKER_TRUTH_SNAPSHOT_STALE",
                    "path": str(path),
                    "generated_at": generated_at.isoformat(),
                    "age_seconds": age,
                    "max_age_seconds": config.max_age_seconds,
                }
            )
    if payload.get("read_only") is not True:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_NOT_READ_ONLY", "path": str(path)})
    if payload.get(complete_field) is not True:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_INCOMPLETE", "path": str(path), "field": complete_field})
    if str(payload.get("account") or payload.get("selected_account_id") or "") != config.account:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_ACCOUNT_MISMATCH", "path": str(path), "expected": config.account})
    if payload.get("request_method") != request_method:
        blockers.append(
            {
                "code": "BROKER_TRUTH_SNAPSHOT_REQUEST_METHOD_MISMATCH",
                "path": str(path),
                "expected": request_method,
                "actual": payload.get("request_method"),
            }
        )


def _validate_lifecycle_read_model(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    pnl_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if not trade_summary:
        blockers.append({"code": "LIFECYCLE_TRADE_SUMMARY_MISSING"})
    if not live_position_status:
        blockers.append({"code": "LIFECYCLE_POSITION_STATUS_MISSING"})
    if not pnl_summary:
        blockers.append({"code": "LIFECYCLE_PNL_SUMMARY_MISSING"})
    if _int_value(live_position_status.get("open_order_count")) != 0:
        blockers.append({"code": "LIFECYCLE_OPEN_ORDER_PRESENT", "count": live_position_status.get("open_order_count")})
    positions_by_instrument = live_position_status.get("positions_by_instrument")
    if positions_by_instrument not in ({}, None) and not isinstance(positions_by_instrument, Mapping):
        blockers.append({"code": "LIFECYCLE_POSITIONS_BY_INSTRUMENT_INVALID"})
    positions_by_strategy = live_position_status.get("positions_by_strategy")
    if positions_by_strategy not in ({}, None) and not isinstance(positions_by_strategy, Mapping):
        blockers.append({"code": "LIFECYCLE_POSITIONS_BY_STRATEGY_INVALID"})
    review_required_count = _max_int(
        trade_summary.get("review_required_count"),
        pnl_summary.get("review_required_count"),
        len(live_position_status.get("review_required_positions") or []),
    )
    if review_required_count:
        blockers.append({"code": "LIFECYCLE_REVIEW_REQUIRED_PRESENT", "count": review_required_count})
    return blockers


def _write_reconciled_summaries(
    *,
    config: ReconciliationConfig,
    now: datetime,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    pnl_summary: Mapping[str, Any],
    report: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
    broker_positions: Sequence[Mapping[str, Any]],
) -> None:
    broker_position_count = len(broker_positions)
    reconciled_state = "BROKER_AND_LIFECYCLE_OPEN_MATCHED" if broker_position_count else "BROKER_AND_LIFECYCLE_FLAT"
    broker_cost_basis_adjustments = [
        dict(item)
        for item in report.get("broker_cost_basis_adjustments", [])
        if isinstance(item, Mapping)
    ]
    broker_truth_warning = (
        "Broker read-only truth agrees with Track B lifecycle open-position state."
        if broker_position_count
        else "Broker read-only truth agrees with Track B lifecycle flat state."
    )
    common = {
        "source": "BROKER_RECONCILED",
        "broker_reconciled": True,
        "broker_reconciliation_classification": report.get("classification"),
        "broker_reconciliation_report_path": str(config.report_path),
        "broker_truth_status_path": str(config.broker_status_path),
        "broker_positions_snapshot_path": str(positions_path),
        "broker_open_orders_snapshot_path": str(open_orders_path),
        "last_broker_reconciliation_time": now.isoformat(),
        "base_source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    reconciled_trade = dict(trade_summary)
    reconciled_trade.update(common)
    reconciled_trade.update(
        {
            "as_of": now.isoformat(),
            "latest_trade_summary_path": str(config.reconciled_trade_summary_path),
            "latest_live_position_status_path": str(config.reconciled_live_position_status_path),
            "latest_pnl_summary_path": str(config.reconciled_pnl_summary_path),
            "broker_truth_warning": broker_truth_warning,
        }
    )
    reconciled_positions = dict(live_position_status)
    reconciled_positions.update(common)
    reconciled_positions.update(
        {
            "as_of": now.isoformat(),
            "account_id": config.account,
            "latest_live_position_status_path": str(config.reconciled_live_position_status_path),
            "broker_truth_warning": broker_truth_warning,
            "broker_track_b_position_count": broker_position_count,
            "broker_track_b_open_order_count": 0,
            "broker_reconciled_state": reconciled_state,
            "broker_track_b_positions": [dict(item) for item in broker_positions],
            "broker_cost_basis_adjustments": broker_cost_basis_adjustments,
        }
    )
    reconciled_pnl = dict(pnl_summary)
    reconciled_pnl.update(common)
    reconciled_pnl.update(
        {
            "as_of": now.isoformat(),
            "latest_pnl_summary_path": str(config.reconciled_pnl_summary_path),
        }
    )
    _write_json_atomic(config.reconciled_trade_summary_path, reconciled_trade)
    _write_json_atomic(config.reconciled_live_position_status_path, reconciled_positions)
    _write_json_atomic(config.reconciled_pnl_summary_path, reconciled_pnl)


def _track_b_lifecycle_positions(live_position_status: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = live_position_status.get("positions_by_instrument")
    if not isinstance(rows, Mapping):
        return []
    matches: list[dict[str, Any]] = []
    for key, value in rows.items():
        if not isinstance(value, Mapping):
            continue
        item = dict(value)
        item.setdefault("position_key", key)
        root = _track_b_root(item, symbols)
        if root is None:
            root = _track_b_root({"instrument": key, "contract_key": key}, symbols)
        if root is None:
            continue
        qty = _decimal_value(item.get("quantity"))
        if qty is None or qty == 0:
            continue
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _broker_lifecycle_position_match(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
) -> dict[str, Any]:
    if not broker_positions and not lifecycle_positions:
        return {"matched": True, "state": "BROKER_AND_LIFECYCLE_FLAT", "matches": []}
    count_mismatch = len(broker_positions) != len(lifecycle_positions)

    unmatched_lifecycle = [dict(item) for item in lifecycle_positions]
    matches: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for broker_position in broker_positions:
        broker_root = _track_b_root(broker_position, symbols)
        broker_qty = _decimal_value(broker_position.get("quantity"))
        match_index = None
        match_detail: dict[str, Any] | None = None
        for index, lifecycle_position in enumerate(unmatched_lifecycle):
            lifecycle_root = _track_b_root(lifecycle_position, symbols)
            lifecycle_qty = _decimal_value(lifecycle_position.get("quantity"))
            lifecycle_signed_qty = _signed_lifecycle_quantity(lifecycle_position, lifecycle_qty)
            root_matches = broker_root is not None and broker_root == lifecycle_root
            local_matches = _local_symbols_compatible(broker_position, lifecycle_position)
            quantity_matches = broker_qty is not None and lifecycle_signed_qty is not None and broker_qty == lifecycle_signed_qty
            if root_matches and local_matches and quantity_matches:
                match_index = index
                match_detail = {
                    "root": broker_root,
                    "broker_local_symbol": broker_position.get("local_symbol") or broker_position.get("localSymbol"),
                    "lifecycle_local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol"),
                    "quantity": str(broker_qty),
                    "broker_position": dict(broker_position),
                    "lifecycle_position": dict(lifecycle_position),
                }
                cost_basis_adjustment = _broker_cost_basis_adjustment(
                    broker_position=broker_position,
                    lifecycle_position=lifecycle_position,
                    signed_quantity=broker_qty,
                    root=broker_root,
                )
                if cost_basis_adjustment is not None:
                    match_detail["broker_cost_basis_adjustment"] = cost_basis_adjustment
                break
        if match_index is None or match_detail is None:
            mismatches.append({"broker_position": dict(broker_position), "unmatched_lifecycle_positions": unmatched_lifecycle})
            continue
        matches.append(match_detail)
        unmatched_lifecycle.pop(match_index)

    if count_mismatch or mismatches or unmatched_lifecycle:
        blocker_code = (
            "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"
            if count_mismatch
            else "TRACK_B_BROKER_LIFECYCLE_POSITION_DETAIL_MISMATCH"
        )
        state = "BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" if count_mismatch else "BROKER_LIFECYCLE_POSITION_DETAIL_MISMATCH"
        detail = (
            "IBKR broker truth and Track B lifecycle report different Track B open-position counts."
            if count_mismatch
            else "IBKR broker truth open positions do not exactly match Track B lifecycle open positions."
        )
        return {
            "matched": False,
            "state": state,
            "broker": [dict(item) for item in broker_positions],
            "lifecycle": [dict(item) for item in lifecycle_positions],
            "matches": matches,
            "mismatches": mismatches,
            "unmatched_lifecycle_positions": unmatched_lifecycle,
            "blocker": {
                "code": blocker_code,
                "detail": detail,
                "broker_position_count": len(broker_positions),
                "lifecycle_position_count": len(lifecycle_positions),
                "broker_positions": [dict(item) for item in broker_positions],
                "lifecycle_positions": [dict(item) for item in lifecycle_positions],
                "matches": matches,
                "mismatches": mismatches,
                "unmatched_lifecycle_positions": unmatched_lifecycle,
            },
        }
    return {"matched": True, "state": "BROKER_AND_LIFECYCLE_OPEN_MATCHED", "matches": matches}


def _broker_cost_basis_adjustments_from_match_report(match_report: Mapping[str, Any]) -> list[dict[str, Any]]:
    matches = match_report.get("matches")
    if not isinstance(matches, list):
        return []
    adjustments: list[dict[str, Any]] = []
    for match in matches:
        if not isinstance(match, Mapping):
            continue
        adjustment = match.get("broker_cost_basis_adjustment")
        if isinstance(adjustment, Mapping):
            adjustments.append(dict(adjustment))
    return adjustments


def _broker_cost_basis_adjustment(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
    signed_quantity: Decimal | None,
    root: str | None,
) -> dict[str, Any] | None:
    broker_average = _broker_average_price(broker_position)
    lifecycle_average = _decimal_value(
        lifecycle_position.get("avg_entry_price")
        or lifecycle_position.get("average_entry_price")
        or lifecycle_position.get("entry_price")
    )
    quantity = abs(signed_quantity) if signed_quantity is not None else _decimal_value(lifecycle_position.get("quantity"))
    if broker_average is None or lifecycle_average is None or quantity is None or quantity == 0:
        return None
    broker_minus_lifecycle = broker_average - lifecycle_average
    total_points = broker_minus_lifecycle * quantity
    return {
        "source": "IBKR_AVERAGE_PRICE_MINUS_TRACK_B_LIFECYCLE_ENTRY_PRICE",
        "root": root,
        "broker_local_symbol": broker_position.get("local_symbol") or broker_position.get("localSymbol"),
        "lifecycle_local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol"),
        "quantity": str(quantity),
        "lifecycle_average_entry_price": str(lifecycle_average),
        "broker_average_price": str(broker_average),
        "broker_minus_lifecycle_points_per_contract": str(broker_minus_lifecycle),
        "broker_minus_lifecycle_points_total": str(total_points),
        "absolute_points_per_contract": str(abs(broker_minus_lifecycle)),
        "absolute_points_total": str(abs(total_points)),
        "note": "Captured for broker fee/cost-basis tracking only; IBKR broker truth remains authoritative for live PAPER position state.",
    }


def _broker_average_price(position: Mapping[str, Any]) -> Decimal | None:
    average_price = _decimal_value(position.get("average_price") or position.get("avg_entry_price"))
    if average_price is not None:
        return average_price
    average_cost = _decimal_value(position.get("average_cost"))
    multiplier = _decimal_value(position.get("multiplier"))
    if average_cost is None:
        return None
    if multiplier is None or multiplier == 0:
        return average_cost
    return average_cost / multiplier


def _signed_lifecycle_quantity(position: Mapping[str, Any], quantity: Decimal | None) -> Decimal | None:
    if quantity is None:
        return None
    side = str(position.get("side") or position.get("position_side") or "").strip().upper()
    if side == "SHORT" and quantity > 0:
        return -quantity
    return quantity


def _local_symbols_compatible(broker_position: Mapping[str, Any], lifecycle_position: Mapping[str, Any]) -> bool:
    broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip().upper()
    lifecycle_local = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip().upper()
    if broker_local and lifecycle_local:
        return broker_local == lifecycle_local
    return True


def _track_b_broker_positions(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("positions") if isinstance(snapshot.get("positions"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        if root is None:
            continue
        qty = _decimal_value(row.get("quantity"))
        if qty is None or qty == 0:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_broker_open_orders(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("open_orders") if isinstance(snapshot.get("open_orders"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        if root is None:
            contract = row.get("contract")
            root = _track_b_root(contract, symbols) if isinstance(contract, Mapping) else None
        if root is None:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_root(row: Mapping[str, Any], symbols: Sequence[str]) -> str | None:
    ordered = sorted((symbol.upper() for symbol in symbols), key=len, reverse=True)
    fields = (
        row.get("symbol"),
        row.get("root"),
        row.get("instrument"),
        row.get("contract_key"),
        row.get("local_symbol"),
        row.get("localSymbol"),
    )
    text_fields = [str(value).upper().replace(" ", "") for value in fields if value not in (None, "")]
    for symbol in ordered:
        for text in text_fields:
            if text == symbol or text.startswith(symbol):
                return symbol
    return None


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decimal_value(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _max_int(*values: Any) -> int:
    return max(_int_value(value) for value in values)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _path_from_payload(value: Any, *, default: Path) -> Path:
    if value in (None, ""):
        return default
    return Path(str(value))


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile Track B PAPER lifecycle summaries with read-only IBKR broker truth.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--ledger-root", default=None)
    parser.add_argument("--broker-truth-root", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--max-age-seconds", type=float, default=DEFAULT_MAX_AGE_SECONDS)
    parser.add_argument("--account", default=PAPER_ACCOUNT)
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root)
    config = ReconciliationConfig(
        repo_root=repo_root,
        ledger_root=Path(args.ledger_root) if args.ledger_root else repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        broker_truth_root=Path(args.broker_truth_root) if args.broker_truth_root else repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
        report_path=Path(args.report_path)
        if args.report_path
        else repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        max_age_seconds=args.max_age_seconds,
        account=str(args.account),
        symbols=tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip()),
    )
    report = reconcile_track_b_paper_broker_truth(config=config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("broker_reconciled") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
