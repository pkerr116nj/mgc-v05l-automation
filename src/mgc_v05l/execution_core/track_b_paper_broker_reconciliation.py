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
from mgc_v05l.execution_core.track_b_exit_safety import (
    DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS,
    bridge_terminal_event_grace_state,
    classify_managed_exit_working_order,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
DEFAULT_BROKER_TRUTH_ROOT = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"
DEFAULT_MARKET_DATA_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_REPORT_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_BROKER_TRUTH_MAX_AGE_SECONDS", "120"))
DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS = float(os.environ.get("TRACK_B_PAPER_BROKER_TRUTH_SETTLEMENT_SECONDS", "300"))
DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS = float(os.environ.get("TRACK_B_PAPER_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS", "15"))
PAPER_ACCOUNT = "DUM882026"
DEFAULT_MIN_TICK_BY_ROOT = {
    "GC": 0.1,
    "MGC": 0.1,
    "NQ": 0.25,
    "MNQ": 0.25,
    "ES": 0.25,
    "MES": 0.25,
    "PL": 0.1,
    "ZT": 0.0078125,
    "ZF": 0.0078125,
    "ZN": 0.015625,
    "ZB": 0.03125,
}


@dataclass(frozen=True)
class ReconciliationConfig:
    repo_root: Path = REPO_ROOT
    ledger_root: Path = DEFAULT_LEDGER_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    report_path: Path = DEFAULT_REPORT_PATH
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS
    bridge_terminal_event_grace_seconds: float = DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS
    broker_truth_settlement_seconds: float = DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS
    broker_truth_settlement_poll_seconds: float = DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS
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
    terminal_event_grace = _bridge_terminal_event_grace_for_flat_lifecycle(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        config=config,
        now=actual_now,
    )
    if terminal_event_grace.get("applied") is True:
        stale_codes = {"BROKER_TRUTH_STATUS_STALE", "BROKER_TRUTH_SNAPSHOT_STALE"}
        stale_blockers = [item for item in blockers if item.get("code") in stale_codes]
        blockers = [item for item in blockers if item.get("code") not in stale_codes]
        terminal_event_grace["downgraded_stale_blockers"] = stale_blockers
    position_match_report = _broker_lifecycle_position_match(
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        symbols=config.symbols,
    )
    known_managed_exit_orders = _known_managed_exit_orders(
        broker_open_orders=track_b_open_orders,
        lifecycle_status=live_position_status,
        position_match_report=position_match_report,
        runtime_restore_orders=_runtime_restore_known_managed_exit_orders(config.repo_root, config.symbols),
        config=config,
        now=actual_now,
    )
    unknown_track_b_open_orders = _unknown_track_b_open_orders(
        broker_open_orders=track_b_open_orders,
        known_managed_exit_orders=known_managed_exit_orders,
    )
    broker_truth_settlement = _broker_truth_settlement_state(
        position_match_report=position_match_report,
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        broker_open_orders=track_b_open_orders,
        unknown_open_orders=unknown_track_b_open_orders,
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        broker_status=broker_status,
        config=config,
        now=actual_now,
    )
    broker_cost_basis_adjustments = _broker_cost_basis_adjustments_from_match_report(position_match_report)
    stale_managed_exit_orders = [
        row
        for row in known_managed_exit_orders
        if str(row.get("managed_order_policy", {}).get("stale_by_policy") or "").lower() == "true"
    ]
    hard_exit_order_not_marketable = [
        row
        for row in known_managed_exit_orders
        if row.get("managed_order_policy", {}).get("classification")
        in {
            "KNOWN_MANAGED_HARD_EXIT_ORDER_NOT_MARKETABLE",
            "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
        }
    ]
    if position_match_report["matched"] is not True:
        settlement_classification = str(broker_truth_settlement.get("classification") or "")
        if settlement_classification == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT":
            pass
        elif settlement_classification in {"BROKER_TRUTH_SETTLEMENT_TIMEOUT", "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE"}:
            blockers.append(
                {
                    "code": settlement_classification,
                    "detail": broker_truth_settlement.get("detail"),
                    "broker_truth_settlement": broker_truth_settlement,
                    "position_match_blocker": position_match_report.get("blocker"),
                }
            )
        else:
            blockers.append(position_match_report["blocker"])
    if unknown_track_b_open_orders:
        blockers.append(
            {
                "code": "UNKNOWN_BROKER_OPEN_ORDER",
                "legacy_code": "TRACK_B_BROKER_OPEN_ORDER_PRESENT",
                "detail": "IBKR broker truth reports Track B futures open orders that are not attributed to a managed exit.",
                "open_orders": unknown_track_b_open_orders,
            }
        )

    settlement_waiting = broker_truth_settlement.get("classification") == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    settlement_resolved = broker_truth_settlement.get("classification") == "BROKER_TRUTH_SETTLEMENT_RESOLVED"
    reconciled = not blockers and position_match_report["matched"] is True
    if settlement_waiting:
        classification = "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    elif settlement_resolved:
        classification = "BROKER_TRUTH_SETTLEMENT_RESOLVED"
    elif blockers and broker_truth_settlement.get("classification") in {
        "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
    }:
        classification = str(broker_truth_settlement.get("classification"))
    elif reconciled and known_managed_exit_orders:
        classification = "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER"
    else:
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
        "bridge_terminal_event_grace_seconds": config.bridge_terminal_event_grace_seconds,
        "broker_truth_settlement_seconds": config.broker_truth_settlement_seconds,
        "broker_truth_settlement_poll_seconds": config.broker_truth_settlement_poll_seconds,
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
        "known_managed_exit_order_count": len(known_managed_exit_orders),
        "stale_managed_exit_order_count": len(stale_managed_exit_orders),
        "hard_exit_order_not_marketable_count": len(hard_exit_order_not_marketable),
        "unknown_broker_open_order_count": len(unknown_track_b_open_orders),
        "track_b_broker_positions": track_b_positions,
        "track_b_broker_open_orders": track_b_open_orders,
        "known_managed_exit_orders": known_managed_exit_orders,
        "stale_managed_exit_orders": stale_managed_exit_orders,
        "hard_exit_order_not_marketable_orders": hard_exit_order_not_marketable,
        "unknown_broker_open_orders": unknown_track_b_open_orders,
        "track_b_lifecycle_positions": lifecycle_positions,
        "position_match_report": position_match_report,
        "broker_cost_basis_adjustments": broker_cost_basis_adjustments,
        "bridge_terminal_event_grace": terminal_event_grace,
        "broker_truth_settlement": broker_truth_settlement,
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


def _bridge_terminal_event_grace_for_flat_lifecycle(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    if _int_value(live_position_status.get("open_position_count")) not in {0, None}:
        return {"applied": False, "reason": "lifecycle positions are not flat"}
    if _int_value(live_position_status.get("open_order_count")) not in {0, None}:
        return {"applied": False, "reason": "lifecycle open orders are not flat"}
    for trade in trade_summary.get("recent_trades", []) or []:
        if not isinstance(trade, Mapping):
            continue
        if not _terminal_trade_has_order_identity(trade):
            continue
        contract_key = str(trade.get("contract_key") or "")
        local_symbol = str(trade.get("local_symbol") or "")
        if _track_b_root({"contract_key": contract_key, "local_symbol": local_symbol}, config.symbols) is None:
            continue
        event = {
            "account_id": trade.get("account_id") or config.account,
            "contract_key": contract_key,
            "local_symbol": local_symbol,
            "con_id": trade.get("con_id"),
            "filled_at": trade.get("exit_timestamp"),
            "broker_order_id": trade.get("exit_order_id"),
            "event_type": "BRIDGE_TERMINAL_CLOSE",
        }
        grace = bridge_terminal_event_grace_state(
            event=event,
            now=now,
            account_id=config.account,
            contract_key=contract_key,
            local_symbol=local_symbol,
            con_id=_int_or_none(trade.get("con_id")),
            ttl_seconds=config.bridge_terminal_event_grace_seconds,
        )
        payload = grace.to_json_dict()
        payload["source"] = "RECENT_TRACK_B_TERMINAL_TRADE"
        if grace.applied:
            return payload
    return {"applied": False, "reason": "no exact recent bridge terminal event inside ttl"}


def _terminal_trade_has_order_identity(trade: Mapping[str, Any]) -> bool:
    return bool(trade.get("exit_timestamp") and trade.get("exit_order_id") and (trade.get("contract_key") or trade.get("local_symbol")))


def _broker_truth_settlement_state(
    *,
    position_match_report: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    unknown_open_orders: Sequence[Mapping[str, Any]],
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    broker_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    base = {
        "window_seconds": config.broker_truth_settlement_seconds,
        "poll_seconds": config.broker_truth_settlement_poll_seconds,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    if broker_status.get("live_money_eligible") is True:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "live_money_eligible=true blocks settlement waiting"}
    if broker_status.get("paper_proof_invoked") is True:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "paper_proof_invoked=true blocks settlement waiting"}
    if position_match_report.get("matched") is True:
        event = _previous_waiting_settlement_event(
            trade_summary=trade_summary,
            live_position_status=live_position_status,
        )
        if event and (_event_age_seconds(event, now) or config.broker_truth_settlement_seconds + 1) <= config.broker_truth_settlement_seconds:
            return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_RESOLVED", "detail": "Broker truth and lifecycle are matched after a recent known broker-effect event.", "event": event}
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_NOT_APPLICABLE", "detail": "broker and lifecycle already match"}
    if unknown_open_orders:
        return {
            **base,
            "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
            "detail": "unknown broker open orders block settlement waiting",
            "unknown_open_order_count": len(unknown_open_orders),
            "unknown_open_orders": [dict(row) for row in unknown_open_orders],
        }
    event = _settlement_event_for_mismatch(
        position_match_report=position_match_report,
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        config=config,
        now=now,
    )
    if not event:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_NOT_APPLICABLE", "detail": "No exact known broker-effect event explains the mismatch."}
    event_age = _event_age_seconds(event, now)
    payload = {
        **base,
        "event": event,
        "event_age_seconds": event_age,
        "broker_position_count": len(broker_positions),
        "lifecycle_position_count": len(lifecycle_positions),
        "open_order_count": len(broker_open_orders),
    }
    if event_age is None:
        return {**payload, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "Known broker-effect event is missing a usable timestamp."}
    if event_age <= config.broker_truth_settlement_seconds:
        return {**payload, "classification": "WAITING_FOR_BROKER_TRUTH_SETTLEMENT", "detail": "Mismatch is temporarily tolerated because it is explained by a recent attributed PAPER broker-effect event."}
    return {**payload, "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT", "detail": "Broker truth did not settle inside the configured PAPER settlement window."}


def _previous_waiting_settlement_event(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
) -> dict[str, Any] | None:
    for source in (live_position_status, trade_summary):
        settlement = source.get("broker_truth_settlement")
        if not isinstance(settlement, Mapping):
            continue
        if settlement.get("classification") != "WAITING_FOR_BROKER_TRUTH_SETTLEMENT":
            continue
        event = settlement.get("event")
        if isinstance(event, Mapping):
            return dict(event)
    return None


def _settlement_event_for_mismatch(
    *,
    position_match_report: Mapping[str, Any],
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any] | None:
    unmatched_lifecycle = [row for row in position_match_report.get("unmatched_lifecycle_positions", []) if isinstance(row, Mapping)]
    mismatches = [row for row in position_match_report.get("mismatches", []) if isinstance(row, Mapping)]
    unmatched_broker = [row.get("broker_position") for row in mismatches if isinstance(row.get("broker_position"), Mapping)]
    if unmatched_lifecycle and not unmatched_broker:
        return _entry_settlement_event(unmatched_lifecycle[0], config=config)
    if unmatched_broker and not unmatched_lifecycle:
        for trade in trade_summary.get("recent_trades", []) or []:
            if not isinstance(trade, Mapping):
                continue
            event = _close_settlement_event(trade, config=config)
            if event and any(_event_matches_position(event, broker) for broker in unmatched_broker if isinstance(broker, Mapping)):
                return event
    return None


def _entry_settlement_event(lifecycle: Mapping[str, Any], *, config: ReconciliationConfig) -> dict[str, Any] | None:
    order_id = lifecycle.get("entry_order_id") or _nested_mapping(lifecycle, "entry_broker_identity").get("broker_order_id")
    timestamp = lifecycle.get("entry_timestamp") or lifecycle.get("entry_time") or lifecycle.get("as_of")
    if not order_id or not timestamp:
        return None
    return {
        "event_type": "ENTRY_FILL_EXPECTING_BROKER_POSITION",
        "account_id": lifecycle.get("account_id") or config.account,
        "contract_key": lifecycle.get("contract_key") or lifecycle.get("position_key"),
        "local_symbol": lifecycle.get("local_symbol") or lifecycle.get("localSymbol"),
        "con_id": lifecycle.get("con_id") or _nested_mapping(lifecycle, "entry_broker_identity").get("con_id"),
        "broker_order_id": str(order_id),
        "event_time": str(timestamp),
        "lifecycle_id": lifecycle.get("lifecycle_id"),
        "strategy_id": lifecycle.get("strategy_id"),
    }


def _close_settlement_event(trade: Mapping[str, Any], *, config: ReconciliationConfig) -> dict[str, Any] | None:
    if not _terminal_trade_has_order_identity(trade):
        return None
    return {
        "event_type": "EXIT_FILL_EXPECTING_BROKER_FLAT",
        "account_id": trade.get("account_id") or config.account,
        "contract_key": trade.get("contract_key"),
        "local_symbol": trade.get("local_symbol") or trade.get("localSymbol"),
        "con_id": trade.get("con_id"),
        "broker_order_id": str(trade.get("exit_order_id")),
        "event_time": str(trade.get("exit_timestamp")),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
    }


def _event_age_seconds(event: Mapping[str, Any], now: datetime) -> float | None:
    event_time = _parse_time(event.get("event_time") or event.get("filled_at") or event.get("submitted_at"))
    if event_time is None:
        return None
    return max((now - event_time).total_seconds(), 0.0)


def _event_matches_position(event: Mapping[str, Any], position: Mapping[str, Any]) -> bool:
    event_con_id = _int_or_none(event.get("con_id"))
    position_con_id = _int_or_none(position.get("con_id") or position.get("conId"))
    if event_con_id is not None and position_con_id is not None and event_con_id != position_con_id:
        return False
    event_local = str(event.get("local_symbol") or "").upper()
    position_local = str(position.get("local_symbol") or position.get("localSymbol") or "").upper()
    if event_local and position_local and event_local != position_local:
        return False
    event_root = _track_b_root(event, PHASE1_RUNTIME_TICKER_ORDER)
    position_root = _track_b_root(position, PHASE1_RUNTIME_TICKER_ORDER)
    return event_root is not None and event_root == position_root


def _known_managed_exit_orders(
    *,
    broker_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_status: Mapping[str, Any],
    position_match_report: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
    runtime_restore_orders: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    declared_orders = _declared_known_managed_exit_orders(lifecycle_status)
    declared_orders.extend(dict(row) for row in runtime_restore_orders if isinstance(row, Mapping))
    if not declared_orders:
        return []
    matched_positions = [
        dict(row)
        for row in position_match_report.get("matches", []) or []
        if isinstance(row, Mapping)
    ]
    known: list[dict[str, Any]] = []
    for broker_order in broker_open_orders:
        for declared in declared_orders:
            if not _broker_order_matches_declared_managed_exit(broker_order=broker_order, declared=declared):
                continue
            matched_position = _matched_position_for_declared_managed_exit(declared=declared, matched_positions=matched_positions)
            if matched_position is None:
                continue
            row = dict(broker_order)
            row["managed_order_status"] = "KNOWN_MANAGED_EXIT_ORDER_WORKING"
            row["lifecycle_id"] = declared.get("lifecycle_id")
            row["strategy_id"] = declared.get("strategy_id")
            row["lane_id"] = declared.get("lane_id")
            row["order_intent_id"] = declared.get("order_intent_id")
            _merge_missing_managed_exit_order_fields(row, declared)
            _merge_missing_managed_exit_order_fields(row, _managed_exit_identity_from_match(matched_position))
            _enrich_managed_exit_order_from_prepared_artifact(row, config.repo_root)
            market_reference = _runtime_market_reference(config=config, root=str(row.get("track_b_root") or row.get("symbol") or ""))
            policy = classify_managed_exit_working_order(
                order=row,
                now=now,
                runtime_market_reference=market_reference.get("price"),
                runtime_market_reference_source=market_reference.get("source_artifact_path"),
            )
            policy_payload = policy.to_json_dict()
            row["managed_order_policy"] = policy_payload
            row["managed_order_status"] = policy_payload["classification"]
            row["exit_urgency"] = policy_payload["exit_urgency"]
            row["order_age_seconds"] = policy_payload["order_age_seconds"]
            row["order_limit_price"] = policy_payload["limit_price"]
            row["runtime_market_reference"] = policy_payload["runtime_market_reference"]
            row["runtime_market_reference_source"] = policy_payload["runtime_market_reference_source"]
            row["distance_from_market_points"] = policy_payload["distance_from_market_points"]
            row["marketable_by_runtime_context"] = policy_payload["marketable_by_runtime_context"]
            row["stale_by_policy"] = policy_payload["stale_by_policy"]
            row["recommended_action"] = policy_payload["recommended_action"]
            proposal = _guarded_cancel_replace_proposal(row, policy_payload)
            if proposal:
                row["guarded_cancel_replace_proposal"] = proposal
            row["source"] = declared.get("source") or "TRACK_B_LIFECYCLE_PENDING_EXIT_ORDER"
            source_artifact_path = declared.get("source_artifact_path")
            if source_artifact_path:
                row["source_artifact_path"] = source_artifact_path
            known.append(row)
            break
    return known


def _runtime_restore_known_managed_exit_orders(repo_root: Path, symbols: Sequence[str]) -> list[dict[str, Any]]:
    lanes_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes"
    rows: list[dict[str, Any]] = []
    try:
        restore_paths = sorted(lanes_root.glob("*/restore_validation_latest.json"))
    except OSError:
        return rows
    for restore_path in restore_paths:
        payload = _load_json(restore_path)
        row = _managed_exit_order_from_restore_payload(payload, source_path=restore_path, symbols=symbols)
        if row is not None:
            rows.append(row)
    return rows


def _merge_missing_managed_exit_order_fields(row: dict[str, Any], declared: Mapping[str, Any]) -> None:
    for key in (
        "submitted_at",
        "acknowledged_at",
        "lifecycle_id",
        "exit_reason",
        "reason_code",
        "hard_exit",
        "exit_family",
        "action",
        "order_action",
        "order_type",
        "limit_price",
        "stop_price",
        "tif",
        "time_in_force",
        "quantity",
        "con_id",
        "conId",
    ):
        if _missing_value(row.get(key)) and not _missing_value(declared.get(key)):
            row[key] = declared.get(key)


def _enrich_managed_exit_order_from_prepared_artifact(row: dict[str, Any], repo_root: Path) -> None:
    lane_id = str(row.get("lane_id") or "").strip()
    if not lane_id:
        return
    base = (
        repo_root
        / "outputs"
        / "reports"
        / "ibkr_runtime_route_dispatch"
        / lane_id
        / "prepared_manual_harness"
    )
    for filename in ("ibkr_manual_paper_close_test_frozen_preview.json", "ibkr_manual_paper_close_test_report.json"):
        payload = _load_json(base / filename)
        if not payload:
            continue
        requested_order = payload.get("requested_order")
        if not isinstance(requested_order, Mapping):
            requested_order = _nested_mapping(payload, "frozen_preview", "requested_order")
        if not requested_order:
            continue
        action = str(requested_order.get("action") or "").strip().upper()
        row_action = str(row.get("action") or "").strip().upper()
        if action and row_action and action != row_action:
            continue
        symbol = str(requested_order.get("symbol") or "").strip().upper()
        row_symbol = str(row.get("symbol") or "").strip().upper()
        if symbol and row_symbol and symbol != row_symbol:
            continue
        for source_key, target_key in (
            ("order_type", "order_type"),
            ("limit_price", "limit_price"),
            ("stop_price", "stop_price"),
            ("time_in_force", "tif"),
            ("quantity", "quantity"),
        ):
            if _missing_value(row.get(target_key)) and not _missing_value(requested_order.get(source_key)):
                row[target_key] = requested_order.get(source_key)
        row["order_price_source_artifact_path"] = str(base / filename)
        return


def _runtime_market_reference(*, config: ReconciliationConfig, root: str) -> dict[str, Any]:
    normalized_root = str(root or "").strip().upper()
    if not normalized_root:
        return {}
    path = config.market_data_root / normalized_root / "1m" / "latest_runtime_candles.json"
    payload = _load_json(path)
    if not payload:
        return {}
    candle = _latest_runtime_candle(payload)
    if not candle:
        return {}
    price = _decimal_value(candle.get("close") or candle.get("last") or candle.get("price"))
    if price is None:
        return {}
    return {
        "price": float(price),
        "bar_start": candle.get("bar_start"),
        "bar_end": candle.get("bar_end"),
        "generated_at": payload.get("generated_at"),
        "source_artifact_path": str(path),
    }


def _guarded_cancel_replace_proposal(row: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any] | None:
    if policy.get("recommended_action") != "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER":
        return None
    root = str(row.get("track_b_root") or row.get("symbol") or "").strip().upper()
    market_reference = _decimal_value(policy.get("runtime_market_reference"))
    if market_reference is None:
        return None
    tick = Decimal(str(DEFAULT_MIN_TICK_BY_ROOT.get(root, 0.25)))
    action = str(row.get("action") or "").strip().upper()
    if action == "SELL":
        replacement_limit = market_reference - tick
    elif action == "BUY":
        replacement_limit = market_reference + tick
    else:
        return None
    replacement_limit = _round_decimal_to_tick(replacement_limit, tick)
    return {
        "enabled": False,
        "requires_explicit_operator_authorization": True,
        "broker_mutation_performed": False,
        "allowed_route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
        "cancel_identity": {
            "account_id": row.get("account_id") or PAPER_ACCOUNT,
            "broker_order_id": row.get("broker_order_id"),
            "client_id": row.get("client_id"),
            "perm_id": row.get("perm_id"),
            "symbol": row.get("symbol"),
            "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
            "expiry": row.get("expiry"),
            "con_id": row.get("con_id") or row.get("conId"),
            "action": row.get("action"),
            "quantity": row.get("quantity"),
        },
        "replacement_order": {
            "account_id": row.get("account_id") or PAPER_ACCOUNT,
            "symbol": row.get("symbol"),
            "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
            "expiry": row.get("expiry"),
            "con_id": row.get("con_id") or row.get("conId"),
            "action": row.get("action"),
            "quantity": row.get("quantity"),
            "order_type": "LMT",
            "tif": row.get("tif") or row.get("time_in_force") or "DAY",
            "limit_price": float(replacement_limit),
            "price_source": "RUNTIME_MARKET_REFERENCE_PLUS_HARD_EXIT_ONE_TICK",
            "runtime_market_reference": float(market_reference),
            "min_tick": float(tick),
        },
    }


def _round_decimal_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    ticks = (price / tick).to_integral_value()
    return ticks * tick


def _latest_runtime_candle(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("candles", "bars"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            last = rows[-1]
            return last if isinstance(last, Mapping) else {}
    return payload


def _missing_value(value: Any) -> bool:
    return value is None or value == ""


def _managed_exit_order_from_restore_payload(
    payload: Mapping[str, Any],
    *,
    source_path: Path,
    symbols: Sequence[str],
) -> dict[str, Any] | None:
    restored = payload.get("restored_state_summary")
    if not isinstance(restored, Mapping):
        return None
    pending_order_ids = [
        str(item).strip()
        for item in restored.get("pending_broker_order_ids", []) or []
        if str(item).strip()
    ]
    open_order_id = str(restored.get("open_broker_order_id") or "").strip()
    order_id = open_order_id or (pending_order_ids[0] if pending_order_ids else "")
    if not order_id:
        return None
    latest_intent_state = str(restored.get("latest_order_intent_state") or "").strip().upper()
    if latest_intent_state in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
        return None
    latest_intent = payload.get("pre_restore_state_summary", {})
    latest_intent = latest_intent.get("latest_order_intent") if isinstance(latest_intent, Mapping) else {}
    if not isinstance(latest_intent, Mapping):
        latest_intent = {}
    intent_type = str(latest_intent.get("intent_type") or restored.get("last_order_intent_id") or "").upper()
    if "SELL_TO_CLOSE" not in intent_type and "BUY_TO_CLOSE" not in intent_type:
        return None
    symbol = str(latest_intent.get("symbol") or latest_intent.get("instrument") or payload.get("symbol") or payload.get("instrument") or "").strip().upper()
    if symbol and symbol not in {item.upper() for item in symbols}:
        return None
    broker_position = _first_mapping(
        _nested_mapping(restored, "broker_snapshot", "broker_truth_position"),
        _nested_mapping(payload, "pre_restore_state_summary", "broker_snapshot", "broker_truth_position"),
    )
    local_symbol = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip()
    expiry = str(broker_position.get("expiry") or "").strip()
    con_id = broker_position.get("con_id") or broker_position.get("conId")
    action = "SELL" if "SELL_TO_CLOSE" in intent_type else "BUY"
    return {
        "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
        "source": "TRACK_B_RUNTIME_RESTORE_PENDING_EXIT_ORDER",
        "source_artifact_path": str(source_path),
        "lifecycle_id": _matched_lifecycle_id_for_restore(payload, local_symbol=local_symbol),
        "strategy_id": latest_intent.get("standalone_strategy_id") or latest_intent.get("strategy_id"),
        "lane_id": latest_intent.get("lane_id") or payload.get("lane_id"),
        "order_intent_id": latest_intent.get("order_intent_id") or restored.get("last_order_intent_id"),
        "broker_order_id": order_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "con_id": con_id,
        "action": action,
        "quantity": latest_intent.get("quantity") or "1",
        "submitted_at": latest_intent.get("submitted_at") or latest_intent.get("acknowledged_at"),
        "exit_reason": latest_intent.get("reason_code"),
    }


def _matched_lifecycle_id_for_restore(payload: Mapping[str, Any], *, local_symbol: str) -> str | None:
    # Runtime restore artifacts are lane-local, so they may not duplicate the
    # lifecycle id. Prefer a direct value, then fall back to the latest fill for
    # the same restored position; the final broker/lifecycle match check still
    # verifies the open lifecycle position before this can become known-managed.
    restored = payload.get("restored_state_summary")
    if isinstance(restored, Mapping):
        lifecycle_id = str(restored.get("lifecycle_id") or "").strip()
        if lifecycle_id:
            return lifecycle_id
    pre_restore = payload.get("pre_restore_state_summary")
    latest_fill = pre_restore.get("latest_fill") if isinstance(pre_restore, Mapping) else None
    if isinstance(latest_fill, Mapping):
        lifecycle_id = str(latest_fill.get("lifecycle_id") or "").strip()
        if lifecycle_id:
            return lifecycle_id
        fill_local_symbol = str(latest_fill.get("local_symbol") or latest_fill.get("localSymbol") or "").strip().upper()
        if local_symbol and fill_local_symbol and local_symbol.upper() != fill_local_symbol:
            return None
    return None


def _declared_known_managed_exit_orders(lifecycle_status: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates: list[Any] = []
    for key in ("known_managed_exit_orders", "pending_managed_exit_orders", "working_managed_exit_orders"):
        value = lifecycle_status.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    for positions_key in ("positions_by_instrument", "positions_by_strategy"):
        positions = lifecycle_status.get(positions_key)
        if not isinstance(positions, Mapping):
            continue
        for position in positions.values():
            if not isinstance(position, Mapping):
                continue
            for key in ("known_managed_exit_orders", "pending_managed_exit_orders", "working_exit_orders"):
                value = position.get(key)
                if isinstance(value, list):
                    candidates.extend(value)
            single = position.get("pending_managed_exit_order")
            if isinstance(single, Mapping):
                candidates.append(single)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        row = dict(candidate)
        status = str(row.get("managed_order_status") or row.get("status") or "").strip().upper()
        if status in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
            continue
        if not _order_id_text(row):
            continue
        rows.append(row)
    return rows


def _broker_order_matches_declared_managed_exit(*, broker_order: Mapping[str, Any], declared: Mapping[str, Any]) -> bool:
    broker_order_id = _order_id_text(broker_order)
    declared_order_id = _order_id_text(declared)
    if broker_order_id and declared_order_id and broker_order_id != declared_order_id:
        return False
    for broker_key, declared_key in (("client_id", "client_id"), ("perm_id", "perm_id")):
        broker_value = str(broker_order.get(broker_key) or broker_order.get(_camel_case(broker_key)) or "").strip()
        declared_value = str(declared.get(declared_key) or declared.get(_camel_case(declared_key)) or "").strip()
        if broker_value and declared_value and broker_value != declared_value:
            return False
    for key in ("symbol", "local_symbol", "expiry"):
        broker_value = str(broker_order.get(key) or broker_order.get(_camel_case(key)) or "").strip().upper()
        declared_value = str(declared.get(key) or declared.get(_camel_case(key)) or "").strip().upper()
        if broker_value and declared_value and broker_value != declared_value:
            return False
    broker_con_id = str(broker_order.get("con_id") or broker_order.get("conId") or broker_order.get("qualified_contract_identifier") or "").strip()
    declared_con_id = str(declared.get("con_id") or declared.get("conId") or declared.get("qualified_contract_identifier") or "").strip()
    if broker_con_id and declared_con_id and broker_con_id != declared_con_id:
        return False
    broker_qty = _decimal_value(
        broker_order.get("remaining_quantity")
        or broker_order.get("remainingQuantity")
        or broker_order.get("total_quantity")
        or broker_order.get("totalQuantity")
        or broker_order.get("quantity")
    )
    declared_qty = _decimal_value(declared.get("qty") or declared.get("quantity"))
    if broker_qty is not None and declared_qty is not None and abs(broker_qty) != abs(declared_qty):
        return False
    broker_action = str(broker_order.get("action") or broker_order.get("order_action") or "").strip().upper()
    declared_action = str(declared.get("action") or declared.get("order_action") or "").strip().upper()
    if broker_action and declared_action and broker_action != declared_action:
        return False
    return True


def _matched_position_for_declared_managed_exit(
    *,
    declared: Mapping[str, Any],
    matched_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    lifecycle_id = str(declared.get("lifecycle_id") or "").strip()
    for match in matched_positions:
        lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
        if lifecycle_id and str(lifecycle_position.get("lifecycle_id") or "").strip() != lifecycle_id:
            continue
        local_symbol = str(declared.get("local_symbol") or declared.get("localSymbol") or "").strip().upper()
        position_local_symbol = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip().upper()
        if local_symbol and position_local_symbol and local_symbol != position_local_symbol:
            continue
        return dict(match)
    return None


def _managed_exit_identity_from_match(match: Mapping[str, Any]) -> dict[str, Any]:
    broker_position = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
    lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
    return {
        "lifecycle_id": lifecycle_position.get("lifecycle_id"),
        "con_id": lifecycle_position.get("con_id") or lifecycle_position.get("conId") or broker_position.get("con_id") or broker_position.get("conId"),
        "local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or broker_position.get("local_symbol") or broker_position.get("localSymbol"),
        "expiry": lifecycle_position.get("expiry") or broker_position.get("expiry"),
        "symbol": lifecycle_position.get("instrument_family") or broker_position.get("symbol"),
    }


def _unknown_track_b_open_orders(
    *,
    broker_open_orders: Sequence[Mapping[str, Any]],
    known_managed_exit_orders: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    known_ids = {_order_id_text(row) for row in known_managed_exit_orders if _order_id_text(row)}
    unknown: list[dict[str, Any]] = []
    for row in broker_open_orders:
        if _order_id_text(row) in known_ids:
            continue
        unknown.append(dict(row))
    return unknown


def _order_id_text(row: Mapping[str, Any]) -> str:
    return str(row.get("broker_order_id") or row.get("order_id") or row.get("orderId") or "").strip()


def _camel_case(key: str) -> str:
    parts = key.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


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
            "broker_track_b_open_order_count": int(report.get("track_b_broker_open_order_count") or 0),
            "known_managed_exit_order_count": int(report.get("known_managed_exit_order_count") or 0),
            "stale_managed_exit_order_count": int(report.get("stale_managed_exit_order_count") or 0),
            "hard_exit_order_not_marketable_count": int(report.get("hard_exit_order_not_marketable_count") or 0),
            "known_managed_exit_orders": [dict(item) for item in report.get("known_managed_exit_orders", []) if isinstance(item, Mapping)],
            "stale_managed_exit_orders": [dict(item) for item in report.get("stale_managed_exit_orders", []) if isinstance(item, Mapping)],
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


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _max_int(*values: Any) -> int:
    return max(_int_value(value) for value in values)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_mapping(*values: object) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _nested_mapping(value: object, *keys: str) -> dict[str, Any]:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return dict(current) if isinstance(current, Mapping) else {}


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
    parser.add_argument("--market-data-root", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--max-age-seconds", type=float, default=DEFAULT_MAX_AGE_SECONDS)
    parser.add_argument("--bridge-terminal-event-grace-seconds", type=float, default=DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS)
    parser.add_argument("--broker-truth-settlement-seconds", type=float, default=DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS)
    parser.add_argument("--broker-truth-settlement-poll-seconds", type=float, default=DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS)
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
        market_data_root=Path(args.market_data_root)
        if args.market_data_root
        else repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        report_path=Path(args.report_path)
        if args.report_path
        else repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        max_age_seconds=args.max_age_seconds,
        bridge_terminal_event_grace_seconds=args.bridge_terminal_event_grace_seconds,
        broker_truth_settlement_seconds=args.broker_truth_settlement_seconds,
        broker_truth_settlement_poll_seconds=args.broker_truth_settlement_poll_seconds,
        account=str(args.account),
        symbols=tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip()),
    )
    report = reconcile_track_b_paper_broker_truth(config=config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("broker_reconciled") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
