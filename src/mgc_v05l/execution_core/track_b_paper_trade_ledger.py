"""Compact Track B PAPER trade ledger and P&L summaries.

This module is a read-model sidecar for guarded Track B PAPER lifecycle
artifacts. It does not query a broker and must not become a submit path.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/paper_trade_ledger")
DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL = (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "track_b_paper_trade_ledger.jsonl"
)
DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON = (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON = (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_live_position_status.json"
)
DEFAULT_TRACK_B_PNL_SUMMARY_JSON = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_pnl_summary.json"
DEFAULT_TRACK_B_ARTIFACT_RECONCILIATION_REPORT_JSON = (
    Path("outputs/track_b_execution_core/diagnostics") / "latest_track_b_artifact_reconciliation_report.json"
)
LEDGER_SCHEMA_VERSION = "track_b_paper_trade_ledger_v1"
RECONCILIATION_SCHEMA_VERSION = "track_b_artifact_reconciliation_v1"
SUMMARY_SCHEMA_VERSION = "track_b_paper_trade_summary_v1"
POSITION_SCHEMA_VERSION = "track_b_live_position_status_v1"
PNL_SCHEMA_VERSION = "track_b_pnl_summary_v1"
MANUALLY_FLATTENED_REVIEWED = "MANUALLY_FLATTENED_REVIEWED"
POINT_VALUE_BY_FAMILY = {
    "MGC": Decimal("10"),
    "GC": Decimal("100"),
    "MNQ": Decimal("2"),
    "NQ": Decimal("20"),
    "MES": Decimal("5"),
    "ES": Decimal("50"),
}
TICK_SIZE_BY_FAMILY = {
    "MGC": Decimal("0.1"),
    "GC": Decimal("0.1"),
    "MNQ": Decimal("0.25"),
    "NQ": Decimal("0.25"),
    "MES": Decimal("0.25"),
    "ES": Decimal("0.25"),
}


@dataclass(frozen=True)
class TrackBPaperTradeLedgerResult:
    ledger_jsonl: Path
    trade_summary_json: Path
    live_position_status_json: Path
    pnl_summary_json: Path
    trade_record_written: bool
    trade_record: dict[str, Any] | None
    trade_summary: dict[str, Any]
    live_position_status: dict[str, Any]
    pnl_summary: dict[str, Any]


@dataclass(frozen=True)
class TrackBArtifactReconciliationResult:
    ledger_jsonl: Path
    trade_summary_json: Path
    live_position_status_json: Path
    pnl_summary_json: Path
    reconciliation_report_json: Path
    reconciliation_record_written: bool
    reconciliation_report: dict[str, Any]
    trade_summary: dict[str, Any]
    live_position_status: dict[str, Any]
    pnl_summary: dict[str, Any]


def reconcile_manually_flattened_proof_lifecycle(
    *,
    lifecycle_id: str,
    preflight_report_json: Path,
    recovery_report_json: Path | None = None,
    ledger_jsonl: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL,
    output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    diagnostics_root: Path = Path("outputs/track_b_execution_core/diagnostics"),
    expected_account_id: str | None = None,
    expected_contract_key: str | None = None,
    expected_local_symbol: str | None = None,
    expected_con_id: int | None = None,
    now: datetime | None = None,
) -> TrackBArtifactReconciliationResult:
    """Archive a stale proof/canary ledger row after read-only broker flat proof.

    This is an artifact-only read-model repair. It never calls a broker and only
    trusts broker truth already captured in read-only preflight/recovery reports.
    """

    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    ledger_path = Path(ledger_jsonl)
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    diagnostics_root.mkdir(parents=True, exist_ok=True)
    if not ledger_path.exists():
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.touch()
    trade_summary_json = root / "latest_track_b_paper_trade_summary.json"
    live_position_status_json = root / "latest_track_b_live_position_status.json"
    pnl_summary_json = root / "latest_track_b_pnl_summary.json"
    reconciliation_report_json = diagnostics_root / "latest_track_b_artifact_reconciliation_report.json"

    records = _read_ledger_records(ledger_path)
    target = _latest_lifecycle_record(records, lifecycle_id)
    preflight = _load_json_path(preflight_report_json)
    recovery = _load_json_path(recovery_report_json) if recovery_report_json else {}
    broker_check = _broker_flat_confirmation(
        preflight=preflight,
        recovery=recovery,
        target=target,
        expected_account_id=expected_account_id,
        expected_contract_key=expected_contract_key,
        expected_local_symbol=expected_local_symbol,
        expected_con_id=expected_con_id,
    )
    proof_check = _proof_canary_confirmation(target)
    existing_resolution = _existing_manual_flat_reconciliation(records, lifecycle_id)
    can_archive = target is not None and proof_check["is_proof_canary"] is True and broker_check["broker_flat_confirmed"] is True
    action = MANUALLY_FLATTENED_REVIEWED if can_archive else "NO_ARCHIVE_REVIEW_REQUIRED"
    report = {
        "schema_version": RECONCILIATION_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "strategy_id": None if target is None else target.get("strategy_id"),
        "instrument": None if target is None else target.get("contract_key"),
        "contract_key": None if target is None else target.get("contract_key"),
        "local_symbol": None if target is None else target.get("local_symbol"),
        "con_id": None if target is None else target.get("con_id"),
        "prior_artifact_state": _prior_artifact_state(target),
        "proof_canary_confirmation": proof_check,
        "broker_flat_confirmation": broker_check,
        "open_orders_confirmation": {
            "open_orders_none": broker_check["open_orders_none"],
            "open_order_count": broker_check["open_order_count"],
        },
        "reconciliation_action": action,
        "new_artifact_classification": action if can_archive else None,
        "compact_summaries_updated": False,
        "proof_canary_excluded_from_managed_trade_counts": True,
        "broker_mutation_attempted": False,
        "submit_attempted": False,
        "paper_proof_cli_invoked": False,
        "reconciliation_source": "READ_ONLY_PREFLIGHT_RECOVERY_REPORTS",
        "preflight_report_path": str(preflight_report_json),
        "recovery_report_path": str(recovery_report_json) if recovery_report_json else None,
        "existing_resolution_record_found": existing_resolution is not None,
        "remaining_blocker": None if can_archive else _reconciliation_blocker(target, proof_check, broker_check),
    }
    wrote = False
    if can_archive and existing_resolution is None:
        reconciliation_record = _manual_flat_reconciliation_record(
            target=target,
            broker_check=broker_check,
            preflight_report_json=preflight_report_json,
            recovery_report_json=recovery_report_json,
            now=actual_now,
        )
        with ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(to_jsonable(reconciliation_record), sort_keys=True) + "\n")
        records.append(reconciliation_record)
        wrote = True

    summaries = build_track_b_paper_trade_summaries(
        ledger_records=records,
        ledger_jsonl=ledger_path,
        trade_summary_json=trade_summary_json,
        live_position_status_json=live_position_status_json,
        pnl_summary_json=pnl_summary_json,
        now=actual_now,
    )
    _write_json(trade_summary_json, summaries["trade_summary"])
    _write_json(live_position_status_json, summaries["live_position_status"])
    _write_json(pnl_summary_json, summaries["pnl_summary"])
    report["compact_summaries_updated"] = True
    report["post_reconciliation_summary"] = {
        "open_position_count": summaries["trade_summary"].get("open_position_count"),
        "review_required_count": summaries["trade_summary"].get("review_required_count"),
        "managed_strategy_trade_count": summaries["trade_summary"].get("managed_strategy_trade_count"),
        "proof_canary_trade_count": summaries["trade_summary"].get("proof_canary_trade_count"),
        "archived_manual_flat_count": summaries["trade_summary"].get("archived_manual_flat_count"),
    }
    _write_json(reconciliation_report_json, report)
    return TrackBArtifactReconciliationResult(
        ledger_jsonl=ledger_path,
        trade_summary_json=trade_summary_json,
        live_position_status_json=live_position_status_json,
        pnl_summary_json=pnl_summary_json,
        reconciliation_report_json=reconciliation_report_json,
        reconciliation_record_written=wrote,
        reconciliation_report=report,
        trade_summary=summaries["trade_summary"],
        live_position_status=summaries["live_position_status"],
        pnl_summary=summaries["pnl_summary"],
    )


def update_track_b_paper_trade_ledger_from_runner_report(
    *,
    runner_report: Mapping[str, Any],
    runner_report_json: Path | None = None,
    monitor_report_json: Path | None = None,
    output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    now: datetime | None = None,
) -> TrackBPaperTradeLedgerResult:
    """Append one compact trade row when a guarded PAPER lifecycle ran.

    Existing trade ids are respected, making the update safe to retry.
    """

    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    ledger_jsonl = root / "track_b_paper_trade_ledger.jsonl"
    trade_summary_json = root / "latest_track_b_paper_trade_summary.json"
    live_position_status_json = root / "latest_track_b_live_position_status.json"
    pnl_summary_json = root / "latest_track_b_pnl_summary.json"
    ledger_jsonl.touch(exist_ok=True)

    existing_records = _read_ledger_records(ledger_jsonl)
    existing_trade_ids = {str(item.get("trade_id")) for item in existing_records if item.get("trade_id")}
    trade_record = _trade_record_from_runner_report(
        runner_report=runner_report,
        runner_report_json=runner_report_json,
        monitor_report_json=monitor_report_json,
        now=actual_now,
    )
    wrote = False
    if trade_record is not None and str(trade_record["trade_id"]) not in existing_trade_ids:
        with ledger_jsonl.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(to_jsonable(trade_record), sort_keys=True) + "\n")
        existing_records.append(trade_record)
        wrote = True

    summaries = build_track_b_paper_trade_summaries(
        ledger_records=existing_records,
        ledger_jsonl=ledger_jsonl,
        trade_summary_json=trade_summary_json,
        live_position_status_json=live_position_status_json,
        pnl_summary_json=pnl_summary_json,
        now=actual_now,
    )
    _write_json(trade_summary_json, summaries["trade_summary"])
    _write_json(live_position_status_json, summaries["live_position_status"])
    _write_json(pnl_summary_json, summaries["pnl_summary"])
    return TrackBPaperTradeLedgerResult(
        ledger_jsonl=ledger_jsonl,
        trade_summary_json=trade_summary_json,
        live_position_status_json=live_position_status_json,
        pnl_summary_json=pnl_summary_json,
        trade_record_written=wrote,
        trade_record=trade_record,
        trade_summary=summaries["trade_summary"],
        live_position_status=summaries["live_position_status"],
        pnl_summary=summaries["pnl_summary"],
    )


def build_track_b_paper_trade_summaries(
    *,
    ledger_records: Iterable[Mapping[str, Any]],
    ledger_jsonl: Path,
    trade_summary_json: Path,
    live_position_status_json: Path,
    pnl_summary_json: Path,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    raw_records = [dict(item) for item in ledger_records]
    records = _apply_manual_flat_reconciliations(raw_records)
    trade_records = [item for item in records if not _is_reconciliation_record(item)]
    today = actual_now.date().isoformat()
    week_start = (actual_now.date() - timedelta(days=actual_now.weekday())).isoformat()
    month_start = actual_now.date().replace(day=1).isoformat()
    year_start = actual_now.date().replace(month=1, day=1).isoformat()
    today_records = [item for item in trade_records if _date_prefix(item.get("exit_timestamp") or item.get("created_at")) == today]
    week_records = [
        item
        for item in trade_records
        if str(item.get("exit_timestamp") or item.get("created_at") or "")[:10] >= week_start
    ]
    month_records = [
        item
        for item in trade_records
        if str(item.get("exit_timestamp") or item.get("created_at") or "")[:10] >= month_start
    ]
    ytd_records = [
        item
        for item in trade_records
        if str(item.get("exit_timestamp") or item.get("created_at") or "")[:10] >= year_start
    ]
    open_records = [item for item in trade_records if _is_open_position_record(item)]
    review_required = [item for item in trade_records if item.get("review_required") is True and not _is_manual_flat_reviewed(item)]
    managed_records = [item for item in trade_records if item.get("paper_lifecycle_type") == "STRATEGY_MANAGED"]
    proof_canary_records = [item for item in trade_records if _is_proof_canary_record(item)]
    archived_manual_flat = [item for item in trade_records if _is_manual_flat_reviewed(item)]
    last_trade = max(trade_records, key=lambda item: str(item.get("entry_timestamp") or item.get("created_at") or ""), default=None)
    recent_trades = sorted(
        trade_records,
        key=lambda item: str(item.get("exit_timestamp") or item.get("entry_timestamp") or item.get("created_at") or ""),
        reverse=True,
    )[:20]

    trade_summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "as_of": actual_now.isoformat(),
        "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "broker_reconciled": False,
        "trade_count": len(trade_records),
        "closed_trade_count": sum(1 for item in trade_records if _is_flat_closed_trade(item)),
        "completed_trade_count": sum(1 for item in trade_records if _is_flat_closed_trade(item)),
        "managed_strategy_trade_count": len(managed_records),
        "meaningful_strategy_trade_count": len(managed_records),
        "proof_canary_trade_count": len(proof_canary_records),
        "archived_manual_flat_count": len(archived_manual_flat),
        "open_position_count": len(open_records),
        "review_required_count": len(review_required),
        "paper_trades_attempted_count": len(trade_records),
        "proof_canary_excluded_from_meaningful_strategy_counts": True,
        "recent_trades": [_compact_trade_row(item) for item in recent_trades],
        "last_trade_time": None if last_trade is None else last_trade.get("entry_timestamp") or last_trade.get("created_at"),
        "last_trade_strategy": None if last_trade is None else last_trade.get("strategy_id"),
        "last_trade_pnl": None if last_trade is None else last_trade.get("realized_pnl"),
        "latest_trade_ledger_path": str(ledger_jsonl),
        "latest_trade_summary_path": str(trade_summary_json),
        "latest_live_position_status_path": str(live_position_status_json),
        "latest_pnl_summary_path": str(pnl_summary_json),
        "hot_path_note": "Dashboard should read compact summaries only; do not scan the full ledger.",
    }

    live_position_status = {
        "schema_version": POSITION_SCHEMA_VERSION,
        "as_of": actual_now.isoformat(),
        "account_id": _first(records, "account_id"),
        "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "broker_reconciled": False,
        "positions_by_instrument": _positions_by(trade_records, "contract_key", actual_now),
        "positions_by_strategy": _positions_by(trade_records, "strategy_id", actual_now),
        "open_position_count": len(open_records),
        "open_order_count": 0,
        "total_unrealized_pnl": "0",
        "realized_pnl_today": _sum_decimal(today_records, "realized_pnl"),
        "review_required_positions": [item for item in open_records if item.get("review_required") is True],
        "last_broker_reconciliation_time": None,
        "source_artifact_paths": _source_paths(trade_records),
        "broker_truth_warning": "Artifact-derived status is not broker truth until source=BROKER_RECONCILED.",
    }

    wins = [item for item in trade_records if (_decimal(item.get("realized_pnl")) or Decimal("0")) > 0]
    losses = [item for item in trade_records if (_decimal(item.get("realized_pnl")) or Decimal("0")) < 0]
    pnl_summary = {
        "schema_version": PNL_SCHEMA_VERSION,
        "as_of": actual_now.isoformat(),
        "date": today,
        "week_start": week_start,
        "month_start": month_start,
        "year_start": year_start,
        "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "broker_reconciled": False,
        "total_realized_pnl_today": _sum_decimal(today_records, "realized_pnl"),
        "total_realized_pnl_week": _sum_decimal(week_records, "realized_pnl"),
        "total_realized_pnl_month": _sum_decimal(month_records, "realized_pnl"),
        "total_realized_pnl_ytd": _sum_decimal(ytd_records, "realized_pnl"),
        "total_realized_pnl_session": _sum_decimal(today_records, "realized_pnl"),
        "total_unrealized_pnl": "0",
        "trades_today": len(today_records),
        "trades_week": len(week_records),
        "trades_month": len(month_records),
        "trades_ytd": len(ytd_records),
        "trades_session": len(today_records),
        "completed_trades": trade_summary["closed_trade_count"],
        "wins": len(wins),
        "losses": len(losses),
        "avg_win": _average_decimal(wins, "realized_pnl"),
        "avg_loss": _average_decimal(losses, "realized_pnl"),
        "by_strategy": _pnl_groups(trade_records, "strategy_id", actual_now),
        "by_instrument": _pnl_groups(trade_records, "contract_key", actual_now),
        "by_side": _pnl_groups(trade_records, "side", actual_now),
        "by_lifecycle_classification": _pnl_groups(trade_records, "paper_lifecycle_classification", actual_now),
        "review_required_count": len(review_required),
        "last_trade_time": trade_summary["last_trade_time"],
        "last_trade_strategy": trade_summary["last_trade_strategy"],
        "last_trade_pnl": trade_summary["last_trade_pnl"],
        "latest_trade_ledger_path": str(ledger_jsonl),
    }
    return {
        "trade_summary": trade_summary,
        "live_position_status": live_position_status,
        "pnl_summary": pnl_summary,
    }


def _trade_record_from_runner_report(
    *,
    runner_report: Mapping[str, Any],
    runner_report_json: Path | None,
    monitor_report_json: Path | None,
    now: datetime,
) -> dict[str, Any] | None:
    if runner_report.get("managed_lifecycle_invoked") is True or runner_report.get("strategy_managed_lifecycle_invoked") is True:
        return _managed_trade_record_from_runner_report(
            runner_report=runner_report,
            runner_report_json=runner_report_json,
            monitor_report_json=monitor_report_json,
            now=now,
        )
    if runner_report.get("paper_proof_invoked") is not True:
        return None
    proof_report = _load_json_path(runner_report.get("paper_proof_report_path"))
    proof_payload = proof_report.get("proof_payload") if isinstance(proof_report.get("proof_payload"), Mapping) else {}
    open_intent = _mapping(runner_report.get("open_intent")) or _mapping(proof_payload.get("open_intent"))
    close_intent = _mapping(runner_report.get("close_intent")) or _mapping(proof_payload.get("close_intent"))
    open_fill = _mapping(runner_report.get("open_fill")) or _mapping(proof_payload.get("open_fill"))
    close_fill = _mapping(runner_report.get("close_fill")) or _mapping(proof_payload.get("close_fill"))
    open_submit = _mapping(runner_report.get("open_submit_attempt")) or _mapping(proof_payload.get("open_submit_attempt"))
    close_submit = _mapping(runner_report.get("close_submit_attempt")) or _mapping(proof_payload.get("close_submit_attempt"))
    contract_key = str(runner_report.get("contract_key") or proof_report.get("contract_key") or open_intent.get("contract_key") or "")
    instrument_family = _instrument_family(contract_key, runner_report.get("local_symbol"))
    side = _side_from_runner(runner_report, open_intent)
    quantity = _decimal(runner_report.get("quantity") or open_fill.get("quantity") or open_intent.get("quantity"))
    entry_fill_price = _decimal(open_fill.get("price"))
    exit_fill_price = _decimal(close_fill.get("price"))
    realized = _realized_pnl(
        side=side,
        quantity=quantity,
        entry=entry_fill_price,
        exit=exit_fill_price,
        instrument_family=instrument_family,
    )
    tick_size = TICK_SIZE_BY_FAMILY.get(instrument_family)
    points = _points_pnl(side=side, entry=entry_fill_price, exit=exit_fill_price)
    ticks = None if points is None or tick_size in {None, Decimal("0")} else points / tick_size
    lifecycle_id = str(
        proof_payload.get("run_id")
        or proof_report.get("run_id")
        or runner_report.get("track_b_strategy_paper_runner_id")
        or uuid.uuid4().hex
    )
    signal_id = str(open_intent.get("signal_event_id") or runner_report.get("signal_id") or lifecycle_id)
    strategy_id = str(runner_report.get("strategy_id") or runner_report.get("signal_source") or "UNKNOWN")
    return {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "trade_id": f"{strategy_id}:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "signal_id": signal_id,
        "strategy_id": strategy_id,
        "instrument_family": instrument_family,
        "contract_key": contract_key or None,
        "local_symbol": runner_report.get("local_symbol") or _nested_get(proof_report, ("config", "local_symbol")),
        "con_id": runner_report.get("con_id") or _nested_get(proof_report, ("config", "con_id")),
        "account_id": runner_report.get("account_id") or proof_report.get("account_id") or open_intent.get("account_id"),
        "monitor_mode": runner_report.get("mode"),
        "runtime_decision_source": runner_report.get("runtime_decision_source") or runner_report.get("runtime_data_source"),
        "signal_timestamp": runner_report.get("signal_timestamp") or open_intent.get("created_at"),
        "entry_timestamp": open_fill.get("filled_at") or open_submit.get("submitted_at") or open_intent.get("created_at"),
        "exit_timestamp": close_fill.get("filled_at") or close_submit.get("submitted_at") or close_intent.get("created_at"),
        "side": side,
        "order_action": open_intent.get("action"),
        "quantity": _decimal_text(quantity),
        "entry_order_id": open_fill.get("broker_order_id") or open_submit.get("broker_order_id"),
        "exit_order_id": close_fill.get("broker_order_id") or close_submit.get("broker_order_id"),
        "entry_limit_price": _string_or_none(open_intent.get("limit_price") or runner_report.get("manual_open_limit_price")),
        "entry_fill_price": _decimal_text(entry_fill_price),
        "exit_limit_price": _string_or_none(close_intent.get("limit_price") or runner_report.get("manual_close_limit_price")),
        "exit_fill_price": _decimal_text(exit_fill_price),
        "realized_pnl": _decimal_text(realized),
        "pnl_currency": "USD",
        "ticks_pnl": _decimal_text(ticks),
        "points_pnl": _decimal_text(points),
        "commissions": None,
        "slippage_vs_reference": None,
        "strategy_verdict": runner_report.get("strategy_paper_runner_verdict"),
        "paper_lifecycle_classification": runner_report.get("paper_proof_lifecycle_status") or proof_payload.get("proof_lifecycle_status"),
        "paper_proof_classification": runner_report.get("paper_proof_classification") or proof_report.get("classification"),
        "final_broker_state_classification": (
            runner_report.get("final_broker_state_classification")
            or runner_report.get("latest_broker_state_classification")
            or proof_report.get("final_broker_state_classification")
            or runner_report.get("paper_proof_classification")
            or proof_report.get("classification")
        ),
        "final_position_status": runner_report.get("final_position_status") or _nested_get(proof_payload, ("final_reconciliation", "status")),
        "review_required": _review_required(runner_report, proof_report, proof_payload),
        "paper_lifecycle_report_path": str(runner_report.get("paper_proof_report_path") or proof_report.get("proof_report_json") or ""),
        "decision_journal_record_id": None,
        "decision_journal_record_path": runner_report.get("decision_journal_record_path"),
        "monitor_report_path": str(monitor_report_json) if monitor_report_json else runner_report.get("monitor_report_path"),
        "strategy_paper_runner_report_path": str(runner_report_json) if runner_report_json else runner_report.get("report_json_path"),
        "created_at": now.isoformat(),
        "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "broker_reconciled": False,
    }


def _managed_trade_record_from_runner_report(
    *,
    runner_report: Mapping[str, Any],
    runner_report_json: Path | None,
    monitor_report_json: Path | None,
    now: datetime,
) -> dict[str, Any] | None:
    lifecycle_report = _load_json_path(runner_report.get("managed_lifecycle_report_path"))
    entry_intent = _mapping(runner_report.get("managed_entry_intent")) or _mapping(lifecycle_report.get("entry_intent"))
    if not entry_intent:
        return None
    close_intent = _mapping(runner_report.get("managed_close_intent")) or _mapping(lifecycle_report.get("close_intent"))
    entry_fill = _mapping(runner_report.get("managed_entry_fill")) or _mapping(lifecycle_report.get("entry_fill"))
    close_fill = _mapping(runner_report.get("managed_close_fill")) or _mapping(lifecycle_report.get("close_fill"))
    entry_submit = _mapping(runner_report.get("managed_entry_submit_attempt")) or _mapping(lifecycle_report.get("entry_submit_attempt"))
    close_submit = _mapping(runner_report.get("managed_close_submit_attempt")) or _mapping(lifecycle_report.get("close_submit_attempt"))
    contract_key = str(runner_report.get("contract_key") or lifecycle_report.get("contract_key") or entry_intent.get("contract_key") or "")
    instrument_family = str(
        runner_report.get("strategy_registry_instrument_family")
        or lifecycle_report.get("instrument_family")
        or _instrument_family(contract_key, runner_report.get("local_symbol"))
    )
    side = str(entry_intent.get("side") or runner_report.get("signal_direction") or "UNKNOWN")
    quantity = _decimal(runner_report.get("quantity") or entry_fill.get("quantity") or entry_intent.get("quantity"))
    entry_fill_price = _decimal(entry_fill.get("price") or entry_fill.get("avg_price"))
    exit_fill_price = _decimal(close_fill.get("price") or close_fill.get("avg_price"))
    realized = _realized_pnl(
        side=side,
        quantity=quantity,
        entry=entry_fill_price,
        exit=exit_fill_price,
        instrument_family=instrument_family,
    )
    tick_size = TICK_SIZE_BY_FAMILY.get(instrument_family)
    points = _points_pnl(side=side, entry=entry_fill_price, exit=exit_fill_price)
    ticks = None if points is None or tick_size in {None, Decimal("0")} else points / tick_size
    lifecycle_id = str(lifecycle_report.get("lifecycle_id") or runner_report.get("managed_lifecycle_id") or runner_report.get("track_b_strategy_paper_runner_id"))
    strategy_id = str(runner_report.get("strategy_id") or lifecycle_report.get("strategy_id") or entry_intent.get("strategy_id") or "UNKNOWN")
    classification = str(runner_report.get("managed_lifecycle_classification") or lifecycle_report.get("strategy_managed_lifecycle_classification") or "")
    return {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "trade_id": f"{strategy_id}:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "signal_id": str(entry_intent.get("signal_id") or entry_intent.get("lifecycle_id") or lifecycle_id),
        "strategy_id": strategy_id,
        "instrument_family": instrument_family,
        "contract_key": contract_key or None,
        "local_symbol": runner_report.get("local_symbol") or lifecycle_report.get("local_symbol") or entry_intent.get("local_symbol"),
        "con_id": runner_report.get("con_id") or lifecycle_report.get("con_id") or entry_intent.get("con_id"),
        "account_id": runner_report.get("account_id") or lifecycle_report.get("account_id") or entry_intent.get("account_id"),
        "monitor_mode": runner_report.get("mode"),
        "runtime_decision_source": runner_report.get("runtime_decision_source") or entry_intent.get("latest_decision_bar_source"),
        "signal_timestamp": runner_report.get("signal_timestamp") or entry_intent.get("signal_timestamp"),
        "entry_timestamp": entry_fill.get("filled_at") or entry_submit.get("submitted_at") or entry_intent.get("created_at"),
        "exit_timestamp": close_fill.get("filled_at") or close_submit.get("submitted_at") or (close_intent or {}).get("created_at"),
        "side": side,
        "order_action": entry_intent.get("order_action"),
        "quantity": _decimal_text(quantity),
        "entry_order_id": entry_fill.get("broker_order_id") or entry_submit.get("broker_order_id"),
        "exit_order_id": close_fill.get("broker_order_id") or close_submit.get("broker_order_id"),
        "entry_limit_price": _string_or_none(entry_intent.get("entry_limit_price")),
        "entry_fill_price": _decimal_text(entry_fill_price),
        "exit_limit_price": _string_or_none((close_intent or {}).get("close_limit_price")),
        "exit_fill_price": _decimal_text(exit_fill_price),
        "realized_pnl": _decimal_text(realized),
        "pnl_currency": "USD",
        "ticks_pnl": _decimal_text(ticks),
        "points_pnl": _decimal_text(points),
        "commissions": None,
        "slippage_vs_reference": None,
        "strategy_verdict": runner_report.get("strategy_paper_runner_verdict"),
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "paper_lifecycle_classification": classification or lifecycle_report.get("paper_lifecycle_classification"),
        "paper_proof_classification": None,
        "managed_exit_policy_id": runner_report.get("managed_exit_policy_id") or lifecycle_report.get("managed_exit_policy_id"),
        "final_broker_state_classification": runner_report.get("final_broker_state_classification") or lifecycle_report.get("final_broker_state_classification") or classification,
        "final_position_status": runner_report.get("final_position_status") or lifecycle_report.get("final_position_status"),
        "review_required": bool(lifecycle_report.get("review_required")) or "REVIEW" in classification or "MISMATCH" in classification,
        "paper_lifecycle_report_path": str(runner_report.get("managed_lifecycle_report_path") or lifecycle_report.get("report_json_path") or ""),
        "decision_journal_record_id": None,
        "decision_journal_record_path": runner_report.get("decision_journal_record_path"),
        "monitor_report_path": str(monitor_report_json) if monitor_report_json else runner_report.get("monitor_report_path"),
        "strategy_paper_runner_report_path": str(runner_report_json) if runner_report_json else runner_report.get("report_json_path"),
        "created_at": now.isoformat(),
        "source": "TRACK_B_STRATEGY_MANAGED_LIFECYCLE",
        "broker_reconciled": bool(lifecycle_report.get("broker_reconciled")),
    }


def _latest_lifecycle_record(records: Iterable[Mapping[str, Any]], lifecycle_id: str) -> dict[str, Any] | None:
    matches = [
        dict(item)
        for item in records
        if str(item.get("lifecycle_id") or "") == lifecycle_id and not _is_reconciliation_record(item)
    ]
    return matches[-1] if matches else None


def _existing_manual_flat_reconciliation(records: Iterable[Mapping[str, Any]], lifecycle_id: str) -> dict[str, Any] | None:
    matches = [
        dict(item)
        for item in records
        if _is_reconciliation_record(item)
        and str(item.get("lifecycle_id") or "") == lifecycle_id
        and item.get("new_artifact_classification") == MANUALLY_FLATTENED_REVIEWED
    ]
    return matches[-1] if matches else None


def _proof_canary_confirmation(target: Mapping[str, Any] | None) -> dict[str, Any]:
    is_proof = False if target is None else _is_proof_canary_record(target)
    return {
        "is_proof_canary": is_proof,
        "paper_lifecycle_type": None if target is None else target.get("paper_lifecycle_type"),
        "paper_proof_classification": None if target is None else target.get("paper_proof_classification"),
        "lifecycle_id": None if target is None else target.get("lifecycle_id"),
    }


def _broker_flat_confirmation(
    *,
    preflight: Mapping[str, Any],
    recovery: Mapping[str, Any],
    target: Mapping[str, Any] | None,
    expected_account_id: str | None,
    expected_contract_key: str | None,
    expected_local_symbol: str | None,
    expected_con_id: int | None,
) -> dict[str, Any]:
    position = _mapping(preflight.get("position"))
    contract = _mapping(preflight.get("contract"))
    raw_rows = _nested_get(position, ("raw", "rows"))
    first_row = raw_rows[0] if isinstance(raw_rows, list) and raw_rows and isinstance(raw_rows[0], Mapping) else {}
    open_orders = preflight.get("open_orders")
    open_order_count = len(open_orders) if isinstance(open_orders, list) else None
    target_account = expected_account_id or (None if target is None else _string_or_none(target.get("account_id")))
    target_contract = expected_contract_key or (None if target is None else _string_or_none(target.get("contract_key")))
    target_local_symbol = expected_local_symbol or (None if target is None else _string_or_none(target.get("local_symbol")))
    target_con_id = expected_con_id if expected_con_id is not None else (None if target is None else target.get("con_id"))
    signed_quantity = _decimal(position.get("signed_quantity"))
    con_id_value = contract.get("con_id") or first_row.get("con_id")
    local_symbol_value = contract.get("local_symbol") or first_row.get("local_symbol")
    contract_key_value = preflight.get("contract_key") or position.get("contract_key") or contract.get("contract_key")
    account_value = preflight.get("account_id") or position.get("account_id") or first_row.get("account_id")
    checks = preflight.get("checks") if isinstance(preflight.get("checks"), list) else []
    check_map = {str(item.get("name")): bool(item.get("passed")) for item in checks if isinstance(item, Mapping)}
    submit_attempted = bool(preflight.get("submit_attempted") or _nested_get(preflight, ("safety", "submit_attempted")))
    recovery_clean = recovery in ({}, None) or str(recovery.get("classification") or "") == "RECOVERY_READY_CLEAN"
    contract_matches = (
        (target_contract in {None, "", str(contract_key_value)})
        and (target_local_symbol in {None, "", str(local_symbol_value)})
        and (target_con_id in {None, "", con_id_value, str(con_id_value)})
        and (target_account in {None, "", str(account_value)})
    )
    flat = signed_quantity == Decimal("0")
    open_orders_none = open_order_count == 0
    preflight_ready = str(preflight.get("classification") or "") == "READY_READ_ONLY"
    broker_flat_confirmed = (
        preflight_ready
        and recovery_clean
        and contract_matches
        and flat
        and open_orders_none
        and submit_attempted is False
    )
    return {
        "broker_flat_confirmed": broker_flat_confirmed,
        "preflight_classification": preflight.get("classification"),
        "recovery_classification": recovery.get("classification") if recovery else None,
        "account_id": account_value,
        "contract_key": contract_key_value,
        "local_symbol": local_symbol_value,
        "con_id": con_id_value,
        "signed_quantity": _decimal_text(signed_quantity),
        "open_orders_none": open_orders_none,
        "open_order_count": open_order_count,
        "contract_matches_lifecycle": contract_matches,
        "submit_attempted": submit_attempted,
        "proof_position_flat_check_passed": check_map.get("proof_position_flat"),
        "proof_open_orders_clean_check_passed": check_map.get("proof_open_orders_clean"),
    }


def _prior_artifact_state(target: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if target is None:
        return None
    return {
        "trade_id": target.get("trade_id"),
        "lifecycle_id": target.get("lifecycle_id"),
        "paper_lifecycle_classification": target.get("paper_lifecycle_classification"),
        "paper_proof_classification": target.get("paper_proof_classification"),
        "final_broker_state_classification": target.get("final_broker_state_classification"),
        "final_position_status": target.get("final_position_status"),
        "review_required": target.get("review_required"),
        "broker_reconciled": target.get("broker_reconciled"),
        "entry_fill_price": target.get("entry_fill_price"),
        "exit_fill_price": target.get("exit_fill_price"),
        "quantity": target.get("quantity"),
    }


def _reconciliation_blocker(
    target: Mapping[str, Any] | None,
    proof_check: Mapping[str, Any],
    broker_check: Mapping[str, Any],
) -> str:
    if target is None:
        return "LIFECYCLE_NOT_FOUND_IN_LEDGER"
    if proof_check.get("is_proof_canary") is not True:
        return "LIFECYCLE_IS_NOT_PROOF_CANARY"
    if broker_check.get("broker_flat_confirmed") is not True:
        return "BROKER_FLAT_CONFIRMATION_FAILED"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _manual_flat_reconciliation_record(
    *,
    target: Mapping[str, Any],
    broker_check: Mapping[str, Any],
    preflight_report_json: Path,
    recovery_report_json: Path | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "ARTIFACT_RECONCILIATION",
        "reconciliation_schema_version": RECONCILIATION_SCHEMA_VERSION,
        "trade_id": f"{target.get('trade_id')}:manual_flat_review",
        "lifecycle_id": target.get("lifecycle_id"),
        "strategy_id": target.get("strategy_id"),
        "instrument_family": target.get("instrument_family"),
        "contract_key": target.get("contract_key"),
        "local_symbol": target.get("local_symbol"),
        "con_id": target.get("con_id"),
        "account_id": target.get("account_id"),
        "prior_artifact_classification": target.get("paper_lifecycle_classification"),
        "prior_review_required": target.get("review_required"),
        "reconciliation_action": MANUALLY_FLATTENED_REVIEWED,
        "new_artifact_classification": MANUALLY_FLATTENED_REVIEWED,
        "final_position_status": MANUALLY_FLATTENED_REVIEWED,
        "review_required": False,
        "broker_reconciled": False,
        "broker_flat_confirmed": True,
        "broker_signed_quantity": broker_check.get("signed_quantity"),
        "broker_open_order_count": broker_check.get("open_order_count"),
        "source": "READ_ONLY_PREFLIGHT_RECOVERY_REPORTS",
        "preflight_report_path": str(preflight_report_json),
        "recovery_report_path": str(recovery_report_json) if recovery_report_json else None,
        "broker_mutation_attempted": False,
        "submit_attempted": False,
        "paper_proof_cli_invoked": False,
        "created_at": now.isoformat(),
    }


def _apply_manual_flat_reconciliations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reconciled_lifecycle_ids = {
        str(item.get("lifecycle_id"))
        for item in records
        if _is_reconciliation_record(item) and item.get("new_artifact_classification") == MANUALLY_FLATTENED_REVIEWED
    }
    if not reconciled_lifecycle_ids:
        return records
    normalized: list[dict[str, Any]] = []
    for item in records:
        row = dict(item)
        if (
            not _is_reconciliation_record(row)
            and str(row.get("lifecycle_id") or "") in reconciled_lifecycle_ids
            and _is_proof_canary_record(row)
        ):
            row["artifact_reconciliation_classification"] = MANUALLY_FLATTENED_REVIEWED
            row["manual_flat_reviewed"] = True
            row["review_required"] = False
            row["prior_paper_lifecycle_classification"] = row.get("paper_lifecycle_classification")
            row["paper_lifecycle_classification"] = MANUALLY_FLATTENED_REVIEWED
            row["final_position_status"] = MANUALLY_FLATTENED_REVIEWED
            row["final_broker_state_classification"] = MANUALLY_FLATTENED_REVIEWED
        normalized.append(row)
    return normalized


def _read_ledger_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _load_json_path(raw_path: object) -> dict[str, Any]:
    if not raw_path:
        return {}
    try:
        value = json.loads(Path(str(raw_path)).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _nested_get(payload: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _instrument_family(contract_key: str, local_symbol: object) -> str:
    if contract_key:
        return contract_key.split("-", 1)[0].upper()
    raw = str(local_symbol or "").upper()
    for prefix in ("MGC", "MNQ", "MES", "GC", "NQ", "ES"):
        if raw.startswith(prefix):
            return prefix
    return "UNKNOWN"


def _side_from_runner(runner_report: Mapping[str, Any], open_intent: Mapping[str, Any]) -> str | None:
    raw_side = str(runner_report.get("signal_direction") or runner_report.get("signal_side") or runner_report.get("side") or "").upper()
    if raw_side in {"LONG", "SHORT"}:
        return raw_side
    action = str(open_intent.get("action") or runner_report.get("order_action") or "").upper()
    if action == "BUY":
        return "LONG"
    if action == "SELL":
        return "SHORT"
    return None


def _realized_pnl(
    *,
    side: str | None,
    quantity: Decimal | None,
    entry: Decimal | None,
    exit: Decimal | None,
    instrument_family: str,
) -> Decimal | None:
    points = _points_pnl(side=side, entry=entry, exit=exit)
    if points is None or quantity is None:
        return None
    point_value = POINT_VALUE_BY_FAMILY.get(instrument_family)
    if point_value is None:
        return None
    return points * quantity * point_value


def _points_pnl(*, side: str | None, entry: Decimal | None, exit: Decimal | None) -> Decimal | None:
    if side not in {"LONG", "SHORT"} or entry is None or exit is None:
        return None
    return exit - entry if side == "LONG" else entry - exit


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _string_or_none(value: object) -> str | None:
    return None if value in {None, ""} else str(value)


def _review_required(
    runner_report: Mapping[str, Any],
    proof_report: Mapping[str, Any],
    proof_payload: Mapping[str, Any],
) -> bool:
    classification = str(runner_report.get("paper_proof_classification") or proof_report.get("classification") or "")
    lifecycle_status = str(runner_report.get("paper_proof_lifecycle_status") or proof_payload.get("proof_lifecycle_status") or "")
    final_position_status = str(runner_report.get("final_position_status") or _nested_get(proof_payload, ("final_reconciliation", "status")) or "")
    if classification != "TRACK_B_PAPER_PROOF_PASSED":
        return True
    if lifecycle_status != "PROOF_COMPLETE_FLAT":
        return True
    return final_position_status not in {"CLEAN", "PROOF_COMPLETE_FLAT"}


def _is_flat_closed_trade(item: Mapping[str, Any]) -> bool:
    if _is_manual_flat_reviewed(item):
        return False
    if item.get("paper_lifecycle_type") == "STRATEGY_MANAGED":
        return (
            item.get("paper_lifecycle_classification") == "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"
            and item.get("final_position_status") == "CLOSED_FLAT"
        )
    return (
        item.get("paper_proof_classification") == "TRACK_B_PAPER_PROOF_PASSED"
        and item.get("paper_lifecycle_classification") == "PROOF_COMPLETE_FLAT"
        and item.get("final_position_status") in {"CLEAN", "PROOF_COMPLETE_FLAT"}
    )


def _is_open_position_record(item: Mapping[str, Any]) -> bool:
    return not _is_reconciliation_record(item) and not _is_flat_closed_trade(item) and not _is_manual_flat_reviewed(item)


def _is_reconciliation_record(item: Mapping[str, Any]) -> bool:
    return item.get("record_type") == "ARTIFACT_RECONCILIATION"


def _is_proof_canary_record(item: Mapping[str, Any]) -> bool:
    if item.get("paper_lifecycle_type") == "STRATEGY_MANAGED":
        return False
    lifecycle_id = str(item.get("lifecycle_id") or "")
    return lifecycle_id.startswith("paper_proof_") or item.get("paper_proof_classification") is not None


def _is_manual_flat_reviewed(item: Mapping[str, Any]) -> bool:
    return (
        item.get("artifact_reconciliation_classification") == MANUALLY_FLATTENED_REVIEWED
        or item.get("new_artifact_classification") == MANUALLY_FLATTENED_REVIEWED
        or item.get("manual_flat_reviewed") is True
    )


def _date_prefix(value: object) -> str | None:
    if not value:
        return None
    return str(value)[:10]


def _sum_decimal(records: Iterable[Mapping[str, Any]], key: str) -> str:
    total = Decimal("0")
    for item in records:
        total += _decimal(item.get(key)) or Decimal("0")
    return _decimal_text(total) or "0"


def _average_decimal(records: list[Mapping[str, Any]], key: str) -> str | None:
    if not records:
        return None
    total = Decimal(_sum_decimal(records, key))
    return _decimal_text(total / Decimal(len(records)))


def _pnl_groups(records: Iterable[Mapping[str, Any]], key: str, now: datetime) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in records:
        grouped[str(item.get(key) or "UNKNOWN")].append(item)
    today = now.date().isoformat()
    week_start = (now.date() - timedelta(days=now.weekday())).isoformat()
    ytd_start = now.date().replace(month=1, day=1).isoformat()
    return {
        name: {
            "trade_count": len(items),
            "trades": len(items),
            "open_position_count": sum(1 for item in items if _is_open_position_record(item)),
            "realized_pnl": _sum_decimal(items, "realized_pnl"),
            "realized_pnl_today": _sum_decimal(_records_since(items, today, exact_date=True), "realized_pnl"),
            "realized_pnl_week": _sum_decimal(_records_since(items, week_start), "realized_pnl"),
            "realized_pnl_ytd": _sum_decimal(_records_since(items, ytd_start), "realized_pnl"),
            "unrealized_pnl": "0",
            "instrument_family": _first(list(items), "instrument_family"),
            "instrument": _first(list(items), "instrument_family") or _first(list(items), "contract_key"),
            "contract_key": _first(list(items), "contract_key"),
            "last_trade_time": max(
                (str(item.get("exit_timestamp") or item.get("entry_timestamp") or item.get("created_at") or "") for item in items),
                default=None,
            ),
            "review_required_count": sum(1 for item in items if item.get("review_required") is True and not _is_manual_flat_reviewed(item)),
        }
        for name, items in sorted(grouped.items())
    }


def _records_since(records: Iterable[Mapping[str, Any]], date_key: str, *, exact_date: bool = False) -> list[Mapping[str, Any]]:
    if exact_date:
        return [item for item in records if _date_prefix(item.get("exit_timestamp") or item.get("created_at")) == date_key]
    return [
        item
        for item in records
        if str(item.get("exit_timestamp") or item.get("created_at") or "")[:10] >= date_key
    ]


def _compact_trade_row(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": item.get("trade_id"),
        "lifecycle_id": item.get("lifecycle_id"),
        "signal_id": item.get("signal_id"),
        "strategy_id": item.get("strategy_id"),
        "instrument_family": item.get("instrument_family"),
        "contract_key": item.get("contract_key"),
        "local_symbol": item.get("local_symbol"),
        "con_id": item.get("con_id"),
        "account_id": item.get("account_id"),
        "time": item.get("exit_timestamp") or item.get("entry_timestamp") or item.get("created_at"),
        "entry_timestamp": item.get("entry_timestamp"),
        "exit_timestamp": item.get("exit_timestamp"),
        "side": item.get("side"),
        "order_action": item.get("order_action"),
        "quantity": item.get("quantity"),
        "entry_price": item.get("entry_fill_price"),
        "exit_price": item.get("exit_fill_price"),
        "entry_fill_price": item.get("entry_fill_price"),
        "exit_fill_price": item.get("exit_fill_price"),
        "realized_pnl": item.get("realized_pnl"),
        "paper_lifecycle_classification": item.get("paper_lifecycle_classification"),
        "final_broker_state_classification": item.get("final_broker_state_classification"),
        "final_position_status": item.get("final_position_status"),
        "artifact_reconciliation_classification": item.get("artifact_reconciliation_classification"),
        "paper_lifecycle_type": item.get("paper_lifecycle_type"),
        "broker_reconciled": item.get("broker_reconciled"),
        "review_required": item.get("review_required"),
        "paper_lifecycle_report_path": item.get("paper_lifecycle_report_path"),
    }


def _positions_by(records: Iterable[Mapping[str, Any]], key: str, now: datetime) -> dict[str, dict[str, Any]]:
    positions: dict[str, dict[str, Any]] = {}
    for item in records:
        if not _is_open_position_record(item):
            continue
        name = str(item.get(key) or "UNKNOWN")
        quantity = _decimal(item.get("quantity")) or Decimal("0")
        positions[name] = {
            "as_of": now.isoformat(),
            "strategy_id": item.get("strategy_id"),
            "lifecycle_id": item.get("lifecycle_id"),
            "instrument_family": item.get("instrument_family"),
            "contract_key": item.get("contract_key"),
            "local_symbol": item.get("local_symbol"),
            "quantity": _decimal_text(quantity),
            "avg_entry_price": item.get("entry_fill_price"),
            "latest_mark_price": None,
            "unrealized_pnl": None,
            "realized_pnl_today": "0",
            "open_order_count": 0,
            "review_required": item.get("review_required") is True,
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        }
    return positions


def _source_paths(records: Iterable[Mapping[str, Any]]) -> list[str]:
    paths: list[str] = []
    for item in records:
        for key in ("paper_lifecycle_report_path", "strategy_paper_runner_report_path", "monitor_report_path"):
            value = item.get(key)
            if value and str(value) not in paths:
                paths.append(str(value))
    return paths[-20:]


def _first(records: list[Mapping[str, Any]], key: str) -> Any:
    for item in records:
        if item.get(key) not in {None, ""}:
            return item.get(key)
    return None
