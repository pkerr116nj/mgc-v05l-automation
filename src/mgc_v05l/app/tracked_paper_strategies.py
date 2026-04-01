"""Tracked paper strategy registry and read models for app-facing surfaces."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Sequence

from .execution_truth import (
    AUTHORITATIVE_INTRABAR_ENTRY_ONLY,
    BASELINE_NEXT_BAR_OPEN,
    CURRENT_CANDLE_VWAP,
    FULL_AUTHORITATIVE_LIFECYCLE,
    PAPER_RUNTIME_LEDGER,
    normalize_trade_lifecycle_records,
)
from .session_phase_labels import label_session_phase


TRACKED_ATP_STRATEGY_ID = "atp_companion_v1_asia_us"
TRACKED_ATP_INTERNAL_LABEL = "ATP_COMPANION_V1_ASIA_US"
TRACKED_ATP_DISPLAY_NAME = "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only"
TRACKED_ATP_BENCHMARK_DESIGNATION = "CURRENT_ATP_COMPANION_BENCHMARK"
TRACKED_ATP_STRATEGY_FAMILY = "active_trend_participation_engine"
TRACKED_ATP_RUNTIME_KIND = "atp_companion_benchmark_paper"


@dataclass(frozen=True)
class TrackedPaperStrategyDefinition:
    strategy_id: str
    display_name: str
    internal_label: str
    environment: str
    benchmark_designation: str
    config_source: str
    benchmark_note_path: str
    description: str
    allowed_sessions: tuple[str, ...]
    diagnostic_only_sessions: tuple[str, ...]
    runtime_kinds: tuple[str, ...]
    source_families: tuple[str, ...]
    lane_id_prefixes: tuple[str, ...]


def build_tracked_paper_strategy_definitions(repo_root: Path) -> tuple[TrackedPaperStrategyDefinition, ...]:
    return (
        TrackedPaperStrategyDefinition(
            strategy_id=TRACKED_ATP_STRATEGY_ID,
            display_name=TRACKED_ATP_DISPLAY_NAME,
            internal_label=TRACKED_ATP_INTERNAL_LABEL,
            environment="paper",
            benchmark_designation=TRACKED_ATP_BENCHMARK_DESIGNATION,
            config_source=str((repo_root / "config" / "probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml").resolve()),
            benchmark_note_path=str((repo_root / "docs" / "specs" / "ATP_COMPANION_BASELINE_V1_BENCHMARK.md").resolve()),
            description=(
                "Productized tracked paper strategy view for the frozen ATP companion benchmark. "
                "ATP execution is limited to ASIA + US while London remains diagnostic-only."
            ),
            allowed_sessions=("ASIA", "US"),
            diagnostic_only_sessions=("LONDON",),
            runtime_kinds=(TRACKED_ATP_RUNTIME_KIND,),
            source_families=(),
            lane_id_prefixes=(TRACKED_ATP_STRATEGY_ID,),
        ),
    )


def build_tracked_paper_strategies_payload(
    *,
    repo_root: Path,
    paper: dict[str, Any],
    generated_at: str | None,
) -> dict[str, Any]:
    definitions = build_tracked_paper_strategy_definitions(repo_root)
    temporary_rows = [
        dict(row)
        for row in list(
            (paper.get("temporary_paper_strategies") or {}).get("rows")
            or (paper.get("non_approved_lanes") or {}).get("rows")
            or []
        )
    ]
    trade_log_rows = [dict(row) for row in list((paper.get("strategy_performance") or {}).get("trade_log") or [])]
    rows: list[dict[str, Any]] = []
    details_by_id: dict[str, dict[str, Any]] = {}
    for definition in definitions:
        matched_rows = [row for row in temporary_rows if _matches_definition(row, definition)]
        if not matched_rows:
            matched_rows = _fallback_rows_for_definition(
                repo_root=repo_root,
                definition=definition,
                generated_at=generated_at,
            )
        detail = _build_tracked_strategy_detail(
            repo_root=repo_root,
            paper=paper,
            definition=definition,
            matched_rows=matched_rows,
            trade_log_rows=trade_log_rows,
            generated_at=generated_at,
        )
        rows.append(detail["summary"])
        details_by_id[definition.strategy_id] = detail["detail"]
    enabled_count = sum(1 for row in rows if row.get("enabled"))
    active_count = sum(1 for row in rows if str(row.get("status") or "").upper() in {"READY", "IN_POSITION", "RECONCILING"})
    return {
        "generated_at": generated_at,
        "scope_label": "Tracked paper strategy audit (secondary read model)",
        "total_count": len(rows),
        "enabled_count": enabled_count,
        "active_count": active_count,
        "rows": rows,
        "details_by_strategy_id": details_by_id,
        "default_strategy_id": definitions[0].strategy_id if definitions else None,
        "note": (
            "Tracked paper strategies are secondary audit read models backed by persisted paper/runtime truth. "
            "Use the shared lane operator detail and shared paper-runtime controls as the primary ATP operator surface."
        ),
    }


def _build_tracked_strategy_detail(
    *,
    repo_root: Path,
    paper: dict[str, Any],
    definition: TrackedPaperStrategyDefinition,
    matched_rows: Sequence[dict[str, Any]],
    trade_log_rows: Sequence[dict[str, Any]],
    generated_at: str | None,
) -> dict[str, Any]:
    lane_ids = {str(row.get("lane_id") or "").strip() for row in matched_rows if row.get("lane_id")}
    strategy_trades = [
        dict(row)
        for row in trade_log_rows
        if str(row.get("lane_id") or "").strip() in lane_ids
        or (
            not lane_ids
            and _string_starts_with_any(str(row.get("lane_id") or ""), definition.lane_id_prefixes)
        )
    ]
    db_paths = [
        _resolve_sqlite_database_path(row.get("database_url"))
        for row in matched_rows
        if row.get("database_url")
    ]
    db_paths = [path for path in db_paths if path is not None]
    recent_bars = _collect_recent_jsonl_rows(matched_rows, artifact_key="processed_bars", timestamp_fields=("end_ts", "timestamp"), limit=12)
    recent_signals = _collect_recent_jsonl_rows(
        matched_rows,
        artifact_key="signals",
        timestamp_fields=("signal_timestamp", "created_at", "timestamp", "entry_timestamp_planned"),
        limit=12,
    )
    recent_event_rows = _collect_recent_jsonl_rows(
        matched_rows,
        artifact_key="events",
        timestamp_fields=("timestamp", "logged_at", "generated_at", "updated_at"),
        limit=12,
    )
    recent_trade_rows = _collect_recent_jsonl_rows(
        matched_rows,
        artifact_key="trades",
        timestamp_fields=("exit_timestamp", "entry_timestamp", "signal_timestamp"),
        limit=12,
    )
    recent_intents = _collect_recent_intent_rows(matched_rows, db_paths, lane_ids, limit=12)
    recent_fills = _collect_recent_fill_rows(matched_rows, db_paths, lane_ids, limit=12)
    recent_snapshots = _collect_recent_state_snapshots(db_paths, lane_ids, limit=8)
    recent_faults = _collect_recent_table_rows(
        db_paths,
        table_name="fault_events",
        order_column="occurred_at",
        lane_ids=lane_ids,
        limit=8,
    )
    recent_reconciliation = _collect_recent_reconciliation_rows(matched_rows, db_paths, lane_ids, limit=8)
    latest_signal = recent_signals[0] if recent_signals else {}
    latest_intent = recent_intents[0] if recent_intents else {}
    latest_fill = recent_fills[0] if recent_fills else {}
    latest_trade = _latest_trade(strategy_trades, recent_trade_rows)
    latest_snapshot = recent_snapshots[0] if recent_snapshots else {}
    latest_operator_status = _latest_operator_status(matched_rows)
    latest_runtime_state = _latest_runtime_state(matched_rows)
    latest_processed_bar_timestamp = _latest_processed_bar_timestamp(matched_rows, recent_bars)
    runtime_instance_present = all(bool(row.get("runtime_instance_present")) for row in matched_rows) if matched_rows else False
    runtime_state_loaded = all(bool(row.get("runtime_state_loaded")) for row in matched_rows) if matched_rows else False
    generated_at_dt = _parse_iso_datetime(generated_at)
    runtime_heartbeat_at = _string_or_none(latest_operator_status.get("runtime_heartbeat_at")) or _string_or_none(
        latest_operator_status.get("updated_at")
    )
    runtime_heartbeat_dt = _parse_iso_datetime(runtime_heartbeat_at)
    runtime_heartbeat_age_seconds = (
        max((generated_at_dt - runtime_heartbeat_dt).total_seconds(), 0.0)
        if generated_at_dt is not None and runtime_heartbeat_dt is not None
        else None
    )
    runtime_attached = bool(matched_rows) and bool(
        paper.get("running")
        or runtime_instance_present
        or latest_operator_status.get("runtime_attached")
    )
    data_stale = bool(
        latest_operator_status.get("data_stale")
        or (runtime_attached and runtime_heartbeat_age_seconds is not None and runtime_heartbeat_age_seconds > 180)
    )
    current_session = _current_session_segment(
        paper_status=paper.get("status") or {},
        latest_processed_bar_timestamp=latest_processed_bar_timestamp,
    )
    session_allowed = current_session in definition.allowed_sessions if current_session else False
    row_entries_enabled = any(bool(row.get("entries_enabled", row.get("state") == "ENABLED")) for row in matched_rows) if matched_rows else False
    operator_entries_enabled = latest_operator_status.get("entries_enabled")
    entries_enabled = bool(operator_entries_enabled) if operator_entries_enabled is not None else row_entries_enabled
    operator_halt = any(bool(_nested_get(row, "operator_status_payload", "operator_halt")) for row in matched_rows)
    warmup_complete = _merge_boolean(
        [_nested_get(row, "operator_status_payload", "warmup_complete") for row in matched_rows]
    )
    position_side, current_quantity, average_price = _current_position_state(
        matched_rows=matched_rows,
        latest_snapshot=latest_snapshot,
        latest_trade=latest_trade,
        latest_fill=latest_fill,
    )
    open_pnl = _open_pnl_from_latest_mark(
        quantity=current_quantity,
        average_price=average_price,
        latest_bar_close=_decimal_or_none(_value_from_rows(recent_bars, "close")),
        position_side=position_side,
    )
    latest_mark_price = _decimal_or_none(_value_from_rows(recent_bars, "close"))
    open_pnl_supported = position_side == "FLAT" or (
        current_quantity not in (None, 0) and average_price is not None and latest_mark_price is not None
    )
    bars_in_trade = _bars_in_trade(
        latest_trade=latest_trade,
        latest_fill=latest_fill,
        latest_processed_bar_timestamp=latest_processed_bar_timestamp,
        position_side=position_side,
    )
    performance = _performance_summary(
        strategy_trades=strategy_trades,
        session_date=str((paper.get("status") or {}).get("session_date") or ""),
    )
    latest_exit_reason = (
        _string_or_none(latest_trade.get("exit_reason"))
        or _string_or_none(_nested_get(latest_fill, "payload", "exit_reason"))
        or _string_or_none(_nested_get(latest_operator_status, "latest_exit_reason"))
    )
    status, status_reason = _tracked_strategy_status(
        matched_rows=matched_rows,
        entries_enabled=entries_enabled,
        position_side=position_side,
        recent_faults=recent_faults,
        recent_reconciliation=recent_reconciliation,
        paper=paper,
        runtime_attached=runtime_attached,
        data_stale=data_stale,
    )
    health_flags = {
        "fault_count": len(recent_faults),
        "reconciliation_issue_count": sum(1 for row in recent_reconciliation if row.get("clean") is False),
        "runtime_instance_present": runtime_instance_present,
        "runtime_state_loaded": runtime_state_loaded,
        "runtime_attached": runtime_attached,
        "runtime_heartbeat_at": runtime_heartbeat_at,
        "runtime_heartbeat_age_seconds": runtime_heartbeat_age_seconds,
        "data_stale": data_stale,
        "duplicate_bar_suppression_count": _nested_get(latest_operator_status, "duplicate_bar_suppression_count"),
        "operator_halt": operator_halt,
        "warmup_complete": warmup_complete,
    }
    latest_signal_summary = _signal_summary(latest_signal, matched_rows)
    latest_stop_risk_context = _latest_stop_risk_context(latest_snapshot, latest_trade, matched_rows)
    config_identity = {
        "config_source": definition.config_source,
        "benchmark_note_path": definition.benchmark_note_path,
        "benchmark_overlay_config": str((repo_root / "config" / "atp_companion_baseline_v1_asia_us.yaml").resolve()),
        "allowed_sessions": list(definition.allowed_sessions),
        "diagnostic_only_sessions": list(definition.diagnostic_only_sessions),
    }
    lifecycle_contract = _tracked_paper_lifecycle_contract(
        definition=definition,
        latest_operator_status=latest_operator_status,
        latest_runtime_state=latest_runtime_state,
        recent_order_intents=recent_intents,
        recent_fills=recent_fills,
        recent_trades=sorted(
            [dict(row) for row in (strategy_trades or recent_trade_rows)],
            key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""),
            reverse=True,
        )[:12],
        recent_state_snapshots=recent_snapshots,
    )
    summary_row = {
        "strategy_id": definition.strategy_id,
        "display_name": definition.display_name,
        "internal_label": definition.internal_label,
        "environment": definition.environment,
        "status": status,
        "status_reason": status_reason,
        "enabled": entries_enabled,
        "benchmark_designation": definition.benchmark_designation,
        "config_source": definition.config_source,
        "benchmark": True,
        "entries_enabled": entries_enabled,
        "operator_halt": operator_halt,
        "warmup_complete": warmup_complete,
        "current_session_segment": current_session,
        "session_allowed": session_allowed,
        "latest_processed_bar_timestamp": latest_processed_bar_timestamp,
        "runtime_attached": runtime_attached,
        "runtime_heartbeat_at": runtime_heartbeat_at,
        "runtime_heartbeat_age_seconds": runtime_heartbeat_age_seconds,
        "data_stale": data_stale,
        "latest_signal_summary": latest_signal_summary,
        "current_position_side": position_side,
        "current_quantity": current_quantity,
        "current_entry_family": _current_entry_family(latest_snapshot, latest_trade, latest_intent),
        "bars_in_trade": bars_in_trade,
        "latest_stop_risk_context": latest_stop_risk_context,
        "latest_order_intent": latest_intent or None,
        "latest_fill": latest_fill or None,
        "latest_exit_reason": latest_exit_reason,
        "health_flags": health_flags,
        "realized_pnl": performance["realized_pnl"],
        "open_pnl": _decimal_to_string(open_pnl),
        "open_pnl_supported": open_pnl_supported,
        "open_pnl_unavailable_reason": (
            None
            if open_pnl_supported
            else "Tracked paper strategy does not currently have a trusted latest mark/reference price for the open position."
        ),
        "open_pnl_truth_source": (
            "latest_processed_bar_close"
            if open_pnl_supported and position_side != "FLAT" and latest_mark_price is not None
            else "flat_position"
            if position_side == "FLAT"
            else None
        ),
        "average_trade_pnl": performance["average_trade_pnl"],
        "win_rate": performance["win_rate"],
        "profit_factor": performance["profit_factor"],
        "max_drawdown": performance["max_drawdown"],
        "trade_count": performance["trade_count"],
        "long_trade_count": performance["long_trade_count"],
        "short_trade_count": performance["short_trade_count"],
        "winner_count": performance["winner_count"],
        "loser_count": performance["loser_count"],
        "current_day_pnl": performance["current_day_pnl"],
        "cumulative_pnl": performance["realized_pnl"],
        "last_trade_summary": performance["last_trade_summary"],
        "session_breakdown": performance["session_breakdown"],
        "trade_family_breakdown": performance["trade_family_breakdown"],
        "trade_history_scope": performance["trade_history_scope"],
        "last_update_timestamp": _latest_timestamp_value(
            (
                latest_processed_bar_timestamp,
                _timestamp_from_row(latest_signal, "signal_timestamp", "created_at", "timestamp", "entry_timestamp_planned"),
                _timestamp_from_row(latest_intent, "created_at"),
                _timestamp_from_row(latest_fill, "fill_timestamp"),
                _timestamp_from_row(latest_trade, "exit_timestamp", "entry_timestamp"),
                _string_or_none(_nested_get(latest_operator_status, "updated_at")),
            )
        ),
        "observed_instruments": sorted({str(row.get("instrument") or "").strip() for row in matched_rows if row.get("instrument")}),
        "lane_count": len(matched_rows),
        "runtime_controls": {
            "start_action": "start-atp-companion-paper",
            "stop_action": "stop-atp-companion-paper",
            "halt_entries_action": "atp-companion-paper-halt-entries",
            "resume_entries_action": "atp-companion-paper-resume-entries",
            "flatten_and_halt_action": "atp-companion-paper-flatten-and-halt",
            "stop_after_cycle_action": "atp-companion-paper-stop-after-cycle",
        },
        **lifecycle_contract,
    }
    detail = {
        **summary_row,
        "description": definition.description,
        "config_identity": config_identity,
        "constituent_lanes": [
            {
                "lane_id": row.get("lane_id"),
                "display_name": row.get("display_name"),
                "instrument": row.get("instrument"),
                "runtime_kind": row.get("runtime_kind"),
                "enabled": row.get("entries_enabled", row.get("state") == "ENABLED"),
                "position_side": row.get("position_side"),
                "quality_bucket_policy": row.get("quality_bucket_policy"),
                "latest_activity_timestamp": row.get("latest_activity_timestamp") or row.get("last_update_timestamp"),
            }
            for row in matched_rows
        ],
        "recent_bars": recent_bars,
        "recent_signals": recent_signals,
        "recent_events": recent_event_rows,
        "recent_order_intents": recent_intents,
        "recent_fills": recent_fills,
        "recent_state_snapshots": recent_snapshots,
        "recent_faults": recent_faults,
        "recent_reconciliation_events": recent_reconciliation,
        "recent_trades": lifecycle_contract["recent_trade_lifecycle_preview"],
        "artifacts": {
            "benchmark_config": definition.config_source,
            "benchmark_note": definition.benchmark_note_path,
            "lane_artifacts": [
                {
                    "lane_id": row.get("lane_id"),
                    "artifacts": dict(row.get("artifacts") or {}),
                }
                for row in matched_rows
            ],
        },
    }
    detail.pop("recent_trade_lifecycle_preview", None)
    return {"summary": summary_row, "detail": detail}


def _matches_definition(row: dict[str, Any], definition: TrackedPaperStrategyDefinition) -> bool:
    if not bool(row.get("temporary_paper_strategy")):
        return False
    lane_id = str(row.get("lane_id") or "")
    runtime_kind = str(row.get("runtime_kind") or "")
    source_family = str(row.get("strategy_family") or row.get("source_family") or "")
    if runtime_kind and runtime_kind in definition.runtime_kinds:
        return True
    if source_family and source_family in definition.source_families:
        return True
    return _string_starts_with_any(lane_id, definition.lane_id_prefixes)


def _fallback_rows_for_definition(
    *,
    repo_root: Path,
    definition: TrackedPaperStrategyDefinition,
    generated_at: str | None,
) -> list[dict[str, Any]]:
    if definition.strategy_id != TRACKED_ATP_STRATEGY_ID:
        return []
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / TRACKED_ATP_STRATEGY_ID
    operator_status_path = lane_dir / "operator_status.json"
    runtime_state_path = lane_dir / "runtime_state.json"
    processed_bars_path = lane_dir / "processed_bars.jsonl"
    if not operator_status_path.exists() and not runtime_state_path.exists() and not processed_bars_path.exists():
        return []
    operator_status = _load_json_path(operator_status_path)
    generated_at_dt = _parse_iso_datetime(generated_at)
    heartbeat_dt = _parse_iso_datetime(_string_or_none(operator_status.get("runtime_heartbeat_at")))
    runtime_attached = bool(operator_status.get("runtime_attached"))
    if not runtime_attached and generated_at_dt is not None and heartbeat_dt is not None:
        runtime_attached = max((generated_at_dt - heartbeat_dt).total_seconds(), 0.0) <= 180
    db_path = repo_root / "mgc_v05l.probationary.paper__atp_companion_v1_asia_us.sqlite3"
    return [
        {
            "lane_id": TRACKED_ATP_STRATEGY_ID,
            "display_name": definition.display_name,
            "instrument": "MGC",
            "runtime_kind": TRACKED_ATP_RUNTIME_KIND,
            "strategy_family": TRACKED_ATP_STRATEGY_FAMILY,
            "temporary_paper_strategy": True,
            "entries_enabled": operator_status.get("entries_enabled", False),
            "state": "ENABLED" if operator_status.get("entries_enabled", False) else "DISABLED",
            "runtime_instance_present": runtime_attached,
            "runtime_state_loaded": runtime_state_path.exists(),
            "database_url": f"sqlite:///{db_path}" if db_path.exists() else None,
            "operator_status_payload": operator_status,
            "artifacts": {
                "lane_dir": str(lane_dir.resolve()),
                "processed_bars": str(processed_bars_path.resolve()) if processed_bars_path.exists() else None,
                "signals": str((lane_dir / "signals.jsonl").resolve()) if (lane_dir / "signals.jsonl").exists() else None,
                "order_intents": str((lane_dir / "order_intents.jsonl").resolve()) if (lane_dir / "order_intents.jsonl").exists() else None,
                "fills": str((lane_dir / "fills.jsonl").resolve()) if (lane_dir / "fills.jsonl").exists() else None,
                "trades": str((lane_dir / "trades.jsonl").resolve()) if (lane_dir / "trades.jsonl").exists() else None,
                "events": str((lane_dir / "events.jsonl").resolve()) if (lane_dir / "events.jsonl").exists() else None,
                "reconciliation": str((lane_dir / "reconciliation_events.jsonl").resolve()) if (lane_dir / "reconciliation_events.jsonl").exists() else None,
                "operator_status": str(operator_status_path.resolve()) if operator_status_path.exists() else None,
                "runtime_state": str(runtime_state_path.resolve()) if runtime_state_path.exists() else None,
            },
        }
    ]


def _string_starts_with_any(value: str, prefixes: Sequence[str]) -> bool:
    normalized = str(value or "")
    return any(normalized.startswith(prefix) for prefix in prefixes)


def _collect_recent_jsonl_rows(
    matched_rows: Sequence[dict[str, Any]],
    *,
    artifact_key: str,
    timestamp_fields: Sequence[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in matched_rows:
        path_value = _nested_get(row, "artifacts", artifact_key)
        if not path_value:
            continue
        rows.extend(_all_jsonl_rows(Path(str(path_value))))
    rows.sort(key=lambda item: _row_sort_key(item, timestamp_fields), reverse=True)
    return rows[:limit]


def _collect_recent_intent_rows(
    matched_rows: Sequence[dict[str, Any]],
    db_paths: Sequence[Path],
    lane_ids: set[str],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = _collect_recent_jsonl_rows(matched_rows, artifact_key="order_intents", timestamp_fields=("created_at", "intent_timestamp", "timestamp"), limit=limit)
    if rows:
        return rows
    rows = _collect_recent_table_rows(db_paths, table_name="order_intents", order_column="created_at", lane_ids=lane_ids, limit=limit)
    return rows


def _collect_recent_fill_rows(
    matched_rows: Sequence[dict[str, Any]],
    db_paths: Sequence[Path],
    lane_ids: set[str],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = _collect_recent_jsonl_rows(matched_rows, artifact_key="fills", timestamp_fields=("fill_timestamp", "timestamp"), limit=limit)
    if rows:
        return rows
    rows = _collect_recent_table_rows(db_paths, table_name="fills", order_column="fill_timestamp", lane_ids=lane_ids, limit=limit)
    return rows


def _collect_recent_state_snapshots(
    db_paths: Sequence[Path],
    lane_ids: set[str],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = _collect_recent_table_rows(
        db_paths,
        table_name="strategy_state_snapshots",
        order_column="updated_at",
        lane_ids=lane_ids,
        limit=limit,
    )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(str(row.get("payload_json") or "")) if row.get("payload_json") else {}
        normalized.append(
            {
                **row,
                "payload": payload,
                "position_quantity": payload.get("position_quantity"),
                "position_average_price": payload.get("position_average_price"),
                "latest_order_intent": payload.get("latest_order_intent"),
                "latest_fill": payload.get("latest_fill"),
                "stop_context": payload.get("stop_context") or payload.get("latest_stop_context"),
            }
        )
    return normalized


def _collect_recent_reconciliation_rows(
    matched_rows: Sequence[dict[str, Any]],
    db_paths: Sequence[Path],
    lane_ids: set[str],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    json_rows = _collect_recent_jsonl_rows(
        matched_rows,
        artifact_key="reconciliation",
        timestamp_fields=("logged_at", "timestamp", "updated_at", "fill_timestamp"),
        limit=limit,
    )
    if json_rows:
        return json_rows
    return _collect_recent_table_rows(
        db_paths,
        table_name="reconciliation_events",
        order_column="occurred_at",
        lane_ids=lane_ids,
        limit=limit,
    )


def _collect_recent_table_rows(
    db_paths: Sequence[Path],
    *,
    table_name: str,
    order_column: str,
    lane_ids: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for db_path in db_paths:
        rows.extend(_all_table_rows_safe(db_path, table_name, order_column))
    if lane_ids:
        rows = [
            row
            for row in rows
            if str(row.get("lane_id") or "").strip() in lane_ids
            or str(row.get("standalone_strategy_id") or "").strip() in lane_ids
        ]
    rows.sort(key=lambda row: str(row.get(order_column) or ""), reverse=True)
    return rows[:limit]


def _all_table_rows_safe(db_path: Path | None, table_name: str, order_column: str) -> list[dict[str, Any]]:
    if db_path is None or not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(f"select * from {table_name} order by {order_column} desc").fetchall()
    except sqlite3.Error:
        connection.close()
        return []
    connection.close()
    return [dict(row) for row in rows]


def _all_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _resolve_sqlite_database_path(value: Any) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("sqlite:///"):
        return Path(raw.replace("sqlite:///", "", 1))
    return Path(raw)


def _load_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _load_json_path(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return _load_json(path.read_text(encoding="utf-8"))
    except OSError:
        return {}


def _current_position_state(
    *,
    matched_rows: Sequence[dict[str, Any]],
    latest_snapshot: dict[str, Any],
    latest_trade: dict[str, Any],
    latest_fill: dict[str, Any],
) -> tuple[str, int | None, Decimal | None]:
    position_side = str(
        latest_snapshot.get("position_side")
        or latest_snapshot.get("payload", {}).get("position_side")
        or _value_from_rows(matched_rows, "position_side")
        or "FLAT"
    ).upper()
    quantity_value = (
        latest_snapshot.get("position_quantity")
        or latest_snapshot.get("payload", {}).get("position_quantity")
        or latest_snapshot.get("payload", {}).get("quantity")
    )
    quantity: int | None
    if quantity_value in (None, ""):
        open_row = next((row for row in matched_rows if bool(row.get("open_position"))), None)
        if position_side == "FLAT":
            quantity = 0
        elif open_row is not None:
            try:
                quantity = int(open_row.get("trade_size") or 1)
            except (TypeError, ValueError):
                quantity = 1
        else:
            quantity = None
    else:
        try:
            quantity = int(quantity_value)
        except (TypeError, ValueError):
            quantity = None
    average_price = (
        _decimal_or_none(latest_snapshot.get("position_average_price"))
        or _decimal_or_none(latest_snapshot.get("payload", {}).get("position_average_price"))
        or _decimal_or_none(latest_trade.get("entry_price") if latest_trade and str(latest_trade.get("status") or "").upper() == "OPEN" else None)
        or _decimal_or_none(latest_fill.get("fill_price"))
    )
    if position_side == "FLAT":
        return "FLAT", 0, None
    return position_side, quantity, average_price


def _performance_summary(*, strategy_trades: Sequence[dict[str, Any]], session_date: str) -> dict[str, Any]:
    closed_trades = [
        dict(row)
        for row in strategy_trades
        if str(row.get("status") or "").upper() == "CLOSED"
        or row.get("exit_timestamp")
        or row.get("realized_pnl") is not None
        or row.get("net_pnl") is not None
        or row.get("gross_pnl") is not None
    ]
    realized_values = [
        _decimal_or_none(row.get("realized_pnl") or row.get("net_pnl") or row.get("gross_pnl"))
        for row in closed_trades
    ]
    realized_values = [value for value in realized_values if value is not None]
    gross_profit = sum((value for value in realized_values if value > 0), Decimal("0"))
    gross_loss_abs = sum((-value for value in realized_values if value < 0), Decimal("0"))
    realized_pnl = sum(realized_values, Decimal("0"))
    profit_factor = None
    if gross_loss_abs > 0:
        profit_factor = gross_profit / gross_loss_abs
    elif gross_profit > 0 and closed_trades:
        profit_factor = Decimal("999")
    average_trade = realized_pnl / Decimal(len(realized_values)) if realized_values else None
    winner_count = sum(1 for value in realized_values if value > 0)
    loser_count = sum(1 for value in realized_values if value < 0)
    trade_count = len(closed_trades)
    long_trade_count = sum(1 for row in closed_trades if str(row.get("side") or row.get("direction") or "").upper() == "LONG")
    short_trade_count = sum(1 for row in closed_trades if str(row.get("side") or row.get("direction") or "").upper() == "SHORT")
    current_day_values = [
        _decimal_or_none(row.get("realized_pnl") or row.get("net_pnl") or row.get("gross_pnl"))
        for row in closed_trades
        if session_date
        and str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")[:10] == session_date
    ]
    current_day_pnl = sum((value for value in current_day_values if value is not None), Decimal("0"))
    max_drawdown = _max_drawdown(realized_values)
    last_trade = closed_trades[0] if closed_trades else (strategy_trades[0] if strategy_trades else {})
    win_rate = (Decimal(winner_count) / Decimal(trade_count) * Decimal("100")) if trade_count else None
    session_breakdown = _tracked_trade_session_breakdown(closed_trades)
    trade_family_breakdown = _tracked_trade_family_breakdown(closed_trades)
    return {
        "trade_count": trade_count,
        "long_trade_count": long_trade_count,
        "short_trade_count": short_trade_count,
        "winner_count": winner_count,
        "loser_count": loser_count,
        "realized_pnl": _decimal_to_string(realized_pnl),
        "average_trade_pnl": _decimal_to_string(average_trade),
        "win_rate": _decimal_to_string(win_rate),
        "profit_factor": _decimal_to_string(profit_factor),
        "max_drawdown": _decimal_to_string(max_drawdown),
        "current_day_pnl": _decimal_to_string(current_day_pnl),
        "session_breakdown": session_breakdown,
        "trade_family_breakdown": trade_family_breakdown,
        "trade_history_scope": (
            "Exact closed-trade history for this tracked strategy, grouped from persisted paper strategy trade-log rows."
            if closed_trades
            else "No closed trades were present in the persisted tracked paper trade log yet."
        ),
        "last_trade_summary": (
            {
                "entry_timestamp": last_trade.get("entry_timestamp"),
                "exit_timestamp": last_trade.get("exit_timestamp"),
                "side": last_trade.get("side") or last_trade.get("direction"),
                "family": _tracked_trade_family(last_trade),
                "realized_pnl": last_trade.get("realized_pnl") or last_trade.get("net_pnl") or last_trade.get("gross_pnl"),
                "exit_reason": last_trade.get("exit_reason"),
                "entry_price": last_trade.get("entry_price"),
                "exit_price": last_trade.get("exit_price"),
                "strategy_name": last_trade.get("strategy_name"),
                "status": last_trade.get("status"),
            }
            if last_trade
            else None
        ),
    }


def _max_drawdown(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for value in values:
        cumulative += value
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _tracked_trade_session_breakdown(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in trades:
        session = _tracked_trade_entry_session(row)
        if not session:
            continue
        payload = grouped.setdefault(
            session,
            {
                "session": session,
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "realized_pnl_available": True,
                "latest_trade_timestamp": None,
            },
        )
        payload["trade_count"] += 1
        pnl = _decimal_or_none(row.get("realized_pnl") or row.get("net_pnl") or row.get("gross_pnl"))
        if pnl is None:
            payload["realized_pnl_available"] = False
        else:
            payload["realized_pnl"] += pnl
            if pnl > 0:
                payload["wins"] += 1
            elif pnl < 0:
                payload["losses"] += 1
        latest_trade_timestamp = str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")
        if latest_trade_timestamp and (
            payload["latest_trade_timestamp"] is None
            or latest_trade_timestamp > payload["latest_trade_timestamp"]
        ):
            payload["latest_trade_timestamp"] = latest_trade_timestamp
    return _finalize_tracked_trade_breakdown_rows(grouped.values(), label_key="session")


def _tracked_trade_family_breakdown(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in trades:
        family = _tracked_trade_family(row)
        if not family:
            continue
        payload = grouped.setdefault(
            family,
            {
                "family": family,
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "realized_pnl_available": True,
                "latest_trade_timestamp": None,
                "source_families": set(),
            },
        )
        payload["trade_count"] += 1
        pnl = _decimal_or_none(row.get("realized_pnl") or row.get("net_pnl") or row.get("gross_pnl"))
        if pnl is None:
            payload["realized_pnl_available"] = False
        else:
            payload["realized_pnl"] += pnl
            if pnl > 0:
                payload["wins"] += 1
            elif pnl < 0:
                payload["losses"] += 1
        source_family = str(row.get("signal_family") or row.get("family") or "").strip()
        if source_family:
            payload["source_families"].add(source_family)
        latest_trade_timestamp = str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")
        if latest_trade_timestamp and (
            payload["latest_trade_timestamp"] is None
            or latest_trade_timestamp > payload["latest_trade_timestamp"]
        ):
            payload["latest_trade_timestamp"] = latest_trade_timestamp
    rows = _finalize_tracked_trade_breakdown_rows(grouped.values(), label_key="family")
    for row in rows:
        source_families = grouped[str(row["family"])]["source_families"]
        if source_families:
            row["source_families"] = sorted(source_families)
    return rows


def _finalize_tracked_trade_breakdown_rows(
    payloads: Iterable[dict[str, Any]],
    *,
    label_key: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        rows.append(
            {
                label_key: payload[label_key],
                "trade_count": payload["trade_count"],
                "wins": payload["wins"],
                "losses": payload["losses"],
                "realized_pnl": _decimal_to_string(payload["realized_pnl"]) if payload["realized_pnl_available"] else None,
                "latest_trade_timestamp": payload["latest_trade_timestamp"],
            }
        )
    rows.sort(
        key=lambda row: (
            _decimal_or_none(row.get("realized_pnl")) or Decimal("-999999999"),
            str(row.get(label_key) or ""),
        ),
        reverse=True,
    )
    return rows


def _tracked_trade_family(row: dict[str, Any]) -> str | None:
    for key in ("signal_family_label", "signal_family", "family", "source", "strategy_name"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return None


def _tracked_trade_entry_session(row: dict[str, Any]) -> str | None:
    explicit = str(row.get("entry_session_phase") or row.get("session_phase") or "").strip()
    if explicit:
        return explicit
    entry_dt = _parse_iso_datetime(str(row.get("entry_timestamp") or ""))
    if entry_dt is None:
        return None
    return label_session_phase(entry_dt)


def _tracked_strategy_status(
    *,
    matched_rows: Sequence[dict[str, Any]],
    entries_enabled: bool,
    position_side: str,
    recent_faults: Sequence[dict[str, Any]],
    recent_reconciliation: Sequence[dict[str, Any]],
    paper: dict[str, Any],
    runtime_attached: bool,
    data_stale: bool,
) -> tuple[str, str]:
    if recent_faults:
        latest_fault = recent_faults[0]
        return "FAULT", str(latest_fault.get("fault_code") or latest_fault.get("detail") or "Recent persisted fault event present.")
    if any(row.get("clean") is False for row in recent_reconciliation):
        return "RECONCILING", "Persisted reconciliation issues are still unresolved."
    runtime_recovery = paper.get("runtime_recovery") or {}
    if runtime_recovery.get("manual_action_required") is True or runtime_recovery.get("restore_unresolved_issue"):
        return "RECONCILING", str(runtime_recovery.get("restore_unresolved_issue") or "Runtime restore uncertainty still active.")
    if matched_rows and not runtime_attached:
        return "RECONCILING", "Tracked strategy restored from persisted paper artifacts; runtime instance is not yet fully reattached."
    if data_stale:
        return "RECONCILING", "Tracked paper runtime heartbeat is stale or data is no longer current."
    if position_side != "FLAT":
        return "IN_POSITION", "Paper position is open."
    if entries_enabled:
        return "READY", "Tracked strategy is enabled and waiting for eligible paper setups."
    return "DISABLED", "Tracked strategy is registered but entries are currently disabled."


def _current_session_segment(*, paper_status: dict[str, Any], latest_processed_bar_timestamp: str | None) -> str | None:
    raw = str(paper_status.get("current_detected_session") or "").upper()
    if not raw:
        latest_dt = _parse_iso_datetime(latest_processed_bar_timestamp)
        if latest_dt is None:
            return None
        raw = label_session_phase(latest_dt)
    if raw.startswith("ASIA"):
        return "ASIA"
    if raw.startswith("LONDON"):
        return "LONDON"
    if raw.startswith("US"):
        return "US"
    return None


def _signal_summary(latest_signal: dict[str, Any], matched_rows: Sequence[dict[str, Any]]) -> str | None:
    if latest_signal:
        timestamp = _timestamp_from_row(latest_signal, "signal_timestamp", "created_at", "timestamp", "entry_timestamp_planned")
        decision = str(latest_signal.get("decision") or latest_signal.get("signal_passed_flag") or latest_signal.get("status") or "-")
        blocker = str(latest_signal.get("block_reason") or latest_signal.get("rejection_reason_code") or latest_signal.get("override_reason") or "").strip()
        parts = [part for part in (timestamp, decision, blocker or None) if part]
        return " | ".join(parts)
    latest_row = next(iter(matched_rows), {})
    return _string_or_none(latest_row.get("operator_status_line"))


def _latest_trade(strategy_trades: Sequence[dict[str, Any]], recent_trade_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if strategy_trades:
        return max(
            (dict(row) for row in strategy_trades),
            key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""),
        )
    if recent_trade_rows:
        return dict(recent_trade_rows[0])
    return {}


def _latest_processed_bar_timestamp(matched_rows: Sequence[dict[str, Any]], recent_bars: Sequence[dict[str, Any]]) -> str | None:
    explicit_bar_timestamps = [
        _string_or_none(_nested_get(row, "operator_status_payload", "last_processed_bar_end_ts"))
        for row in matched_rows
    ]
    if recent_bars:
        explicit_bar_timestamps.append(_timestamp_from_row(recent_bars[0], "end_ts", "timestamp", "bar_end_ts"))
    resolved = _latest_timestamp_value(explicit_bar_timestamps)
    if resolved:
        return resolved
    return _latest_timestamp_value(
        [
            _string_or_none(row.get("latest_activity_timestamp")) or _string_or_none(row.get("last_update_timestamp"))
            for row in matched_rows
        ]
    )


def _latest_operator_status(matched_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    statuses = [
        dict(_nested_get(row, "operator_status_payload") or {})
        for row in matched_rows
        if _nested_get(row, "operator_status_payload")
    ]
    statuses.sort(key=lambda row: str(row.get("updated_at") or row.get("generated_at") or row.get("last_processed_bar_end_ts") or ""), reverse=True)
    return statuses[0] if statuses else {}


def _latest_runtime_state(matched_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    payloads: list[dict[str, Any]] = []
    for row in matched_rows:
        runtime_state_path = _nested_get(row, "artifacts", "runtime_state")
        if not runtime_state_path:
            continue
        payload = _load_json_path(Path(str(runtime_state_path)))
        if payload:
            payloads.append(payload)
    payloads.sort(key=lambda row: str(row.get("generated_at") or row.get("latest_polled_end_ts") or ""), reverse=True)
    return payloads[0] if payloads else {}


def _tracked_paper_lifecycle_contract(
    *,
    definition: TrackedPaperStrategyDefinition,
    latest_operator_status: dict[str, Any],
    latest_runtime_state: dict[str, Any],
    recent_order_intents: Sequence[dict[str, Any]],
    recent_fills: Sequence[dict[str, Any]],
    recent_trades: Sequence[dict[str, Any]],
    recent_state_snapshots: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    explicit_contract = {
        key: latest_operator_status.get(key)
        for key in (
            "entry_model",
            "active_entry_model",
            "supported_entry_models",
            "entry_model_supported",
            "execution_truth_emitter",
            "intrabar_execution_authoritative",
            "authoritative_intrabar_available",
            "authoritative_entry_truth_available",
            "authoritative_exit_truth_available",
            "authoritative_trade_lifecycle_available",
            "lifecycle_records",
            "authoritative_trade_lifecycle_records",
            "pnl_truth_basis",
            "lifecycle_truth_class",
            "unsupported_reason",
            "truth_provenance",
        )
        if key in latest_operator_status
    }
    if not explicit_contract:
        explicit_contract = {
            key: latest_runtime_state.get(key)
            for key in (
                "entry_model",
                "active_entry_model",
                "supported_entry_models",
                "entry_model_supported",
                "execution_truth_emitter",
                "intrabar_execution_authoritative",
                "authoritative_intrabar_available",
                "authoritative_entry_truth_available",
                "authoritative_exit_truth_available",
                "authoritative_trade_lifecycle_available",
                "lifecycle_records",
                "authoritative_trade_lifecycle_records",
                "pnl_truth_basis",
                "lifecycle_truth_class",
                "unsupported_reason",
                "truth_provenance",
            )
            if key in latest_runtime_state
        }

    if explicit_contract:
        contract = dict(explicit_contract)
        contract.setdefault("entry_model", contract.get("active_entry_model") or CURRENT_CANDLE_VWAP)
        contract.setdefault("active_entry_model", contract.get("entry_model") or CURRENT_CANDLE_VWAP)
        contract.setdefault("supported_entry_models", [BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP])
        contract.setdefault("entry_model_supported", True)
        contract.setdefault("execution_truth_emitter", "atp_phase3_timing_emitter")
        contract.setdefault("intrabar_execution_authoritative", bool(contract.get("authoritative_intrabar_available")))
        contract.setdefault("authoritative_intrabar_available", bool(contract.get("intrabar_execution_authoritative")))
        contract.setdefault("authoritative_entry_truth_available", False)
        contract.setdefault("authoritative_exit_truth_available", False)
        contract.setdefault("authoritative_trade_lifecycle_available", False)
        contract.setdefault("pnl_truth_basis", PAPER_RUNTIME_LEDGER)
        contract.setdefault("lifecycle_truth_class", AUTHORITATIVE_INTRABAR_ENTRY_ONLY)
        contract.setdefault("unsupported_reason", None)
        contract.setdefault(
            "truth_provenance",
            {
                "runtime_context": "PAPER",
                "run_lane": "PAPER_RUNTIME",
                "artifact_context": "TRACKED_PAPER_STRATEGY",
                "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                "study_mode": "paper_runtime",
                "artifact_rebuilt": False,
            },
        )
        lifecycle_records = list(
            contract.get("lifecycle_records")
            or contract.get("authoritative_trade_lifecycle_records")
            or normalize_trade_lifecycle_records(
                recent_trades,
                entry_model=str(contract.get("active_entry_model") or contract.get("entry_model") or CURRENT_CANDLE_VWAP),
                pnl_truth_basis=str(contract.get("pnl_truth_basis") or PAPER_RUNTIME_LEDGER),
                lifecycle_truth_class=str(contract.get("lifecycle_truth_class") or AUTHORITATIVE_INTRABAR_ENTRY_ONLY),
                truth_provenance=dict(contract.get("truth_provenance") or {}),
                record_source="TRACKED_PAPER_TRADE_LEDGER",
            )
        )
        contract["lifecycle_records"] = [dict(row) for row in lifecycle_records]
        contract["authoritative_trade_lifecycle_records"] = [dict(row) for row in lifecycle_records]
        contract["recent_trade_lifecycle_preview"] = [dict(row) for row in lifecycle_records[:12]]
        return contract

    entry_model = CURRENT_CANDLE_VWAP if definition.strategy_id == TRACKED_ATP_STRATEGY_ID else BASELINE_NEXT_BAR_OPEN
    entry_events_present = bool(recent_order_intents or recent_fills or latest_operator_status.get("latest_atp_entry_state") or latest_operator_status.get("latest_atp_timing_state"))
    exit_events_present = any(
        str(row.get("intent_type") or "").upper() == "SELL_TO_CLOSE"
        for row in recent_order_intents
    ) or any(
        str(row.get("intent_type") or "").upper() == "SELL_TO_CLOSE"
        for row in recent_fills
    ) or any(row.get("exit_timestamp") or row.get("exit_reason") for row in recent_trades)
    lifecycle_records_present = bool(recent_trades or recent_state_snapshots or recent_order_intents or recent_fills)
    lifecycle_truth_class = (
        FULL_AUTHORITATIVE_LIFECYCLE
        if entry_events_present and exit_events_present and lifecycle_records_present
        else AUTHORITATIVE_INTRABAR_ENTRY_ONLY
        if entry_events_present
        else AUTHORITATIVE_INTRABAR_ENTRY_ONLY
    )
    return {
        "entry_model": entry_model,
        "active_entry_model": entry_model,
        "supported_entry_models": [BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP],
        "entry_model_supported": True,
        "execution_truth_emitter": "atp_phase3_timing_emitter",
        "intrabar_execution_authoritative": True,
        "authoritative_intrabar_available": bool(
            latest_operator_status.get("latest_atp_entry_state")
            or latest_operator_status.get("latest_atp_timing_state")
            or recent_state_snapshots
            or recent_order_intents
        ),
        "authoritative_entry_truth_available": entry_events_present,
        "authoritative_exit_truth_available": exit_events_present,
        "authoritative_trade_lifecycle_available": lifecycle_records_present,
        "lifecycle_records": normalize_trade_lifecycle_records(
            recent_trades,
            entry_model=entry_model,
            pnl_truth_basis=PAPER_RUNTIME_LEDGER,
            lifecycle_truth_class=lifecycle_truth_class,
            truth_provenance={
                "runtime_context": "PAPER",
                "run_lane": "PAPER_RUNTIME",
                "artifact_context": "TRACKED_PAPER_STRATEGY",
                "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                "study_mode": "paper_runtime",
                "artifact_rebuilt": False,
            },
            record_source="TRACKED_PAPER_TRADE_LEDGER",
        ),
        "pnl_truth_basis": PAPER_RUNTIME_LEDGER,
        "lifecycle_truth_class": lifecycle_truth_class,
        "unsupported_reason": None,
        "truth_provenance": {
            "runtime_context": "PAPER",
            "run_lane": "PAPER_RUNTIME",
            "artifact_context": "TRACKED_PAPER_STRATEGY",
            "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
            "study_mode": "paper_runtime",
            "artifact_rebuilt": False,
        },
        "authoritative_trade_lifecycle_records": normalize_trade_lifecycle_records(
            recent_trades,
            entry_model=entry_model,
            pnl_truth_basis=PAPER_RUNTIME_LEDGER,
            lifecycle_truth_class=lifecycle_truth_class,
            truth_provenance={
                "runtime_context": "PAPER",
                "run_lane": "PAPER_RUNTIME",
                "artifact_context": "TRACKED_PAPER_STRATEGY",
                "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                "study_mode": "paper_runtime",
                "artifact_rebuilt": False,
            },
            record_source="TRACKED_PAPER_TRADE_LEDGER",
        ),
        "recent_trade_lifecycle_preview": normalize_trade_lifecycle_records(
            recent_trades,
            entry_model=entry_model,
            pnl_truth_basis=PAPER_RUNTIME_LEDGER,
            lifecycle_truth_class=lifecycle_truth_class,
            truth_provenance={
                "runtime_context": "PAPER",
                "run_lane": "PAPER_RUNTIME",
                "artifact_context": "TRACKED_PAPER_STRATEGY",
                "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                "study_mode": "paper_runtime",
                "artifact_rebuilt": False,
            },
            record_source="TRACKED_PAPER_TRADE_LEDGER",
        ),
    }


def _current_entry_family(latest_snapshot: dict[str, Any], latest_trade: dict[str, Any], latest_intent: dict[str, Any]) -> str | None:
    payload = latest_snapshot.get("payload") or {}
    return _string_or_none(
        payload.get("long_entry_family")
        or payload.get("short_entry_family")
        or latest_trade.get("signal_family")
        or latest_trade.get("family")
        or latest_intent.get("reason_code")
    )


def _latest_stop_risk_context(
    latest_snapshot: dict[str, Any],
    latest_trade: dict[str, Any],
    matched_rows: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    payload = latest_snapshot.get("payload") or {}
    stop_context = latest_snapshot.get("stop_context") or payload.get("stop_context") or payload.get("latest_stop_context")
    if isinstance(stop_context, dict) and stop_context:
        return stop_context
    if latest_trade:
        return {
            "exit_reason": latest_trade.get("exit_reason"),
            "status": latest_trade.get("status"),
            "entry_price": latest_trade.get("entry_price"),
        }
    latest_row = next(iter(matched_rows), {})
    latest_atp_state = latest_row.get("latest_atp_state") or {}
    latest_atp_entry_state = latest_row.get("latest_atp_entry_state") or {}
    latest_atp_timing_state = latest_row.get("latest_atp_timing_state") or {}
    if latest_atp_state or latest_atp_entry_state or latest_atp_timing_state:
        return {
            "bias_state": latest_atp_state.get("bias_state"),
            "pullback_state": latest_atp_state.get("pullback_state"),
            "entry_state": latest_atp_entry_state.get("entry_state"),
            "timing_state": latest_atp_timing_state.get("timing_state"),
            "vwap_price_quality_state": latest_atp_timing_state.get("vwap_price_quality_state"),
        }
    return None


def _open_pnl_from_latest_mark(
    *,
    quantity: int | None,
    average_price: Decimal | None,
    latest_bar_close: Decimal | None,
    position_side: str,
) -> Decimal | None:
    if quantity in (None, 0) or average_price is None or latest_bar_close is None or position_side == "FLAT":
        return None
    signed_quantity = Decimal(abs(quantity))
    if position_side == "LONG":
        return (latest_bar_close - average_price) * signed_quantity
    if position_side == "SHORT":
        return (average_price - latest_bar_close) * signed_quantity
    return None


def _bars_in_trade(
    *,
    latest_trade: dict[str, Any],
    latest_fill: dict[str, Any],
    latest_processed_bar_timestamp: str | None,
    position_side: str,
) -> int | None:
    if position_side == "FLAT":
        return 0
    entry_timestamp = _timestamp_from_row(latest_trade, "entry_timestamp") or _timestamp_from_row(latest_fill, "fill_timestamp")
    entry_dt = _parse_iso_datetime(entry_timestamp)
    latest_bar_dt = _parse_iso_datetime(latest_processed_bar_timestamp)
    if entry_dt is None or latest_bar_dt is None or latest_bar_dt < entry_dt:
        return None
    elapsed_minutes = int((latest_bar_dt - entry_dt).total_seconds() // 60)
    return max(elapsed_minutes // 5, 0)


def _merge_boolean(values: Iterable[Any]) -> bool | None:
    normalized = [value for value in values if isinstance(value, bool)]
    if not normalized:
        return None
    return all(normalized)


def _nested_get(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _value_from_rows(rows: Sequence[dict[str, Any]], key: str) -> Any:
    for row in rows:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _timestamp_from_row(row: dict[str, Any], *fields: str) -> str | None:
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _latest_timestamp_value(values: Iterable[str | None]) -> str | None:
    normalized = [str(value) for value in values if value not in (None, "")]
    return max(normalized) if normalized else None


def _row_sort_key(row: dict[str, Any], fields: Sequence[str]) -> str:
    for field in fields:
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return ""


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _string_or_none(value: Any) -> str | None:
    normalized = str(value).strip() if value is not None else ""
    return normalized or None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
