"""Tests for the operator dashboard data surface."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest

import mgc_v05l.app.operator_dashboard as operator_dashboard_module
from mgc_v05l.app.experimental_canaries_dashboard_payloads import load_experimental_canaries_snapshot
from mgc_v05l.app.operator_dashboard import (
    DashboardServerInfo,
    OperatorDashboardService,
    _bind_dashboard_server,
    _build_handler,
    _json_ready,
    _market_index_rows,
    _treasury_curve_rows,
)
from mgc_v05l.app.tracked_paper_strategies import build_tracked_paper_strategies_payload
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.db import create_schema
from mgc_v05l.persistence.tables import research_capture_status_table


_DASHBOARD_DB_SCHEMA = """
    create table features (
      bar_id text primary key,
      payload_json text not null,
      created_at text not null
    );
    create table signals (
      bar_id text primary key,
      payload_json text not null,
      created_at text not null
    );
    create table order_intents (
      order_intent_id text primary key,
      bar_id text,
      symbol text,
      intent_type text,
      quantity integer,
      created_at text,
      reason_code text,
      broker_order_id text,
      order_status text
    );
    create table fills (
      fill_id integer primary key autoincrement,
      order_intent_id text,
      intent_type text,
      order_status text,
      fill_timestamp text,
      fill_price text,
      broker_order_id text
    );
    create table bars (
      bar_id text primary key,
      data_source text,
      ticker text,
      symbol text,
      timeframe text,
      timestamp text,
      start_ts text,
      end_ts text,
      open text,
      high text,
      low text,
      close text,
      volume integer,
      is_final integer,
      session_asia integer,
      session_london integer,
      session_us integer,
      session_allowed integer,
      created_at text
    );
    create table processed_bars (
      bar_id text primary key,
      end_ts text
    );
"""


def _init_dashboard_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "intent-1",
                "bar-1",
                "MGC",
                "BUY_TO_OPEN",
                1,
                "2026-03-18T14:00:00-04:00",
                "asiaEarlyNormalBreakoutRetestHoldTurn",
                "paper-intent-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "intent-1",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-18T14:05:00-04:00",
                "100.0",
                "paper-intent-1",
            ),
        )
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "bar-1",
                "schwab_live_poll",
                "MGC",
                "MGC",
                "5m",
                "2026-03-18T14:05:00-04:00",
                "2026-03-18T14:00:00-04:00",
                "2026-03-18T14:05:00-04:00",
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                "2026-03-18T14:05:00-04:00",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _init_empty_dashboard_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.commit()
    finally:
        connection.close()


def _write_experimental_canary_snapshot(
    repo_root: Path,
    *,
    enabled: bool = True,
    kill_switch_active: bool = False,
) -> Path:
    canary_root = repo_root / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine"
    lane_dir = canary_root / "lanes" / "atpe_long_medium_high_canary"
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "enabled": enabled,
                "experimental_status": "experimental_canary",
                "generated_at": "2026-03-23T19:45:00-04:00",
                "kill_switch_active": kill_switch_active,
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "latest_atp_state": {
                    "bias_state": "LONG_BIAS",
                    "bias_reasons": ["ema_aligned_up", "close_above_vwap"],
                    "pullback_state": "NORMAL_PULLBACK",
                    "pullback_envelope_state": "STANDARD",
                    "pullback_depth_score": 0.82,
                    "pullback_violence_score": 0.44,
                    "pullback_reason": None,
                    "standard_pullback_envelope": {
                        "min_reset_depth": 0.3,
                        "standard_depth": 0.75,
                        "stretched_depth": 1.05,
                        "disqualify_depth": 1.35,
                    },
                },
                "latest_atp_entry_state": {
                    "family_name": "atp_v1_long_pullback_continuation",
                    "continuation_trigger_state": "CONTINUATION_TRIGGER_CONFIRMED",
                    "entry_state": "ENTRY_ELIGIBLE",
                    "primary_blocker": None,
                    "blocker_codes": [],
                },
                "latest_atp_timing_state": {
                    "timing_state": "ATP_TIMING_CONFIRMED",
                    "vwap_price_quality_state": "VWAP_FAVORABLE",
                    "primary_blocker": None,
                    "blocker_codes": [],
                    "entry_executed": True,
                },
                "paper_only": True,
                "priority_tier": "lower_priority_than_live_strategies",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_count": 3,
                "trade_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (lane_dir / "signals.jsonl").write_text(
        json.dumps(
            {
                "decision": "allowed",
                "experimental_status": "experimental_canary",
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "override_reason": "paper_only_experimental_canary",
                "paper_only": True,
                "quality_bucket": "MEDIUM",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_passed_flag": True,
                "signal_timestamp": "2026-03-23T19:40:00-04:00",
                "symbol": "MES",
            }
        )
        + "\n"
        + json.dumps(
            {
                "decision": "blocked",
                "experimental_status": "experimental_canary",
                "lane_id": "atpe_long_medium_high_canary",
                "lane_name": "ATPE Long Medium+High Canary",
                "override_reason": "paper_only_experimental_canary",
                "paper_only": True,
                "quality_bucket": "HIGH",
                "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                "side": "LONG",
                "signal_passed_flag": False,
                "signal_timestamp": "2026-03-23T19:41:00-04:00",
                "symbol": "MNQ",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (lane_dir / "events.jsonl").write_text(
        json.dumps({"event_type": "snapshot_written", "timestamp": "2026-03-23T19:45:00-04:00"}) + "\n",
        encoding="utf-8",
    )
    (canary_root / "experimental_canaries_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-23T19:45:00-04:00",
                "kill_switch": {
                    "active": kill_switch_active,
                    "operator_action": "Toggle the canary kill switch file.",
                    "path": str(canary_root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"),
                },
                "module": "Active Trend Participation Engine",
                "operator_summary_line": "ATPE Long Medium+High Canary ready for dashboard observation.",
                "rows": [
                    {
                        "artifacts": {
                            "events": str((lane_dir / "events.jsonl").resolve()),
                            "operator_status": str((lane_dir / "operator_status.json").resolve()),
                            "signals": str((lane_dir / "signals.jsonl").resolve()),
                        },
                        "experimental_status": "experimental_canary",
                        "lane_id": "atpe_long_medium_high_canary",
                        "lane_name": "ATPE Long Medium+High Canary",
                        "latest_atp_state": {
                            "bias_state": "LONG_BIAS",
                            "pullback_state": "NORMAL_PULLBACK",
                            "pullback_depth_score": 0.82,
                            "pullback_violence_score": 0.44,
                            "pullback_reason": None,
                        },
                        "latest_atp_entry_state": {
                            "entry_state": "ENTRY_ELIGIBLE",
                            "primary_blocker": None,
                            "continuation_trigger_state": "CONTINUATION_TRIGGER_CONFIRMED",
                        },
                        "latest_atp_timing_state": {
                            "timing_state": "ATP_TIMING_CONFIRMED",
                            "vwap_price_quality_state": "VWAP_FAVORABLE",
                            "primary_blocker": None,
                        },
                        "metrics": {
                            "max_drawdown": 42.5,
                            "net_pnl_cash": 18.75,
                            "total_trades": 1,
                        },
                        "operator_summary": {
                            "what_it_is": "Paper-only canary.",
                            "what_it_is_not": "Not a production alpha strategy.",
                        },
                        "paper_only": True,
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "side": "LONG",
                        "symbols": ["MES", "MNQ"],
                        "variant_id": "trend_participation.pullback_continuation.long.conservative",
                    }
                ],
                "scope_label": "Experimental paper canaries for Active Trend Participation Engine",
                "status": "available",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (canary_root / "experimental_canaries_snapshot.md").write_text("# Experimental Canaries\n", encoding="utf-8")
    (canary_root / "operator_summary.md").write_text("# Operator Summary\n", encoding="utf-8")
    return canary_root


def _init_strategy_lane_dashboard_db(
    path: Path,
    *,
    symbol: str,
    entry_reason: str,
    closed_trade_pnl: str | None,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_DASHBOARD_DB_SCHEMA)
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{symbol}-entry",
                f"{symbol}-bar-entry",
                symbol,
                "BUY_TO_OPEN",
                1,
                "2026-03-22T13:30:00-04:00",
                entry_reason,
                f"{symbol}-broker-entry",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{symbol}-entry",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-22T13:35:00-04:00",
                "100.0",
                f"{symbol}-broker-entry",
            ),
        )
        if closed_trade_pnl is not None:
            exit_price = "102.5" if closed_trade_pnl == "25.0" else "100.5"
            connection.execute(
                "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-exit",
                    f"{symbol}-bar-exit",
                    symbol,
                    "SELL_TO_CLOSE",
                    1,
                    "2026-03-22T13:40:00-04:00",
                    entry_reason,
                    f"{symbol}-broker-exit",
                    "FILLED",
                ),
            )
            connection.execute(
                "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
                (
                    f"{symbol}-exit",
                    "SELL_TO_CLOSE",
                    "FILLED",
                    "2026-03-22T13:45:00-04:00",
                    exit_price,
                    f"{symbol}-broker-exit",
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _append_strategy_lane_closed_trade(
    path: Path,
    *,
    symbol: str,
    trade_id: str,
    entry_reason: str,
    entry_created_at: str,
    entry_fill_at: str,
    exit_created_at: str,
    exit_fill_at: str,
    entry_price: str,
    exit_price: str,
    entry_bar_id: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-entry",
                entry_bar_id,
                symbol,
                "BUY_TO_OPEN",
                1,
                entry_created_at,
                entry_reason,
                f"{trade_id}-broker-entry",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-entry",
                "BUY_TO_OPEN",
                "FILLED",
                entry_fill_at,
                entry_price,
                f"{trade_id}-broker-entry",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-exit",
                f"{entry_bar_id}-exit",
                symbol,
                "SELL_TO_CLOSE",
                1,
                exit_created_at,
                entry_reason,
                f"{trade_id}-broker-exit",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                f"{trade_id}-exit",
                "SELL_TO_CLOSE",
                "FILLED",
                exit_fill_at,
                exit_price,
                f"{trade_id}-broker-exit",
            ),
        )
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_bar_id,
                "schwab_live_poll",
                symbol,
                symbol,
                "5m",
                entry_created_at,
                entry_created_at,
                entry_fill_at,
                entry_price,
                entry_price,
                entry_price,
                entry_price,
                100,
                1,
                0,
                0,
                1,
                1,
                entry_created_at,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_bar(
    path: Path,
    *,
    bar_id: str,
    symbol: str,
    start_ts: str,
    end_ts: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                bar_id,
                "schwab_live_poll",
                symbol,
                symbol,
                "5m",
                end_ts,
                start_ts,
                end_ts,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                100,
                1,
                0,
                0,
                1,
                1,
                end_ts,
            ),
        )
        connection.execute(
            "insert into processed_bars values (?, ?)",
            (bar_id, end_ts),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_signal(
    path: Path,
    *,
    bar_id: str,
    created_at: str,
    payload: dict[str, object],
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into signals values (?, ?, ?)",
            (
                bar_id,
                json.dumps(payload),
                created_at,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_intent(
    path: Path,
    *,
    order_intent_id: str,
    bar_id: str,
    symbol: str,
    intent_type: str,
    created_at: str,
    reason_code: str,
    broker_order_id: str,
    order_status: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order_intent_id,
                bar_id,
                symbol,
                intent_type,
                1,
                created_at,
                reason_code,
                broker_order_id,
                order_status,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_dashboard_fill(
    path: Path,
    *,
    order_intent_id: str,
    intent_type: str,
    order_status: str,
    fill_timestamp: str,
    fill_price: str,
    broker_order_id: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                order_intent_id,
                intent_type,
                order_status,
                fill_timestamp,
                fill_price,
                broker_order_id,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _write_jsonl_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def _same_underlying_snapshot(
    tmp_path: Path,
    *,
    lanes: list[dict[str, object]],
    production_link_snapshot: dict[str, object] | None = None,
) -> dict[str, object]:
    service = _same_underlying_service(
        tmp_path,
        lanes=lanes,
        production_link_snapshot=production_link_snapshot,
    )
    return service.snapshot()


def _same_underlying_service(
    tmp_path: Path,
    *,
    lanes: list[dict[str, object]],
    production_link_snapshot: dict[str, object] | None = None,
) -> OperatorDashboardService:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True, exist_ok=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T13:45:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": lanes,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }
    service._production_link_service.snapshot = lambda: production_link_snapshot or {  # type: ignore[method-assign]
        "portfolio": {"positions": []},
        "orders": {"open_rows": []},
        "reconciliation": {"status": "CLEAR", "label": "CLEAR"},
    }
    return service


class _TestEnum(Enum):
    READY = "ready"


@dataclass
class _TestPayload:
    value: Decimal
    when: datetime


def test_json_ready_normalizes_nested_dashboard_payload_values(tmp_path: Path) -> None:
    payload = {
        "rows": [
            {
                "lane_id": "lane-a",
                "realized_pnl": Decimal("12.34"),
                "as_of": datetime(2026, 3, 22, 12, 34, 56, tzinfo=timezone.utc),
                "artifact": tmp_path / "row.json",
                "status": _TestEnum.READY,
                "detail": _TestPayload(
                    value=Decimal("56.78"),
                    when=datetime(2026, 3, 22, 12, 35, 0, tzinfo=timezone.utc),
                ),
            }
        ],
        "trade_log": (
            {
                "fill_price": Decimal("100.25"),
                "closed_at": datetime(2026, 3, 22, 12, 40, 0, tzinfo=timezone.utc),
            },
        ),
    }

    normalized = _json_ready(payload)

    assert normalized["rows"][0]["realized_pnl"] == "12.34"
    assert normalized["rows"][0]["as_of"] == "2026-03-22T12:34:56+00:00"
    assert normalized["rows"][0]["artifact"] == str(tmp_path / "row.json")
    assert normalized["rows"][0]["status"] == "ready"
    assert normalized["rows"][0]["detail"]["value"] == "56.78"
    assert normalized["trade_log"][0]["fill_price"] == "100.25"
    json.dumps(normalized, sort_keys=True)


def test_same_underlying_conflicts_treat_multiple_runtime_instances_as_informational_only(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull.sqlite3"
    gc_bear_db = tmp_path / "gc_bear.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    conflicts = snapshot["same_underlying_conflicts"]["rows"]
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict["instrument"] == "GC"
    assert conflict["conflict_kind"] == "multiple_runtime_instances_same_instrument"
    assert conflict["severity"] == "INFO"
    assert conflict["operator_action_required"] is False
    assert conflict["execution_risk"] is False
    assert conflict["observational_only"] is True
    assert conflict["overlap_scope"] == "STRATEGY_ONLY"

    audit_rows = [row for row in snapshot["paper"]["signal_intent_fill_audit"]["rows"] if row["instrument"] == "GC"]
    assert len(audit_rows) == 2
    assert all(row["same_underlying_conflict_present"] is True for row in audit_rows)
    assert all(row["same_underlying_conflict_severity"] == "INFO" for row in audit_rows)


def test_same_underlying_conflicts_detect_pending_order_overlap_as_blocking(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_pending.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_pending.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    _append_dashboard_bar(
        gc_bull_db,
        bar_id="gc-bull-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )
    _append_dashboard_bar(
        gc_bear_db,
        bar_id="gc-bear-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:35:00-04:00",
        end_ts="2026-03-23T09:40:00-04:00",
    )
    _append_dashboard_intent(
        gc_bull_db,
        order_intent_id="gc-bull-intent-1",
        bar_id="gc-bull-bar-1",
        symbol="GC",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:35:05-04:00",
        reason_code="bullSnap",
        broker_order_id="gc-bull-broker-1",
        order_status="WORKING",
    )
    _append_dashboard_intent(
        gc_bear_db,
        order_intent_id="gc-bear-intent-1",
        bar_id="gc-bear-bar-1",
        symbol="GC",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T09:40:05-04:00",
        reason_code="bearSnap",
        broker_order_id="gc-bear-broker-1",
        order_status="WORKING",
    )

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "multiple_pending_orders_same_instrument"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["operator_action_required"] is True
    assert conflict["execution_risk"] is True
    assert conflict["observational_only"] is False
    assert conflict["pending_order_overlap_present"] is True
    assert conflict["broker_overlap_present"] is False


def test_same_underlying_conflicts_detect_opposite_side_in_position_overlap_as_blocking(tmp_path: Path) -> None:
    gc_long_db = tmp_path / "gc_long.sqlite3"
    gc_short_db = tmp_path / "gc_short.sqlite3"
    _init_empty_dashboard_db(gc_long_db)
    _init_empty_dashboard_db(gc_short_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_long_lane",
                "display_name": "GC Long",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_db}",
            },
            {
                "lane_id": "gc_short_lane",
                "display_name": "GC Short",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "SHORT",
                "strategy_status": "IN_SHORT_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_short_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "opposite_side_in_position_overlap"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["in_position_overlap_present"] is True
    assert conflict["operator_action_required"] is True
    assert conflict["execution_risk"] is True
    assert conflict["position_side_profile"] == "BOTH"


def test_same_underlying_conflicts_treat_same_side_in_position_overlap_as_actionable_warning(tmp_path: Path) -> None:
    gc_long_a_db = tmp_path / "gc_long_a.sqlite3"
    gc_long_b_db = tmp_path / "gc_long_b.sqlite3"
    _init_empty_dashboard_db(gc_long_a_db)
    _init_empty_dashboard_db(gc_long_b_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_long_a_lane",
                "display_name": "GC Long A",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapA"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_a_db}",
            },
            {
                "lane_id": "gc_long_b_lane",
                "display_name": "GC Long B",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapB"],
                "approved_short_entry_sources": [],
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_long_b_db}",
            },
        ],
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "same_side_in_position_overlap"
    assert conflict["severity"] == "ACTION"
    assert conflict["in_position_overlap_present"] is True
    assert conflict["operator_action_required"] is False
    assert conflict["execution_risk"] is False
    assert conflict["observational_only"] is False
    assert conflict["position_side_profile"] == "LONG_ONLY"


def test_same_underlying_conflicts_detect_broker_runtime_overlap(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_broker.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_broker.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
        production_link_snapshot={
            "portfolio": {"positions": [{"symbol": "GC", "quantity": "1"}]},
            "orders": {"open_rows": [{"symbol": "GC", "broker_order_id": "gc-live-1"}]},
            "reconciliation": {"status": "DRIFTED", "label": "DRIFTED"},
        },
    )

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "broker_vs_strategy_overlap_mismatch"
    assert conflict["severity"] == "BLOCKING"
    assert conflict["broker_overlap_present"] is True
    assert conflict["overlap_scope"] == "BROKER_AND_STRATEGY"
    assert conflict["reconciliation_state"] == "DRIFTED"
    assert conflict["reconciliation_clear"] is False


def test_same_underlying_conflicts_ignore_different_instruments(tmp_path: Path) -> None:
    gc_db = tmp_path / "gc.sqlite3"
    cl_db = tmp_path / "cl.sqlite3"
    _init_empty_dashboard_db(gc_db)
    _init_empty_dashboard_db(cl_db)

    snapshot = _same_underlying_snapshot(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_db}",
            },
            {
                "lane_id": "cl_lane",
                "display_name": "CL Bear",
                "symbol": "CL",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{cl_db}",
            },
        ],
    )

    assert snapshot["same_underlying_conflicts"]["summary"]["conflict_count"] == 0
    assert snapshot["same_underlying_conflicts"]["rows"] == []


def test_same_underlying_conflict_acknowledgement_persists_by_instrument(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_ack.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_ack.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-acknowledge",
        {"instrument": "GC", "operator_label": "desk-op", "note": "Reviewed pre-open overlap."},
    )

    assert result["ok"] is True
    snapshot = result["snapshot"]
    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["acknowledged"] is True
    assert conflict["acknowledged_by"] == "desk-op"
    assert conflict["acknowledgement_note"] == "Reviewed pre-open overlap."
    assert conflict["review_state_status"] == "ACKNOWLEDGED"

    persisted = json.loads(
        (tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_state.json").read_text(encoding="utf-8")
    )
    assert persisted["records"]["GC"]["acknowledged"] is True
    assert persisted["records"]["GC"]["acknowledged_by"] == "desk-op"


def test_same_underlying_conflict_actions_persist_local_auth_metadata(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_auth.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_auth.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-acknowledge",
        {
            "instrument": "GC",
            "operator_label": "manual operator",
            "requested_operator_label": "desk-op-note",
            "local_operator_identity": "local_touch_id_operator",
            "auth_method": "TOUCH_ID",
            "authenticated_at": "2026-03-23T15:10:00+00:00",
            "auth_session_id": "auth-session-1",
            "note": "Reviewed with local auth.",
        },
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["acknowledged_by"] == "local_touch_id_operator"
    assert conflict["last_local_operator_identity"] == "local_touch_id_operator"
    assert conflict["last_auth_method"] == "TOUCH_ID"
    assert conflict["last_authenticated_at"] == "2026-03-23T15:10:00+00:00"
    assert conflict["last_auth_session_id"] == "auth-session-1"
    assert conflict["last_operator_authenticated"] is True
    assert conflict["last_requested_operator_label"] == "desk-op-note"

    latest_event = result["snapshot"]["same_underlying_conflicts"]["events"]["latest_event"]
    assert latest_event["event_type"] == "conflict_acknowledged"
    assert latest_event["local_operator_identity"] == "local_touch_id_operator"
    assert latest_event["auth_method"] == "TOUCH_ID"
    assert latest_event["authenticated_at"] == "2026-03-23T15:10:00+00:00"
    assert latest_event["auth_session_id"] == "auth-session-1"
    assert latest_event["operator_authenticated"] is True
    assert latest_event["requested_operator_label"] == "desk-op-note"

    history_rows = (
        tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_history.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    latest_history = json.loads(history_rows[-1])
    assert latest_history["local_operator_identity"] == "local_touch_id_operator"
    assert latest_history["auth_method"] == "TOUCH_ID"
    assert latest_history["authenticated_at"] == "2026-03-23T15:10:00+00:00"


def test_same_underlying_conflict_hold_updates_payloads_and_blocks_state(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_hold.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_hold.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-hold-entries",
        {"instrument": "GC", "operator_label": "desk-op", "reason": "Do not open fresh GC overlap until reviewed."},
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["hold_new_entries"] is True
    assert conflict["entry_hold_effective"] is True
    assert conflict["hold_reason"] == "Do not open fresh GC overlap until reviewed."
    assert conflict["review_state_status"] == "HOLDING"
    audit_rows = [row for row in result["snapshot"]["paper"]["signal_intent_fill_audit"]["rows"] if row["instrument"] == "GC"]
    assert all(row["same_underlying_hold_new_entries"] is True for row in audit_rows)
    assert all(row["same_underlying_entry_block_effective"] is True for row in audit_rows)
    events = result["snapshot"]["same_underlying_conflicts"]["events"]["rows"]
    assert events[0]["event_type"] == "conflict_hold_set"


def test_same_underlying_conflict_hold_expiry_is_enforced_and_preserved(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_expire.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_expire.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    service.run_action(
        "same-underlying-hold-entries",
        {
            "instrument": "GC",
            "operator_label": "desk-op",
            "reason": "Temporary hold through review window.",
            "hold_expires_at": "2026-03-20T12:00:00+00:00",
        },
    )
    snapshot = service.snapshot()

    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["hold_new_entries"] is False
    assert conflict["hold_expired"] is True
    assert conflict["hold_expiry_enforced"] is True
    assert conflict["entry_hold_effective"] is False
    assert conflict["review_state_status"] == "HOLD_EXPIRED"
    assert "expired" in str(conflict["hold_state_reason"]).lower()
    assert snapshot["same_underlying_conflicts"]["summary"]["hold_expired_count"] == 1
    assert snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_hold_expired"

    persisted = json.loads(
        (tmp_path / "outputs" / "operator_dashboard" / "same_underlying_conflict_review_state.json").read_text(encoding="utf-8")
    )
    assert persisted["records"]["GC"]["hold_expired"] is True


def test_same_underlying_conflict_material_change_auto_reopens_review(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_reopen.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_reopen.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})

    _append_dashboard_intent(
        gc_bull_db,
        order_intent_id="gc-bull-intent-overlap",
        bar_id="gc-bull-bar-overlap",
        symbol="GC",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T10:05:00-04:00",
        reason_code="bullSnap",
        broker_order_id="gc-bull-broker-overlap",
        order_status="WORKING",
    )
    _append_dashboard_intent(
        gc_bear_db,
        order_intent_id="gc-bear-intent-overlap",
        bar_id="gc-bear-bar-overlap",
        symbol="GC",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T10:06:00-04:00",
        reason_code="bearSnap",
        broker_order_id="gc-bear-broker-overlap",
        order_status="WORKING",
    )

    snapshot = service.snapshot()
    conflict = snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["review_state_status"] == "STALE"
    assert conflict["auto_reopen_required"] is True
    assert "pending-order overlap" in str(conflict["reopened_reason"])
    assert snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_auto_reopened"


def test_same_underlying_conflict_strategy_identity_churn_stays_acknowledged(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_identity.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_identity.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    baseline_snapshot = service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})["snapshot"]
    baseline_ids = list(baseline_snapshot["same_underlying_conflicts"]["rows"][0]["standalone_strategy_ids"])
    for path in (tmp_path / "shadow.sqlite3", tmp_path / "paper.sqlite3"):
        if path.exists():
            path.unlink()

    churned_snapshot = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnapRefined"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    ).snapshot()

    conflict = churned_snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["standalone_strategy_ids"] != baseline_ids
    assert conflict["review_state_status"] == "ACKNOWLEDGED"
    assert conflict["auto_reopen_required"] is False
    assert conflict["reopened_reason"] is None
    assert churned_snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_acknowledged"


def test_same_underlying_conflict_kind_churn_without_exposure_stays_acknowledged(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_kind.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_kind.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    service.run_action("same-underlying-acknowledge", {"instrument": "GC", "operator_label": "desk-op"})
    for path in (tmp_path / "shadow.sqlite3", tmp_path / "paper.sqlite3"):
        if path.exists():
            path.unlink()

    changed_snapshot = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    ).snapshot()

    conflict = changed_snapshot["same_underlying_conflicts"]["rows"][0]
    assert conflict["conflict_kind"] == "multiple_eligible_same_instrument"
    assert conflict["severity"] == "INFO"
    assert conflict["review_state_status"] == "ACKNOWLEDGED"
    assert conflict["auto_reopen_required"] is False
    assert conflict["reopened_reason"] is None
    assert changed_snapshot["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_acknowledged"


def test_same_underlying_conflict_observational_override_is_surfaced(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_override.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_override.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )

    result = service.run_action(
        "same-underlying-mark-observational",
        {"instrument": "GC", "operator_label": "desk-op", "override_reason": "Shared GC watch only; no exposure yet."},
    )

    conflict = result["snapshot"]["same_underlying_conflicts"]["rows"][0]
    assert conflict["override_observational_only"] is True
    assert conflict["override_reason"] == "Shared GC watch only; no exposure yet."
    assert conflict["review_state_status"] == "OVERRIDDEN"
    assert result["snapshot"]["same_underlying_conflicts"]["events"]["latest_event"]["event_type"] == "conflict_marked_observational_only"


def test_same_underlying_conflict_runtime_entry_block_event_is_surfaced(tmp_path: Path) -> None:
    gc_bull_db = tmp_path / "gc_bull_block.sqlite3"
    gc_bear_db = tmp_path / "gc_bear_block.sqlite3"
    _init_empty_dashboard_db(gc_bull_db)
    _init_empty_dashboard_db(gc_bear_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_bull_lane",
                "display_name": "GC Bull",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bull_db}",
            },
            {
                "lane_id": "gc_bear_lane",
                "display_name": "GC Bear",
                "symbol": "GC",
                "approved_long_entry_sources": [],
                "approved_short_entry_sources": ["bearSnap"],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_bear_db}",
            },
        ],
    )
    paper_controls_path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_controls.jsonl"
    paper_controls_path.write_text(
        json.dumps(
            {
                "event_type": "entry_blocked_by_same_underlying_hold",
                "action": "same_underlying_entry_hold_blocked",
                "occurred_at": "2026-03-23T15:05:00+00:00",
                "instrument": "GC",
                "standalone_strategy_id": "gc_bull_lane__GC",
                "blocked_standalone_strategy_id": "gc_bull_lane__GC",
                "blocked_reason": "New entries held by operator for same-underlying conflict review on GC.",
                "entry_hold_effective": True,
                "review_state_status": "HOLDING",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot = service.snapshot()

    latest_block = snapshot["same_underlying_conflicts"]["events"]["latest_entry_blocked_event"]
    assert latest_block["event_type"] == "entry_blocked_by_same_underlying_hold"
    assert latest_block["blocked_standalone_strategy_id"] == "gc_bull_lane__GC"


def test_same_underlying_conflict_review_state_not_created_without_conflict(tmp_path: Path) -> None:
    gc_db = tmp_path / "gc_only.sqlite3"
    _init_empty_dashboard_db(gc_db)
    service = _same_underlying_service(
        tmp_path,
        lanes=[
            {
                "lane_id": "gc_lane",
                "display_name": "GC Only",
                "symbol": "GC",
                "approved_long_entry_sources": ["bullSnap"],
                "approved_short_entry_sources": [],
                "position_side": "FLAT",
                "strategy_status": "READY",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "risk_state": "OK",
                "database_url": f"sqlite:///{gc_db}",
            }
        ],
    )

    with pytest.raises(ValueError):
        service.run_action("same-underlying-acknowledge", {"instrument": "GC"})


def test_dashboard_health_payload_reports_ready(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=12345,
        started_at="2026-03-21T12:00:00+00:00",
        build_stamp="abc123def456",
        info_file=str(tmp_path / "dashboard.json"),
    )
    service._record_dashboard_probe(  # type: ignore[attr-defined]
        snapshot={
            "generated_at": "2026-03-21T12:00:05+00:00",
            "operator_surface": {"readiness": {"title": "Runtime / Readiness"}},
        },
        error=None,
    )

    payload = service.health_payload()

    assert payload["status"] == "ok"
    assert payload["ready"] is True
    assert payload["build_stamp"] == service._build_stamp
    assert payload["pid"] == 12345
    assert payload["checks"]["operator_surface_loadable"]["ok"] is True
    assert payload["checks"]["api_dashboard_responding"]["ok"] is True
    assert payload["endpoints"]["dashboard"] == "/api/dashboard"


def test_dashboard_health_payload_reports_degraded_when_snapshot_fails(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("snapshot failed"))  # type: ignore[attr-defined]

    payload = service.health_payload()

    assert payload["status"] == "degraded"
    assert payload["ready"] is False
    assert "snapshot failed" in payload["error"]
    assert payload["checks"]["operator_surface_loadable"]["ok"] is False
    assert payload["checks"]["api_dashboard_responding"]["ok"] is False


def test_research_daily_capture_payload_surfaces_latest_run_and_symbol_failures(tmp_path: Path) -> None:
    repo_root = tmp_path
    latest_dir = repo_root / "outputs" / "research" / "daily_capture"
    latest_dir.mkdir(parents=True, exist_ok=True)
    (latest_dir / "latest.json").write_text(
        json.dumps(
            {
                    "status": "partial_failure",
                    "capture_started_at": "2026-03-28T22:15:00+00:00",
                    "capture_completed_at": "2026-03-28T22:16:00+00:00",
                "attempted_symbols": ["MGC", "MES"],
                "succeeded_symbols": ["MGC"],
                "failed_symbols": [
                    {
                        "symbol": "MES",
                        "capture_class": "watched",
                        "timeframe": "5m",
                        "failure_code": "ValueError",
                        "failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    }
                ],
                "target_rows": [
                    {
                        "symbol": "MGC",
                            "capture_class": "research_universe",
                            "timeframe": "5m",
                            "status": "success",
                            "last_captured_bar_end_ts": "2026-03-28T22:10:00+00:00",
                        "failure_code": None,
                        "failure_detail": None,
                    },
                    {
                        "symbol": "MES",
                        "capture_class": "watched",
                        "timeframe": "5m",
                        "status": "failure",
                        "last_captured_bar_end_ts": None,
                        "failure_code": "ValueError",
                        "failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    },
                ],
                "target_count": 2,
                "success_count": 1,
                "failure_count": 1,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    engine = build_engine(f"sqlite:///{repo_root / 'mgc_v05l.replay.sqlite3'}")
    create_schema(engine)
    with engine.begin() as connection:
        connection.execute(
            research_capture_status_table.insert(),
            [
                {
                    "symbol": "MGC",
                    "timeframe": "5m",
                    "capture_class": "research_universe",
                    "data_source": "schwab_history",
                    "last_attempted_at": "2026-03-28T22:16:00+00:00",
                    "last_succeeded_at": "2026-03-28T22:16:00+00:00",
                    "last_bar_end_ts": "2026-03-28T22:10:00+00:00",
                    "last_status": "success",
                    "last_failure_code": None,
                    "last_failure_detail": None,
                    "last_capture_run_id": 1,
                },
                {
                    "symbol": "MES",
                    "timeframe": "5m",
                    "capture_class": "watched",
                    "data_source": "schwab_history",
                    "last_attempted_at": "2026-03-28T22:16:00+00:00",
                    "last_succeeded_at": None,
                    "last_bar_end_ts": None,
                    "last_status": "failure",
                    "last_failure_code": "ValueError",
                    "last_failure_detail": "No Schwab historical symbol mapping configured for MES.",
                    "last_capture_run_id": 2,
                },
            ],
        )

    service = OperatorDashboardService(repo_root)

    payload = service._research_daily_capture_payload(generated_at="2026-03-28T22:20:00+00:00")  # type: ignore[attr-defined]

    assert payload["run_status"] == "partial_failure"
    assert payload["freshness_state"] == "current"
    assert payload["attempted_symbols"] == ["MGC", "MES"]
    assert payload["succeeded_symbols"] == ["MGC"]
    assert payload["failed_symbols"][0]["symbol"] == "MES"
    assert {row["symbol"] for row in payload["status_rows"]} == {"MGC", "MES"}


def test_dashboard_bind_reports_conflicting_listener(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    port = 8790
    service = OperatorDashboardService(tmp_path)
    handler = _build_handler(service)

    def _raise_bind_error(*args: object, **kwargs: object) -> object:
        raise OSError("Address already in use")

    monkeypatch.setattr(operator_dashboard_module, "ThreadingHTTPServer", _raise_bind_error)
    monkeypatch.setattr(
        operator_dashboard_module,
        "_listening_process_details",
        lambda requested_port: {"pid": "4242", "command": "python", "listener": f"TCP 127.0.0.1:{requested_port}"},
    )

    with pytest.raises(OSError) as excinfo:
        _bind_dashboard_server("127.0.0.1", port, handler, allow_port_fallback=False)

    message = str(excinfo.value)
    assert f"127.0.0.1:{port}" in message
    assert "already in use" in message
    assert "PID" in message


def test_dashboard_bind_reports_permission_denied_truthfully(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    handler = _build_handler(service)

    def _raise_permission_error(*args: object, **kwargs: object) -> object:
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(operator_dashboard_module, "ThreadingHTTPServer", _raise_permission_error)

    with pytest.raises(OSError) as excinfo:
        _bind_dashboard_server("127.0.0.1", 8790, handler, allow_port_fallback=False)

    assert "permission was denied" in str(excinfo.value)


def test_dashboard_assets_use_operator_first_surface_and_preserve_legacy_surfaces() -> None:
    html = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.html").read_text(encoding="utf-8")
    js = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.js").read_text(encoding="utf-8")
    css = Path("src/mgc_v05l/app/dashboard_assets/operator_dashboard.css").read_text(encoding="utf-8")

    assert 'data-lane-section="' not in html
    assert "<h2>Runtime / Readiness</h2>" in html
    assert "<h2>Portfolio P&amp;L / Risk</h2>" in html
    assert "<h2>Instrument Rollup</h2>" in html
    assert "<h2>Current Active Positions</h2>" in html
    assert "<h2>Unified Active Lane / Instrument Surface</h2>" in html
    assert "<h2>Experimental Paper Strategy / Paper Only</h2>" in html
    assert "<h2>Unified Active Lane Table</h2>" not in html
    assert "<h2>Secondary Market Context</h2>" in html
    assert "<h2>Diagnostics / Evidence</h2>" in html
    assert 'class="panel diagnostics-shell secondary-panel"' in html
    assert 'class="diagnostics-toggle"' in html
    assert 'class="diagnostics-stack"' in html
    assert 'id="operator-readiness-cards"' in html
    assert 'id="operator-canary-cards"' in html
    assert 'id="temporary-paper-strategies-table"' in html
    assert 'id="tracked-paper-strategies-table"' in html
    assert 'id="tracked-paper-detail-name"' in html
    assert 'id="tracked-paper-detail-runtime-attached"' in html
    assert 'data-action="start-atp-companion-paper"' in html
    assert 'data-action="atp-companion-paper-flatten-and-halt"' in html
    assert "renderOperatorCanarySummary" in js
    assert "renderTemporaryPaperStrategies" in js
    assert "renderTrackedPaperStrategies" in js
    assert ".operator-canary-panel" in css
    assert 'id="operator-risk-cards"' in html
    assert 'id="operator-risk-notes"' in html
    assert 'id="operator-instrument-table"' in html
    assert 'id="operator-active-positions-table"' in html
    assert 'id="operator-universe-cards"' in html
    assert 'id="operator-lane-grid-summary"' in html
    assert 'id="operator-lane-grid-table"' in html
    assert 'id="operator-context-items"' in html
    assert html.count("operator-flow-table-wrap") == 3
    assert 'id="market-value-djia"' not in html
    assert 'id="treasury-summary-10y"' not in html
    assert "renderLaneRegistrySections(dashboard.lane_registry || {});" in js
    assert "renderOperatorSurface(dashboard.operator_surface || {});" in js
    assert "renderRuntimeBuildInfo(dashboard);" in js
    assert "function renderOperatorLaneGrid(rows)" in js
    assert "function renderOperatorInstrumentRollup(payload)" in js
    assert "function renderOperatorActivePositions(payload)" in js
    assert "function renderOperatorContext(payload)" in js
    assert "function contextStatusLevel(status)" in js
    assert "function horizonAvailable(horizon)" in js
    assert "operator-context-value" in js
    assert "operator-context-note" in js
    assert '"diagnostics diagnostics diagnostics diagnostics diagnostics"' in css
    assert ".diagnostics-shell { grid-area: diagnostics; }" in css
    assert ".diagnostics-stack {" in css
    assert ".operator-context-gap-list {" in css
    assert ".operator-flow-table-wrap {" in css
    assert "max-height: none;" in css
    assert ".operator-context-items {" in css
    assert "grid-template-columns: repeat(4, minmax(0, 1fr));" in css
    assert ".operator-context-value {" in css
    assert ".operator-context-reference {" in css
    assert 'id="runtime-build-stamp"' in html
    assert 'id="runtime-server-pid"' in html
    assert 'id="runtime-started-at"' in html
    assert 'id="runtime-snapshot-generated"' in html
    assert 'id="runtime-approved-quant-count"' in html
    assert 'id="runtime-admitted-paper-count"' in html
    assert 'id="runtime-temporary-paper-count"' in html
    assert 'id="runtime-registry-line"' in html
    assert 'id="performance-realized"' in html
    assert 'id="history-vs-prior"' in html
    assert 'id="branch-performance-table"' in html
    assert 'id="run-start-history-link"' in html
    assert "<h2>Historical Playback Test</h2>" in html
    assert 'id="historical-playback-table"' in html
    assert 'id="historical-playback-filter"' in html
    assert 'id="paper-ready-lane-eligibility-note"' in html
    assert 'id="paper-ready-lane-eligibility-table"' in html
    assert 'renderHistoricalPlayback(historical_playback || {});' in js
    assert 'function renderHistoricalPlayback(payload)' in js
    assert 'historical-playback-trigger-json-link' in html
    assert 'paper-ready-lane-eligibility-table' in js
    assert 'Current runtime session:' in js


def test_dashboard_snapshot_reads_real_artifacts(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    admitted_lanes = [
        {
            "lane_id": "mgc_us_late_pause_resume_long",
            "display_name": "MGC / usLatePauseResumeLongTurn",
            "symbol": "MGC",
            "session_restriction": "US_LATE",
            "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-500",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "symbol": "MGC",
            "session_restriction": "ASIA_EARLY",
            "approved_long_entry_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
            "entries_enabled": False,
            "operator_halt": True,
            "risk_state": "HALTED_DEGRADATION",
            "halt_reason": "lane_realized_loser_limit_per_session",
            "unblock_action": "Clear Risk Halts, then Resume Entries",
            "realized_losing_trades": 2,
            "catastrophic_open_loss_threshold": "-500",
            "position_side": "LONG",
            "broker_position_qty": 1,
            "internal_position_qty": 1,
            "entry_price": "100.0",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "mgc_asia_early_pause_resume_short",
            "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
            "symbol": "MGC",
            "session_restriction": "ASIA_EARLY",
            "approved_short_entry_sources": ["asiaEarlyPauseResumeShortTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-500",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "pl_us_late_pause_resume_long",
            "display_name": "PL / usLatePauseResumeLongTurn",
            "symbol": "PL",
            "session_restriction": "US_LATE",
            "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-1000",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
        {
            "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "symbol": "GC",
            "session_restriction": "ASIA_EARLY",
            "approved_long_entry_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
            "entries_enabled": False,
            "operator_halt": False,
            "risk_state": "OK",
            "halt_reason": None,
            "unblock_action": None,
            "realized_losing_trades": 0,
            "catastrophic_open_loss_threshold": "-750",
            "database_url": f"sqlite:///{paper_db}",
            "artifacts_dir": str(paper_artifacts),
        },
    ]
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-18T14:05:00-04:00",
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "entries_enabled": False,
                "operator_halt": True,
                "approved_long_entry_sources": [
                    "asiaEarlyNormalBreakoutRetestHoldTurn",
                    "usLatePauseResumeLongTurn",
                ],
                "approved_short_entry_sources": [
                    "asiaEarlyPauseResumeShortTurn",
                ],
                "desk_risk_state": "HALT_NEW_ENTRIES",
                "desk_risk_reason": "desk_halt_new_entries_loss",
                "desk_unblock_action": "Clear Risk Halts, then Resume Entries",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 1,
                    "broker_average_price": "100.0",
                },
                "lanes": admitted_lanes,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "branch_sources.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T14:00:00-04:00",
                        "source": "asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "decision": "allowed",
                    }
                ),
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T13:55:00-04:00",
                        "source": "usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "decision": "blocked",
                        "block_reason": "probationary_long_source_not_allowlisted",
                    }
                ),
                json.dumps(
                    {
                        "bar_end_ts": "2026-03-18T13:50:00-04:00",
                        "source": "asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "decision": "allowed",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "alerts.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "rule_blocks.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-18T13:55:00-04:00",
                "source": "usLatePauseResumeLongTurn",
                "symbol": "MGC",
                "lane_id": "mgc_us_late_pause_resume_long",
                "block_reason": "daily_pause_condition",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "operator_controls.jsonl").write_text(
        json.dumps(
            {
                "requested_at": "2026-03-18T14:09:00-04:00",
                "applied_at": "2026-03-18T14:09:05-04:00",
                "action": "halt_entries",
                "status": "applied",
                "message": "entries halted for paper runtime",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-18T14:10:00-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "25.0",
                "session_date": "2026-03-18",
                "closed_trade_count": 1,
                "fill_count": 1,
                "order_intent_count": 1,
                "allowed_branch_decisions_by_source": {"asiaEarlyNormalBreakoutRetestHoldTurn": 3},
                "blocked_branch_decisions_by_source": {"usLatePauseResumeLongTurn": 1},
                "fills_by_intent_type": {"BUY_TO_OPEN": 1, "SELL_TO_CLOSE": 1},
                "processed_bars_session": 55,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-18T14:05:00-04:00,2026-03-18T14:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,100.5,5.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_desk_risk_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "session_date": "2026-03-18",
                "desk_risk_state": "HALT_NEW_ENTRIES",
                "session_realized_pnl": "-1600",
                "session_unrealized_pnl": "5",
                "session_total_pnl": "-1595",
                "desk_halt_new_entries_loss": "-1500",
                "desk_flatten_and_halt_loss": "-2500",
                "trigger_reason": "desk_halt_new_entries_loss",
                "unblock_action_required": "Clear Risk Halts, then Resume Entries",
                "reconciliation_clean": True,
                "faulted": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "session_date": "2026-03-18",
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "HALTED_DEGRADATION",
                        "halt_reason": "lane_realized_loser_limit_per_session",
                        "unblock_action": "Clear Risk Halts, then Resume Entries",
                        "realized_losing_trades": 2,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "-40",
                        "session_unrealized_pnl": "5",
                        "session_total_pnl": "-35",
                    },
                    {
                        "lane_id": "mgc_asia_early_pause_resume_short",
                        "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-500",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "pl_us_late_pause_resume_long",
                        "display_name": "PL / usLatePauseResumeLongTurn",
                        "symbol": "PL",
                        "session_restriction": "US_LATE",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-1000",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                    {
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "session_restriction": "ASIA_EARLY",
                        "risk_state": "OK",
                        "halt_reason": None,
                        "unblock_action": None,
                        "realized_losing_trades": 0,
                        "catastrophic_open_loss_threshold": "-750",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "0",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "desk_halt_new_entries_loss": "-1500",
                "desk_flatten_and_halt_loss": "-2500",
                "lane_realized_loser_limit_per_session": 2,
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "mgc_asia_early_pause_resume_short",
                        "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                        "symbol": "MGC",
                        "session_restriction": "ASIA_EARLY",
                        "short_sources": ["asiaEarlyPauseResumeShortTurn"],
                        "catastrophic_open_loss": "-500",
                    },
                    {
                        "lane_id": "pl_us_late_pause_resume_long",
                        "display_name": "PL / usLatePauseResumeLongTurn",
                        "symbol": "PL",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                        "catastrophic_open_loss": "-1000",
                    },
                    {
                        "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                        "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                        "symbol": "GC",
                        "session_restriction": "ASIA_EARLY",
                        "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                        "catastrophic_open_loss": "-750",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "paper_risk_events.jsonl").write_text(
        json.dumps(
            {
                "logged_at": "2026-03-18T14:10:00-04:00",
                "lane_id": "DESK",
                "symbol": "DESK",
                "severity": "WATCH",
                "event_code": "DESK_HALT_NEW_ENTRIES_LOSS",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_source": "Direct Schwab /quotes cash-index symbols.",
        "feed_state": "LIVE",
        "feed_label": "INDEX FEED LIVE",
        "updated_at": "2026-03-18T14:10:00-04:00",
        "age_seconds": 0,
        "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        "note": "Direct quote fetch.",
        "diagnostics": {"fetch_state": "SUCCESS", "symbols": []},
        "symbols": [
            {
                "label": "DJIA",
                "name": "Dow Jones",
                "external_symbol": "$DJI",
                "display_symbol": "$DJI",
                "source_type": "cash_index",
                "current_value": "39000.0",
                "absolute_change": "100.0",
                "percent_change": "0.26%",
                "bid": None,
                "ask": None,
                "state": "LIVE",
                "value_state": "LIVE",
                "bid_ask_state": "UNAVAILABLE",
                "bid_state": "UNAVAILABLE",
                "ask_state": "UNAVAILABLE",
                "field_states": {},
                "diagnostic_codes": ["BID_UNAVAILABLE", "ASK_UNAVAILABLE"],
                "note": "Bid/ask unavailable from current payload.",
            },
            {
                "label": "SPX",
                "name": "S&P 500",
                "external_symbol": "$SPX",
                "display_symbol": "$SPX",
                "source_type": "cash_index",
                "current_value": "5100.0",
                "absolute_change": "-10.0",
                "percent_change": "-0.20%",
                "bid": None,
                "ask": None,
                "state": "LIVE",
                "value_state": "LIVE",
                "bid_ask_state": "UNAVAILABLE",
                "bid_state": "UNAVAILABLE",
                "ask_state": "UNAVAILABLE",
                "field_states": {},
                "diagnostic_codes": ["BID_UNAVAILABLE", "ASK_UNAVAILABLE"],
                "note": "Bid/ask unavailable from current payload.",
            },
        ],
    }

    history_dir = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews"
    history_dir.mkdir(parents=True, exist_ok=True)
    prior_close_review = {
        "generated_at": "2026-03-17T21:00:00+00:00",
        "session_date": "2026-03-17",
        "desk_close_verdict": "CLEAN_WITH_ACTIVITY",
        "review_required_lanes": [
            "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        ],
        "rows": [
            {
                "branch": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "evidence_chain_status": "PARTIAL",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "FILLED_AND_FLAT",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_LOW",
                "attribution_gap_reason": [
                    "FAMILY_TAGGED_BLOTTER_ONLY",
                    "MULTI_LANE_SAME_FAMILY_AMBIGUITY",
                ],
            },
            {
                "branch": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "evidence_chain_status": "BROKEN",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "SIGNAL_NO_FILL",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": ["INSUFFICIENT_PERSISTED_EVIDENCE"],
            },
            {
                "branch": "PL / usLatePauseResumeLongTurn",
                "evidence_chain_status": "COMPLETE",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "IDLE",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": [],
            },
            {
                "branch": "MGC / asiaEarlyPauseResumeShortTurn",
                "evidence_chain_status": "COMPLETE",
                "realized_pnl_attribution_status": "UNATTRIBUTABLE",
                "session_verdict": "IDLE",
                "open_position": False,
                "review_confidence": "REVIEW_TRUST_HIGH",
                "attribution_gap_reason": [],
            },
        ],
    }
    (history_dir / "2026-03-17_2026-03-17T21-00-00p00-00.json").write_text(
        json.dumps(prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17_2026-03-17T21-00-00p00-00.md").write_text(
        "# prior close review\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17.json").write_text(
        json.dumps(prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17.md").write_text(
        "# prior close review canonical\n",
        encoding="utf-8",
    )
    duplicated_prior_close_review = dict(prior_close_review)
    duplicated_prior_close_review["generated_at"] = "2026-03-17T22:00:00+00:00"
    (history_dir / "2026-03-17_2026-03-17T22-00-00p00-00.json").write_text(
        json.dumps(duplicated_prior_close_review) + "\n",
        encoding="utf-8",
    )
    (history_dir / "2026-03-17_2026-03-17T22-00-00p00-00.md").write_text(
        "# duplicate prior close review\n",
        encoding="utf-8",
    )
    historical_playback_dir = repo_root / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_test.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_test.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_test.trigger_report.md"
    historical_strategy_study_path = historical_playback_dir / "historical_playback_mgc_test.strategy_study.json"
    historical_strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_test.strategy_study.md"
    historical_summary_path.write_text(
        json.dumps(
            {
                "symbol": "MGC",
                "processed_bars": 1429,
                "run_stamp": "test",
                "primary_standalone_strategy_id": "legacy_runtime__MGC",
                "per_strategy_summaries": [
                    {
                        "standalone_strategy_id": "legacy_runtime__MGC",
                        "strategy_family": "legacy_runtime",
                        "instrument": "MGC",
                        "processed_bars": 1429,
                        "order_intents": 2,
                        "fills": 2,
                        "entries": 1,
                        "exits": 1,
                        "long_entries": 1,
                        "short_entries": 0,
                        "final_position_side": "FLAT",
                        "final_strategy_status": "READY",
                        "realized_pnl": "25.0",
                        "unrealized_pnl": "0",
                        "cumulative_pnl": "25.0",
                        "pnl_unavailable_reason": None,
                    }
                ],
                "aggregate_portfolio_summary": {
                    "standalone_strategy_count": 1,
                    "strategy_count": 1,
                    "standalone_strategy_ids": ["legacy_runtime__MGC"],
                    "processed_bars": 1429,
                    "order_intents": 2,
                    "fills": 2,
                    "entries": 1,
                    "exits": 1,
                    "long_entries": 1,
                    "short_entries": 0,
                    "realized_pnl": "25.0",
                    "unrealized_pnl": "0",
                    "cumulative_pnl": "25.0",
                    "pnl_unavailable_reason": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "side": "LONG",
                    "bars_processed": 1429,
                    "signals_seen": 2,
                    "intents_created": 2,
                    "fills_created": 2,
                    "first_trigger_timestamp": "2026-03-10T16:45:00-04:00",
                    "first_fill_timestamp": "2026-03-10T16:45:00-04:00",
                    "block_or_fault_reason": None,
                },
                {
                    "symbol": "MGC",
                    "lane_family": "asiaEarlyPauseResumeShortTurn",
                    "side": "SHORT",
                    "bars_processed": 1429,
                    "signals_seen": 0,
                    "intents_created": 0,
                    "fills_created": 0,
                    "first_trigger_timestamp": None,
                    "first_fill_timestamp": None,
                    "block_or_fault_reason": "no_trigger_seen",
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text(
        "# Historical Playback\n",
        encoding="utf-8",
    )
    historical_strategy_study_path.write_text(
        json.dumps(
            {
                "contract_version": "strategy_study_v1",
                "symbol": "MGC",
                "timeframe": "5m",
                "standalone_strategy_id": "legacy_runtime__MGC",
                "strategy_family": "legacy_runtime",
                "rows": [
                    {
                        "bar_id": "MGC|5m|2026-03-10T16:45:00-04:00",
                        "timestamp": "2026-03-10T16:45:00-04:00",
                        "start_timestamp": "2026-03-10T16:40:00-04:00",
                        "end_timestamp": "2026-03-10T16:45:00-04:00",
                        "open": "100.0",
                        "high": "101.0",
                        "low": "99.5",
                        "close": "100.5",
                        "session_vwap": "100.2",
                        "atr": "0.8",
                        "position_side": "FLAT",
                        "position_qty": 0,
                        "position_phase": None,
                        "strategy_status": "READY",
                        "transition_label": "bar_close",
                        "entry_marker": True,
                        "exit_marker": False,
                        "fill_marker": False,
                        "entry_markers": [{"kind": "intent", "reason_code": "usLatePauseResumeLongTurn"}],
                        "exit_markers": [],
                        "fill_markers": [],
                        "realized_pnl": "0",
                        "unrealized_pnl": "0",
                        "cumulative_realized_pnl": "0",
                        "cumulative_total_pnl": "0",
                        "current_bias_state": "LONG_BIAS",
                        "current_pullback_state": "NORMAL_PULLBACK",
                        "pullback_envelope_band": "STANDARD",
                        "pullback_depth_score": 0.82,
                        "pullback_violence_score": 0.44,
                        "entry_eligible": True,
                        "entry_blocked": False,
                        "blocker_code": None,
                        "legacy_entry_eligible": True,
                        "legacy_entry_blocked": False,
                        "legacy_blocker_code": None,
                        "latest_signal_side": "LONG",
                        "latest_signal_source": "usLatePauseResumeLongTurn",
                        "latest_signal_state": "LONG_INTENT_CREATED",
                        "legacy_latest_signal_side": "LONG",
                        "legacy_latest_signal_source": "usLatePauseResumeLongTurn",
                        "legacy_latest_signal_state": "LONG_INTENT_CREATED",
                        "continuation_state": "CONTINUATION_TRIGGER_CONFIRMED",
                        "atp_entry_state": "ENTRY_ELIGIBLE",
                        "atp_entry_ready": True,
                        "atp_entry_blocked": False,
                        "atp_entry_blocker_code": None,
                        "atp_timing_state": "ATP_TIMING_CONFIRMED",
                        "atp_timing_confirmed": True,
                        "atp_timing_executable": True,
                        "atp_timing_blocker_code": None,
                        "atp_blocker_code": None,
                        "atp_timing_bar_timestamp": "2026-03-10T16:45:00-04:00",
                        "vwap_entry_quality_state": "VWAP_FAVORABLE",
                        "entry_source_family": "usLatePauseResumeLongTurn",
                    },
                    {
                        "bar_id": "MGC|5m|2026-03-10T16:50:00-04:00",
                        "timestamp": "2026-03-10T16:50:00-04:00",
                        "start_timestamp": "2026-03-10T16:45:00-04:00",
                        "end_timestamp": "2026-03-10T16:50:00-04:00",
                        "open": "100.5",
                        "high": "101.5",
                        "low": "100.2",
                        "close": "101.1",
                        "session_vwap": "100.6",
                        "atr": "0.8",
                        "position_side": "LONG",
                        "position_qty": 1,
                        "position_phase": None,
                        "strategy_status": "READY",
                        "transition_label": "bar_close",
                        "entry_marker": False,
                        "exit_marker": False,
                        "fill_marker": True,
                        "entry_markers": [],
                        "exit_markers": [],
                        "fill_markers": [{"kind": "fill", "is_entry": True, "is_exit": False}],
                        "realized_pnl": "0",
                        "unrealized_pnl": "6.0",
                        "cumulative_realized_pnl": "0",
                        "cumulative_total_pnl": "6.0",
                        "current_bias_state": "LONG_BIAS",
                        "current_pullback_state": "NO_PULLBACK",
                        "pullback_envelope_band": "SHALLOW",
                        "pullback_depth_score": 0.0,
                        "pullback_violence_score": 0.0,
                        "entry_eligible": False,
                        "entry_blocked": False,
                        "blocker_code": None,
                        "legacy_entry_eligible": False,
                        "legacy_entry_blocked": False,
                        "legacy_blocker_code": None,
                        "latest_signal_side": None,
                        "latest_signal_source": None,
                        "latest_signal_state": "NO_SIGNAL",
                        "legacy_latest_signal_side": None,
                        "legacy_latest_signal_source": None,
                        "legacy_latest_signal_state": "NO_SIGNAL",
                        "continuation_state": "CONTINUATION_TRIGGER_UNAVAILABLE",
                        "atp_entry_state": "ENTRY_BLOCKED",
                        "atp_entry_ready": False,
                        "atp_entry_blocked": True,
                        "atp_entry_blocker_code": "ATP_NO_PULLBACK",
                        "atp_timing_state": None,
                        "atp_timing_confirmed": None,
                        "atp_timing_executable": None,
                        "atp_timing_blocker_code": None,
                        "atp_blocker_code": "ATP_NO_PULLBACK",
                        "atp_timing_bar_timestamp": None,
                        "vwap_entry_quality_state": None,
                        "entry_source_family": "usLatePauseResumeLongTurn",
                    },
                ],
                "summary": {
                    "bar_count": 2,
                    "total_trades": 1,
                    "long_trades": 1,
                    "short_trades": 0,
                    "winners": 1,
                    "losers": 0,
                    "cumulative_realized_pnl": "25.0",
                    "cumulative_total_pnl": "25.0",
                    "max_run_up": "25.0",
                    "max_drawdown": "0",
                    "most_common_blocker_codes": [],
                    "most_common_legacy_blocker_codes": [],
                    "no_trade_regions": [],
                    "session_level_behavior": [],
                    "atp_summary": {
                        "available": True,
                        "timing_available": True,
                        "bar_count": 2,
                        "ready_bar_count": 1,
                        "bias_state_percent": {"LONG_BIAS": 100.0},
                        "pullback_state_percent": {"NORMAL_PULLBACK": 50.0, "NO_PULLBACK": 50.0},
                        "entry_state_percent": {"ENTRY_BLOCKED": 50.0, "ENTRY_ELIGIBLE": 50.0},
                        "continuation_state_percent": {"CONTINUATION_TRIGGER_CONFIRMED": 50.0, "CONTINUATION_TRIGGER_UNAVAILABLE": 50.0},
                        "timing_state_percent": {"ATP_TIMING_CONFIRMED": 100.0},
                        "vwap_entry_quality_state_percent": {"VWAP_FAVORABLE": 100.0},
                        "ready_to_timing_confirmed_percent": 100.0,
                        "timing_confirmed_to_executed_percent": 100.0,
                        "ready_to_executed_percent": 100.0,
                        "top_atp_blocker_codes": [{"code": "ATP_NO_PULLBACK", "count": 1}],
                        "top_no_trade_reasons": [{"code": "ATP_NO_PULLBACK", "count": 1}],
                    },
                    "pnl_supportable": True,
                    "pnl_unavailable_reason": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_strategy_study_md_path.write_text(
        "# Strategy Study\n",
        encoding="utf-8",
    )
    (historical_playback_dir / "historical_playback_test.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "test",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 1429,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                        "strategy_study_json_path": str(historical_strategy_study_path),
                        "strategy_study_markdown_path": str(historical_strategy_study_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot = service.snapshot()

    assert snapshot["global"]["mode"] == "IDLE"
    assert snapshot["paper"]["status"]["health_status"] == "HEALTHY"
    assert snapshot["paper"]["position"]["average_price"] == "100.0"
    assert snapshot["paper"]["position"]["realized_pnl"] == "25.0"
    assert snapshot["paper"]["latest_blotter_rows"][0]["setup_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["summary_available"] is True
    assert snapshot["paper"]["readiness"]["runtime_running"] is False
    assert snapshot["paper"]["readiness"]["runtime_phase"] == "STOPPED"
    assert snapshot["paper"]["readiness"]["entries_enabled"] is False
    assert snapshot["paper"]["readiness"]["approved_models_active"] == 5
    assert snapshot["paper"]["readiness"]["approved_models_total"] == 5
    assert "PL / usLatePauseResumeLongTurn / US_LATE" in snapshot["paper"]["readiness"]["instrument_scope"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn / ASIA_EARLY" in snapshot["paper"]["readiness"]["instrument_scope"]
    assert snapshot["paper"]["readiness"]["desk_risk_state"] == "HALT_NEW_ENTRIES"
    assert snapshot["paper"]["readiness"]["desk_risk_reason"] == "desk_halt_new_entries_loss"
    assert snapshot["paper"]["readiness"]["desk_unblock_action"] == "Clear Risk Halts, then Resume Entries"
    assert snapshot["paper"]["readiness"]["session_total_pnl"] == "-1595"
    lane_risk_rows = {
        row["lane_id"]: row
        for row in snapshot["paper"]["readiness"]["lane_risk_rows"]
    }
    assert lane_risk_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["risk_state"] == "HALTED_DEGRADATION"
    assert lane_risk_rows["pl_us_late_pause_resume_long"]["risk_state"] == "OK"
    assert lane_risk_rows["gc_asia_early_normal_breakout_retest_hold_long"]["risk_state"] == "OK"
    assert snapshot["paper"]["readiness"]["latest_paper_fill_timestamp"] == "2026-03-18T14:05:00-04:00"
    approved_rows = {
        row["branch"]: row
        for row in snapshot["paper"]["approved_models"]["rows"]
    }
    assert snapshot["paper"]["approved_models"]["enabled_count"] == 5
    assert snapshot["paper"]["approved_models"]["total_count"] == 5
    assert snapshot["paper"]["approved_models"]["instrument_scope"] == "5 admitted lanes / multi-lane paper mode"
    assert set(approved_rows) == {
        "MGC / usLatePauseResumeLongTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyPauseResumeShortTurn",
        "PL / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    assert approved_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["last_intent"] == "2026-03-18T14:00:00-04:00"
    assert approved_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "FILLED_OPEN"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["state"] == "ENABLED"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["chain_state"] == "BLOCKED"
    assert approved_rows["MGC / usLatePauseResumeLongTurn"]["decision_count"] == 1
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["state"] == "ENABLED"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["instrument"] == "PL"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["session_restriction"] == "US_LATE"
    assert approved_rows["PL / usLatePauseResumeLongTurn"]["chain_state"] == "NO_SIGNAL"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["state"] == "ENABLED"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["instrument"] == "GC"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_restriction"] == "ASIA_EARLY"
    assert approved_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "DECISION_WITHOUT_INTENT"
    assert approved_rows["MGC / asiaEarlyPauseResumeShortTurn"]["state"] == "ENABLED"
    assert snapshot["paper"]["approved_models"]["default_branch"] == "MGC / asiaEarlyNormalBreakoutRetestHoldTurn"
    asia_detail = snapshot["paper"]["approved_models"]["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert asia_detail["open_position"] is True
    assert asia_detail["chain_state"] == "FILLED_OPEN"
    assert asia_detail["unrealized_pnl"] == "5"
    assert asia_detail["latest_activity_type"] == "FILL"
    assert asia_detail["event_trail"][0]["category"] in {"position", "control", "trade", "fill", "intent", "signal"}
    blocked_detail = snapshot["paper"]["approved_models"]["details_by_branch"]["MGC / usLatePauseResumeLongTurn"]
    assert blocked_detail["chain_state"] == "BLOCKED"
    assert blocked_detail["latest_activity_type"] == "BLOCK"
    assert blocked_detail["latest_blocked_reason"] in {"daily_pause_condition", "probationary_long_source_not_allowlisted"}
    assert snapshot["paper"]["approved_models"]["details_by_branch"]["PL / usLatePauseResumeLongTurn"]["chain_state"] == "NO_SIGNAL"
    assert snapshot["paper"]["approved_models"]["details_by_branch"]["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "DECISION_WITHOUT_INTENT"
    assert snapshot["paper"]["approved_models"]["out_of_scope_blocked_count"] == 0
    assert snapshot["paper"]["activity_proof"]["verdict"] == "PAPER DESK NOT ACTUALLY RUNNING / NOT POLLING"
    assert snapshot["paper"]["activity_proof"]["session_summary"]["approved_models_seen_count"] == 3
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_signals_count"] == 2
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_blocked_count"] == 1
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_decisions_count"] == 3
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_intents_count"] == 1
    assert snapshot["paper"]["activity_proof"]["session_summary"]["total_fills_count"] == 1
    lane_activity_rows = {
        row["branch"]: row
        for row in snapshot["paper"]["lane_activity"]["rows"]
    }
    operator_surface_rows = snapshot["operator_surface"]["lane_rows"]
    assert any(row["classification_tag"] == "admitted_paper" for row in operator_surface_rows)
    assert snapshot["operator_surface"]["readiness"]["title"] == "Runtime / Readiness"
    assert snapshot["operator_surface"]["daily_risk"]["title"] == "Daily Risk / Performance"
    assert snapshot["operator_surface"]["lane_universe"]["title"] == "Unified Active Lane / Instrument Surface"
    assert snapshot["operator_surface"]["context"]["title"] == "Secondary Context"
    assert snapshot["paper"]["lane_activity"]["summary"]["any_activity_today"] is True
    assert snapshot["paper"]["lane_activity"]["summary"]["idle_only_count"] == 2
    assert snapshot["paper"]["lane_activity"]["summary"]["blocked_count"] == 1
    assert snapshot["paper"]["lane_activity"]["summary"]["filled_count"] == 1
    assert snapshot["paper"]["lane_activity"]["summary"]["open_now_count"] == 1
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["verdict"] == "HALTED_BY_RISK"
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["filled"] is True
    assert lane_activity_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert lane_activity_rows["MGC / usLatePauseResumeLongTurn"]["verdict"] == "BLOCKED"
    assert lane_activity_rows["MGC / usLatePauseResumeLongTurn"]["blocked"] is True
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["verdict"] == "NO_ACTIVITY_YET"
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["filled"] is False
    assert lane_activity_rows["PL / usLatePauseResumeLongTurn"]["blocked"] is False
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["verdict"] == "SIGNAL_ONLY"
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["filled"] is False
    assert lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["has_signal_or_decision"] is True
    assert "branch_sources.jsonl" in lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["used_sources"]
    assert "fills" not in lane_activity_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["used_sources"]
    assert snapshot["paper"]["exceptions"]["session_verdict"] == "NEEDS_OPERATOR_REVIEW"
    assert snapshot["paper"]["exceptions"]["summary"]["owning_model"] == "MGC / asiaEarlyNormalBreakoutRetestHoldTurn"
    assert {row["code"] for row in snapshot["paper"]["exceptions"]["exceptions"]} >= {
        "OPEN_EXPOSURE_AFTER_RESTART_REQUIRES_REVIEW",
        "OPEN_EXPOSURE_WHILE_ENTRIES_HALTED",
    }
    assert snapshot["paper"]["entry_eligibility"]["verdict"] == "NOT ELIGIBLE: OPEN-RISK / REVIEW REQUIRED"
    assert snapshot["paper"]["entry_eligibility"]["clear_action"] == "Clear Risk Halts, then Resume Entries"
    assert snapshot["paper"]["entry_eligibility"]["approved_models_eligible_now"] is False
    assert any(
        row["label"] == "Runtime phase" and row["value"] == "STOPPED"
        for row in snapshot["paper"]["entry_eligibility"]["reasons"]
    )
    assert snapshot["paper"]["soak_session"]["models_signaled"] == [
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / usLatePauseResumeLongTurn",
    ]
    assert snapshot["paper"]["soak_session"]["models_filled"] == ["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert snapshot["paper"]["soak_session"]["models_open_now"] == ["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]
    assert snapshot["paper"]["soak_session"]["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    close_review = snapshot["paper_session_close_review"]
    assert close_review["desk_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert close_review["admitted_lanes_count"] == 5
    assert close_review["active_lanes_count"] == 3
    assert close_review["blocked_lanes_count"] == 1
    assert close_review["filled_lanes_count"] == 1
    assert close_review["open_lanes_count"] == 1
    assert close_review["total_attributable_realized_pnl"] == "0"
    assert close_review["realized_attribution_coverage"] == "0/5 lanes exact"
    assert close_review["desk_attribution_summary"]["exact_realized_attribution_count"] == 0
    assert close_review["desk_attribution_summary"]["partial_realized_attribution_count"] == 0
    assert close_review["desk_attribution_summary"]["unattributable_realized_attribution_count"] == 5
    assert close_review["desk_attribution_summary"]["exact_open_risk_ownership_count"] == 1
    assert close_review["desk_attribution_summary"]["ambiguous_open_risk_ownership_count"] == 0
    assert close_review["desk_attribution_summary"]["unattributed_realized_pnl_present"] is True
    assert close_review["desk_attribution_summary"]["desk_review_confidence"] == "LOW"
    assert close_review["desk_attribution_summary"]["desk_pnl_completeness"] == "PARTIAL"
    assert close_review["desk_attribution_summary"]["reliable_pnl_judgment_lanes"] == []
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["manual_pnl_inspection_lanes"]
    assert "PL / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert "MGC / asiaEarlyPauseResumeShortTurn" in close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["partial_evidence_chain_lanes"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["broken_evidence_chain_lanes"]
    assert "MGC / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["broken_evidence_chain_lanes"]
    assert close_review["desk_attribution_summary"]["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert close_review["desk_attribution_summary"]["desk_history_confidence"] == "MEDIUM"
    assert close_review["desk_attribution_summary"]["history_threshold_note"] == "Clean history judgment requires at least 3 prior archived close reviews per lane."
    assert "PL / usLatePauseResumeLongTurn" in close_review["desk_attribution_summary"]["lanes_with_insufficient_history"]
    assert close_review["desk_attribution_summary"]["lanes_with_sufficient_history"] == []
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_partial_chain_lanes"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_broken_chain_lanes"]
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["desk_attribution_summary"]["repeated_unattributable_realized_lanes"]
    assert close_review["desk_attribution_summary"]["top_attribution_gap_reasons"][0]["reason"] in {
        "FAMILY_TAGGED_BLOTTER_ONLY",
        "INSUFFICIENT_PERSISTED_EVIDENCE",
        "MULTI_LANE_SAME_FAMILY_AMBIGUITY",
    }
    assert close_review["history_summary"]["prior_reviews_count"] == 1
    assert close_review["history_summary"]["desk_history_confidence"] == "MEDIUM"
    assert close_review["history_summary"]["history_threshold_note"] == "Clean history judgment requires at least 3 prior archived close reviews per lane."
    assert "PL / usLatePauseResumeLongTurn" in close_review["history_summary"]["lanes_with_insufficient_history"]
    close_rows = {row["branch"]: row for row in close_review["rows"]}
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_verdict"] == "HALTED_BY_RISK"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_realized_pnl"] is None
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unattributed_realized_pnl_present"] is True
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unrealized_pnl_attribution_status"] == "EXACT"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_unrealized_pnl"] == "5"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_confidence"] == "LOW"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["review_confidence"] == "REVIEW_TRUST_LOW"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_chain_status"] == "PARTIAL"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_intents"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_fills"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["matching_position_rows"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["ambiguous_family_rows"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_first_recommendation"]["label"] == "Position"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sessions_found"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["minimum_history_threshold_for_clean_judgment"] == 3
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["partial_chain_close_count"] == 1
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_verdict"] == "WATCH_REPEAT_PARTIAL"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_confidence"] == "MEDIUM"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["last_partial_close_ts"] == "2026-03-17T21:00:00+00:00"
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_links"]["blotter"] == "/api/operator-artifact/paper-latest-blotter"
    assert "persisted current position" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["unrealized_attribution_evidence_summary"]
    assert "FAMILY_TAGGED_BLOTTER_ONLY" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_gap_reason"]
    assert "MULTI_LANE_SAME_FAMILY_AMBIGUITY" in close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attribution_gap_reason"]
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["session_verdict"] == "BLOCKED_ONLY"
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["evidence_chain_status"] == "BROKEN"
    assert "INSUFFICIENT_PERSISTED_EVIDENCE" in close_rows["MGC / usLatePauseResumeLongTurn"]["attribution_gap_reason"]
    assert close_rows["MGC / usLatePauseResumeLongTurn"]["open_first_recommendation"]["label"] == "Decisions"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["session_verdict"] == "SIGNAL_NO_FILL"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["attributable_realized_pnl"] is None
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_chain_status"] == "BROKEN"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["evidence_counts"]["missing_lane_links"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_first_recommendation"]["label"] == "Decisions"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sessions_found"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["broken_chain_close_count"] == 1
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_verdict"] == "WATCH_REPEAT_BROKEN"
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["repeat_review_confidence"] == "MEDIUM"
    assert "No realized P/L is attributable" in close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["realized_attribution_evidence_summary"]
    assert close_rows["PL / usLatePauseResumeLongTurn"]["session_verdict"] == "IDLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["realized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["unrealized_pnl_attribution_status"] == "UNATTRIBUTABLE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["review_confidence"] == "REVIEW_TRUST_HIGH"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["evidence_chain_status"] == "COMPLETE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["prior_close_reviews_found"] == 1
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_sessions_found"] == 1
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_sufficiency_status"] == "HISTORY_SPARSE"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["clean_history_judgment_allowed"] is False
    assert close_rows["PL / usLatePauseResumeLongTurn"]["repeat_review_verdict"] == "NO_REPEAT_ISSUE_SEEN"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["repeat_review_confidence"] == "LOW"
    assert close_rows["PL / usLatePauseResumeLongTurn"]["history_note"] == "No repeat issue seen yet, but history is still sparse (1/3 archived close reviews)."
    assert close_rows["GC / asiaEarlyNormalBreakoutRetestHoldTurn"]["fill_count"] == 0
    assert close_rows["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert "MGC / asiaEarlyNormalBreakoutRetestHoldTurn" in close_review["review_required_lanes"]
    assert snapshot["paper"]["performance"]["realized_pnl"] == "25.0"
    assert snapshot["paper"]["performance"]["fill_count"] == 1
    assert snapshot["paper"]["performance"]["session_metrics"]["processed_bars"] == 55
    assert snapshot["paper"]["performance"]["session_metrics"]["signals_generated"] == 3
    assert snapshot["paper"]["performance"]["branch_performance"][0]["branch"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["performance"]["recent_trades"][0]["source"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["session_shape"]["session_start"] == "2026-03-18T14:00:00-04:00"
    assert snapshot["paper"]["session_shape"]["intraday_high_pnl"] == "10.0"
    assert snapshot["paper"]["session_shape"]["intraday_low_pnl"] == "0"
    assert snapshot["paper"]["session_shape"]["shape_label"] == "Steady up"
    assert snapshot["paper"]["session_shape"]["close_location"] == "Closed near highs"
    assert snapshot["paper"]["session_shape"]["current_or_latest_pnl"] == "10.0"
    assert snapshot["paper"]["session_shape"]["path_points"][-1]["kind"] == "current_open_estimate"
    assert snapshot["paper"]["branch_session_contribution"]["top_contributor"]["branch"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert snapshot["paper"]["branch_session_contribution"]["rows"][0]["total_contribution"] == "10.0"
    assert snapshot["paper"]["branch_session_contribution"]["rows"][0]["timing_hint"] == "Late contributor"
    assert any(event["title"] == "Session Start" for event in snapshot["paper"]["session_event_timeline"]["events"])
    assert any(event["category"] == "branch" for event in snapshot["paper"]["session_event_timeline"]["events"])
    assert snapshot["market_context"]["feed_state"] == "LIVE"
    assert snapshot["market_context"]["symbols"][0]["label"] == "DJIA"
    assert snapshot["review"]["paper"]["links"]["json"] == "/api/summary/paper/json"
    assert snapshot["manual_controls"]["controls"][3]["action"] == "paper-halt-entries"
    assert snapshot["paper_operator_state"]["entries_enabled"] is False
    assert snapshot["paper_operator_state"]["flatten_state"] == "idle"
    assert snapshot["paper_closeout"]["summary_generated"] is True
    assert snapshot["paper_closeout"]["position_flat"] is False
    assert snapshot["paper_closeout"]["sign_off_available"] is False
    assert snapshot["paper_carry_forward"]["active"] is False
    assert snapshot["paper_pre_session_review"]["ready_for_run"] is True
    assert snapshot["paper_continuity"]["entries"][0]["kind"] == "prior_close"
    assert snapshot["action_log"] == []
    session_shape_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_shape_snapshot.json"
    assert session_shape_path.exists()
    written_shape = json.loads(session_shape_path.read_text(encoding="utf-8"))
    assert written_shape["shape_label"] == snapshot["paper"]["session_shape"]["shape_label"]
    assert service.operator_artifact_file("paper-session-shape")[0] == session_shape_path
    branch_contrib_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_branch_contribution_snapshot.json"
    assert branch_contrib_path.exists()
    written_branch_contrib = json.loads(branch_contrib_path.read_text(encoding="utf-8"))
    assert written_branch_contrib["top_contributor"]["branch"] == snapshot["paper"]["branch_session_contribution"]["top_contributor"]["branch"]
    assert service.operator_artifact_file("paper-session-branch-contribution")[0] == branch_contrib_path
    session_timeline_path = repo_root / "outputs" / "operator_dashboard" / "paper_session_event_timeline_snapshot.json"
    assert session_timeline_path.exists()
    written_timeline = json.loads(session_timeline_path.read_text(encoding="utf-8"))
    assert any(event["title"] == "Session Start" for event in written_timeline["events"])
    assert service.operator_artifact_file("paper-session-event-timeline")[0] == session_timeline_path
    market_index_path = repo_root / "outputs" / "operator_dashboard" / "market_index_strip_snapshot.json"
    assert market_index_path.exists()
    written_market_index = json.loads(market_index_path.read_text(encoding="utf-8"))
    assert written_market_index["feed_state"] == "LIVE"
    assert service.operator_artifact_file("market-index-strip")[0] == market_index_path
    market_index_diag_path = repo_root / "outputs" / "operator_dashboard" / "market_index_strip_diagnostics.json"
    assert market_index_diag_path.exists()
    assert service.operator_artifact_file("market-index-strip-diagnostics")[0] == market_index_diag_path
    paper_readiness_path = repo_root / "outputs" / "operator_dashboard" / "paper_readiness_snapshot.json"
    assert paper_readiness_path.exists()
    assert service.operator_artifact_file("paper-readiness")[0] == paper_readiness_path
    paper_approved_models_path = repo_root / "outputs" / "operator_dashboard" / "paper_approved_models_snapshot.json"
    assert paper_approved_models_path.exists()
    written_approved_models = json.loads(paper_approved_models_path.read_text(encoding="utf-8"))
    assert written_approved_models["enabled_count"] == 5
    assert written_approved_models["instrument_scope"] == "5 admitted lanes / multi-lane paper mode"
    assert written_approved_models["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["chain_state"] == "FILLED_OPEN"
    assert "PL / usLatePauseResumeLongTurn" in written_approved_models["details_by_branch"]
    assert "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in written_approved_models["details_by_branch"]
    assert service.operator_artifact_file("paper-approved-models")[0] == paper_approved_models_path
    paper_lane_activity_path = repo_root / "outputs" / "operator_dashboard" / "paper_lane_activity_snapshot.json"
    assert paper_lane_activity_path.exists()
    written_lane_activity = json.loads(paper_lane_activity_path.read_text(encoding="utf-8"))
    assert written_lane_activity["summary"]["blocked_count"] == 1
    assert written_lane_activity["summary"]["filled_count"] == 1
    assert service.operator_artifact_file("paper-lane-activity")[0] == paper_lane_activity_path
    paper_tracked_strategies_path = repo_root / "outputs" / "operator_dashboard" / "paper_tracked_strategies_snapshot.json"
    assert paper_tracked_strategies_path.exists()
    written_tracked = json.loads(paper_tracked_strategies_path.read_text(encoding="utf-8"))
    assert written_tracked["rows"][0]["strategy_id"] == "atp_companion_v1_asia_us"
    assert written_tracked["rows"][0]["internal_label"] == "ATP_COMPANION_V1_ASIA_US"
    assert service.operator_artifact_file("paper-tracked-strategies")[0] == paper_tracked_strategies_path
    paper_tracked_details_path = repo_root / "outputs" / "operator_dashboard" / "paper_tracked_strategy_details_snapshot.json"
    assert paper_tracked_details_path.exists()
    assert service.operator_artifact_file("paper-tracked-strategy-details")[0] == paper_tracked_details_path
    paper_exceptions_path = repo_root / "outputs" / "operator_dashboard" / "paper_exceptions_snapshot.json"
    assert paper_exceptions_path.exists()
    written_exceptions = json.loads(paper_exceptions_path.read_text(encoding="utf-8"))
    assert written_exceptions["session_verdict"] == "NEEDS_OPERATOR_REVIEW"
    assert service.operator_artifact_file("paper-exceptions")[0] == paper_exceptions_path
    paper_soak_session_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_session_snapshot.json"
    assert paper_soak_session_path.exists()
    written_soak_session = json.loads(paper_soak_session_path.read_text(encoding="utf-8"))
    assert written_soak_session["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    assert service.operator_artifact_file("paper-soak-session")[0] == paper_soak_session_path
    paper_close_review_latest_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_review_latest.json"
    paper_close_review_latest_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_review_latest.md"
    paper_close_review_archive_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "2026-03-18.json"
    paper_close_review_archive_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "2026-03-18.md"
    paper_close_review_history_json = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "history_index.json"
    paper_close_review_history_md = repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews" / "history_index.md"
    timestamped_close_review_archives = sorted((repo_root / "outputs" / "operator_dashboard" / "paper_session_close_reviews").glob("2026-03-18_*.json"))
    assert paper_close_review_latest_json.exists()
    assert paper_close_review_latest_md.exists()
    assert paper_close_review_archive_json.exists()
    assert paper_close_review_archive_md.exists()
    assert paper_close_review_history_json.exists()
    assert paper_close_review_history_md.exists()
    assert timestamped_close_review_archives
    written_close_review = json.loads(paper_close_review_latest_json.read_text(encoding="utf-8"))
    assert written_close_review["desk_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert written_close_review["desk_attribution_summary"]["desk_review_confidence"] == "LOW"
    assert written_close_review["desk_attribution_summary"]["desk_pnl_completeness"] == "PARTIAL"
    assert "PL / usLatePauseResumeLongTurn" in written_close_review["desk_attribution_summary"]["complete_evidence_chain_lanes"]
    assert written_close_review["desk_attribution_summary"]["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert written_close_review["desk_attribution_summary"]["desk_history_confidence"] == "MEDIUM"
    assert written_close_review["rows"][0]["branch"] in {
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    written_close_review_md = paper_close_review_latest_md.read_text(encoding="utf-8")
    assert "Desk review confidence: LOW" in written_close_review_md
    assert "Desk history confidence: MEDIUM" in written_close_review_md
    assert "history_sufficiency=HISTORY_SPARSE" in written_close_review_md
    assert "repeat_review_confidence=LOW" in written_close_review_md
    assert "Complete evidence chains:" in written_close_review_md
    assert "Broken evidence chains:" in written_close_review_md
    assert "gap_reason=FAMILY_TAGGED_BLOTTER_ONLY, MULTI_LANE_SAME_FAMILY_AMBIGUITY" in written_close_review_md
    assert "evidence_chain=PARTIAL" in written_close_review_md
    assert "open_first=Decisions" in written_close_review_md
    written_history = json.loads(paper_close_review_history_json.read_text(encoding="utf-8"))
    assert written_history["prior_reviews_count"] == 1
    assert written_history["historical_trust_verdict"] == "CLOSE_HISTORY_REVIEW_REQUIRED"
    assert service.operator_artifact_file("paper-session-close-review")[0] == paper_close_review_latest_json
    assert service.operator_artifact_file("paper-session-close-review-md")[0] == paper_close_review_latest_md
    assert service.operator_artifact_file("paper-session-close-review-history")[0] == paper_close_review_history_json
    assert service.operator_artifact_file("paper-session-close-review-history-md")[0] == paper_close_review_history_md
    paper_latest_intents_path = repo_root / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json"
    assert paper_latest_intents_path.exists()
    assert service.operator_artifact_file("paper-latest-intents")[0] == paper_latest_intents_path
    paper_latest_blotter_path = repo_root / "outputs" / "operator_dashboard" / "paper_latest_blotter_snapshot.json"
    assert paper_latest_blotter_path.exists()
    assert service.operator_artifact_file("paper-latest-blotter")[0] == paper_latest_blotter_path
    paper_position_state_path = repo_root / "outputs" / "operator_dashboard" / "paper_position_state_snapshot.json"
    assert paper_position_state_path.exists()
    assert service.operator_artifact_file("paper-position-state")[0] == paper_position_state_path
    historical_snapshot_path = repo_root / "outputs" / "operator_dashboard" / "historical_playback_snapshot.json"
    assert historical_snapshot_path.exists()
    historical_payload = snapshot["historical_playback"]["latest_run"]
    assert historical_payload["run_stamp"] == "test"
    assert historical_payload["bars_processed"] == 1429
    assert historical_payload["signals_seen"] == 2
    assert historical_payload["fills_created"] == 2
    assert historical_payload["rows"][0]["lane_family"] == "usLatePauseResumeLongTurn"
    assert historical_payload["rows"][0]["result_status"] == "FIRED"
    assert historical_payload["rows"][1]["result_status"] == "NO FIRE"
    assert historical_payload["truth_label"] == "REPLAY"
    assert historical_payload["replay_summary_available"] is True
    assert historical_payload["primary_standalone_strategy_id"] == "legacy_runtime__MGC"
    assert historical_payload["aggregate_portfolio_summary"]["standalone_strategy_count"] == 1
    assert historical_payload["per_strategy_summaries"][0]["standalone_strategy_id"] == "legacy_runtime__MGC"
    assert historical_payload["strategy_study_available"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["label"] == "Replay Strategy Study"
    assert snapshot["historical_playback"]["strategy_study_status"]["hint"] == (
        "Available after a replay/historical playback run with strategy-study artifacts."
    )
    assert snapshot["historical_playback"]["strategy_study_status"]["run_loaded"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["artifact_found"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["base_timeframe"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["structural_signal_timeframe"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["execution_resolution"] == "5m"
    assert snapshot["historical_playback"]["strategy_study_status"]["study_mode"] == "baseline_parity_mode"
    assert snapshot["historical_playback"]["strategy_study_status"]["atp_timing_available"] is True
    assert snapshot["historical_playback"]["strategy_study_status"]["mode"] == "ATP_ENHANCED"
    assert historical_payload["strategy_study_status"]["artifact_row_count"] == 2
    assert historical_payload["strategy_study"]["summary"]["bar_count"] == 2
    assert historical_payload["strategy_study"]["rows"][0]["entry_marker"] is True
    assert historical_payload["strategy_study"]["rows"][0]["current_bias_state"] == "LONG_BIAS"
    assert historical_payload["strategy_study"]["rows"][0]["atp_timing_state"] == "ATP_TIMING_CONFIRMED"
    assert historical_payload["strategy_study"]["meta"]["timeframe_truth"]["artifact_timeframe"] == "5m"
    assert historical_payload["strategy_study"]["summary"]["atp_summary"]["available"] is True
    assert historical_payload["strategy_study"]["summary"]["atp_summary"]["top_atp_blocker_codes"][0]["code"] == "ATP_NO_PULLBACK"
    assert service.operator_artifact_file("historical-playback-snapshot")[0] == historical_snapshot_path
    assert service.operator_artifact_file("historical-playback-manifest")[0] == historical_playback_dir / "historical_playback_test.manifest.json"
    assert service.operator_artifact_file("historical-playback-summary")[0] == historical_summary_path
    assert service.operator_artifact_file("historical-playback-trigger-report")[0] == historical_trigger_report_path
    assert service.operator_artifact_file("historical-playback-trigger-report-md")[0] == historical_trigger_report_md_path
    assert service.operator_artifact_file("historical-playback-strategy-study")[0] == historical_strategy_study_path
    assert service.operator_artifact_file("historical-playback-strategy-study-md")[0] == historical_strategy_study_md_path

    result = service.run_action("capture-paper-soak-evidence")
    assert result["ok"] is True
    latest_json_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_evidence_latest.json"
    latest_md_path = repo_root / "outputs" / "operator_dashboard" / "paper_soak_evidence_latest.md"
    assert latest_json_path.exists()
    assert latest_md_path.exists()
    latest_bundle = json.loads(latest_json_path.read_text(encoding="utf-8"))
    assert latest_bundle["end_of_session_verdict"] == "FILLED_WITH_OPEN_RISK"
    assert latest_bundle["approved_models_snapshot"]["details_by_branch"]["MGC / asiaEarlyNormalBreakoutRetestHoldTurn"]["open_position"] is True
    assert service.operator_artifact_file("paper-soak-evidence-latest-json")[0] == latest_json_path

    service._risk_ack_path.write_text(  # type: ignore[attr-defined]
        json.dumps(
            {
                "risk_hash": snapshot["paper_risk_state"]["risk_hash"],
                "acknowledged_at": "2026-03-18T14:12:00-04:00",
                "reasons": snapshot["paper_risk_state"]["reasons"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    signoff_result = service.run_action("sign-off-paper-session")
    assert signoff_result["ok"] is True
    lane_history_dir = repo_root / "outputs" / "operator_dashboard" / "paper_session_lane_history"
    lane_history_paths = sorted(lane_history_dir.glob("2026-03-18_*.json"))
    assert len(lane_history_paths) == 1
    lane_history = json.loads(lane_history_paths[0].read_text(encoding="utf-8"))
    assert lane_history["session_close_verdict"] == "HALTED_WITH_OPEN_RISK"
    assert lane_history["admitted_lane_count"] == 5
    assert lane_history["active_lane_count"] == 3
    assert lane_history["filled_lane_count"] == 1
    assert lane_history["open_risk_lane_count"] == 1
    assert lane_history["dirty_close_lane_count"] == 1
    assert lane_history["manual_review_lane_count"] >= 1
    archived_lanes = {row["lane_id"]: row for row in lane_history["lanes"]}
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "MGC"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["fill"] is True
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["open_risk_at_close"] is True
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["clean_vs_dirty_close"] == "DIRTY"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "GC"
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["signal"] is True
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["fill"] is False
    assert archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["primary_gap_reason"] == "INSUFFICIENT_PERSISTED_EVIDENCE"
    assert archived_lanes["mgc_asia_early_normal_breakout_retest_hold_long"]["lane_id"] != archived_lanes["gc_asia_early_normal_breakout_retest_hold_long"]["lane_id"]
    assert len(
        [
            row
            for row in lane_history["lanes"]
            if row["source_family"] == "asiaEarlyNormalBreakoutRetestHoldTurn" and row["instrument"] in {"MGC", "GC"}
        ]
    ) == 2
    service._archive_paper_session_lane_history(  # type: ignore[attr-defined]
        snapshot=snapshot,
        signoff_payload=json.loads(service._session_signoff_path.read_text(encoding="utf-8")),  # type: ignore[attr-defined]
    )
    lane_history_paths = sorted(lane_history_dir.glob("2026-03-18_*.json"))
    assert len(lane_history_paths) == 2
    assert lane_history_paths[0].name != lane_history_paths[1].name


def test_dashboard_historical_playback_strategy_study_status_marks_missing_artifacts(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    historical_playback_dir = tmp_path / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_missing.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_missing.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_missing.trigger_report.md"
    historical_summary_path.write_text(
        json.dumps({"symbol": "MGC", "processed_bars": 12, "run_stamp": "missing-study"}) + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "bars_processed": 12,
                    "signals_seen": 0,
                    "intents_created": 0,
                    "fills_created": 0,
                    "block_or_fault_reason": "no_trigger_seen",
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text("# Historical Playback\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_missing.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "missing-study",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 12,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._historical_playback_payload()

    assert payload["available"] is True
    assert payload["strategy_study_status"]["run_loaded"] is True
    assert payload["strategy_study_status"]["artifact_found"] is False
    assert payload["strategy_study_status"]["artifact_row_count"] == 0
    assert payload["strategy_study_status"]["base_timeframe"] is None
    assert payload["strategy_study_status"]["atp_timing_available"] is False
    assert payload["strategy_study_status"]["mode"] == "NO_DATA"
    assert payload["latest_run"]["strategy_study_available"] is False
    assert payload["latest_run"]["strategy_study_status"]["mode"] == "NO_DATA"


def test_dashboard_historical_playback_payload_backfills_legacy_strategy_study_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = OperatorDashboardService(tmp_path)
    historical_playback_dir = tmp_path / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    historical_summary_path = historical_playback_dir / "historical_playback_mgc_backfill.summary.json"
    historical_trigger_report_path = historical_playback_dir / "historical_playback_mgc_backfill.trigger_report.json"
    historical_trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_backfill.trigger_report.md"
    historical_strategy_study_path = historical_playback_dir / "historical_playback_mgc_backfill.strategy_study.json"
    historical_strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_backfill.strategy_study.md"
    historical_summary_path.write_text(
        json.dumps(
            {
                "symbol": "MGC",
                "processed_bars": 12,
                "run_stamp": "backfill-study",
                "config_paths": ["config/base.yaml"],
                "source_db_path": str(tmp_path / "source.sqlite3"),
                "replay_db_path": str(tmp_path / "replay.sqlite3"),
                "source_timeframe": "5m",
                "target_timeframe": "5m",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "usLatePauseResumeLongTurn",
                    "bars_processed": 12,
                    "signals_seen": 1,
                    "intents_created": 1,
                    "fills_created": 1,
                    "block_or_fault_reason": None,
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    historical_trigger_report_md_path.write_text("# Historical Playback\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_backfill-study.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "backfill-study",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "processed_bars": 12,
                        "summary_path": str(historical_summary_path),
                        "trigger_report_json_path": str(historical_trigger_report_path),
                        "trigger_report_markdown_path": str(historical_trigger_report_md_path),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def _fake_backfill(*, summary_path: str | Path, summary_payload: dict[str, object] | None = None) -> tuple[Path, Path]:
        historical_strategy_study_path.write_text(
            json.dumps(
                {
                    "contract_version": "strategy_study_v2",
                    "symbol": "MGC",
                    "timeframe": "5m",
                    "rows": [
                        {
                            "bar_id": "bar-1",
                            "timestamp": "2026-03-18T14:05:00-04:00",
                            "entry_marker": True,
                        }
                    ],
                    "summary": {
                        "bar_count": 1,
                        "atp_summary": {"available": True, "timing_available": False},
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        historical_strategy_study_md_path.write_text("# Strategy Study\n", encoding="utf-8")
        return historical_strategy_study_path, historical_strategy_study_md_path

    monkeypatch.setattr(operator_dashboard_module, "ensure_strategy_study_artifacts", _fake_backfill)

    payload = service._historical_playback_payload()

    assert payload["latest_run"]["strategy_study_available"] is True
    assert payload["selected_study"]["contract_version"] == "strategy_study_v3"
    assert payload["study_catalog"]["selected_study_key"]
    assert payload["study_catalog"]["items"][0]["contract_version"] == "strategy_study_v3"
    assert payload["study_catalog"]["facets"]["symbols"] == ["MGC"]
    assert payload["study_catalog"]["facets"]["study_modes"] == ["baseline_parity_mode"]
    assert payload["study_catalog"]["facets"]["entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["facets"]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["facets"]["pnl_truth_bases"] == ["BASELINE_FILL_TRUTH"]
    assert payload["study_catalog"]["facets"]["lifecycle_truth_classes"] == ["BASELINE_PARITY_ONLY"]
    assert payload["study_catalog"]["items"][0]["scope_label"] == "Legacy Benchmark"
    assert payload["study_catalog"]["items"][0]["entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert payload["study_catalog"]["items"][0]["active_entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert payload["study_catalog"]["items"][0]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert payload["study_catalog"]["items"][0]["execution_truth_emitter"] == "baseline_parity_emitter"
    assert payload["study_catalog"]["items"][0]["entry_model_supported"] is True
    assert payload["study_catalog"]["items"][0]["intrabar_execution_authoritative"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_intrabar_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_entry_truth_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_exit_truth_available"] is False
    assert payload["study_catalog"]["items"][0]["authoritative_trade_lifecycle_available"] is False
    assert payload["study_catalog"]["items"][0]["pnl_truth_basis"] == "BASELINE_FILL_TRUTH"
    assert payload["study_catalog"]["items"][0]["lifecycle_truth_class"] == "BASELINE_PARITY_ONLY"
    assert payload["study_catalog"]["items"][0]["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert payload["latest_run"]["artifact_paths"]["strategy_study_json"] == str(historical_strategy_study_path)
    assert payload["latest_run"]["artifact_paths"]["strategy_study_markdown"] == str(historical_strategy_study_md_path)
    assert payload["latest_run"]["strategy_study"]["summary"]["bar_count"] == 1
    assert payload["latest_run"]["strategy_study_status"]["artifact_found"] is True
    assert payload["latest_run"]["strategy_study_status"]["mode"] == "ATP_ENHANCED"


def test_dashboard_paper_readiness_surfaces_lane_eligibility_rows_and_stale_override(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {
            "lanes": [
                {"lane_id": "mgc_us_late_pause_resume_long", "risk_state": "OK"},
                {"lane_id": "mgc_asia_early_normal_breakout_retest_hold_long", "risk_state": "OK"},
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "mgc_us_late_pause_resume_long",
                    "display_name": "MGC / usLatePauseResumeLongTurn",
                    "symbol": "MGC",
                    "session_restriction": "US_LATE",
                },
                {
                    "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                    "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                    "symbol": "MGC",
                    "session_restriction": "ASIA_EARLY",
                },
            ]
        },
        "raw_operator_status": {
            "current_detected_session": "ASIA_EARLY",
            "lanes": [
                {
                    "lane_id": "mgc_us_late_pause_resume_long",
                    "display_name": "MGC / usLatePauseResumeLongTurn",
                    "symbol": "MGC",
                    "session_restriction": "US_LATE",
                    "current_detected_session": "ASIA_EARLY",
                    "eligible_now": False,
                    "eligibility_reason": "wrong_session",
                },
                {
                    "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                    "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                    "symbol": "MGC",
                    "session_restriction": "ASIA_EARLY",
                    "current_detected_session": "ASIA_EARLY",
                    "eligible_now": True,
                    "eligibility_reason": None,
                },
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    rows = {row["lane_id"]: row for row in payload["lane_eligibility_rows"]}
    status_rows = {row["lane_id"]: row for row in payload["lane_status_rows"]}

    assert payload["current_detected_session"] == "ASIA_EARLY"
    assert rows["mgc_us_late_pause_resume_long"]["eligible_now"] is False
    assert rows["mgc_us_late_pause_resume_long"]["eligibility_reason"] == "wrong_session"
    assert rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is True
    assert rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] is None
    assert status_rows["mgc_us_late_pause_resume_long"]["loaded_in_runtime"] is True
    assert status_rows["mgc_us_late_pause_resume_long"]["eligible_to_trade"] is False
    assert status_rows["mgc_us_late_pause_resume_long"]["tradability_status"] == "LOADED_NOT_ELIGIBLE"
    assert status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_to_trade"] is True
    assert status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["tradability_status"] == "ELIGIBLE_TO_TRADE"
    assert payload["lane_status_summary"]["loaded_in_runtime_count"] == 2
    assert payload["lane_status_summary"]["eligible_to_trade_count"] == 1

    paper["status"]["stale"] = True
    stale_payload = service._paper_readiness_payload(paper)
    stale_rows = {row["lane_id"]: row for row in stale_payload["lane_eligibility_rows"]}
    stale_status_rows = {row["lane_id"]: row for row in stale_payload["lane_status_rows"]}

    assert stale_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligible_now"] is False
    assert stale_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["eligibility_reason"] == "stale_runtime"
    assert stale_status_rows["mgc_asia_early_normal_breakout_retest_hold_long"]["tradability_status"] == "LOADED_NOT_ELIGIBLE"


def test_dashboard_paper_readiness_treats_harmless_same_underlying_coexistence_as_informational_only(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "GC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "gc_lane_a", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "gc_lane_a",
                    "display_name": "GC Lane A",
                    "symbol": "GC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "same_underlying_ambiguity": True,
                    "position_side": "FLAT",
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    row = payload["lane_status_rows"][0]

    assert row["loaded_in_runtime"] is True
    assert row["eligible_to_trade"] is True
    assert row["informational_degradation_only"] is True
    assert row["tradability_status"] == "INFORMATIONAL_ONLY"
    assert row["manual_action_required"] is False


def test_dashboard_paper_readiness_surfaces_heartbeat_reconciliation_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "heartbeat_reconciliation": {
                        "status": "RECONCILING",
                        "classification": "unsafe_ambiguity",
                        "last_attempted_at": "2026-03-26T10:15:00+00:00",
                        "reason": "broker_position_quantity_mismatch",
                        "recommended_action": "Inspect reconciliation and wait for a clean/safe-repair result.",
                        "active_issue": True,
                        "cadence_seconds": 60,
                    },
                }
            ],
        },
        "status": {"entries_enabled": False, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["heartbeat_reconciliation_summary"]
    row = payload["lane_status_rows"][0]

    assert row["heartbeat_reconciliation_status"] == "RECONCILING"
    assert row["heartbeat_reconciliation_active_issue"] is True
    assert summary["last_status"] == "RECONCILING"
    assert summary["cadence_seconds"] == 60
    assert summary["active_issue_count"] == 1
    assert summary["active_issue_rows"][0]["lane_id"] == "mgc_lane"


def test_dashboard_paper_readiness_surfaces_order_timeout_watchdog_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "order_timeout_watchdog": {
                        "status": "ACTIVE_TIMEOUTS",
                        "last_checked_at": "2026-03-26T10:20:00+00:00",
                        "overdue_ack_count": 1,
                        "overdue_fill_count": 2,
                        "reason": "Pending-order timeouts are active.",
                        "recommended_action": "Wait for broker progression or reconciliation.",
                        "active_issue_count": 1,
                    },
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["order_timeout_watchdog_summary"]
    row = payload["lane_status_rows"][0]

    assert row["order_timeout_watchdog_status"] == "ACTIVE_TIMEOUTS"
    assert row["overdue_ack_count"] == 1
    assert row["overdue_fill_count"] == 2
    assert summary["last_status"] == "ACTIVE_TIMEOUTS"
    assert summary["overdue_ack_count"] == 1
    assert summary["overdue_fill_count"] == 2
    assert summary["active_issue_count"] == 1
    assert summary["active_issue_rows"][0]["reason"] == "Pending-order timeouts are active."


def test_dashboard_paper_readiness_surfaces_restore_validation_summary(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    paper = {
        "running": True,
        "approved_models": {"rows": []},
        "position": {"side": "FLAT", "instrument": "MGC", "quantity": 0},
        "operator_state": {},
        "desk_risk": {},
        "lane_risk": {"lanes": [{"lane_id": "mgc_lane", "risk_state": "OK"}]},
        "raw_operator_status": {
            "current_detected_session": "US_LATE",
            "lanes": [
                {
                    "lane_id": "mgc_lane",
                    "display_name": "MGC Lane",
                    "symbol": "MGC",
                    "current_detected_session": "US_LATE",
                    "eligible_now": True,
                    "startup_restore_validation": {
                        "restore_result": "SAFE_CLEANUP_READY",
                        "restore_completed_at": "2026-03-26T10:30:00+00:00",
                        "safe_cleanup_applied": True,
                        "safe_cleanup_actions": ["clear_stale_open_order_markers"],
                        "unresolved_restore_issue": False,
                        "recommended_action": "No action needed; safe cleanup was applied automatically.",
                        "duplicate_action_prevention_held": True,
                    },
                }
            ],
        },
        "status": {"entries_enabled": True, "operator_halt": False, "stale": False},
        "events": {},
        "latest_fills": [],
    }

    payload = service._paper_readiness_payload(paper)
    summary = payload["restore_validation_summary"]
    row = payload["lane_status_rows"][0]

    assert row["restore_result"] == "SAFE_CLEANUP_READY"
    assert row["restore_safe_cleanup_applied"] is True
    assert row["restore_unresolved_issue"] is False
    assert row["duplicate_action_prevention_held"] is True
    assert summary["last_restore_result"] == "SAFE_CLEANUP_READY"
    assert summary["safe_cleanup_count"] == 1
    assert summary["unresolved_issue_count"] == 0
    assert summary["duplicate_action_prevention_held"] is True
    assert summary["recommended_action"] == "No action needed; safe cleanup was applied automatically."


def test_dashboard_paper_soak_validation_surfaces_latest_validation_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_validation").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_validation" / "paper_soak_validation_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-26T19:30:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "scenario_count": 10,
                    "passed_count": 10,
                    "failed_count": 0,
                    "runtime_phase": "READY",
                    "strategy_state": "READY",
                    "position_state": {"side": "FLAT"},
                    "market_data_health": {"market_data_ok": True},
                },
                "scenarios": [
                    {"scenario_id": "clean_entry_exit_cycle", "status": "PASS", "detail": "ok", "summary": {"runtime_phase": "READY", "strategy_state": "READY"}},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_validation_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["passed_count"] == 10
    assert payload["summary"]["runtime_phase"] == "READY"
    assert payload["scenario_rows"][0]["scenario_id"] == "clean_entry_exit_cycle"
    assert "10/10 scenarios passed" in payload["summary_line"]


def test_dashboard_paper_live_timing_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "live_timing_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:00:00+00:00",
                "runtime_phase": "RECONCILING",
                "strategy_state": "READY",
                "position_state": {"side": "FLAT", "internal_qty": 0, "broker_qty": 0},
                "evaluated_bar_id": "MGC|5m|2026-03-27T14:55:00+00:00",
                "intent_created_at": "2026-03-27T14:55:01+00:00",
                "submit_attempted_at": "2026-03-27T14:55:01+00:00",
                "broker_ack_at": "2026-03-27T14:55:02+00:00",
                "broker_fill_at": None,
                "pending_since": "2026-03-27T14:55:02+00:00",
                "pending_reason": "fill_timeout_escalated",
                "pending_stage": "RECONCILING",
                "reconcile_trigger_source": "fill_timeout",
                "entries_disabled_blocker": "fill_timeout_escalated",
                "broker_truth": {
                    "decision_order": ["direct_order_status", "open_orders", "position_truth", "fill_truth"],
                    "direct_order_status": "ACKNOWLEDGED",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_live_timing_summary_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["runtime_phase"] == "RECONCILING"
    assert payload["pending_stage"] == "RECONCILING"
    assert payload["broker_truth"]["direct_order_status"] == "ACKNOWLEDGED"
    assert "stage=RECONCILING" in payload["summary_line"]


def test_dashboard_paper_broker_truth_shadow_validation_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    service = OperatorDashboardService(repo_root)
    artifact_path = service._production_link_service.config.snapshot_path.with_name("broker_truth_schema_validation_latest.json")  # type: ignore[attr-defined]
    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    artifact_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:05:00+00:00",
                "operator_path": "mgc-v05l probationary-broker-truth-shadow-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "READ_ONLY_LIVE_SHADOW"},
                "selected_account_hash": "hash-123",
                "schemas": {
                    "order_status": {"required_fields": ["broker_order_id", "status"], "optional_fields": ["symbol"]},
                    "open_orders": {"required_fields": ["broker_order_id", "symbol", "status", "instruction", "quantity"], "optional_fields": []},
                    "position": {"required_fields": ["symbol", "side", "quantity"], "optional_fields": []},
                    "account_health": {"required_fields": ["status", "broker_reachable", "auth_ready", "account_selected"], "optional_fields": []},
                },
                "validations": {
                    "order_status": {"classification": "partial_but_usable_truth", "issues": ["representative_order_unavailable"]},
                    "open_orders": {"classification": "sufficient_broker_truth", "issues": []},
                    "position": {"classification": "sufficient_broker_truth", "issues": []},
                    "account_health": {"classification": "sufficient_broker_truth", "issues": []},
                },
                "summary": {
                    "result": "WARN",
                    "overall_classification": "partial_but_usable_truth",
                    "representative_broker_order_id": None,
                    "missing_or_ambiguous_fields": [{"schema_name": "order_status", "issues": ["representative_order_unavailable"]}],
                    "summary_line": "WARN | classification=partial_but_usable_truth | order_status=partial_but_usable_truth | open_orders=sufficient_broker_truth | position=sufficient_broker_truth | account_health=sufficient_broker_truth",
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_broker_truth_shadow_validation_payload({"artifacts_dir": str(repo_root / "outputs" / "probationary_pattern_engine" / "paper_session")})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "WARN"
    assert payload["selected_account_hash"] == "hash-123"
    assert payload["validations"]["order_status"]["classification"] == "partial_but_usable_truth"
    assert "classification=partial_but_usable_truth" in payload["summary_line"]


def test_dashboard_shadow_live_shadow_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (shadow_artifacts / "live_shadow_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:08:00+00:00",
                "operator_path": "mgc-v05l probationary-live-shadow",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "LIVE_SHADOW_NO_SUBMIT"},
                "current_runtime_phase": "RECONCILING",
                "strategy_state": "READY",
                "last_finalized_live_bar_id": "MGC|5m|2026-03-27T15:05:00+00:00",
                "session_classification": "US_MIDDAY",
                "latest_signal_summary": {"long_entry": True, "long_entry_source": "usLatePauseResumeLongTurn"},
                "latest_shadow_intent": {"intent_type": "BUY_TO_OPEN", "reason_code": "usLatePauseResumeLongTurn"},
                "submit_would_be_allowed_if_shadow_disabled": False,
                "entries_disabled_blocker": "broker_reconciliation_not_clear",
                "pending_stage": "SHADOW_INTENT_SUPPRESSED",
                "pending_reason": "shadow_submit_suppressed",
                "reconcile_trigger_source": "broker_reconciliation",
                "broker_truth_summary": {
                    "classification": "INSUFFICIENT_TRUTH_RECONCILE",
                    "reconciliation_status": "blocked",
                },
                "summary_line": "phase=RECONCILING | last_bar=MGC|5m|2026-03-27T15:05:00+00:00 | submit=BLOCKED | blocker=broker_reconciliation_not_clear",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._shadow_live_shadow_summary_payload({"artifacts_dir": str(shadow_artifacts)})

    assert payload["available"] is True
    assert payload["current_runtime_phase"] == "RECONCILING"
    assert payload["latest_shadow_intent"]["intent_type"] == "BUY_TO_OPEN"
    assert payload["entries_disabled_blocker"] == "broker_reconciliation_not_clear"
    assert payload["broker_truth_summary"]["classification"] == "INSUFFICIENT_TRUTH_RECONCILE"
    assert "submit=BLOCKED" in payload["summary_line"]


def test_dashboard_shadow_live_strategy_pilot_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (shadow_artifacts / "live_strategy_pilot_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:18:00+00:00",
                "operator_path": "mgc-v05l probationary-live-strategy-pilot",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "LIVE_STRATEGY_PILOT"},
                "live_strategy_pilot_enabled": True,
                "live_strategy_submit_enabled": True,
                "live_strategy_single_cycle_mode": True,
                "pilot_armed": False,
                "pilot_rearm_required": True,
                "submit_currently_enabled": False,
                "cycle_status": "completed",
                "remaining_allowed_live_submits": 0,
                "current_runtime_phase": "READY",
                "strategy_state": "READY",
                "current_strategy_readiness": False,
                "latest_evaluated_bar": {"bar_id": "MGC|5m|2026-03-27T15:15:00+00:00"},
                "latest_live_strategy_intent": {"intent_type": "BUY_TO_OPEN", "submit_attempted": True},
                "submit_attempted_at": "2026-03-27T15:15:01+00:00",
                "broker_ack_at": "2026-03-27T15:15:02+00:00",
                "broker_fill_at": None,
                "broker_order_id": "broker-123",
                "pending_stage": "AWAITING_FILL",
                "pending_reason": "awaiting_broker_fill",
                "reconcile_trigger_source": None,
                "entries_disabled_blocker": "pending_unresolved_order",
                "submit_gate": {"blocker": "pending_unresolved_order", "submit_eligible": False},
                "pilot_cycle": {
                    "pilot_armed": False,
                    "rearm_required": True,
                    "cycle_status": "completed",
                    "remaining_allowed_live_submits": 0,
                    "entry": {"intent_type": "BUY_TO_OPEN"},
                    "exit": {"intent_type": "SELL_TO_CLOSE"},
                    "final_result": "completed",
                    "rearm_action": "rearm_live_strategy_pilot",
                },
                "broker_truth_summary": {"classification": "SUFFICIENT_BROKER_TRUTH"},
                "position_state": {"side": "FLAT", "internal_qty": 0},
                "signal_observability": {
                    "available": True,
                    "why_no_trade_so_far": "No final entries yet. Raw long candidates: 2 -> final long entries: 0. Raw short candidates: 1 -> final short entries: 0.",
                    "session_counts": {
                        "bull_snap_turn_candidate": 3,
                        "firstBullSnapTurn": 0,
                        "asia_reclaim_bar_raw": 1,
                        "asia_hold_bar_ok": 0,
                        "asia_acceptance_bar_ok": 0,
                        "asiaVWAPLongSignal": 0,
                        "bear_snap_turn_candidate": 1,
                        "firstBearSnapTurn": 0,
                        "longEntryRaw": 2,
                        "shortEntryRaw": 1,
                        "longEntry": 0,
                        "shortEntry": 0,
                    },
                    "top_failed_predicates": {
                        "bullSnapLong": [{"predicate": "bull_snap_turn_candidate", "count": 5}],
                        "asiaVWAPLong": [{"predicate": "asia_reclaim_bar_raw", "count": 4}],
                        "bearSnapShort": [{"predicate": "bear_snap_turn_candidate", "count": 5}],
                    },
                    "per_bar_rows": [
                        {
                            "bar_id": "MGC|5m|2026-03-27T15:15:00+00:00",
                            "why_no_trade": "bullSnapLong stalled at bull_snap_turn_candidate",
                            "recentLongSetup": False,
                            "recentShortSetup": False,
                            "barsSinceLongSetup": None,
                            "barsSinceShortSetup": None,
                        }
                    ],
                },
                "summary_line": "pilot=ENABLED | phase=READY | submit=BLOCKED | bar=MGC|5m|2026-03-27T15:15:00+00:00 | blocker=pending_unresolved_order",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._shadow_live_strategy_pilot_summary_payload({"artifacts_dir": str(shadow_artifacts)})

    assert payload["available"] is True
    assert payload["live_strategy_pilot_enabled"] is True
    assert payload["live_strategy_submit_enabled"] is True
    assert payload["pilot_armed"] is False
    assert payload["cycle_status"] == "completed"
    assert payload["remaining_allowed_live_submits"] == 0
    assert payload["pending_stage"] == "AWAITING_FILL"
    assert payload["entries_disabled_blocker"] == "pending_unresolved_order"
    assert payload["latest_live_strategy_intent"]["intent_type"] == "BUY_TO_OPEN"
    assert dict(payload["pilot_cycle"])["rearm_action"] == "rearm_live_strategy_pilot"
    assert dict(payload["signal_observability"])["session_counts"]["longEntryRaw"] == 2
    assert dict(payload["signal_observability"])["top_failed_predicates"]["bullSnapLong"][0]["predicate"] == "bull_snap_turn_candidate"


def test_dashboard_paper_live_timing_validation_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_live_timing_validation").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_live_timing_validation" / "paper_live_timing_validation_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T15:10:00+00:00",
                "operator_path": "mgc-v05l probationary-live-timing-validate",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER_RUNTIME_WITH_LIVE_TIMING_BOUNDARY"},
                "contract": {
                    "broker_truth_decision_order": ["direct_order_status", "open_orders", "position_truth", "fill_truth"],
                    "acknowledgement_window_seconds": 30,
                    "fill_confirmation_window_seconds": 60,
                },
                "summary": {
                    "result": "PASS",
                    "scenario_count": 8,
                    "passed_count": 8,
                    "final_runtime_phase": "FILLED",
                    "final_strategy_state": "READY",
                    "final_pending_stage": "FILLED",
                    "final_blocker": None,
                },
                "scenarios": [
                    {"scenario_id": "submit_after_completed_bar_close", "status": "PASS", "detail": "ok", "summary": {"pending_stage": "AWAITING_FILL", "runtime_phase": "READY"}},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_live_timing_validation_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["result"] == "PASS"
    assert payload["summary"]["passed_count"] == 8
    assert payload["contract"]["broker_truth_decision_order"] == ["direct_order_status", "open_orders", "position_truth", "fill_truth"]
    assert payload["scenario_rows"][0]["scenario_id"] == "submit_after_completed_bar_close"
    assert "8/8 scenarios passed" in payload["summary_line"]


def test_dashboard_paper_soak_extended_surfaces_latest_extended_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_extended").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_extended" / "paper_soak_extended_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T14:30:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-extended",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "bars_processed": 24,
                    "restart_count": 5,
                    "drift_detected": False,
                    "final_runtime_phase": "RECONCILING",
                    "final_strategy_state": "READY",
                    "final_position_state": {"side": "FLAT"},
                    "final_entry_blocker": "fill_timeout_escalated",
                },
                "checkpoint_rows": [
                    {"checkpoint_id": "pending_acknowledged_order", "trigger_state": "PENDING_ORDER", "drift_detected": False},
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_extended_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["bars_processed"] == 24
    assert payload["summary"]["restart_count"] == 5
    assert payload["checkpoint_rows"][0]["checkpoint_id"] == "pending_acknowledged_order"
    assert "bars=24" in payload["summary_line"]


def test_dashboard_signal_selectivity_analysis_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    artifact_dir = repo_root / "outputs" / "probationary_pattern_engine" / "signal_selectivity_analysis"
    artifact_dir.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (artifact_dir / "signal_selectivity_analysis_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T19:45:00+00:00",
                "dataset_count": 4,
                "summary_line": "raw long 3 -> final 0, raw short 1 -> final 0; top blockers: bullSnapLong -> range, asiaVWAPLong -> reclaim color, bearSnapShort -> location",
                "key_findings": [
                    "Live pilot: long raw 0 -> final 0, short raw 0 -> final 0.",
                    "Bear Snap location comparison: short raw 2 -> 5, short final 0 -> 1, location primary blocks 7 -> 1.",
                ],
                "live_pilot_focus": {
                    "why_no_trade_so_far": "No trade so far because raw long 0 -> final 0 and raw short 0 -> final 0.",
                    "top_failed_predicates": {
                        "bullSnapLong": [{"predicate": "range", "count": 12}],
                        "asiaVWAPLong": [{"predicate": "reclaim color", "count": 9}],
                        "bearSnapShort": [{"predicate": "location", "count": 7}],
                    },
                    "raw_candidates_vs_final_entries": {
                        "long": {"raw_candidates": 3, "final_entries": 0},
                        "short": {"raw_candidates": 1, "final_entries": 0},
                    },
                    "anti_churn": {
                        "suppression_by_family": {
                            "bullSnapLong": {"suppressed_count": 1},
                            "asiaVWAPLong": {"suppressed_count": 0},
                            "bearSnapShort": {"suppressed_count": 0},
                        }
                    },
                },
                "before_after_bear_snap_location": {
                    "available": True,
                    "summary_line": "short raw 2 -> 5, short final 0 -> 1, location primary blocks 7 -> 1",
                    "materially_improved_short_opportunity_rate": True,
                },
                "bear_snap_up_stretch_ladder": {
                    "available": True,
                    "recommended_value": "0.90",
                    "range_becomes_next_dominant_blocker": True,
                    "summary_line": "1.00 -> 0.90: short raw 21 -> 22, short final 21 -> 22, short/100 1.471 -> 1.541, top blocker upside stretch -> range",
                },
                "bear_snap_range_ladder": {
                    "available": True,
                    "recommended_value": "0.80",
                    "next_dominant_blocker_after_recommended": "upside stretch",
                    "summary_line": "0.90 -> 0.80: short raw 22 -> 25, short final 22 -> 25, short/100 1.541 -> 1.751, top blocker range -> upside stretch",
                },
                "regime_comparison": {
                    "red_day_down_tape": {
                        "short_raw_candidates_per_100_bars": 1.2,
                    }
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._signal_selectivity_analysis_payload({})

    assert payload["available"] is True
    assert payload["dataset_count"] == 4
    assert payload["live_pilot_focus"]["top_failed_predicates"]["bearSnapShort"][0]["predicate"] == "location"
    assert payload["before_after_bear_snap_location"]["materially_improved_short_opportunity_rate"] is True
    assert payload["bear_snap_range_ladder"]["recommended_value"] == "0.80"
    assert payload["bear_snap_up_stretch_ladder"]["recommended_value"] == "0.90"
    assert service._signal_selectivity_analysis_path == repo_root / "outputs" / "operator_dashboard" / "signal_selectivity_analysis_snapshot.json"


def test_dashboard_paper_soak_unattended_surfaces_latest_unattended_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "runtime" / "paper_soak_unattended").mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "runtime" / "paper_soak_unattended" / "paper_soak_unattended_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T18:00:00+00:00",
                "operator_path": "mgc-v05l probationary-paper-soak-unattended",
                "allowed_scope": {"symbol": "MGC", "timeframe": "5m", "mode": "PAPER"},
                "summary": {
                    "result": "PASS",
                    "bars_processed": 60,
                    "runtime_duration_minutes": 295,
                    "restart_count": 7,
                    "drift_detected": False,
                    "final_runtime_phase": "RECONCILING",
                    "final_strategy_state": "READY",
                    "final_position_state": {"side": "FLAT"},
                    "final_entry_blocker": "fill_timeout_escalated",
                },
                "checkpoint_rows": [
                    {
                        "checkpoint_id": "heartbeat_reconcile_restart",
                        "trigger_state": "HEARTBEAT_RECONCILE",
                        "drift_detected": False,
                        "summary_alignment_held": True,
                    },
                ],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_soak_unattended_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["summary"]["bars_processed"] == 60
    assert payload["summary"]["restart_count"] == 7
    assert payload["checkpoint_rows"][0]["checkpoint_id"] == "heartbeat_reconcile_restart"
    assert "duration=295m" in payload["summary_line"]


def test_dashboard_paper_exit_parity_summary_surfaces_latest_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    service = OperatorDashboardService(repo_root)

    (paper_artifacts / "exit_parity_summary_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-27T19:00:00+00:00",
                "position_side": "LONG",
                "current_position_family": "VWAP",
                "latest_exit_decision": {
                    "primary_reason": "VWAP_LOSS",
                    "all_true_reasons": ["VWAP_LOSS", "VWAP_WEAK_FOLLOWTHROUGH"],
                },
                "stop_refs": {"active_long_stop_ref": "100.0"},
                "break_even": {"long_break_even_armed": True, "short_break_even_armed": False},
                "latest_restore_result": "READY",
                "exit_fill_pending": True,
                "exit_fill_confirmed": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = service._paper_exit_parity_summary_payload({"artifacts_dir": str(paper_artifacts)})

    assert payload["available"] is True
    assert payload["current_position_family"] == "VWAP"
    assert payload["latest_exit_decision"]["primary_reason"] == "VWAP_LOSS"
    assert payload["break_even"]["long_break_even_armed"] is True
    assert "family=VWAP" in payload["summary_line"]


def test_dashboard_falls_back_to_configured_paper_lanes_when_runtime_lane_artifacts_are_missing(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    shadow_artifacts.mkdir(parents=True, exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:00:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T08:55:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": [
                    "asiaEarlyNormalBreakoutRetestHoldTurn",
                    "usLatePauseResumeLongTurn",
                ],
                "approved_short_entry_sources": ["asiaEarlyPauseResumeShortTurn"],
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_source": "test",
        "feed_state": "LIVE",
        "feed_label": "INDEX FEED LIVE",
        "updated_at": "2026-03-19T09:00:00-04:00",
        "age_seconds": 0,
        "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        "note": "test",
        "diagnostics": {"fetch_state": "SUCCESS", "symbols": []},
        "symbols": [],
    }
    service._paper_config_in_force_fallback = lambda artifacts_dir, db_path: {  # type: ignore[method-assign]
        "desk_halt_new_entries_loss": "-1500",
        "desk_flatten_and_halt_loss": "-2500",
        "lane_realized_loser_limit_per_session": 2,
        "lanes": [
            {
                "lane_id": "mgc_us_late_pause_resume_long",
                "display_name": "MGC / usLatePauseResumeLongTurn",
                "symbol": "MGC",
                "long_sources": ["usLatePauseResumeLongTurn"],
                "short_sources": [],
                "session_restriction": "US_LATE",
            },
            {
                "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "MGC",
                "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                "short_sources": [],
                "session_restriction": "ASIA_EARLY",
            },
            {
                "lane_id": "mgc_asia_early_pause_resume_short",
                "display_name": "MGC / asiaEarlyPauseResumeShortTurn",
                "symbol": "MGC",
                "long_sources": [],
                "short_sources": ["asiaEarlyPauseResumeShortTurn"],
                "session_restriction": "ASIA_EARLY",
            },
            {
                "lane_id": "pl_us_late_pause_resume_long",
                "display_name": "PL / usLatePauseResumeLongTurn",
                "symbol": "PL",
                "long_sources": ["usLatePauseResumeLongTurn"],
                "short_sources": [],
                "session_restriction": "US_LATE",
            },
            {
                "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "GC",
                "long_sources": ["asiaEarlyNormalBreakoutRetestHoldTurn"],
                "short_sources": [],
                "session_restriction": "ASIA_EARLY",
            },
        ],
    }  # type: ignore[method-assign]

    snapshot = service.snapshot()

    assert snapshot["paper"]["raw_operator_status"]["paper_lane_count"] == 5
    assert len(snapshot["paper"]["raw_operator_status"]["lanes"]) == 5
    assert snapshot["paper"]["approved_models"]["enabled_count"] == 5
    assert snapshot["paper"]["approved_models"]["total_count"] == 5
    assert snapshot["paper"]["approved_models"]["instrument_scope"] == "5 admitted lanes / multi-lane paper mode"
    assert {row["branch"] for row in snapshot["paper"]["approved_models"]["rows"]} == {
        "MGC / usLatePauseResumeLongTurn",
        "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "MGC / asiaEarlyPauseResumeShortTurn",
        "PL / usLatePauseResumeLongTurn",
        "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
    }
    assert snapshot["paper"]["readiness"]["approved_models_active"] == 5
    assert snapshot["paper"]["entry_eligibility"]["verdict"] == "NOT ELIGIBLE: RUNTIME STOPPED"
    assert snapshot["paper"]["entry_eligibility"]["primary_blocking_reason"] == "RUNTIME_STOPPED"


def test_market_index_rows_keep_primary_quote_fields_when_bid_ask_missing() -> None:
    raw_payload = {
        "$SPX": {
            "assetMainType": "INDEX",
            "quote": {
                "lastPrice": 6624.7,
                "netChange": -91.39,
                "netPercentChange": -1.36076199,
                "tradeTime": 1773864761067,
                "securityStatus": "Closed",
            },
            "realtime": True,
            "reference": {
                "description": "S&P 500 INDEX",
                "exchangeName": "Index",
            },
            "symbol": "$SPX",
        }
    }

    rows, diagnostics = _market_index_rows(
        raw_payload,
        [
            {"label": "SPX", "name": "S&P 500", "external_symbol": "$SPX", "source_type": "cash_index"},
        ],
    )

    assert rows[0]["state"] == "LIVE"
    assert rows[0]["value_state"] == "LIVE"
    assert rows[0]["current_value"] == "6624.7"
    assert rows[0]["absolute_change"] == "-91.39"
    assert rows[0]["percent_change"] == "-1.36%"
    assert rows[0]["bid"] is None
    assert rows[0]["ask"] is None
    assert rows[0]["bid_state"] == "UNAVAILABLE"
    assert rows[0]["ask_state"] == "UNAVAILABLE"
    assert "BID_UNAVAILABLE" in rows[0]["diagnostic_codes"]
    assert diagnostics[0]["payload_present"] is True
    assert diagnostics[0]["field_states"]["current_value"]["available"] is True
    assert diagnostics[0]["field_states"]["bid"]["available"] is False
    assert diagnostics[0]["matched_symbol"] == "$SPX"


def test_market_index_rows_match_future_root_via_reference_product() -> None:
    raw_payload = {
        "/GCJ26": {
            "assetMainType": "FUTURE",
            "quote": {
                "lastPrice": 4823.9,
                "netChange": -184.3,
                "futurePercentChange": -3.67996486,
                "bidPrice": 4810.0,
                "askPrice": 4832.2,
            },
            "realtime": True,
            "reference": {
                "description": "Gold Futures,Apr-2026, ETH",
                "product": "/GC",
            },
            "symbol": "/GCJ26",
        }
    }

    rows, diagnostics = _market_index_rows(
        raw_payload,
        [
            {"label": "GOLD", "name": "Gold Futures", "external_symbol": "/GC", "source_type": "future"},
        ],
    )

    assert rows[0]["state"] == "LIVE"
    assert rows[0]["current_value"] == "4823.9"
    assert rows[0]["absolute_change"] == "-184.3"
    assert rows[0]["matched_symbol"] == "/GC"
    assert rows[0]["matched_via"] == "reference.product"
    assert diagnostics[0]["matched_via"] == "reference.product"
    assert diagnostics[0]["field_states"]["bid"]["available"] is True
    assert diagnostics[0]["field_states"]["ask"]["available"] is True


def test_treasury_curve_rows_scale_verified_yield_indices_and_keep_missing_tenors_explicit() -> None:
    raw_payload = {
        "$IRX": {
            "assetMainType": "INDEX",
            "quote": {"lastPrice": 36.1, "closePrice": 36.05, "netChange": 0.05},
            "reference": {"description": "CBOE INT RATE 13 WK T BILL     13 WK T BILL"},
            "symbol": "$IRX",
        },
        "$FVX": {
            "assetMainType": "INDEX",
            "quote": {"lastPrice": 38.62, "closePrice": 37.86, "netChange": 0.76},
            "reference": {"description": "CBOE INT RATE 5 YEAR T NOTE    5 YEAR T NOTE"},
            "symbol": "$FVX",
        },
        "errors": {"invalidSymbols": ["$UST2Y"]},
    }

    rows, diagnostics = _treasury_curve_rows(
        raw_payload,
        [
            {"tenor": "3M", "name": "3M", "external_symbol": "$IRX", "source_type": "cash_treasury_yield", "source_note": "13-week source"},
            {"tenor": "5Y", "name": "5Y", "external_symbol": "$FVX", "source_type": "cash_treasury_yield", "source_note": "5-year source"},
            {"tenor": "2Y", "name": "2Y", "external_symbol": "$UST2Y", "source_type": "cash_treasury_yield", "source_note": "2-year source"},
        ],
    )

    assert rows[0]["current_yield"] == "3.610"
    assert rows[0]["prior_yield"] == "3.605"
    assert rows[0]["day_change_bp"] == "0.5"
    assert rows[0]["render_classification"] == "LIVE_WITH_COMPARISON"
    assert rows[1]["current_yield"] == "3.862"
    assert rows[1]["prior_yield"] == "3.786"
    assert rows[1]["day_change_bp"] == "7.6"
    assert rows[2]["render_classification"] == "UNAVAILABLE_UNSUPPORTED_SYMBOL"
    assert diagnostics[2]["diagnostic_codes"] == ["INVALID_SYMBOL"]


def test_dashboard_snapshot_surfaces_prior_session_carry_forward_risk(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T09:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-18",
                "realized_net_pnl": "10.0",
                "flat_at_end": False,
                "reconciliation_clean": False,
                "unresolved_open_intents": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    assert snapshot["global"]["desk_clean"] is False
    assert snapshot["global"]["desk_clean_label"] == "DESK GUARDED"
    assert snapshot["global"]["paper_run_ready"] is False
    assert snapshot["paper_carry_forward"]["active"] is True
    assert snapshot["paper_carry_forward"]["session_date"] == "2026-03-18"
    assert snapshot["paper_carry_forward"]["not_flat_at_close"] is True
    assert snapshot["paper_carry_forward"]["reconciliation_dirty"] is True
    assert snapshot["paper_carry_forward"]["unresolved_open_intents"] == 2
    assert snapshot["paper_pre_session_review"]["required"] is True
    assert snapshot["paper_pre_session_review"]["completed"] is False
    assert snapshot["paper_continuity"]["entries"][2]["kind"] == "carry_forward"


def test_dashboard_snapshot_reads_paper_run_start_artifacts(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-19T09:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-19T09:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.summary.json").write_text(
        json.dumps({"realized_net_pnl": "0.0", "session_date": "2026-03-19"}) + "\n",
        encoding="utf-8",
    )

    dashboard_dir = repo_root / "outputs" / "operator_dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    (dashboard_dir / "paper_current_run_start.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-03-19T09:30:00-04:00",
                "run_start_id": "paper-run-1",
                "desk_state_at_start": "GUARDED",
                "started_after_guarded_review": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (dashboard_dir / "paper_run_start_blocks.jsonl").write_text(
        json.dumps({"timestamp": "2026-03-19T09:00:00-04:00", "blocked_reason": "Inherited risk review pending."}) + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    assert snapshot["paper_run_start"]["current"]["run_start_id"] == "paper-run-1"
    assert snapshot["paper_run_start"]["current"]["desk_state_at_start"] == "GUARDED"
    assert snapshot["paper_run_start"]["blocked_history"][0]["blocked_reason"] == "Inherited risk review pending."
    assert snapshot["paper_continuity"]["entries"][-1]["kind"] == "run_start"


def test_paper_runtime_recovery_auto_starts_stopped_runtime_when_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    pre_paper = {
        "running": False,
        "readiness": {"runtime_phase": "STOPPED"},
        "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
        "operator_state": {},
        "status": {"session_date": "2026-03-26"},
        "non_approved_lanes": {"rows": []},
    }
    post_paper = {
        "running": True,
        "status": {"session_date": "2026-03-26"},
    }

    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="started", stderr=""),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: post_paper)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper=pre_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUCCEEDED"
    assert payload["manual_action_required"] is False
    assert refreshed_paper == post_paper
    assert result is not None
    assert result["action"] == "auto-start-paper"


def test_paper_runtime_recovery_requires_manual_action_when_stopped_runtime_is_not_safe(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {
                "primary_blocking_reason": "RECONCILIATION_DIRTY",
                "state_note": "Persisted reconciliation is dirty.",
                "clear_action": "Manual inspection required",
            },
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert payload["manual_action_required"] is True
    assert payload["next_action"] == "Manual inspection required"
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_respects_restart_backoff_and_does_not_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_BACKOFF",
            "attempted_at": now.isoformat(),
            "failed_at": now.isoformat(),
            "last_restart_result": "FAILED",
            "restart_backoff_until": (now.replace(microsecond=0) + operator_dashboard_module.timedelta(seconds=120)).isoformat(),
            "restart_attempt_history": [
                {
                    "attempted_at": now.isoformat(),
                    "result": "FAILED",
                }
            ],
            "last_runtime_stop_detected_at": now.isoformat(),
        }
    )

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("auto-restart should not run while backoff is active")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_BACKOFF"
    assert payload["manual_action_required"] is False
    assert payload["auto_restart_allowed"] is False
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_suppresses_after_budget_exhaustion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(
        service,
        "_paper_runtime_supervisor_policy",
        lambda: {
            "restart_window_seconds": 900,
            "max_auto_restarts_per_window": 2,
            "restart_backoff_seconds": 60,
            "restart_suppression_seconds": 600,
            "failure_cooldown_seconds": 120,
        },
    )
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_BACKOFF",
            "attempted_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
            "failed_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
            "last_restart_result": "FAILED",
            "restart_attempt_history": [
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
                    "result": "FAILED",
                }
            ],
            "last_runtime_stop_detected_at": (now - operator_dashboard_module.timedelta(minutes=4)).isoformat(),
        }
    )
    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 1, stdout="", stderr="boom"),
    )

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUPPRESSED"
    assert payload["manual_action_required"] is True
    assert payload["restart_suppressed"] is True
    assert payload["restart_attempts_in_window"] == 2
    assert refreshed_paper is None
    assert result is not None
    supervisor_events = operator_dashboard_module._tail_jsonl(
        tmp_path / "outputs" / "operator_dashboard" / "paper_runtime_supervisor_events.jsonl",
        10,
    )
    assert any(row["event_type"] == "restart_failed" for row in supervisor_events)
    assert any(row["event_type"] == "restart_suppressed" for row in supervisor_events)


def test_paper_runtime_recovery_suppression_blocks_further_attempts_without_duplicate_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    now = datetime.now(timezone.utc)
    suppressed_until = (now + operator_dashboard_module.timedelta(minutes=10)).isoformat()
    service._write_paper_runtime_recovery_state(
        {
            "status": "AUTO_RESTART_SUPPRESSED",
            "attempted_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
            "failed_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
            "last_restart_result": "FAILED",
            "restart_suppressed_until": suppressed_until,
            "restart_attempt_history": [
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=2)).isoformat(),
                    "result": "FAILED",
                },
                {
                    "attempted_at": (now - operator_dashboard_module.timedelta(minutes=1)).isoformat(),
                    "result": "FAILED",
                },
            ],
            "last_runtime_stop_detected_at": (now - operator_dashboard_module.timedelta(minutes=3)).isoformat(),
        }
    )
    event_path = tmp_path / "outputs" / "operator_dashboard" / "paper_runtime_supervisor_events.jsonl"
    operator_dashboard_module._append_jsonl(
        event_path,
        {
            "event_type": "restart_suppressed",
            "occurred_at": now.isoformat(),
            "supervisor_status": "AUTO_RESTART_SUPPRESSED",
            "message": "Automatic restart has been suppressed because the rolling restart budget was exhausted.",
        },
    )

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("suppressed runtime should not auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "AUTO_RESTART_SUPPRESSED"
    assert payload["manual_action_required"] is True
    assert refreshed_paper is None
    assert result is None
    supervisor_events = operator_dashboard_module._tail_jsonl(event_path, 10)
    assert [row["event_type"] for row in supervisor_events].count("restart_suppressed") == 1


def test_paper_runtime_recovery_respects_explicit_operator_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)

    def _unexpected_start(*args, **kwargs):
        raise AssertionError("explicit operator stop should block auto-restart")

    monkeypatch.setattr(service, "_paper_start_command_with_enabled_temp_paper", _unexpected_start)

    payload, refreshed_paper, result = service._paper_runtime_recovery_payload(
        paper={
            "running": False,
            "readiness": {"runtime_phase": "STOPPED"},
            "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
            "operator_state": {"last_control_action": "stop-paper"},
            "status": {"session_date": "2026-03-26"},
            "non_approved_lanes": {"rows": []},
        },
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert payload["status"] == "STOPPED_MANUAL_REQUIRED"
    assert payload["reason_code"] == "OPERATOR_STOP"
    assert payload["manual_action_required"] is True
    assert refreshed_paper is None
    assert result is None


def test_paper_runtime_recovery_success_clears_stopped_runtime_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OperatorDashboardService(tmp_path)
    pre_paper = {
        "running": False,
        "readiness": {"runtime_phase": "STOPPED"},
        "entry_eligibility": {"primary_blocking_reason": "RUNTIME_STOPPED"},
        "operator_state": {},
        "status": {"session_date": "2026-03-26"},
        "non_approved_lanes": {"rows": []},
    }
    post_paper = {
        "running": True,
        "status": {"session_date": "2026-03-26"},
    }

    monkeypatch.setattr(
        service,
        "_paper_start_command_with_enabled_temp_paper",
        lambda snapshot: (["bash", "scripts/run_probationary_paper_soak.sh", "--background"], {"unresolved_lane_ids": []}),
    )
    monkeypatch.setattr(
        operator_dashboard_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="started", stderr=""),
    )
    monkeypatch.setattr(service, "_runtime_snapshot", lambda runtime_name: post_paper)

    first_payload, _, _ = service._paper_runtime_recovery_payload(
        paper=pre_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )
    second_payload, _, _ = service._paper_runtime_recovery_payload(
        paper=post_paper,
        auth_status={"runtime_ready": True},
        carry_forward={"active": False},
        pre_session_review={"required": False, "completed": True},
        closeout_state={"unresolved_open_intents": 0},
    )

    assert first_payload["status"] == "AUTO_RESTART_SUCCEEDED"
    assert second_payload["status"] in {"AUTO_RESTART_SUCCEEDED", "RUNNING"}
    assert second_payload["last_runtime_stop_detected_at"] is None
    assert second_payload["restart_suppressed"] is False


def test_dashboard_snapshot_writes_paper_performance_artifact(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-18T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-18T14:05:00-04:00",
                "position_side": "LONG",
                "strategy_status": "IN_LONG_K",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 1,
                    "broker_average_price": "100.0",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.summary.json").write_text(
        json.dumps({"realized_net_pnl": "25.0", "session_date": "2026-03-18", "closed_trade_count": 1}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-18.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-18T14:05:00-04:00,2026-03-18T14:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,100.5,5.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()
    performance_path = repo_root / "outputs" / "operator_dashboard" / "paper_performance_snapshot.json"

    assert performance_path.exists()
    written = json.loads(performance_path.read_text(encoding="utf-8"))
    assert written["realized_pnl"] == snapshot["paper"]["performance"]["realized_pnl"]
    assert service.operator_artifact_file("paper-performance")[0] == performance_path


def test_dashboard_snapshot_surfaces_strategy_performance_by_lane_and_instrument(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    mgc_lane_db = repo_root / "paper__mgc_bull.sqlite3"
    gc_lane_db = repo_root / "paper__gc_bear.sqlite3"
    _init_strategy_lane_dashboard_db(
        mgc_lane_db,
        symbol="MGC",
        entry_reason="bullSnap",
        closed_trade_pnl=None,
    )
    _init_strategy_lane_dashboard_db(
        gc_lane_db,
        symbol="GC",
        entry_reason="asiaVwapReclaim",
        closed_trade_pnl="25.0",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "mgc_bull",
                        "display_name": "MGC Bull",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "LONG",
                        "strategy_status": "IN_LONG_K",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "0",
                        "session_unrealized_pnl": "12.5",
                        "session_total_pnl": "12.5",
                        "entry_timestamp": "2026-03-22T13:35:00-04:00",
                        "entry_price": "100.0",
                        "last_mark": "101.25",
                        "point_value": "10",
                        "database_url": f"sqlite:///{mgc_lane_db}",
                    },
                    {
                        "lane_id": "gc_vwap",
                        "display_name": "GC VWAP",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "25.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "25.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{gc_lane_db}",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    strategy_rows = {row["lane_id"]: row for row in snapshot["paper"]["strategy_performance"]["rows"]}
    assert set(strategy_rows) == {"mgc_bull", "gc_vwap"}
    assert strategy_rows["mgc_bull"]["instrument"] == "MGC"
    assert strategy_rows["mgc_bull"]["standalone_strategy_id"] == "bull_snap__MGC"
    assert strategy_rows["mgc_bull"]["status"] == "OPEN_LONG"
    assert strategy_rows["mgc_bull"]["unrealized_pnl"] == "12.5"
    assert strategy_rows["gc_vwap"]["instrument"] == "GC"
    assert strategy_rows["gc_vwap"]["standalone_strategy_id"] == "asia_vwap_reclaim__GC"
    assert strategy_rows["gc_vwap"]["realized_pnl"] == "25.0"
    assert strategy_rows["gc_vwap"]["day_pnl"] == "25.0"
    assert strategy_rows["gc_vwap"]["trade_count"] == 1
    assert strategy_rows["gc_vwap"]["entry_count"] == 1
    assert strategy_rows["gc_vwap"]["expected_fire_cadence"] == "insufficient history"

    trade_log = snapshot["paper"]["strategy_performance"]["trade_log"]
    assert len(trade_log) == 1
    assert trade_log[0]["lane_id"] == "gc_vwap"
    assert trade_log[0]["instrument"] == "GC"
    assert trade_log[0]["standalone_strategy_id"] == "asia_vwap_reclaim__GC"
    assert trade_log[0]["signal_family_label"] == "VWAP reclaim"

    attribution_rows = {
        row["family_label"]: row for row in snapshot["paper"]["strategy_performance"]["attribution"]["rows"]
    }
    assert attribution_rows["VWAP reclaim"]["realized_pnl"] == "25.0"
    assert attribution_rows["VWAP reclaim"]["standalone_strategy_ids"] == ["asia_vwap_reclaim__GC"]

    portfolio_snapshot = snapshot["paper"]["strategy_performance"]["portfolio_snapshot"]
    assert portfolio_snapshot["total_realized_pnl"] == "25.0"
    assert portfolio_snapshot["total_unrealized_pnl"] == "12.5"
    assert portfolio_snapshot["total_day_pnl"] == "37.5"
    assert portfolio_snapshot["active_strategy_count"] == 2
    assert portfolio_snapshot["active_instrument_count"] == 2

    runtime_summary = snapshot["paper"]["strategy_runtime_summary"]
    assert runtime_summary["configured_standalone_strategies"] == 2
    assert runtime_summary["runtime_instances_present"] == 2
    assert runtime_summary["runtime_states_loaded"] == 0
    assert runtime_summary["can_process_bars"] == 2
    assert runtime_summary["in_position_strategies"] == 1


def test_dashboard_snapshot_builds_unified_strategy_analysis_surface(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)
    historical_playback_dir = repo_root / "outputs" / "historical_playback"
    historical_playback_dir.mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    lane_db = repo_root / "paper__mgc_bull.sqlite3"
    _init_strategy_lane_dashboard_db(
        lane_db,
        symbol="MGC",
        entry_reason="bullSnap",
        closed_trade_pnl="25.0",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_CASH_OPEN_IMPULSE",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "mgc_bull",
                        "display_name": "MGC Bull",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "25.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "25.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary_path = historical_playback_dir / "historical_playback_mgc_unified.summary.json"
    trigger_report_path = historical_playback_dir / "historical_playback_mgc_unified.trigger_report.json"
    trigger_report_md_path = historical_playback_dir / "historical_playback_mgc_unified.trigger_report.md"
    strategy_study_path = historical_playback_dir / "historical_playback_mgc_unified.strategy_study.json"
    strategy_study_md_path = historical_playback_dir / "historical_playback_mgc_unified.strategy_study.md"

    summary_path.write_text(
        json.dumps(
            {
                "processed_bars": 2,
                "aggregate_portfolio_summary": {
                    "standalone_strategy_ids": ["bull_snap__MGC"],
                    "standalone_strategy_count": 1,
                    "realized_pnl": "30.0",
                    "unrealized_pnl": "0",
                    "cumulative_pnl": "30.0",
                },
                "per_strategy_summaries": [
                    {
                        "standalone_strategy_id": "bull_snap__MGC",
                        "strategy_family": "bullSnap",
                        "instrument": "MGC",
                        "processed_bars": 2,
                        "order_intents": 2,
                        "fills": 2,
                        "entries": 1,
                        "exits": 1,
                        "realized_pnl": "30.0",
                    }
                ],
                "primary_standalone_strategy_id": "bull_snap__MGC",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    trigger_report_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "MGC",
                    "lane_family": "bullSnap",
                    "bars_processed": 2,
                    "signals_seen": 1,
                    "intents_created": 2,
                    "fills_created": 2,
                    "block_or_fault_reason": "",
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    trigger_report_md_path.write_text("# Trigger Report\n", encoding="utf-8")
    strategy_study_path.write_text(
        json.dumps(
            {
                "contract_version": "strategy_study_v3",
                "symbol": "MGC",
                "standalone_strategy_id": "bull_snap__MGC",
                "strategy_family": "bullSnap",
                "timeframe": "5m",
                "meta": {
                    "study_id": "bull-snap-replay-study",
                    "strategy_id": "bull_snap__MGC",
                    "strategy_family": "bullSnap",
                    "study_mode": "baseline_parity_mode",
                    "entry_model": "BASELINE_NEXT_BAR_OPEN",
                    "pnl_truth_basis": "BASELINE_FILL_TRUTH",
                    "coverage_start": "2026-03-22T13:35:00-04:00",
                    "coverage_end": "2026-03-22T13:45:00-04:00",
                    "timeframe_truth": {
                        "structural_signal_timeframe": "5m",
                        "execution_timeframe": "5m",
                        "artifact_timeframe": "5m",
                        "execution_timeframe_role": "matches_signal_evaluation",
                    },
                },
                "summary": {
                    "bar_count": 2,
                    "total_trades": 1,
                    "long_trades": 1,
                    "short_trades": 0,
                    "winners": 1,
                    "losers": 0,
                    "cumulative_realized_pnl": "30.0",
                    "cumulative_total_pnl": "30.0",
                    "max_drawdown": "5.0",
                    "session_level_behavior": [{"session_phase": "US", "bar_count": 2}],
                    "atp_summary": {"available": False},
                },
                "bars": [
                    {"bar_id": "bar-1", "timestamp": "2026-03-22T13:35:00-04:00", "strategy_status": "READY"},
                    {"bar_id": "bar-2", "timestamp": "2026-03-22T13:40:00-04:00", "strategy_status": "READY"},
                ],
                "trade_events": [
                    {"event_type": "ENTRY_FILL", "event_timestamp": "2026-03-22T13:35:00-04:00", "family": "bullSnap", "side": "LONG"},
                    {"event_type": "EXIT_FILL", "event_timestamp": "2026-03-22T13:40:00-04:00", "family": "bullSnap", "side": "LONG"},
                ],
                "pnl_points": [{"timestamp": "2026-03-22T13:40:00-04:00", "realized": "30.0", "open_pnl": "0", "total": "30.0"}],
                "execution_slices": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    strategy_study_md_path.write_text("# Strategy Study\n", encoding="utf-8")
    (historical_playback_dir / "historical_playback_unified.manifest.json").write_text(
        json.dumps(
            {
                "run_stamp": "historical_playback_unified",
                "symbols": [
                    {
                        "symbol": "MGC",
                        "summary_path": str(summary_path),
                        "trigger_report_json_path": str(trigger_report_path),
                        "trigger_report_markdown_path": str(trigger_report_md_path),
                        "strategy_study_json_path": str(strategy_study_path),
                        "strategy_study_markdown_path": str(strategy_study_md_path),
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    strategy_analysis = snapshot["strategy_analysis"]
    assert strategy_analysis["available"] is True
    detail = strategy_analysis["details_by_strategy_key"]["bull_snap__MGC"]
    lane_types = {row["lane_type"] for row in detail["lanes"]}
    assert lane_types == {"benchmark_replay", "paper_runtime"}
    benchmark_lane = next(row for row in detail["lanes"] if row["lane_type"] == "benchmark_replay")
    paper_lane = next(row for row in detail["lanes"] if row["lane_type"] == "paper_runtime")
    assert benchmark_lane["source_of_truth"]["primary_artifact"] == "strategy_study_v3"
    assert paper_lane["source_of_truth"]["primary_artifact"] == "paper_strategy_performance_snapshot"
    assert benchmark_lane["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert paper_lane["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"
    assert detail["comparison_presets"][0]["comparison_type"] == "benchmark_vs_paper_runtime"
    assert detail["comparison_presets"][0]["left_lane"]["lane_type"] == "benchmark_replay"
    assert detail["comparison_presets"][0]["right_lane"]["lane_type"] == "paper_runtime"
    assert detail["comparison_presets"][0]["left_lane"]["lifecycle_truth"]["class"] == "BASELINE_ONLY"
    assert detail["comparison_presets"][0]["right_lane"]["lifecycle_truth"]["class"] == "FULL_LIFECYCLE_TRUTH"

    strategy_analysis_path = repo_root / "outputs" / "operator_dashboard" / "strategy_analysis_snapshot.json"
    assert strategy_analysis_path.exists()
    assert service.operator_artifact_file("strategy-analysis")[0] == strategy_analysis_path


def test_dashboard_strategy_performance_tags_temporary_paper_metrics_bucket(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_db = repo_root / "atpe_lane.sqlite3"
    _init_empty_dashboard_db(lane_db)

    service = OperatorDashboardService(repo_root)
    payload = service._paper_strategy_performance_payload(
        paper={
            "raw_operator_status": {
                "current_detected_session": "US",
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary",
                        "display_name": "ATPE Long Medium+High Canary",
                        "symbol": "MES",
                        "source_family": "trend_participation.pullback_continuation.long.conservative",
                        "position_side": "FLAT",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{lane_db}",
                        "experimental_status": "experimental_canary",
                        "paper_only": True,
                        "non_approved": True,
                        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
                        "side": "LONG",
                    }
                ],
            },
            "status": {"strategy_status": "RUNNING"},
            "runtime_registry": {"rows": []},
        },
        session_date="2026-03-23",
        root_db_path=lane_db,
        approved_quant_baselines={"rows": []},
    )

    row = payload["rows"][0]
    assert row["lane_id"] == "atpe_long_medium_high_canary"
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["metrics_bucket"] == "experimental_temporary_paper"
    assert row["paper_only"] is True
    assert row["non_approved"] is True
    assert payload["metrics_buckets"]["experimental_temporary_paper"]["active_strategy_count"] == 1


def test_dashboard_snapshot_includes_strategy_execution_likelihood_statistics(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    lane_db = repo_root / "paper__gc_signal.sqlite3"
    _init_empty_dashboard_db(lane_db)
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade1",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-17T18:05:00-04:00",
        entry_fill_at="2026-03-17T18:10:00-04:00",
        exit_created_at="2026-03-17T18:15:00-04:00",
        exit_fill_at="2026-03-17T18:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-1",
    )
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade2",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-18T18:05:00-04:00",
        entry_fill_at="2026-03-18T18:10:00-04:00",
        exit_created_at="2026-03-18T18:15:00-04:00",
        exit_fill_at="2026-03-18T18:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-2",
    )
    _append_strategy_lane_closed_trade(
        lane_db,
        symbol="GC",
        trade_id="trade3",
        entry_reason="asiaVwapReclaim",
        entry_created_at="2026-03-20T03:05:00-04:00",
        entry_fill_at="2026-03-20T03:10:00-04:00",
        exit_created_at="2026-03-20T03:15:00-04:00",
        exit_fill_at="2026-03-20T03:20:00-04:00",
        entry_price="100.0",
        exit_price="101.0",
        entry_bar_id="bar-3",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-22T13:50:00-04:00",
                "last_processed_bar_end_ts": "2026-03-22T13:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "gc_signal",
                        "display_name": "GC Signal",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "risk_state": "OK",
                        "session_realized_pnl": "30.0",
                        "session_unrealized_pnl": "0",
                        "session_total_pnl": "30.0",
                        "point_value": "10",
                        "database_url": f"sqlite:///{lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    likelihood_rows = snapshot["paper"]["strategy_performance"]["execution_likelihood"]["rows"]
    assert len(likelihood_rows) == 1
    row = likelihood_rows[0]
    assert row["entry_count"] == 3
    assert row["entries_by_session_bucket"]["ASIA_EARLY"] == 2
    assert row["entries_by_session_bucket"]["LONDON_OPEN"] == 1
    assert row["most_common_session_bucket"] == "ASIA_EARLY"
    assert row["expected_fire_cadence"] in {"frequent", "occasional", "rare"}
    assert "ASIA_EARLY" in row["most_likely_next_window"]
    assert row["operator_interpretation_state"] == "outside_usual_window"

    strategy_performance_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    strategy_trade_log_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json"
    strategy_attribution_path = repo_root / "outputs" / "operator_dashboard" / "paper_strategy_attribution_snapshot.json"
    assert strategy_performance_path.exists()
    assert strategy_trade_log_path.exists()
    assert strategy_attribution_path.exists()
    assert service.operator_artifact_file("paper-strategy-performance")[0] == strategy_performance_path


def test_dashboard_snapshot_builds_signal_intent_fill_audit_verdicts(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    root_paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(root_paper_db)

    no_setup_db = repo_root / "paper__no_setup.sqlite3"
    gated_db = repo_root / "paper__gated.sqlite3"
    intent_db = repo_root / "paper__intent.sqlite3"
    filled_db = repo_root / "paper__filled.sqlite3"
    mismatch_db = repo_root / "paper__mismatch.sqlite3"
    sparse_db = repo_root / "paper__sparse.sqlite3"
    for path in (no_setup_db, gated_db, intent_db, filled_db, mismatch_db, sparse_db):
        _init_empty_dashboard_db(path)

    _append_dashboard_bar(
        no_setup_db,
        bar_id="no-setup-bar-1",
        symbol="MGC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )

    _append_dashboard_bar(
        gated_db,
        bar_id="gated-bar-1",
        symbol="GC",
        start_ts="2026-03-23T09:35:00-04:00",
        end_ts="2026-03-23T09:40:00-04:00",
    )
    _append_dashboard_signal(
        gated_db,
        bar_id="gated-bar-1",
        created_at="2026-03-23T09:40:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "bullSnap",
            "short_entry_source": None,
        },
    )

    _append_dashboard_bar(
        intent_db,
        bar_id="intent-bar-1",
        symbol="SI",
        start_ts="2026-03-23T09:40:00-04:00",
        end_ts="2026-03-23T09:45:00-04:00",
    )
    _append_dashboard_signal(
        intent_db,
        bar_id="intent-bar-1",
        created_at="2026-03-23T09:45:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "asiaVwapReclaim",
            "short_entry_source": None,
        },
    )
    _append_dashboard_intent(
        intent_db,
        order_intent_id="intent-only-1",
        bar_id="intent-bar-1",
        symbol="SI",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:45:05-04:00",
        reason_code="asiaVwapReclaim",
        broker_order_id="intent-only-broker",
        order_status="SUBMITTED",
    )

    _append_dashboard_bar(
        filled_db,
        bar_id="filled-bar-1",
        symbol="CL",
        start_ts="2026-03-23T09:45:00-04:00",
        end_ts="2026-03-23T09:50:00-04:00",
    )
    _append_dashboard_signal(
        filled_db,
        bar_id="filled-bar-1",
        created_at="2026-03-23T09:50:00-04:00",
        payload={
            "long_entry_raw": False,
            "short_entry_raw": True,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": False,
            "short_entry": True,
            "long_entry_source": None,
            "short_entry_source": "bearSnap",
        },
    )
    _append_dashboard_intent(
        filled_db,
        order_intent_id="filled-1",
        bar_id="filled-bar-1",
        symbol="CL",
        intent_type="SELL_TO_OPEN",
        created_at="2026-03-23T09:50:05-04:00",
        reason_code="bearSnap",
        broker_order_id="filled-broker-1",
        order_status="FILLED",
    )
    _append_dashboard_fill(
        filled_db,
        order_intent_id="filled-1",
        intent_type="SELL_TO_OPEN",
        order_status="FILLED",
        fill_timestamp="2026-03-23T09:55:00-04:00",
        fill_price="100.0",
        broker_order_id="filled-broker-1",
    )

    _append_dashboard_bar(
        mismatch_db,
        bar_id="mismatch-bar-1",
        symbol="PL",
        start_ts="2026-03-23T09:50:00-04:00",
        end_ts="2026-03-23T09:55:00-04:00",
    )
    _append_dashboard_signal(
        mismatch_db,
        bar_id="mismatch-bar-1",
        created_at="2026-03-23T09:55:00-04:00",
        payload={
            "long_entry_raw": True,
            "short_entry_raw": False,
            "recent_long_setup": False,
            "recent_short_setup": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_source": "bullSnap",
            "short_entry_source": None,
        },
    )
    _append_dashboard_intent(
        mismatch_db,
        order_intent_id="mismatch-1",
        bar_id="mismatch-bar-1",
        symbol="PL",
        intent_type="BUY_TO_OPEN",
        created_at="2026-03-23T09:55:05-04:00",
        reason_code="bullSnap",
        broker_order_id="mismatch-broker-1",
        order_status="FILLED",
    )
    _append_dashboard_fill(
        mismatch_db,
        order_intent_id="mismatch-1",
        intent_type="BUY_TO_OPEN",
        order_status="FILLED",
        fill_timestamp="2026-03-23T10:00:00-04:00",
        fill_price="101.0",
        broker_order_id="mismatch-broker-1",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T10:05:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T10:00:00-04:00",
                "position_side": "MULTI",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "no_setup",
                        "display_name": "MGC Quiet",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{no_setup_db}",
                    },
                    {
                        "lane_id": "gated_lane",
                        "display_name": "GC Gated",
                        "symbol": "GC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": False,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "eligibility_reason": "entries_disabled",
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{gated_db}",
                    },
                    {
                        "lane_id": "intent_lane",
                        "display_name": "SI Intent",
                        "symbol": "SI",
                        "approved_long_entry_sources": ["asiaVwapReclaim"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{intent_db}",
                    },
                    {
                        "lane_id": "filled_lane",
                        "display_name": "CL Filled",
                        "symbol": "CL",
                        "approved_long_entry_sources": [],
                        "approved_short_entry_sources": ["bearSnap"],
                        "position_side": "SHORT",
                        "strategy_status": "IN_SHORT_K",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "session_unrealized_pnl": "0",
                        "database_url": f"sqlite:///{filled_db}",
                    },
                    {
                        "lane_id": "mismatch_lane",
                        "display_name": "PL Surfacing Gap",
                        "symbol": "PL",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{mismatch_db}",
                    },
                    {
                        "lane_id": "sparse_lane",
                        "display_name": "HG Sparse",
                        "symbol": "HG",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{sparse_db}",
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": root_paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    audit_rows = {row["lane_id"]: row for row in snapshot["paper"]["signal_intent_fill_audit"]["rows"]}
    assert audit_rows["no_setup"]["audit_verdict"] == "NO_SETUP_OBSERVED"
    assert audit_rows["no_setup"]["standalone_strategy_id"] == "bull_snap__MGC"
    assert audit_rows["gated_lane"]["audit_verdict"] == "SETUP_GATED"
    assert audit_rows["intent_lane"]["audit_verdict"] == "INTENT_NO_FILL_YET"
    assert audit_rows["filled_lane"]["audit_verdict"] == "FILLED"
    assert audit_rows["mismatch_lane"]["audit_verdict"] == "SURFACING_MISMATCH_SUSPECTED"
    assert audit_rows["sparse_lane"]["audit_verdict"] == "INSUFFICIENT_HISTORY"
    assert "entries were disabled" in audit_rows["gated_lane"]["audit_reason"]
    assert audit_rows["intent_lane"]["last_order_intent_id"] == "intent-only-1"
    assert audit_rows["filled_lane"]["last_fill_broker_order_id"] == "filled-broker-1"
    assert audit_rows["mismatch_lane"]["trade_log_rows_exist"] is False
    assert audit_rows["mismatch_lane"]["strategy_performance_row_exists"] is True
    assert audit_rows["no_setup"]["processed_bar_count"] == 1

    audit_summary = snapshot["paper"]["signal_intent_fill_audit"]["summary"]["verdict_counts"]
    assert audit_summary["NO_SETUP_OBSERVED"] == 1
    assert audit_summary["SETUP_GATED"] == 1
    assert audit_summary["INTENT_NO_FILL_YET"] == 1
    assert audit_summary["FILLED"] == 1
    assert audit_summary["SURFACING_MISMATCH_SUSPECTED"] == 1
    assert audit_summary["INSUFFICIENT_HISTORY"] == 1

    audit_path = repo_root / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    assert audit_path.exists()
    assert service.operator_artifact_file("paper-signal-intent-fill-audit")[0] == audit_path


def test_dashboard_snapshot_builds_recent_paper_history(tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine").mkdir(exist_ok=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_dashboard_db(shadow_db)
    _init_dashboard_db(paper_db)

    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T14:10:00-04:00",
                "last_processed_bar_end_ts": "2026-03-20T14:05:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "READY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "reconciliation": {
                    "broker_position_quantity": 0,
                    "broker_average_price": None,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "30.0",
                "session_date": "2026-03-20",
                "closed_trade_count": 2,
                "fill_count": 4,
                "allowed_branch_decisions_by_source": {
                    "asiaEarlyNormalBreakoutRetestHoldTurn": 4,
                    "usLatePauseResumeLongTurn": 2,
                },
                "blocked_branch_decisions_by_source": {"asiaEarlyPauseResumeShortTurn": 1},
                "flat_at_end": True,
                "reconciliation_clean": True,
                "unresolved_open_intents": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T10:00:00-04:00,2026-03-20T10:10:00-04:00,LONG,asiaEarlyNormalBreakoutRetestHoldTurn,100.0,101.0,10.0,LONG_TIME_EXIT\n"
        "2026-03-20T11:00:00-04:00,2026-03-20T11:10:00-04:00,LONG,usLatePauseResumeLongTurn,101.0,103.0,20.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.summary.json").write_text(
        json.dumps(
            {
                "realized_net_pnl": "-10.0",
                "session_date": "2026-03-19",
                "closed_trade_count": 2,
                "fill_count": 4,
                "allowed_branch_decisions_by_source": {"asiaEarlyPauseResumeShortTurn": 3},
                "blocked_branch_decisions_by_source": {"usLatePauseResumeLongTurn": 2},
                "flat_at_end": True,
                "reconciliation_clean": True,
                "unresolved_open_intents": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-19.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-19T10:00:00-04:00,2026-03-19T10:10:00-04:00,SHORT,asiaEarlyPauseResumeShortTurn,100.0,99.0,10.0,SHORT_TIME_EXIT\n"
        "2026-03-19T11:00:00-04:00,2026-03-19T11:10:00-04:00,LONG,usLatePauseResumeLongTurn,99.0,97.0,-20.0,LONG_TIME_EXIT\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else repo_root / "outputs" / "probationary_pattern_engine",
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }

    snapshot = service.snapshot()

    history = snapshot["paper"]["history"]
    assert history["recent_sessions"][0]["session_date"] == "2026-03-20"
    assert history["recent_sessions"][1]["session_date"] == "2026-03-19"
    assert history["comparison"]["latest_vs_prior_realized"] == "40.0"
    assert history["comparison"]["trend"] == "IMPROVING / LOW SAMPLE"
    assert history["comparison"]["recent_win_rate"] == "75.0%"
    assert history["distribution"]["best_session"] == "30.0"
    assert history["distribution"]["worst_session"] == "-10.0"
    assert history["drawdown"]["worst_drawdown"] == "10.0"
    assert any(row["branch"] == "usLatePauseResumeLongTurn" for row in history["branch_history"])
    assert all("stability" in row for row in history["branch_history"])
    history_path = repo_root / "outputs" / "operator_dashboard" / "paper_history_snapshot.json"
    assert history_path.exists()
    assert service.operator_artifact_file("paper-history")[0] == history_path


def test_dashboard_exposes_paper_canary_separately_from_approved_lanes(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_artifacts = paper_artifacts / "lanes" / "canary_gc_us_early_execution_once"
    (paper_artifacts / "daily").mkdir(parents=True)
    lane_artifacts.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    canary_db = repo_root / "paper__canary.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(canary_db)

    connection = sqlite3.connect(canary_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "canary-entry-intent",
                "bar-1",
                "GC",
                "BUY_TO_OPEN",
                1,
                "2026-03-20T09:35:30-04:00",
                "paperExecutionCanaryEntry",
                "paper-entry-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "canary-exit-intent",
                "bar-2",
                "GC",
                "SELL_TO_CLOSE",
                1,
                "2026-03-20T09:40:30-04:00",
                "paperExecutionCanaryExitNextBar",
                "paper-exit-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "canary-entry-intent",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-20T09:40:00-04:00",
                "3050.0",
                "paper-entry-1",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "canary-exit-intent",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-20T09:45:00-04:00",
                "3051.5",
                "paper-exit-1",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T09:45:05-04:00",
                "last_processed_bar_end_ts": "2026-03-20T09:45:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                "approved_short_entry_sources": [],
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                        "entries_enabled": True,
                        "database_url": f"sqlite:///{paper_db}",
                        "artifacts_dir": str(paper_artifacts / "lanes" / "mgc_us_late_pause_resume_long"),
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "entries_enabled": True,
                        "position_side": "FLAT",
                        "database_url": f"sqlite:///{canary_db}",
                        "artifacts_dir": str(lane_artifacts),
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "canary_gc_us_early_execution_once",
                "updated_at": "2026-03-20T09:45:05-04:00",
                "position_side": "FLAT",
                "last_processed_bar_end_ts": "2026-03-20T09:45:00-04:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-20T09:35:00-04:00",
                "source": "paperExecutionCanary",
                "lane_id": "canary_gc_us_early_execution_once",
                "symbol": "GC",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-20T09:45:01-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "risk_state": "OK",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,instrument,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T09:40:00-04:00,2026-03-20T09:45:00-04:00,LONG,paperExecutionCanaryEntry,GC,3050.0,3051.5,1.5,paperExecutionCanaryExitNextBar\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
        "status": {"session_date": "2026-03-20"},
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((paper_artifacts / "runtime" / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((paper_artifacts / "runtime" / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    approved_payload = service._paper_approved_models_payload(paper)
    canary_payload = service._paper_non_approved_lanes_payload(paper)

    assert approved_payload["total_count"] == 1
    assert approved_payload["rows"][0]["lane_id"] == "mgc_us_late_pause_resume_long"
    assert canary_payload["total_count"] == 1
    assert canary_payload["canary_count"] == 1
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "canary_gc_us_early_execution_once"
    assert canary_row["is_canary"] is True
    assert canary_row["non_approved"] is True
    assert canary_row["paper_only"] is True
    assert canary_row["instrument"] == "GC"
    assert canary_row["session_restriction"] == "US_EARLY_OBSERVATION"
    assert canary_row["fired"] is True
    assert canary_row["entry_completed"] is True
    assert canary_row["exit_completed"] is True
    assert canary_row["entry_state"] == "COMPLETE"
    assert canary_row["exit_state"] == "COMPLETE"
    assert canary_row["lifecycle_state"] == "ENTRY_AND_EXIT_COMPLETE"
    assert canary_row["latest_signal_label"].startswith("2026-03-20T09:35:00-04:00")
    assert canary_row["latest_fill_label"].startswith("2026-03-20T09:45:00-04:00")
    assert canary_payload["artifacts"]["snapshot"] == "/api/operator-artifact/paper-non-approved-lanes"


def test_dashboard_non_approved_canary_uses_config_in_force_lane_universe_when_operator_status_lags(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_artifacts = paper_artifacts / "lanes" / "canary_gc_us_early_execution_once"
    (paper_artifacts / "daily").mkdir(parents=True)
    lane_artifacts.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    canary_db = repo_root / "paper__canary.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(canary_db)

    connection = sqlite3.connect(canary_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "force-entry-intent",
                "bar-10",
                "GC",
                "BUY_TO_OPEN",
                1,
                "2026-03-20T12:45:00-04:00",
                "paperExecutionCanaryForceFireOnceEntry:proof",
                "paper-force-entry-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "force-exit-intent",
                "bar-11",
                "GC",
                "SELL_TO_CLOSE",
                1,
                "2026-03-20T12:50:00-04:00",
                "paperExecutionCanaryForceFireOnceExitNextBar:proof",
                "paper-force-exit-1",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "force-entry-intent",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-20T12:50:00-04:00",
                "3048.0",
                "paper-force-entry-1",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "force-exit-intent",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-20T12:55:00-04:00",
                "3049.0",
                "paper-force-exit-1",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-20T12:55:05-04:00",
                "last_processed_bar_end_ts": "2026-03-20T12:55:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                "approved_short_entry_sources": [],
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "approved_long_entry_sources": ["usLatePauseResumeLongTurn"],
                        "entries_enabled": True,
                        "database_url": f"sqlite:///{paper_db}",
                        "artifacts_dir": str(paper_artifacts / "lanes" / "mgc_us_late_pause_resume_long"),
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "canary_gc_us_early_execution_once",
                "updated_at": "2026-03-20T12:55:05-04:00",
                "position_side": "FLAT",
                "last_processed_bar_end_ts": "2026-03-20T12:55:00-04:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-20T12:45:00-04:00",
                "source": "paperExecutionCanaryForceFireOnce",
                "lane_id": "canary_gc_us_early_execution_once",
                "symbol": "GC",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lane_artifacts.joinpath("reconciliation_events.jsonl").write_text(
        json.dumps({"logged_at": "2026-03-20T12:55:01-04:00", "clean": True, "issues": []}) + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    (paper_artifacts / "runtime" / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "canary_force_fire_once_token": "proof",
                "lanes": [
                    {
                        "lane_id": "mgc_us_late_pause_resume_long",
                        "display_name": "MGC / usLatePauseResumeLongTurn",
                        "symbol": "MGC",
                        "session_restriction": "US_LATE",
                        "long_sources": ["usLatePauseResumeLongTurn"],
                    },
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "display_name": "CANARY / GC / paperExecutionLifecycleOnce / US_EARLY",
                        "symbol": "GC",
                        "session_restriction": "US_EARLY_OBSERVATION",
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                        "database_url": f"sqlite:///{canary_db}",
                        "artifacts_dir": str(lane_artifacts),
                    },
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "runtime" / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "canary_gc_us_early_execution_once",
                        "risk_state": "OK",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "daily" / "2026-03-20.blotter.csv").write_text(
        "entry_ts,exit_ts,direction,setup_family,instrument,entry_px,exit_px,net_pnl,exit_reason\n"
        "2026-03-20T12:50:00-04:00,2026-03-20T12:55:00-04:00,LONG,paperExecutionCanaryForceFireOnceEntry:proof,GC,3048.0,3049.0,100.0,paperExecutionCanaryForceFireOnceExitNextBar:proof\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
        "status": {"session_date": "2026-03-20"},
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((paper_artifacts / "runtime" / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((paper_artifacts / "runtime" / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
    }

    canary_payload = service._paper_non_approved_lanes_payload(paper)

    assert canary_payload["total_count"] == 1
    assert canary_payload["canary_count"] == 1
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "canary_gc_us_early_execution_once"
    assert canary_row["fired"] is True
    assert canary_row["entry_completed"] is True
    assert canary_row["exit_completed"] is True
    assert canary_row["latest_fill_label"].startswith("2026-03-20T12:55:00-04:00")


def test_dashboard_auto_clears_stale_atpe_decision_without_intent_when_overnight_fill_evidence_exists(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_artifacts / "runtime"
    paper_artifacts.mkdir(parents=True)
    runtime_dir.mkdir(parents=True)

    paper_db = repo_root / "paper.sqlite3"
    atpe_db = repo_root / "paper__atpe_long_medium_high_canary__MES.sqlite3"
    _init_empty_dashboard_db(paper_db)
    _init_empty_dashboard_db(atpe_db)

    connection = sqlite3.connect(atpe_db)
    try:
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "MES|1m|2026-03-26T03:36:00Z",
                "MES",
                "BUY_TO_OPEN",
                1,
                "2026-03-25T23:36:00-04:00",
                "trend_participation.pullback_continuation.long.conservative",
                "paper-MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
                "BUY_TO_OPEN",
                "FILLED",
                "2026-03-25T23:36:00-04:00",
                "6642.75",
                "paper-MES|1m|2026-03-26T03:36:00Z|BUY_TO_OPEN",
            ),
        )
        connection.execute(
            "insert into order_intents values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "operator-control|1774497240000|SELL_TO_CLOSE",
                "operator-control|1774497240000",
                "MES",
                "SELL_TO_CLOSE",
                1,
                "2026-03-25T23:54:00-04:00",
                "atpe_time_stop",
                "paper-operator-control|1774497240000|SELL_TO_CLOSE",
                "FILLED",
            ),
        )
        connection.execute(
            "insert into fills (order_intent_id, intent_type, order_status, fill_timestamp, fill_price, broker_order_id) values (?, ?, ?, ?, ?, ?)",
            (
                "operator-control|1774497240000|SELL_TO_CLOSE",
                "SELL_TO_CLOSE",
                "FILLED",
                "2026-03-25T23:54:00-04:00",
                "6638.75",
                "paper-operator-control|1774497240000|SELL_TO_CLOSE",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    (paper_artifacts / "branch_sources.jsonl").write_text(
        json.dumps(
            {
                "bar_end_ts": "2026-03-26T03:36:04.286065+00:00",
                "logged_at": "2026-03-26T03:36:04.286065+00:00",
                "source": "trend_participation.pullback_continuation.long.conservative",
                "lane_id": "atpe_long_medium_high_canary__MES",
                "symbol": "MES",
                "decision": "allowed",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (paper_artifacts / "rule_blocks.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "reconciliation_events.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "operator_controls.jsonl").write_text("", encoding="utf-8")
    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-26T04:09:25.170322-04:00",
                "last_processed_bar_end_ts": "2026-03-26T04:09:00-04:00",
                "position_side": "FLAT",
                "entries_enabled": True,
                "operator_halt": False,
                "strategy_status": "RUNNING_MULTI_LANE",
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "display_name": "ATPE Long Medium+High Canary / MES",
                        "symbol": "MES",
                        "approved_long_entry_sources": ["trend_participation.pullback_continuation.long.conservative"],
                        "entries_enabled": True,
                        "position_side": "FLAT",
                        "internal_position_qty": 0,
                        "broker_position_qty": 0,
                        "open_order_count": 0,
                        "fill_count": 2,
                        "intent_count": 2,
                        "database_url": f"sqlite:///{atpe_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "display_name": "ATPE Long Medium+High Canary / MES",
                        "symbol": "MES",
                        "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                        "lane_mode": "PAPER_EXECUTION_CANARY",
                        "runtime_kind": "atpe_canary_observer",
                        "database_url": f"sqlite:///{atpe_db}",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "paper_lane_risk_status.json").write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "atpe_long_medium_high_canary__MES",
                        "risk_state": "HALTED_DEGRADATION",
                        "halt_reason": "lane_realized_loser_limit_per_session",
                        "unblock_action": "Next session reset required",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "db_path": str(paper_db),
        "status": {
            "session_date": "2026-03-26",
            "last_update_ts": "2026-03-26T08:30:11.670560+00:00",
            "reconciliation_clean": True,
            "entries_enabled": True,
        },
        "running": True,
        "raw_operator_status": json.loads((paper_artifacts / "operator_status.json").read_text(encoding="utf-8")),
        "config_in_force": json.loads((runtime_dir / "paper_config_in_force.json").read_text(encoding="utf-8")),
        "lane_risk": json.loads((runtime_dir / "paper_lane_risk_status.json").read_text(encoding="utf-8")),
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": operator_dashboard_module._latest_table_rows_across_paths([atpe_db], "fills", "fill_timestamp", 25),
        "latest_intents": operator_dashboard_module._latest_table_rows_across_paths([atpe_db], "order_intents", "created_at", 25),
        "daily_summary": None,
        "position": {"side": "FLAT", "quantity": 0},
        "operator_state": {"operator_halt": False},
        "performance": {"branch_performance": []},
    }

    approved_payload = service._paper_approved_models_payload(paper)
    detail = approved_payload["details_by_branch"]["ATPE Long Medium+High Canary / MES"]
    assert detail["intent_count"] == 1
    assert detail["fill_count"] == 1
    assert detail["latest_intent_timestamp"] == "2026-03-25T23:36:00-04:00"
    assert detail["latest_fill_timestamp"] == "2026-03-25T23:36:00-04:00"
    assert detail["chain_state"] == "FILLED_CLOSED"

    paper_with_models = {**paper, "approved_models": approved_payload}
    exceptions_payload = service._paper_exceptions_payload(paper_with_models, {"links": {}})
    assert not {
        row["code"]
        for row in exceptions_payload["exceptions"]
        if row["code"] in {"DECISION_WITHOUT_INTENT", "MODEL_SIGNAL_SEEN_BUT_NEVER_PROGRESSING"}
    }
    assert exceptions_payload["session_verdict"] == "RUNNING_CLEAN"


def test_dashboard_non_approved_payload_merges_experimental_canary_snapshot(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_experimental_canary_snapshot(repo_root)
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    paper_artifacts.mkdir(parents=True)

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-23"},
        "raw_operator_status": {"lanes": []},
        "config_in_force": {"lanes": []},
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": load_experimental_canaries_snapshot(
            repo_root / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine" / "experimental_canaries_snapshot.json"
        ),
    }

    canary_payload = service._paper_non_approved_lanes_payload(paper)
    temporary_payload = service._paper_temporary_paper_strategies_payload(canary_payload)

    assert canary_payload["total_count"] == 1
    assert canary_payload["experimental_count"] == 1
    assert canary_payload["temporary_paper_count"] == 1
    assert canary_payload["enabled_count"] == 1
    assert canary_payload["disabled_count"] == 0
    assert canary_payload["kill_switch_active"] is False
    assert canary_payload["recent_signal_count"] == 2
    assert canary_payload["recent_event_count"] == 1
    assert canary_payload["operator_state_label"] == "ENABLED (PAPER ONLY)"
    assert canary_payload["artifacts"]["experimental_snapshot"] == "/api/operator-artifact/experimental-canaries"
    canary_row = canary_payload["rows"][0]
    assert canary_row["lane_id"] == "atpe_long_medium_high_canary"
    assert canary_row["experimental_status"] == "experimental_canary"
    assert canary_row["instrument"] == "MES/MNQ"
    assert canary_row["quality_bucket_policy"] == "MEDIUM_HIGH_ONLY"
    assert canary_row["recent_signal_count"] == 2
    assert canary_row["recent_event_count"] == 1
    assert canary_row["state"] == "ENABLED"
    assert canary_row["temporary_paper_strategy"] is True
    assert canary_row["paper_strategy_class"] == "temporary_paper_strategy"
    assert canary_row["metrics_bucket"] == "experimental_temporary_paper"
    assert canary_row["runtime_instance_present"] is False
    assert canary_row["runtime_state_loaded"] is False
    assert canary_row["snapshot_only"] is True
    assert canary_row["allow_block_override_summary"]["label"] == "allowed=1 blocked=1 override=paper_only_experimental_canary"
    assert canary_row["atp_bias_state"] == "LONG_BIAS"
    assert canary_row["atp_pullback_state"] == "NORMAL_PULLBACK"
    assert canary_row["latest_atp_state"]["pullback_depth_score"] == 0.82
    assert canary_row["atp_entry_state"] == "ENTRY_ELIGIBLE"
    assert canary_row["atp_primary_blocker"] is None
    assert canary_row["atp_continuation_trigger_state"] == "CONTINUATION_TRIGGER_CONFIRMED"
    assert canary_row["atp_timing_state"] == "ATP_TIMING_CONFIRMED"
    assert canary_row["atp_vwap_price_quality_state"] == "VWAP_FAVORABLE"
    assert "bias=LONG_BIAS" in canary_row["operator_status_line"]
    assert "entry=ENTRY_ELIGIBLE" in canary_row["operator_status_line"]
    assert "timing=ATP_TIMING_CONFIRMED" in canary_row["operator_status_line"]
    assert canary_row["note"].startswith("Experimental Paper Strategy | Paper Only")
    assert temporary_payload["total_count"] == 1
    assert temporary_payload["enabled_count"] == 1
    assert temporary_payload["metrics_bucket"] == "experimental_temporary_paper"
    assert temporary_payload["rows"][0]["lane_id"] == "atpe_long_medium_high_canary"

    integrity_payload = service._paper_temporary_paper_runtime_integrity_payload(
        {
            **paper,
            "non_approved_lanes": canary_payload,
            "temporary_paper_strategies": temporary_payload,
            "runtime_registry": {"rows": []},
        }
    )
    assert integrity_payload["enabled_in_app_count"] == 1
    assert integrity_payload["loaded_in_runtime_count"] == 0
    assert integrity_payload["snapshot_only_count"] == 1
    assert integrity_payload["mismatch_status"] == "MISMATCH"
    assert integrity_payload["missing_lane_ids"] == ["atpe_long_medium_high_canary"]
    assert integrity_payload["start_flags"] == ["--include-atpe-canary"]


def test_tracked_paper_strategy_payload_registers_live_attached_atp_benchmark_from_persisted_runtime_truth(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
            {"bar_id": "bar-2", "symbol": "MGC", "end_ts": "2026-03-23T14:40:00-04:00", "close": "101.25"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:40:00-04:00",
                "duplicate_bar_suppression_count": 1,
                "entry_model": "CURRENT_CANDLE_VWAP",
                "active_entry_model": "CURRENT_CANDLE_VWAP",
                "supported_entry_models": ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"],
                "entry_model_supported": True,
                "execution_truth_emitter": "atp_phase3_timing_emitter",
                "intrabar_execution_authoritative": True,
                "authoritative_intrabar_available": True,
                "authoritative_entry_truth_available": True,
                "authoritative_exit_truth_available": True,
                "authoritative_trade_lifecycle_available": True,
                "lifecycle_records": [
                    {
                        "trade_id": "atp-trade-1",
                        "decision_id": "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00",
                        "decision_ts": "2026-03-23T14:31:00-04:00",
                        "entry_ts": "2026-03-23T14:31:10-04:00",
                        "exit_ts": "2026-03-23T14:41:10-04:00",
                        "entry_price": "100.25",
                        "exit_price": "101.25",
                        "primary_exit_reason": "atpe_target",
                        "exit_reason": "atpe_target",
                        "setup_signature": "benchmark-setup",
                        "setup_state_signature": "benchmark-state",
                        "family": "atp_v1_long_pullback_continuation",
                        "entry_source_family": "atp_v1_long_pullback_continuation",
                        "side": "LONG",
                        "decision_context_linkage_available": True,
                        "decision_context_linkage_status": "AVAILABLE",
                        "entry_model": "CURRENT_CANDLE_VWAP",
                        "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                        "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                        "truth_provenance": {
                            "runtime_context": "PAPER",
                            "run_lane": "PAPER_RUNTIME",
                        },
                    }
                ],
                "authoritative_trade_lifecycle_records": [
                    {
                        "trade_id": "atp-trade-1",
                        "decision_id": "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00",
                        "decision_ts": "2026-03-23T14:31:00-04:00",
                        "entry_ts": "2026-03-23T14:31:10-04:00",
                        "exit_ts": "2026-03-23T14:41:10-04:00",
                        "entry_price": "100.25",
                        "exit_price": "101.25",
                        "primary_exit_reason": "atpe_target",
                        "exit_reason": "atpe_target",
                        "setup_signature": "benchmark-setup",
                        "setup_state_signature": "benchmark-state",
                        "family": "atp_v1_long_pullback_continuation",
                        "entry_source_family": "atp_v1_long_pullback_continuation",
                        "side": "LONG",
                        "decision_context_linkage_available": True,
                        "decision_context_linkage_status": "AVAILABLE",
                        "entry_model": "CURRENT_CANDLE_VWAP",
                        "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                        "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                        "truth_provenance": {
                            "runtime_context": "PAPER",
                            "run_lane": "PAPER_RUNTIME",
                        },
                    }
                ],
                "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                "lifecycle_truth_class": "FULL_AUTHORITATIVE_LIFECYCLE",
                "unsupported_reason": None,
                "truth_provenance": {
                    "runtime_context": "PAPER",
                    "run_lane": "PAPER_RUNTIME",
                    "artifact_context": "ATP_COMPANION_PAPER_RUNTIME_STATUS",
                    "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
                    "study_mode": "paper_runtime",
                    "artifact_rebuilt": False,
                },
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
                "latest_atp_entry_state": {"entry_state": "ENTRY_ELIGIBLE", "primary_blocker": None},
                "latest_atp_timing_state": {"timing_state": "ATP_TIMING_CONFIRMED", "primary_blocker": None},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_jsonl_rows(
        lane_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "atp-entry-1",
                "created_at": "2026-03-23T14:31:00-04:00",
                "intent_type": "BUY_TO_OPEN",
                "reason_code": "atp_v1_long_pullback_continuation",
                "order_status": "FILLED",
            }
        ],
    )
    _write_jsonl_rows(
        lane_dir / "fills.jsonl",
        [
            {
                "order_intent_id": "atp-entry-1",
                "fill_timestamp": "2026-03-23T14:31:10-04:00",
                "intent_type": "BUY_TO_OPEN",
                "fill_price": "100.25",
                "order_status": "FILLED",
            }
        ],
    )
    _write_jsonl_rows(
        lane_dir / "trades.jsonl",
        [
            {
                "trade_id": "atp-trade-1",
                "symbol": "MGC",
                "direction": "LONG",
                "entry_timestamp": "2026-03-23T14:31:10-04:00",
                "exit_timestamp": "2026-03-23T14:41:10-04:00",
                "entry_price": "100.25",
                "exit_price": "101.25",
                "realized_pnl": "10.0",
                "exit_reason": "atpe_target",
                "setup_family": "atp_v1_long_pullback_continuation",
            }
        ],
    )

    db_path = repo_root / "mgc_v05l.probationary.paper__atp_companion_v1_asia_us.sqlite3"
    _init_empty_dashboard_db(db_path)
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": True,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "temporary_paper_strategy": True,
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "database_url": f"sqlite:///{db_path}",
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                        "order_intents": str((lane_dir / "order_intents.jsonl").resolve()),
                        "fills": str((lane_dir / "fills.jsonl").resolve()),
                        "trades": str((lane_dir / "trades.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {
            "trade_log": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "side": "LONG",
                    "entry_timestamp": "2026-03-23T14:31:10-04:00",
                    "exit_timestamp": "2026-03-23T14:41:10-04:00",
                    "exit_reason": "atpe_target",
                    "net_pnl": "10.0",
                    "signal_family": "active_trend_participation_engine",
                    "signal_family_label": "ATP Companion",
                    "entry_session_phase": "US_LATE",
                }
            ]
        },
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    assert tracked_payload["total_count"] == 1
    row = tracked_payload["rows"][0]
    detail = tracked_payload["details_by_strategy_id"]["atp_companion_v1_asia_us"]
    assert row["display_name"] == "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only"
    assert row["internal_label"] == "ATP_COMPANION_V1_ASIA_US"
    assert row["environment"] == "paper"
    assert row["benchmark_designation"] == "CURRENT_ATP_COMPANION_BENCHMARK"
    assert row["status"] == "READY"
    assert row["entries_enabled"] is True
    assert row["session_allowed"] is True
    assert row["runtime_attached"] is True
    assert row["data_stale"] is False
    assert row["latest_processed_bar_timestamp"] == "2026-03-23T14:40:00-04:00"
    assert row["realized_pnl"] == "10.0"
    assert row["current_day_pnl"] == "10.0"
    assert row["profit_factor"] == "999"
    assert row["trade_family_breakdown"][0]["family"] == "ATP Companion"
    assert row["session_breakdown"][0]["session"] == "US_LATE"
    assert row["last_trade_summary"]["family"] == "ATP Companion"
    assert row["lane_count"] == 1
    assert row["observed_instruments"] == ["MGC"]
    assert row["health_flags"]["duplicate_bar_suppression_count"] == 1
    assert row["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert row["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert row["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert row["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert row["authoritative_intrabar_available"] is True
    assert row["authoritative_entry_truth_available"] is True
    assert row["authoritative_exit_truth_available"] is True
    assert row["authoritative_trade_lifecycle_available"] is True
    assert row["authoritative_trade_lifecycle_records"][0]["trade_id"] == "atp-trade-1"
    assert row["authoritative_trade_lifecycle_records"][0]["decision_id"] == "MGC|atp_v1_long_pullback_continuation|2026-03-23T14:31:00-04:00"
    assert row["authoritative_trade_lifecycle_records"][0]["decision_context_linkage_status"] == "AVAILABLE"
    assert row["pnl_truth_basis"] == "PAPER_RUNTIME_LEDGER"
    assert row["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert row["truth_provenance"]["run_lane"] == "PAPER_RUNTIME"
    assert detail["authoritative_trade_lifecycle_records"][0]["primary_exit_reason"] == "atpe_target"
    assert detail["recent_trades"][0]["decision_ts"] == "2026-03-23T14:31:00-04:00"
    assert detail["recent_bars"][0]["bar_id"] == "bar-2"
    assert detail["recent_order_intents"][0]["order_intent_id"] == "atp-entry-1"
    assert detail["recent_fills"][0]["fill_price"] == "100.25"
    assert detail["recent_trades"][0]["primary_exit_reason"] == "atpe_target"
    assert [lane["lane_id"] for lane in detail["constituent_lanes"]] == ["atp_companion_v1_asia_us"]
    assert detail["config_identity"]["config_source"].endswith("probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml")
    assert detail["config_identity"]["allowed_sessions"] == ["ASIA", "US"]
    assert detail["config_identity"]["diagnostic_only_sessions"] == ["LONDON"]


def test_tracked_paper_strategy_payload_marks_atp_benchmark_reconciling_when_runtime_detached_or_stale(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:30:00-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:30:00-04:00",
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "data_stale": True,
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "temporary_paper_strategy": True,
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": False,
                    "runtime_state_loaded": True,
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["status"] == "RECONCILING"
    assert row["runtime_attached"] is False
    assert row["data_stale"] is True
    assert "reattach" in str(row["status_reason"]).lower()


def test_tracked_paper_strategy_payload_marks_open_pnl_unavailable_without_trusted_mark(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "temporary_paper_strategy": True,
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "position_side": "LONG",
                    "entry_price": "100.0",
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {},
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["open_pnl"] is None
    assert row["open_pnl_supported"] is False
    assert row["open_pnl_unavailable_reason"] == (
        "Tracked paper strategy does not currently have a trusted latest mark/reference price for the open position."
    )


def test_tracked_paper_strategy_payload_treats_lane_local_runtime_instance_as_attached_even_without_global_running_flag(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "temporary_paper_strategy": True,
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "operator_status_payload": json.loads((lane_dir / "operator_status.json").read_text(encoding="utf-8")),
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["runtime_attached"] is True
    assert row["status"] == "READY"
    assert row["data_stale"] is False


def test_tracked_paper_strategy_payload_prefers_operator_status_entries_enabled_truth(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    operator_payload = {
        "updated_at": "2026-03-23T19:44:30-04:00",
        "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
        "runtime_attached": True,
        "entries_enabled": False,
        "operator_halt": True,
        "warmup_complete": True,
        "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
        "latest_atp_state": {"bias_state": "LONG_BIAS"},
    }
    (lane_dir / "operator_status.json").write_text(
        json.dumps(operator_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {
            "rows": [
                {
                    "lane_id": "atp_companion_v1_asia_us",
                    "display_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                    "instrument": "MGC",
                    "runtime_kind": "atp_companion_benchmark_paper",
                    "strategy_family": "active_trend_participation_engine",
                    "temporary_paper_strategy": True,
                    "entries_enabled": True,
                    "state": "ENABLED",
                    "runtime_instance_present": True,
                    "runtime_state_loaded": True,
                    "operator_status_payload": operator_payload,
                    "artifacts": {
                        "processed_bars": str((lane_dir / "processed_bars.jsonl").resolve()),
                    },
                }
            ]
        },
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    assert row["entries_enabled"] is False
    assert row["enabled"] is False
    assert row["operator_halt"] is True
    assert row["runtime_attached"] is True


def test_tracked_paper_strategy_payload_falls_back_to_lane_artifacts_when_dashboard_rows_are_missing(tmp_path: Path) -> None:
    repo_root = tmp_path
    lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "atp_companion_v1_asia_us"
    lane_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl_rows(
        lane_dir / "processed_bars.jsonl",
        [
            {"bar_id": "bar-1", "symbol": "MGC", "end_ts": "2026-03-23T14:35:00-04:00", "close": "100.75"},
        ],
    )
    _write_jsonl_rows(
        lane_dir / "signals.jsonl",
        [
            {"signal_timestamp": "2026-03-23T14:35:00-04:00", "decision": "blocked"},
        ],
    )
    _write_jsonl_rows(
        lane_dir / "trades.jsonl",
        [
            {
                "trade_id": "atp-trade-1",
                "symbol": "MGC",
                "direction": "LONG",
                "entry_timestamp": "2026-03-23T14:31:10-04:00",
                "exit_timestamp": "2026-03-23T14:41:10-04:00",
                "entry_price": "100.25",
                "exit_price": "101.25",
                "realized_pnl": "10.0",
                "exit_reason": "atp_companion_target",
                "strategy_name": "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only",
                "status": "CLOSED",
            }
        ],
    )
    (lane_dir / "runtime_state.json").write_text(json.dumps({"duplicate_bar_suppression_count": 0}), encoding="utf-8")
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T19:44:30-04:00",
                "runtime_heartbeat_at": "2026-03-23T19:44:30-04:00",
                "runtime_attached": True,
                "entries_enabled": True,
                "operator_halt": False,
                "warmup_complete": True,
                "last_processed_bar_end_ts": "2026-03-23T14:35:00-04:00",
                "duplicate_bar_suppression_count": 0,
                "latest_atp_state": {"bias_state": "LONG_BIAS"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    db_path = repo_root / "mgc_v05l.probationary.paper__atp_companion_v1_asia_us.sqlite3"
    _init_empty_dashboard_db(db_path)
    paper = {
        "artifacts_dir": str((repo_root / "outputs" / "probationary_pattern_engine" / "paper_session").resolve()),
        "running": False,
        "status": {"session_date": "2026-03-23", "current_detected_session": "US_LATE"},
        "temporary_paper_strategies": {"rows": []},
        "non_approved_lanes": {"rows": []},
        "strategy_performance": {"trade_log": []},
    }

    tracked_payload = build_tracked_paper_strategies_payload(
        repo_root=repo_root,
        paper=paper,
        generated_at="2026-03-23T19:45:00-04:00",
    )

    row = tracked_payload["rows"][0]
    detail = tracked_payload["details_by_strategy_id"]["atp_companion_v1_asia_us"]
    assert row["runtime_attached"] is True
    assert row["entries_enabled"] is True
    assert row["lane_count"] == 1
    assert row["latest_processed_bar_timestamp"] == "2026-03-23T14:35:00-04:00"
    assert detail["recent_bars"][0]["bar_id"] == "bar-1"
    assert detail["recent_trades"][0]["exit_reason"] == "atp_companion_target"


def test_dashboard_non_approved_payload_marks_gc_mgc_temp_paper_runtime_rows_as_temporary_strategy(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "gc_mgc_london_open_acceptance_continuation_long__GC"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps({"display_name": "GC/MGC London-Open Acceptance Continuation Long / GC", "position_side": "FLAT"}),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                    "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                    "symbol": "GC",
                    "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    "entries_enabled": True,
                    "position_side": "FLAT",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                    "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                    "symbol": "GC",
                    "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    "long_sources": ["gc_mgc_london_open_acceptance_continuation_long"],
                    "experimental_status": "experimental_temp_paper",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "LONG",
                    "observer_variant_id": "gc_mgc_london_open_acceptance_continuation_long",
                    "session_restriction": "LONDON_OPEN",
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": {"rows": [], "generated_at": "2026-03-24T07:30:00+00:00", "kill_switch": {"active": False}},
    }

    payload = service._paper_non_approved_lanes_payload(paper)

    assert payload["total_count"] == 1
    row = payload["rows"][0]
    assert row["lane_id"] == "gc_mgc_london_open_acceptance_continuation_long__GC"
    assert row["temporary_paper_strategy"] is True
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["metrics_bucket"] == "experimental_temporary_paper"
    assert row["experimental_status"] == "experimental_temp_paper"
    assert row["display_name"] == "GC/MGC London-Open Acceptance Continuation Long / GC"


def test_dashboard_non_approved_payload_uses_live_temp_paper_runtime_counts(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "atpe_long_medium_high_canary__MES"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "atpe_long_medium_high_canary__MES",
                "display_name": "ATPE Long Medium+High Canary / MES",
                "updated_at": "2026-03-24T15:55:33-04:00",
                "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                "signal_count": 23,
                "intent_count": 30,
                "fill_count": 30,
                "closed_trades": 15,
                "session_realized_pnl": "-133.750",
                "position_side": "FLAT",
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atpe_long_medium_high_canary__MES",
                    "display_name": "ATPE Long Medium+High Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "entries_enabled": True,
                    "position_side": "FLAT",
                    "recent_signal_count": 23,
                    "intent_count": 30,
                    "fill_count": 30,
                    "closed_trades": 15,
                    "session_realized_pnl": "-133.750",
                    "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atpe_long_medium_high_canary__MES",
                    "display_name": "ATPE Long Medium+High Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                    "experimental_status": "experimental_canary",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "LONG",
                    "observer_variant_id": "trend_participation.pullback_continuation.long.conservative",
                    "session_restriction": "ASIA/LONDON/US",
                    "artifacts_dir": str(lane_dir),
                }
            ]
        },
        "lane_risk": {"lanes": []},
        "events": {"branch_sources": [], "rule_blocks": [], "operator_controls": [], "reconciliation": []},
        "latest_fills": [],
        "latest_intents": [],
        "daily_summary": None,
        "position": {"side": "FLAT"},
        "operator_state": {},
        "performance": {"branch_performance": []},
        "experimental_canaries": {"rows": [], "generated_at": "2026-03-24T19:55:40+00:00", "kill_switch": {"active": False}},
    }

    payload = service._paper_non_approved_lanes_payload(paper)

    assert payload["total_count"] == 1
    row = payload["rows"][0]
    assert row["lane_id"] == "atpe_long_medium_high_canary__MES"
    assert row["intent_count"] == 30
    assert row["fill_count"] == 30
    assert row["trade_count"] == 15
    assert row["entry_completed"] is True
    assert row["exit_completed"] is True
    assert row["lifecycle_state"] == "ENTRY_AND_EXIT_COMPLETE"
    assert row["can_process_bars"] is True
    assert row["realized_pnl"] == "-133.750"


def test_dashboard_signal_intent_fill_audit_uses_lane_id_identity_for_temp_paper(tmp_path: Path) -> None:
    repo_root = tmp_path
    paper_artifacts = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    lane_dir = paper_artifacts / "lanes" / "atpe_short_high_only_canary__MES"
    (paper_artifacts / "runtime").mkdir(parents=True, exist_ok=True)
    lane_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "operator_status.json").write_text(
        json.dumps(
            {
                "lane_id": "atpe_short_high_only_canary__MES",
                "display_name": "ATPE Short High-Only Canary / MES",
                "updated_at": "2026-03-24T15:55:34-04:00",
                "last_processed_bar_end_ts": "2026-03-24T15:55:00-04:00",
                "position_side": "FLAT",
            }
        ),
        encoding="utf-8",
    )
    service = OperatorDashboardService(repo_root)
    paper = {
        "artifacts_dir": str(paper_artifacts),
        "status": {"session_date": "2026-03-24"},
        "raw_operator_status": {
            "lanes": [
                {
                    "lane_id": "atpe_short_high_only_canary__MES",
                    "display_name": "ATPE Short High-Only Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "strategy_status": "RUNNING_PAPER_ONLY_EXPERIMENTAL_CANARY",
                    "entries_enabled": True,
                    "paper_only": True,
                    "non_approved": True,
                    "experimental_status": "experimental_canary",
                    "position_side": "FLAT",
                }
            ]
        },
        "config_in_force": {
            "lanes": [
                {
                    "lane_id": "atpe_short_high_only_canary__MES",
                    "display_name": "ATPE Short High-Only Canary / MES",
                    "symbol": "MES",
                    "runtime_kind": "atpe_canary_observer",
                    "short_sources": ["trend_participation.failed_countertrend_resumption.short.active"],
                    "experimental_status": "experimental_canary",
                    "paper_only": True,
                    "non_approved": True,
                    "observer_side": "SHORT",
                }
            ]
        },
        "strategy_performance": {"rows": [], "trade_log": []},
    }

    payload = service._paper_signal_intent_fill_audit_payload(
        paper=paper,
        session_date="2026-03-24",
        root_db_path=None,
    )
    row = payload["rows"][0]
    assert row["lane_id"] == "atpe_short_high_only_canary__MES"
    assert row["standalone_strategy_id"] == "atpe_short_high_only_canary__MES"
    assert row["paper_strategy_class"] == "temporary_paper_strategy"
    assert row["temporary_paper_strategy"] is True


def test_start_paper_command_auto_includes_enabled_temp_paper_overlays(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_experimental_canary_snapshot(repo_root)
    service = OperatorDashboardService(repo_root)
    snapshot = {
        "paper": {
            "non_approved_lanes": {
                "rows": [
                    {
                        "lane_id": "atpe_long_medium_high_canary",
                        "display_name": "ATPE Long Medium+High Canary",
                        "temporary_paper_strategy": True,
                        "paper_strategy_class": "temporary_paper_strategy",
                        "state": "ENABLED",
                        "runtime_kind": "atpe_canary_observer",
                    },
                    {
                        "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                        "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                        "temporary_paper_strategy": True,
                        "paper_strategy_class": "temporary_paper_strategy",
                        "state": "ENABLED",
                        "runtime_kind": "gc_mgc_london_open_acceptance_temp_paper",
                    },
                ]
            }
        }
    }

    command, metadata = service._paper_start_command_with_enabled_temp_paper(snapshot)

    assert command is not None
    assert command[:3] == ["bash", "scripts/run_probationary_paper_soak.sh", "--include-atpe-canary"]
    assert "--include-gc-mgc-acceptance" in command
    assert command[-1] == "--background"
    assert metadata["enabled_lane_ids"] == [
        "atpe_long_medium_high_canary",
        "gc_mgc_london_open_acceptance_continuation_long__GC",
    ]
    assert metadata["requested_flags"] == ["--include-atpe-canary", "--include-gc-mgc-acceptance"]
    assert metadata["unresolved_lane_ids"] == []


def test_dashboard_snapshot_includes_approved_quant_baselines_snapshot(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    paper_artifacts = shadow_artifacts / "paper_session"
    (shadow_artifacts / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "daily").mkdir(parents=True)
    (repo_root / "outputs" / "probationary_quant_baselines").mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(paper_db)

    breakout_weekly_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / "phase2c.breakout.metals_only.us_unknown.baseline" / "weekly"
    breakout_weekly_dir.mkdir(parents=True)
    (breakout_weekly_dir / "2026-W12.json").write_text(
        json.dumps(
            {
                "week_id": "2026-W12",
                "symbol_attribution": [
                    {"symbol": "GC", "trade_count": 3, "net_r_020_total": 0.51},
                    {"symbol": "HG", "trade_count": 2, "net_r_020_total": 0.24},
                ],
                "session_attribution": [
                    {"session_label": "US", "trade_count": 4, "net_r_020_total": 0.62},
                    {"session_label": "UNKNOWN", "trade_count": 1, "net_r_020_total": 0.13},
                ],
                "warning_flags": ["unknown_session_labeling_watch"],
            }
        ) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.json").write_text(
        json.dumps({"freeze_mode": "logic_frozen_monitoring_only"}) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.md").write_text(
        "# Current Active Baseline Status\n",
        encoding="utf-8",
    )

    approved_quant_snapshot = {
        "generated_at": "2026-03-20T23:18:11+00:00",
        "status": "available",
        "rows": [
            {
                "lane_id": "phase2c.breakout.metals_only.us_unknown.baseline",
                "lane_name": "breakout_metals_us_unknown_continuation",
                "probation_status": "watch",
                "baseline_status": "operator_baseline_candidate",
                "approved_scope": {
                    "symbols": ["GC", "MGC", "HG", "PL"],
                    "allowed_sessions": ["US", "UNKNOWN"],
                    "excluded_sessions": ["ASIA", "LONDON"],
                    "permanent_exclusions": ["6J", "LONDON", "broad_fx_metals_breakout", "cross_universe_breakout"],
                    "hold_bars": 24,
                    "stop_r": 1.0,
                    "target_r": None,
                    "exit_style": "time_stop_only",
                    "structural_invalidation_r": None,
                },
                "active_exit_logic": {
                    "exit_style": "time_stop_only",
                    "hold_bars": 24,
                    "stop_r": 1.0,
                    "target_r": None,
                    "structural_invalidation_r": None,
                },
                "artifacts": {
                    "weekly_dir": str(breakout_weekly_dir),
                },
            }
        ],
        "summary_line": "breakout_metals_us_unknown_continuation=watch/operator_baseline_candidate",
    }
    (repo_root / "outputs" / "probationary_quant_baselines" / "approved_quant_baselines_snapshot.json").write_text(
        json.dumps(approved_quant_snapshot) + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {  # type: ignore[method-assign]
        "feed_state": "TEST",
        "feed_label": "INDEX FEED TEST",
        "symbols": [],
        "diagnostics": {},
    }
    service._treasury_curve_payload = lambda: {  # type: ignore[method-assign]
        "curve_state": "TEST",
        "rows": [],
        "diagnostics": {},
    }
    snapshot = service.snapshot()

    assert snapshot["approved_quant_baselines"]["status"] == "available"
    assert snapshot["approved_quant_baselines"]["rows"][0]["lane_id"] == "phase2c.breakout.metals_only.us_unknown.baseline"
    assert snapshot["approved_quant_baselines"]["rows"][0]["lane_classification"] == "approved_baseline_lane"
    assert snapshot["approved_quant_baselines"]["rows"][0]["promotion_state"] == "operator_baseline_candidate"
    assert snapshot["approved_quant_baselines"]["rows"][0]["post_cost_monitoring_read"]["label"] == "stable_positive_post_cost"
    assert snapshot["approved_quant_baselines"]["rows"][0]["approved_exit_label"] == "time_stop_only.h24"
    assert snapshot["approved_quant_baselines"]["rows"][0]["symbol_attribution_summary"][0].startswith("GC")
    assert snapshot["approved_quant_baselines"]["rows"][0]["session_attribution_summary"][0].startswith("US")
    assert snapshot["approved_quant_baselines"]["artifacts"]["snapshot"] == "/api/operator-artifact/approved-quant-baselines"
    assert snapshot["approved_quant_baselines"]["artifacts"]["current_status_json"] == "/api/operator-artifact/approved-quant-baselines-current-status"
    assert "APPROVED BASELINE" in snapshot["approved_quant_baselines"]["rows"][0]["operator_status_line"]
    operator_surface = snapshot["operator_surface"]
    assert operator_surface["readiness"]["provenance"] == "operator_critical"
    assert operator_surface["daily_risk"]["provenance"] == "operator_critical"
    assert operator_surface["context"]["provenance"] == "informational_only"
    lane_rows = operator_surface["lane_rows"]
    assert any(row["classification_tag"] == "approved_quant" for row in lane_rows)
    assert any(row["display_name"] == "breakout_metals_us_unknown_continuation" and row["instrument"] == "GC" for row in lane_rows)
    summary_cards = {row["label"]: row["value"] for row in operator_surface["lane_universe"]["cards"]}
    approved_quant_lane_ids = {
        row["lane_id"]
        for row in lane_rows
        if row["classification_tag"] == "approved_quant"
    }
    assert summary_cards["Approved Quant"] == str(len(approved_quant_lane_ids))
    assert operator_surface["lane_universe"]["title"] == "Unified Active Lane / Instrument Surface"
    assert operator_surface["daily_risk"]["cards"][0]["label"] == "Daily Realized"
    assert operator_surface["readiness"]["cards"][0]["label"] == "System Health"
    assert [section["key"] for section in snapshot["lane_registry"]["sections"]] == [
        "approved_quant",
        "admitted_paper",
        "canary",
    ]
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["display_name"] == "breakout_metals_us_unknown_continuation"
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["standalone_strategy_id"] == "breakout_metals_us_unknown_continuation__GC"
    assert snapshot["lane_registry"]["sections"][0]["rows"][0]["active_exit"] == "time_stop_only.h24"
    assert snapshot["lane_registry"]["diagnostics"]["approved_quant"]["source_row_count"] == 1
    assert snapshot["lane_registry"]["diagnostics"]["approved_quant"]["registry_row_count"] == 4
    assert snapshot["lane_registry"]["diagnostics"]["admitted_paper"]["registry_row_count"] == len(snapshot["paper"]["approved_models"]["rows"])
    assert snapshot["paper"]["approved_models"]["surface_alignment"]["aligned"] is True
    assert snapshot["lane_registry"]["diagnostics"]["canary"]["registry_row_count"] == len(snapshot["paper"]["non_approved_lanes"]["rows"])
    assert snapshot["paper"]["non_approved_lanes"]["surface_alignment"]["aligned"] is True
    assert snapshot["market_context"]["feed_state"] == "TEST"


def test_dashboard_snapshot_extends_signal_intent_fill_audit_to_quant_rows(tmp_path: Path) -> None:
    repo_root = tmp_path
    shadow_artifacts = repo_root / "outputs" / "probationary_pattern_engine"
    paper_artifacts = shadow_artifacts / "paper_session"
    paper_artifacts.mkdir(parents=True)
    (repo_root / "outputs" / "probationary_quant_baselines").mkdir(parents=True)

    shadow_db = repo_root / "shadow.sqlite3"
    paper_db = repo_root / "paper.sqlite3"
    _init_empty_dashboard_db(shadow_db)
    _init_empty_dashboard_db(paper_db)
    paper_lane_db = repo_root / "paper__legacy_lane.sqlite3"
    _init_empty_dashboard_db(paper_lane_db)
    _append_dashboard_bar(
        paper_lane_db,
        bar_id="legacy-bar-1",
        symbol="MGC",
        start_ts="2026-03-23T09:30:00-04:00",
        end_ts="2026-03-23T09:35:00-04:00",
    )

    lane_one = "phase2c.breakout.metals_only.us_unknown.baseline"
    lane_two = "phase2c.failed.core4_plus_qc.no_us.baseline"
    lane_one_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_one
    lane_two_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_two
    (lane_one_dir / "daily").mkdir(parents=True)
    (lane_two_dir / "daily").mkdir(parents=True)

    (lane_one_dir / "daily" / "2026-03-23.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-23",
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "lane_classification": "approved_baseline_lane",
                "signal_count": 3,
                "trade_count": 1,
            }
        ) + "\n",
        encoding="utf-8",
    )
    (lane_two_dir / "daily" / "2026-03-23.json").write_text(
        json.dumps(
            {
                "session_date": "2026-03-23",
                "lane_id": lane_two,
                "lane_name": "failed_move_no_us_reversal_short",
                "lane_classification": "approved_baseline_lane",
                "signal_count": 1,
                "trade_count": 0,
            }
        ) + "\n",
        encoding="utf-8",
    )

    _write_jsonl_rows(
        lane_one_dir / "processed_bars.jsonl",
        [
            {"bar_id": "gc-bar-1", "symbol": "GC", "end_ts": "2026-03-23T09:35:00-04:00"},
            {"bar_id": "mgc-bar-1", "symbol": "MGC", "end_ts": "2026-03-23T09:40:00-04:00"},
            {"bar_id": "hg-bar-1", "symbol": "HG", "end_ts": "2026-03-23T09:45:00-04:00"},
            {"bar_id": "pl-bar-1", "symbol": "PL", "end_ts": "2026-03-23T09:50:00-04:00"},
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "signals.jsonl",
        [
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "HG",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:45:00-04:00",
                "entry_timestamp_planned": "2026-03-23T09:50:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            },
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "PL",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:50:00-04:00",
                "entry_timestamp_planned": "2026-03-23T09:55:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            },
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "mgc-intent-1",
                "symbol": "MGC",
                "created_at": "2026-03-23T09:40:05-04:00",
                "intent_type": "BUY_TO_OPEN",
                "reason_code": "breakout_continuation",
                "broker_order_id": "mgc-broker-1",
            }
        ],
    )
    _write_jsonl_rows(
        lane_one_dir / "trades.jsonl",
        [
            {
                "variant_id": lane_one,
                "lane_id": lane_one,
                "lane_name": "breakout_metals_us_unknown_continuation",
                "symbol": "HG",
                "direction": "LONG",
                "signal_timestamp": "2026-03-23T09:45:00-04:00",
                "entry_timestamp": "2026-03-23T09:50:00-04:00",
                "entry_price": 100.0,
                "exit_timestamp": "2026-03-23T10:00:00-04:00",
                "exit_price": 101.0,
            }
        ],
    )

    _write_jsonl_rows(
        lane_two_dir / "processed_bars.jsonl",
        [
            {"bar_id": "cl-bar-1", "symbol": "CL", "end_ts": "2026-03-23T09:55:00-04:00"},
        ],
    )
    _write_jsonl_rows(
        lane_two_dir / "signals.jsonl",
        [
            {
                "variant_id": lane_two,
                "lane_id": lane_two,
                "lane_name": "failed_move_no_us_reversal_short",
                "symbol": "CL",
                "direction": "SHORT",
                "signal_timestamp": "2026-03-23T09:55:00-04:00",
                "entry_timestamp_planned": "2026-03-23T10:00:00-04:00",
                "signal_passed_flag": True,
                "rejection_reason_code": None,
            }
        ],
    )

    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.json").write_text(
        json.dumps({"freeze_mode": "logic_frozen_monitoring_only"}) + "\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.md").write_text(
        "# Current Active Baseline Status\n",
        encoding="utf-8",
    )
    (repo_root / "outputs" / "probationary_quant_baselines" / "approved_quant_baselines_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-23T14:00:00+00:00",
                "status": "available",
                "rows": [
                    {
                        "lane_id": lane_one,
                        "lane_name": "breakout_metals_us_unknown_continuation",
                        "probation_status": "watch",
                        "baseline_status": "operator_baseline_candidate",
                        "approved_scope": {
                            "symbols": ["GC", "MGC", "HG", "PL"],
                            "allowed_sessions": ["US", "UNKNOWN"],
                            "excluded_sessions": ["ASIA", "LONDON"],
                            "direction": "LONG",
                            "family": "breakout_continuation",
                        },
                    },
                    {
                        "lane_id": lane_two,
                        "lane_name": "failed_move_no_us_reversal_short",
                        "probation_status": "review",
                        "baseline_status": "operator_baseline_candidate",
                        "approved_scope": {
                            "symbols": ["CL", "SI", "NG", "ZN", "ZB"],
                            "allowed_sessions": ["ASIA", "LONDON", "UNKNOWN"],
                            "excluded_sessions": ["US"],
                            "direction": "SHORT",
                            "family": "failed_move_reversal",
                        },
                    },
                ],
            }
        ) + "\n",
        encoding="utf-8",
    )

    (paper_artifacts / "operator_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-23T10:05:00-04:00",
                "last_processed_bar_end_ts": "2026-03-23T10:00:00-04:00",
                "position_side": "FLAT",
                "strategy_status": "RUNNING_MULTI_LANE",
                "entries_enabled": True,
                "operator_halt": False,
                "current_detected_session": "US_MIDDAY",
                "health": {
                    "health_status": "HEALTHY",
                    "market_data_ok": True,
                    "broker_ok": True,
                    "persistence_ok": True,
                    "reconciliation_clean": True,
                    "invariants_ok": True,
                },
                "lanes": [
                    {
                        "lane_id": "legacy_lane",
                        "display_name": "Legacy Paper Lane",
                        "symbol": "MGC",
                        "approved_long_entry_sources": ["bullSnap"],
                        "approved_short_entry_sources": [],
                        "position_side": "FLAT",
                        "strategy_status": "READY",
                        "entries_enabled": True,
                        "operator_halt": False,
                        "warmup_complete": True,
                        "risk_state": "OK",
                        "database_url": f"sqlite:///{paper_lane_db}",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root)
    service._load_or_refresh_auth_gate_result = lambda run_if_missing: {"runtime_ready": True, "source": "test"}  # type: ignore[method-assign]
    service._runtime_paths = lambda runtime_name: {  # type: ignore[method-assign]
        "artifacts_dir": paper_artifacts if runtime_name == "paper" else shadow_artifacts,
        "pid_file": repo_root / f"{runtime_name}.pid",
        "log_file": repo_root / f"{runtime_name}.log",
        "db_path": paper_db if runtime_name == "paper" else shadow_db,
    }
    service._market_index_strip_payload = lambda: {"feed_state": "TEST", "feed_label": "INDEX FEED TEST", "symbols": [], "diagnostics": {}}  # type: ignore[method-assign]
    service._treasury_curve_payload = lambda: {"curve_state": "TEST", "rows": [], "diagnostics": {}}  # type: ignore[method-assign]

    snapshot = service.snapshot()

    audit_rows = snapshot["paper"]["signal_intent_fill_audit"]["rows"]
    quant_rows = [row for row in audit_rows if row.get("lane_id") in {lane_one, lane_two}]
    assert len(quant_rows) == 9
    keyed_rows = {(row["lane_id"], row["instrument"]): row for row in quant_rows}
    assert keyed_rows[(lane_one, "GC")]["audit_verdict"] == "NO_SETUP_OBSERVED"
    assert keyed_rows[(lane_one, "GC")]["standalone_strategy_id"] == "breakout_metals_us_unknown_continuation__GC"
    assert keyed_rows[(lane_one, "MGC")]["audit_verdict"] == "INTENT_NO_FILL_YET"
    assert keyed_rows[(lane_one, "HG")]["audit_verdict"] == "FILLED"
    assert keyed_rows[(lane_two, "CL")]["audit_verdict"] == "SETUP_GATED"
    assert keyed_rows[(lane_one, "PL")]["performance_row_present"] is True
    assert keyed_rows[(lane_one, "PL")]["auditable_now"] is True
    assert keyed_rows[(lane_one, "PL")]["eligible_now"] is True
    assert keyed_rows[(lane_two, "CL")]["eligible_now"] is False
    assert keyed_rows[(lane_two, "CL")]["trade_log_present"] is False
    strategy_rows = {
        row["standalone_strategy_id"]: row
        for row in snapshot["paper"]["strategy_performance"]["rows"]
        if row["lane_id"] in {lane_one, lane_two}
    }
    assert len(strategy_rows) == 9
    assert "breakout_metals_us_unknown_continuation__GC" in strategy_rows
    assert "failed_move_no_us_reversal_short__CL" in strategy_rows
    assert any(row["lane_id"] == "legacy_lane" for row in audit_rows)

    assert snapshot["treasury_curve"]["curve_state"] == "TEST"
    assert "performance" in snapshot["paper"]
    assert "history" in snapshot["paper"]
