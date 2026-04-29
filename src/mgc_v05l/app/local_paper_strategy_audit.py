"""Full local-paper strategy audit and IBKR migration readiness pass."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "local_strategy_audit"
DEFAULT_GOVERNANCE_STATUS_PATH = REPO_ROOT / "var" / "per_strategy_paper_status.json"
DEFAULT_PORTING_STATUS_CSV = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting" / "per_strategy_ibkr_paper_status.csv"
DEFAULT_PORTING_INTENT_JSONL = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting" / "per_strategy_order_intent_examples.jsonl"
DEFAULT_INVENTORY_CSV = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting" / "ibkr_live_paper_strategy_inventory.csv"
DEFAULT_PERFORMANCE_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
DEFAULT_SIGNAL_AUDIT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
DEFAULT_MONITOR_STATUS_PATH = REPO_ROOT / "var" / "paper_strategy_monitor_runtime_status.json"
DEFAULT_EXPOSURE_STATE_PATH = REPO_ROOT / "outputs" / "reports" / "paper_strategy_exposure" / "paper_aggregate_exposure_state.json"
DEFAULT_PAPER_SESSION_LANES_DIR = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes"


@dataclass(frozen=True)
class AuditArtifacts:
    local_strategy_audit_classification: str
    migration_classification: str
    inventory_rows: list[dict[str, Any]]
    overlooked_rows: list[dict[str, Any]]
    readiness_rows: list[dict[str, Any]]
    scorecard_rows: list[dict[str, Any]]
    dashboard: dict[str, Any]
    report_markdown: str
    unsupported_scope_markdown: str
    separation_markdown: str


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="local-paper-strategy-audit")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    artifacts = run_audit(output_dir=Path(args.output_dir))
    write_artifacts(output_dir=Path(args.output_dir), artifacts=artifacts)
    print(artifacts.report_markdown)
    return 0


def run_audit(*, output_dir: Path) -> AuditArtifacts:
    governance_payload = _load_json(DEFAULT_GOVERNANCE_STATUS_PATH)
    performance_snapshot = _load_json(DEFAULT_PERFORMANCE_SNAPSHOT_PATH)
    signal_audit = _load_json(DEFAULT_SIGNAL_AUDIT_PATH)
    monitor_status = _load_json(DEFAULT_MONITOR_STATUS_PATH)
    exposure_state = _load_json(DEFAULT_EXPOSURE_STATE_PATH)
    porting_status_rows = _load_csv(DEFAULT_PORTING_STATUS_CSV)
    porting_inventory_rows = _load_csv(DEFAULT_INVENTORY_CSV)
    intent_rows = _load_jsonl(DEFAULT_PORTING_INTENT_JSONL)

    governance_rows = list(governance_payload.get("strategies") or [])
    performance_rows = {str(row.get("lane_id") or ""): dict(row) for row in list(performance_snapshot.get("rows") or [])}
    signal_rows = {str(row.get("lane_id") or ""): dict(row) for row in list(signal_audit.get("rows") or [])}
    trade_log = list(performance_snapshot.get("trade_log") or [])
    porting_status_by_lane = {str(row.get("strategy_id") or ""): dict(row) for row in porting_status_rows}
    inventory_by_lane = {str(row.get("strategy_id") or ""): dict(row) for row in porting_inventory_rows}
    intent_by_lane = {str(row.get("strategy_id") or ""): dict(row) for row in intent_rows}
    recent_trade_stats = _recent_trade_stats_by_lane(trade_log)
    runtime_activity_by_lane = _runtime_activity_by_lane(DEFAULT_PAPER_SESSION_LANES_DIR)

    inventory_rows: list[dict[str, Any]] = []
    for row in governance_rows:
        lane_id = str(row.get("strategy_id") or "")
        source_instrument = str(row.get("instrument") or "")
        port_row = porting_status_by_lane.get(lane_id, {})
        inventory_row = inventory_by_lane.get(lane_id, {})
        signal_row = signal_rows.get(lane_id, {})
        perf_row = performance_rows.get(lane_id, {})
        intent_row = intent_by_lane.get(lane_id, {})
        runtime_activity = runtime_activity_by_lane.get(lane_id, {})
        derived_routing_mode = _derive_routing_mode(
            governance_row=row,
            porting_status_row=port_row,
            runtime_activity=runtime_activity,
            source_instrument=source_instrument,
        )
        recent_stats = recent_trade_stats.get(lane_id, {})
        recent_trade_route_kind = str(row.get("recent_trade_route_kind") or runtime_activity.get("recent_trade_route_kind") or "NONE")
        recent_local_trade_count = recent_stats.get("trades_last_10_sessions", 0) if recent_trade_route_kind == "INTERNAL_ONLY" else 0
        eligible, blocker = _ibkr_routing_eligibility(
            governance_row=row,
            inventory_row=inventory_row,
            porting_status_row=port_row,
            intent_row=intent_row,
            source_instrument=source_instrument,
        )
        inventory_rows.append(
            {
                "lane_id": lane_id,
                "strategy_id": str(row.get("bridge_strategy_id") or row.get("standalone_strategy_id") or lane_id),
                "source_instrument": source_instrument,
                "runtime_status": row.get("runtime_status"),
                "governance_status": row.get("strategy_status"),
                "routing_mode": derived_routing_mode,
                "local_paper_trading_enabled": bool(row.get("local_paper_trading_enabled")),
                "recent_local_trades_count": recent_local_trade_count,
                "recent_local_open_timestamp": runtime_activity.get("recent_open_timestamp"),
                "recent_local_close_timestamp": runtime_activity.get("recent_close_timestamp"),
                "current_app_internal_position": f"{row.get('current_position_state')} {row.get('current_position_quantity')}",
                "current_broker_attributed_position": f"{row.get('side')} {row.get('current_position_quantity')}" if row.get("ownership_source") == "ledger" else "FLAT 0.0",
                "recent_trade_path": recent_trade_route_kind,
                "ibkr_submit_capable": bool(row.get("ibkr_bridge_submit_capable")),
                "eligible_for_ibkr_paper_routing": eligible,
                "exact_blocker": blocker,
                "intent_action": row.get("intent_action"),
                "submit_block_reasons": ";".join(list(row.get("submit_block_reasons") or [])),
                "realized_pnl": row.get("realized_pnl"),
                "unrealized_pnl": row.get("unrealized_pnl"),
                "total_net_pnl": row.get("total_net_pnl"),
                "average_trade": row.get("average_trade"),
                "win_rate": row.get("win_rate"),
                "profit_factor": row.get("profit_factor"),
                "max_drawdown": row.get("max_drawdown"),
                "max_consecutive_losers": row.get("max_consecutive_losers"),
                "rejection_count": row.get("rejection_count"),
                "reconciliation_error_count": row.get("reconciliation_error_count"),
                "open_order_ambiguity_count": row.get("open_order_ambiguity_count"),
            }
        )

    inventory_rows.sort(key=lambda item: (item["source_instrument"], item["lane_id"]))

    overlooked_rows = [
        {
            "lane_id": row["lane_id"],
            "source_instrument": row["source_instrument"],
            "routing_mode": row["routing_mode"],
            "governance_status": row["governance_status"],
            "recent_local_trades_count": row["recent_local_trades_count"],
            "recent_local_open_timestamp": row["recent_local_open_timestamp"],
            "recent_local_close_timestamp": row["recent_local_close_timestamp"],
            "eligible_for_ibkr_paper_routing": row["eligible_for_ibkr_paper_routing"],
            "exact_blocker": row["exact_blocker"],
        }
        for row in inventory_rows
        if row["recent_local_trades_count"] > 0 and row["routing_mode"] not in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}
    ]

    readiness_rows = []
    for row in inventory_rows:
        next_action = _next_action_for_lane(row)
        readiness_rows.append(
            {
                "lane_id": row["lane_id"],
                "source_instrument": row["source_instrument"],
                "routing_mode": row["routing_mode"],
                "governance_status": row["governance_status"],
                "ibkr_submit_capable": row["ibkr_submit_capable"],
                "eligible_for_ibkr_paper_routing": row["eligible_for_ibkr_paper_routing"],
                "current_intent_action": row["intent_action"],
                "next_action": next_action,
                "exact_blocker": row["exact_blocker"],
            }
        )

    scorecard_rows = []
    for row in inventory_rows:
        recent_stats = recent_trade_stats.get(row["lane_id"], {})
        readiness_class = _readiness_class(row=row, recent_stats=recent_stats)
        scorecard_rows.append(
            {
                "lane_id": row["lane_id"],
                "source_instrument": row["source_instrument"],
                "routing_mode": row["routing_mode"],
                "governance_status": row["governance_status"],
                "trades_today": recent_stats.get("trades_today", 0),
                "trades_last_3_sessions": recent_stats.get("trades_last_3_sessions", 0),
                "trades_last_5_sessions": recent_stats.get("trades_last_5_sessions", 0),
                "trades_last_10_sessions": recent_stats.get("trades_last_10_sessions", 0),
                "realized_pnl": row.get("realized_pnl"),
                "unrealized_pnl": row.get("unrealized_pnl"),
                "total_net_pnl": row.get("total_net_pnl"),
                "average_trade": row.get("average_trade"),
                "win_rate": row.get("win_rate"),
                "profit_factor": row.get("profit_factor"),
                "max_drawdown": row.get("max_drawdown"),
                "max_consecutive_losers": row.get("max_consecutive_losers"),
                "trade_frequency_last_10_sessions": _format_decimal(_decimal(recent_stats.get("trades_last_10_sessions", 0)) / Decimal("10")),
                "cost_slippage_drag": _format_decimal(_decimal(recent_stats.get("fees_last_10_sessions", 0)) + _decimal(recent_stats.get("slippage_last_10_sessions", 0))),
                "order_rejection_count": row.get("rejection_count"),
                "reconciliation_error_count": row.get("reconciliation_error_count"),
                "open_order_ambiguity_count": row.get("open_order_ambiguity_count"),
                "execution_cleanliness": _execution_cleanliness(row=row),
                "reconciliation_cleanliness": _reconciliation_cleanliness(row=row),
                "overlap_redundancy": _overlap_redundancy(row=row),
                "operational_risk": _operational_risk(row=row),
                "readiness_class": readiness_class,
            }
        )

    strategy_live_readiness_dashboard = _build_live_readiness_dashboard(
        inventory_rows=inventory_rows,
        scorecard_rows=scorecard_rows,
        monitor_status=monitor_status,
        exposure_state=exposure_state,
    )

    local_strategy_audit_classification = "LOCAL_STRATEGY_AUDIT_READY" if inventory_rows else "LOCAL_STRATEGY_AUDIT_BLOCKED"
    migration_classification = _migration_classification(inventory_rows=inventory_rows, monitor_status=monitor_status)
    report_markdown = _render_report(
        local_strategy_audit_classification=local_strategy_audit_classification,
        migration_classification=migration_classification,
        inventory_rows=inventory_rows,
        overlooked_rows=overlooked_rows,
        scorecard_rows=scorecard_rows,
        monitor_status=monitor_status,
        exposure_state=exposure_state,
    )
    unsupported_scope_markdown = _render_unsupported_scope_plan(inventory_rows=inventory_rows, scorecard_rows=scorecard_rows)
    separation_markdown = _render_trade_separation_report(inventory_rows=inventory_rows, scorecard_rows=scorecard_rows)
    return AuditArtifacts(
        local_strategy_audit_classification=local_strategy_audit_classification,
        migration_classification=migration_classification,
        inventory_rows=inventory_rows,
        overlooked_rows=overlooked_rows,
        readiness_rows=readiness_rows,
        scorecard_rows=scorecard_rows,
        dashboard=strategy_live_readiness_dashboard,
        report_markdown=report_markdown,
        unsupported_scope_markdown=unsupported_scope_markdown,
        separation_markdown=separation_markdown,
    )


def write_artifacts(*, output_dir: Path, artifacts: AuditArtifacts) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "full_local_paper_strategy_inventory.csv", artifacts.inventory_rows)
    _write_csv(output_dir / "overlooked_local_paper_strategies.csv", artifacts.overlooked_rows)
    _write_csv(output_dir / "ibkr_migration_readiness_matrix.csv", artifacts.readiness_rows)
    _write_csv(output_dir / "short_window_strategy_performance_scorecard.csv", artifacts.scorecard_rows)
    (output_dir / "strategy_live_readiness_dashboard.json").write_text(json.dumps(artifacts.dashboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "strategy_live_readiness_report.md").write_text(artifacts.report_markdown + "\n", encoding="utf-8")
    (output_dir / "unsupported_instrument_scope_plan.md").write_text(artifacts.unsupported_scope_markdown + "\n", encoding="utf-8")
    (output_dir / "local_vs_ibkr_paper_trade_separation_report.md").write_text(artifacts.separation_markdown + "\n", encoding="utf-8")


def _derive_routing_mode(*, governance_row: dict[str, Any], porting_status_row: dict[str, Any], runtime_activity: dict[str, Any], source_instrument: str) -> str:
    governance_mode = str(governance_row.get("current_routing_mode") or "").strip().upper()
    strategy_status = str(governance_row.get("strategy_status") or "").strip().upper()
    destination = str(governance_row.get("current_order_destination") or porting_status_row.get("order_destination") or "")
    intent_action = str(governance_row.get("intent_action") or "").upper()
    if strategy_status == "PAUSED":
        return "PAUSED"
    if strategy_status == "DISABLED":
        return "DISABLED"
    if destination in {"ibkr_paper_bridge_submit_capable", "ibkr_paper_bridge_adopted_position"}:
        if intent_action in {"NO_ACTION", "HOLD", ""}:
            return "IBKR_SUBMIT_CAPABLE_NO_ACTION"
        return "IBKR_ROUTED"
    if source_instrument not in {"GC", "MGC"} and bool(governance_row.get("local_paper_trading_enabled")):
        return "LEGACY_LOCAL_PAPER"
    if bool(governance_row.get("local_paper_trading_enabled")) and bool(runtime_activity.get("recent_local_trades_occurred")):
        return "LEGACY_LOCAL_PAPER"
    if governance_mode == "INTERNAL_ONLY_DIAGNOSTIC":
        return "INTERNAL_ONLY_DIAGNOSTIC"
    if source_instrument not in {"GC", "MGC"}:
        return "INVENTORY_ONLY"
    return "UNKNOWN"


def _ibkr_routing_eligibility(
    *,
    governance_row: dict[str, Any],
    inventory_row: dict[str, Any],
    porting_status_row: dict[str, Any],
    intent_row: dict[str, Any],
    source_instrument: str,
) -> tuple[bool, str]:
    if str(governance_row.get("strategy_status") or "").upper() in {"PAUSED", "DISABLED"}:
        return False, str(governance_row.get("strategy_status") or "").upper().lower()
    if source_instrument not in {"GC", "MGC"}:
        return False, "unsupported_instrument_scope"
    if not bool(governance_row.get("ibkr_bridge_submit_capable") or porting_status_row.get("bridge_submit_capable")):
        return False, "missing_bridge_adapter"
    if str(governance_row.get("current_order_destination") or inventory_row.get("current_order_destination") or "") not in {
        "ibkr_paper_bridge_submit_capable",
        "ibkr_paper_bridge_adopted_position",
    }:
        return False, "not_ported"
    block_reasons = list(governance_row.get("submit_block_reasons") or [])
    if block_reasons:
        return False, ";".join(block_reasons)
    route_blockers = list(intent_row.get("route_blockers") or [])
    if route_blockers:
        return False, ";".join(route_blockers)
    return True, ""


def _recent_trade_stats_by_lane(trade_log: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_log:
        lane_id = str(row.get("lane_id") or "")
        if lane_id:
            grouped[lane_id].append(dict(row))
    output: dict[str, dict[str, Any]] = {}
    for lane_id, rows in grouped.items():
        rows.sort(key=lambda item: _parse_datetime(item.get("exit_timestamp")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        distinct_sessions: list[str] = []
        for row in rows:
            exit_ts = _parse_datetime(row.get("exit_timestamp"))
            if exit_ts is None:
                continue
            session_key = exit_ts.astimezone().date().isoformat()
            if session_key not in distinct_sessions:
                distinct_sessions.append(session_key)
        session_sets = {
            3: set(distinct_sessions[:3]),
            5: set(distinct_sessions[:5]),
            10: set(distinct_sessions[:10]),
        }
        stats = {
            "trades_today": 0,
            "trades_last_3_sessions": 0,
            "trades_last_5_sessions": 0,
            "trades_last_10_sessions": 0,
            "fees_last_10_sessions": Decimal("0"),
            "slippage_last_10_sessions": Decimal("0"),
        }
        today = datetime.now(timezone.utc).astimezone().date().isoformat()
        for row in rows:
            exit_ts = _parse_datetime(row.get("exit_timestamp"))
            if exit_ts is None:
                continue
            session_key = exit_ts.astimezone().date().isoformat()
            if session_key == today:
                stats["trades_today"] += 1
            if session_key in session_sets[3]:
                stats["trades_last_3_sessions"] += 1
            if session_key in session_sets[5]:
                stats["trades_last_5_sessions"] += 1
            if session_key in session_sets[10]:
                stats["trades_last_10_sessions"] += 1
                stats["fees_last_10_sessions"] += _decimal(row.get("fees"))
                stats["slippage_last_10_sessions"] += _decimal(row.get("slippage"))
        output[lane_id] = stats
    return output


def _runtime_activity_by_lane(root: Path) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    if not root.exists():
        return output
    for operator_status_path in root.glob("*/operator_status.json"):
        lane_id = operator_status_path.parent.name
        payload = _load_json(operator_status_path)
        exit_parity = dict(payload.get("exit_parity_summary") or {})
        latest_order = dict(exit_parity.get("latest_order_intent") or {})
        latest_fill = dict(exit_parity.get("latest_fill") or {})
        broker_order_id = str(latest_order.get("broker_order_id") or latest_fill.get("broker_order_id") or latest_fill.get("fill_broker_order_id") or "").strip()
        recent_local = broker_order_id.startswith("paper-")
        output[lane_id] = {
            "recent_trade_route_kind": "INTERNAL_ONLY" if recent_local else ("BROKER_PATH" if broker_order_id else "NONE"),
            "recent_local_trades_occurred": recent_local,
            "recent_open_timestamp": _find_latest_alert_timestamp(operator_status_path.parent / "alerts.jsonl", {"entry_created", "entry_submitted", "entry_filled"}),
            "recent_close_timestamp": _find_latest_alert_timestamp(operator_status_path.parent / "alerts.jsonl", {"exit_created", "exit_submitted", "exit_filled"}),
        }
    return output


def _find_latest_alert_timestamp(path: Path, categories: set[str]) -> str | None:
    if not path.exists():
        return None
    latest: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if str(row.get("code") or row.get("category") or "") not in categories:
            continue
        latest = str(row.get("occurred_at") or row.get("logged_at") or latest)
    return latest


def _next_action_for_lane(row: dict[str, Any]) -> str:
    routing_mode = row["routing_mode"]
    if routing_mode in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"} and row["intent_action"] in {"BUY", "SELL", "EXIT"}:
        return "route_one_order_if_gates_pass"
    if routing_mode in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}:
        return "wait_for_actionable_signal"
    if row["source_instrument"] in {"GC", "MGC"}:
        return "finish_gc_mgc_porting"
    return "expand_instrument_scope_before_porting"


def _readiness_class(*, row: dict[str, Any], recent_stats: dict[str, Any]) -> str:
    routing_mode = row["routing_mode"]
    instrument = row["source_instrument"]
    governance_status = row["governance_status"]
    total_net_pnl = _decimal(row.get("total_net_pnl"))
    average_trade = _decimal(row.get("average_trade"))
    if routing_mode in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"} and instrument in {"GC", "MGC"}:
        if total_net_pnl > 0 and average_trade >= 0 and str(governance_status) in {"PROMISING", "PROBATION_ACTIVE"}:
            return "PAPER_PROBATION"
        return "WATCHLIST"
    if routing_mode in {"LEGACY_LOCAL_PAPER", "INTERNAL_ONLY_DIAGNOSTIC"}:
        return "DIAGNOSTIC_ONLY"
    if instrument not in {"GC", "MGC"}:
        return "NOT_READY"
    return "WATCHLIST"


def _execution_cleanliness(*, row: dict[str, Any]) -> str:
    if int(_to_int(row.get("rejection_count"))) > 0 or int(_to_int(row.get("open_order_ambiguity_count"))) > 0:
        return "NOISY"
    if row["routing_mode"] in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}:
        return "CLEAN"
    return "LOCAL_ONLY"


def _reconciliation_cleanliness(*, row: dict[str, Any]) -> str:
    if int(_to_int(row.get("reconciliation_error_count"))) > 0:
        return "DIRTY"
    return "CLEAN"


def _overlap_redundancy(*, row: dict[str, Any]) -> str:
    if row["source_instrument"] in {"GC", "MGC"}:
        return "HIGH_OVERLAP_SAME_UNDERLYING"
    return "MODERATE"


def _operational_risk(*, row: dict[str, Any]) -> str:
    if row["routing_mode"] in {"LEGACY_LOCAL_PAPER", "INTERNAL_ONLY_DIAGNOSTIC"}:
        return "LOCAL_ONLY"
    if row["governance_status"] == "KILL_CANDIDATE":
        return "ELEVATED_PERFORMANCE_WARNING"
    return "NORMAL"


def _build_live_readiness_dashboard(
    *,
    inventory_rows: list[dict[str, Any]],
    scorecard_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    exposure_state: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": _utc_now(),
        "local_strategy_audit_classification": "LOCAL_STRATEGY_AUDIT_READY" if inventory_rows else "LOCAL_STRATEGY_AUDIT_BLOCKED",
        "migration_classification": _migration_classification(inventory_rows=inventory_rows, monitor_status=monitor_status),
        "paper_monitor_health": monitor_status.get("health_classification"),
        "paper_monitor_stale": monitor_status.get("stale"),
        "routing_mode_counts": _count_by_key(inventory_rows, "routing_mode"),
        "readiness_counts": _count_by_key(scorecard_rows, "readiness_class"),
        "supported_scope_counts": {
            "gc_mgc": len([row for row in inventory_rows if row["source_instrument"] in {"GC", "MGC"}]),
            "unsupported": len([row for row in inventory_rows if row["source_instrument"] not in {"GC", "MGC"}]),
        },
        "aggregate_exposure": {
            "broker_net_mgc": exposure_state.get("broker_net_position"),
            "strategy_attributed_mgc": exposure_state.get("strategy_attributed_position_sum"),
            "broker_minus_ledger_difference": exposure_state.get("broker_minus_strategy_difference"),
            "orphan_exposure": exposure_state.get("unmatched_orphan_quantity"),
        },
        "top_paper_probation_candidates": [row for row in scorecard_rows if row["readiness_class"] == "PAPER_PROBATION"][:10],
    }


def _migration_classification(*, inventory_rows: list[dict[str, Any]], monitor_status: dict[str, Any]) -> str:
    if bool(monitor_status.get("stale")) or str(monitor_status.get("health_classification") or "").upper() != "HEALTHY":
        return "IBKR_MIGRATION_BLOCKED_BY_STATE"
    remaining_supported = [
        row for row in inventory_rows
        if row["source_instrument"] in {"GC", "MGC"} and row["routing_mode"] not in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}
    ]
    if remaining_supported:
        return "IBKR_MIGRATION_NEEDS_MORE_GC_MGC_PORTING"
    return "IBKR_MIGRATION_READY_FOR_NEXT_SCOPE"


def _render_report(
    *,
    local_strategy_audit_classification: str,
    migration_classification: str,
    inventory_rows: list[dict[str, Any]],
    overlooked_rows: list[dict[str, Any]],
    scorecard_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    exposure_state: dict[str, Any],
) -> str:
    lines = [
        "# Local Paper Strategy Audit",
        "",
        f"- local strategy audit classification: `{local_strategy_audit_classification}`",
        f"- migration classification: `{migration_classification}`",
        f"- total lanes audited: `{len(inventory_rows)}`",
        f"- broker-path capable lanes now: `{len([row for row in inventory_rows if row['routing_mode'] in {'IBKR_ROUTED', 'IBKR_SUBMIT_CAPABLE_NO_ACTION'}])}`",
        f"- legacy local-paper lanes still trading: `{len([row for row in inventory_rows if row['routing_mode'] == 'LEGACY_LOCAL_PAPER'])}`",
        f"- overlooked recent local-only traders: `{len(overlooked_rows)}`",
        f"- monitor health: `{monitor_status.get('health_classification')}`",
        f"- monitor stale: `{monitor_status.get('stale')}`",
        f"- broker net MGC: `{exposure_state.get('broker_net_position')}`",
        f"- strategy-attributed MGC: `{exposure_state.get('strategy_attributed_position_sum')}`",
        f"- broker-minus-ledger difference: `{exposure_state.get('broker_minus_strategy_difference')}`",
        "",
        "## Key Findings",
        "",
        f"- supported `GC/MGC` scope is effectively fully ported: `{len([row for row in inventory_rows if row['source_instrument'] in {'GC', 'MGC'} and row['routing_mode'] in {'IBKR_ROUTED', 'IBKR_SUBMIT_CAPABLE_NO_ACTION'}])}` lanes are broker-path ready.",
        f"- unsupported scope remains the main source of ongoing local-only paper trading: `{len([row for row in inventory_rows if row['routing_mode'] == 'LEGACY_LOCAL_PAPER'])}` lanes.",
        f"- `KILL_CANDIDATE` remains a warning label, not an automatic execution stop; current count: `{len([row for row in inventory_rows if row['governance_status'] == 'KILL_CANDIDATE'])}`.",
        "",
        "## Near-Term Readiness",
        "",
    ]
    for row in scorecard_rows[:10]:
        if row["readiness_class"] in {"PAPER_PROBATION", "WATCHLIST"}:
            lines.append(
                f"- `{row['lane_id']}` / `{row['source_instrument']}`: readiness=`{row['readiness_class']}` governance=`{row['governance_status']}` trades_last_10=`{row['trades_last_10_sessions']}` total_net=`{row['total_net_pnl']}`"
            )
    return "\n".join(lines)


def _render_unsupported_scope_plan(*, inventory_rows: list[dict[str, Any]], scorecard_rows: list[dict[str, Any]]) -> str:
    unsupported = [row for row in inventory_rows if row["source_instrument"] not in {"GC", "MGC"}]
    by_instrument = _count_by_key(unsupported, "source_instrument")
    recent_traders = [row for row in unsupported if int(row["recent_local_trades_count"]) > 0]
    es_mes = len([row for row in recent_traders if row["source_instrument"] in {"ES", "MES"}])
    nq_mnq = len([row for row in recent_traders if row["source_instrument"] in {"NQ", "MNQ"}])
    recommended = "MES/ES" if es_mes >= nq_mnq else "MNQ/NQ"
    lines = [
        "# Unsupported Instrument Scope Plan",
        "",
        f"- unsupported local-paper lanes: `{len(unsupported)}`",
        f"- unsupported recent traders: `{len(recent_traders)}`",
        f"- by instrument: `{json.dumps(by_instrument, sort_keys=True)}`",
        f"- recent ES/MES traders: `{es_mes}`",
        f"- recent NQ/MNQ traders: `{nq_mnq}`",
        f"- recommended next scope: `{recommended}`",
        "",
        "## Minimal Expansion Work",
        "",
        "- qualify exact IBKR contracts for the chosen index-futures scope",
        "- add bridge execution mappings and contract target metadata",
        "- add exposure attribution for the new underlying",
        "- refresh governance and lane-port tests for the new scope",
        "- keep all new lanes paper-only until broker-path truth is proven",
    ]
    return "\n".join(lines)


def _render_trade_separation_report(*, inventory_rows: list[dict[str, Any]], scorecard_rows: list[dict[str, Any]]) -> str:
    broker_count = len([row for row in inventory_rows if row["routing_mode"] in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}])
    local_count = len([row for row in inventory_rows if row["routing_mode"] in {"LEGACY_LOCAL_PAPER", "INTERNAL_ONLY_DIAGNOSTIC"}])
    broker_path_pnl = sum((_decimal(row.get("total_net_pnl")) for row in inventory_rows if row["routing_mode"] in {"IBKR_ROUTED", "IBKR_SUBMIT_CAPABLE_NO_ACTION"}), start=Decimal("0"))
    local_only_pnl = sum((_decimal(row.get("total_net_pnl")) for row in inventory_rows if row["routing_mode"] in {"LEGACY_LOCAL_PAPER", "INTERNAL_ONLY_DIAGNOSTIC"}), start=Decimal("0"))
    lines = [
        "# Local vs IBKR Paper Trade Separation",
        "",
        f"- broker-path routed lanes: `{broker_count}`",
        f"- local/internal-only lanes: `{local_count}`",
        f"- aggregate broker-path pnl: `{_format_decimal(broker_path_pnl)}`",
        f"- aggregate local/internal-only pnl: `{_format_decimal(local_only_pnl)}`",
        "",
        "- broker-path paper performance is the only bucket that should influence near-term live-money probation decisions.",
        "- local legacy paper trades remain valuable for signal discovery and runtime stress, but they must not be counted as broker-path proof.",
    ]
    return "\n".join(lines)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _count_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decimal(value: Any) -> Decimal:
    text = str(value or "0").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _format_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.01'))}"

def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
