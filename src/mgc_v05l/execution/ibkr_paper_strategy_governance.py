"""Per-strategy governance and performance status for IBKR paper strategy lanes."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ..app.shared_strategy_identities import shared_strategy_identities
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    run_ibkr_paper_strategy_porting,
)

_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_governance"
_DEFAULT_VAR_STATUS_PATH = Path("var") / "per_strategy_paper_status.json"
_DEFAULT_VAR_DASHBOARD_PATH = Path("var") / "strategy_probation_dashboard.json"
_DEFAULT_VAR_PERFORMANCE_PATH = Path("var") / "per_strategy_paper_performance.csv"
_DEFAULT_PERFORMANCE_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
_DEFAULT_SIGNAL_AUDIT_PATH = Path("outputs") / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
_DEFAULT_DASHBOARD_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_PORTING_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_porting"
_DEFAULT_PAPER_SESSION_LANES_DIR = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"
_PERFORMANCE_CSV = "per_strategy_paper_performance.csv"
_STATUS_JSON = "per_strategy_paper_status.json"
_PROBATION_DASHBOARD_JSON = "strategy_probation_dashboard.json"
_PAUSE_REASONS_CSV = "strategy_pause_reasons.csv"
_REPORT_MD = "strategy_performance_governance_report.md"
_AUDIT_JSONL = "ibkr_paper_strategy_governance_audit.jsonl"
_ROUTING_POLICY_REPORT_MD = "paper_lane_routing_policy_report.md"
_ROUTING_POLICY_REPORT_CSV = "paper_lane_routing_policy_report.csv"
_LOCAL_ONLY_AUDIT_CSV = "local_only_lane_audit.csv"
_TRADE_SEPARATION_REPORT_MD = "ibkr_vs_internal_paper_trade_separation_report.md"

_SUPPORTED_EXECUTABLE_INSTRUMENTS = {"MGC", "GC", "MNQ", "NQ"}
_EXPLICIT_INTERNAL_ONLY_DIAGNOSTIC_LANE_IDS: set[str] = set()
_STATUS_PRECEDENCE = {
    "DISABLED": 6,
    "KILL_CANDIDATE": 5,
    "PAUSED": 4,
    "DEGRADED": 3,
    "WATCHLIST": 2,
    "PROMISING": 1,
    "PROBATION_ACTIVE": 0,
}


@dataclass(frozen=True)
class IbkrPaperStrategyGovernanceConfig:
    repo_root: Path
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    var_status_path: Path = _DEFAULT_VAR_STATUS_PATH
    var_dashboard_path: Path = _DEFAULT_VAR_DASHBOARD_PATH
    var_performance_path: Path = _DEFAULT_VAR_PERFORMANCE_PATH
    performance_snapshot_path: Path = _DEFAULT_PERFORMANCE_SNAPSHOT_PATH
    signal_audit_path: Path = _DEFAULT_SIGNAL_AUDIT_PATH
    dashboard_snapshot_path: Path = _DEFAULT_DASHBOARD_SNAPSHOT_PATH
    ledger_path: Path = _DEFAULT_LEDGER_PATH
    porting_output_dir: Path = _DEFAULT_PORTING_OUTPUT_DIR
    paper_session_lanes_dir: Path = _DEFAULT_PAPER_SESSION_LANES_DIR
    freshness_window_seconds: float = 120.0
    daily_order_limit: int = 2
    weekly_order_limit: int = 5
    drawdown_limit: float = 2500.0


@dataclass(frozen=True)
class IbkrPaperStrategyGovernanceArtifacts:
    classification: str
    report: dict[str, Any]
    performance_rows: list[dict[str, Any]]
    status_payload: dict[str, Any]
    probation_dashboard: dict[str, Any]
    pause_rows: list[dict[str, Any]]
    audit_events: list[dict[str, Any]]


def run_ibkr_paper_strategy_governance(
    *,
    config: IbkrPaperStrategyGovernanceConfig,
) -> IbkrPaperStrategyGovernanceArtifacts:
    audit_events: list[dict[str, Any]] = []
    now = _utc_now()
    _record_audit(audit_events, "governance_started", "Started IBKR paper strategy governance refresh.")

    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    porting = run_ibkr_paper_strategy_porting(
        config=IbkrPaperStrategyPortingConfig(
            repo_root=config.repo_root,
            output_dir=config.porting_output_dir,
        )
    )
    performance_snapshot = _load_json(config.repo_root / config.performance_snapshot_path)
    signal_audit_snapshot = _load_json(config.repo_root / config.signal_audit_path)
    dashboard_snapshot = _load_json(config.repo_root / config.dashboard_snapshot_path)
    ledger = _load_json(config.repo_root / config.ledger_path)

    performance_rows = list(performance_snapshot.get("rows") or [])
    signal_rows = list(signal_audit_snapshot.get("rows") or [])
    inventory_rows = list(porting.inventory_rows)
    intent_rows = list(porting.intent_rows)
    tracked_details = dict((((dashboard_snapshot.get("paper") or {}).get("tracked_strategies") or {}).get("details_by_strategy_id") or {}))
    trade_log = list(performance_snapshot.get("trade_log") or [])

    performance_by_lane = {str(row.get("lane_id") or ""): dict(row) for row in performance_rows}
    signal_by_lane = {str(row.get("lane_id") or ""): dict(row) for row in signal_rows}
    intent_by_lane = {str(row.get("strategy_id") or ""): dict(row) for row in intent_rows}
    ledger_positions = list(ledger.get("positions") or [])
    trade_stats_by_lane = _trade_stats_by_lane(trade_log)
    shared_identity_map = {
        identity.lane_id: identity.identity_id
        for identity in shared_strategy_identities()
    }
    lane_id_by_identity = {
        identity.identity_id: identity.lane_id
        for identity in shared_strategy_identities()
    }
    strategy_rows: list[dict[str, Any]] = []
    pause_rows: list[dict[str, Any]] = []
    seen_lane_ids: set[str] = set()
    for inventory_row in inventory_rows:
        seen_lane_ids.add(str(inventory_row.get("strategy_id") or "").strip())
        row = _build_governance_row(
            config=config,
            now=now,
            inventory_row=inventory_row,
            performance_row=performance_by_lane.get(str(inventory_row.get("strategy_id") or ""), {}),
            signal_row=signal_by_lane.get(str(inventory_row.get("strategy_id") or ""), {}),
            intent_row=intent_by_lane.get(str(inventory_row.get("strategy_id") or ""), {}),
            tracked_details=tracked_details,
            ledger_positions=ledger_positions,
            monitor_status=monitor_status,
            trade_stats=trade_stats_by_lane.get(str(inventory_row.get("strategy_id") or ""), {}),
            shared_strategy_id=shared_identity_map.get(str(inventory_row.get("strategy_id") or "")),
            global_monitor_owner="",
        )
        strategy_rows.append(row)
        if list(row.get("pause_reasons") or []):
            pause_rows.append(
                {
                    "strategy_id": row.get("strategy_id"),
                    "bridge_strategy_id": row.get("bridge_strategy_id"),
                    "instrument": row.get("instrument"),
                    "governance_status": row.get("strategy_status"),
                    "pause_reasons": ";".join(list(row.get("pause_reasons") or [])),
                    "submit_allowed": row.get("submit_allowed"),
                }
            )

    for ledger_position in ledger_positions:
        bridge_strategy_id = str(ledger_position.get("strategy_id") or "").strip()
        if not bridge_strategy_id:
            continue
        synthetic_lane_id = lane_id_by_identity.get(bridge_strategy_id, bridge_strategy_id.lower())
        if synthetic_lane_id in seen_lane_ids:
            continue
        synthetic_inventory_row = _synthetic_inventory_row(
            lane_id=synthetic_lane_id,
            bridge_strategy_id=bridge_strategy_id,
            ledger_position=ledger_position,
        )
        row = _build_governance_row(
            config=config,
            now=now,
            inventory_row=synthetic_inventory_row,
            performance_row=performance_by_lane.get(synthetic_lane_id, {}),
            signal_row=signal_by_lane.get(synthetic_lane_id, {}),
            intent_row=intent_by_lane.get(synthetic_lane_id, {}),
            tracked_details=tracked_details,
            ledger_positions=ledger_positions,
            monitor_status=monitor_status,
            trade_stats=trade_stats_by_lane.get(synthetic_lane_id, {}),
            shared_strategy_id=bridge_strategy_id,
            global_monitor_owner="",
        )
        strategy_rows.append(row)
        seen_lane_ids.add(synthetic_lane_id)

    strategy_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))
    pause_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))

    overall_classification = _overall_governance_classification(strategy_rows=strategy_rows, monitor_status=monitor_status)
    status_payload = _build_status_payload(
        now=now,
        classification=overall_classification,
        monitor_status=monitor_status,
        strategy_rows=strategy_rows,
        config=config,
    )
    probation_dashboard = _build_probation_dashboard(
        now=now,
        classification=overall_classification,
        strategy_rows=strategy_rows,
        monitor_status=monitor_status,
    )
    report = {
        "generated_at": now,
        "classification": overall_classification,
        "routing_policy_classification": _routing_policy_classification(strategy_rows),
        "monitor_status": {
            key: monitor_status.get(key)
            for key in [
                "classification",
                "monitor_running",
                "health_classification",
                "stale",
                "submit_allowed",
                "open_order_count",
                "broker_position_quantity",
                "ledger_position_quantity",
                "last_successful_broker_refresh",
                "block_reasons",
            ]
        },
        "strategy_count": len(strategy_rows),
        "status_counts": _count_by_key(strategy_rows, "strategy_status"),
        "routing_mode_counts": _count_by_key(strategy_rows, "current_routing_mode"),
        "supported_instrument_counts": _count_supported(strategy_rows),
        "submit_capable_count": len([row for row in strategy_rows if row.get("submit_allowed")]),
        "trade_separation_summary": _trade_separation_summary(strategy_rows),
        "strategy_rows": strategy_rows,
    }
    _record_audit(
        audit_events,
        "governance_finished",
        "Finished IBKR paper strategy governance refresh.",
        extra={
            "classification": overall_classification,
            "strategy_count": len(strategy_rows),
            "submit_capable_count": len([row for row in strategy_rows if row.get("submit_allowed")]),
        },
    )
    return IbkrPaperStrategyGovernanceArtifacts(
        classification=overall_classification,
        report=report,
        performance_rows=strategy_rows,
        status_payload=status_payload,
        probation_dashboard=probation_dashboard,
        pause_rows=pause_rows,
        audit_events=audit_events,
    )


def write_ibkr_paper_strategy_governance_artifacts(
    *,
    config: IbkrPaperStrategyGovernanceConfig,
    artifacts: IbkrPaperStrategyGovernanceArtifacts,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / _PERFORMANCE_CSV, artifacts.performance_rows)
    _write_csv(output_dir / _ROUTING_POLICY_REPORT_CSV, [_routing_policy_row(row) for row in artifacts.performance_rows])
    _write_csv(output_dir / _LOCAL_ONLY_AUDIT_CSV, [_local_only_audit_row(row) for row in artifacts.performance_rows if row.get("current_routing_mode") != "IBKR_ROUTED"])
    (output_dir / _STATUS_JSON).write_text(json.dumps(artifacts.status_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / _PROBATION_DASHBOARD_JSON).write_text(json.dumps(artifacts.probation_dashboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(output_dir / _PAUSE_REASONS_CSV, artifacts.pause_rows)
    (output_dir / _REPORT_MD).write_text(render_ibkr_paper_strategy_governance_markdown(artifacts.report) + "\n", encoding="utf-8")
    (output_dir / _ROUTING_POLICY_REPORT_MD).write_text(render_paper_lane_routing_policy_markdown(artifacts.report) + "\n", encoding="utf-8")
    (output_dir / _TRADE_SEPARATION_REPORT_MD).write_text(render_ibkr_vs_internal_trade_separation_markdown(artifacts.report) + "\n", encoding="utf-8")
    with (output_dir / _AUDIT_JSONL).open("w", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")

    status_path = config.repo_root / config.var_status_path
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(artifacts.status_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    dashboard_path = config.repo_root / config.var_dashboard_path
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard_path.write_text(json.dumps(artifacts.probation_dashboard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_csv(config.repo_root / config.var_performance_path, artifacts.performance_rows)


def load_paper_strategy_governance_status(*, repo_root: Path, strategy_id: str | None = None) -> dict[str, Any]:
    path = repo_root / _DEFAULT_VAR_STATUS_PATH
    if not path.exists():
        return {
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["paper_strategy_governance_status_missing"],
            "detail": "Paper strategy governance status has not been generated yet.",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["paper_strategy_governance_status_invalid"],
            "detail": "Paper strategy governance status could not be decoded.",
        }

    rows = list(payload.get("strategies") or [])
    requested = str(strategy_id or "").strip()
    selected = None
    if requested:
        for row in rows:
            identifiers = {
                str(row.get("strategy_id") or "").strip(),
                str(row.get("bridge_strategy_id") or "").strip(),
                str(row.get("standalone_strategy_id") or "").strip(),
            }
            if requested in identifiers:
                selected = dict(row)
                break
    payload["selected_strategy"] = selected
    payload["submit_allowed"] = bool(selected.get("submit_allowed")) if isinstance(selected, dict) else False
    payload["block_reasons"] = list(selected.get("submit_block_reasons") or []) if isinstance(selected, dict) else ["paper_strategy_governance_strategy_missing"]
    if requested and selected is None:
        payload["detail"] = f"Paper strategy governance has no row for strategy identity {requested}."
    return payload


def render_ibkr_paper_strategy_governance_markdown(report: dict[str, Any]) -> str:
    counts = dict(report.get("status_counts") or {})
    lines = [
        "# IBKR Paper Strategy Performance Governance",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- strategy rows: `{report.get('strategy_count')}`",
        f"- submit-capable rows now: `{report.get('submit_capable_count')}`",
        f"- monitor health: `{dict(report.get('monitor_status') or {}).get('health_classification')}`",
        f"- monitor stale: `{dict(report.get('monitor_status') or {}).get('stale')}`",
        f"- current monitor-owned quantity: `{dict(report.get('monitor_status') or {}).get('broker_position_quantity')}`",
        "",
        "## Status Counts",
        "",
    ]
    for key in sorted(counts):
        lines.append(f"- `{key}`: `{counts.get(key)}`")
    lines.extend(
        [
            "",
            "## Summary",
            "",
            "- this governance layer tracks per-strategy paper P&L, position state, broker/ledger safety, and submit eligibility before any new IBKR paper order is allowed.",
            "- supported lanes can remain active while still being marked degraded, watchlist, or kill-candidate; only paused/disabled rows are hard blocked by governance status itself.",
            "- unsupported or not-yet-submit-ported lanes remain inventory and dry-run only until their broker path is proven lane-by-lane.",
        ]
    )
    return "\n".join(lines)


def render_paper_lane_routing_policy_markdown(report: dict[str, Any]) -> str:
    rows = list(report.get("strategy_rows") or [])
    mode_counts = _count_by_key(rows, "current_routing_mode")
    lines = [
        "# Paper Lane Routing Policy",
        "",
        f"- classification: `{report.get('routing_policy_classification')}`",
        f"- audited lanes: `{len(rows)}`",
        "",
        "## Routing Modes",
        "",
    ]
    for key in sorted(mode_counts):
        lines.append(f"- `{key}`: `{mode_counts.get(key)}`")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- `IBKR_ROUTED` lanes may use the shared IBKR paper bridge when monitor, governance, exposure, and readiness gates all pass.",
            "- `INTERNAL_ONLY_DIAGNOSTIC` lanes are explicit local simulation only and must not contaminate IBKR broker-path performance.",
            "- `PAUSED` lanes must not continue local paper trading, even if a legacy paper runtime still has entries enabled.",
            "- `DISABLED` lanes are fail-closed and should not route locally or through IBKR.",
        ]
    )
    return "\n".join(lines)


def render_ibkr_vs_internal_trade_separation_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("trade_separation_summary") or {})
    lines = [
        "# IBKR vs Internal Paper Trade Separation",
        "",
        f"- broker-path pnl: `{summary.get('broker_path_pnl')}`",
        f"- internal-sim pnl: `{summary.get('internal_sim_pnl')}`",
        f"- diagnostic-only pnl: `{summary.get('diagnostic_only_pnl')}`",
        f"- broker-path lanes: `{summary.get('broker_path_lane_count')}`",
        f"- internal-only lanes: `{summary.get('internal_only_lane_count')}`",
        f"- paused/disabled lanes with recent local trades: `{summary.get('policy_violation_local_trade_count')}`",
        "",
        "- broker-path P&L is reserved for lanes with real IBKR bridge ownership / broker-path truth.",
        "- local legacy paper-runtime trades remain separated as internal simulation unless a lane is explicitly diagnostic-only.",
    ]
    return "\n".join(lines)


def _build_governance_row(
    *,
    config: IbkrPaperStrategyGovernanceConfig,
    now: str,
    inventory_row: dict[str, Any],
    performance_row: dict[str, Any],
    signal_row: dict[str, Any],
    intent_row: dict[str, Any],
    tracked_details: dict[str, Any],
    ledger_positions: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    trade_stats: dict[str, Any],
    shared_strategy_id: str | None,
    global_monitor_owner: str,
) -> dict[str, Any]:
    lane_id = str(inventory_row.get("strategy_id") or "").strip()
    bridge_strategy_id = str(shared_strategy_id or performance_row.get("standalone_strategy_id") or signal_row.get("id") or "").strip()
    tracked_detail = _select_tracked_detail(tracked_details=tracked_details, lane_id=lane_id, bridge_strategy_id=bridge_strategy_id)
    ledger_position = _matching_ledger_position(ledger_positions=ledger_positions, lane_id=lane_id, bridge_strategy_id=bridge_strategy_id)

    instrument = str(inventory_row.get("instrument") or "").strip().upper()
    current_position_state = str(inventory_row.get("current_position_state") or "UNKNOWN").strip().upper()
    current_quantity = float(inventory_row.get("current_quantity") or 0.0)
    strategy_state = _strategy_state(
        current_position_state=current_position_state,
        current_quantity=current_quantity,
        blockers=list(inventory_row.get("blockers_to_ibkr_paper_routing") or []),
    )
    trade_count = _int_or_none(tracked_detail.get("trade_count")) or _int_or_none(performance_row.get("trade_count")) or trade_stats.get("trade_count") or 0
    realized_pnl = _decimal_or_none(tracked_detail.get("realized_pnl"))
    if realized_pnl is None:
        realized_pnl = _decimal_or_none(performance_row.get("realized_pnl"))
    if realized_pnl is None:
        realized_pnl = trade_stats.get("realized_pnl")
    unrealized_pnl = _decimal_or_none(_extract_unrealized_pnl(ledger_position=ledger_position, tracked_detail=tracked_detail, performance_row=performance_row))
    total_net_pnl = (realized_pnl or Decimal("0")) + (unrealized_pnl or Decimal("0"))
    gross_profit = trade_stats.get("gross_profit")
    gross_loss = trade_stats.get("gross_loss")
    gross_pnl = None
    if gross_profit is not None and gross_loss is not None:
        gross_pnl = gross_profit + gross_loss
    average_trade = _decimal_or_none(tracked_detail.get("average_trade_pnl"))
    if average_trade is None and trade_count > 0:
        average_trade = (realized_pnl or Decimal("0")) / Decimal(str(trade_count))
    profit_factor = _decimal_or_none(tracked_detail.get("profit_factor")) or trade_stats.get("profit_factor")
    win_rate = _decimal_or_none(tracked_detail.get("win_rate")) or trade_stats.get("win_rate")
    max_drawdown = _decimal_or_none(tracked_detail.get("max_drawdown")) or _decimal_or_none(performance_row.get("max_drawdown"))
    max_consecutive_losers = trade_stats.get("max_consecutive_losers")
    daily_pnl = _decimal_or_none(tracked_detail.get("current_day_pnl")) or _decimal_or_none(performance_row.get("day_pnl"))
    weekly_pnl = trade_stats.get("weekly_realized_pnl")
    order_count = trade_count
    daily_order_count = trade_stats.get("daily_trade_count")
    weekly_order_count = trade_stats.get("weekly_trade_count")
    rejection_count = 0
    reconciliation_error_count = 0
    open_order_ambiguity_count = 0
    runtime_activity = _load_lane_runtime_activity(
        repo_root=config.repo_root,
        paper_session_lanes_dir=config.paper_session_lanes_dir,
        lane_id=lane_id,
    )

    pause_reasons: list[str] = []
    submit_block_reasons: list[str] = []
    inventory_blockers = list(inventory_row.get("blockers_to_ibkr_paper_routing") or [])
    if "unsupported_instrument_scope" in inventory_blockers:
        submit_block_reasons.append("unsupported_instrument_scope")
    if "lane_not_yet_submit_ported" in inventory_blockers or "strategy_lane_not_yet_submit_ported" in inventory_blockers:
        submit_block_reasons.append("lane_not_yet_submit_ported")
    if "unknown_strategy_state" in inventory_blockers or strategy_state in {"UNKNOWN", "BLOCKED"}:
        pause_reasons.append("unknown_strategy_state")
    if "broker_ledger_mismatch" in inventory_blockers:
        pause_reasons.append("broker_ledger_mismatch")
        reconciliation_error_count += 1
    if bool(monitor_status.get("stale")):
        submit_block_reasons.append("paper_monitor_stale")
    if str(monitor_status.get("health_classification") or "").upper() != "HEALTHY":
        submit_block_reasons.append("paper_monitor_not_healthy")
    if int(monitor_status.get("open_order_count") or 0) != 0:
        submit_block_reasons.append("conflicting_open_order_present")
        open_order_ambiguity_count += 1
    for reason in list(monitor_status.get("block_reasons") or []):
        normalized = str(reason or "").strip()
        if normalized and normalized not in submit_block_reasons:
            submit_block_reasons.append(normalized)
    if daily_order_count is not None and int(daily_order_count) >= int(config.daily_order_limit):
        submit_block_reasons.append("daily_order_limit_reached")
    if weekly_order_count is not None and int(weekly_order_count) >= int(config.weekly_order_limit):
        submit_block_reasons.append("weekly_order_limit_reached")
    if max_drawdown is not None and abs(float(max_drawdown)) >= float(config.drawdown_limit):
        submit_block_reasons.append("drawdown_limit_reached")
    if not bool(_dashboard_live_ready(monitor_status)):
        submit_block_reasons.append("backend_or_source_not_live_ready")

    strategy_status = _strategy_governance_status(
        instrument=instrument,
        strategy_state=strategy_state,
        current_app_runtime_status=str(inventory_row.get("current_app_runtime_status") or "").strip().upper(),
        inventory_blockers=inventory_blockers,
        realized_pnl=realized_pnl,
        profit_factor=profit_factor,
        max_drawdown=max_drawdown,
        pause_reasons=pause_reasons,
    )
    if strategy_status in {"PAUSED", "DISABLED"}:
        submit_block_reasons = list(dict.fromkeys(submit_block_reasons + [strategy_status.lower()]))
    submit_allowed = not submit_block_reasons and strategy_status not in {"PAUSED", "DISABLED"}
    routing_mode, local_trading_allowed = _routing_mode_and_local_policy(
        strategy_status=strategy_status,
        current_order_destination=str(inventory_row.get("current_order_destination") or ""),
        explicit_internal_only_diagnostic=lane_id in _EXPLICIT_INTERNAL_ONLY_DIAGNOSTIC_LANE_IDS,
        instrument=instrument,
    )
    local_paper_trading_enabled = str(inventory_row.get("current_order_destination") or "") == "legacy_app_paper_runtime" and bool(inventory_row.get("entries_enabled"))
    recent_trade_route_kind = _recent_trade_route_kind(
        runtime_activity=runtime_activity,
        current_order_destination=str(inventory_row.get("current_order_destination") or ""),
        bridge_strategy_id=bridge_strategy_id,
        ownership_source="ledger" if ledger_position is not None else "runtime_snapshot",
    )
    broker_path_pnl, internal_sim_pnl, diagnostic_only_pnl = _split_pnl_buckets(
        total_net_pnl=total_net_pnl,
        current_order_destination=str(inventory_row.get("current_order_destination") or ""),
        routing_mode=routing_mode,
        recent_trade_route_kind=recent_trade_route_kind,
    )

    last_trade_time = (
        runtime_activity.get("last_trade_time")
        or runtime_activity.get("last_fill_timestamp")
        or runtime_activity.get("last_event_time")
        or
        tracked_detail.get("latest_trade_timestamp")
        or performance_row.get("latest_fill_timestamp")
        or signal_row.get("last_fill_timestamp")
    )
    last_signal_time = signal_row.get("last_actionable_signal_timestamp") or performance_row.get("last_fire_timestamp")
    last_broker_reconciliation_time = monitor_status.get("last_successful_broker_refresh") or monitor_status.get("last_broker_refresh_timestamp")

    return {
        "strategy_id": lane_id,
        "bridge_strategy_id": bridge_strategy_id or None,
        "standalone_strategy_id": performance_row.get("standalone_strategy_id") or signal_row.get("id"),
        "instrument": instrument,
        "contract_symbol": dict(intent_row.get("contract_target") or {}).get("symbol"),
        "contract_month": dict(intent_row.get("contract_target") or {}).get("contract_month"),
        "exact_expiry": dict(intent_row.get("contract_target") or {}).get("expiry") or (ledger_position or {}).get("expiry"),
        "con_id": dict(intent_row.get("contract_target") or {}).get("con_id") or (ledger_position or {}).get("con_id"),
        "local_symbol": dict(intent_row.get("contract_target") or {}).get("local_symbol") or (ledger_position or {}).get("local_symbol"),
        "current_state": strategy_state,
        "current_position_state": current_position_state,
        "current_position_quantity": current_quantity,
        "side": (ledger_position or {}).get("side") or current_position_state,
        "current_signal_state": inventory_row.get("current_signal_state"),
        "trades": trade_count,
        "realized_pnl": _format_decimal(realized_pnl),
        "unrealized_pnl": _format_decimal(unrealized_pnl),
        "total_net_pnl": _format_decimal(total_net_pnl),
        "gross_pnl": _format_decimal(gross_pnl),
        "estimated_costs": None,
        "average_trade": _format_decimal(average_trade),
        "win_rate": _format_decimal(win_rate),
        "profit_factor": _format_decimal(profit_factor),
        "max_drawdown": _format_decimal(max_drawdown),
        "max_consecutive_losers": max_consecutive_losers,
        "daily_pnl": _format_decimal(daily_pnl),
        "weekly_pnl": _format_decimal(weekly_pnl),
        "order_count": order_count,
        "daily_order_count": daily_order_count,
        "weekly_order_count": weekly_order_count,
        "rejection_count": rejection_count,
        "reconciliation_error_count": reconciliation_error_count,
        "open_order_ambiguity_count": open_order_ambiguity_count,
        "last_trade_time": last_trade_time,
        "last_signal_time": last_signal_time,
        "last_broker_reconciliation_time": last_broker_reconciliation_time,
        "strategy_status": strategy_status,
        "runtime_status": inventory_row.get("current_app_runtime_status"),
        "entries_enabled": inventory_row.get("entries_enabled"),
        "eligible_now": inventory_row.get("eligible_now"),
        "local_paper_trading_enabled": local_paper_trading_enabled,
        "ibkr_bridge_submit_capable": bool(inventory_row.get("bridge_adapter_ready")),
        "current_order_destination": inventory_row.get("current_order_destination"),
        "current_routing_mode": routing_mode,
        "recent_local_trades_occurred": bool(runtime_activity.get("recent_local_trades_occurred")),
        "recent_trade_route_kind": recent_trade_route_kind,
        "recent_trade_origin_label": runtime_activity.get("recent_trade_origin_label"),
        "local_trading_allowed": local_trading_allowed,
        "intent_action": intent_row.get("action"),
        "intent_reason": intent_row.get("reason"),
        "route_blockers": inventory_blockers,
        "pause_reasons": list(dict.fromkeys(pause_reasons)),
        "submit_block_reasons": list(dict.fromkeys(submit_block_reasons)),
        "submit_allowed": submit_allowed,
        "bridge_invocation_allowed": submit_allowed and bool(inventory_row.get("bridge_adapter_ready")),
        "monitor_health": monitor_status.get("health_classification"),
        "monitor_stale": monitor_status.get("stale"),
        "monitor_open_orders": monitor_status.get("open_order_count"),
        "broker_path_pnl": _format_decimal(broker_path_pnl),
        "internal_sim_pnl": _format_decimal(internal_sim_pnl),
        "diagnostic_only_pnl": _format_decimal(diagnostic_only_pnl),
        "current_average_entry_price": _format_decimal(_decimal_or_none((ledger_position or {}).get("average_entry_price"))),
        "ownership_source": "ledger" if ledger_position is not None else "runtime_snapshot",
        "governance_generated_at": now,
    }


def _strategy_state(*, current_position_state: str, current_quantity: float, blockers: list[str]) -> str:
    if "broker_ledger_mismatch" in blockers or "unknown_strategy_state" in blockers:
        return "BLOCKED"
    if current_position_state == "LONG" and current_quantity > 0.0:
        return "LONG"
    if current_position_state == "FLAT" and current_quantity == 0.0:
        return "FLAT"
    return "UNKNOWN"


def _strategy_governance_status(
    *,
    instrument: str,
    strategy_state: str,
    current_app_runtime_status: str,
    inventory_blockers: list[str],
    realized_pnl: Decimal | None,
    profit_factor: Decimal | None,
    max_drawdown: Decimal | None,
    pause_reasons: list[str],
) -> str:
    if pause_reasons:
        return "PAUSED"
    if current_app_runtime_status in {"DISABLED", "HALTED"} and strategy_state == "FLAT":
        return "DISABLED"
    if instrument not in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        return "WATCHLIST"
    if "lane_not_yet_submit_ported" in inventory_blockers or "strategy_lane_not_yet_submit_ported" in inventory_blockers:
        return "WATCHLIST"
    if realized_pnl is not None and realized_pnl < 0 and profit_factor is not None and profit_factor < Decimal("1.0"):
        if max_drawdown is not None and abs(max_drawdown) > Decimal("1000"):
            return "KILL_CANDIDATE"
        return "DEGRADED"
    if realized_pnl is not None and realized_pnl > 0 and (profit_factor is None or profit_factor >= Decimal("1.0")):
        return "PROMISING"
    return "PROBATION_ACTIVE"


def _dashboard_live_ready(monitor_status: dict[str, Any]) -> bool:
    return not any(
        reason in {"backend_down", "source_snapshot_fallback", "paper_runtime_stale", "temp_paper_blocked"}
        for reason in list(monitor_status.get("block_reasons") or [])
    )


def _matching_ledger_position(
    *,
    ledger_positions: list[dict[str, Any]],
    lane_id: str,
    bridge_strategy_id: str,
) -> dict[str, Any] | None:
    identifiers = {lane_id, bridge_strategy_id}
    identifiers = {item for item in identifiers if item}
    for row in ledger_positions:
        row_strategy_id = str(row.get("strategy_id") or "").strip()
        if row_strategy_id in identifiers:
            return dict(row)
    return None


def _select_tracked_detail(*, tracked_details: dict[str, Any], lane_id: str, bridge_strategy_id: str) -> dict[str, Any]:
    for key in (lane_id, bridge_strategy_id, bridge_strategy_id.lower(), lane_id.lower()):
        if key and key in tracked_details:
            return dict(tracked_details.get(key) or {})
    return {}


def _synthetic_inventory_row(*, lane_id: str, bridge_strategy_id: str, ledger_position: dict[str, Any]) -> dict[str, Any]:
    symbol = str(ledger_position.get("symbol") or "").strip().upper()
    quantity = float(ledger_position.get("quantity") or 0.0)
    side = str(ledger_position.get("side") or "").strip().upper()
    return {
        "strategy_id": lane_id,
        "standalone_strategy_id": bridge_strategy_id,
        "instrument": symbol,
        "strategy_family": "ibkr_paper_bridge_adopted_position",
        "current_app_runtime_status": "OWNED_BROKER_POSITION",
        "current_position_state": side if side else "UNKNOWN",
        "current_quantity": quantity,
        "current_signal_state": "NO_ACTION",
        "entry_exit_capability": "EXIT_ONLY_WHILE_LONG" if side == "LONG" and quantity > 0.0 else "UNKNOWN",
        "current_order_destination": "ibkr_paper_bridge_adopted_position",
        "can_emit_standardized_order_intent_now": True,
        "blockers_to_ibkr_paper_routing": [],
        "entries_enabled": True,
        "eligible_now": False,
        "last_signal_family": None,
        "last_signal_timestamp": None,
        "last_fill_timestamp": None,
        "audit_verdict": "ADOPTED_BROKER_POSITION",
        "monitor_submit_allowed": True,
    }


def _trade_stats_by_lane(trade_log: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in trade_log:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        grouped.setdefault(lane_id, []).append(dict(row))
    return {lane_id: _summarize_trade_rows(rows) for lane_id, rows in grouped.items()}


def _summarize_trade_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnl_values: list[Decimal] = []
    wins: list[Decimal] = []
    losses: list[Decimal] = []
    max_consecutive_losers = 0
    current_loser_streak = 0
    weekly_pnl = Decimal("0")
    weekly_count = 0
    daily_count = 0
    now = datetime.now(timezone.utc)
    for row in rows:
        pnl = _decimal_or_none(row.get("realized_pnl"))
        if pnl is None:
            continue
        pnl_values.append(pnl)
        if pnl > 0:
            wins.append(pnl)
            current_loser_streak = 0
        elif pnl < 0:
            losses.append(pnl)
            current_loser_streak += 1
            max_consecutive_losers = max(max_consecutive_losers, current_loser_streak)
        else:
            current_loser_streak = 0
        exit_ts = _parse_datetime(row.get("exit_ts"))
        if exit_ts is not None and exit_ts.date() == now.date():
            daily_count += 1
        if exit_ts is not None and (now - exit_ts).total_seconds() <= 7 * 24 * 60 * 60:
            weekly_pnl += pnl
            weekly_count += 1
    trade_count = len(pnl_values)
    realized_pnl = sum(pnl_values, start=Decimal("0"))
    gross_profit = sum((value for value in wins), start=Decimal("0"))
    gross_loss = sum((value for value in losses), start=Decimal("0"))
    profit_factor = None
    if gross_loss != 0:
        profit_factor = abs(gross_profit / gross_loss)
    elif gross_profit > 0:
        profit_factor = Decimal("999")
    win_rate = None
    if trade_count > 0:
        win_rate = (Decimal(str(len(wins))) / Decimal(str(trade_count))) * Decimal("100")
    return {
        "trade_count": trade_count,
        "realized_pnl": realized_pnl,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor,
        "win_rate": win_rate,
        "max_consecutive_losers": max_consecutive_losers,
        "weekly_realized_pnl": weekly_pnl if weekly_count > 0 else None,
        "daily_trade_count": daily_count if daily_count > 0 else None,
        "weekly_trade_count": weekly_count if weekly_count > 0 else None,
    }


def _overall_governance_classification(*, strategy_rows: list[dict[str, Any]], monitor_status: dict[str, Any]) -> str:
    if not strategy_rows:
        return "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    if bool(monitor_status.get("stale")) or str(monitor_status.get("health_classification") or "").upper() != "HEALTHY":
        return "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    if any(str(row.get("strategy_status") or "").upper() == "PAUSED" for row in strategy_rows):
        return "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    if any(str(row.get("strategy_status") or "").upper() in {"WATCHLIST", "DEGRADED"} for row in strategy_rows):
        return "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    return "PAPER_STRATEGY_GOVERNANCE_READY"


def _build_status_payload(
    *,
    now: str,
    classification: str,
    monitor_status: dict[str, Any],
    strategy_rows: list[dict[str, Any]],
    config: IbkrPaperStrategyGovernanceConfig,
) -> dict[str, Any]:
    payload = {
        "generated_at": now,
        "classification": classification,
        "routing_policy_classification": _routing_policy_classification(strategy_rows),
        "freshness_window_seconds": float(config.freshness_window_seconds),
        "monitor_health": monitor_status.get("health_classification"),
        "monitor_stale": monitor_status.get("stale"),
        "monitor_submit_allowed": monitor_status.get("submit_allowed"),
        "monitor_block_reasons": list(monitor_status.get("block_reasons") or []),
        "strategies": strategy_rows,
        "summary": {
            "strategy_count": len(strategy_rows),
            "status_counts": _count_by_key(strategy_rows, "strategy_status"),
            "routing_mode_counts": _count_by_key(strategy_rows, "current_routing_mode"),
            "submit_capable_count": len([row for row in strategy_rows if row.get("submit_allowed")]),
        },
    }
    return payload


def _build_probation_dashboard(
    *,
    now: str,
    classification: str,
    strategy_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": now,
        "classification": classification,
        "routing_policy_classification": _routing_policy_classification(strategy_rows),
        "paper_monitor_health": monitor_status.get("health_classification"),
        "paper_monitor_stale": monitor_status.get("stale"),
        "active_rows": [row for row in strategy_rows if row.get("strategy_status") in {"PROBATION_ACTIVE", "PROMISING", "DEGRADED"}],
        "blocked_rows": [row for row in strategy_rows if not row.get("submit_allowed")],
        "ibkr_routed_rows": [row for row in strategy_rows if row.get("current_routing_mode") == "IBKR_ROUTED"],
        "internal_only_rows": [row for row in strategy_rows if row.get("current_routing_mode") == "INTERNAL_ONLY_DIAGNOSTIC"],
        "paused_or_disabled_rows": [row for row in strategy_rows if row.get("current_routing_mode") in {"PAUSED", "DISABLED"}],
        "summary": {
            "status_counts": _count_by_key(strategy_rows, "strategy_status"),
            "instrument_counts": _count_by_key(strategy_rows, "instrument"),
            "routing_mode_counts": _count_by_key(strategy_rows, "current_routing_mode"),
        },
    }


def _routing_policy_classification(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "PAPER_LANE_ROUTING_POLICY_BLOCKED"
    if any(str(row.get("current_routing_mode") or "UNKNOWN") == "UNKNOWN" for row in rows):
        return "PAPER_LANE_ROUTING_POLICY_PARTIAL"
    return "PAPER_LANE_ROUTING_POLICY_READY"


def _routing_mode_and_local_policy(
    *,
    strategy_status: str,
    current_order_destination: str,
    explicit_internal_only_diagnostic: bool,
    instrument: str,
) -> tuple[str, bool]:
    if strategy_status == "DISABLED":
        return "DISABLED", False
    if strategy_status == "PAUSED" and not explicit_internal_only_diagnostic:
        return "PAUSED", False
    if current_order_destination in {"ibkr_paper_bridge_submit_capable", "ibkr_paper_bridge_adopted_position"} and instrument in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        return "IBKR_ROUTED", False
    if current_order_destination == "legacy_app_paper_runtime":
        return "INTERNAL_ONLY_DIAGNOSTIC", True
    return "UNKNOWN", False


def _load_lane_runtime_activity(*, repo_root: Path, paper_session_lanes_dir: Path, lane_id: str) -> dict[str, Any]:
    lane_dir = repo_root / paper_session_lanes_dir / lane_id
    operator_status_path = lane_dir / "operator_status.json"
    alerts_path = lane_dir / "alerts.jsonl"
    operator_status = _load_json(operator_status_path)
    latest_order = dict((((operator_status.get("exit_parity_summary") or {}).get("latest_order_intent")) or {}))
    latest_fill = dict((((operator_status.get("exit_parity_summary") or {}).get("latest_fill")) or {}))
    broker_order_id = str(latest_order.get("broker_order_id") or latest_fill.get("fill_broker_order_id") or latest_fill.get("broker_order_id") or "").strip()
    recent_local_trade = broker_order_id.startswith("paper-")
    last_event_time = latest_order.get("submitted_at") or latest_fill.get("fill_timestamp")
    origin = None
    if recent_local_trade:
        origin = "internal_simulation_legacy_app_paper_runtime"
    elif broker_order_id:
        origin = "broker_path_or_unknown_external_order_id"
    if not operator_status and alerts_path.exists():
        last_event_time = None
        for line in reversed(alerts_path.read_text(encoding="utf-8").splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            detail = dict(row.get("detail") or {})
            broker_order_id = str(detail.get("broker_order_id") or "").strip()
            if broker_order_id.startswith("paper-"):
                recent_local_trade = True
                origin = "internal_simulation_legacy_app_paper_runtime"
            elif broker_order_id and origin is None:
                origin = "broker_path_or_unknown_external_order_id"
            last_event_time = row.get("occurred_at") or row.get("logged_at")
            break
    return {
        "recent_local_trades_occurred": recent_local_trade,
        "last_trade_time": last_event_time,
        "last_fill_timestamp": latest_fill.get("fill_timestamp"),
        "last_event_time": last_event_time,
        "recent_trade_origin_label": origin,
    }


def _recent_trade_route_kind(
    *,
    runtime_activity: dict[str, Any],
    current_order_destination: str,
    bridge_strategy_id: str,
    ownership_source: str,
) -> str:
    if bool(runtime_activity.get("recent_local_trades_occurred")):
        return "INTERNAL_ONLY"
    if current_order_destination == "ibkr_paper_bridge_adopted_position" or ownership_source == "ledger" or bridge_strategy_id == "ATP_COMPANION_V1_ASIA_US":
        return "BROKER_PATH"
    return "NONE"


def _split_pnl_buckets(
    *,
    total_net_pnl: Decimal,
    current_order_destination: str,
    routing_mode: str,
    recent_trade_route_kind: str,
) -> tuple[Decimal, Decimal, Decimal]:
    zero = Decimal("0")
    if current_order_destination == "ibkr_paper_bridge_adopted_position":
        return total_net_pnl, zero, zero
    if routing_mode == "INTERNAL_ONLY_DIAGNOSTIC":
        return zero, zero, total_net_pnl
    if recent_trade_route_kind == "INTERNAL_ONLY" or current_order_destination == "legacy_app_paper_runtime":
        return zero, total_net_pnl, zero
    return zero, total_net_pnl, zero


def _trade_separation_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    broker_path_pnl = Decimal("0")
    internal_sim_pnl = Decimal("0")
    diagnostic_only_pnl = Decimal("0")
    policy_violation_local_trade_count = 0
    broker_path_lane_count = 0
    internal_only_lane_count = 0
    for row in rows:
        broker_path_pnl += _decimal_or_none(row.get("broker_path_pnl")) or Decimal("0")
        internal_sim_pnl += _decimal_or_none(row.get("internal_sim_pnl")) or Decimal("0")
        diagnostic_only_pnl += _decimal_or_none(row.get("diagnostic_only_pnl")) or Decimal("0")
        if row.get("current_routing_mode") == "IBKR_ROUTED":
            broker_path_lane_count += 1
        if row.get("current_routing_mode") == "INTERNAL_ONLY_DIAGNOSTIC":
            internal_only_lane_count += 1
        if bool(row.get("recent_local_trades_occurred")) and not bool(row.get("local_trading_allowed")):
            policy_violation_local_trade_count += 1
    return {
        "broker_path_pnl": _format_decimal(broker_path_pnl),
        "internal_sim_pnl": _format_decimal(internal_sim_pnl),
        "diagnostic_only_pnl": _format_decimal(diagnostic_only_pnl),
        "broker_path_lane_count": broker_path_lane_count,
        "internal_only_lane_count": internal_only_lane_count,
        "policy_violation_local_trade_count": policy_violation_local_trade_count,
    }


def _routing_policy_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": row.get("strategy_id"),
        "source_instrument": row.get("instrument"),
        "governance_status": row.get("strategy_status"),
        "current_runtime_status": row.get("runtime_status"),
        "local_paper_trading_enabled": row.get("local_paper_trading_enabled"),
        "ibkr_bridge_submit_capable": row.get("ibkr_bridge_submit_capable"),
        "current_routing_mode": row.get("current_routing_mode"),
        "recent_local_trades_occurred": row.get("recent_local_trades_occurred"),
        "recent_trade_route_kind": row.get("recent_trade_route_kind"),
        "current_order_destination": row.get("current_order_destination"),
        "local_trading_allowed": row.get("local_trading_allowed"),
        "submit_allowed": row.get("submit_allowed"),
        "submit_block_reasons": ";".join(list(row.get("submit_block_reasons") or [])),
        "broker_path_pnl": row.get("broker_path_pnl"),
        "internal_sim_pnl": row.get("internal_sim_pnl"),
        "diagnostic_only_pnl": row.get("diagnostic_only_pnl"),
    }


def _local_only_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": row.get("strategy_id"),
        "source_instrument": row.get("instrument"),
        "governance_status": row.get("strategy_status"),
        "current_routing_mode": row.get("current_routing_mode"),
        "recent_local_trades_occurred": row.get("recent_local_trades_occurred"),
        "recent_trade_route_kind": row.get("recent_trade_route_kind"),
        "local_paper_trading_enabled": row.get("local_paper_trading_enabled"),
        "local_trading_allowed": row.get("local_trading_allowed"),
        "recent_trade_origin_label": row.get("recent_trade_origin_label"),
        "internal_sim_pnl": row.get("internal_sim_pnl"),
        "diagnostic_only_pnl": row.get("diagnostic_only_pnl"),
    }


def _count_supported(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"supported": 0, "unsupported": 0}
    for row in rows:
        if str(row.get("instrument") or "").upper() in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
            counts["supported"] += 1
        else:
            counts["unsupported"] += 1
    return counts


def _count_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (_STATUS_PRECEDENCE.get(item[0], 99), item[0])))


def _extract_unrealized_pnl(
    *,
    ledger_position: dict[str, Any] | None,
    tracked_detail: dict[str, Any],
    performance_row: dict[str, Any],
) -> Any:
    if ledger_position is not None and ledger_position.get("unrealized_pnl") is not None:
        return ledger_position.get("unrealized_pnl")
    if tracked_detail.get("open_pnl") is not None:
        return tracked_detail.get("open_pnl")
    return performance_row.get("session_unrealized_pnl")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, "", "None"):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    quantized = value.quantize(Decimal("0.01"))
    return format(quantized, "f")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_audit(audit_events: list[dict[str, Any]], event_type: str, detail: str, extra: dict[str, Any] | None = None) -> None:
    audit_events.append(
        {
            "event_type": event_type,
            "observed_at": _utc_now(),
            "detail": detail,
            **dict(extra or {}),
        }
    )
