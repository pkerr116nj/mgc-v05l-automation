"""Per-strategy governance and performance status for IBKR paper strategy lanes."""

from __future__ import annotations

import csv
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from ..app.shared_strategy_identities import shared_strategy_identities
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    lane_submit_bridge_adapter,
    run_ibkr_paper_strategy_porting,
)
from .track_b_phase1_submit_authority import evaluate_phase1_broker_reconciliation_submit_gate
from ..execution_core.phase1_gc_paper_candidate_registry import (
    is_phase1_gc_guarded_paper_eligible_strategy,
)
from ..execution_core.track_b_runtime_authority_resolver import (
    RuntimeAuthorityResolverConfig,
    resolve_track_b_runtime_authority,
)

_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_governance"
_DEFAULT_VAR_STATUS_PATH = Path("var") / "per_strategy_paper_status.json"
_DEFAULT_VAR_DASHBOARD_PATH = Path("var") / "strategy_probation_dashboard.json"
_DEFAULT_VAR_PERFORMANCE_PATH = Path("var") / "per_strategy_paper_performance.csv"
_DEFAULT_PERFORMANCE_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
_DEFAULT_SIGNAL_AUDIT_PATH = Path("outputs") / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
_DEFAULT_DASHBOARD_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json"
_DEFAULT_PAPER_READINESS_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "paper_readiness_snapshot.json"
_DEFAULT_CANONICAL_READINESS_PATH = Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
_DEFAULT_STARTUP_CONTROL_PLANE_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "startup_control_plane_snapshot.json"
_DEFAULT_SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "supervised_paper_operability_snapshot.json"
_DEFAULT_TEMP_PAPER_INTEGRITY_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "paper_temporary_paper_runtime_integrity_snapshot.json"
_DEFAULT_CONTROL_PLANE_SNAPSHOT_PATH = Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
_DEFAULT_SAFE_STATE_ENVELOPE_PATH = Path("outputs") / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
_DEFAULT_GUARDED_PAPER_LOOP_PATH = Path("outputs") / "track_b_execution_core" / "p0_observe_only" / "latest_p0_observe_only_loop.json"
_DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_PORTING_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_porting"
_DEFAULT_PAPER_SESSION_LANES_DIR = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"
_DEFAULT_PAPER_CONFIG_IN_FORCE_PATH = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
_DEFAULT_PAPER_RUNTIME_TRUTH_PATH = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"
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

_SUPPORTED_EXECUTABLE_INSTRUMENTS = {"MGC", "GC", "MNQ", "NQ", "MES", "ES"}
_PHASE1_CANDLE_FRESHNESS_SECONDS = {"1M": 180.0, "3M": 360.0, "5M": 600.0}
_BACKEND_SOURCE_MONITOR_BLOCK_REASONS = {
    "backend_down",
    "source_snapshot_fallback",
    "paper_runtime_stale",
    "temp_paper_blocked",
}
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
    phase1_reconciliation_gate = evaluate_phase1_broker_reconciliation_submit_gate(
        repo_root=config.repo_root,
        max_age_seconds=config.freshness_window_seconds,
    )
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
    paper_config_in_force = _load_json(config.repo_root / _DEFAULT_PAPER_CONFIG_IN_FORCE_PATH)
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
            phase1_reconciliation_gate=phase1_reconciliation_gate,
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
            phase1_reconciliation_gate=phase1_reconciliation_gate,
            trade_stats=trade_stats_by_lane.get(synthetic_lane_id, {}),
            shared_strategy_id=bridge_strategy_id,
            global_monitor_owner="",
        )
        strategy_rows.append(row)
        seen_lane_ids.add(synthetic_lane_id)

    for configured_row in list(paper_config_in_force.get("lanes") or []):
        lane_id = str(configured_row.get("lane_id") or "").strip()
        if not lane_id or lane_id in seen_lane_ids:
            continue
        if not _configured_runtime_lane_needs_governance_row(configured_row):
            continue
        bridge_adapter = lane_submit_bridge_adapter(lane_id=lane_id)
        if not bridge_adapter:
            continue
        row = _build_governance_row(
            config=config,
            now=now,
            inventory_row=_synthetic_configured_inventory_row(configured_row=configured_row, bridge_adapter=bridge_adapter),
            performance_row=performance_by_lane.get(lane_id, {}),
            signal_row=signal_by_lane.get(lane_id, {}),
            intent_row=intent_by_lane.get(lane_id, {}),
            tracked_details=tracked_details,
            ledger_positions=ledger_positions,
            monitor_status=monitor_status,
            phase1_reconciliation_gate=phase1_reconciliation_gate,
            trade_stats=trade_stats_by_lane.get(lane_id, {}),
            shared_strategy_id=shared_identity_map.get(lane_id),
            global_monitor_owner="",
        )
        strategy_rows.append(row)
        seen_lane_ids.add(lane_id)

    strategy_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))
    pause_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))

    overall_classification = _overall_governance_classification(
        strategy_rows=strategy_rows,
        monitor_status=monitor_status,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
    )
    status_payload = _build_status_payload(
        now=now,
        classification=overall_classification,
        monitor_status=monitor_status,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
        strategy_rows=strategy_rows,
        config=config,
    )
    probation_dashboard = _build_probation_dashboard(
        now=now,
        classification=overall_classification,
        strategy_rows=strategy_rows,
        monitor_status=monitor_status,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
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
        "phase1_broker_reconciliation_gate": phase1_reconciliation_gate,
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


def _configured_runtime_lane_needs_governance_row(configured_row: dict[str, Any]) -> bool:
    if not bool(configured_row.get("paper_only")):
        return False
    lane_mode = str(configured_row.get("lane_mode") or "").strip().upper()
    submit_authority = str(configured_row.get("submit_authority") or "").strip().upper()
    active_evidence_lane = lane_mode in {
        "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        "PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
    }
    promotion_contract_authorized = submit_authority == "PAPER_ONLY_GUARDED_RUNTIME_AFTER_PROMOTION_CONTRACT"
    legacy_configured_canary = bool(configured_row.get("non_approved")) and bool(configured_row.get("exclude_from_strategy_performance"))
    return active_evidence_lane or promotion_contract_authorized or legacy_configured_canary


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
    requested = str(strategy_id or "").strip()
    payload = _refresh_governance_payload_if_needed(repo_root=repo_root, payload=payload, strategy_id=requested)
    return _select_governance_strategy(payload=payload, strategy_id=requested)


def _select_governance_strategy(*, payload: dict[str, Any], strategy_id: str) -> dict[str, Any]:
    rows = list(payload.get("strategies") or [])
    selected = None
    if strategy_id:
        for row in rows:
            identifiers = {
                str(row.get("strategy_id") or "").strip(),
                str(row.get("bridge_strategy_id") or "").strip(),
                str(row.get("standalone_strategy_id") or "").strip(),
            }
            if strategy_id in identifiers:
                selected = dict(row)
                break
    payload = dict(payload)
    payload["selected_strategy"] = selected
    payload["submit_allowed"] = bool(selected.get("submit_allowed")) if isinstance(selected, dict) else False
    payload["block_reasons"] = list(selected.get("submit_block_reasons") or []) if isinstance(selected, dict) else ["paper_strategy_governance_strategy_missing"]
    if strategy_id and selected is None:
        payload["detail"] = f"Paper strategy governance has no row for strategy identity {strategy_id}."
    elif isinstance(selected, dict) and not bool(selected.get("submit_allowed")):
        payload["detail"] = _selected_governance_block_detail(selected)
        payload["backend_source_readiness"] = selected.get("backend_source_readiness")
    return payload


def _refresh_governance_payload_if_needed(
    *,
    repo_root: Path,
    payload: dict[str, Any],
    strategy_id: str,
) -> dict[str, Any]:
    generated_at = _parse_datetime(payload.get("generated_at"))
    freshness_window_seconds = float(IbkrPaperStrategyGovernanceConfig(repo_root=repo_root).freshness_window_seconds)
    payload_stale = (
        generated_at is None
        or max(0.0, (datetime.now(timezone.utc) - generated_at).total_seconds()) > freshness_window_seconds
    )
    strategy_missing = bool(strategy_id) and _select_governance_strategy(payload=payload, strategy_id=strategy_id).get("selected_strategy") is None
    readiness_newer = _backend_readiness_artifacts_newer_than(repo_root=repo_root, generated_at=generated_at)
    if not payload_stale and not strategy_missing and not readiness_newer:
        return payload
    try:
        refreshed = run_ibkr_paper_strategy_governance(
            config=IbkrPaperStrategyGovernanceConfig(repo_root=repo_root),
        )
        write_ibkr_paper_strategy_governance_artifacts(
            config=IbkrPaperStrategyGovernanceConfig(repo_root=repo_root),
            artifacts=refreshed,
        )
        return dict(refreshed.status_payload)
    except Exception:
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
    phase1_reconciliation_gate: dict[str, Any],
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
    backend_source_readiness = _backend_source_live_readiness(
        config=config,
        strategy_id=lane_id,
        instrument=instrument,
        required_instruments=_backend_source_required_instruments(
            inventory_row=inventory_row,
            performance_row=performance_row,
            signal_row=signal_row,
            instrument=instrument,
        ),
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
    if not bool(phase1_reconciliation_gate.get("ready")):
        submit_block_reasons.append("phase1_broker_reconciliation_not_clear")
    if daily_order_count is not None and int(daily_order_count) >= int(config.daily_order_limit):
        submit_block_reasons.append("daily_order_limit_reached")
    if weekly_order_count is not None and int(weekly_order_count) >= int(config.weekly_order_limit):
        submit_block_reasons.append("weekly_order_limit_reached")
    if max_drawdown is not None and abs(float(max_drawdown)) >= float(config.drawdown_limit):
        submit_block_reasons.append("drawdown_limit_reached")
    if not bool(backend_source_readiness.get("live_ready")):
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
    approved_phase1_strategy = is_phase1_gc_guarded_paper_eligible_strategy(
        strategy_id=lane_id,
        instrument=instrument,
    )
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
        "strategy_approved": approved_phase1_strategy,
        "paper_strategy_approved": approved_phase1_strategy,
        "approved_phase1_strategy": approved_phase1_strategy,
        "paper_candidate_scope": "GC_ONLY" if approved_phase1_strategy else None,
        "live_money_eligible": False,
        "intent_action": intent_row.get("action"),
        "intent_reason": intent_row.get("reason"),
        "route_blockers": inventory_blockers,
        "pause_reasons": list(dict.fromkeys(pause_reasons)),
        "submit_block_reasons": list(dict.fromkeys(submit_block_reasons)),
        "submit_allowed": submit_allowed,
        "bridge_invocation_allowed": submit_allowed and bool(inventory_row.get("bridge_adapter_ready")),
        "broker_session_authority_classification": backend_source_readiness.get(
            "broker_session_authority_classification"
        ),
        "broker_session_connection_mode": backend_source_readiness.get("broker_session_connection_mode"),
        "broker_session_allowed_uses": dict(backend_source_readiness.get("broker_session_allowed_uses") or {}),
        "broker_session_authority_blockers": list(
            backend_source_readiness.get("broker_session_authority_blockers") or []
        ),
        "callback_ownership_attribution": backend_source_readiness.get("callback_ownership_attribution"),
        "broker_session_submit_alignment": backend_source_readiness.get("broker_session_submit_alignment"),
        "backend_source_readiness": backend_source_readiness,
        "backend_source_readiness_detail": backend_source_readiness.get("detail"),
        "monitor_health": monitor_status.get("health_classification"),
        "monitor_stale": monitor_status.get("stale"),
        "monitor_open_orders": monitor_status.get("open_order_count"),
        "legacy_monitor_authority": "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY",
        "phase1_broker_reconciliation_gate": {
            "classification": phase1_reconciliation_gate.get("classification"),
            "ready": phase1_reconciliation_gate.get("ready"),
            "block_reasons": list(phase1_reconciliation_gate.get("block_reasons") or []),
            "detail": phase1_reconciliation_gate.get("detail"),
            "generated_at": phase1_reconciliation_gate.get("generated_at"),
            "age_seconds": phase1_reconciliation_gate.get("age_seconds"),
        },
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


def _selected_governance_block_detail(selected: dict[str, Any]) -> str:
    reasons = [str(reason or "").strip() for reason in list(selected.get("submit_block_reasons") or []) if str(reason or "").strip()]
    detail = f"Paper strategy governance blocked submit: {', '.join(reasons) or 'unknown_reason'}"
    readiness_detail = str(selected.get("backend_source_readiness_detail") or "").strip()
    if "backend_or_source_not_live_ready" in reasons and readiness_detail:
        detail = f"{detail}; {readiness_detail}"
    return detail


def _backend_source_required_instruments(
    *,
    inventory_row: dict[str, Any],
    performance_row: dict[str, Any],
    signal_row: dict[str, Any],
    instrument: str,
) -> list[str]:
    required: list[str] = []
    for row in (inventory_row, performance_row, signal_row):
        for key in (
            "required_market_data_symbols",
            "required_market_data_instruments",
            "cross_instrument_dependencies",
            "required_source_symbols",
            "source_instruments",
        ):
            required.extend(_coerce_instrument_list(row.get(key)))
    required.extend(_coerce_instrument_list(instrument))
    return sorted(dict.fromkeys(required))


def _coerce_instrument_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.replace(";", ",").split(",")]
    elif isinstance(value, dict):
        raw_values = []
        for key in ("symbol", "instrument", "symbols", "instruments"):
            raw_values.extend(_coerce_instrument_list(value.get(key)))
        return raw_values
    else:
        try:
            raw_values = list(value)
        except TypeError:
            raw_values = [value]
    normalized: list[str] = []
    for raw_value in raw_values:
        symbol = str(raw_value or "").strip().upper()
        if symbol:
            normalized.append(symbol)
    return normalized


def _backend_shared_services_authority(
    *,
    config: IbkrPaperStrategyGovernanceConfig,
    required_instruments: list[str],
) -> dict[str, Any]:
    control_plane = _load_json(config.repo_root / _DEFAULT_CONTROL_PLANE_SNAPSHOT_PATH)
    safe_state = _load_json(config.repo_root / _DEFAULT_SAFE_STATE_ENVELOPE_PATH)
    guarded_loop = _load_json(config.repo_root / _DEFAULT_GUARDED_PAPER_LOOP_PATH)
    runtime_authority = resolve_track_b_runtime_authority(
        RuntimeAuthorityResolverConfig(repo_root=config.repo_root),
        process_rows_provider=lambda repo_root: _guarded_loop_processes(repo_root),
    )
    if not control_plane and not safe_state and not guarded_loop:
        return {
            "present": False,
            "ready": False,
            "source": "execution_core_control_plane_safe_state_guarded_loop_phase1",
            "block_reasons": ["shared_services_authority_missing"],
            "dashboard_projection_consumed": False,
        }

    block_reasons: list[str] = []
    cp_classification = str(control_plane.get("classification") or "").strip().upper()
    coherence = str(control_plane.get("shared_truth_coherence_status") or "").strip().upper()
    safe_classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "").strip().upper()

    block_reasons.extend(str(reason) for reason in runtime_authority.get("blockers") or [])
    if control_plane and cp_classification not in {"CONTROL_PLANE_READY", "CONTROL_PLANE_SNAPSHOT_READY"}:
        block_reasons.append("control_plane_not_ready")
    if control_plane and coherence and coherence != "COHERENT":
        block_reasons.append("shared_truth_not_coherent")
    if safe_state and safe_classification not in {"", "SAFE_STATE_NORMAL", "NORMAL"}:
        block_reasons.append("safe_state_not_normal")
    if safe_state and safe_state.get("submit_allowed") is False:
        block_reasons.append("safe_state_submit_not_allowed")
    if safe_state and safe_state.get("broker_mutation_allowed") is False:
        block_reasons.append("safe_state_broker_mutation_not_allowed")
    if control_plane.get("live_money_eligible") is True or safe_state.get("live_money_eligible") is True or guarded_loop.get("live_money_eligible") is True:
        block_reasons.append("live_money_eligible_true")
    if control_plane.get("paper_proof_invoked") is True or safe_state.get("paper_proof_invoked") is True or guarded_loop.get("paper_proof_invoked") is True:
        block_reasons.append("paper_proof_invoked_true")
    if control_plane.get("agent_health_has_duplicate_writer") is True:
        block_reasons.append("duplicate_writer_detected")

    phase1_status = _phase1_market_data_readiness(config.repo_root, required_instruments)
    if required_instruments and not phase1_status.get("ready"):
        block_reasons.append("phase1_runtime_market_data_not_ready")

    unique_block_reasons = list(dict.fromkeys(block_reasons))
    return {
        "present": True,
        "ready": not unique_block_reasons,
        "source": "execution_core_control_plane_safe_state_guarded_loop_phase1",
        "block_reasons": unique_block_reasons,
        "control_plane_snapshot_id": control_plane.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": control_plane.get("shared_truth_generation_id")
        or control_plane.get("shared_truth_refresh_generation_id")
        or safe_state.get("shared_truth_generation_id"),
        "safe_state_classification": safe_classification or None,
        "runtime_pid": runtime_authority.get("runtime_pid"),
        "runtime_command": runtime_authority.get("runtime_command"),
        "runtime_generation_id": runtime_authority.get("runtime_generation_id"),
        "runtime_authority_classification": runtime_authority.get("classification"),
        "runtime_authority_diagnostics": list(runtime_authority.get("diagnostics") or []),
        "runtime_authority_legacy_stale_policy": runtime_authority.get("legacy_stale_policy"),
        "process_count": int(runtime_authority.get("process_count") or 0),
        "matching_process_count": int(runtime_authority.get("matching_process_count") or 0),
        "phase1_runtime_market_data": phase1_status,
        "dashboard_projection_consumed": False,
    }


def _guarded_loop_runtime_generation(payload: dict[str, Any]) -> str | None:
    latest_iteration = payload.get("latest_iteration")
    if isinstance(latest_iteration, dict):
        snapshot = latest_iteration.get("control_plane_snapshot")
        if isinstance(snapshot, dict):
            generation = snapshot.get("safe_state_runtime_generation_id") or snapshot.get("runtime_generation_id")
            if generation:
                return str(generation)
    for key in ("runtime_generation_id", "safe_state_runtime_generation_id"):
        if payload.get(key):
            return str(payload.get(key))
    return None


def _guarded_loop_processes(repo_root: Path) -> tuple[dict[str, Any], ...]:
    try:
        completed = subprocess.run(
            ["ps", "-efww"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return ()
    repo_text = str(repo_root.resolve())
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        if "track_b_p0_observe_only_loop" not in line or "--mode guarded-paper" not in line:
            continue
        if (
            " egrep " in line
            or " grep " in line
            or "/bin/zsh -c" in line
            or "SCREEN -dmS" in line
            or " login -pflq " in line
        ):
            continue
        parts = line.split(None, 7)
        pid = int(parts[1]) if len(parts) > 1 and str(parts[1]).isdigit() else 0
        if pid <= 0:
            continue
        command = parts[7] if len(parts) > 7 else line
        rows.append(
            {
                "pid": pid,
                "command": command,
                "root_matches": repo_text in line,
                "wrong_root": "Documents/MGC-v05l" in line or "Mobile Documents" in line,
            }
        )
    return tuple(rows)


def _phase1_market_data_readiness(repo_root: Path, required_instruments: list[str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for instrument in required_instruments:
        candidates: list[dict[str, Any]] = []
        for symbol in _phase1_symbols_for_instrument(instrument):
            for timeframe in ("1m", "5m"):
                candidates.append(_phase1_runtime_candle_status(repo_root, symbol=symbol, timeframe=timeframe))
        ready_candidates = [row for row in candidates if row.get("ready")]
        selected = ready_candidates[0] if ready_candidates else (candidates[0] if candidates else {})
        rows.append(
            {
                "instrument": instrument,
                "ready": bool(ready_candidates),
                "selected": selected,
                "candidates": candidates,
            }
        )
    return {
        "ready": all(row.get("ready") for row in rows) if rows else True,
        "required_instruments": list(required_instruments),
        "rows": rows,
    }


def _phase1_symbols_for_instrument(instrument: str) -> list[str]:
    symbol = str(instrument or "").strip().upper()
    mapping = {
        "GC": ["MGC", "GC"],
        "NQ": ["MNQ", "NQ"],
        "ES": ["MES", "ES"],
    }
    values = mapping.get(symbol, [symbol])
    return [value for value in values if value]


def _phase1_runtime_candle_status(repo_root: Path, *, symbol: str, timeframe: str) -> dict[str, Any]:
    path = repo_root / _DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT / symbol / timeframe / "latest_runtime_candles.json"
    payload = _load_json(path)
    generated_at = _parse_datetime(payload.get("generated_at"))
    now = datetime.now(timezone.utc)
    age_seconds = None if generated_at is None else max(0.0, (now - generated_at).total_seconds())
    normalized_timeframe = timeframe.upper()
    freshness_window = _PHASE1_CANDLE_FRESHNESS_SECONDS.get(normalized_timeframe, 600.0)
    bars = list(payload.get("bars") or payload.get("candles") or payload.get("candle_history") or [])
    source_category = str(payload.get("source_category") or "").strip().upper()
    block_reasons: list[str] = []
    if not payload:
        block_reasons.append("phase1_runtime_candle_artifact_missing")
    if payload and str(payload.get("symbol") or "").strip().upper() not in {"", symbol.upper()}:
        block_reasons.append("phase1_runtime_candle_symbol_mismatch")
    if payload and str(payload.get("timeframe") or "").strip().upper() not in {"", normalized_timeframe}:
        block_reasons.append("phase1_runtime_candle_timeframe_mismatch")
    if payload and payload.get("completed_candles_only") is False:
        block_reasons.append("phase1_runtime_candles_not_completed_only")
    if payload and payload.get("realtime_feed_confirmed") is False:
        block_reasons.append("phase1_runtime_feed_not_confirmed")
    if payload and source_category in {"RESEARCH", "OFFLINE", "REPLAY", "DASHBOARD_PROJECTION"}:
        block_reasons.append("phase1_runtime_candle_not_runtime_authority")
    if not bars:
        block_reasons.append("phase1_runtime_candles_empty")
    if generated_at is None:
        block_reasons.append("phase1_runtime_candle_generated_at_missing")
    elif age_seconds is not None and age_seconds > freshness_window:
        block_reasons.append("phase1_runtime_candle_artifact_stale")
    latest_bar_timestamp = payload.get("latest_bar_timestamp")
    if not latest_bar_timestamp and bars:
        last_bar = bars[-1]
        if isinstance(last_bar, dict):
            latest_bar_timestamp = last_bar.get("bar_end") or last_bar.get("timestamp") or last_bar.get("time")
    return {
        "ready": not block_reasons,
        "symbol": symbol,
        "timeframe": timeframe,
        "path": str(path),
        "generated_at": generated_at.isoformat() if generated_at is not None else None,
        "age_seconds": _round_age(age_seconds),
        "freshness_window_seconds": freshness_window,
        "latest_bar_timestamp": latest_bar_timestamp,
        "bar_count": len(bars),
        "block_reasons": block_reasons,
        "source_category": source_category or "PHASE1_RUNTIME_MARKET_DATA",
        "dashboard_projection_consumed": False,
    }


def _backend_source_live_readiness(
    *,
    config: IbkrPaperStrategyGovernanceConfig,
    strategy_id: str | None = None,
    instrument: str | None = None,
    required_instruments: list[str] | None = None,
) -> dict[str, Any]:
    freshness_window = float(config.freshness_window_seconds)
    canonical = _load_json(config.repo_root / _DEFAULT_CANONICAL_READINESS_PATH)
    paper_runtime_truth = _load_json(config.repo_root / _DEFAULT_PAPER_RUNTIME_TRUTH_PATH)
    readiness = _load_json(config.repo_root / _DEFAULT_PAPER_READINESS_SNAPSHOT_PATH)
    startup = _load_json(config.repo_root / _DEFAULT_STARTUP_CONTROL_PLANE_SNAPSHOT_PATH)
    supervised = _load_json(config.repo_root / _DEFAULT_SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH)
    temp_integrity = _load_json(config.repo_root / _DEFAULT_TEMP_PAPER_INTEGRITY_SNAPSHOT_PATH)
    artifacts = {
        "canonical_readiness": _artifact_status(
            config.repo_root / _DEFAULT_CANONICAL_READINESS_PATH,
            canonical,
            freshness_window_seconds=freshness_window,
            required=False,
        ),
        "paper_runtime_truth": _artifact_status(
            config.repo_root / _DEFAULT_PAPER_RUNTIME_TRUTH_PATH,
            paper_runtime_truth,
            freshness_window_seconds=freshness_window,
            required=False,
        ),
        "paper_readiness": _artifact_status(
            config.repo_root / _DEFAULT_PAPER_READINESS_SNAPSHOT_PATH,
            readiness,
            freshness_window_seconds=freshness_window,
        ),
        "startup_control_plane": _artifact_status(
            config.repo_root / _DEFAULT_STARTUP_CONTROL_PLANE_SNAPSHOT_PATH,
            startup,
            freshness_window_seconds=freshness_window,
        ),
        "supervised_paper_operability": _artifact_status(
            config.repo_root / _DEFAULT_SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH,
            supervised,
            freshness_window_seconds=freshness_window,
        ),
        "temporary_paper_runtime_integrity": _artifact_status(
            config.repo_root / _DEFAULT_TEMP_PAPER_INTEGRITY_SNAPSHOT_PATH,
            temp_integrity,
            freshness_window_seconds=freshness_window,
            required=False,
        ),
    }
    block_reasons: list[str] = []
    required_instrument_list = sorted(dict.fromkeys(_coerce_instrument_list(required_instruments) or _coerce_instrument_list(instrument)))
    shared_services_authority = _backend_shared_services_authority(
        config=config,
        required_instruments=required_instrument_list,
    )
    canonical_status = artifacts["canonical_readiness"]
    canonical_present = bool(canonical)
    canonical_fresh = bool(canonical_status["fresh"])
    canonical_authoritative = bool(canonical_present and canonical_fresh)
    if canonical_present and not canonical_fresh:
        block_reasons.append("canonical_readiness_artifact_stale")

    source_faults = (
        {
            "market_data_stale_count": 0,
            "bar_authority_unavailable_count": 0,
            "blocking_fault_count": 0,
            "readiness_scope": "canonical_track_b_runtime_readiness",
            "required_instruments": required_instrument_list,
            "relevant_lane_ids": [],
            "global_market_data_stale_count": int(readiness.get("market_data_stale_count") or 0),
            "global_bar_authority_unavailable_count": int(readiness.get("bar_authority_unavailable_count") or 0),
            "global_blocking_fault_count": int(readiness.get("blocking_fault_count") or 0),
        }
        if canonical_authoritative
        else _scoped_backend_source_fault_counts(
            readiness=readiness,
            strategy_id=strategy_id,
            required_instruments=required_instrument_list,
        )
    )
    market_data_stale_count = int(source_faults.get("market_data_stale_count") or 0)
    bar_authority_unavailable_count = int(source_faults.get("bar_authority_unavailable_count") or 0)
    blocking_fault_count = int(source_faults.get("blocking_fault_count") or 0)

    canonical_state = str(canonical.get("canonical_readiness") or canonical.get("state") or "").strip().upper()
    canonical_runtime = dict(canonical.get("runtime") or {})
    canonical_root_guard = dict(canonical.get("root_guard_summary") or {})
    broker_session_diagnostic = _canonical_broker_session_diagnostic(canonical)
    paper_stack_authority = _canonical_paper_stack_submit_authority(
        canonical=canonical,
        canonical_status=canonical_status,
        paper_runtime_truth=paper_runtime_truth,
        paper_runtime_truth_status=artifacts["paper_runtime_truth"],
        freshness_window_seconds=freshness_window,
    )
    if canonical_authoritative:
        runtime_running = bool(paper_stack_authority.get("runtime_running"))
        paper_runtime_ready = bool(paper_stack_authority.get("paper_runtime_ready"))
        paper_trade_allowed = bool(paper_stack_authority.get("paper_trade_allowed"))
        startup_ready = paper_stack_authority.get("canonical_readiness") in {
            "READY_SUBMIT_CAPABLE",
            "READY_OBSERVATION_ONLY",
        }
        supervised_usable = paper_trade_allowed
    else:
        paper_runtime_ready = bool(readiness.get("paper_runtime_ready"))
        runtime_running = bool(readiness.get("runtime_running"))
        paper_trade_allowed = bool(readiness.get("paper_trade_allowed"))
        startup_ready = str(startup.get("overall_state") or "").strip().upper() == "READY"
        supervised_usable = bool(supervised.get("app_usable_for_supervised_paper"))
    temp_paper_blocked = bool(temp_integrity.get("temp_paper_blocked"))

    if canonical_authoritative and canonical.get("live_money_eligible") is True:
        block_reasons.append("canonical_live_money_eligible_true")
    if canonical_authoritative and canonical.get("paper_proof_invoked") is True:
        block_reasons.append("canonical_paper_proof_invoked_true")
    if canonical_authoritative and canonical_root_guard.get("root_match") is not True:
        block_reasons.append("canonical_root_not_matched")
    if canonical_authoritative and canonical_state != "READY_SUBMIT_CAPABLE":
        block_reasons.append("canonical_readiness_not_submit_capable")
    if canonical_authoritative and not bool(paper_stack_authority.get("ready")):
        block_reasons.extend(str(reason) for reason in list(paper_stack_authority.get("block_reasons") or []))
    if (readiness or canonical_authoritative) and not runtime_running:
        block_reasons.append("paper_runtime_not_running")
    if (readiness or canonical_authoritative) and not paper_runtime_ready:
        block_reasons.append("paper_runtime_not_ready")
    if (readiness or canonical_authoritative) and not paper_trade_allowed:
        block_reasons.append("paper_trade_not_allowed")
    if market_data_stale_count > 0:
        block_reasons.append("source_market_data_stale")
    if bar_authority_unavailable_count > 0:
        block_reasons.append("bar_authority_unavailable")
    if blocking_fault_count > 0:
        block_reasons.append("blocking_faults_present")
    if not canonical_authoritative:
        required_artifacts = [
            artifacts["paper_readiness"],
            artifacts["startup_control_plane"],
            artifacts["supervised_paper_operability"],
        ]
        missing_required = [row["label"] for row in required_artifacts if not row["present"]]
        stale_required = [row["label"] for row in required_artifacts if row["present"] and not row["fresh"]]
        if missing_required:
            block_reasons.append("backend_readiness_artifact_missing")
        if stale_required:
            block_reasons.append("backend_readiness_artifact_stale")
        if startup and not startup_ready:
            block_reasons.append("startup_control_plane_not_ready")
        if supervised and not supervised_usable:
            block_reasons.append("supervised_paper_not_usable")
    temp_status = artifacts["temporary_paper_runtime_integrity"]
    if not canonical_authoritative and temp_status["present"] and not temp_status["fresh"]:
        block_reasons.append("backend_readiness_artifact_stale")
    if temp_paper_blocked:
        block_reasons.append("temp_paper_blocked")
    shared_service_block_reasons = [
        str(reason)
        for reason in list(shared_services_authority.get("block_reasons") or [])
        if str(reason)
    ]
    shared_phase1_status = dict(shared_services_authority.get("phase1_runtime_market_data") or {})
    if shared_phase1_status and shared_phase1_status.get("ready") is not True and not canonical_authoritative:
        block_reasons.append("phase1_runtime_market_data_not_ready")
    if shared_services_authority.get("present") is True and shared_services_authority.get("ready") is not True:
        if canonical_authoritative and canonical_state == "READY_SUBMIT_CAPABLE":
            conflict_reasons = [
                reason
                for reason in shared_service_block_reasons
                if reason.startswith("safe_state")
                or "live_money" in reason
                or "paper_proof" in reason
                or "broker_reconciliation" in reason
                or "duplicate_writer" in reason
            ]
            if conflict_reasons:
                block_reasons.append("runtime_authority_conflict")
                block_reasons.extend(conflict_reasons)
        elif not canonical_authoritative:
            block_reasons.append("shared_services_authority_not_ready")
            block_reasons.extend(shared_service_block_reasons)

    block_reasons = list(dict.fromkeys(block_reasons))
    live_ready = not block_reasons
    if canonical_authoritative and live_ready:
        detail = (
            "backend/source readiness ready from canonical_paper_stack_runtime_authority; "
            "deprecated guarded-loop artifacts treated as diagnostic only; "
            f"required_instruments={required_instrument_list}; "
            f"runtime_instance_id={paper_runtime_truth.get('runtime_instance_id')} "
            f"writer_authority={paper_runtime_truth.get('writer_authority')} "
            f"canonical_readiness={canonical_state}"
        )
    elif shared_services_authority.get("ready") is True and not canonical_present:
        detail = (
            "backend/source readiness ready from execution_core_control_plane_safe_state_guarded_loop_phase1; "
            "legacy canonical/operator readiness treated as diagnostic projection; "
            f"required_instruments={required_instrument_list}; "
            f"runtime_pid={shared_services_authority.get('runtime_pid')} "
            f"runtime_generation_id={shared_services_authority.get('runtime_generation_id')} "
            f"control_plane_snapshot_id={shared_services_authority.get('control_plane_snapshot_id')} "
            f"shared_truth_generation_id={shared_services_authority.get('shared_truth_generation_id')}"
        )
    else:
        detail = _backend_source_readiness_detail(
            live_ready=live_ready,
            block_reasons=block_reasons,
            artifacts=artifacts,
            freshness_window_seconds=freshness_window,
            readiness=readiness,
            startup=startup,
            supervised=supervised,
            temp_integrity=temp_integrity,
            source_faults=source_faults,
            canonical=canonical,
            canonical_authoritative=canonical_authoritative,
            runtime_running=runtime_running,
            paper_runtime_ready=paper_runtime_ready,
            paper_trade_allowed=paper_trade_allowed,
            startup_ready=startup_ready,
            supervised_usable=supervised_usable,
        )
    return {
        "live_ready": live_ready,
        "block_reasons": block_reasons,
        "detail": detail,
        "freshness_window_seconds": freshness_window,
        "artifacts": artifacts,
        "paper_runtime_ready": paper_runtime_ready,
        "runtime_running": runtime_running,
        "paper_trade_allowed": paper_trade_allowed,
        "market_data_stale_count": market_data_stale_count,
        "bar_authority_unavailable_count": bar_authority_unavailable_count,
        "blocking_fault_count": blocking_fault_count,
        "global_market_data_stale_count": int(readiness.get("market_data_stale_count") or 0),
        "global_bar_authority_unavailable_count": int(readiness.get("bar_authority_unavailable_count") or 0),
        "global_blocking_fault_count": int(readiness.get("blocking_fault_count") or 0),
        "readiness_scope": source_faults.get("readiness_scope"),
        "required_instruments": required_instrument_list,
        "relevant_lane_ids": list(source_faults.get("relevant_lane_ids") or []),
        "startup_control_plane_ready": startup_ready,
        "supervised_paper_usable": supervised_usable,
        "temp_paper_blocked": temp_paper_blocked,
        "canonical_readiness": canonical_state or None,
        "canonical_readiness_authoritative": canonical_authoritative,
        **broker_session_diagnostic,
        "paper_stack_authority": paper_stack_authority,
        "canonical_readiness_artifact": artifacts["canonical_readiness"],
        "presentation_readiness_authority": "DIAGNOSTIC_ONLY_WHEN_CANONICAL_PRESENT",
        "source": (
            "canonical_paper_stack_runtime_authority"
            if canonical_authoritative and live_ready
            else str(shared_services_authority.get("source"))
            if shared_services_authority.get("ready") is True and not canonical_present
            else "canonical_track_b_runtime_readiness" if canonical_present else "operator_dashboard_readiness_artifacts"
        ),
        "shared_services_authority": shared_services_authority,
        "shared_services_authority_ready": bool(shared_services_authority.get("ready")),
        "dashboard_projection_consumed": False,
    }


def _canonical_paper_stack_submit_authority(
    *,
    canonical: dict[str, Any],
    canonical_status: dict[str, Any],
    paper_runtime_truth: dict[str, Any],
    paper_runtime_truth_status: dict[str, Any],
    freshness_window_seconds: float,
) -> dict[str, Any]:
    canonical_state = str(canonical.get("canonical_readiness") or canonical.get("state") or "").strip().upper()
    canonical_runtime = dict(canonical.get("runtime") or {})
    broker_truth_lease = dict(canonical.get("broker_truth_lease") or {})
    canonical_reconciliation = dict(canonical.get("phase1_reconciliation") or {})
    root_guard = dict(canonical.get("root_guard_summary") or {})
    duplicate_writer = dict(paper_runtime_truth.get("duplicate_writer_detection") or {})
    block_reasons: list[str] = []

    if not canonical:
        block_reasons.append("canonical_readiness_artifact_missing")
    elif not bool(canonical_status.get("fresh")):
        block_reasons.append("canonical_readiness_artifact_stale")
    if canonical and canonical_state != "READY_SUBMIT_CAPABLE":
        block_reasons.append("canonical_readiness_not_submit_capable")
    if canonical and canonical.get("paper_only") is not True:
        block_reasons.append("canonical_paper_only_not_true")
    if canonical.get("live_money_eligible") is True:
        block_reasons.append("canonical_live_money_eligible_true")
    if canonical.get("paper_proof_invoked") is True:
        block_reasons.append("canonical_paper_proof_invoked_true")
    if root_guard and root_guard.get("root_match") is not True:
        block_reasons.append("canonical_root_not_matched")

    runtime_running = bool(canonical_runtime.get("running"))
    paper_runtime_ready = bool(
        runtime_running
        and canonical_runtime.get("healthy") is True
        and canonical_runtime.get("runtime_ingestion_fresh") is True
    )
    paper_trade_allowed = canonical_state == "READY_SUBMIT_CAPABLE"
    if canonical and not runtime_running:
        block_reasons.append("paper_runtime_not_running")
    if canonical and not paper_runtime_ready:
        block_reasons.append("paper_runtime_not_ready")
    if canonical and not paper_trade_allowed:
        block_reasons.append("paper_trade_not_allowed")

    if not paper_runtime_truth:
        block_reasons.append("paper_runtime_truth_missing")
    elif not bool(paper_runtime_truth_status.get("fresh")):
        block_reasons.append("paper_runtime_truth_stale")
    if paper_runtime_truth and str(paper_runtime_truth.get("writer_authority") or "").strip().upper() != "SINGLE_WRITER":
        block_reasons.append("paper_runtime_truth_not_single_writer")
    if paper_runtime_truth and paper_runtime_truth.get("paper_only") is not True:
        block_reasons.append("paper_runtime_truth_paper_only_not_true")
    if paper_runtime_truth.get("live_money_eligible") is True:
        block_reasons.append("paper_runtime_truth_live_money_eligible_true")
    if paper_runtime_truth.get("paper_proof_invoked") is True:
        block_reasons.append("paper_runtime_truth_paper_proof_invoked_true")
    if paper_runtime_truth and str(paper_runtime_truth.get("freshness_state") or "").strip().upper() not in {"", "FRESH"}:
        block_reasons.append("paper_runtime_truth_not_fresh")
    if paper_runtime_truth and str(paper_runtime_truth.get("heartbeat_state") or "").strip().upper() not in {"", "HEALTHY"}:
        block_reasons.append("paper_runtime_truth_heartbeat_not_healthy")
    if duplicate_writer.get("duplicate_writer_detected") is True:
        block_reasons.append("paper_runtime_truth_duplicate_writer_detected")
    duplicate_count = _int_or_none(duplicate_writer.get("duplicate_runtime_submitter_count")) or 0
    if duplicate_count > 0:
        block_reasons.append("paper_runtime_truth_duplicate_writer_detected")

    if broker_truth_lease:
        lease_age = _decimal_or_none(broker_truth_lease.get("age_seconds"))
        if broker_truth_lease.get("available") is False:
            block_reasons.append("broker_truth_lease_unavailable")
        if str(broker_truth_lease.get("lease_state") or "").strip().upper() != "ACTIVE":
            block_reasons.append("broker_truth_lease_not_active")
        if lease_age is not None and lease_age > Decimal(str(freshness_window_seconds)):
            block_reasons.append("broker_truth_lease_stale")
        if broker_truth_lease.get("broker_reconciled") is False:
            block_reasons.append("broker_truth_lease_not_reconciled")
        if _int_or_none(broker_truth_lease.get("review_required_count")) not in {None, 0}:
            block_reasons.append("broker_truth_review_required_present")
        if broker_truth_lease.get("live_money_eligible") is True:
            block_reasons.append("broker_truth_live_money_eligible_true")
        for reason in list(broker_truth_lease.get("blockers") or []):
            normalized = str(reason or "").strip()
            if normalized:
                block_reasons.append(normalized)

    if canonical_reconciliation:
        reconciliation_age = _decimal_or_none(canonical_reconciliation.get("age_seconds"))
        classification = str(canonical_reconciliation.get("classification") or "").strip().upper()
        if canonical_reconciliation.get("fresh") is False:
            block_reasons.append("phase1_broker_reconciliation_stale")
        if reconciliation_age is not None and reconciliation_age > Decimal(str(freshness_window_seconds)):
            block_reasons.append("phase1_broker_reconciliation_stale")
        if "RECONCILED" not in classification:
            block_reasons.append("phase1_broker_reconciliation_not_reconciled")
        if canonical_reconciliation.get("broker_reconciled") is False:
            block_reasons.append("phase1_broker_reconciled_false")
        if _int_or_none(canonical_reconciliation.get("review_required_count")) not in {None, 0}:
            block_reasons.append("phase1_review_required_present")
        if _int_or_none(canonical_reconciliation.get("track_b_broker_open_order_count")) not in {None, 0}:
            block_reasons.append("phase1_open_orders_present")
        if canonical_reconciliation.get("live_money_eligible") is True:
            block_reasons.append("phase1_live_money_eligible_true")
        for reason in list(canonical_reconciliation.get("blockers") or []):
            normalized = str(reason or "").strip()
            if normalized:
                block_reasons.append(normalized)

    block_reasons = list(dict.fromkeys(block_reasons))
    return {
        "ready": not block_reasons,
        "source": "canonical_paper_stack_runtime_authority",
        "block_reasons": block_reasons,
        "canonical_readiness": canonical_state or None,
        "runtime_running": runtime_running,
        "paper_runtime_ready": paper_runtime_ready,
        "paper_trade_allowed": paper_trade_allowed,
        "paper_runtime_truth_artifact": paper_runtime_truth_status,
        "paper_runtime_truth_writer_authority": paper_runtime_truth.get("writer_authority"),
        "paper_runtime_truth_freshness_state": paper_runtime_truth.get("freshness_state"),
        "paper_runtime_truth_heartbeat_state": paper_runtime_truth.get("heartbeat_state"),
        "broker_truth_lease_state": broker_truth_lease.get("lease_state"),
        "phase1_reconciliation_classification": canonical_reconciliation.get("classification"),
    }


def _canonical_broker_session_diagnostic(canonical: dict[str, Any]) -> dict[str, Any]:
    if not canonical:
        return {
            "broker_session_authority_classification": "BROKER_SESSION_AUTHORITY_MISSING",
            "broker_session_connection_mode": "UNKNOWN",
            "broker_session_allowed_uses": {},
            "broker_session_authority_blockers": ["broker_session_authority_missing"],
            "callback_ownership_attribution": None,
            "broker_session_submit_alignment": "UNKNOWN",
        }
    blockers = []
    for row in list(canonical.get("broker_session_authority_blockers") or []):
        if isinstance(row, dict):
            code = str(row.get("code") or "").strip()
            blockers.append(code or dict(row))
        else:
            text = str(row or "").strip()
            if text:
                blockers.append(text)
    return {
        "broker_session_authority_classification": canonical.get("broker_session_authority_classification")
        or "BROKER_SESSION_AUTHORITY_MISSING",
        "broker_session_connection_mode": canonical.get("broker_session_connection_mode") or "UNKNOWN",
        "broker_session_allowed_uses": dict(canonical.get("broker_session_allowed_uses") or {}),
        "broker_session_authority_blockers": blockers,
        "callback_ownership_attribution": canonical.get("callback_ownership_attribution"),
        "broker_session_submit_alignment": canonical.get("broker_session_submit_alignment") or "UNKNOWN",
    }


def _scoped_backend_source_fault_counts(
    *,
    readiness: dict[str, Any],
    strategy_id: str | None,
    required_instruments: list[str],
) -> dict[str, Any]:
    global_counts = {
        "market_data_stale_count": int(readiness.get("market_data_stale_count") or 0),
        "bar_authority_unavailable_count": int(readiness.get("bar_authority_unavailable_count") or 0),
        "blocking_fault_count": int(readiness.get("blocking_fault_count") or 0),
    }
    lane_rows = list(readiness.get("lane_eligibility_rows") or readiness.get("lane_status_rows") or [])
    if not lane_rows:
        return {
            **global_counts,
            "readiness_scope": "global_fallback",
            "relevant_lane_ids": [],
        }

    relevant_rows = [
        row
        for row in lane_rows
        if _readiness_row_applies_to_strategy(
            row=dict(row),
            strategy_id=strategy_id,
            required_instruments=required_instruments,
        )
    ]
    return {
        "market_data_stale_count": sum(1 for row in relevant_rows if _readiness_row_market_data_stale(dict(row))),
        "bar_authority_unavailable_count": sum(1 for row in relevant_rows if _readiness_row_bar_authority_unavailable(dict(row))),
        "blocking_fault_count": sum(1 for row in relevant_rows if _readiness_row_blocking_fault(dict(row))),
        "readiness_scope": "instrument_scoped",
        "required_instruments": list(required_instruments),
        "relevant_lane_ids": [str(row.get("lane_id") or row.get("strategy_id") or "").strip() for row in relevant_rows if str(row.get("lane_id") or row.get("strategy_id") or "").strip()],
        "global_market_data_stale_count": global_counts["market_data_stale_count"],
        "global_bar_authority_unavailable_count": global_counts["bar_authority_unavailable_count"],
        "global_blocking_fault_count": global_counts["blocking_fault_count"],
    }


def _readiness_row_applies_to_strategy(*, row: dict[str, Any], strategy_id: str | None, required_instruments: list[str]) -> bool:
    row_strategy_id = str(row.get("lane_id") or row.get("strategy_id") or "").strip()
    if row_strategy_id and strategy_id and row_strategy_id == str(strategy_id).strip():
        return True
    row_instruments = _coerce_instrument_list(row.get("symbol"))
    row_instruments.extend(_coerce_instrument_list(row.get("instrument")))
    row_instruments.extend(_coerce_instrument_list(row.get("affected_symbols")))
    row_instruments.extend(_coerce_instrument_list(row.get("affected_instruments")))
    return bool(set(row_instruments).intersection(required_instruments))


def _readiness_row_market_data_stale(row: dict[str, Any]) -> bool:
    return bool(row.get("market_data_stale")) or _readiness_row_has_any_token(
        row,
        {
            "MARKET_DATA_STALE",
            "NO_NEW_COMPLETED_BAR",
            "NO_COMPLETED_BAR",
        },
    )


def _readiness_row_bar_authority_unavailable(row: dict[str, Any]) -> bool:
    return bool(row.get("bar_authority_unavailable")) or _readiness_row_has_any_token(
        row,
        {
            "BAR_AUTHORITY_UNAVAILABLE",
            "NO_BAR_AUTHORITY",
        },
    )


def _readiness_row_blocking_fault(row: dict[str, Any]) -> bool:
    return bool(row.get("blocking_fault") or row.get("faulted") or row.get("blocking_fault_active"))


def _readiness_row_has_any_token(row: dict[str, Any], tokens: set[str]) -> bool:
    for key in (
        "bar_state",
        "state",
        "status",
        "reason",
        "reject_reason",
        "no_trade_reason",
        "dominant_reason",
        "no_trade_dominant_reason",
        "bar_authority_reason",
    ):
        value = row.get(key)
        if isinstance(value, list):
            values = value
        else:
            values = [value]
        for item in values:
            if str(item or "").strip().upper() in tokens:
                return True
    for key in ("block_reasons", "reject_reasons", "reasons"):
        for item in list(row.get(key) or []):
            if str(item or "").strip().upper() in tokens:
                return True
    return False


def _backend_readiness_artifacts_newer_than(*, repo_root: Path, generated_at: datetime | None) -> bool:
    if generated_at is None:
        return False
    for relative_path in (
        _DEFAULT_CANONICAL_READINESS_PATH,
        _DEFAULT_PAPER_READINESS_SNAPSHOT_PATH,
        _DEFAULT_STARTUP_CONTROL_PLANE_SNAPSHOT_PATH,
        _DEFAULT_SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH,
        _DEFAULT_TEMP_PAPER_INTEGRITY_SNAPSHOT_PATH,
    ):
        artifact_payload = _load_json(repo_root / relative_path)
        artifact_generated_at = _parse_datetime(artifact_payload.get("generated_at"))
        if artifact_generated_at is not None and artifact_generated_at > generated_at:
            return True
    return False


def _artifact_status(path: Path, payload: dict[str, Any], *, freshness_window_seconds: float, required: bool = True) -> dict[str, Any]:
    generated_at = _parse_datetime(payload.get("generated_at"))
    age_seconds = None if generated_at is None else max(0.0, (datetime.now(timezone.utc) - generated_at).total_seconds())
    present = bool(payload)
    fresh = bool(present and generated_at is not None and age_seconds is not None and age_seconds <= freshness_window_seconds)
    return {
        "label": path.stem,
        "path": str(path),
        "required": required,
        "present": present,
        "generated_at": generated_at.isoformat() if generated_at is not None else None,
        "age_seconds": age_seconds,
        "fresh": fresh,
    }


def _backend_source_readiness_detail(
    *,
    live_ready: bool,
    block_reasons: list[str],
    artifacts: dict[str, dict[str, Any]],
    freshness_window_seconds: float,
    readiness: dict[str, Any],
    startup: dict[str, Any],
    supervised: dict[str, Any],
    temp_integrity: dict[str, Any],
    source_faults: dict[str, Any],
    canonical: dict[str, Any] | None = None,
    canonical_authoritative: bool = False,
    runtime_running: bool = False,
    paper_runtime_ready: bool = False,
    paper_trade_allowed: bool = False,
    startup_ready: bool = False,
    supervised_usable: bool = False,
) -> str:
    readiness_status = artifacts["paper_readiness"]
    startup_status = artifacts["startup_control_plane"]
    supervised_status = artifacts["supervised_paper_operability"]
    temp_status = artifacts["temporary_paper_runtime_integrity"]
    return (
        f"backend/source readiness {'ready' if live_ready else 'not live-ready'} "
        f"from {'canonical_track_b_runtime_readiness' if canonical_authoritative else 'operator_dashboard_readiness_artifacts'}; "
        f"block_reasons={block_reasons}; "
        f"canonical_readiness={(canonical or {}).get('canonical_readiness') or (canonical or {}).get('state')} "
        f"canonical_authoritative={canonical_authoritative}; "
        f"freshness_window_seconds={freshness_window_seconds}; "
        f"paper_readiness_generated_at={readiness_status.get('generated_at')} "
        f"paper_readiness_age_seconds={_round_age(readiness_status.get('age_seconds'))} "
        f"paper_readiness_fresh={readiness_status.get('fresh')}; "
        f"runtime_running={runtime_running} "
        f"paper_runtime_ready={paper_runtime_ready} "
        f"paper_trade_allowed={paper_trade_allowed} "
        f"market_data_stale_count={int(source_faults.get('market_data_stale_count') or 0)} "
        f"bar_authority_unavailable_count={int(source_faults.get('bar_authority_unavailable_count') or 0)} "
        f"blocking_fault_count={int(source_faults.get('blocking_fault_count') or 0)} "
        f"readiness_scope={source_faults.get('readiness_scope')} "
        f"required_instruments={list(source_faults.get('required_instruments') or [])} "
        f"global_market_data_stale_count={int(readiness.get('market_data_stale_count') or 0)} "
        f"global_bar_authority_unavailable_count={int(readiness.get('bar_authority_unavailable_count') or 0)} "
        f"global_blocking_fault_count={int(readiness.get('blocking_fault_count') or 0)}; "
        f"startup_generated_at={startup_status.get('generated_at')} "
        f"startup_age_seconds={_round_age(startup_status.get('age_seconds'))} "
        f"startup_ready={startup_ready}; "
        f"supervised_generated_at={supervised_status.get('generated_at')} "
        f"supervised_age_seconds={_round_age(supervised_status.get('age_seconds'))} "
        f"supervised_usable={supervised_usable}; "
        f"temp_integrity_present={temp_status.get('present')} "
        f"temp_integrity_fresh={temp_status.get('fresh')} "
        f"temp_paper_blocked={bool(temp_integrity.get('temp_paper_blocked'))}"
    )


def _round_age(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 3)


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


def _synthetic_configured_inventory_row(
    *,
    configured_row: dict[str, Any],
    bridge_adapter: dict[str, Any],
) -> dict[str, Any]:
    lane_id = str(configured_row.get("lane_id") or "").strip()
    instrument = str(configured_row.get("symbol") or "").strip().upper()
    return {
        "strategy_id": lane_id,
        "standalone_strategy_id": lane_id,
        "instrument": instrument,
        "strategy_family": str(configured_row.get("strategy_family") or configured_row.get("display_name") or lane_id),
        "current_app_runtime_status": "ACTIVE_RUNTIME_READY",
        "current_position_state": "FLAT",
        "current_quantity": 0.0,
        "current_signal_state": "NO_ACTION",
        "entry_exit_capability": "ENTRY_AND_EXIT_WHEN_FLAT",
        "current_order_destination": str(bridge_adapter.get("current_order_destination") or ""),
        "can_emit_standardized_order_intent_now": True,
        "blockers_to_ibkr_paper_routing": [],
        "entries_enabled": True,
        "eligible_now": False,
        "last_signal_family": None,
        "last_signal_timestamp": None,
        "last_fill_timestamp": None,
        "audit_verdict": "CONFIGURED_CANARY_ROUTE_READY",
        "monitor_submit_allowed": True,
        "bridge_adapter_ready": True,
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


def _overall_governance_classification(
    *,
    strategy_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    phase1_reconciliation_gate: dict[str, Any],
) -> str:
    if not strategy_rows:
        return "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    if not bool(phase1_reconciliation_gate.get("ready")):
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
    phase1_reconciliation_gate: dict[str, Any],
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
        "legacy_monitor_authority": "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY",
        "phase1_broker_reconciliation_gate": phase1_reconciliation_gate,
        "strategies": strategy_rows,
        "summary": {
            "strategy_count": len(strategy_rows),
            "status_counts": _count_by_key(strategy_rows, "strategy_status"),
            "routing_mode_counts": _count_by_key(strategy_rows, "current_routing_mode"),
            "submit_capable_count": len([row for row in strategy_rows if row.get("submit_allowed")]),
            "broker_session_submit_alignment_counts": _count_by_key(
                strategy_rows,
                "broker_session_submit_alignment",
            ),
        },
    }
    return payload


def _build_probation_dashboard(
    *,
    now: str,
    classification: str,
    strategy_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    phase1_reconciliation_gate: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at": now,
        "classification": classification,
        "routing_policy_classification": _routing_policy_classification(strategy_rows),
        "paper_monitor_health": monitor_status.get("health_classification"),
        "paper_monitor_stale": monitor_status.get("stale"),
        "legacy_monitor_authority": "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY",
        "phase1_broker_reconciliation_gate": phase1_reconciliation_gate,
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
