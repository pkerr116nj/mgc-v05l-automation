"""Port one non-ATP GC/MGC lane from dry-run-only into submit-capable IBKR paper routing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ibkr_paper_strategy_bridge import IbkrPaperStrategyBridgeConfig, run_ibkr_paper_strategy_bridge
from .ibkr_paper_strategy_governance import load_paper_strategy_governance_status
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    lane_submit_bridge_adapter,
    run_ibkr_paper_strategy_porting,
    write_ibkr_paper_strategy_porting_artifacts,
)

_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_lane_submit_port"
_DEFAULT_PORTING_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_porting"
_REPORT_JSON = "ibkr_lane_submit_port_report.json"
_REPORT_MD = "ibkr_lane_submit_port_report.md"
_AUDIT_JSONL = "ibkr_lane_submit_port_audit.jsonl"
_PREFERRED_LANES = (
    "gc_1x_all_lanes__asia_early_long",
    "gc_1x_all_lanes__asia_early_short",
    "gc_1x_all_lanes__us_early_short",
    "gc_1x_all_lanes__us_midday_short",
    "gc_1x_asia_london_participation__asia_london_long_v5",
    "gc_1x_asia_london_participation__asia_london_short_v2",
    "mgc_1x_asia_london_participation__asia_london_long_v5",
    "mgc_1x_asia_london_participation__asia_london_short_v2",
)
_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_ACCOUNT = "DUM882026"


@dataclass(frozen=True)
class IbkrLaneSubmitPortConfig:
    repo_root: Path
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    porting_output_dir: Path = _DEFAULT_PORTING_OUTPUT_DIR
    submit: bool = True
    client_id: int = 9241
    timeout_seconds: float = 15.0
    strategy_id: str | None = None


@dataclass(frozen=True)
class IbkrLaneSubmitPortArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]


def run_ibkr_lane_submit_port(*, config: IbkrLaneSubmitPortConfig) -> IbkrLaneSubmitPortArtifacts:
    audit_events: list[dict[str, Any]] = []
    porting_config = IbkrPaperStrategyPortingConfig(repo_root=config.repo_root, output_dir=config.porting_output_dir)
    porting_artifacts = run_ibkr_paper_strategy_porting(config=porting_config)
    write_ibkr_paper_strategy_porting_artifacts(config=porting_config, artifacts=porting_artifacts)
    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    selected_lane = _select_lane(report=porting_artifacts.report, repo_root=config.repo_root, strategy_id=config.strategy_id)
    if not selected_lane:
        report = {
            "classification": "PAPER_LANE_PREFLIGHT_BLOCKED",
            "generated_at": _utc_now(),
            "detail": "No eligible non-ATP GC/MGC lane could be selected for submit-capable porting.",
            "paper_strategy_monitor_status": monitor_status,
            "selected_lane": {},
        }
        return IbkrLaneSubmitPortArtifacts(classification="PAPER_LANE_PREFLIGHT_BLOCKED", report=report, audit_events=audit_events)

    strategy_id = str(selected_lane.get("strategy_id") or "")
    governance_status = load_paper_strategy_governance_status(repo_root=config.repo_root, strategy_id=strategy_id)
    inventory_row = _find_row(porting_artifacts.report.get("inventory_rows"), strategy_id)
    intent_row = _find_row(porting_artifacts.report.get("intent_rows"), strategy_id)
    adapter = lane_submit_bridge_adapter(lane_id=strategy_id)
    checks = _build_checks(
        monitor_status=monitor_status,
        governance_status=governance_status,
        inventory_row=inventory_row,
        intent_row=intent_row,
        adapter=adapter,
    )
    _record(audit_events, "lane_selected", f"Selected {strategy_id} as the first non-ATP submit-capable lane candidate.", {"checks": checks})

    action = str(intent_row.get("action") or "NO_ACTION").upper()
    classification = "PAPER_LANE_PREFLIGHT_BLOCKED"
    delegated_result: dict[str, Any] | None = None
    detail = "Selected lane did not pass submit-capable preflight."
    if not any(check["blocking"] and not check["passed"] for check in checks):
        if action in {"NO_ACTION", "HOLD"}:
            classification = "PAPER_LANE_SUBMIT_READY_NO_ACTION"
            detail = "Selected lane is now submit-capable through the shared IBKR paper bridge path, but the live intent remains NO_ACTION so no order was submitted."
        elif action in {"BUY", "SELL", "EXIT"} and config.submit:
            bridge_config = _bridge_config_for_lane(
                config=config,
                strategy_id=strategy_id,
                inventory_row=inventory_row,
                intent_row=intent_row,
                adapter=adapter or {},
            )
            bridge_artifacts = run_ibkr_paper_strategy_bridge(config=bridge_config)
            delegated_result = bridge_artifacts.report
            classification = _map_bridge_classification(str(bridge_artifacts.classification or ""))
            detail = str(bridge_artifacts.report.get("detail") or f"Shared bridge returned {bridge_artifacts.classification}.")
            _record(audit_events, "bridge_executed", "Executed one shared-bridge pass for the selected lane.", {"bridge_classification": bridge_artifacts.classification})
        else:
            classification = "PAPER_LANE_PREFLIGHT_BLOCKED"
            detail = "Selected lane emitted an actionable intent but submit was disabled for this lane-port run."

    report = {
        "classification": classification,
        "generated_at": _utc_now(),
        "selected_lane": selected_lane,
        "selected_inventory_row": inventory_row,
        "selected_intent_row": intent_row,
        "selected_lane_governance_status": governance_status,
        "paper_strategy_monitor_status": monitor_status,
        "bridge_adapter": adapter,
        "preflight_checks": checks,
        "delegated_result": delegated_result,
        "detail": detail,
    }
    return IbkrLaneSubmitPortArtifacts(classification=classification, report=report, audit_events=audit_events)


def write_ibkr_lane_submit_port_artifacts(*, config: IbkrLaneSubmitPortConfig, artifacts: IbkrLaneSubmitPortArtifacts) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / _REPORT_JSON).write_text(json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / _REPORT_MD).write_text(render_ibkr_lane_submit_port_markdown(artifacts.report) + "\n", encoding="utf-8")
    with (output_dir / _AUDIT_JSONL).open("w", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def render_ibkr_lane_submit_port_markdown(report: dict[str, Any]) -> str:
    selected_lane = dict(report.get("selected_lane") or {})
    governance = dict(report.get("selected_lane_governance_status") or {})
    monitor = dict(report.get("paper_strategy_monitor_status") or {})
    adapter = dict(report.get("bridge_adapter") or {})
    intent = dict(report.get("selected_intent_row") or {})
    lines = [
        "# IBKR Lane Submit Port",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- selected lane: `{selected_lane.get('strategy_id')}`",
        f"- source instrument: `{selected_lane.get('instrument')}`",
        f"- governance status: `{dict(governance.get('selected_strategy') or {}).get('strategy_status')}`",
        f"- monitor health: `{monitor.get('health_classification')}`",
        f"- monitor stale: `{monitor.get('stale')}`",
        f"- open orders: `{monitor.get('open_order_count')}`",
        f"- intent action: `{intent.get('action')}`",
        f"- bridge submit capable: `{bool(adapter)}`",
        f"- bridge execution target: `{dict(adapter.get('bridge_execution_target') or {}).get('symbol')}` / `{dict(adapter.get('bridge_execution_target') or {}).get('contract_month')}`",
        f"- detail: `{report.get('detail')}`",
    ]
    return "\n".join(lines)


def _select_lane(*, report: dict[str, Any], repo_root: Path, strategy_id: str | None = None) -> dict[str, Any]:
    rows = list(report.get("inventory_rows") or [])
    governance_rows = {
        str(row.get("strategy_id") or ""): row for row in list(_load_governance_rows(repo_root) or [])
    }
    requested = str(strategy_id or "").strip()
    search_order = (requested,) if requested else _PREFERRED_LANES
    for preferred in search_order:
        row = next((candidate for candidate in rows if str(candidate.get("strategy_id") or "") == preferred), None)
        if row is None:
            continue
        gov = governance_rows.get(preferred, {})
        if not gov:
            continue
        if str(row.get("instrument") or "") not in {"GC", "MGC"}:
            continue
        if str(gov.get("strategy_status") or "WATCHLIST") in {"PAUSED", "DISABLED", "KILL_CANDIDATE"}:
            continue
        return {
            "strategy_id": row.get("strategy_id"),
            "instrument": row.get("instrument"),
            "current_position_state": row.get("current_position_state"),
            "current_order_destination": row.get("current_order_destination"),
            "governance_status": gov.get("strategy_status"),
        }
    return {}


def _load_governance_rows(repo_root: Any) -> list[dict[str, Any]]:
    path = Path(repo_root or ".") / "var" / "per_strategy_paper_status.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return list(payload.get("strategies") or [])


def _find_row(rows: Any, strategy_id: str) -> dict[str, Any]:
    for row in list(rows or []):
        if str(row.get("strategy_id") or "") == strategy_id:
            return dict(row)
    return {}


def _build_checks(
    *,
    monitor_status: dict[str, Any],
    governance_status: dict[str, Any],
    inventory_row: dict[str, Any],
    intent_row: dict[str, Any],
    adapter: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    governance_row = dict(governance_status.get("selected_strategy") or {})
    action = str(intent_row.get("action") or "").upper()
    requires_live_submit_gate = action in {"BUY", "SELL", "EXIT"}
    return [
        _check("paper_environment_lock", True, True, "Lane-port pass remains hard-locked to PAPER / 127.0.0.1 / 7497 / DUM882026."),
        _check("selected_lane_present", bool(inventory_row), True, "A selected non-ATP GC/MGC lane must be present in the live inventory."),
        _check("bridge_adapter_present", adapter is not None, True, "Selected lane requires an explicit shared-bridge adapter."),
        _check("current_order_destination_submit_capable", str(inventory_row.get("current_order_destination") or "") == "ibkr_paper_bridge_submit_capable", True, "Selected lane must now point at the shared IBKR paper bridge path."),
        _check("monitor_fresh_and_healthy", bool(monitor_status.get("monitor_running")) and not bool(monitor_status.get("stale")) and str(monitor_status.get("health_classification") or "").upper() == "HEALTHY", True, "Monitor must be running, fresh, and HEALTHY."),
        _check("governance_row_present", bool(governance_row), True, "Selected lane requires a governance row."),
        _check("governance_status_allowed", str(governance_row.get("strategy_status") or "").upper() not in {"PAUSED", "DISABLED", "KILL_CANDIDATE"}, True, "Governance status must remain submit-eligible."),
        _check(
            "governance_submit_allowed",
            (not requires_live_submit_gate) or bool(governance_status.get("submit_allowed")),
            True,
            "Governance submit gate must remain open before any real BUY / SELL / EXIT order can be sent for the selected lane.",
        ),
        _check("route_blockers_empty", len(list(intent_row.get("route_blockers") or [])) == 0, True, "Selected lane must have no IBKR routing blockers."),
        _check("no_conflicting_open_orders", int(monitor_status.get("open_order_count") or 0) == 0, True, "No conflicting broker open order is allowed."),
    ]


def _bridge_config_for_lane(
    *,
    config: IbkrLaneSubmitPortConfig,
    strategy_id: str,
    inventory_row: dict[str, Any],
    intent_row: dict[str, Any],
    adapter: dict[str, Any],
) -> IbkrPaperStrategyBridgeConfig:
    bridge_target = dict(adapter.get("bridge_execution_target") or {})
    action = str(intent_row.get("action") or "").upper()
    if action == "EXIT":
        bridge_action = "SELL"
        limit_price_model = "DELAYED_BID_MINUS_1T_MARKETABLE_SELL"
        quantity = float(inventory_row.get("current_quantity") or 0.0)
    elif action == "BUY":
        bridge_action = "BUY"
        limit_price_model = "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY"
        quantity = float(intent_row.get("quantity") or 1.0)
    else:
        bridge_action = action
        limit_price_model = str(intent_row.get("limit_price_model") or "")
        quantity = float(intent_row.get("quantity") or 0.0)
    return IbkrPaperStrategyBridgeConfig(
        repo_root=config.repo_root,
        mode=_EXPECTED_MODE,
        host=_EXPECTED_HOST,
        port=_EXPECTED_PORT,
        client_id=config.client_id,
        account_id=_EXPECTED_ACCOUNT,
        strategy_id=strategy_id,
        symbol=str(bridge_target.get("symbol") or "MGC"),
        contract_month=str(bridge_target.get("contract_month") or "202606"),
        action=bridge_action,
        quantity=quantity,
        order_type="LMT",
        limit_price_model=limit_price_model,
        time_in_force="DAY",
        reason=str(intent_row.get("reason") or "IBKR_LANE_SUBMIT_PORT"),
        risk_tags=("IBKR_LANE_SUBMIT_PORT", str(adapter.get("bridge_proxy_mode") or "")),
        paper_only=True,
        submit=True,
        timeout_seconds=config.timeout_seconds,
        caller_path="manual_strategy_bridge_cli",
        output_dir=config.output_dir / "delegated_bridge",
    )


def _map_bridge_classification(classification: str) -> str:
    normalized = str(classification or "").upper()
    if normalized == "PAPER_STRATEGY_ORDER_FILLED":
        return "PAPER_LANE_ORDER_FILLED"
    if normalized == "PAPER_STRATEGY_ORDER_WORKING":
        return "PAPER_LANE_ORDER_WORKING"
    if normalized == "PAPER_STRATEGY_ORDER_REJECTED":
        return "PAPER_LANE_ORDER_REJECTED"
    if normalized == "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW":
        return "PAPER_LANE_RECONCILIATION_FAILED"
    return "PAPER_LANE_PREFLIGHT_BLOCKED"


def _check(name: str, passed: bool, blocking: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "blocking": bool(blocking), "detail": detail}


def _record(audit_events: list[dict[str, Any]], event_type: str, detail: str, extra: dict[str, Any] | None = None) -> None:
    audit_events.append({"event_type": event_type, "observed_at": _utc_now(), "detail": detail, **dict(extra or {})})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
