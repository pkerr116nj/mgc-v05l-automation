from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..execution.ibkr_lane_submit_port import (
    IbkrLaneSubmitPortConfig,
    run_ibkr_lane_submit_port,
    write_ibkr_lane_submit_port_artifacts,
)
from ..execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)
from ..execution.ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from ..execution.ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    run_ibkr_paper_strategy_porting,
    write_ibkr_paper_strategy_porting_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_mnq_nq_lane_submit_port"
PORTING_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting"
SCOPE_SUPPORT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_mnq_nq_scope_support"
GOVERNANCE_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_governance"
REPORT_MD = "ibkr_mnq_nq_lane_submit_port_report.md"
REPORT_JSON = "ibkr_mnq_nq_lane_submit_port_report.json"
AUDIT_JSONL = "ibkr_mnq_nq_lane_submit_port_audit.jsonl"
QUOTE_DIAGNOSTIC_JSON = SCOPE_SUPPORT_DIR / "ibkr_mnq_nq_quote_diagnostic_report.json"
INDEX_EXPOSURE_JSON = SCOPE_SUPPORT_DIR / "paper_index_exposure_state.json"
PREFERRED_LANE_ORDER = (
    "mnq_1x_ny_early_core__us_early_long",
    "mnq_1x_ny_early_core__us_midday_long",
    "mnq_1x_ny_early_core__us_late_long",
    "mnq_1x_asia_london_participation__asia_london_long_v5",
    "mnq_1x_asia_london_participation__asia_london_long_v6",
    "mnq_1x_asia_london_participation__asia_london_short_v2",
    "nq_1x_ny_early_core__us_early_long",
    "nq_1x_ny_early_core__us_midday_long",
    "nq_1x_ny_early_core__us_late_long",
    "nq_1x_asia_london_participation__asia_london_long_v5",
    "nq_1x_asia_london_participation__asia_london_long_v6",
    "nq_1x_asia_london_participation__asia_london_short_v2",
    "mnq_1x_ny_early_core__us_early_short_breakdown",
    "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
    "mnq_1x_ny_early_core__us_late_short_reclaim_fail",
    "mnq_1x_ny_early_core__us_midday_short_breakdown",
    "nq_1x_ny_early_core__us_early_short_breakdown",
    "nq_1x_ny_early_core__us_early_short_reclaim_fail",
    "nq_1x_ny_early_core__us_late_short_reclaim_fail",
    "nq_1x_ny_early_core__us_midday_short_breakdown",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process MNQ/NQ lanes into live submit-capable IBKR paper status, lane by lane.")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    audit_events: list[dict[str, Any]] = []
    monitor_status = load_paper_strategy_monitor_status(repo_root=REPO_ROOT)
    governance_config = IbkrPaperStrategyGovernanceConfig(
        repo_root=REPO_ROOT,
        output_dir=GOVERNANCE_OUTPUT_DIR.relative_to(REPO_ROOT),
    )
    governance_artifacts = run_ibkr_paper_strategy_governance(config=governance_config)
    write_ibkr_paper_strategy_governance_artifacts(config=governance_config, artifacts=governance_artifacts)

    porting_config = IbkrPaperStrategyPortingConfig(
        repo_root=REPO_ROOT,
        output_dir=PORTING_OUTPUT_DIR.relative_to(REPO_ROOT),
    )
    porting_artifacts = run_ibkr_paper_strategy_porting(config=porting_config)
    write_ibkr_paper_strategy_porting_artifacts(config=porting_config, artifacts=porting_artifacts)

    governance_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(governance_artifacts.status_payload.get("strategies") or [])
    }
    inventory_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(porting_artifacts.inventory_rows or [])
        if str(row.get("instrument") or "").upper() in {"MNQ", "NQ"}
    }
    intent_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(porting_artifacts.intent_rows or [])
        if str(row.get("strategy_id") or "") in inventory_rows
    }
    quote_diag = _load_json(QUOTE_DIAGNOSTIC_JSON)
    index_exposure = _load_json(INDEX_EXPOSURE_JSON)

    lane_results: list[dict[str, Any]] = []
    first_order_event: dict[str, Any] | None = None
    overall_classification = "IBKR_MNQ_NQ_LANE_PORT_READY"
    quote_classification = str(quote_diag.get("classification") or "IBKR_MNQ_NQ_QUOTES_UNAVAILABLE_UNKNOWN")

    for strategy_id in _ordered_strategy_ids(inventory_rows):
        inventory_row = inventory_rows[strategy_id]
        intent_row = intent_rows.get(strategy_id, {})
        governance_row = governance_rows.get(strategy_id, {})
        action = str(intent_row.get("action") or "UNKNOWN").upper()
        route_blockers = list(intent_row.get("route_blockers") or inventory_row.get("blockers_to_ibkr_paper_routing") or [])
        monitor_gate = bool(monitor_status.get("monitor_running")) and not bool(monitor_status.get("stale")) and str(monitor_status.get("health_classification") or "").upper() == "HEALTHY"
        governance_gate = str(governance_row.get("strategy_status") or "").upper() not in {"PAUSED", "DISABLED"} and bool(governance_row.get("submit_allowed", True))
        quote_gate = quote_classification in {"IBKR_MNQ_NQ_QUOTES_READY", "IBKR_MNQ_NQ_QUOTES_DELAYED_ONLY"}
        exposure_gate = (
            float(index_exposure.get("broker_net_mnq") or 0.0) == 0.0
            and float(index_exposure.get("strategy_attributed_mnq") or 0.0) == 0.0
            and float(index_exposure.get("broker_minus_ledger_difference") or 0.0) == 0.0
            and float(index_exposure.get("orphan_exposure") or 0.0) == 0.0
        )
        bridge_gate = (
            str(inventory_row.get("current_order_destination") or "") == "ibkr_paper_bridge_submit_capable"
            and bool(inventory_row.get("bridge_adapter_ready"))
            and not route_blockers
            and monitor_gate
            and governance_gate
            and quote_gate
            and exposure_gate
        )
        result = {
            "strategy_id": strategy_id,
            "instrument": inventory_row.get("instrument"),
            "current_order_destination": inventory_row.get("current_order_destination"),
            "bridge_submit_capable": bool(inventory_row.get("bridge_adapter_ready")),
            "governance_status": governance_row.get("strategy_status"),
            "submit_allowed": governance_row.get("submit_allowed"),
            "monitor_gate": "PASS" if monitor_gate else "FAIL",
            "governance_gate": "PASS" if governance_gate else "FAIL",
            "exposure_gate": "PASS" if exposure_gate else "FAIL",
            "quote_gate": "PASS" if quote_gate else "FAIL",
            "quote_source_label": _quote_source_label_for_lane(strategy_id=strategy_id, quote_diag=quote_diag),
            "action": action,
            "route_blockers": route_blockers,
            "classification": "PAPER_LANE_PREFLIGHT_BLOCKED",
            "detail": "",
        }
        if action in {"NO_ACTION", "HOLD"} and bridge_gate:
            result["classification"] = "PAPER_LANE_SUBMIT_READY_NO_ACTION"
            result["detail"] = "Lane is submit-capable through the shared IBKR paper bridge and current live intent is NO_ACTION."
        elif action in {"BUY", "SELL", "EXIT"} and bridge_gate and first_order_event is None:
            lane_output_dir = OUTPUT_DIR / strategy_id
            lane_output_dir.mkdir(parents=True, exist_ok=True)
            lane_artifacts = run_ibkr_lane_submit_port(
                config=IbkrLaneSubmitPortConfig(
                    repo_root=REPO_ROOT,
                    output_dir=lane_output_dir.relative_to(REPO_ROOT),
                    porting_output_dir=PORTING_OUTPUT_DIR.relative_to(REPO_ROOT),
                    submit=False,
                    strategy_id=strategy_id,
                )
            )
            write_ibkr_lane_submit_port_artifacts(
                config=IbkrLaneSubmitPortConfig(
                    repo_root=REPO_ROOT,
                    output_dir=lane_output_dir.relative_to(REPO_ROOT),
                    porting_output_dir=PORTING_OUTPUT_DIR.relative_to(REPO_ROOT),
                    submit=False,
                    strategy_id=strategy_id,
                ),
                artifacts=lane_artifacts,
            )
            result["classification"] = lane_artifacts.classification
            result["detail"] = str(lane_artifacts.report.get("detail") or "")
            result["delegated_result"] = lane_artifacts.report.get("delegated_result")
            first_order_event = result
            overall_classification = result["classification"]
        else:
            if not bridge_gate:
                result["classification"] = "PAPER_LANE_PREFLIGHT_BLOCKED"
                result["detail"] = _blocked_detail(
                    monitor_gate=monitor_gate,
                    governance_gate=governance_gate,
                    exposure_gate=exposure_gate,
                    quote_gate=quote_gate,
                    route_blockers=route_blockers,
                )
            else:
                result["classification"] = "PAPER_LANE_PREFLIGHT_BLOCKED"
                result["detail"] = "Lane emitted an unrecognized action."

        lane_results.append(result)
        audit_events.append(
            {
                "event_type": "mnq_nq_lane_evaluated",
                "observed_at": _utc_now(),
                "strategy_id": strategy_id,
                "classification": result["classification"],
                "action": action,
                "quote_source_label": result["quote_source_label"],
                "detail": result["detail"],
            }
        )
        if first_order_event is not None:
            break

    report = {
        "classification": overall_classification,
        "generated_at": _utc_now(),
        "scope_classification": "IBKR_MNQ_NQ_SCOPE_READY",
        "quote_classification": quote_classification,
        "monitor_status": {
            "monitor_running": monitor_status.get("monitor_running"),
            "health_classification": monitor_status.get("health_classification"),
            "ibkr_connection_state": monitor_status.get("ibkr_connection_state"),
            "stale": monitor_status.get("stale"),
        },
        "index_exposure_state": index_exposure,
        "lane_count": len(inventory_rows),
        "processed_lane_count": len(lane_results),
        "lane_results": lane_results,
        "first_order_event": first_order_event,
        "detail": (
            "Processed MNQ/NQ lanes lane-by-lane and all currently emitted NO_ACTION, so no order was submitted."
            if first_order_event is None
            else "Stopped after the first actionable MNQ/NQ broker-path event."
        ),
    }
    _write_artifacts(report=report, audit_events=audit_events)
    print(json.dumps(report, indent=2, sort_keys=True))
    print()
    print(_render_markdown(report))
    return 0


def _ordered_strategy_ids(inventory_rows: dict[str, dict[str, Any]]) -> list[str]:
    ordered = [strategy_id for strategy_id in PREFERRED_LANE_ORDER if strategy_id in inventory_rows]
    remaining = sorted([strategy_id for strategy_id in inventory_rows if strategy_id not in ordered])
    return [*ordered, *remaining]


def _quote_source_label_for_lane(*, strategy_id: str, quote_diag: dict[str, Any]) -> str | None:
    symbol = "MNQ" if strategy_id.startswith("mnq_") or "__MNQ" in strategy_id else "NQ"
    comparison = dict(quote_diag.get("comparison") or {})
    row = dict(comparison.get(symbol.lower()) or {})
    return row.get("best_mode") or row.get("quote_source_label") or row.get("best_response")


def _blocked_detail(
    *,
    monitor_gate: bool,
    governance_gate: bool,
    exposure_gate: bool,
    quote_gate: bool,
    route_blockers: list[str],
) -> str:
    reasons: list[str] = []
    if not monitor_gate:
        reasons.append("monitor_gate_failed")
    if not governance_gate:
        reasons.append("governance_gate_failed")
    if not exposure_gate:
        reasons.append("index_exposure_gate_failed")
    if not quote_gate:
        reasons.append("quote_gate_failed")
    reasons.extend(route_blockers)
    return ", ".join(reasons) if reasons else "unknown_preflight_blocker"


def _write_artifacts(*, report: dict[str, Any], audit_events: list[dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / REPORT_JSON).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (OUTPUT_DIR / REPORT_MD).write_text(_render_markdown(report) + "\n", encoding="utf-8")
    with (OUTPUT_DIR / AUDIT_JSONL).open("w", encoding="utf-8") as handle:
        for row in audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# IBKR MNQ/NQ Lane Submit Port",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- scope classification: `{report.get('scope_classification')}`",
        f"- quote classification: `{report.get('quote_classification')}`",
        f"- processed lanes: `{report.get('processed_lane_count')}` / `{report.get('lane_count')}`",
        f"- detail: `{report.get('detail')}`",
        "",
        "## Monitor",
        "",
    ]
    monitor = dict(report.get("monitor_status") or {})
    lines.extend(
        [
            f"- running: `{monitor.get('monitor_running')}`",
            f"- health: `{monitor.get('health_classification')}`",
            f"- connection: `{monitor.get('ibkr_connection_state')}`",
            f"- stale: `{monitor.get('stale')}`",
            "",
            "## Lane Results",
            "",
        ]
    )
    for row in list(report.get("lane_results") or []):
        lines.append(
            f"- `{row.get('strategy_id')}` `{row.get('instrument')}` `{row.get('classification')}` action=`{row.get('action')}` quote=`{row.get('quote_source_label')}`"
        )
    return "\n".join(lines)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
