"""Normalize surfaced dashboard lanes into a single registry payload."""

from __future__ import annotations

from typing import Any

from .strategy_identity import build_standalone_strategy_identity


def build_dashboard_lane_registry(
    *,
    approved_quant_baselines: dict[str, Any],
    paper_approved_models: dict[str, Any],
    paper_non_approved_lanes: dict[str, Any],
) -> dict[str, Any]:
    approved_quant_section = _approved_quant_section(approved_quant_baselines)
    admitted_paper_section = _admitted_paper_section(paper_approved_models)
    canary_section = _canary_section(paper_non_approved_lanes)
    sections = [approved_quant_section, admitted_paper_section, canary_section]
    for section in sections:
        section["rows"] = _annotate_registry_rows(section.get("rows", []))
    all_rows = [
        row
        for section in sections
        for row in section.get("rows", [])
    ]
    return {
        "generated_at": (
            approved_quant_baselines.get("generated_at")
            or paper_approved_models.get("generated_at")
            or paper_non_approved_lanes.get("generated_at")
        ),
        "section_order": [section["key"] for section in sections],
        "sections": sections,
        "rows": all_rows,
        "total_rows": len(all_rows),
    }


def _approved_quant_section(payload: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    display_priority = 1
    for baseline_row in payload.get("rows", []):
        for row in _approved_quant_rows(baseline_row, display_priority=display_priority):
            rows.append(row)
            display_priority += 1
    normal_count = sum(1 for row in rows if row["probation_state"] == "normal")
    flagged_count = sum(1 for row in rows if row["probation_state"] in {"watch", "review", "suspend"})
    stable_count = sum(1 for row in rows if row["monitoring_summary"] == "stable_positive_post_cost")
    return {
        "key": "approved_quant",
        "title": "Approved Quant Baselines",
        "eyebrow": "Frozen Quant Baselines",
        "surface_group": "approved_quant",
        "display_priority": 10,
        "badge_label": f"{len(rows)} APPROVED QUANT LANES" if rows else "NO APPROVED QUANT LANES",
        "badge_level": "accent" if rows else "muted",
        "summary_line": payload.get("operator_summary_line") or payload.get("summary_line") or "No approved quant baseline snapshot loaded.",
        "summary_metrics": [
            {"label": "Visible", "value": str(len(rows))},
            {"label": "Normal", "value": str(normal_count)},
            {"label": "Flagged", "value": str(flagged_count)},
            {"label": "Stable Post-Cost", "value": str(stable_count)},
        ],
        "primary_link": {
            "label": "Snapshot JSON",
            "href": (payload.get("artifacts") or {}).get("snapshot"),
        },
        "secondary_link": {
            "label": "Current Status",
            "href": (
                (payload.get("artifacts") or {}).get("current_status_markdown")
                or (payload.get("artifacts") or {}).get("current_status_json")
            ),
        },
        "rows": rows,
    }


def _approved_quant_rows(row: dict[str, Any], *, display_priority: int) -> list[dict[str, Any]]:
    scope = row.get("approved_scope") or {}
    warnings = list(row.get("warning_flags") or [])
    unknown_session_warning = row.get("unknown_session_warning") or {}
    if unknown_session_warning.get("flag"):
        warnings.append(str(unknown_session_warning.get("label") or "unknown_session_labeling_watch"))
    if row.get("slice_weakness_flag"):
        warnings.append("slice_weakness_watch")
    warnings = sorted(dict.fromkeys(warnings))
    symbols = list(scope.get("symbols") or [])
    allowed_sessions = list(scope.get("allowed_sessions") or [])
    excluded_sessions = list(scope.get("excluded_sessions") or [])
    permanent_exclusions = list(scope.get("permanent_exclusions") or [])
    display_name = str(row.get("lane_name") or row.get("lane_id") or "-")
    probation_state = str(row.get("probation_status") or "unknown").lower()
    promotion_state = str(row.get("promotion_state") or row.get("baseline_status") or "unknown")
    monitoring_summary = str((row.get("post_cost_monitoring_read") or {}).get("label") or "-")
    rows: list[dict[str, Any]] = []
    for index, symbol in enumerate(symbols or [None]):
        rows.append(
            {
                "lane_id": str(row.get("lane_id") or ""),
                "lane_name": str(row.get("lane_name") or ""),
                "display_name": display_name,
                "instrument": str(symbol or ""),
                "family": str((scope.get("family") or row.get("lane_name") or row.get("lane_classification") or "approved_baseline_lane")),
                "classification": str(row.get("lane_classification") or "approved_baseline_lane"),
                "admission_state": "approved_baseline",
                "probation_state": probation_state,
                "promotion_state": promotion_state,
                "scope": {
                    "symbols": [symbol] if symbol else [],
                    "allowed_sessions": allowed_sessions,
                    "excluded_sessions": excluded_sessions,
                    "permanent_exclusions": permanent_exclusions,
                },
                "scope_summary": " / ".join(
                    [
                        str(symbol or "-"),
                        "/".join(allowed_sessions) or "-",
                    ]
                ),
                "active_exit": str(row.get("approved_exit_label") or "-"),
                "monitoring_summary": monitoring_summary,
                "warnings": warnings,
                "surface_group": "approved_quant",
                "display_priority": display_priority + index,
                "primary_badge": {
                    "label": probation_state.upper(),
                    "level": _probation_level(probation_state),
                },
                "card_metrics": [
                    {"label": "Instrument", "value": str(symbol or "-")},
                    {"label": "Probation", "value": probation_state.upper()},
                    {"label": "Promotion", "value": promotion_state},
                    {"label": "Post-Cost", "value": monitoring_summary},
                ],
                "summary_lines": [
                    f"Standalone strategy: {display_name} / {symbol or '-'}",
                    f"Allowed sessions: {'/'.join(allowed_sessions) or '-'} | Excluded: {'/'.join(excluded_sessions) or '-'}",
                    f"Symbol attribution: {', '.join(row.get('symbol_attribution_summary') or []) or 'No attribution summary yet.'}",
                    f"Session attribution: {', '.join(row.get('session_attribution_summary') or []) or 'No attribution summary yet.'}",
                ],
                "warning_summary": ", ".join(warnings) if warnings else "No active warning flags.",
                "artifacts": row.get("artifacts") or {},
            }
        )
    return rows


def _admitted_paper_section(payload: dict[str, Any]) -> dict[str, Any]:
    rows = [_admitted_paper_row(row, display_priority=index) for index, row in enumerate(payload.get("rows", []), start=1)]
    enabled_count = sum(1 for row in rows if row.get("admission_state") == "admitted_paper")
    halted_count = sum(1 for row in rows if row.get("warnings"))
    open_count = sum(1 for row in rows if row.get("open_position"))
    return {
        "key": "admitted_paper",
        "title": "Admitted Paper Lanes",
        "eyebrow": "Legacy Operator Lanes",
        "surface_group": "admitted_paper",
        "display_priority": 20,
        "badge_label": f"{len(rows)} ADMITTED PAPER LANES" if rows else "NO ADMITTED PAPER LANES",
        "badge_level": "accent" if rows else "muted",
        "summary_line": payload.get("scope_label") or "No admitted paper lanes are active.",
        "summary_metrics": [
            {"label": "Visible", "value": str(len(rows))},
            {"label": "Enabled", "value": str(enabled_count)},
            {"label": "Open", "value": str(open_count)},
            {"label": "Flagged", "value": str(halted_count)},
        ],
        "primary_link": {
            "label": "Approved Models JSON",
            "href": (payload.get("artifacts") or {}).get("approved_models"),
        },
        "secondary_link": {
            "label": "Lane Activity JSON",
            "href": "/api/operator-artifact/paper-lane-activity",
        },
        "rows": rows,
    }


def _admitted_paper_row(row: dict[str, Any], *, display_priority: int) -> dict[str, Any]:
    risk_state = str(row.get("risk_state") or "OK")
    warnings = []
    if risk_state != "OK":
        warnings.append(risk_state)
    if row.get("halt_reason"):
        warnings.append(str(row.get("halt_reason")))
    instrument = str(row.get("instrument") or "-")
    session_restriction = str(row.get("session_restriction") or "-")
    chain_state = str(row.get("chain_state") or "UNKNOWN")
    return {
        "lane_id": str(row.get("lane_id") or row.get("branch") or ""),
        "display_name": str(row.get("branch") or row.get("lane_id") or "-"),
        "family": str(row.get("source_family") or "-"),
        "classification": "admitted_paper_lane",
        "admission_state": "admitted_paper",
        "probation_state": None,
        "promotion_state": "legacy_admitted_paper",
        "scope": {
            "symbols": [instrument] if instrument and instrument != "-" else [],
            "allowed_sessions": [session_restriction] if session_restriction and session_restriction != "-" else [],
            "excluded_sessions": [],
            "permanent_exclusions": [],
        },
        "scope_summary": f"{instrument} • {session_restriction}",
        "active_exit": "runtime_managed_legacy_exit",
        "monitoring_summary": str(row.get("latest_activity_type") or "NO_ACTIVITY"),
        "warnings": warnings,
        "surface_group": "admitted_paper",
        "display_priority": display_priority,
        "open_position": bool(row.get("open_position")),
        "primary_badge": {
            "label": str(row.get("state") or "UNKNOWN"),
            "level": "accent" if row.get("enabled") else "muted",
        },
        "card_metrics": [
            {"label": "Family", "value": str(row.get("source_family") or "-")},
            {"label": "Side", "value": str(row.get("side") or "-")},
            {"label": "Activity", "value": str(row.get("latest_activity_type") or "NO_ACTIVITY")},
            {"label": "Chain", "value": chain_state},
        ],
        "summary_lines": [
            f"Session: {session_restriction} | Signals {row.get('signal_count', 0)} | Blocked {row.get('blocked_count', 0)}",
            f"Intents {row.get('intent_count', 0)} | Fills {row.get('fill_count', 0)} | Open {'YES' if row.get('open_position') else 'NO'}",
            f"Realized {row.get('realized_pnl', 'N/A')} | Unrealized {row.get('unrealized_pnl', 'N/A')}",
        ],
        "warning_summary": ", ".join(warnings) if warnings else "No active lane risk flags.",
        "artifacts": {},
    }


def _canary_section(payload: dict[str, Any]) -> dict[str, Any]:
    rows = [_canary_row(row, display_priority=index) for index, row in enumerate(payload.get("rows", []), start=1)]
    temporary_paper_count = sum(1 for row in rows if row.get("classification") == "temporary_paper_strategy")
    canary_count = sum(1 for row in rows if row.get("classification") == "paper_execution_canary")
    fired_count = sum(1 for row in rows if row.get("monitoring_summary") != "IDLE")
    completed_count = sum(1 for row in rows if "COMPLETE" in " ".join(row.get("summary_lines") or []))
    return {
        "key": "canary",
        "title": "Temporary Paper Strategies",
        "eyebrow": "Experimental / Paper Only / Non-Approved",
        "surface_group": "temporary_paper",
        "display_priority": 30,
        "badge_label": f"{len(rows)} TEMPORARY PAPER LANES" if rows else "NO TEMPORARY PAPER LANES",
        "badge_level": "warning" if rows else "muted",
        "summary_line": payload.get("note") or payload.get("scope_label") or "No temporary paper strategies are active.",
        "summary_metrics": [
            {"label": "Visible", "value": str(len(rows))},
            {"label": "Temp Paper", "value": str(temporary_paper_count)},
            {"label": "Canary", "value": str(canary_count)},
            {"label": "Fired", "value": str(fired_count)},
            {"label": "Complete", "value": str(completed_count)},
        ],
        "primary_link": {
            "label": "Snapshot JSON",
            "href": (payload.get("artifacts") or {}).get("snapshot"),
        },
        "secondary_link": {
            "label": "Operator Status",
            "href": (payload.get("artifacts") or {}).get("status"),
        },
        "rows": rows,
    }


def _canary_row(row: dict[str, Any], *, display_priority: int) -> dict[str, Any]:
    instrument = str(row.get("instrument") or "-")
    session_restriction = str(row.get("session_restriction") or "-")
    note = str(row.get("note") or "-")
    warnings = [str(row.get("risk_state"))] if row.get("risk_state") and str(row.get("risk_state")) != "OK" else []
    is_temporary_paper = bool(
        row.get("temporary_paper_strategy")
        or row.get("paper_strategy_class") == "temporary_paper_strategy"
        or row.get("experimental_status") == "experimental_canary"
    )
    return {
        "lane_id": str(row.get("lane_id") or ""),
        "display_name": str(row.get("display_name") or row.get("lane_id") or "-"),
        "family": str(row.get("source_family") or row.get("lane_mode") or "-"),
        "classification": (
            "temporary_paper_strategy"
            if is_temporary_paper
            else ("paper_execution_canary" if row.get("is_canary") else "paper_only_non_approved")
        ),
        "admission_state": "paper_only_non_approved",
        "probation_state": None,
        "promotion_state": "not_admitted",
        "scope": {
            "symbols": [instrument] if instrument and instrument != "-" else [],
            "allowed_sessions": [session_restriction] if session_restriction and session_restriction != "-" else [],
            "excluded_sessions": [],
            "permanent_exclusions": [],
        },
        "scope_summary": f"{instrument} • {session_restriction}",
        "active_exit": str(row.get("exit_state") or "NOT_STARTED"),
        "monitoring_summary": str(row.get("lifecycle_state") or "IDLE"),
        "warnings": warnings,
        "surface_group": "temporary_paper" if is_temporary_paper else "canary",
        "display_priority": display_priority,
        "primary_badge": {
            "label": "TEMP PAPER" if is_temporary_paper else ("CANARY" if row.get("is_canary") else "NON-APPROVED"),
            "level": "warning" if (is_temporary_paper or row.get("is_canary")) else "muted",
        },
        "card_metrics": [
            {"label": "Scope", "value": str(row.get("lane_mode") or row.get("scope_label") or "-")},
            {"label": "Lifecycle", "value": str(row.get("lifecycle_state") or "-")},
            {"label": "Fired", "value": "YES" if row.get("fired") else "NO"},
            {"label": "Exit", "value": str(row.get("exit_state") or "-")},
        ],
        "summary_lines": [
            f"Session: {session_restriction} | Position {row.get('position_side') or 'FLAT'} | Open {'YES' if row.get('open_position') else 'NO'}",
            f"Signals {row.get('signal_count', 0)} | Intents {row.get('intent_count', 0)} | Fills {row.get('fill_count', 0)}",
            f"Entry {row.get('entry_state') or '-'} | Exit {row.get('exit_state') or '-'} | Latest {row.get('latest_activity_timestamp') or '-'}",
        ],
        "warning_summary": ", ".join(warnings) if warnings else note,
        "artifacts": {},
    }


def _annotate_registry_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for row in rows:
        identity = build_standalone_strategy_identity(
            instrument=row.get("instrument") or (row.get("scope") or {}).get("symbols", [None])[0],
            lane_id=row.get("lane_id"),
            strategy_name=row.get("display_name"),
            source_family=row.get("family"),
            lane_name=row.get("lane_name"),
        )
        annotated.append(
            {
                **row,
                **identity,
            }
        )
    return annotated


def _probation_level(status: str) -> str:
    if status == "normal":
        return "accent"
    if status == "watch":
        return "warning"
    if status in {"review", "suspend"}:
        return "danger"
    return "muted"
