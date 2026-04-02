"""Authoritative operator-surface payload for the dashboard top fold."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .strategy_identity import build_standalone_strategy_identity

_OD_FILE = "src/mgc_v05l/app/operator_dashboard.py"
_OS_FILE = "src/mgc_v05l/app/operator_surface.py"
_AQ_FILE = "src/mgc_v05l/app/approved_quant_lanes/dashboard_payloads.py"


def build_operator_surface(
    *,
    generated_at: str,
    global_payload: dict[str, Any],
    auth_status: dict[str, Any],
    paper: dict[str, Any],
    approved_quant_baselines: dict[str, Any],
    market_context: dict[str, Any],
    treasury_curve: dict[str, Any],
    bootstrap_prerequisites: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bootstrap_prerequisites = dict(bootstrap_prerequisites or {})
    session_date = _safe_session_date(global_payload.get("current_session_date"))
    active_rows = _build_active_instrument_surface_rows(
        paper=paper,
        approved_quant_baselines=approved_quant_baselines,
        session_date=session_date,
    )
    evidence = _build_instrument_evidence_maps(paper=paper, active_rows=active_rows)
    instrument_block = _build_operator_metrics_by_instrument(
        active_rows=active_rows,
        evidence=evidence,
    )
    portfolio_block = _build_operator_metrics_portfolio(
        paper=paper,
        active_rows=active_rows,
        instrument_rows=instrument_block["rows"],
    )
    runtime_block = _build_runtime_readiness(
        global_payload=global_payload,
        auth_status=auth_status,
        paper=paper,
        market_context=market_context,
        treasury_curve=treasury_curve,
        bootstrap_prerequisites=bootstrap_prerequisites,
        active_rows=active_rows,
    )
    active_surface = _build_active_instrument_surface_block(active_rows=active_rows)
    current_active_positions = _build_current_active_positions(
        paper=paper,
        active_rows=active_rows,
    )
    secondary_context = _build_secondary_context(
        paper=paper,
        market_context=market_context,
        treasury_curve=treasury_curve,
    )
    rollup_integrity = _build_rollup_integrity(
        portfolio_metrics=portfolio_block,
        instrument_rows=instrument_block["rows"],
        active_rows=active_rows,
        active_surface=active_surface,
    )
    source_manifest = _build_source_manifest()

    surface = {
        "generated_at": generated_at,
        "contract_version": "operator_surface_v2",
        "runtime_readiness": runtime_block,
        "operator_metrics_portfolio": portfolio_block,
        "operator_metrics_by_instrument": instrument_block,
        "current_active_positions": current_active_positions,
        "active_instrument_surface": active_surface,
        "secondary_context": secondary_context,
        "rollup_integrity": rollup_integrity,
        "source_manifest": source_manifest,
    }
    surface["market_context"] = secondary_context

    # Compatibility aliases for the currently shipped dashboard JS while the page
    # is still reading the older names.
    surface["readiness"] = _legacy_readiness_alias(runtime_block)
    surface["daily_risk"] = _legacy_portfolio_alias(portfolio_block)
    surface["lane_universe"] = _legacy_universe_alias(active_surface)
    surface["lane_rows"] = active_rows
    surface["context"] = _legacy_context_alias(secondary_context)
    return surface


def _build_runtime_readiness(
    *,
    global_payload: dict[str, Any],
    auth_status: dict[str, Any],
    paper: dict[str, Any],
    market_context: dict[str, Any],
    treasury_curve: dict[str, Any],
    bootstrap_prerequisites: dict[str, Any] | None,
    active_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    bootstrap_prerequisites = dict(bootstrap_prerequisites or {})
    readiness = paper.get("readiness") or {}
    exceptions = paper.get("exceptions") or {}
    runtime_status = str(readiness.get("runtime_phase") or global_payload.get("paper_label") or "UNKNOWN")
    paper_enabled = bool(paper.get("running"))
    entries_enabled = bool(readiness.get("entries_enabled"))
    auth_readiness = bool(auth_status.get("runtime_ready"))
    market_data_readiness = str(global_payload.get("market_data_label") or "-")
    fault_state = str(global_payload.get("fault_state") or "CLEAR")
    runtime_recovery = paper.get("runtime_recovery") or {}
    runtime_recovery_state = str(runtime_recovery.get("status") or "NOT_APPLICABLE")
    runtime_recovery_message = str(runtime_recovery.get("operator_message") or runtime_recovery.get("detail") or "")
    runtime_recovery_attempts = int(runtime_recovery.get("restart_attempts_in_window") or 0)
    runtime_recovery_attempt_budget = int(runtime_recovery.get("max_auto_restarts_per_window") or 0)
    runtime_recovery_suppressed = bool(runtime_recovery.get("restart_suppressed"))
    blocking_faults = [
        {
            "code": row.get("code"),
            "severity": row.get("severity"),
            "summary": row.get("summary"),
            "owner": row.get("owning_model"),
        }
        for row in list(exceptions.get("exceptions") or [])
    ]
    degraded_informational_feeds = [
        label
        for label, item in _secondary_context_items(
            paper=paper,
            market_context=market_context,
            treasury_curve=treasury_curve,
        ).items()
        if not bool(item.get("available"))
    ]
    bootstrap_items = list(bootstrap_prerequisites.get("items") or [])
    bootstrap_issues = [str(item.get("label") or item.get("key") or "") for item in bootstrap_items if str(item.get("status") or "") != "ready"]
    active_lane_count = len(_unique_lane_ids(active_rows, enabled_only=True))
    active_instruments_count = len({str(row.get("instrument") or "") for row in active_rows if row.get("instrument")})
    payload = {
        "runtime_status": runtime_status,
        "paper_enabled": paper_enabled,
        "entries_enabled": entries_enabled,
        "auth_readiness": auth_readiness,
        "market_data_readiness": market_data_readiness,
        "blocking_faults": blocking_faults,
        "blocking_faults_active": bool(blocking_faults) or fault_state.upper() == "FAULTED",
        "degraded_informational_feeds": degraded_informational_feeds,
        "active_instruments_count": active_instruments_count,
        "active_lanes_count": active_lane_count,
        "status_line": (
            f"runtime={runtime_status} | paper={'ENABLED' if paper_enabled else 'DISABLED'} | "
            f"entries={'ENABLED' if entries_enabled else 'HALTED'} | "
            f"auth={'READY' if auth_readiness else 'NOT_READY'} | "
            f"market_data={market_data_readiness} | faults={len(blocking_faults)} | "
            f"runtime_recovery={runtime_recovery_state} | restart_budget={runtime_recovery_attempts}/{runtime_recovery_attempt_budget or '?'} | "
            f"bootstrap_issues={len(bootstrap_issues)}"
        ),
        "field_sources": {
            "runtime_status": _source(_OD_FILE, "_paper_readiness_payload", "paper.readiness.runtime_phase"),
            "paper_enabled": _source(_OD_FILE, "snapshot", "paper.running"),
            "entries_enabled": _source(_OD_FILE, "_paper_readiness_payload", "paper.readiness.entries_enabled"),
            "auth_readiness": _source(_OD_FILE, "_load_or_refresh_auth_gate_result", "global.auth.runtime_ready"),
            "market_data_readiness": _source(_OD_FILE, "snapshot", "global.market_data_label"),
            "runtime_recovery_state": _source(_OD_FILE, "_paper_runtime_recovery_payload", "paper.runtime_recovery.status"),
            "runtime_recovery_message": _source(_OD_FILE, "_paper_runtime_recovery_payload", "paper.runtime_recovery.operator_message"),
            "blocking_faults": _source(_OD_FILE, "_paper_exceptions_payload", "paper.exceptions.exceptions"),
            "degraded_informational_feeds": _source(_OS_FILE, "_build_runtime_readiness", "secondary_context.*.available"),
            "bootstrap_prerequisites": _source(_OD_FILE, "_dashboard_bootstrap_prerequisites_payload", "bootstrap_prerequisites.items"),
        },
    }
    payload["values"] = {
        "runtime_status": runtime_status,
        "paper_enabled": paper_enabled,
        "entries_enabled": entries_enabled,
        "auth_readiness": auth_readiness,
        "market_data_readiness": market_data_readiness,
        "runtime_recovery_state": runtime_recovery_state,
        "runtime_recovery_message": runtime_recovery_message,
        "runtime_recovery_manual_action_required": bool(runtime_recovery.get("manual_action_required")),
        "runtime_recovery_attempts": runtime_recovery_attempts,
        "runtime_recovery_attempt_budget": runtime_recovery_attempt_budget,
        "runtime_recovery_suppressed": runtime_recovery_suppressed,
        "runtime_recovery_last_result": runtime_recovery.get("last_restart_result"),
        "blocking_faults_count": len(blocking_faults),
        "blocking_faults_active": payload["blocking_faults_active"],
        "degraded_informational_feeds": degraded_informational_feeds,
        "bootstrap_prerequisite_issues": bootstrap_issues,
        "bootstrap_prerequisites_reduced_mode": bool(bootstrap_prerequisites.get("reduced_mode")),
        "active_lanes_count": active_lane_count,
        "active_instruments_count": active_instruments_count,
    }
    payload["bootstrap_prerequisites"] = bootstrap_prerequisites
    return payload


def _build_operator_metrics_portfolio(
    *,
    paper: dict[str, Any],
    active_rows: list[dict[str, Any]],
    instrument_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    performance = paper.get("performance") or {}
    session_shape = paper.get("session_shape") or {}
    session_metrics = performance.get("session_metrics") or {}
    daily_realized_pnl = performance.get("realized_pnl")
    daily_unrealized_pnl = performance.get("unrealized_pnl")
    daily_net_pnl = performance.get("total_pnl")
    active_positions_count_direct = _int_or_none(session_metrics.get("open_trade_count"))
    active_positions_count_rollup = sum(int(row.get("active_position_count", 0) or 0) for row in instrument_rows)
    active_positions_count = (
        active_positions_count_direct
        if active_positions_count_direct is not None
        else active_positions_count_rollup
    )
    active_signals_count = len(_unique_lane_ids([row for row in active_rows if row.get("signaled_today")]))
    blocked_lanes_count = len(_unique_lane_ids([row for row in active_rows if row.get("blocked")]))
    active_instruments_count = len({str(row.get("instrument") or "") for row in active_rows if row.get("instrument")})
    active_lanes_count = len(_unique_lane_ids(active_rows, enabled_only=True))
    realized_horizons = _portfolio_realized_horizons(performance)
    payload = {
        "daily_realized_pnl": daily_realized_pnl,
        "daily_unrealized_pnl": daily_unrealized_pnl,
        "daily_net_pnl": daily_net_pnl,
        "realized_pnl_horizons": realized_horizons,
        "intraday_max_drawdown": session_shape.get("max_intraday_drawdown"),
        "active_positions_count": active_positions_count,
        "active_signals_count": active_signals_count,
        "blocked_lanes_count": blocked_lanes_count,
        "active_instruments_count": active_instruments_count,
        "active_lanes_count": active_lanes_count,
        "status_line": (
            f"realized={daily_realized_pnl or 'Unavailable'} | "
            f"unrealized={daily_unrealized_pnl or 'Unavailable'} | "
            f"net={daily_net_pnl or 'Unavailable'} | "
            f"max_dd={session_shape.get('max_intraday_drawdown') or 'Unavailable'}"
        ),
        "field_sources": {
            "daily_realized_pnl": _source(_OD_FILE, "_paper_performance_payload", "paper.performance.realized_pnl"),
            "daily_unrealized_pnl": _source(_OD_FILE, "_paper_performance_payload", "paper.performance.unrealized_pnl"),
            "daily_net_pnl": _source(_OD_FILE, "_paper_performance_payload", "paper.performance.total_pnl"),
            "realized_pnl_horizons": _source(_OS_FILE, "_portfolio_realized_horizons", "paper.performance.*session_date + paper.performance.realized_pnl"),
            "intraday_max_drawdown": _source(_OD_FILE, "_paper_session_shape_payload", "paper.session_shape.max_intraday_drawdown"),
            "active_positions_count": _source(_OD_FILE, "_paper_performance_payload", "paper.performance.session_metrics.open_trade_count"),
            "active_signals_count": _source(_OS_FILE, "_build_operator_metrics_portfolio", "active_instrument_surface.rows[].signaled_today"),
            "blocked_lanes_count": _source(_OS_FILE, "_build_operator_metrics_portfolio", "active_instrument_surface.rows[].blocked"),
            "active_instruments_count": _source(_OS_FILE, "_build_operator_metrics_portfolio", "active_instrument_surface.rows[].instrument"),
            "active_lanes_count": _source(_OS_FILE, "_build_operator_metrics_portfolio", "active_instrument_surface.rows[].lane_id"),
        },
    }
    payload["values"] = {
        "daily_realized_pnl": daily_realized_pnl,
        "daily_unrealized_pnl": daily_unrealized_pnl,
        "daily_net_pnl": daily_net_pnl,
        "realized_pnl_horizons": realized_horizons,
        "intraday_max_drawdown": session_shape.get("max_intraday_drawdown"),
        "active_positions_count": active_positions_count,
        "active_signals_count": active_signals_count,
        "blocked_lanes_count": blocked_lanes_count,
        "active_instruments_count": active_instruments_count,
        "active_lanes_count": active_lanes_count,
    }
    return payload


def _build_operator_metrics_by_instrument(
    *,
    active_rows: list[dict[str, Any]],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in active_rows:
        instrument = str(row.get("instrument") or "").strip()
        if instrument:
            grouped[instrument].append(row)

    result_rows: list[dict[str, Any]] = []
    for instrument in sorted(grouped):
        rows = grouped[instrument]
        warning_values = sorted({warning for row in rows for warning in list(row.get("warnings") or [])})
        lane_ids = _unique_lane_ids(rows)
        signaled_lane_ids = _unique_lane_ids([row for row in rows if row.get("signaled_today")])
        blocked_lane_ids = _unique_lane_ids([row for row in rows if row.get("blocked")])
        latest_activity_timestamp = max(
            (str(row.get("latest_timestamp") or "") for row in rows if row.get("latest_timestamp")),
            default="",
        ) or None
        realized_value = evidence["realized"].get(instrument, Decimal("0"))
        unrealized_value = evidence["unrealized"].get(instrument, Decimal("0"))
        net_value = realized_value + unrealized_value
        drawdown_value = evidence["drawdown"].get(instrument, Decimal("0"))
        attribution_status = evidence["attribution_status"].get(
            instrument,
            "no_current_session_trade_or_position_evidence",
        )
        attribution_notes = list(evidence["attribution_notes"].get(instrument, []))
        result_rows.append(
            {
                "instrument": instrument,
                "classification_mix": sorted({str(row.get("classification") or "-") for row in rows}),
                "realized_pnl_horizons": _instrument_realized_horizons(
                    instrument=instrument,
                    today_realized=_decimal_to_string(realized_value),
                ),
                "realized_pnl": _decimal_to_string(realized_value),
                "unrealized_pnl": _decimal_to_string(unrealized_value),
                "net_pnl": _decimal_to_string(net_value),
                "current_session_max_drawdown": _decimal_to_string(drawdown_value),
                "active_position_count": sum(1 for row in rows if row.get("open_position")),
                "open_risk_flag": any(bool(row.get("open_position")) for row in rows),
                "active_signal_count": len(signaled_lane_ids),
                "blocked_lane_count": len(blocked_lane_ids),
                "active_lane_count": len(lane_ids),
                "warning_count": len(warning_values),
                "warning_summary": ", ".join(warning_values) if warning_values else "No active warnings.",
                "warnings": warning_values,
                "latest_activity_timestamp": latest_activity_timestamp,
                "attribution_status": attribution_status,
                "attribution_notes": attribution_notes,
                "field_sources": {
                    "realized_pnl": _source(_OS_FILE, "_build_instrument_evidence_maps", "paper.full_blotter_rows / paper.latest_blotter_rows"),
                    "unrealized_pnl": _source(_OS_FILE, "_build_instrument_evidence_maps", "paper.position.unrealized_pnl"),
                    "net_pnl": _source(_OS_FILE, "_build_operator_metrics_by_instrument", "realized_pnl + unrealized_pnl"),
                    "current_session_max_drawdown": _source(_OS_FILE, "_build_instrument_evidence_maps", "instrument trade path + current open position"),
                    "active_position_count": _source(_OS_FILE, "_build_operator_metrics_by_instrument", "active_instrument_surface.rows[].open_position"),
                    "active_signal_count": _source(_OS_FILE, "_build_operator_metrics_by_instrument", "active_instrument_surface.rows[].signaled_today"),
                    "blocked_lane_count": _source(_OS_FILE, "_build_operator_metrics_by_instrument", "active_instrument_surface.rows[].blocked"),
                    "active_lane_count": _source(_OS_FILE, "_build_operator_metrics_by_instrument", "active_instrument_surface.rows[].lane_id"),
                },
            }
        )
    return {
        "rows": result_rows,
        "active_instruments_count": len(result_rows),
        "status_line": f"{len(result_rows)} surfaced instruments with unified operator metrics.",
        "aggregation_note": (
            "Instrument metrics roll up from lane/instrument rows plus current-session paper trade and position evidence. "
            "When blotter rows are untagged by instrument, a narrow fallback is used only when a unique surfaced lane makes attribution explicit."
        ),
    }


def _build_current_active_positions(
    *,
    paper: dict[str, Any],
    active_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    position = paper.get("position") or {}
    open_rows = [row for row in active_rows if row.get("open_position")]
    if str(position.get("side") or "FLAT") == "FLAT" or not open_rows:
        return {
            "rows": [],
            "open_position_count": 0,
            "status_line": "No active positions.",
            "field_sources": {
                "rows": _source(_OS_FILE, "_build_current_active_positions", "paper.position + active_instrument_surface.rows[].open_position"),
            },
        }

    rows: list[dict[str, Any]] = []
    for row in open_rows:
        if str(row.get("instrument") or "") != str(position.get("instrument") or ""):
            continue
        realized = row.get("current_realized_pnl")
        unrealized = row.get("current_unrealized_pnl") or position.get("unrealized_pnl")
        rows.append(
            {
                "instrument": str(row.get("instrument") or position.get("instrument") or "-"),
                "classification": str(row.get("classification") or "-"),
                "lane_id": str(row.get("lane_id") or "-"),
                "display_name": str(row.get("display_name") or "-"),
                "side": str(row.get("side") or position.get("side") or "-"),
                "quantity": position.get("quantity"),
                "entry_basis": position.get("average_price"),
                "entry_timestamp": None,
                "realized_pnl": realized,
                "unrealized_pnl": unrealized,
                "net_pnl": _sum_string_values(realized, unrealized),
                "active_exit": row.get("active_exit"),
                "warnings": list(row.get("warnings") or []),
                "warning_summary": row.get("warning_summary") or "No active warnings.",
                "open_risk_state": "OPEN_POSITION",
            }
        )
    return {
        "rows": rows,
        "open_position_count": len(rows),
        "status_line": f"{len(rows)} active position{'s' if len(rows) != 1 else ''}." if rows else "No active positions.",
        "field_sources": {
            "rows": _source(_OS_FILE, "_build_current_active_positions", "paper.position + active_instrument_surface.rows[].open_position"),
            "quantity": _source(_OD_FILE, "_paper_position_payload", "paper.position.quantity"),
            "entry_basis": _source(_OD_FILE, "_paper_position_payload", "paper.position.average_price"),
        },
    }


def _build_active_instrument_surface_rows(
    *,
    paper: dict[str, Any],
    approved_quant_baselines: dict[str, Any],
    session_date: date | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    admitted_details = (paper.get("approved_models") or {}).get("details_by_branch") or {}
    for row in list((paper.get("approved_models") or {}).get("rows") or []):
        detail = admitted_details.get(str(row.get("branch") or "")) or {}
        latest_timestamp = str(row.get("latest_activity_timestamp") or row.get("last_signal_seen") or "")
        warnings = []
        if str(row.get("risk_state") or "OK") != "OK":
            warnings.append(str(row.get("risk_state")))
        if row.get("halt_reason"):
            warnings.append(str(row.get("halt_reason")))
        rows.append(
            {
                "instrument": str(row.get("instrument") or "-"),
                "lane_id": str(row.get("lane_id") or row.get("branch") or ""),
                "display_name": str(row.get("branch") or row.get("lane_id") or "-"),
                "classification": "admitted_paper",
                "classification_tag": "admitted_paper",
                "enabled": bool(row.get("enabled")),
                "state": str(row.get("chain_state") or row.get("state") or "UNKNOWN"),
                "blocked": bool(int(row.get("blocked_count", 0) or 0) > 0 or str(row.get("risk_state") or "OK") != "OK"),
                "side": str(row.get("side") or "-"),
                "session": str(row.get("session_restriction") or "-"),
                "active_exit": "runtime_managed_legacy_exit",
                "latest_timestamp": latest_timestamp or None,
                "today_contribution": detail.get("latest_activity_type") or row.get("latest_activity_type") or "NO_ACTIVITY",
                "current_realized_pnl": row.get("realized_pnl"),
                "current_unrealized_pnl": row.get("unrealized_pnl"),
                "current_net_pnl": _sum_string_values(row.get("realized_pnl"), row.get("unrealized_pnl")),
                "current_session_max_drawdown": None,
                "warnings": warnings,
                "warning_summary": ", ".join(warnings) if warnings else "No active warnings.",
                "signaled_today": _timestamp_matches_session(latest_timestamp, session_date),
                "open_position": bool(row.get("open_position")),
                "approved_classification": "admitted_paper",
            }
        )
    for row in list((paper.get("non_approved_lanes") or {}).get("rows") or []):
        latest_timestamp = str(row.get("latest_activity_timestamp") or row.get("fired_at") or "")
        warnings = []
        if str(row.get("risk_state") or "OK") != "OK":
            warnings.append(str(row.get("risk_state")))
        is_temporary_paper = bool(
            row.get("temporary_paper_strategy")
            or row.get("paper_strategy_class") == "temporary_paper_strategy"
            or row.get("experimental_status") == "experimental_canary"
        )
        if is_temporary_paper:
            warnings.append("paper_only")
            warnings.append("experimental")
            warnings.append("non_approved")
        allow_block_summary = row.get("allow_block_override_summary") or {}
        operator_warning_summary = ", ".join(warnings) if warnings else "No active warnings."
        if is_temporary_paper:
            operator_warning_summary = (
                f"Paper Only | Experimental | Non-Approved | {allow_block_summary.get('label') or 'no recent signal summary'}"
            )
        rows.append(
            {
                "instrument": str(row.get("instrument") or "-"),
                "lane_id": str(row.get("lane_id") or row.get("display_name") or ""),
                "display_name": str(row.get("display_name") or row.get("branch") or "-"),
                "classification": "Experimental Paper Strategy" if is_temporary_paper else "Canary",
                "classification_tag": "temporary_paper" if is_temporary_paper else "canary",
                "enabled": str(row.get("state") or "").upper() == "ENABLED",
                "state": str(row.get("lifecycle_state") or row.get("state") or "UNKNOWN"),
                "blocked": bool(str(row.get("risk_state") or "OK") != "OK" or row.get("kill_switch_active")),
                "side": str(row.get("side") or row.get("position_side") or "-"),
                "session": str(row.get("session_restriction") or "-"),
                "active_exit": (
                    f"paper_only.{row.get('quality_bucket_policy') or 'temporary_paper'}"
                    if is_temporary_paper
                    else "runtime_managed_canary_exit"
                ),
                "latest_timestamp": str(row.get("last_update_timestamp") or latest_timestamp) or None,
                "today_contribution": (
                    allow_block_summary.get("label")
                    or row.get("lifecycle_state")
                    or ("FIRED" if row.get("fired") else "NO_ACTIVITY")
                ),
                "current_realized_pnl": row.get("metrics_net_pnl_cash", row.get("realized_pnl")),
                "current_unrealized_pnl": None,
                "current_net_pnl": row.get("metrics_net_pnl_cash", row.get("realized_pnl")),
                "current_session_max_drawdown": row.get("metrics_max_drawdown"),
                "warnings": warnings,
                "warning_summary": operator_warning_summary,
                "signaled_today": bool(row.get("fired")) and _timestamp_matches_session(str(row.get("last_update_timestamp") or latest_timestamp), session_date),
                "open_position": bool(row.get("open_position")),
                "approved_classification": "temporary_paper" if is_temporary_paper else "canary",
                "family": (
                    f"Temporary Paper / {row.get('quality_bucket_policy') or '-'}"
                    if is_temporary_paper
                    else str(row.get("source_family") or row.get("lane_mode") or "Canary")
                ),
            }
        )
    for row in list(approved_quant_baselines.get("rows") or []):
        approved_scope = row.get("approved_scope") or {}
        latest_timestamp = str(row.get("latest_signal_timestamp") or "")
        warnings = list(row.get("warning_flags") or [])
        unknown_warning = row.get("unknown_session_warning") or {}
        if unknown_warning.get("flag"):
            warnings.append(str(unknown_warning.get("label") or "unknown_session_watch"))
        if row.get("slice_weakness_flag"):
            warnings.append("slice_weakness_watch")
        warnings = sorted(dict.fromkeys(warnings))
        for symbol in list(approved_scope.get("symbols") or []):
            rows.append(
                {
                    "instrument": str(symbol),
                    "lane_id": str(row.get("lane_id") or row.get("lane_name") or ""),
                    "display_name": str(row.get("lane_name") or row.get("lane_id") or "-"),
                    "classification": "approved_quant",
                    "classification_tag": "approved_quant",
                    "enabled": str(row.get("probation_status") or "unknown").lower() != "suspend",
                    "state": str(row.get("probation_status") or "unknown").upper(),
                    "blocked": str(row.get("probation_status") or "unknown").lower() in {"review", "suspend", "downgraded"},
                    "side": str(approved_scope.get("direction") or "-"),
                    "session": "/".join(list(approved_scope.get("allowed_sessions") or [])) or "-",
                    "active_exit": str(row.get("approved_exit_label") or "-"),
                    "latest_timestamp": latest_timestamp or None,
                    "today_contribution": row.get("post_cost_monitoring_read", {}).get("label") or "probation_monitoring",
                    "current_realized_pnl": None,
                    "current_unrealized_pnl": None,
                    "current_net_pnl": None,
                    "current_session_max_drawdown": None,
                    "warnings": warnings,
                    "warning_summary": ", ".join(warnings) if warnings else "No active warnings.",
                    "signaled_today": _timestamp_matches_session(latest_timestamp, session_date),
                    "open_position": False,
                    "approved_classification": "approved_quant",
                }
            )
    rows = _annotate_active_strategy_identity(rows)
    rows = _dedupe_engine_backed_quant_rows(rows)
    return sorted(
        rows,
        key=lambda row: (
            _classification_priority(row.get("classification_tag") or row.get("classification")),
            str(row.get("instrument") or ""),
            str(row.get("display_name") or ""),
        ),
    )


def _build_active_instrument_surface_block(*, active_rows: list[dict[str, Any]]) -> dict[str, Any]:
    classification_row_counts = {
        "approved_quant": sum(1 for row in active_rows if row.get("classification") == "approved_quant"),
        "admitted_paper": sum(1 for row in active_rows if row.get("classification") == "admitted_paper"),
        "temporary_paper": sum(1 for row in active_rows if row.get("classification_tag") == "temporary_paper"),
        "canary": sum(1 for row in active_rows if row.get("classification_tag") == "canary"),
    }
    classification_lane_counts = {
        "approved_quant": len(_unique_lane_ids([row for row in active_rows if row.get("classification") == "approved_quant"])),
        "admitted_paper": len(_unique_lane_ids([row for row in active_rows if row.get("classification") == "admitted_paper"])),
        "temporary_paper": len(_unique_lane_ids([row for row in active_rows if row.get("classification_tag") == "temporary_paper"])),
        "canary": len(_unique_lane_ids([row for row in active_rows if row.get("classification_tag") == "canary"])),
    }
    active_instruments_count = len({str(row.get("instrument") or "") for row in active_rows if row.get("instrument")})
    active_lanes_count = len(_unique_lane_ids(active_rows, enabled_only=True))
    return {
        "rows": active_rows,
        "active_instruments_count": active_instruments_count,
        "active_lanes_count": active_lanes_count,
        "classification_counts": classification_lane_counts,
        "classification_row_counts": classification_row_counts,
        "status_line": (
            f"{classification_lane_counts['approved_quant']} approved quant + "
            f"{classification_lane_counts['admitted_paper']} admitted paper + "
            f"{classification_lane_counts['temporary_paper']} temporary paper + "
            f"{classification_lane_counts['canary']} canary logical lanes across {active_instruments_count} instruments"
        ),
        "field_sources": {
            "rows": _source(_OS_FILE, "_build_active_instrument_surface_rows", "paper.approved_models + paper.non_approved_lanes + approved_quant_baselines"),
            "classification_counts": _source(_OS_FILE, "_build_active_instrument_surface_block", "active_instrument_surface.rows[].lane_id"),
            "classification_row_counts": _source(_OS_FILE, "_build_active_instrument_surface_block", "active_instrument_surface.rows[].classification"),
            "active_instruments_count": _source(_OS_FILE, "_build_active_instrument_surface_block", "active_instrument_surface.rows[].instrument"),
            "active_lanes_count": _source(_OS_FILE, "_build_active_instrument_surface_block", "active_instrument_surface.rows[].lane_id"),
        },
    }


def _annotate_active_strategy_identity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    instrument_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        instrument = str(row.get("instrument") or "").strip().upper()
        if instrument:
            instrument_counts[instrument] += 1

    annotated: list[dict[str, Any]] = []
    for row in rows:
        identity = build_standalone_strategy_identity(
            instrument=row.get("instrument"),
            lane_id=row.get("lane_id"),
            strategy_name=row.get("display_name"),
            source_family=row.get("source_family") or row.get("family"),
            lane_name=row.get("lane_name"),
        )
        instrument = identity["instrument"]
        same_underlying_ambiguity = instrument_counts.get(instrument, 0) > 1
        ambiguity_note = (
            "Multiple standalone strategy identities currently share this underlying instrument. Same-underlying execution/netting remains explicitly constrained."
            if same_underlying_ambiguity
            else None
        )
        annotated.append(
            {
                **row,
                **identity,
                "same_underlying_ambiguity": same_underlying_ambiguity,
                "same_underlying_ambiguity_note": ambiguity_note,
            }
        )
    return annotated


def _dedupe_engine_backed_quant_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    engine_backed_ids = {
        str(row.get("standalone_strategy_id") or "")
        for row in rows
        if str(row.get("classification") or "") != "approved_quant" and row.get("standalone_strategy_id")
    }
    deduped: list[dict[str, Any]] = []
    for row in rows:
        if (
            str(row.get("classification") or "") == "approved_quant"
            and str(row.get("standalone_strategy_id") or "") in engine_backed_ids
        ):
            continue
        deduped.append(row)
    return deduped


def _build_secondary_context(
    *,
    paper: dict[str, Any],
    market_context: dict[str, Any],
    treasury_curve: dict[str, Any],
) -> dict[str, Any]:
    items = _secondary_context_items(
        paper=paper,
        market_context=market_context,
        treasury_curve=treasury_curve,
    )
    compact_items = [
        items["mgc_key_quote"],
        items["index_levels"],
        items["vix"],
        items["treasury_curve_current"],
        items["treasury_curve_prior"],
    ]
    status_counts = {
        "live": sum(1 for item in compact_items if item.get("status") == "live"),
        "live_thin_comparison": sum(1 for item in compact_items if item.get("status") == "live_thin_comparison"),
        "live_no_valid_prior": sum(1 for item in compact_items if item.get("status") == "live_no_valid_prior"),
        "stale": sum(1 for item in compact_items if item.get("status") == "stale"),
        "unavailable": sum(1 for item in compact_items if item.get("status") == "unavailable"),
    }
    return {
        "mgc_key_quote": items["mgc_key_quote"],
        "mgc_key_level": items["mgc_key_quote"],
        "vix": items["vix"],
        "index_levels": items["index_levels"],
        "major_equity_indices": items["index_levels"],
        "treasury_curve": {
            "current": items["treasury_curve_current"],
            "prior": items["treasury_curve_prior"],
            "summary": items["treasury_curve_summary"],
        },
        "treasury_curve_current": items["treasury_curve_current"],
        "treasury_curve_prior": items["treasury_curve_prior"],
        "status_line": (
            "Informational context: "
            f"live={status_counts['live']} | thin_cmp={status_counts['live_thin_comparison']} | "
            f"no_valid_prior={status_counts['live_no_valid_prior']} | "
            f"stale={status_counts['stale']} | unavailable={status_counts['unavailable']}"
        ),
        "status_counts": status_counts,
        "field_sources": {
            "mgc_key_quote": _source(_OD_FILE, "_paper_position_payload", "paper.position.latest_bar_close"),
            "vix": _source(_OD_FILE, "_market_index_strip_payload", "market_context.symbols[VIX]"),
            "index_levels": _source(_OD_FILE, "_market_index_strip_payload", "market_context.symbols[DJIA/SPX/NDX/RUT]"),
            "treasury_curve_current": _source(_OD_FILE, "_treasury_curve_payload", "treasury_curve.tenors[].current_yield"),
            "treasury_curve_prior": _source(_OD_FILE, "_treasury_curve_payload", "treasury_curve.tenors[].prior_yield"),
        },
        "items": [
            {
                **item,
                "value_label": item.get("value"),
            }
            for item in compact_items
        ],
    }


def _build_rollup_integrity(
    *,
    portfolio_metrics: dict[str, Any],
    instrument_rows: list[dict[str, Any]],
    active_rows: list[dict[str, Any]],
    active_surface: dict[str, Any],
) -> dict[str, Any]:
    realized_sum = sum((_decimal(row.get("realized_pnl")) or Decimal("0")) for row in instrument_rows)
    unrealized_sum = sum((_decimal(row.get("unrealized_pnl")) or Decimal("0")) for row in instrument_rows)
    net_sum = sum((_decimal(row.get("net_pnl")) or Decimal("0")) for row in instrument_rows)
    portfolio_realized = _decimal(portfolio_metrics.get("daily_realized_pnl"))
    portfolio_unrealized = _decimal(portfolio_metrics.get("daily_unrealized_pnl"))
    portfolio_net = _decimal(portfolio_metrics.get("daily_net_pnl"))
    instrument_position_count = sum(int(row.get("active_position_count", 0) or 0) for row in instrument_rows)
    portfolio_position_count = int(portfolio_metrics.get("active_positions_count", 0) or 0)
    return {
        "realized_pnl_reconciliation": {
            "portfolio_value": _decimal_to_string(portfolio_realized),
            "instrument_sum": _decimal_to_string(realized_sum),
            "reconciles": portfolio_realized == realized_sum if portfolio_realized is not None else realized_sum == Decimal("0"),
        },
        "unrealized_pnl_reconciliation": {
            "portfolio_value": _decimal_to_string(portfolio_unrealized),
            "instrument_sum": _decimal_to_string(unrealized_sum),
            "reconciles": portfolio_unrealized == unrealized_sum if portfolio_unrealized is not None else unrealized_sum == Decimal("0"),
        },
        "net_pnl_reconciliation": {
            "portfolio_value": _decimal_to_string(portfolio_net),
            "instrument_sum": _decimal_to_string(net_sum),
            "reconciles": portfolio_net == net_sum if portfolio_net is not None else net_sum == Decimal("0"),
        },
        "position_count_reconciliation": {
            "portfolio_value": portfolio_position_count,
            "instrument_sum": instrument_position_count,
            "reconciles": portfolio_position_count == instrument_position_count,
        },
        "count_reconciliation": {
            "active_instruments_count": active_surface.get("active_instruments_count"),
            "active_lanes_count": active_surface.get("active_lanes_count"),
            "classification_counts": active_surface.get("classification_counts"),
            "classification_row_counts": active_surface.get("classification_row_counts"),
            "active_instrument_surface_rows": len(active_rows),
        },
        "aggregation_logic": {
            "lane_to_instrument_counts": "Direct rollup from active_instrument_surface rows grouped by instrument and unique lane_id.",
            "lane_to_instrument_pnl": (
                "Instrument PnL uses current-session paper trade and position evidence grouped by instrument. "
                "If blotter rows are missing instrument tags, a narrow canary-only fallback is used only when a single surfaced canary instrument exists."
            ),
            "instrument_to_portfolio_pnl": "Portfolio realized/unrealized/net should match the sum of instrument-level values.",
            "lane_level_drawdown": "Not persisted as a fully reliable metric for every visible lane row.",
            "instrument_level_drawdown": "Derived from per-instrument current-session equity path built from trade PnL steps plus any open-position unrealized PnL.",
            "portfolio_level_drawdown": "Direct from paper.session_shape.max_intraday_drawdown.",
        },
    }


def _build_source_manifest() -> dict[str, Any]:
    return {
        "runtime_readiness": {
            "file": _OD_FILE,
            "functions": [
                "_paper_readiness_payload",
                "_paper_exceptions_payload",
                "_load_or_refresh_auth_gate_result",
                "snapshot",
            ],
        },
        "operator_metrics_portfolio": {
            "file": _OD_FILE,
            "functions": [
                "_paper_performance_payload",
                "_paper_session_shape_payload",
            ],
        },
        "operator_metrics_by_instrument": {
            "file": _OS_FILE,
            "functions": [
                "_build_instrument_evidence_maps",
                "_build_operator_metrics_by_instrument",
            ],
        },
        "current_active_positions": {
            "file": _OS_FILE,
            "functions": ["_build_current_active_positions"],
        },
        "active_instrument_surface": {
            "file": _OS_FILE,
            "functions": ["_build_active_instrument_surface_rows"],
        },
        "approved_quant_baselines": {
            "file": _AQ_FILE,
            "functions": ["load_approved_quant_baselines_snapshot"],
        },
        "secondary_context": {
            "file": _OD_FILE,
            "functions": [
                "_market_index_strip_payload",
                "_treasury_curve_payload",
                "_paper_position_payload",
            ],
        },
    }


def _legacy_readiness_alias(payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("values") or {}
    return {
        "provenance": "operator_critical",
        "title": "Runtime / Readiness",
        "status_line": payload.get("status_line"),
        "cards": [
            {"label": "System Health", "value": values.get("runtime_status") or "-", "level": "info"},
            {"label": "Paper Runtime", "value": "RUNNING" if values.get("paper_enabled") else "STOPPED", "level": "ok" if values.get("paper_enabled") else "warning"},
            {"label": "Entries", "value": "ENABLED" if values.get("entries_enabled") else "HALTED", "level": "ok" if values.get("entries_enabled") else "warning"},
            {"label": "Market Data", "value": values.get("market_data_readiness") or "-", "level": "info"},
            {"label": "Faults", "value": str(values.get("blocking_faults_count") or 0), "level": "danger" if values.get("blocking_faults_count") else "ok"},
            {"label": "Active Lanes", "value": str(values.get("active_lanes_count") or 0), "level": "info"},
            {"label": "Active Instruments", "value": str(values.get("active_instruments_count") or 0), "level": "info"},
        ],
        "notes": [
            f"Auth readiness: {'READY' if values.get('auth_readiness') else 'NOT_READY'}",
            f"Degraded informational feeds: {', '.join(values.get('degraded_informational_feeds') or []) or 'None'}",
        ],
    }


def _legacy_portfolio_alias(payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("values") or {}
    return {
        "provenance": "operator_critical",
        "title": "Daily Risk / Performance",
        "status_line": payload.get("status_line"),
        "cards": [
            {"label": "Daily Realized", "value": values.get("daily_realized_pnl") or "Unavailable"},
            {"label": "Daily Unrealized", "value": values.get("daily_unrealized_pnl") or "Unavailable"},
            {"label": "Daily Net", "value": values.get("daily_net_pnl") or "Unavailable"},
            {"label": "Intraday Max DD", "value": values.get("intraday_max_drawdown") or "Unavailable"},
            {"label": "Open Positions", "value": str(values.get("active_positions_count") or 0)},
            {"label": "Signaled Lanes", "value": str(values.get("active_signals_count") or 0)},
            {"label": "Blocked Lanes", "value": str(values.get("blocked_lanes_count") or 0)},
            {"label": "Active Lanes", "value": str(values.get("active_lanes_count") or 0)},
        ],
        "artifact_href": "/api/operator-artifact/paper-performance",
    }


def _legacy_universe_alias(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "provenance": "operator_critical",
        "title": "Unified Active Lane / Instrument Surface",
        "status_line": payload.get("status_line"),
        "cards": [
            {"label": "Total Instruments", "value": str(payload.get("active_instruments_count") or 0)},
            {"label": "Total Lanes", "value": str(payload.get("active_lanes_count") or 0)},
            {"label": "Approved Quant", "value": str((payload.get("classification_counts") or {}).get("approved_quant", 0))},
            {"label": "Admitted Paper", "value": str((payload.get("classification_counts") or {}).get("admitted_paper", 0))},
            {"label": "Canary", "value": str((payload.get("classification_counts") or {}).get("canary", 0))},
        ],
        "instrument_summary": sorted({str(row.get("instrument") or "") for row in payload.get("rows") or [] if row.get("instrument")}),
        "table_note": "Counts and lane state come from the unified active_instrument_surface rows.",
    }


def _legacy_context_alias(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "provenance": "informational_only",
        "title": "Secondary Context",
        "status_line": payload.get("status_line"),
        "items": [
            {
                "label": item.get("label"),
                "value": item.get("value_label"),
                "note": item.get("note"),
                "level": "ok" if item.get("available") else "muted",
                "artifact_href": item.get("artifact_href"),
            }
            for item in payload.get("items") or []
        ],
    }


def _build_instrument_evidence_maps(
    *,
    paper: dict[str, Any],
    active_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    realized: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    unrealized: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    trade_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    attribution_status: dict[str, str] = {}
    attribution_notes: dict[str, list[str]] = defaultdict(list)
    blotter_rows = list(paper.get("full_blotter_rows") or paper.get("latest_blotter_rows") or [])
    untagged_canary_rows: list[dict[str, Any]] = []

    for row in blotter_rows:
        instrument = _row_instrument(row)
        pnl = _decimal(row.get("net_pnl"))
        if pnl is None:
            continue
        if instrument:
            realized[instrument] += pnl
            trade_rows[instrument].append(row)
            attribution_status[instrument] = "exact_trade_evidence"
        elif str(row.get("setup_family") or "").startswith("paperExecutionCanary"):
            untagged_canary_rows.append(row)

    canary_instruments = sorted(
        {
            str(row.get("instrument") or "")
            for row in active_rows
            if row.get("classification") == "canary" and row.get("instrument")
        }
    )
    if untagged_canary_rows and len(canary_instruments) == 1:
        canary_instrument = canary_instruments[0]
        if not trade_rows.get(canary_instrument):
            for row in untagged_canary_rows:
                pnl = _decimal(row.get("net_pnl"))
                if pnl is None:
                    continue
                realized[canary_instrument] += pnl
                trade_rows[canary_instrument].append(row)
            attribution_status[canary_instrument] = "fallback_canary_untagged_blotter"
            attribution_notes[canary_instrument].append(
                "Used untagged canary blotter rows because a single surfaced canary instrument made attribution unambiguous."
            )
    elif untagged_canary_rows and len(canary_instruments) != 1:
        for instrument in canary_instruments:
            attribution_notes[instrument].append(
                "Canary blotter rows were untagged by instrument and could not be assigned uniquely."
            )

    position = paper.get("position") or {}
    if str(position.get("side") or "FLAT") != "FLAT":
        instrument = str(position.get("instrument") or "")
        pnl = _decimal(position.get("unrealized_pnl"))
        if instrument and pnl is not None:
            unrealized[instrument] += pnl
            attribution_notes[instrument].append("Included current open-position unrealized P/L.")
            attribution_status[instrument] = (
                "exact_trade_and_position_evidence"
                if attribution_status.get(instrument) == "exact_trade_evidence"
                else "exact_position_evidence"
            )

    drawdown: dict[str, Decimal] = {}
    surfaced_instruments = sorted({str(row.get("instrument") or "") for row in active_rows if row.get("instrument")})
    for instrument in surfaced_instruments:
        rows = sorted(
            trade_rows.get(instrument, []),
            key=lambda row: str(row.get("exit_ts") or row.get("entry_ts") or ""),
        )
        points: list[Decimal] = [Decimal("0")]
        running = Decimal("0")
        for row in rows:
            pnl = _decimal(row.get("net_pnl"))
            if pnl is None:
                continue
            running += pnl
            points.append(running)
        if instrument in unrealized:
            points.append(running + unrealized[instrument])
        drawdown[instrument] = _drawdown_from_points(points)
        attribution_status.setdefault(instrument, "no_current_session_trade_or_position_evidence")
    return {
        "realized": dict(realized),
        "unrealized": dict(unrealized),
        "drawdown": drawdown,
        "attribution_status": attribution_status,
        "attribution_notes": dict(attribution_notes),
    }


def _secondary_context_items(
    *,
    paper: dict[str, Any],
    market_context: dict[str, Any],
    treasury_curve: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    position = paper.get("position") or {}
    mgc_value = position.get("latest_bar_close")
    paper_status = paper.get("status") or {}
    paper_freshness = str(paper_status.get("freshness") or "UNKNOWN").upper()
    symbol_rows = {str(row.get("label") or "").upper(): row for row in list(market_context.get("symbols") or [])}
    vix_row = symbol_rows.get("VIX") or {}
    index_rows = [symbol_rows.get(label) or {} for label in ("DJIA", "SPX", "NDX", "RUT")]
    available_index_rows = [row for row in index_rows if _context_row_has_value(row.get("current_value"))]
    market_failure = _context_failure_reason(
        market_context.get("diagnostics", {}) if isinstance(market_context.get("diagnostics"), dict) else {},
        str(market_context.get("note") or ""),
    )
    treasury_failure = _context_failure_reason(
        treasury_curve.get("diagnostics", {}) if isinstance(treasury_curve.get("diagnostics"), dict) else {},
        str(treasury_curve.get("curve_note") or treasury_curve.get("coverage_note") or ""),
    )
    mgc_available = _context_row_has_value(mgc_value)
    mgc_status = _local_mark_status(has_value=mgc_available, freshness=paper_freshness)
    vix_available = _context_row_has_value(vix_row.get("current_value"))
    vix_base_status = _quote_feed_status(
        feed_state=str(market_context.get("feed_state") or "UNAVAILABLE"),
        has_value=vix_available,
        row_state=str(vix_row.get("value_state") or ""),
        partial=False,
    )
    vix_comparison = _quote_comparison_semantics(vix_row)
    vix_status = _quote_context_status(base_status=vix_base_status, comparison=vix_comparison)
    indices_partial = len(available_index_rows) not in {0, len(index_rows)}
    index_base_status = _quote_feed_status(
        feed_state=str(market_context.get("feed_state") or "UNAVAILABLE"),
        has_value=bool(available_index_rows),
        partial=indices_partial,
    )
    index_comparison = _index_comparison_semantics(index_rows)
    index_status = _quote_context_status(base_status=index_base_status, comparison=index_comparison)
    treasury_rows = list(treasury_curve.get("tenors") or treasury_curve.get("rows") or [])
    treasury_current_rows = [row for row in treasury_rows if _context_row_has_value(row.get("current_yield"))]
    treasury_prior_rows = [row for row in treasury_rows if _context_row_has_value(row.get("prior_yield"))]
    treasury_current_status = _quote_feed_status(
        feed_state=str(treasury_curve.get("feed_state") or "UNAVAILABLE"),
        has_value=bool(treasury_current_rows),
        row_state="DELAYED" if any(str(row.get("current_state") or "").upper() == "DELAYED" for row in treasury_current_rows) else "",
        partial=len(treasury_current_rows) not in {0, len(treasury_rows)} if treasury_rows else False,
    )
    treasury_prior_base_status = _historical_reference_status(
        feed_state=str(treasury_curve.get("feed_state") or "UNAVAILABLE"),
        has_value=bool(treasury_prior_rows),
        partial=len(treasury_prior_rows) not in {0, len(treasury_rows)} if treasury_rows else False,
    )
    treasury_prior_semantics = _treasury_prior_semantics(treasury_rows)
    treasury_prior_status = _treasury_prior_context_status(
        base_status=treasury_prior_base_status,
        semantics=treasury_prior_semantics,
    )
    index_summary_value = ", ".join(
        f"{row.get('label')} {row.get('current_value')}"
        for row in available_index_rows
        if row.get("label") and _context_row_has_value(row.get("current_value"))
    ) or "Unavailable"
    treasury_current_value = _tenor_value_summary(treasury_current_rows, "current_yield")
    treasury_prior_value = (
        _tenor_value_summary(treasury_prior_rows, "prior_yield")
        if treasury_prior_status == "live"
        else "No valid prior snapshot"
    )
    items = {
        "mgc_key_quote": {
            "label": "MGC Key Quote",
            "available": mgc_available,
            "status": mgc_status,
            "status_label": _context_status_label(mgc_status),
            "status_level": _context_status_level(mgc_status),
            "value": str(mgc_value) if mgc_available else "Unavailable",
            "reference_value": None,
            "source": "_paper_position_payload",
            "source_path": "paper.position.latest_bar_close",
            "criticality": "informational_only",
            "last_refresh_timestamp": str(paper_status.get("last_processed_bar_end_ts") or paper_status.get("last_update_ts") or ""),
            "artifact_href": "/api/operator-artifact/paper-position-state",
            "reason_code": "ok" if mgc_available else "no_data_returned",
            "reason": (
                "Latest captured paper-runtime mark is available."
                if mgc_available
                else "Latest paper-runtime mark unavailable from current paper position state."
            ),
            "note": (
                f"Latest captured paper-runtime mark. Runtime freshness={paper_freshness}."
                if mgc_available
                else f"No latest paper-runtime mark. Runtime freshness={paper_freshness}."
            ),
        },
        "vix": {
            "label": "VIX",
            "available": vix_available,
            "status": vix_status,
            "status_label": _context_status_label(vix_status),
            "status_level": _context_status_level(vix_status),
            "value": str(vix_row.get("current_value") or "Unavailable"),
            "reference_value": None,
            "source": "_market_index_strip_payload",
            "source_path": "market_context.symbols[VIX]",
            "criticality": "informational_only",
            "last_refresh_timestamp": str(market_context.get("updated_at") or ""),
            "artifact_href": "/api/operator-artifact/market-index-strip",
            "reason_code": market_failure["code"] if not vix_available else vix_comparison["reason_code"],
            "reason": market_failure["reason"] if not vix_available else vix_comparison["reason"],
            "note": str(vix_row.get("note") or market_context.get("note") or market_failure["reason"] or "VIX feed unavailable."),
        },
        "index_levels": {
            "label": "Major Equity Indices",
            "available": bool(available_index_rows),
            "status": index_status,
            "status_label": _context_status_label(index_status),
            "status_level": _context_status_level(index_status),
            "value": index_summary_value,
            "summary_value": index_summary_value,
            "reference_value": None,
            "rows": [
                {
                    "label": row.get("label"),
                    "value": row.get("current_value"),
                    "reference_value": None,
                    "status": row.get("value_state"),
                    "note": row.get("note"),
                }
                for row in index_rows
                if row
            ],
            "source": "_market_index_strip_payload",
            "source_path": "market_context.symbols[DJIA/SPX/NDX/RUT]",
            "criticality": "informational_only",
            "last_refresh_timestamp": str(market_context.get("updated_at") or ""),
            "artifact_href": "/api/operator-artifact/market-index-strip",
            "reason_code": market_failure["code"] if not available_index_rows else index_comparison["reason_code"],
            "reason": (
                market_failure["reason"]
                if not available_index_rows
                else index_comparison["reason"]
            ),
            "note": market_context.get("note") or market_failure["reason"] or "Index feed unavailable.",
        },
        "treasury_curve_current": {
            "label": "Treasury Curve Current",
            "available": bool(treasury_current_rows),
            "status": treasury_current_status,
            "status_label": _context_status_label(treasury_current_status),
            "status_level": _context_status_level(treasury_current_status),
            "value": treasury_current_value,
            "reference_value": None,
            "rows": treasury_rows,
            "source": "_treasury_curve_payload",
            "source_path": "treasury_curve.tenors[].current_yield",
            "criticality": "informational_only",
            "last_refresh_timestamp": str(treasury_curve.get("updated_at") or ""),
            "artifact_href": "/api/operator-artifact/treasury-curve",
            "reason_code": treasury_failure["code"] if not treasury_current_rows else ("partial" if len(treasury_current_rows) != len(treasury_rows) else "ok"),
            "reason": (
                treasury_failure["reason"]
                if not treasury_current_rows
                else f"{len(treasury_current_rows)}/{len(treasury_rows)} current Treasury tenors available."
            ),
            "note": str(treasury_curve.get("curve_note") or treasury_curve.get("coverage_note") or treasury_failure["reason"] or "Treasury curve unavailable."),
        },
        "treasury_curve_prior": {
            "label": "Treasury Curve Prior",
            "available": treasury_prior_status == "live",
            "status": treasury_prior_status,
            "status_label": _context_status_label(treasury_prior_status),
            "status_level": _context_status_level(treasury_prior_status),
            "value": treasury_prior_value,
            "reference_value": None,
            "rows": treasury_rows,
            "source": "_treasury_curve_payload",
            "source_path": "treasury_curve.tenors[].prior_yield",
            "criticality": "informational_only",
            "last_refresh_timestamp": str(treasury_curve.get("updated_at") or ""),
            "artifact_href": "/api/operator-artifact/treasury-curve",
            "reason_code": treasury_failure["code"] if treasury_prior_status == "unavailable" else treasury_prior_semantics["reason_code"],
            "reason": (
                treasury_failure["reason"]
                if treasury_prior_status == "unavailable"
                else treasury_prior_semantics["reason"]
            ),
            "note": str(treasury_curve.get("coverage_note") or treasury_curve.get("curve_note") or treasury_failure["reason"] or "Treasury prior curve unavailable."),
        },
        "treasury_curve_summary": {
            "feed_label": str(treasury_curve.get("feed_label") or "Unavailable"),
            "curve_state_label": str((treasury_curve.get("summary") or {}).get("curve_state_label") or "Unavailable"),
            "available_tenors": [str(row.get("tenor") or "") for row in treasury_current_rows],
            "available_prior_tenors": [str(row.get("tenor") or "") for row in treasury_prior_rows],
            "last_successful_snapshot": str(treasury_curve.get("updated_at") or ""),
            "audit_artifact": "/api/operator-artifact/treasury-symbol-audit",
        },
    }
    return items


def _context_row_has_value(value: Any) -> bool:
    return value not in {None, "", "N/A", "Unavailable"}


def _local_mark_status(*, has_value: bool, freshness: str) -> str:
    normalized = freshness.upper()
    if not has_value:
        return "unavailable"
    if normalized == "FRESH":
        return "live"
    return "stale"


def _quote_feed_status(
    *,
    feed_state: str,
    has_value: bool,
    row_state: str = "",
    partial: bool = False,
) -> str:
    normalized_feed = str(feed_state or "").upper()
    normalized_row = str(row_state or "").upper()
    if normalized_feed == "STALE":
        return "stale"
    if not has_value:
        return "unavailable"
    if normalized_feed in {"PARTIAL", "DELAYED"} or normalized_row == "DELAYED" or partial:
        return "stale"
    if normalized_feed == "LIVE":
        return "live"
    return "stale"


def _historical_reference_status(*, feed_state: str, has_value: bool, partial: bool = False) -> str:
    normalized_feed = str(feed_state or "").upper()
    if normalized_feed == "STALE":
        return "stale"
    if not has_value:
        return "unavailable"
    if normalized_feed in {"PARTIAL", "DELAYED"} or partial:
        return "stale"
    return "live"


def _context_status_level(status: str) -> str:
    normalized = str(status or "").lower()
    if normalized == "live":
        return "ok"
    if normalized in {"live_thin_comparison", "live_no_valid_prior"}:
        return "info"
    if normalized == "stale":
        return "warning"
    return "muted"


def _context_status_label(status: str) -> str:
    normalized = str(status or "").lower()
    if normalized == "live":
        return "LIVE"
    if normalized == "live_thin_comparison":
        return "LIVE, THIN COMPARISON"
    if normalized == "live_no_valid_prior":
        return "LIVE, NO VALID PRIOR"
    if normalized == "stale":
        return "STALE"
    return "UNAVAILABLE"


def _quote_comparison_semantics(row: dict[str, Any]) -> dict[str, str]:
    percent_state = (row.get("field_states") or {}).get("percent_change") or {}
    comparison_available = bool(percent_state.get("available")) and _context_row_has_value(row.get("percent_change"))
    comparison_source = str(percent_state.get("source_field") or "")
    if comparison_available:
        return {
            "reason_code": "thin_provider_comparison",
            "reason": (
                "Current quote is live. Comparison on this provider path is a thin quote-field comparison "
                f"({comparison_source or 'provider field'}) and is not promoted in the operator strip."
            ),
        }
    return {
        "reason_code": "comparison_unavailable",
        "reason": "Current quote is live. Comparison is unavailable on the current provider path.",
    }


def _index_comparison_semantics(rows: list[dict[str, Any]]) -> dict[str, str]:
    live_rows = [row for row in rows if _context_row_has_value(row.get("current_value"))]
    comparison_rows = [row for row in live_rows if ((row.get("field_states") or {}).get("percent_change") or {}).get("available")]
    if comparison_rows:
        return {
            "reason_code": "thin_provider_comparison",
            "reason": (
                f"{len(live_rows)}/{len(rows)} index values are live. Comparison fields are provider-thin quote fields "
                "and are not promoted in the operator strip."
            ),
        }
    return {
        "reason_code": "comparison_unavailable",
        "reason": (
            f"{len(live_rows)}/{len(rows)} index values are live. Comparison is unavailable on the current provider path."
        ),
    }


def _quote_context_status(*, base_status: str, comparison: dict[str, str]) -> str:
    if base_status != "live":
        return base_status
    if comparison.get("reason_code") in {"thin_provider_comparison", "comparison_unavailable"}:
        return "live_thin_comparison"
    return "live"


def _treasury_prior_semantics(rows: list[dict[str, Any]]) -> dict[str, str]:
    comparable_pairs = [
        (str(row.get("current_yield") or ""), str(row.get("prior_yield") or ""))
        for row in rows
        if _context_row_has_value(row.get("current_yield")) and _context_row_has_value(row.get("prior_yield"))
    ]
    if not comparable_pairs:
        return {
            "reason_code": "no_valid_prior_snapshot",
            "reason": "Current Treasury curve is live, but this provider path does not expose a valid prior snapshot.",
        }
    if all(current == prior for current, prior in comparable_pairs):
        return {
            "reason_code": "no_valid_prior_snapshot",
            "reason": (
                "Current Treasury curve is live, but the current provider path returns prior values that mirror current values, "
                "so no valid prior snapshot is promoted."
            ),
        }
    return {
        "reason_code": "ok",
        "reason": f"{len(comparable_pairs)}/{len(rows)} prior Treasury tenors available from a distinct comparison snapshot.",
    }


def _treasury_prior_context_status(*, base_status: str, semantics: dict[str, str]) -> str:
    if base_status != "live":
        return base_status
    if semantics.get("reason_code") == "ok":
        return "live"
    return "live_no_valid_prior"


def _context_failure_reason(diagnostics: dict[str, Any], note: str) -> dict[str, str]:
    error_text = str(diagnostics.get("error") or note or "").strip()
    lowered = error_text.lower()
    if any(token in error_text for token in ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET", "SCHWAB_CALLBACK_URL")):
        return {
            "code": "auth",
            "reason": "Schwab auth environment is not loaded for this dashboard process. Next fix: launch the dashboard with SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL set.",
        }
    if "stored access token" in lowered and "expired" in lowered:
        return {
            "code": "token_bootstrap_failure",
            "reason": "Stored Schwab access token is expired and cannot refresh in this dashboard process. Next fix: rerun Schwab token bootstrap refresh or launch with Schwab auth env.",
        }
    if "no token file found" in lowered:
        return {
            "code": "token_bootstrap_failure",
            "reason": "No local Schwab token file is available for dashboard quote fallback. Next fix: run Schwab token bootstrap or set SCHWAB_TOKEN_FILE.",
        }
    if "config not found" in lowered:
        return {
            "code": "missing_adapter_path",
            "reason": "Required Schwab config path is missing for this feed.",
        }
    if any(token in lowered for token in ("timed out", "connection", "dns", "network", "temporarily unavailable")):
        return {
            "code": "network",
            "reason": error_text or "Network path to the Schwab quote feed failed.",
        }
    if any(token in lowered for token in ("unsupported", "invalid symbol", "mapping")):
        return {
            "code": "mapping",
            "reason": error_text or "Configured symbol mapping did not resolve to a supported quote payload.",
        }
    if "no quote payload returned" in lowered or "no data" in lowered:
        return {
            "code": "no_data_returned",
            "reason": error_text or "The quote path returned no usable data for this feed.",
        }
    return {
        "code": "no_data_returned" if error_text else "unknown",
        "reason": error_text or "No usable context data returned from the current feed path.",
    }


def _tenor_value_summary(rows: list[dict[str, Any]], value_key: str) -> str:
    summary = ", ".join(
        f"{row.get('tenor')} {row.get(value_key)}"
        for row in rows
        if row.get("tenor") and _context_row_has_value(row.get(value_key))
    )
    return summary or "Unavailable"


def _portfolio_realized_horizons(performance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    current_session_date = str(performance.get("current_session_date") or "")
    session_date = str(performance.get("session_date") or "")
    today_available = bool(current_session_date and session_date and current_session_date == session_date)
    today_value = performance.get("realized_pnl") if today_available else None
    unavailable_gap = (
        "No persisted cumulative operator-performance ledger is exposed in current dashboard artifacts; "
        "a historical portfolio P&L store or query path is required."
    )
    return {
        "lifetime": _unavailable_horizon(unavailable_gap, _source(_OD_FILE, "_paper_history_payload", "recent session sample only")),
        "ytd": _unavailable_horizon(unavailable_gap, _source(_OD_FILE, "_paper_history_payload", "recent session sample only")),
        "mtd": _unavailable_horizon(unavailable_gap, _source(_OD_FILE, "_paper_history_payload", "recent session sample only")),
        "yesterday": _unavailable_horizon(
            "No explicit prior-day portfolio-realized field is persisted in current operator artifacts.",
            _source(_OD_FILE, "_paper_history_payload", "recent_sessions[] but no authoritative yesterday rollup field"),
        ),
        "today": {
            "available": today_available,
            "value": today_value if today_available else None,
            "source": _source(_OD_FILE, "_paper_performance_payload", "paper.performance.realized_pnl"),
            "source_gap": None if today_available else "Current session date does not reconcile cleanly to the realized-session artifact.",
        },
    }


def _instrument_realized_horizons(*, instrument: str, today_realized: str | None) -> dict[str, dict[str, Any]]:
    unavailable_gap = (
        "No persisted instrument-level historical realized-P&L ledger is exposed in current operator artifacts."
    )
    return {
        "lifetime": _unavailable_horizon(unavailable_gap, _source(_OS_FILE, "_build_instrument_evidence_maps", "current-session evidence only")),
        "ytd": _unavailable_horizon(unavailable_gap, _source(_OS_FILE, "_build_instrument_evidence_maps", "current-session evidence only")),
        "mtd": _unavailable_horizon(unavailable_gap, _source(_OS_FILE, "_build_instrument_evidence_maps", "current-session evidence only")),
        "yesterday": _unavailable_horizon(
            "No explicit prior-day instrument-realized field is persisted in current operator artifacts.",
            _source(_OS_FILE, "_build_instrument_evidence_maps", "current-session evidence only"),
        ),
        "today": {
            "available": True,
            "value": today_realized or "0",
            "source": _source(_OS_FILE, "_build_instrument_evidence_maps", f"current-session trade evidence for {instrument}"),
            "source_gap": None,
        },
    }


def _unavailable_horizon(source_gap: str, source: dict[str, str]) -> dict[str, Any]:
    return {
        "available": False,
        "value": None,
        "source": source,
        "source_gap": source_gap,
    }


def _unique_lane_ids(rows: list[dict[str, Any]], *, enabled_only: bool = False) -> set[str]:
    lane_ids: set[str] = set()
    for row in rows:
        if enabled_only and not row.get("enabled"):
            continue
        lane_id = str(row.get("lane_id") or row.get("display_name") or "").strip()
        if lane_id:
            lane_ids.add(lane_id)
    return lane_ids


def _classification_priority(label: Any) -> int:
    normalized = str(label or "")
    if normalized == "approved_quant":
        return 10
    if normalized == "admitted_paper":
        return 20
    if normalized == "temporary_paper":
        return 25
    if normalized == "canary":
        return 30
    return 99


def _row_instrument(row: dict[str, Any]) -> str:
    return str(row.get("instrument") or row.get("symbol") or "").strip()


def _sum_string_values(left: Any, right: Any) -> str | None:
    left_dec = _decimal(left)
    right_dec = _decimal(right)
    if left_dec is None and right_dec is None:
        return None
    return _decimal_to_string((left_dec or Decimal("0")) + (right_dec or Decimal("0")))


def _drawdown_from_points(points: list[Decimal]) -> Decimal:
    if not points:
        return Decimal("0")
    high_water = points[0]
    worst = Decimal("0")
    for point in points:
        if point > high_water:
            high_water = point
        drawdown = high_water - point
        if drawdown > worst:
            worst = drawdown
    return worst


def _timestamp_matches_session(value: str, session_date: date | None) -> bool:
    if not value or session_date is None:
        return False
    parsed = _safe_datetime(value)
    return bool(parsed and parsed.date() == session_date)


def _safe_session_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _safe_datetime(value: str) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _decimal(value: Any) -> Decimal | None:
    if value in {None, "", "N/A"}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _int_or_none(value: Any) -> int | None:
    if value in {None, "", "N/A"}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _source(file_path: str, function_name: str, field_path: str) -> dict[str, str]:
    return {
        "file": file_path,
        "function": function_name,
        "field_path": field_path,
    }
