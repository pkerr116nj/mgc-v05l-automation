"""Unified strategy monitor payloads for shared operator analysis surfaces."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Sequence

EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY = "benchmark_replay"
EVIDENCE_LANE_TYPE_PAPER_RUNTIME = "paper_runtime"
EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION = "research_execution"
EVIDENCE_LANE_TYPE_TRACKED_AUDIT = "tracked_audit"

STRATEGY_CLASS_LEGACY_BENCHMARK = "legacy_benchmark"
STRATEGY_CLASS_APPROVED_PAPER = "approved_paper"
STRATEGY_CLASS_TEMPORARY_PAPER = "temporary_paper"
STRATEGY_CLASS_ATP = "ATP"
STRATEGY_CLASS_RESEARCH_CANDIDATE = "research_candidate"

COMPATIBLE_EVIDENCE_BUCKETS = (
    EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY,
    EVIDENCE_LANE_TYPE_PAPER_RUNTIME,
    EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION,
    EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
)
DEFAULT_GROUP_KEYS = ("strategy_class", "instrument", "family")
AVAILABLE_GROUP_KEYS = (
    "strategy_class",
    "instrument",
    "family",
    "evidence_lane_type",
    "session_scope",
)
ROLLING_AVERAGE_TRADE_WINDOW = 10


def build_unified_strategy_monitor(
    *,
    catalog_rows: Sequence[dict[str, Any]],
    details_by_strategy_key: dict[str, dict[str, Any]],
    evidence_lanes: Sequence[dict[str, Any]],
    historical_playback: dict[str, Any],
    paper: dict[str, Any],
    runtime_registry: dict[str, Any] | None,
    lane_registry: dict[str, Any] | None,
    research_analytics: dict[str, Any] | None,
    generated_at: str,
) -> dict[str, Any]:
    runtime_registry = dict(runtime_registry or {})
    lane_registry = dict(lane_registry or {})
    research_analytics = dict(research_analytics or {})

    runtime_rows_by_lane_id = {
        str(row.get("lane_id") or "").strip(): dict(row)
        for row in list(runtime_registry.get("rows") or [])
        if str(row.get("lane_id") or "").strip()
    }
    runtime_rows_by_strategy_key = {
        str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip(): dict(row)
        for row in list(runtime_registry.get("rows") or [])
        if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
    }
    lane_registry_rows_by_lane_id = {
        str(row.get("lane_id") or "").strip(): dict(row)
        for row in list(lane_registry.get("rows") or [])
        if str(row.get("lane_id") or "").strip()
    }
    lane_registry_rows_by_strategy_key = {
        str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip(): dict(row)
        for row in list(lane_registry.get("rows") or [])
        if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
    }
    temporary_lane_ids = {
        str(row.get("lane_id") or "").strip()
        for row in list(
            (paper.get("temporary_paper_strategies") or {}).get("rows")
            or (paper.get("non_approved_lanes") or {}).get("rows")
            or []
        )
        if str(row.get("lane_id") or "").strip()
    }
    tracked_rows = {
        str(row.get("strategy_id") or "").strip(): dict(row)
        for row in list(((paper.get("tracked_strategies") or {}).get("rows") or []))
        if str(row.get("strategy_id") or "").strip()
    }
    paper_trade_rows_by_strategy = _group_rows_by_key(
        rows=list(((paper.get("strategy_performance") or {}).get("trade_log") or [])),
        keys=("standalone_strategy_id", "strategy_key", "strategy_id"),
    )
    paper_trade_rows_by_lane = _group_rows_by_key(
        rows=list(((paper.get("strategy_performance") or {}).get("trade_log") or [])),
        keys=("lane_id",),
    )
    tracked_details = {
        str(key).strip(): dict(value)
        for key, value in dict(((paper.get("tracked_strategies") or {}).get("details_by_strategy_id") or {})).items()
        if str(key).strip()
    }

    playback_items_by_study_key = {
        str(item.get("study_key") or "").strip(): dict(item)
        for item in list(((historical_playback.get("study_catalog") or {}).get("items") or []))
        if str(item.get("study_key") or "").strip()
    }
    research_summary_rows = {
        str(row.get("strategy_key") or row.get("strategy_id") or row.get("target_id") or "").strip(): dict(row)
        for row in list(research_analytics.get("strategy_summaries") or [])
        if str(row.get("strategy_key") or row.get("strategy_id") or row.get("target_id") or "").strip()
    }
    research_equity_rows_by_key = _group_rows_by_key(
        rows=list(research_analytics.get("equity_curve") or []),
        keys=("strategy_key", "strategy_id", "target_id"),
    )
    research_trade_rows_by_key = _group_rows_by_key(
        rows=list(research_analytics.get("trade_blotter") or []),
        keys=("strategy_key", "strategy_id", "target_id"),
    )

    comparison_rows: list[dict[str, Any]] = []
    strategy_catalog_rows: list[dict[str, Any]] = []
    metric_support_map: dict[str, dict[str, Any]] = {}
    chart_series_by_lane_id: dict[str, dict[str, Any]] = {}
    detail_views_by_lane_id: dict[str, dict[str, Any]] = {}
    data_quality_rows: list[dict[str, Any]] = []
    rollups_by_lane_id: dict[str, dict[str, Any]] = {}
    contribution_accumulators: dict[str, list[dict[str, Any]]] = {
        "strategy_class": [],
        "instrument": [],
        "family": [],
        "evidence_lane_type": [],
        "session_scope": [],
    }
    identity_candidates: list[dict[str, Any]] = []

    for strategy_row in catalog_rows:
        strategy_catalog_rows.append(
            {
                "strategy_key": strategy_row.get("strategy_key"),
                "display_name": strategy_row.get("display_name") or strategy_row.get("strategy_key"),
                "instrument": strategy_row.get("instrument"),
                "family": strategy_row.get("strategy_family"),
                "standalone_strategy_id": strategy_row.get("standalone_strategy_id"),
                "lane_presence": dict(strategy_row.get("lane_presence") or {}),
                "source_types": list(strategy_row.get("source_types") or []),
                "discovery_sources": list(strategy_row.get("discovery_sources") or []),
            }
        )

    for lane in evidence_lanes:
        comparison_row, chart_payload, detail_payload = _build_comparison_row(
            lane=lane,
            runtime_rows_by_lane_id=runtime_rows_by_lane_id,
            runtime_rows_by_strategy_key=runtime_rows_by_strategy_key,
            lane_registry_rows_by_lane_id=lane_registry_rows_by_lane_id,
            lane_registry_rows_by_strategy_key=lane_registry_rows_by_strategy_key,
            temporary_lane_ids=temporary_lane_ids,
            tracked_rows=tracked_rows,
            paper_trade_rows_by_strategy=paper_trade_rows_by_strategy,
            paper_trade_rows_by_lane=paper_trade_rows_by_lane,
            playback_items_by_study_key=playback_items_by_study_key,
            research_summary_rows=research_summary_rows,
            research_equity_rows_by_key=research_equity_rows_by_key,
            research_trade_rows_by_key=research_trade_rows_by_key,
            generated_at=generated_at,
        )
        lane_id = str(comparison_row.get("lane_id") or "").strip()
        comparison_rows.append(comparison_row)
        metric_support_map[lane_id] = dict(comparison_row.get("metric_support") or {})
        chart_series_by_lane_id[lane_id] = chart_payload
        detail_views_by_lane_id[lane_id] = detail_payload
        data_quality_rows.append(_data_quality_row(comparison_row))
        rollups_by_lane_id[lane_id] = _rollup_views_for_series(chart_payload)
        identity_candidates.append(
            {
                "strategy_key": comparison_row.get("strategy_key"),
                "lane_id": lane_id,
                "display_name": comparison_row.get("display_name"),
                "instrument": comparison_row.get("instrument"),
                "family": comparison_row.get("family"),
            }
        )

    tracked_audit_rows = _build_tracked_audit_rows(
        tracked_rows=tracked_rows,
        tracked_details=tracked_details,
        generated_at=generated_at,
    )
    for comparison_row, chart_payload, detail_payload in tracked_audit_rows:
        lane_id = str(comparison_row.get("lane_id") or "").strip()
        comparison_rows.append(comparison_row)
        metric_support_map[lane_id] = dict(comparison_row.get("metric_support") or {})
        chart_series_by_lane_id[lane_id] = chart_payload
        detail_views_by_lane_id[lane_id] = detail_payload
        data_quality_rows.append(_data_quality_row(comparison_row))
        rollups_by_lane_id[lane_id] = _rollup_views_for_series(chart_payload)
        identity_candidates.append(
            {
                "strategy_key": comparison_row.get("strategy_key"),
                "lane_id": lane_id,
                "display_name": comparison_row.get("display_name"),
                "instrument": comparison_row.get("instrument"),
                "family": comparison_row.get("family"),
            }
        )

    comparison_rows.sort(
        key=lambda row: (
            _strategy_class_sort_rank(str(row.get("strategy_class") or "")),
            str(row.get("instrument") or ""),
            str(row.get("family") or ""),
            str(row.get("display_name") or ""),
            str(row.get("lane_id") or ""),
        )
    )

    for row in comparison_rows:
        for dimension in contribution_accumulators:
            contribution_accumulators[dimension].append(row)

    default_group_tree, group_selection_map = _build_group_tree(
        rows=comparison_rows,
        group_keys=DEFAULT_GROUP_KEYS,
    )

    selection_summary = {
        "active_grouping": list(DEFAULT_GROUP_KEYS),
        "selected_strategy_count": len({str(row.get("strategy_key") or "") for row in comparison_rows if str(row.get("strategy_key") or "")}),
        "selected_lane_count": len(comparison_rows),
        "visible_lane_count": len(comparison_rows),
        "active_provenance_buckets": sorted({str(row.get("evidence_lane_type") or "") for row in comparison_rows if str(row.get("evidence_lane_type") or "")}),
        "active_time_window": {
            "preset": "since_2024_01_01",
            "start": "2024-01-01",
            "end": None,
        },
        "normalization_mode": "absolute_dollars",
        "chart_mode": "overlay",
        "chart_aggregate_scope_note": "Aggregate mode is only valid within a single compatible provenance bucket.",
    }

    leaderboard_views = _build_leaderboard_views(comparison_rows)
    contribution_views = _build_contribution_views(contribution_accumulators)
    identity_exceptions = _build_identity_exceptions(identity_candidates, comparison_rows)
    data_quality_report = _build_data_quality_report(
        comparison_rows=comparison_rows,
        metric_support_map=metric_support_map,
        identity_exceptions=identity_exceptions,
    )

    return {
        "generated_at": generated_at,
        "available": bool(comparison_rows),
        "window_defaults": {
            "preset": "since_2024_01_01",
            "start": "2024-01-01",
            "end": None,
            "group_by": list(DEFAULT_GROUP_KEYS),
            "normalization_mode": "absolute_dollars",
            "chart_mode": "overlay",
        },
        "view_modes": {
            "available": [
                "comparison",
                "charts",
                "leaderboard",
                "rollup",
                "data_quality",
                "detail",
                "contribution",
            ],
            "default": "comparison",
            "navigation_model": {
                "top_level_modes": ["comparison", "charts", "leaderboard", "rollup", "data_quality"],
                "secondary_views": ["detail", "contribution"],
                "preserve_selection_state": True,
                "preserve_filter_state": True,
            },
        },
        "strategy_catalog": {
            "rows": strategy_catalog_rows,
        },
        "comparison_rows": comparison_rows,
        "grouping": {
            "available_group_keys": list(AVAILABLE_GROUP_KEYS),
            "default_group_keys": list(DEFAULT_GROUP_KEYS),
            "group_tree": default_group_tree,
            "default_group_tree": default_group_tree,
            "group_value_maps": {
                str(row.get("lane_id") or ""): dict(row.get("group_values") or {})
                for row in comparison_rows
                if str(row.get("lane_id") or "")
            },
        },
        "selection_contract": {
            "default_selection_behavior": {
                "mode": "select_all_visible_lanes",
                "include_temporary_paper": True,
                "include_legacy_benchmarks": True,
            },
            "group_selection_map": group_selection_map,
            "selected_vs_visible_semantics": {
                "visible_rows_define_selection_domain": True,
                "group_selection_applies_to_visible_children_only": True,
            },
        },
        "selection_summary": selection_summary,
        "sort_contract": {
            "sortable_fields": [
                "display_name",
                "instrument",
                "family",
                "strategy_class",
                "evidence_lane_type",
                "current_status",
                "realized_pnl",
                "open_pnl",
                "net_pnl",
                "trade_count",
                "latest_update_timestamp",
                "freshness_state",
            ],
            "default_sort": {
                "field": "display_name",
                "direction": "asc",
                "within_groups_only": True,
            },
            "null_sort_policy": "last",
            "unsupported_sort_policy": "last_with_explicit_marker",
        },
        "metric_support_map": metric_support_map,
        "aggregate_rules": _aggregate_rules_payload(),
        "chart_series": {
            "series_by_lane_id": chart_series_by_lane_id,
            "aggregate_support": {
                "compatible_buckets": list(COMPATIBLE_EVIDENCE_BUCKETS),
                "notes": [
                    "Overlay mode is always provenance-safe because selected lanes remain distinct.",
                    "Aggregate mode is only valid within a single compatible provenance bucket.",
                    "Mixed incompatible provenance selections must render as grouped overlays or separate provenance aggregates.",
                ],
            },
        },
        "detail_views": {
            "by_lane_id": detail_views_by_lane_id,
        },
        "contribution_views": contribution_views,
        "leaderboard_views": leaderboard_views,
        "rollup_views": {
            "by_lane_id": rollups_by_lane_id,
            "available_dimensions": ["strategy_class", "instrument", "family"],
        },
        "data_quality_report": data_quality_report,
        "identity_exceptions": identity_exceptions,
        "provenance_notes": [
            "Replay, paper-runtime, research-execution, and tracked-audit evidence remain explicitly separated.",
            "Tracked-audit rows stay visible and selectable but remain audit-only read-models.",
            "Unsupported metrics stay unavailable with explicit support-gap reasons rather than synthetic values.",
        ],
    }


def _build_comparison_row(
    *,
    lane: dict[str, Any],
    runtime_rows_by_lane_id: dict[str, dict[str, Any]],
    runtime_rows_by_strategy_key: dict[str, dict[str, Any]],
    lane_registry_rows_by_lane_id: dict[str, dict[str, Any]],
    lane_registry_rows_by_strategy_key: dict[str, dict[str, Any]],
    temporary_lane_ids: set[str],
    tracked_rows: dict[str, dict[str, Any]],
    paper_trade_rows_by_strategy: dict[str, list[dict[str, Any]]],
    paper_trade_rows_by_lane: dict[str, list[dict[str, Any]]],
    playback_items_by_study_key: dict[str, dict[str, Any]],
    research_summary_rows: dict[str, dict[str, Any]],
    research_equity_rows_by_key: dict[str, list[dict[str, Any]]],
    research_trade_rows_by_key: dict[str, list[dict[str, Any]]],
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    strategy_key = str(lane.get("strategy_key") or "").strip()
    raw_lane_id = str(lane.get("lane_id") or "").strip()
    paper_lane_id = str(lane.get("paper_lane_id") or "").strip()
    runtime_row = runtime_rows_by_lane_id.get(paper_lane_id) or runtime_rows_by_lane_id.get(raw_lane_id) or runtime_rows_by_strategy_key.get(strategy_key) or {}
    lane_registry_row = lane_registry_rows_by_lane_id.get(paper_lane_id) or lane_registry_rows_by_lane_id.get(raw_lane_id) or lane_registry_rows_by_strategy_key.get(strategy_key) or {}
    lane_type = str(lane.get("lane_type") or lane.get("source_lane") or "").strip()
    evidence_lane_type = _map_evidence_lane_type(lane_type)
    family = (
        str(lane.get("strategy_family") or lane_registry_row.get("family") or runtime_row.get("strategy_family") or "").strip()
        or None
    )
    instrument = str(lane.get("instrument") or runtime_row.get("instrument") or lane_registry_row.get("instrument") or "").strip() or None
    session_scope = _derive_session_scope(
        lane=lane,
        runtime_row=runtime_row,
        lane_registry_row=lane_registry_row,
    )
    strategy_class = _derive_strategy_class(
        lane=lane,
        evidence_lane_type=evidence_lane_type,
        family=family,
        lane_registry_row=lane_registry_row,
        paper_lane_id=paper_lane_id,
        raw_lane_id=raw_lane_id,
        temporary_lane_ids=temporary_lane_ids,
    )
    runtime_attached = _derive_runtime_attached(
        lane=lane,
        runtime_row=runtime_row,
        lane_registry_row=lane_registry_row,
    )
    current_status = (
        _metric_value_or_none(lane, "latest_status")
        or str((lane.get("runtime_health") or {}).get("label") or "").strip()
        or str(runtime_row.get("status") or lane_registry_row.get("monitoring_summary") or "").strip()
        or None
    )
    runtime_health = _derive_runtime_health(
        lane,
        runtime_row=runtime_row,
        lane_registry_row=lane_registry_row,
        runtime_attached=runtime_attached,
    )
    position_state = _derive_position_state(lane)
    freshness = _derive_freshness(lane=lane, generated_at=generated_at)
    metric_support = _metric_support_for_lane(lane)
    metric_values = _metric_values_for_lane(lane)
    chart_payload = _chart_series_for_lane(
        lane=lane,
        evidence_lane_type=evidence_lane_type,
        paper_trade_rows_by_strategy=paper_trade_rows_by_strategy,
        paper_trade_rows_by_lane=paper_trade_rows_by_lane,
        playback_items_by_study_key=playback_items_by_study_key,
        research_summary_rows=research_summary_rows,
        research_equity_rows_by_key=research_equity_rows_by_key,
        research_trade_rows_by_key=research_trade_rows_by_key,
    )
    trade_stats = _trade_stats_from_chart_payload(chart_payload)
    latest_trade_summary = _latest_trade_summary_label(lane)
    metric_support = _merge_metric_support_with_trade_stats(metric_support, trade_stats)

    row = {
        "row_type": "lane",
        "strategy_key": strategy_key,
        "lane_id": raw_lane_id,
        "display_name": str(lane.get("display_name") or lane.get("strategy_label") or strategy_key).strip() or raw_lane_id,
        "family": family,
        "instrument": instrument,
        "session_scope": session_scope,
        "session_scope_label": " / ".join(session_scope),
        "strategy_class": strategy_class,
        "evidence_lane_type": evidence_lane_type,
        "evidence_subtype": lane_type,
        "lifecycle_truth_class": str((lane.get("lifecycle_truth") or {}).get("class") or "").strip() or None,
        "enabled": _derive_enabled(lane, runtime_row),
        "runtime_attached": runtime_attached,
        "current_status": current_status,
        "position_side": position_state,
        "status": {
            "runtime_health": runtime_health,
            "position_state": position_state,
            "freshness_state": freshness["state"],
            "fault_present": runtime_health == "faulted",
        },
        "group_values": {
            "strategy_class": strategy_class,
            "instrument": instrument or "UNKNOWN",
            "family": family or "UNKNOWN",
            "evidence_lane_type": evidence_lane_type,
            "session_scope": " / ".join(session_scope),
        },
        "metrics": {
            "realized_pnl": _metric_object(metric_values.get("realized_pnl"), metric_support["realized_pnl"]),
            "open_pnl": _metric_object(metric_values.get("open_pnl"), metric_support["open_pnl"]),
            "net_pnl": _metric_object(metric_values.get("net_pnl"), metric_support["net_pnl"]),
            "trade_count": _metric_object(metric_values.get("trade_count"), metric_support["trade_count"]),
            "win_rate": _metric_object(metric_values.get("win_rate"), metric_support["win_rate"]),
            "average_trade": _metric_object(metric_values.get("average_trade"), metric_support["average_trade"]),
            "max_drawdown": _metric_object(metric_values.get("max_drawdown"), metric_support["max_drawdown"]),
            "profit_factor": _metric_object(metric_values.get("profit_factor"), metric_support["profit_factor"]),
        },
        "realized_pnl": metric_values.get("realized_pnl"),
        "open_pnl": metric_values.get("open_pnl"),
        "net_pnl": metric_values.get("net_pnl"),
        "trade_count": metric_values.get("trade_count"),
        "win_rate": metric_values.get("win_rate"),
        "average_trade": metric_values.get("average_trade"),
        "max_drawdown": metric_values.get("max_drawdown"),
        "profit_factor": metric_values.get("profit_factor"),
        "winner_count": trade_stats.get("winner_count") if trade_stats.get("winner_count") is not None else _metric_number(lane, "winners"),
        "loser_count": trade_stats.get("loser_count") if trade_stats.get("loser_count") is not None else _metric_number(lane, "losers"),
        "gross_win_pnl": trade_stats.get("gross_win_pnl"),
        "gross_loss_pnl_abs": trade_stats.get("gross_loss_pnl_abs"),
        "latest_trade_summary": latest_trade_summary,
        "latest_update_timestamp": freshness["latest_update_timestamp"],
        "freshness": freshness,
        "stale": freshness["stale"],
        "blocking_gap_note": _blocking_gap_note(metric_support),
        "metric_support": metric_support,
    }
    detail_payload = _detail_view_for_lane(
        lane=lane,
        strategy_class=strategy_class,
        evidence_lane_type=evidence_lane_type,
        family=family,
        instrument=instrument,
        session_scope=session_scope,
        metric_support=metric_support,
        latest_trade_summary=latest_trade_summary,
    )
    chart_payload["meta"]["strategy_class"] = strategy_class
    return row, chart_payload, detail_payload


def _build_tracked_audit_rows(
    *,
    tracked_rows: dict[str, dict[str, Any]],
    tracked_details: dict[str, dict[str, Any]],
    generated_at: str,
) -> list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]:
    rows: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for strategy_id, summary in tracked_rows.items():
        detail = tracked_details.get(strategy_id, {})
        instrument = next((str(value).strip() for value in list(summary.get("observed_instruments") or []) if str(value).strip()), None)
        family = str(summary.get("strategy_family") or "").strip() or None
        session_scope = _derive_session_scope_from_text(
            str(summary.get("display_name") or strategy_id),
            fallback=[str(summary.get("current_session_segment") or "").strip()] if str(summary.get("current_session_segment") or "").strip() else [],
        )
        metric_support = {
            "realized_pnl": _support(summary.get("realized_pnl") is not None),
            "open_pnl": _support(summary.get("open_pnl") is not None, "Tracked audit row does not currently publish live mark/open-position truth."),
            "net_pnl": _support(summary.get("cumulative_pnl") is not None),
            "trade_count": _support(summary.get("trade_count") is not None),
            "win_rate": _support(summary.get("win_rate") is not None, "Tracked audit win rate is only available when the tracked summary publishes outcome counts."),
            "average_trade": _support(summary.get("average_trade_pnl") is not None, "Tracked audit average trade is unavailable because the tracked summary omitted it."),
            "max_drawdown": _support(summary.get("max_drawdown") is not None, "Tracked audit max drawdown is unavailable because the tracked summary omitted it."),
            "profit_factor": _support(summary.get("profit_factor") is not None, "Tracked audit profit factor is unavailable because the tracked summary omitted it."),
        }
        comparison_row = {
            "row_type": "lane",
            "strategy_key": str(strategy_id),
            "lane_id": f"tracked_audit:{strategy_id}",
            "display_name": str(summary.get("display_name") or strategy_id),
            "family": family,
            "instrument": instrument,
            "session_scope": session_scope,
            "session_scope_label": " / ".join(session_scope),
            "strategy_class": STRATEGY_CLASS_ATP if _is_atp_family(family, str(summary.get("display_name") or strategy_id)) else STRATEGY_CLASS_TEMPORARY_PAPER,
            "evidence_lane_type": EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
            "evidence_subtype": "tracked_paper_summary",
            "lifecycle_truth_class": "FULL_LIFECYCLE_TRUTH",
            "enabled": bool(summary.get("enabled")),
            "runtime_attached": bool(summary.get("runtime_attached")),
            "current_status": str(summary.get("status") or "").strip() or None,
            "position_side": _normalize_position_side(summary.get("position_side")),
            "status": {
                "runtime_health": _derive_runtime_health_from_status(summary),
                "position_state": _normalize_position_side(summary.get("position_side")),
                "freshness_state": "stale" if summary.get("data_stale") else "fresh",
                "fault_present": False,
            },
            "group_values": {
                "strategy_class": STRATEGY_CLASS_ATP if _is_atp_family(family, str(summary.get("display_name") or strategy_id)) else STRATEGY_CLASS_TEMPORARY_PAPER,
                "instrument": instrument or "UNKNOWN",
                "family": family or "UNKNOWN",
                "evidence_lane_type": EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
                "session_scope": " / ".join(session_scope),
            },
            "metrics": {
                "realized_pnl": _metric_object(_safe_float(summary.get("realized_pnl")), metric_support["realized_pnl"]),
                "open_pnl": _metric_object(_safe_float(summary.get("open_pnl")), metric_support["open_pnl"]),
                "net_pnl": _metric_object(_safe_float(summary.get("cumulative_pnl")), metric_support["net_pnl"]),
                "trade_count": _metric_object(_safe_int(summary.get("trade_count")), metric_support["trade_count"]),
                "win_rate": _metric_object(_safe_float(summary.get("win_rate")), metric_support["win_rate"]),
                "average_trade": _metric_object(_safe_float(summary.get("average_trade_pnl")), metric_support["average_trade"]),
                "max_drawdown": _metric_object(_safe_float(summary.get("max_drawdown")), metric_support["max_drawdown"]),
                "profit_factor": _metric_object(_safe_float(summary.get("profit_factor")), metric_support["profit_factor"]),
            },
            "realized_pnl": _safe_float(summary.get("realized_pnl")),
            "open_pnl": _safe_float(summary.get("open_pnl")),
            "net_pnl": _safe_float(summary.get("cumulative_pnl")),
            "trade_count": _safe_int(summary.get("trade_count")),
            "win_rate": _safe_float(summary.get("win_rate")),
            "average_trade": _safe_float(summary.get("average_trade_pnl")),
            "max_drawdown": _safe_float(summary.get("max_drawdown")),
            "profit_factor": _safe_float(summary.get("profit_factor")),
            "winner_count": _safe_int(summary.get("winner_count")),
            "loser_count": _safe_int(summary.get("loser_count")),
            "gross_win_pnl": None,
            "gross_loss_pnl_abs": None,
            "latest_trade_summary": _tracked_latest_trade_summary_label(summary),
            "latest_update_timestamp": str(summary.get("last_update_timestamp") or generated_at),
            "freshness": {
                "state": "stale" if summary.get("data_stale") else "fresh",
                "stale": bool(summary.get("data_stale")),
                "age_seconds": None,
                "latest_update_timestamp": str(summary.get("last_update_timestamp") or generated_at),
            },
            "stale": bool(summary.get("data_stale")),
            "blocking_gap_note": _blocking_gap_note(metric_support),
            "metric_support": metric_support,
        }
        chart_payload = {
            "meta": {
                "strategy_key": strategy_id,
                "display_name": str(summary.get("display_name") or strategy_id),
                "strategy_class": comparison_row["strategy_class"],
                "evidence_lane_type": EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
                "instrument": instrument,
                "session_scope": session_scope,
            },
            "curves": {
                "cumulative_realized_pnl": [],
                "normalized_equity_indexed_zero": [],
                "drawdown": [],
                "cumulative_trade_count": [],
                "rolling_average_trade": [],
            },
            "support": {
                "cumulative_realized_pnl": False,
                "normalized_equity_indexed_zero": False,
                "drawdown": False,
                "cumulative_trade_count": False,
                "rolling_average_trade": False,
            },
            "aggregate_inputs": {
                "winner_count": _safe_int(summary.get("winner_count")),
                "loser_count": _safe_int(summary.get("loser_count")),
                "gross_win_pnl": None,
                "gross_loss_pnl_abs": None,
            },
            "support_gaps": ["Tracked audit rows are summary/audit views and do not publish full historical curves in the shared contract."],
        }
        detail_payload = {
            "identity": {
                "strategy_key": strategy_id,
                "lane_id": comparison_row["lane_id"],
                "display_name": comparison_row["display_name"],
            },
            "classification": {
                "strategy_class": comparison_row["strategy_class"],
                "evidence_lane_type": EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
                "family": family,
                "instrument": instrument,
                "session_scope": session_scope,
            },
            "provenance": {
                "note": "Tracked audit rows are visible secondary read-models and not primary runtime economics.",
            },
            "recent_signals": list(detail.get("recent_signals") or [])[:12],
            "recent_order_intents": list(detail.get("recent_order_intents") or [])[:12],
            "recent_fills": list(detail.get("recent_fills") or [])[:12],
            "recent_state_snapshots": list(detail.get("recent_state_snapshots") or [])[:12],
            "recent_trade_summaries": [summary.get("last_trade_summary")] if summary.get("last_trade_summary") else [],
            "metric_support": metric_support,
            "readiness_refs": [],
            "audit_refs": list(detail.get("artifacts") or []),
        }
        rows.append((comparison_row, chart_payload, detail_payload))
    return rows


def _group_rows_by_key(*, rows: Sequence[dict[str, Any]], keys: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = next((str(row.get(candidate) or "").strip() for candidate in keys if str(row.get(candidate) or "").strip()), "")
        if key:
            grouped[key].append(dict(row))
    return grouped


def _build_group_tree(
    *,
    rows: Sequence[dict[str, Any]],
    group_keys: Sequence[str],
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    group_selection_map: dict[str, list[str]] = {}

    def build_level(level_rows: Sequence[dict[str, Any]], depth: int, parent_group_id: str | None) -> list[dict[str, Any]]:
        if depth >= len(group_keys):
            return []
        group_key = group_keys[depth]
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in level_rows:
            value = str((row.get("group_values") or {}).get(group_key) or "UNKNOWN")
            buckets[value].append(row)
        nodes: list[dict[str, Any]] = []
        for group_value, bucket_rows in sorted(buckets.items(), key=lambda item: str(item[0])):
            group_id = f"{group_key}:{group_value}" if parent_group_id is None else f"{parent_group_id}|{group_key}:{group_value}"
            child_nodes = build_level(bucket_rows, depth + 1, group_id)
            child_lane_ids = [str(row.get("lane_id") or "") for row in bucket_rows if str(row.get("lane_id") or "")]
            subtotal_metrics = _aggregate_metric_bundle(bucket_rows)
            status_summary = {
                "stale_count": sum(1 for row in bucket_rows if bool(row.get("stale"))),
                "fault_count": sum(1 for row in bucket_rows if bool(((row.get("status") or {}).get("fault_present")))),
                "open_position_count": sum(1 for row in bucket_rows if str(row.get("position_side") or "FLAT") not in {"", "FLAT", "UNKNOWN"}),
            }
            node = {
                "group_id": group_id,
                "group_key": group_key,
                "group_value": group_value,
                "label": group_value,
                "depth": depth,
                "parent_group_id": parent_group_id,
                "child_group_ids": [str(child.get("group_id") or "") for child in child_nodes],
                "child_lane_ids": child_lane_ids,
                "visible_lane_count": len(child_lane_ids),
                "default_selected_lane_ids": child_lane_ids,
                "selected_child_count": len(child_lane_ids),
                "status_summary": status_summary,
                "subtotal_metrics": subtotal_metrics,
            }
            group_selection_map[group_id] = child_lane_ids
            nodes.append(node)
            nodes.extend(child_nodes)
        return nodes

    return build_level(rows, 0, None), group_selection_map


def _aggregate_metric_bundle(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    realized_values = [row.get("realized_pnl") for row in rows if row.get("realized_pnl") is not None]
    open_values = [row.get("open_pnl") for row in rows if row.get("open_pnl") is not None]
    trade_values = [int(row.get("trade_count") or 0) for row in rows if row.get("trade_count") is not None]
    winner_count = sum(int(row.get("winner_count") or 0) for row in rows if row.get("winner_count") is not None)
    loser_count = sum(int(row.get("loser_count") or 0) for row in rows if row.get("loser_count") is not None)
    total_trades = sum(trade_values)
    gross_win_values = [row.get("gross_win_pnl") for row in rows if row.get("gross_win_pnl") is not None]
    gross_loss_values = [row.get("gross_loss_pnl_abs") for row in rows if row.get("gross_loss_pnl_abs") is not None]
    can_sum_open = len(open_values) == len(rows) and bool(rows)
    average_trade = (sum(realized_values) / total_trades) if realized_values and total_trades else None
    win_rate = ((winner_count / total_trades) * 100.0) if total_trades and (winner_count or loser_count) else None
    profit_factor = None
    if gross_win_values and gross_loss_values and len(gross_win_values) == len(rows) and len(gross_loss_values) == len(rows):
        gross_win = sum(gross_win_values)
        gross_loss = sum(gross_loss_values)
        if gross_loss > 0:
            profit_factor = gross_win / gross_loss
        elif gross_win > 0:
            profit_factor = 999.0
    return {
        "realized_pnl": {"value": sum(realized_values) if realized_values else 0.0, "supported": True, "rule": "sum"},
        "open_pnl": {
            "value": sum(open_values) if can_sum_open else None,
            "supported": can_sum_open,
            "rule": "conditional_sum",
        },
        "net_pnl": {
            "value": (sum(realized_values) if realized_values else 0.0) + (sum(open_values) if can_sum_open else 0.0),
            "supported": True,
            "rule": "sum",
        },
        "trade_count": {"value": total_trades, "supported": True, "rule": "sum"},
        "win_rate": {
            "value": win_rate,
            "supported": win_rate is not None,
            "rule": "recompute_from_trade_counts",
        },
        "average_trade": {
            "value": average_trade,
            "supported": average_trade is not None,
            "rule": "recompute_from_realized_and_trade_count",
        },
        "max_drawdown": {
            "value": None,
            "supported": False,
            "rule": "recompute_from_aggregate_curve",
        },
        "profit_factor": {
            "value": profit_factor,
            "supported": profit_factor is not None,
            "rule": "recompute_from_gross_win_loss",
        },
    }


def _build_leaderboard_views(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    def sort_rows(field: str, *, reverse: bool) -> list[dict[str, Any]]:
        def key(row: dict[str, Any]) -> tuple[int, float | str]:
            value = row.get(field)
            if value is None:
                return (1, "" if not reverse else -float("inf"))
            return (0, float(value) if isinstance(value, (int, float)) else str(value))

        return sorted(rows, key=key, reverse=reverse)[:25]

    freshness_rank = sorted(
        rows,
        key=lambda row: (
            0 if not bool(row.get("stale")) else 1,
            str(row.get("latest_update_timestamp") or ""),
            str(row.get("display_name") or ""),
        ),
    )[:25]
    fault_rank = sorted(
        rows,
        key=lambda row: (
            0 if bool(((row.get("status") or {}).get("fault_present"))) else 1,
            str(row.get("display_name") or ""),
        ),
    )[:25]
    return {
        "rankings": {
            "realized_pnl": sort_rows("realized_pnl", reverse=True),
            "net_pnl": sort_rows("net_pnl", reverse=True),
            "trade_count": sort_rows("trade_count", reverse=True),
            "freshness": freshness_rank,
            "fault_presence": fault_rank,
            "max_drawdown": sort_rows("max_drawdown", reverse=False),
        }
    }


def _build_contribution_views(grouped_rows: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    trees: dict[str, list[dict[str, Any]]] = {}
    for dimension, rows in grouped_rows.items():
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            value = str((row.get("group_values") or {}).get(dimension) or "UNKNOWN")
            buckets[value].append(row)
        trees[dimension] = [
            {
                "group_value": group_value,
                "lane_ids": [str(row.get("lane_id") or "") for row in bucket_rows if str(row.get("lane_id") or "")],
                "metrics": _aggregate_metric_bundle(bucket_rows),
            }
            for group_value, bucket_rows in sorted(buckets.items(), key=lambda item: str(item[0]))
        ]
    return {
        "dimensions": ["strategy_class", "instrument", "family", "evidence_lane_type", "session_scope"],
        "trees": trees,
    }


def _build_data_quality_report(
    *,
    comparison_rows: Sequence[dict[str, Any]],
    metric_support_map: dict[str, dict[str, Any]],
    identity_exceptions: dict[str, Any],
) -> dict[str, Any]:
    stale_lanes = [row.get("lane_id") for row in comparison_rows if bool(row.get("stale"))]
    metric_missing: dict[str, list[str]] = defaultdict(list)
    for lane_id, support_map in metric_support_map.items():
        for metric_name, support in support_map.items():
            if dict(support).get("supported") is not True:
                metric_missing[metric_name].append(lane_id)
    return {
        "metric_support_by_lane": metric_support_map,
        "stale_lanes": stale_lanes,
        "identity_conflicts": list(identity_exceptions.get("possible_duplicates") or []) + list(identity_exceptions.get("naming_mismatches") or []),
        "normalization_gaps": list(identity_exceptions.get("incomplete_mapping_candidates") or []) + list(identity_exceptions.get("low_confidence_strategy_keys") or []),
        "missing_metric_support": dict(metric_missing),
    }


def _build_identity_exceptions(
    identity_candidates: Sequence[dict[str, Any]],
    comparison_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    by_name_instrument: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    by_strategy_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in identity_candidates:
        key = (
            str(candidate.get("display_name") or "").strip().lower(),
            str(candidate.get("instrument") or "").strip().upper(),
        )
        if key[0]:
            by_name_instrument[key].append(candidate)
        strategy_key = str(candidate.get("strategy_key") or "").strip()
        if strategy_key:
            by_strategy_key[strategy_key].append(candidate)
    possible_duplicates = [
        {
            "display_name": display_name,
            "instrument": instrument,
            "lane_ids": sorted({str(row.get("lane_id") or "") for row in rows if str(row.get("lane_id") or "")}),
            "strategy_keys": sorted({str(row.get("strategy_key") or "") for row in rows if str(row.get("strategy_key") or "")}),
        }
        for (display_name, instrument), rows in by_name_instrument.items()
        if len({str(row.get("strategy_key") or "") for row in rows if str(row.get("strategy_key") or "")}) > 1
    ]
    naming_mismatches = []
    for strategy_key, rows in by_strategy_key.items():
        display_names = sorted({str(row.get("display_name") or "") for row in rows if str(row.get("display_name") or "")})
        if len(display_names) <= 1:
            continue
        evidence_lane_types = sorted({str(row.get("evidence_lane_type") or "") for row in rows if str(row.get("evidence_lane_type") or "")})
        families = sorted({str(row.get("family") or "") for row in rows if str(row.get("family") or "")})
        # Multiple lane labels inside a single runtime package are expected and not actionable.
        # Keep this report focused on conflicts that span provenance families or normalization buckets.
        if len(evidence_lane_types) <= 1 and len(families) <= 1:
            continue
        naming_mismatches.append(
            {
                "strategy_key": strategy_key,
                "display_names": display_names,
                "lane_ids": sorted({str(row.get("lane_id") or "") for row in rows if str(row.get("lane_id") or "")}),
                "evidence_lane_types": evidence_lane_types,
                "families": families,
            }
        )
    incomplete_mapping_candidates = [
        {
            "lane_id": row.get("lane_id"),
            "display_name": row.get("display_name"),
            "family": row.get("family"),
            "instrument": row.get("instrument"),
        }
        for row in comparison_rows
        if not row.get("family") or not row.get("instrument")
    ]
    low_confidence_strategy_keys = [
        {
            "strategy_key": row.get("strategy_key"),
            "lane_id": row.get("lane_id"),
            "reason": "Strategy key lacks family or instrument context for confident normalization.",
        }
        for row in comparison_rows
        if _strategy_key_lacks_confident_normalization_context(row)
    ]
    return {
        "possible_duplicates": possible_duplicates,
        "naming_mismatches": naming_mismatches,
        "incomplete_mapping_candidates": incomplete_mapping_candidates,
        "low_confidence_strategy_keys": low_confidence_strategy_keys,
    }


def _strategy_key_lacks_confident_normalization_context(row: dict[str, Any]) -> bool:
    strategy_key = str(row.get("strategy_key") or "").strip()
    if not strategy_key:
        return True
    if "::" in strategy_key or "__" in strategy_key:
        return False
    key_lower = strategy_key.lower()
    instrument = str(row.get("instrument") or "").strip().lower()
    family = str(row.get("family") or "").strip().lower()
    instrument_scoped = bool(
        instrument
        and (
            f"::{instrument}" in key_lower
            or f"__{instrument}" in key_lower
            or key_lower.endswith(f"_{instrument}")
            or key_lower.endswith(instrument)
        )
    )
    family_scoped = bool(family and family in key_lower)
    segmented = "::" in strategy_key or "__" in strategy_key
    # Treat segmented keys with an explicit instrument token as confident enough for
    # first-pass operator review, even if the family naming differs across evidence
    # sources. Reserve the low-confidence bucket for keys that are genuinely hard to
    # align without extra human interpretation.
    if segmented and instrument_scoped:
        return False
    if segmented and family_scoped:
        return False
    return True


def _aggregate_rules_payload() -> dict[str, Any]:
    return {
        "metric_rules": {
            "realized_pnl": {"mode": "additive", "group_rule": "sum"},
            "open_pnl": {"mode": "conditional_additive", "group_rule": "sum_if_all_supported"},
            "net_pnl": {"mode": "additive", "group_rule": "sum"},
            "trade_count": {"mode": "additive", "group_rule": "sum"},
            "win_rate": {"mode": "recomputed", "group_rule": "winner_count_over_trade_count"},
            "average_trade": {"mode": "recomputed", "group_rule": "realized_pnl_over_trade_count"},
            "max_drawdown": {"mode": "recomputed", "group_rule": "aggregate_curve_drawdown_only"},
            "profit_factor": {"mode": "recomputed", "group_rule": "gross_win_over_gross_loss"},
        },
        "chart_rules": {
            "overlay": "Always provenance-safe because selected lanes remain distinct.",
            "aggregate": "Allowed only within a single compatible evidence bucket and only for mathematically valid curves.",
            "small_multiples": "Derived from the same selected lanes as table and overlay mode.",
        },
        "compatible_evidence_buckets": list(COMPATIBLE_EVIDENCE_BUCKETS),
    }


def _detail_view_for_lane(
    *,
    lane: dict[str, Any],
    strategy_class: str,
    evidence_lane_type: str,
    family: str | None,
    instrument: str | None,
    session_scope: Sequence[str],
    metric_support: dict[str, Any],
    latest_trade_summary: str | None,
) -> dict[str, Any]:
    evidence = dict(lane.get("evidence") or {})
    return {
        "identity": {
            "strategy_key": lane.get("strategy_key"),
            "lane_id": lane.get("lane_id"),
            "display_name": lane.get("display_name") or lane.get("strategy_label") or lane.get("strategy_key"),
        },
        "classification": {
            "strategy_class": strategy_class,
            "evidence_lane_type": evidence_lane_type,
            "family": family,
            "instrument": instrument,
            "session_scope": list(session_scope),
            "lifecycle_truth_class": (lane.get("lifecycle_truth") or {}).get("class"),
        },
        "provenance": {
            "source_of_truth": dict(lane.get("source_of_truth") or {}),
            "provenance": dict(lane.get("provenance") or {}),
            "mode_truth": dict(lane.get("mode_truth") or {}),
            "timeframe_truth": dict(lane.get("timeframe_truth") or {}),
        },
        "recent_signals": list(((evidence.get("signals") or {}).get("preview_rows") or []))[:12],
        "recent_order_intents": list(((evidence.get("order_intents") or {}).get("preview_rows") or []))[:12],
        "recent_fills": list(((evidence.get("fills") or {}).get("preview_rows") or []))[:12],
        "recent_state_snapshots": list(((evidence.get("state_snapshots") or {}).get("preview_rows") or []))[:12],
        "recent_trade_summaries": [latest_trade_summary] if latest_trade_summary else [],
        "curve_view": {
            "lane_id": lane.get("lane_id"),
        },
        "metric_support": metric_support,
        "readiness_refs": list(((evidence.get("readiness_artifacts") or {}).get("preview_rows") or []))[:12],
        "audit_refs": list(((evidence.get("trade_lifecycle") or {}).get("preview_rows") or []))[:12],
    }


def _chart_series_for_lane(
    *,
    lane: dict[str, Any],
    evidence_lane_type: str,
    paper_trade_rows_by_strategy: dict[str, list[dict[str, Any]]],
    paper_trade_rows_by_lane: dict[str, list[dict[str, Any]]],
    playback_items_by_study_key: dict[str, dict[str, Any]],
    research_summary_rows: dict[str, dict[str, Any]],
    research_equity_rows_by_key: dict[str, list[dict[str, Any]]],
    research_trade_rows_by_key: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    strategy_key = str(lane.get("strategy_key") or "").strip()
    lane_id = str(lane.get("lane_id") or "").strip()
    curves = {
        "cumulative_realized_pnl": [],
        "normalized_equity_indexed_zero": [],
        "drawdown": [],
        "cumulative_trade_count": [],
        "rolling_average_trade": [],
    }
    support = {
        "cumulative_realized_pnl": False,
        "normalized_equity_indexed_zero": False,
        "drawdown": False,
        "cumulative_trade_count": False,
        "rolling_average_trade": False,
    }
    aggregate_inputs = {
        "winner_count": None,
        "loser_count": None,
        "gross_win_pnl": None,
        "gross_loss_pnl_abs": None,
    }
    support_gaps: list[str] = []

    if evidence_lane_type == EVIDENCE_LANE_TYPE_PAPER_RUNTIME:
        paper_lane_id = str(lane.get("paper_lane_id") or "").strip()
        trade_rows = [
            dict(row)
            for row in (
                paper_trade_rows_by_lane.get(paper_lane_id)
                or paper_trade_rows_by_strategy.get(strategy_key)
                or []
            )
        ]
        curves = _chart_curves_from_paper_trades(trade_rows)
        support.update(_curve_support_from_curves(curves))
        aggregate_inputs.update(_trade_stats_from_paper_trades(trade_rows))
        if not trade_rows:
            support_gaps.append("Paper runtime lane did not embed full trade-log rows in the shared lane payload.")
        return {
            "meta": _chart_meta(lane, evidence_lane_type),
            "curves": curves,
            "support": support,
            "aggregate_inputs": aggregate_inputs,
            "support_gaps": support_gaps,
        }
    elif evidence_lane_type == EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION:
        if str(lane.get("evidence_subtype") or lane.get("lane_type") or "").strip() == "research_analytics":
            equity_rows = research_equity_rows_by_key.get(strategy_key, [])
            trade_rows = research_trade_rows_by_key.get(strategy_key, [])
            curves = _chart_curves_from_research_rows(equity_rows=equity_rows, trade_rows=trade_rows)
            support.update(_curve_support_from_curves(curves))
            aggregate_inputs.update(_trade_stats_from_research_trades(trade_rows))
            return {
                "meta": _chart_meta(lane, evidence_lane_type),
                "curves": curves,
                "support": support,
                "aggregate_inputs": aggregate_inputs,
                "support_gaps": support_gaps,
            }
        study_key = str(((lane.get("run_identity") or {}).get("study_key")) or "").strip()
        study_item = playback_items_by_study_key.get(study_key, {})
        study = dict(study_item.get("study") or {})
        curves = _chart_curves_from_replay_study(study)
        support.update(_curve_support_from_curves(curves))
        aggregate_inputs.update(_trade_stats_from_replay_study(study))
        if not support["cumulative_realized_pnl"]:
            support_gaps.append("Replay/research-execution study did not publish chartable pnl_points or reconstructable trade rows.")
        return {
            "meta": _chart_meta(lane, evidence_lane_type),
            "curves": curves,
            "support": support,
            "aggregate_inputs": aggregate_inputs,
            "support_gaps": support_gaps,
        }
    elif evidence_lane_type == EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY:
        study_key = str(((lane.get("run_identity") or {}).get("study_key")) or "").strip()
        study_item = playback_items_by_study_key.get(study_key, {})
        study = dict(study_item.get("study") or {})
        curves = _chart_curves_from_replay_study(study)
        support.update(_curve_support_from_curves(curves))
        aggregate_inputs.update(_trade_stats_from_replay_study(study))
        if not support["cumulative_realized_pnl"]:
            support_gaps.append("Replay study did not publish chartable pnl_points or reconstructable trade rows.")
    else:
        support_gaps.append("Tracked-audit rows do not publish full historical curves in the shared contract.")

    return {
        "meta": _chart_meta(lane, evidence_lane_type),
        "curves": curves,
        "support": support,
        "aggregate_inputs": aggregate_inputs,
        "support_gaps": support_gaps,
    }


def _chart_curves_from_research_rows(
    *,
    equity_rows: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    normalized = []
    drawdown = []
    cumulative_trade_count = []
    rolling_average_trade = []
    equity_points: list[dict[str, Any]] = []
    peak = None
    base = None
    for row in sorted(equity_rows, key=lambda item: str(item.get("timestamp") or "")):
        ts = str(row.get("timestamp") or "").strip()
        value = _safe_float(row.get("equity_pnl_cash"))
        if not ts or value is None:
            continue
        base = value if base is None else base
        peak = value if peak is None else max(peak, value)
        equity_points.append({"ts": ts, "value": value})
        normalized.append({"ts": ts, "value": value - (base or 0.0)})
        drawdown.append({"ts": ts, "value": value - (peak or value)})
    cumulative_trade_count = _trade_count_curve_from_trade_rows(trade_rows)
    rolling_average_trade = _rolling_average_trade_curve(trade_rows)
    return {
        "cumulative_realized_pnl": equity_points,
        "normalized_equity_indexed_zero": normalized,
        "drawdown": drawdown,
        "cumulative_trade_count": cumulative_trade_count,
        "rolling_average_trade": rolling_average_trade,
    }


def _chart_curves_from_replay_study(study: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    pnl_points = list(study.get("pnl_points") or [])
    if pnl_points:
        cumulative = []
        normalized = []
        drawdown = []
        peak = None
        base = None
        for row in pnl_points:
            ts = str(row.get("timestamp") or row.get("event_timestamp") or "").strip()
            value = _safe_float(row.get("realized"))
            if value is None:
                value = _safe_float(row.get("total"))
            if not ts or value is None:
                continue
            base = value if base is None else base
            peak = value if peak is None else max(peak, value)
            cumulative.append({"ts": ts, "value": value})
            normalized.append({"ts": ts, "value": value - (base or 0.0)})
            drawdown.append({"ts": ts, "value": value - (peak or value)})
        trade_events = list(study.get("trade_events") or [])
        trade_rows = [row for row in trade_events if "EXIT" in str(row.get("event_type") or "").upper()]
        return {
            "cumulative_realized_pnl": cumulative,
            "normalized_equity_indexed_zero": normalized,
            "drawdown": drawdown,
            "cumulative_trade_count": _trade_count_curve_from_trade_rows(trade_rows, timestamp_key="event_timestamp"),
            "rolling_average_trade": [],
        }
    trade_events = list(study.get("trade_events") or [])
    closed_events = [row for row in trade_events if "EXIT" in str(row.get("event_type") or "").upper()]
    cumulative = []
    normalized = []
    drawdown = []
    peak = None
    base = None
    running = 0.0
    for row in sorted(closed_events, key=lambda item: str(item.get("event_timestamp") or "")):
        ts = str(row.get("event_timestamp") or "").strip()
        pnl = _safe_float(row.get("realized_pnl"))
        if not ts or pnl is None:
            continue
        running += pnl
        base = running if base is None else base
        peak = running if peak is None else max(peak, running)
        cumulative.append({"ts": ts, "value": running})
        normalized.append({"ts": ts, "value": running - (base or 0.0)})
        drawdown.append({"ts": ts, "value": running - (peak or running)})
    return {
        "cumulative_realized_pnl": cumulative,
        "normalized_equity_indexed_zero": normalized,
        "drawdown": drawdown,
        "cumulative_trade_count": _trade_count_curve_from_trade_rows(closed_events, timestamp_key="event_timestamp"),
        "rolling_average_trade": [],
    }


def _chart_curves_from_paper_trades(trade_rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    cumulative = []
    normalized = []
    drawdown = []
    peak = None
    base = None
    running = 0.0
    closed_rows = []
    for row in sorted(
        trade_rows,
        key=lambda item: str(item.get("exit_timestamp") or item.get("fill_timestamp") or item.get("entry_timestamp") or ""),
    ):
        if str(row.get("status") or "").strip().upper() not in {"CLOSED", ""} and row.get("realized_pnl") in (None, ""):
            continue
        ts = str(row.get("exit_timestamp") or row.get("fill_timestamp") or row.get("entry_timestamp") or "").strip()
        pnl = _safe_float(row.get("realized_pnl"))
        if pnl is None:
            pnl = _safe_float(row.get("net_pnl"))
        if not ts or pnl is None:
            continue
        running += pnl
        base = running if base is None else base
        peak = running if peak is None else max(peak, running)
        cumulative.append({"ts": ts, "value": running})
        normalized.append({"ts": ts, "value": running - (base or 0.0)})
        drawdown.append({"ts": ts, "value": running - (peak or running)})
        closed_rows.append(dict(row))
    return {
        "cumulative_realized_pnl": cumulative,
        "normalized_equity_indexed_zero": normalized,
        "drawdown": drawdown,
        "cumulative_trade_count": _trade_count_curve_from_trade_rows(closed_rows),
        "rolling_average_trade": _rolling_average_trade_curve(closed_rows),
    }


def _trade_count_curve_from_trade_rows(
    trade_rows: Sequence[dict[str, Any]],
    *,
    timestamp_key: str | None = None,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    running = 0
    for row in sorted(trade_rows, key=lambda item: str(item.get(timestamp_key or "exit_timestamp") or item.get("entry_timestamp") or "")):
        ts = str(row.get(timestamp_key or "exit_timestamp") or row.get("entry_timestamp") or "").strip()
        if not ts:
            continue
        running += 1
        points.append({"ts": ts, "value": running})
    return points


def _rolling_average_trade_curve(trade_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = []
    for row in sorted(trade_rows, key=lambda item: str(item.get("exit_ts") or item.get("exit_timestamp") or item.get("entry_ts") or "")):
        ts = str(row.get("exit_ts") or row.get("exit_timestamp") or row.get("entry_ts") or "").strip()
        pnl = _safe_float(row.get("pnl_cash"))
        if pnl is None:
            pnl = _safe_float(row.get("realized_pnl"))
        if not ts or pnl is None:
            continue
        ordered.append((ts, pnl))
    points: list[dict[str, Any]] = []
    if len(ordered) < 2:
        return points
    for index in range(len(ordered)):
        start = max(0, index - ROLLING_AVERAGE_TRADE_WINDOW + 1)
        window = ordered[start : index + 1]
        if not window:
            continue
        average = sum(value for _, value in window) / len(window)
        points.append({"ts": ordered[index][0], "value": average})
    return points


def _trade_stats_from_research_trades(trade_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = []
    for row in trade_rows:
        value = _safe_float(row.get("pnl_cash"))
        if value is None:
            value = _safe_float(row.get("realized_pnl"))
        if value is not None:
            pnl_values.append(value)
    return _trade_stats_from_pnl_values(pnl_values)


def _trade_stats_from_paper_trades(trade_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = []
    for row in trade_rows:
        if str(row.get("status") or "").strip().upper() not in {"CLOSED", ""} and row.get("realized_pnl") in (None, ""):
            continue
        value = _safe_float(row.get("realized_pnl"))
        if value is None:
            value = _safe_float(row.get("net_pnl"))
        if value is not None:
            pnl_values.append(value)
    return _trade_stats_from_pnl_values(pnl_values)


def _trade_stats_from_replay_study(study: dict[str, Any]) -> dict[str, Any]:
    trade_events = list(study.get("trade_events") or [])
    pnl_values = []
    for row in trade_events:
        if "EXIT" not in str(row.get("event_type") or "").upper():
            continue
        value = _safe_float(row.get("realized_pnl"))
        if value is not None:
            pnl_values.append(value)
    return _trade_stats_from_pnl_values(pnl_values)


def _trade_stats_from_chart_payload(chart_payload: dict[str, Any]) -> dict[str, Any]:
    return dict(chart_payload.get("aggregate_inputs") or {})


def _trade_stats_from_pnl_values(pnl_values: Sequence[float]) -> dict[str, Any]:
    winners = [value for value in pnl_values if value > 0]
    losers = [-value for value in pnl_values if value < 0]
    return {
        "winner_count": len(winners),
        "loser_count": len(losers),
        "gross_win_pnl": sum(winners) if winners else 0.0,
        "gross_loss_pnl_abs": sum(losers) if losers else 0.0,
    }


def _curve_support_from_curves(curves: dict[str, list[dict[str, Any]]]) -> dict[str, bool]:
    return {
        "cumulative_realized_pnl": bool(curves.get("cumulative_realized_pnl")),
        "normalized_equity_indexed_zero": bool(curves.get("normalized_equity_indexed_zero")),
        "drawdown": bool(curves.get("drawdown")),
        "cumulative_trade_count": bool(curves.get("cumulative_trade_count")),
        "rolling_average_trade": bool(curves.get("rolling_average_trade")),
    }


def _rollup_views_for_series(chart_payload: dict[str, Any]) -> dict[str, Any]:
    realized_curve = list(((chart_payload.get("curves") or {}).get("cumulative_realized_pnl") or []))
    trade_curve = list(((chart_payload.get("curves") or {}).get("cumulative_trade_count") or []))
    daily = _rollup_curve_points(realized_curve=realized_curve, trade_curve=trade_curve, frequency="daily")
    weekly = _rollup_curve_points(realized_curve=realized_curve, trade_curve=trade_curve, frequency="weekly")
    return {
        "daily": daily,
        "weekly": weekly,
    }


def _rollup_curve_points(
    *,
    realized_curve: Sequence[dict[str, Any]],
    trade_curve: Sequence[dict[str, Any]],
    frequency: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    prior_realized = 0.0
    realized_points = sorted(realized_curve, key=lambda row: str(row.get("ts") or ""))
    for point in realized_points:
        ts = str(point.get("ts") or "").strip()
        value = _safe_float(point.get("value"))
        if not ts or value is None:
            continue
        bucket_id = _bucket_for_timestamp(ts, frequency=frequency)
        bucket = buckets.setdefault(bucket_id, {"bucket": bucket_id, "realized_pnl": 0.0, "trade_count": 0})
        bucket["realized_pnl"] += value - prior_realized
        prior_realized = value
    prior_trade_count = 0
    trade_points = sorted(trade_curve, key=lambda row: str(row.get("ts") or ""))
    for point in trade_points:
        ts = str(point.get("ts") or "").strip()
        value = _safe_int(point.get("value"))
        if not ts or value is None:
            continue
        bucket_id = _bucket_for_timestamp(ts, frequency=frequency)
        bucket = buckets.setdefault(bucket_id, {"bucket": bucket_id, "realized_pnl": 0.0, "trade_count": 0})
        bucket["trade_count"] += value - prior_trade_count
        prior_trade_count = value
    return [buckets[key] for key in sorted(buckets.keys())]


def _bucket_for_timestamp(timestamp: str, *, frequency: str) -> str:
    dt = _parse_timestamp(timestamp)
    if dt is None:
        return timestamp[:10]
    if frequency == "weekly":
        start = dt - timedelta(days=dt.weekday())
        return start.date().isoformat()
    return dt.date().isoformat()


def _map_evidence_lane_type(lane_type: str) -> str:
    if lane_type == "paper_runtime":
        return EVIDENCE_LANE_TYPE_PAPER_RUNTIME
    if lane_type == "research_execution":
        return EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION
    if lane_type == "research_analytics":
        return EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION
    return EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY


def _derive_strategy_class(
    *,
    lane: dict[str, Any],
    evidence_lane_type: str,
    family: str | None,
    lane_registry_row: dict[str, Any],
    paper_lane_id: str,
    raw_lane_id: str,
    temporary_lane_ids: set[str],
) -> str:
    lane_name = " ".join(
        value
        for value in [
            str(lane.get("display_name") or "").strip(),
            str(lane.get("strategy_key") or "").strip(),
            str(raw_lane_id),
            str(paper_lane_id),
            str(family or ""),
        ]
        if value
    )
    if _is_atp_family(family, lane_name):
        return STRATEGY_CLASS_ATP
    if evidence_lane_type == EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION:
        return STRATEGY_CLASS_RESEARCH_CANDIDATE
    if evidence_lane_type == EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY:
        return STRATEGY_CLASS_LEGACY_BENCHMARK
    if evidence_lane_type == EVIDENCE_LANE_TYPE_TRACKED_AUDIT:
        return STRATEGY_CLASS_TEMPORARY_PAPER
    if paper_lane_id in temporary_lane_ids or raw_lane_id in temporary_lane_ids:
        return STRATEGY_CLASS_TEMPORARY_PAPER
    admission_state = str(lane_registry_row.get("admission_state") or "").strip().lower()
    if admission_state in {"approved_baseline", "admitted_paper"}:
        return STRATEGY_CLASS_APPROVED_PAPER
    return STRATEGY_CLASS_APPROVED_PAPER


def _derive_session_scope(
    *,
    lane: dict[str, Any],
    runtime_row: dict[str, Any],
    lane_registry_row: dict[str, Any],
) -> list[str]:
    allowed_sessions = [str(value).strip() for value in list(runtime_row.get("allowed_sessions") or []) if str(value).strip()]
    if allowed_sessions:
        return allowed_sessions
    scope_allowed = [str(value).strip() for value in list(((lane_registry_row.get("scope") or {}).get("allowed_sessions") or [])) if str(value).strip()]
    if scope_allowed:
        return scope_allowed
    text_sources = [
        str(lane.get("display_name") or ""),
        str(lane.get("strategy_key") or ""),
        str(lane.get("strategy_family") or ""),
        str(lane.get("lane_id") or ""),
    ]
    return _derive_session_scope_from_text(" ".join(text_sources), fallback=[str((lane.get("date_range") or {}).get("session_window") or "").strip()])


def _derive_session_scope_from_text(text: str, fallback: Sequence[str] | None = None) -> list[str]:
    normalized = str(text or "").strip().lower()
    matches: list[str] = []
    mapping = (
        ("session_open", "SESSION_OPEN"),
        ("asia_early", "ASIA_EARLY"),
        ("asia_open", "ASIA_EARLY"),
        ("asia_late", "ASIA_LATE"),
        ("london_early", "LONDON_EARLY"),
        ("london_open", "LONDON_EARLY"),
        ("london_late", "LONDON_LATE"),
        ("us_early", "US_EARLY"),
        ("ny_early", "US_EARLY"),
        ("ny_late", "NY_LATE"),
        ("us_late", "US_LATE"),
        ("us_midday", "US_MIDDAY"),
    )
    for token, label in mapping:
        if token in normalized and label not in matches:
            matches.append(label)
    if matches:
        return matches
    fallback_values = [str(value).strip().upper() for value in list(fallback or []) if str(value).strip()]
    return fallback_values or ["UNKNOWN"]


def _derive_runtime_attached(
    *,
    lane: dict[str, Any],
    runtime_row: dict[str, Any],
    lane_registry_row: dict[str, Any],
) -> bool:
    runtime_health = dict(lane.get("runtime_health") or {})
    if runtime_health.get("attached") is True:
        return True
    if runtime_row.get("runtime_instance_present") is True or lane_registry_row.get("runtime_instance_present") is True:
        return True
    if str(runtime_row.get("runtime_presence") or "").strip().upper() == "ACTIVE_RUNTIME":
        return True
    if str(lane_registry_row.get("runtime_presence") or "").strip().upper() == "ACTIVE_RUNTIME":
        return True
    return False


def _derive_runtime_health(
    lane: dict[str, Any],
    *,
    runtime_row: dict[str, Any],
    lane_registry_row: dict[str, Any],
    runtime_attached: bool,
) -> str:
    runtime_health = dict(lane.get("runtime_health") or {})
    if runtime_health.get("healthy") is True:
        return "healthy"
    if runtime_health.get("reconciling") is True:
        return "degraded"
    runtime_tone = str(
        runtime_row.get("runtime_presence_tone")
        or lane_registry_row.get("runtime_presence_tone")
        or ""
    ).strip().lower()
    if runtime_tone == "good" and runtime_attached:
        return "healthy"
    if runtime_tone in {"warn", "warning"}:
        return "degraded"
    if runtime_tone in {"danger", "bad", "error"}:
        return "faulted"
    if runtime_health.get("attached") is False and not runtime_attached:
        return "unknown"
    if str(runtime_health.get("label") or "").strip().upper() in {"FAULT", "FAULTED"}:
        return "faulted"
    if runtime_health.get("stale") is True:
        return "degraded"
    if runtime_attached and str(runtime_health.get("label") or "").strip().upper() in {"READY", "RUNNING", "ACTIVE"}:
        return "healthy"
    return "unknown"


def _derive_runtime_health_from_status(summary: dict[str, Any]) -> str:
    status = str(summary.get("status") or "").strip().upper()
    if status in {"READY", "IN_POSITION"} and not summary.get("data_stale"):
        return "healthy"
    if status == "RECONCILING":
        return "degraded"
    if status in {"FAULT", "FAULTED"}:
        return "faulted"
    if summary.get("data_stale"):
        return "degraded"
    return "unknown"


def _derive_position_state(lane: dict[str, Any]) -> str:
    latest_status = str(_metric_value_or_none(lane, "latest_status") or "").strip().upper()
    if "LONG" in latest_status:
        return "LONG"
    if "SHORT" in latest_status:
        return "SHORT"
    if latest_status in {"IN_POSITION", "OPEN"}:
        return "OPEN"
    open_pnl_metric = dict(((lane.get("metrics") or {}).get("open_pnl") or {}))
    if open_pnl_metric.get("available") is True:
        open_pnl_value = _safe_float(open_pnl_metric.get("value"))
        if open_pnl_value and abs(open_pnl_value) > 0:
            return "OPEN"
    return "FLAT"


def _derive_freshness(*, lane: dict[str, Any], generated_at: str) -> dict[str, Any]:
    timestamp = _metric_value_or_none(lane, "latest_update_timestamp")
    latest = str(timestamp or "").strip() or generated_at
    generated_at_dt = _parse_timestamp(generated_at)
    latest_dt = _parse_timestamp(latest)
    age_seconds = None
    if generated_at_dt is not None and latest_dt is not None:
        age_seconds = max((generated_at_dt - latest_dt).total_seconds(), 0.0)
    lane_runtime = dict(lane.get("runtime_health") or {})
    lane_type = str(lane.get("lane_type") or "").strip()
    if lane_type in {
        EVIDENCE_LANE_TYPE_BENCHMARK_REPLAY,
        EVIDENCE_LANE_TYPE_RESEARCH_EXECUTION,
        "research_analytics",
        EVIDENCE_LANE_TYPE_TRACKED_AUDIT,
    }:
        return {
            "state": "snapshot",
            "stale": False,
            "age_seconds": age_seconds,
            "latest_update_timestamp": latest,
        }
    stale = bool(lane_runtime.get("stale")) or bool(age_seconds is not None and age_seconds > 3600)
    return {
        "state": "stale" if stale else "fresh",
        "stale": stale,
        "age_seconds": age_seconds,
        "latest_update_timestamp": latest,
    }


def _metric_support_for_lane(lane: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metrics = dict(lane.get("metrics") or {})
    support: dict[str, dict[str, Any]] = {}
    for metric_name in (
        "realized_pnl",
        "open_pnl",
        "net_pnl",
        "trade_count",
        "win_rate",
        "average_trade",
        "max_drawdown",
        "profit_factor",
    ):
        metric = dict(metrics.get(metric_name) or {})
        supported = metric.get("available") is True
        reason = None if supported else str(metric.get("reason") or f"{metric_name} is unsupported for this lane.").strip()
        support[metric_name] = {"supported": supported, "reason": reason}
    return support


def _merge_metric_support_with_trade_stats(
    metric_support: dict[str, dict[str, Any]],
    trade_stats: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    support = {key: dict(value) for key, value in metric_support.items()}
    if trade_stats.get("winner_count") is not None and trade_stats.get("loser_count") is not None:
        support["win_rate"] = {"supported": True, "reason": None}
    if trade_stats.get("gross_win_pnl") is not None and trade_stats.get("gross_loss_pnl_abs") is not None:
        support["profit_factor"] = {"supported": True, "reason": None}
    return support


def _metric_values_for_lane(lane: dict[str, Any]) -> dict[str, Any]:
    return {
        "realized_pnl": _metric_number(lane, "realized_pnl"),
        "open_pnl": _metric_number(lane, "open_pnl"),
        "net_pnl": _metric_number(lane, "net_pnl"),
        "trade_count": _metric_int(lane, "trade_count"),
        "win_rate": _metric_number(lane, "win_rate"),
        "average_trade": _metric_number(lane, "average_trade"),
        "max_drawdown": _metric_number(lane, "max_drawdown"),
        "profit_factor": _metric_number(lane, "profit_factor"),
    }


def _metric_object(value: Any, support: dict[str, Any]) -> dict[str, Any]:
    return {
        "value": value,
        "supported": bool(support.get("supported")),
        "reason": None if support.get("supported") else support.get("reason"),
    }


def _metric_number(lane: dict[str, Any], metric_name: str) -> float | None:
    metric = dict(((lane.get("metrics") or {}).get(metric_name) or {}))
    if metric.get("available") is not True:
        return None
    return _safe_float(metric.get("value"))


def _metric_int(lane: dict[str, Any], metric_name: str) -> int | None:
    metric = dict(((lane.get("metrics") or {}).get(metric_name) or {}))
    if metric.get("available") is not True:
        return None
    return _safe_int(metric.get("value"))


def _metric_value_or_none(lane: dict[str, Any], metric_name: str) -> Any:
    metric = dict(((lane.get("metrics") or {}).get(metric_name) or {}))
    if metric.get("available") is not True:
        return None
    return metric.get("value")


def _data_quality_row(row: dict[str, Any]) -> dict[str, Any]:
    unsupported = [
        {"metric": metric_name, "reason": support.get("reason")}
        for metric_name, support in dict(row.get("metric_support") or {}).items()
        if dict(support).get("supported") is not True
    ]
    return {
        "lane_id": row.get("lane_id"),
        "strategy_key": row.get("strategy_key"),
        "display_name": row.get("display_name"),
        "stale": row.get("stale"),
        "unsupported_metrics": unsupported,
    }


def _blocking_gap_note(metric_support: dict[str, Any]) -> str | None:
    unsupported = [
        f"{metric_name}: {dict(support).get('reason')}"
        for metric_name, support in metric_support.items()
        if dict(support).get("supported") is not True
    ]
    return unsupported[0] if unsupported else None


def _derive_enabled(lane: dict[str, Any], runtime_row: dict[str, Any]) -> bool:
    if runtime_row:
        return bool(runtime_row.get("can_process_bars", runtime_row.get("enabled", True)))
    runtime_health = dict(lane.get("runtime_health") or {})
    if runtime_health.get("attached") is False:
        return False
    return True


def _latest_trade_summary_label(lane: dict[str, Any]) -> str | None:
    metric = dict(((lane.get("metrics") or {}).get("latest_trade_summary") or {}))
    if metric.get("available") is not True:
        return None
    value = dict(metric.get("value") or {})
    parts = [
        str(value.get("family") or value.get("signal_family") or value.get("trade_id") or "").strip(),
        str(value.get("exit_timestamp") or value.get("latest_timestamp") or "").strip(),
        str(value.get("realized_pnl") or "").strip(),
    ]
    filtered = [part for part in parts if part]
    return " | ".join(filtered) if filtered else "Available"


def _tracked_latest_trade_summary_label(summary: dict[str, Any]) -> str | None:
    last_trade = dict(summary.get("last_trade_summary") or {})
    if not last_trade:
        return None
    parts = [
        str(last_trade.get("exit_timestamp") or "").strip(),
        str(last_trade.get("realized_pnl") or "").strip(),
    ]
    filtered = [part for part in parts if part]
    return " | ".join(filtered) if filtered else "Available"


def _support(supported: bool, reason: str | None = None) -> dict[str, Any]:
    return {"supported": bool(supported), "reason": None if supported else reason}


def _is_atp_family(family: str | None, text: str) -> bool:
    family_text = str(family or "").strip().lower()
    normalized = str(text or "").strip().lower()
    return "active_trend_participation_engine" in family_text or "atp" in normalized


def _strategy_class_sort_rank(value: str) -> int:
    order = {
        STRATEGY_CLASS_LEGACY_BENCHMARK: 0,
        STRATEGY_CLASS_APPROVED_PAPER: 1,
        STRATEGY_CLASS_TEMPORARY_PAPER: 2,
        STRATEGY_CLASS_ATP: 3,
        STRATEGY_CLASS_RESEARCH_CANDIDATE: 4,
    }
    return order.get(value, 99)


def _chart_meta(lane: dict[str, Any], evidence_lane_type: str) -> dict[str, Any]:
    return {
        "strategy_key": lane.get("strategy_key"),
        "display_name": lane.get("display_name") or lane.get("strategy_label") or lane.get("strategy_key"),
        "strategy_class": None,
        "evidence_lane_type": evidence_lane_type,
        "instrument": lane.get("instrument"),
        "session_scope": _derive_session_scope_from_text(
            " ".join(
                str(value)
                for value in [
                    lane.get("display_name"),
                    lane.get("strategy_key"),
                    lane.get("strategy_family"),
                ]
                if value
            ),
            fallback=[str((lane.get("date_range") or {}).get("session_window") or "").strip()],
        ),
    }


def _normalize_position_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"LONG", "SHORT", "FLAT", "OPEN"}:
        return text
    return "UNKNOWN"


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
