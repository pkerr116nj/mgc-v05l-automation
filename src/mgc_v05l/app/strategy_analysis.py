"""Unified strategy analysis payloads for replay and paper evidence lanes."""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Sequence

from .session_phase_labels import label_session_phase
from .strategy_identity import build_standalone_strategy_identity

LANE_TYPE_BENCHMARK_REPLAY = "benchmark_replay"
LANE_TYPE_PAPER_RUNTIME = "paper_runtime"
LANE_TYPE_HISTORICAL_PLAYBACK = "historical_playback"
LANE_TYPE_RESEARCH_EXECUTION = "research_execution"
LIFECYCLE_TRUTH_FULL = "FULL_LIFECYCLE_TRUTH"
LIFECYCLE_TRUTH_HYBRID = "HYBRID_ENTRY_BASELINE_EXIT_TRUTH"
LIFECYCLE_TRUTH_BASELINE_ONLY = "BASELINE_ONLY"
LIFECYCLE_TRUTH_UNSUPPORTED = "UNSUPPORTED"
PNL_TRUTH_BASIS_BASELINE = "BASELINE_FILL_TRUTH"
PNL_TRUTH_BASIS_ENRICHED = "ENRICHED_EXECUTION_TRUTH"
PNL_TRUTH_BASIS_HYBRID = "HYBRID_ENTRY_BASELINE_EXIT_TRUTH"
PNL_TRUTH_BASIS_UNSUPPORTED = "UNSUPPORTED_ENTRY_MODEL"
_PREVIEW_ROW_LIMIT = 24


def build_strategy_analysis_payload(
    *,
    historical_playback: dict[str, Any],
    paper: dict[str, Any],
    runtime_registry: dict[str, Any] | None = None,
    lane_registry: dict[str, Any] | None = None,
    generated_at: str,
) -> dict[str, Any]:
    """Build a strategy-centric analysis surface across replay and paper lanes."""
    strategies: dict[str, dict[str, Any]] = {}
    catalog_rows: list[dict[str, Any]] = []
    details_by_strategy_key: dict[str, dict[str, Any]] = {}

    replay_lanes = _replay_strategy_lanes(historical_playback)
    paper_lanes = _paper_strategy_lanes(paper, generated_at=generated_at)
    evidence_lanes = [*replay_lanes, *paper_lanes]
    runtime_registry = dict(runtime_registry or {})
    lane_registry = dict(lane_registry or {})

    for strategy_key, seed in _discover_strategy_catalog_entries(
        evidence_lanes=evidence_lanes,
        historical_playback=historical_playback,
        paper=paper,
        runtime_registry=runtime_registry,
        lane_registry=lane_registry,
    ).items():
        strategies[strategy_key] = {
            "strategy_key": strategy_key,
            "display_name": seed.get("display_name") or strategy_key,
            "instrument": seed.get("instrument"),
            "strategy_family": seed.get("strategy_family"),
            "standalone_strategy_id": seed.get("standalone_strategy_id"),
            "discovery_sources": list(seed.get("discovery_sources") or []),
            "source_types": list(seed.get("source_types") or []),
            "lanes": [],
        }

    for lane in evidence_lanes:
        strategy_key = str(lane.get("strategy_key") or "").strip()
        if not strategy_key:
            continue
        bucket = strategies.setdefault(
            strategy_key,
            {
                "strategy_key": strategy_key,
                "display_name": lane.get("display_name") or lane.get("strategy_label") or strategy_key,
                "instrument": lane.get("instrument"),
                "strategy_family": lane.get("strategy_family"),
                "standalone_strategy_id": lane.get("standalone_strategy_id"),
                "discovery_sources": [],
                "source_types": [],
                "lanes": [],
            },
        )
        bucket["display_name"] = bucket.get("display_name") or lane.get("display_name") or strategy_key
        bucket["instrument"] = bucket.get("instrument") or lane.get("instrument")
        bucket["strategy_family"] = bucket.get("strategy_family") or lane.get("strategy_family")
        bucket["standalone_strategy_id"] = bucket.get("standalone_strategy_id") or lane.get("standalone_strategy_id")
        bucket["discovery_sources"] = sorted(
            {
                *(str(value) for value in list(bucket.get("discovery_sources") or []) if str(value).strip()),
                "evidence_lanes",
            }
        )
        bucket["source_types"] = sorted(
            {
                *(str(value) for value in list(bucket.get("source_types") or []) if str(value).strip()),
                str(lane.get("lane_type") or lane.get("source_lane") or "unknown"),
            }
        )
        bucket["lanes"].append(lane)

    for strategy_key, bucket in strategies.items():
        lanes = list(bucket["lanes"])
        lanes.sort(
            key=lambda row: (
                _lane_priority(str(row.get("lane_type") or "")),
                str(row.get("display_name") or ""),
                str(_nested_get(row, "run_identity", "run_id") or ""),
            )
        )
        comparison_presets = _comparison_presets_for_strategy(strategy_key, lanes)
        default_lane_id = _default_lane_id(lanes)
        lane_presence = Counter(str(lane.get("lane_type") or "unknown") for lane in lanes)
        catalog_rows.append(
            {
                "strategy_key": strategy_key,
                "display_name": bucket.get("display_name") or strategy_key,
                "instrument": bucket.get("instrument"),
                "strategy_family": bucket.get("strategy_family"),
                "standalone_strategy_id": bucket.get("standalone_strategy_id"),
                "lane_count": len(lanes),
                "lane_presence": dict(lane_presence),
                "has_benchmark_replay": lane_presence.get(LANE_TYPE_BENCHMARK_REPLAY, 0) > 0,
                "has_paper_runtime": lane_presence.get(LANE_TYPE_PAPER_RUNTIME, 0) > 0,
                "has_research_execution": lane_presence.get(LANE_TYPE_RESEARCH_EXECUTION, 0) > 0,
                "has_historical_playback": lane_presence.get(LANE_TYPE_HISTORICAL_PLAYBACK, 0) > 0,
                "default_lane_id": default_lane_id,
                "comparison_preset_count": len(comparison_presets),
                "has_data": bool(lanes),
                "discovery_sources": list(bucket.get("discovery_sources") or []),
                "source_types": list(bucket.get("source_types") or []),
                "latest_update_timestamp": _latest_timestamp_value(
                    _nested_get(lane, "metrics", "latest_update_timestamp", "value") for lane in lanes
                ),
            }
        )
        details_by_strategy_key[strategy_key] = {
            "strategy_key": strategy_key,
            "strategy_identity": {
                "strategy_key": strategy_key,
                "display_name": bucket.get("display_name") or strategy_key,
                "instrument": bucket.get("instrument"),
                "strategy_family": bucket.get("strategy_family"),
                "standalone_strategy_id": bucket.get("standalone_strategy_id"),
                "has_data": bool(lanes),
                "discovery_sources": list(bucket.get("discovery_sources") or []),
                "source_types": list(bucket.get("source_types") or []),
            },
            "default_lane_id": default_lane_id,
            "lanes": lanes,
            "comparison_presets": comparison_presets,
        }

    catalog_rows.sort(
        key=lambda row: (
            0 if row.get("has_data") else 1,
            0 if row.get("has_benchmark_replay") and row.get("has_paper_runtime") else 1,
            0 if row.get("has_paper_runtime") else 1,
            str(row.get("display_name") or ""),
        )
    )
    default_strategy_key = catalog_rows[0]["strategy_key"] if catalog_rows else None

    return {
        "generated_at": generated_at,
        "available": bool(catalog_rows),
        "default_strategy_key": default_strategy_key,
        "strategy_count": len(catalog_rows),
        "lane_count": len(replay_lanes) + len(paper_lanes),
        "catalog": {
            "rows": catalog_rows,
            "default_strategy_key": default_strategy_key,
        },
        "details_by_strategy_key": details_by_strategy_key,
        "results_board": _build_results_board_payload(
            strategies=strategies,
            catalog_rows=catalog_rows,
            details_by_strategy_key=details_by_strategy_key,
            evidence_lanes=evidence_lanes,
            runtime_registry=runtime_registry,
            lane_registry=lane_registry,
        ),
        "metric_support": {
            "universal_metrics": [
                "net_pnl",
                "realized_pnl",
                "trade_count",
                "long_trades",
                "short_trades",
                "winners",
                "losers",
                "win_rate",
                "average_trade",
                "max_drawdown",
                "session_breakdown",
                "latest_trade_summary",
                "latest_status",
                "latest_update_timestamp",
            ],
            "lane_specific_metrics": {
                "open_pnl": [
                    LANE_TYPE_PAPER_RUNTIME,
                    LANE_TYPE_BENCHMARK_REPLAY,
                    LANE_TYPE_RESEARCH_EXECUTION,
                    LANE_TYPE_HISTORICAL_PLAYBACK,
                ],
                "profit_factor": [LANE_TYPE_PAPER_RUNTIME],
                "trade_family_breakdown": [LANE_TYPE_PAPER_RUNTIME, LANE_TYPE_BENCHMARK_REPLAY, LANE_TYPE_RESEARCH_EXECUTION],
            },
            "notes": [
                "Metrics are rendered only when the underlying lane publishes enough truth to support them.",
                "Replay, paper-runtime, and research-execution lanes stay provenance-separated in this surface.",
                "Unavailable metrics include an explicit reason instead of synthetic placeholders.",
            ],
        },
        "lifecycle_truth_classes": {
            LIFECYCLE_TRUTH_FULL: "Lane publishes or derives end-to-end lifecycle truth authoritative enough to inspect full strategy execution flow.",
            LIFECYCLE_TRUTH_HYBRID: "Lane publishes authoritative entry detail, but lifecycle truth still relies on baseline-style exit truth for part of the trade path.",
            LIFECYCLE_TRUTH_BASELINE_ONLY: "Lane reflects baseline or compatibility replay truth only; it is not equivalent to full lifecycle execution truth.",
            LIFECYCLE_TRUTH_UNSUPPORTED: "Requested execution semantics are not supported for this artifact or family, so lifecycle truth cannot be treated as authoritative.",
        },
        "truth_sources": {
            "benchmark_replay": "Historical playback manifests, replay summary artifacts, and strategy-study artifacts under outputs/historical_playback.",
            "paper_runtime": "Current paper dashboard snapshots, lane-local SQLite truth, and tracked paper detail artifacts under outputs/operator_dashboard and paper lane stores.",
            "research_execution": "Strategy-study artifacts whose study_mode is research_execution_mode.",
            "historical_playback": "Legacy/backfilled strategy-study artifacts where benchmark/research semantics are not explicit.",
        },
        "notes": [
            "This surface is strategy-centric: choose a strategy first, then inspect lane-specific evidence and comparisons.",
            "Benchmark/replay truth is kept separate from paper/runtime truth and broker truth.",
            "Lifecycle-truth labels are explicit so baseline-only, hybrid, and full-lifecycle evidence are not presented as equivalent.",
            "ATP semantics, session gating, and execution rules are not modified by this read model.",
        ],
    }


def _discover_strategy_catalog_entries(
    *,
    evidence_lanes: Sequence[dict[str, Any]],
    historical_playback: dict[str, Any],
    paper: dict[str, Any],
    runtime_registry: dict[str, Any],
    lane_registry: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    discovered: dict[str, dict[str, Any]] = {}

    for lane in evidence_lanes:
        _register_strategy_discovery(
            discovered,
            strategy_key=str(lane.get("strategy_key") or "").strip(),
            display_name=lane.get("display_name") or lane.get("strategy_label"),
            instrument=lane.get("instrument"),
            strategy_family=lane.get("strategy_family"),
            standalone_strategy_id=lane.get("standalone_strategy_id") or lane.get("strategy_key"),
            discovery_source="evidence_lanes",
            source_type=str(lane.get("lane_type") or lane.get("source_lane") or "unknown"),
        )

    for item in list(((historical_playback.get("study_catalog") or {}).get("items") or [])):
        strategy_key = _canonical_strategy_key(
            item.get("strategy_id") or item.get("strategy_family"),
            instrument=item.get("symbol"),
            strategy_family=item.get("strategy_family"),
        )
        _register_strategy_discovery(
            discovered,
            strategy_key=str(strategy_key or "").strip(),
            display_name=item.get("label") or item.get("strategy_id") or item.get("strategy_family"),
            instrument=item.get("symbol"),
            strategy_family=item.get("strategy_family"),
            standalone_strategy_id=item.get("strategy_id") or strategy_key,
            discovery_source="historical_playback_study_catalog",
            source_type=str(item.get("study_mode") or "baseline_parity_mode"),
        )

    for row in list((runtime_registry.get("rows") or [])):
        strategy_key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_key:
            continue
        _register_strategy_discovery(
            discovered,
            strategy_key=strategy_key,
            display_name=row.get("display_name") or row.get("standalone_strategy_label") or strategy_key,
            instrument=row.get("instrument"),
            strategy_family=row.get("strategy_family"),
            standalone_strategy_id=row.get("standalone_strategy_id") or strategy_key,
            discovery_source="runtime_registry",
            source_type="runtime_registry",
        )

    for row in list((lane_registry.get("rows") or [])):
        strategy_key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_key:
            identity = build_standalone_strategy_identity(
                instrument=row.get("instrument"),
                lane_id=row.get("lane_id"),
                strategy_name=row.get("display_name"),
                source_family=row.get("family") or row.get("strategy_family"),
                lane_name=row.get("lane_name") or row.get("display_name"),
            )
            strategy_key = identity["standalone_strategy_id"]
        _register_strategy_discovery(
            discovered,
            strategy_key=strategy_key,
            display_name=row.get("display_name") or row.get("lane_name") or strategy_key,
            instrument=row.get("instrument"),
            strategy_family=row.get("strategy_family") or row.get("family"),
            standalone_strategy_id=row.get("standalone_strategy_id") or strategy_key,
            discovery_source="lane_registry",
            source_type="lane_registry",
        )

    tracked_rows = list(((paper.get("tracked_strategies") or {}).get("rows") or []))
    for row in tracked_rows:
        instrument = next(iter(list(row.get("observed_instruments") or [])), None)
        strategy_key = _canonical_strategy_key(
            row.get("strategy_id"),
            instrument=instrument,
            strategy_family=row.get("strategy_family"),
        )
        _register_strategy_discovery(
            discovered,
            strategy_key=str(strategy_key or "").strip(),
            display_name=row.get("display_name") or row.get("strategy_id"),
            instrument=instrument,
            strategy_family=row.get("strategy_family"),
            standalone_strategy_id=row.get("strategy_id") or strategy_key,
            discovery_source="tracked_paper_strategies",
            source_type="paper_runtime",
        )

    return discovered


def _register_strategy_discovery(
    discovered: dict[str, dict[str, Any]],
    *,
    strategy_key: str,
    display_name: Any,
    instrument: Any,
    strategy_family: Any,
    standalone_strategy_id: Any,
    discovery_source: str,
    source_type: str,
) -> None:
    if not strategy_key:
        return
    payload = discovered.setdefault(
        strategy_key,
        {
            "strategy_key": strategy_key,
            "display_name": None,
            "instrument": None,
            "strategy_family": None,
            "standalone_strategy_id": None,
            "discovery_sources": set(),
            "source_types": set(),
        },
    )
    payload["display_name"] = payload.get("display_name") or display_name or strategy_key
    payload["instrument"] = payload.get("instrument") or instrument
    payload["strategy_family"] = payload.get("strategy_family") or strategy_family
    payload["standalone_strategy_id"] = payload.get("standalone_strategy_id") or standalone_strategy_id or strategy_key
    payload["discovery_sources"].add(discovery_source)
    if source_type:
        payload["source_types"].add(source_type)
    payload["discovery_sources"] = set(payload["discovery_sources"])
    payload["source_types"] = set(payload["source_types"])


def _build_results_board_payload(
    *,
    strategies: dict[str, dict[str, Any]],
    catalog_rows: Sequence[dict[str, Any]],
    details_by_strategy_key: dict[str, dict[str, Any]],
    evidence_lanes: Sequence[dict[str, Any]],
    runtime_registry: dict[str, Any],
    lane_registry: dict[str, Any],
) -> dict[str, Any]:
    board_rows = [
        _results_board_row(
            lane=lane,
            strategy_display_name=strategies.get(str(lane.get("strategy_key") or "").strip(), {}).get("display_name"),
        )
        for lane in evidence_lanes
    ]
    strategy_keys_with_data = {str(lane.get("strategy_key") or "").strip() for lane in evidence_lanes if str(lane.get("strategy_key") or "").strip()}
    board_rows.sort(
        key=lambda row: (
            0 if row.get("has_data") else 1,
            _sort_missing_rank(row.get("sort_values"), "net_pnl"),
            -(_sort_numeric_value(row.get("sort_values"), "net_pnl") or 0),
            -(_sort_numeric_value(row.get("sort_values"), "latest_update_timestamp") or 0),
            str(row.get("strategy_display_name") or ""),
            str(row.get("lane_label") or ""),
        )
    )
    row_ids_with_data = {str(row.get("lane_id") or "") for row in board_rows if str(row.get("lane_id") or "").strip()}
    paper_lane_ids_with_data = {
        str(lane.get("paper_lane_id") or "").strip()
        for lane in evidence_lanes
        if str(lane.get("paper_lane_id") or "").strip()
    }
    strategy_options = [
        {
            "id": row.get("strategy_key"),
            "label": row.get("display_name") or row.get("strategy_key"),
            "instrument": row.get("instrument"),
            "strategy_family": row.get("strategy_family"),
            "standalone_strategy_id": row.get("standalone_strategy_id"),
            "source_types": list(row.get("source_types") or []),
            "discovery_sources": list(row.get("discovery_sources") or []),
            "has_data": bool(row.get("has_data")),
            "data_lane_count": int(row.get("lane_count") or 0),
        }
        for row in catalog_rows
    ]
    lane_options = _results_board_lane_options(
        evidence_lanes=evidence_lanes,
        runtime_registry=runtime_registry,
        lane_registry=lane_registry,
        strategy_keys_with_data=strategy_keys_with_data,
        row_ids_with_data=row_ids_with_data,
        paper_lane_ids_with_data=paper_lane_ids_with_data,
    )
    source_types = _selector_value_options(lane_options, key="source_type", label_key="source_label")
    candidate_statuses = _selector_value_options(lane_options, key="candidate_status_id", label_key="candidate_status_label")
    lifecycle_truth_classes = _selector_value_options(lane_options, key="lifecycle_truth_class", label_key="lifecycle_truth_label")
    sort_fields = _results_board_sort_fields(board_rows)
    run_scope_presets = _results_board_run_scope_presets(board_rows=board_rows, sort_fields=sort_fields)
    rank_limit_options = [
        {"id": "all", "label": "All"},
        {"id": "1", "label": "Top 1"},
        {"id": "5", "label": "Top 5"},
        {"id": "10", "label": "Top 10"},
    ]
    saved_views = _results_board_saved_views(
        board_rows=board_rows,
        details_by_strategy_key=details_by_strategy_key,
        sort_fields=sort_fields,
    )
    default_strategy_key = next((str(row.get("id") or "") for row in strategy_options if row.get("has_data")), None)
    if default_strategy_key is None and strategy_options:
        default_strategy_key = str(strategy_options[0].get("id") or "")
    default_row_id = next((str(row.get("lane_id") or "") for row in board_rows if str(row.get("lane_id") or "").strip()), None)

    return {
        "available": bool(strategy_options),
        "row_count": len(board_rows),
        "discovered_strategy_count": len(strategy_options),
        "discovered_lane_count": len(lane_options),
        "default_row_id": default_row_id,
        "default_columns": [
            {"key": "strategy_display_name", "label": "Strategy"},
            {"key": "lane_label", "label": "Lane"},
            {"key": "run_study_identity", "label": "Run / Study"},
            {"key": "date_range_label", "label": "Date Range"},
            {"key": "trade_count", "label": "Trades"},
            {"key": "net_pnl", "label": "Net P/L"},
            {"key": "average_trade", "label": "Avg Trade"},
            {"key": "profit_factor", "label": "Profit Factor"},
            {"key": "max_drawdown", "label": "Max Drawdown"},
            {"key": "win_rate", "label": "Win Rate"},
            {"key": "latest_trade_summary_label", "label": "Latest Trade"},
            {"key": "lifecycle_truth", "label": "Lifecycle Truth"},
            {"key": "source_lane", "label": "Provenance"},
        ],
        "rows": board_rows,
        "discovery": {
            "strategies": sorted(
                strategy_options,
                key=lambda row: (0 if row.get("has_data") else 1, str(row.get("label") or "")),
            ),
            "lanes": lane_options,
            "source_types": source_types,
            "candidate_statuses": candidate_statuses,
            "lifecycle_truth_classes": lifecycle_truth_classes,
            "sources": [
                {
                    "id": "historical_playback_study_catalog",
                    "label": "Historical Playback Study Catalog",
                    "note": "Replay and research strategy-study artifacts under outputs/historical_playback.",
                },
                {
                    "id": "paper_strategy_performance",
                    "label": "Paper Strategy Performance",
                    "note": "Persisted paper performance rows and trade-log evidence.",
                },
                {
                    "id": "tracked_paper_strategies",
                    "label": "Tracked Paper Strategies",
                    "note": "Tracked paper strategy summary/detail identities, even when full performance rows lag.",
                },
                {
                    "id": "runtime_registry",
                    "label": "Runtime Registry",
                    "note": "Config/runtime-derived standalone strategy identities from the current registry.",
                },
                {
                    "id": "lane_registry",
                    "label": "Lane Registry",
                    "note": "Lane registry rows for approved baselines, admitted paper lanes, and canary candidates.",
                },
            ],
        },
        "sort_fields": sort_fields,
        "run_scope_presets": run_scope_presets,
        "rank_limit_options": rank_limit_options,
        "saved_views": saved_views,
        "defaults": {
            "strategy_key": default_strategy_key,
            "lane_id": "all",
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "date_window": "all_dates",
            "run_scope": "top",
            "sort_field": "net_pnl",
            "rank_limit": "10",
        },
        "date_windows": [
            {"id": "all_dates", "label": "All Dates"},
            {"id": "recent_7d", "label": "Recent 7d"},
            {"id": "recent_30d", "label": "Recent 30d"},
            {"id": "recent_90d", "label": "Recent 90d"},
        ],
        "notes": [
            "The default experience is a ranked results table; deeper evidence stays in the drill-down surface below.",
            "Selectors are discovery-driven from evidence lanes, runtime registry rows, lane registry rows, and tracked paper identities.",
            "Registry-only or config-only selectors remain visible with has_data=false so operators can distinguish known identities from result-bearing lanes.",
            "Unavailable metrics retain explicit reasons instead of synthetic values.",
        ],
    }


def _results_board_row(*, lane: dict[str, Any], strategy_display_name: Any) -> dict[str, Any]:
    latest_trade_summary = dict(_nested_get(lane, "metrics", "latest_trade_summary") or {})
    latest_update_metric = dict(_nested_get(lane, "metrics", "latest_update_timestamp") or {})
    return {
        **lane,
        "id": lane.get("lane_id"),
        "row_id": lane.get("lane_id"),
        "has_data": True,
        "strategy_display_name": strategy_display_name or lane.get("strategy_key"),
        "source_type": str(lane.get("lane_type") or lane.get("source_lane") or "unknown"),
        "source_label": _lane_type_label(str(lane.get("lane_type") or lane.get("source_lane") or "unknown")),
        "run_study_identity": _results_board_run_study_identity(lane),
        "date_range_label": _results_board_date_range_label(dict(lane.get("date_range") or {})),
        "latest_trade_summary_label": _results_board_latest_trade_label(latest_trade_summary),
        "candidate_status": _results_board_candidate_status(lane),
        "candidate_status_id": _results_board_candidate_status(lane)["id"],
        "candidate_status_label": _results_board_candidate_status(lane)["label"],
        "lifecycle_truth_class": str(_nested_get(lane, "lifecycle_truth", "class") or "").strip() or None,
        "lifecycle_truth_label": str(_nested_get(lane, "lifecycle_truth", "label") or _nested_get(lane, "lifecycle_truth", "class") or "").strip() or None,
        "sort_values": {
            "net_pnl": _metric_sort_value(_nested_get(lane, "metrics", "net_pnl")),
            "average_trade": _metric_sort_value(_nested_get(lane, "metrics", "average_trade")),
            "profit_factor": _metric_sort_value(_nested_get(lane, "metrics", "profit_factor")),
            "max_drawdown": _metric_sort_value(_nested_get(lane, "metrics", "max_drawdown")),
            "trade_count": _metric_sort_value(_nested_get(lane, "metrics", "trade_count")),
            "latest_update_timestamp": _timestamp_metric_sort_value(latest_update_metric),
            "win_rate": _metric_sort_value(_nested_get(lane, "metrics", "win_rate")),
        },
    }


def _results_board_lane_options(
    *,
    evidence_lanes: Sequence[dict[str, Any]],
    runtime_registry: dict[str, Any],
    lane_registry: dict[str, Any],
    strategy_keys_with_data: set[str],
    row_ids_with_data: set[str],
    paper_lane_ids_with_data: set[str],
) -> list[dict[str, Any]]:
    options: dict[str, dict[str, Any]] = {}

    def register(option: dict[str, Any]) -> None:
        option_id = str(option.get("id") or "").strip()
        if not option_id:
            return
        if option_id in options:
            existing = options[option_id]
            existing["has_data"] = bool(existing.get("has_data")) or bool(option.get("has_data"))
            return
        options[option_id] = option

    for lane in evidence_lanes:
        candidate_status = _results_board_candidate_status(lane)
        register(
            {
                "id": str(lane.get("lane_id") or ""),
                "label": f"{_lane_type_label(str(lane.get('lane_type') or 'unknown'))} | {lane.get('display_name') or lane.get('strategy_key')}",
                "strategy_key": lane.get("strategy_key"),
                "strategy_display_name": lane.get("display_name") or lane.get("strategy_key"),
                "source_type": str(lane.get("lane_type") or lane.get("source_lane") or "unknown"),
                "source_label": _lane_type_label(str(lane.get("lane_type") or lane.get("source_lane") or "unknown")),
                "candidate_status_id": candidate_status["id"],
                "candidate_status_label": candidate_status["label"],
                "lifecycle_truth_class": str(_nested_get(lane, "lifecycle_truth", "class") or "").strip() or None,
                "lifecycle_truth_label": str(_nested_get(lane, "lifecycle_truth", "label") or _nested_get(lane, "lifecycle_truth", "class") or "").strip() or None,
                "has_data": True,
                "internal_identity": str(lane.get("lane_id") or ""),
            }
        )

    for row in list((runtime_registry.get("rows") or [])):
        strategy_key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        lane_id = str(row.get("lane_id") or strategy_key).strip()
        if not strategy_key or not lane_id:
            continue
        register(
            {
                "id": f"runtime_registry:{lane_id}",
                "label": f"Runtime Registry | {row.get('display_name') or strategy_key}",
                "strategy_key": strategy_key,
                "strategy_display_name": row.get("display_name") or strategy_key,
                "source_type": "runtime_registry",
                "source_label": "Runtime Registry",
                "candidate_status_id": "CONFIGURED_RUNTIME",
                "candidate_status_label": "Configured Runtime",
                "lifecycle_truth_class": None,
                "lifecycle_truth_label": None,
                "has_data": strategy_key in strategy_keys_with_data or lane_id in paper_lane_ids_with_data,
                "internal_identity": lane_id,
            }
        )

    for row in list((lane_registry.get("rows") or [])):
        strategy_key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_key:
            identity = build_standalone_strategy_identity(
                instrument=row.get("instrument"),
                lane_id=row.get("lane_id"),
                strategy_name=row.get("display_name"),
                source_family=row.get("family") or row.get("strategy_family"),
                lane_name=row.get("lane_name") or row.get("display_name"),
            )
            strategy_key = identity["standalone_strategy_id"]
        lane_id = str(row.get("lane_id") or strategy_key).strip()
        admission_state = str(row.get("admission_state") or row.get("surface_group") or "registry").strip()
        register(
            {
                "id": f"lane_registry:{lane_id}:{strategy_key}",
                "label": f"{row.get('display_name') or row.get('lane_name') or lane_id} | {row.get('instrument') or admission_state}",
                "strategy_key": strategy_key,
                "strategy_display_name": row.get("display_name") or row.get("lane_name") or strategy_key,
                "source_type": "lane_registry",
                "source_label": "Lane Registry",
                "candidate_status_id": admission_state.upper() if admission_state else "REGISTRY_ONLY",
                "candidate_status_label": sentence_case(admission_state.replace("_", " ")) if admission_state else "Registry Only",
                "lifecycle_truth_class": None,
                "lifecycle_truth_label": None,
                "has_data": strategy_key in strategy_keys_with_data or lane_id in paper_lane_ids_with_data or lane_id in row_ids_with_data,
                "internal_identity": lane_id,
            }
        )

    return sorted(
        options.values(),
        key=lambda row: (0 if row.get("has_data") else 1, str(row.get("label") or "")),
    )


def _selector_value_options(rows: Sequence[dict[str, Any]], *, key: str, label_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        raw_value = str(row.get(key) or "").strip()
        if not raw_value:
            continue
        payload = grouped.setdefault(
            raw_value,
            {
                "id": raw_value,
                "label": row.get(label_key) or raw_value,
                "count": 0,
                "has_data": False,
            },
        )
        payload["count"] += 1
        payload["has_data"] = bool(payload["has_data"]) or bool(row.get("has_data"))
    return sorted(grouped.values(), key=lambda row: (0 if row.get("has_data") else 1, str(row.get("label") or "")))


def _results_board_sort_fields(board_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        ("net_pnl", "Net P/L", "desc"),
        ("average_trade", "Avg Trade", "desc"),
        ("profit_factor", "Profit Factor", "desc"),
        ("max_drawdown", "Max Drawdown", "asc"),
        ("trade_count", "Trade Count", "desc"),
        ("latest_update_timestamp", "Latest Timestamp", "desc"),
    ]
    payload: list[dict[str, Any]] = []
    for field_id, label, direction in fields:
        supported_row_count = sum(
            1 for row in board_rows if _sort_numeric_value(dict(row.get("sort_values") or {}), field_id) is not None
        )
        payload.append(
            {
                "id": field_id,
                "label": label,
                "default_direction": direction,
                "supported_row_count": supported_row_count,
                "available": supported_row_count > 0,
            }
        )
    return payload


def _results_board_run_scope_presets(
    *,
    board_rows: Sequence[dict[str, Any]],
    sort_fields: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest_supported = next(
        (row for row in sort_fields if row.get("id") == "latest_update_timestamp"),
        {"available": False, "supported_row_count": 0},
    )
    drawdown_supported = next(
        (row for row in sort_fields if row.get("id") == "max_drawdown"),
        {"available": False, "supported_row_count": 0},
    )
    return [
        {
            "id": "all",
            "label": "All",
            "description": "Show every currently visible result row.",
            "available": bool(board_rows),
            "recommended_sort_field": "net_pnl",
        },
        {
            "id": "latest",
            "label": "Latest",
            "description": "Rank by the newest supported timestamp.",
            "available": bool(latest_supported.get("available")),
            "recommended_sort_field": "latest_update_timestamp",
            "forced_rank_limit": "1",
        },
        {
            "id": "top",
            "label": "Top",
            "description": "Rank by the selected sort field.",
            "available": bool(board_rows),
            "recommended_sort_field": "net_pnl",
        },
        {
            "id": "lowest_drawdown",
            "label": "Lowest Drawdown",
            "description": "Rank ascending by max drawdown when that metric is supported.",
            "available": bool(drawdown_supported.get("available")),
            "recommended_sort_field": "max_drawdown",
        },
    ]


def _results_board_saved_views(
    *,
    board_rows: Sequence[dict[str, Any]],
    details_by_strategy_key: dict[str, dict[str, Any]],
    sort_fields: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    profit_factor_available = any(row.get("id") == "profit_factor" and row.get("available") for row in sort_fields)
    drawdown_available = any(row.get("id") == "max_drawdown" and row.get("available") for row in sort_fields)
    atp_strategy_key = next(
        (
            strategy_key
            for strategy_key, detail in details_by_strategy_key.items()
            if "active_trend_participation_engine" in str(_nested_get(detail, "strategy_identity", "strategy_family") or "")
        ),
        None,
    )
    has_replay_vs_paper = any(
        any(preset.get("comparison_type") == "benchmark_vs_paper_runtime" for preset in list(detail.get("comparison_presets") or []))
        for detail in details_by_strategy_key.values()
    )
    has_atp_candidate_view = any(
        "active_trend_participation_engine" in str(_nested_get(detail, "strategy_identity", "strategy_family") or "")
        and any(
            lane.get("lane_type") in {LANE_TYPE_BENCHMARK_REPLAY, LANE_TYPE_PAPER_RUNTIME, LANE_TYPE_RESEARCH_EXECUTION}
            for lane in list(detail.get("lanes") or [])
        )
        for detail in details_by_strategy_key.values()
    )
    return [
        {
            "id": "latest_runs",
            "label": "Latest Runs",
            "available": bool(board_rows),
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "latest",
            "sort_field": "latest_update_timestamp",
            "rank_limit": "10",
            "note": "Newest result-bearing rows across all provenance-separated lanes.",
        },
        {
            "id": "top_10_net_pnl",
            "label": "Top 10 by Net P/L",
            "available": bool(board_rows),
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "top",
            "sort_field": "net_pnl",
            "rank_limit": "10",
        },
        {
            "id": "top_10_profit_factor",
            "label": "Top 10 by PF",
            "available": profit_factor_available,
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "top",
            "sort_field": "profit_factor",
            "rank_limit": "10",
            "unavailable_reason": None if profit_factor_available else "No current result row publishes supported profit-factor truth.",
        },
        {
            "id": "lowest_drawdown",
            "label": "Lowest Drawdown",
            "available": drawdown_available,
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "lowest_drawdown",
            "sort_field": "max_drawdown",
            "rank_limit": "10",
            "unavailable_reason": None if drawdown_available else "No current result row publishes supported max-drawdown truth.",
        },
        {
            "id": "atp_baseline_vs_active_candidate",
            "label": "ATP Baseline vs Active Candidate",
            "available": has_atp_candidate_view,
            "strategy_key": atp_strategy_key,
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "top",
            "sort_field": "latest_update_timestamp",
            "rank_limit": "10",
            "source_type_list": [LANE_TYPE_BENCHMARK_REPLAY, LANE_TYPE_PAPER_RUNTIME, LANE_TYPE_RESEARCH_EXECUTION],
            "unavailable_reason": None if has_atp_candidate_view else "No ATP-family strategy currently exposes comparable baseline/candidate rows.",
        },
        {
            "id": "replay_vs_paper_selected_strategy",
            "label": "Replay vs Paper",
            "available": has_replay_vs_paper,
            "source_type": "all",
            "candidate_status": "all",
            "lifecycle_truth_class": "all",
            "run_scope": "top",
            "sort_field": "latest_update_timestamp",
            "rank_limit": "10",
            "source_type_list": [LANE_TYPE_BENCHMARK_REPLAY, LANE_TYPE_PAPER_RUNTIME],
            "requires_strategy_selection": True,
            "unavailable_reason": None if has_replay_vs_paper else "No current strategy has both benchmark replay and paper-runtime evidence lanes.",
        },
    ]


def _results_board_run_study_identity(lane: dict[str, Any]) -> str:
    display_name = str(lane.get("display_name") or lane.get("strategy_label") or lane.get("strategy_key") or "").strip()
    study_key = str(_nested_get(lane, "run_identity", "study_key") or _nested_get(lane, "run_identity", "study_id") or "").strip()
    run_id = str(_nested_get(lane, "run_identity", "run_id") or "").strip()
    if display_name and study_key and study_key not in display_name:
        return f"{display_name} [{study_key}]"
    if display_name and run_id and run_id not in display_name and run_id != study_key:
        return f"{display_name} [{run_id}]"
    return display_name or study_key or run_id or "Unavailable"


def _results_board_date_range_label(date_range: dict[str, Any]) -> str:
    start = str(date_range.get("start_timestamp") or "—")
    end = str(date_range.get("end_timestamp") or "—")
    return f"{start} -> {end}"


def _results_board_latest_trade_label(metric: dict[str, Any]) -> str:
    if metric.get("available") is not True:
        reason = str(metric.get("reason") or "").strip()
        return f"Unavailable: {reason}" if reason else "Unavailable"
    value = dict(metric.get("value") or {})
    parts = [
        str(value.get("family") or value.get("signal_family") or value.get("trade_id") or "").strip(),
        str(value.get("exit_timestamp") or value.get("latest_timestamp") or "").strip(),
        str(value.get("realized_pnl") or "").strip(),
    ]
    filtered = [part for part in parts if part]
    return " | ".join(filtered) if filtered else "Available"


def _results_board_candidate_status(lane: dict[str, Any]) -> dict[str, str]:
    lane_type = str(lane.get("lane_type") or "").strip()
    candidate_id = str(_nested_get(lane, "run_identity", "candidate_id") or "").strip()
    benchmark_designation = str(_nested_get(lane, "config_identity", "benchmark_designation") or "").strip()
    if lane_type == LANE_TYPE_BENCHMARK_REPLAY:
        return {"id": "BENCHMARK_REFERENCE", "label": "Benchmark Reference"}
    if lane_type == LANE_TYPE_RESEARCH_EXECUTION:
        return {"id": "RESEARCH_CANDIDATE" if candidate_id else "RESEARCH_EXECUTION", "label": "Research Candidate" if candidate_id else "Research Execution"}
    if lane_type == LANE_TYPE_PAPER_RUNTIME:
        return {
            "id": "PAPER_BENCHMARK" if benchmark_designation else "PAPER_RUNTIME",
            "label": "Paper Benchmark" if benchmark_designation else "Paper Runtime",
        }
    if lane_type == LANE_TYPE_HISTORICAL_PLAYBACK:
        return {
            "id": "HISTORICAL_CANDIDATE" if candidate_id else "HISTORICAL_REFERENCE",
            "label": "Historical Candidate" if candidate_id else "Historical Reference",
        }
    return {"id": "UNKNOWN", "label": "Unknown"}


def _metric_sort_value(metric: Any) -> float | None:
    metric_dict = dict(metric or {})
    if metric_dict.get("available") is not True:
        return None
    value = _decimal_or_none(metric_dict.get("value"))
    if value is None or not value.is_finite():
        return None
    return float(value)


def _timestamp_metric_sort_value(metric: dict[str, Any]) -> float | None:
    if metric.get("available") is not True:
        return None
    value = str(metric.get("value") or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _sort_numeric_value(sort_values: dict[str, Any] | None, key: str) -> float | None:
    if not sort_values:
        return None
    value = sort_values.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sort_missing_rank(sort_values: dict[str, Any] | None, key: str) -> int:
    return 1 if _sort_numeric_value(sort_values, key) is None else 0


def sentence_case(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(part.capitalize() for part in text.split())


def _replay_strategy_lanes(historical_playback: dict[str, Any]) -> list[dict[str, Any]]:
    study_catalog_items = list(((historical_playback.get("study_catalog") or {}).get("items") or []))
    lanes: list[dict[str, Any]] = []
    for item in study_catalog_items:
        study = dict(item.get("study") or {})
        summary = dict(item.get("summary") or study.get("summary") or {})
        meta = dict(item.get("meta") or study.get("meta") or {})
        timeframe_truth = dict(item.get("timeframe_truth") or meta.get("timeframe_truth") or {})
        explicit_study_mode = str(item.get("study_mode") or meta.get("study_mode") or "").strip()
        study_mode = explicit_study_mode or "baseline_parity_mode"
        lane_type = _replay_lane_type(study_mode, summary, meta, explicit_study_mode=bool(explicit_study_mode))
        bars = list(study.get("bars") or study.get("rows") or [])
        trade_events = list(study.get("trade_events") or [])
        pnl_points = list(study.get("pnl_points") or [])
        execution_slices = list(study.get("execution_slices") or [])
        latest_bar = bars[-1] if bars else {}
        latest_point = pnl_points[-1] if pnl_points else {}
        replay_trade_rows = _replay_closed_trade_rows(
            study=study,
            summary=summary,
            meta=meta,
            trade_events=trade_events,
            bars=bars,
        )
        open_pnl = latest_point.get("open_pnl") if latest_point else latest_bar.get("unrealized_pnl")
        realized_pnl = summary.get("cumulative_realized_pnl")
        net_pnl = summary.get("cumulative_total_pnl") or latest_point.get("total")
        winners = summary.get("winners")
        losers = summary.get("losers")
        if winners is None and replay_trade_rows and all(row.get("realized_pnl") is not None for row in replay_trade_rows):
            winners = sum(1 for row in replay_trade_rows if _decimal_or_none(row.get("realized_pnl")) and _decimal_or_none(row.get("realized_pnl")) > 0)
        if losers is None and replay_trade_rows and all(row.get("realized_pnl") is not None for row in replay_trade_rows):
            losers = sum(1 for row in replay_trade_rows if _decimal_or_none(row.get("realized_pnl")) and _decimal_or_none(row.get("realized_pnl")) < 0)
        total_decisions = (winners or 0) + (losers or 0) if winners is not None and losers is not None else None
        win_rate = None
        if winners is not None and total_decisions:
            win_rate = _decimal_to_string((Decimal(int(winners)) / Decimal(int(total_decisions))) * Decimal("100"))
        average_trade = None
        if summary.get("total_trades") and realized_pnl not in (None, ""):
            average_trade = _average_metric(realized_pnl, summary.get("total_trades"))
        replay_profit_factor = _replay_profit_factor(summary=summary, trade_rows=replay_trade_rows)
        trade_family_breakdown = _replay_trade_family_breakdown(summary=summary, trade_rows=replay_trade_rows, trade_events=trade_events)
        session_breakdown = _replay_session_breakdown(summary=summary, trade_rows=replay_trade_rows)
        latest_trade_summary = _replay_latest_trade_summary(summary=summary, trade_rows=replay_trade_rows, trade_events=trade_events)
        strategy_key = _canonical_strategy_key(
            meta.get("strategy_id")
            or study.get("standalone_strategy_id")
            or item.get("strategy_id")
            or study.get("strategy_family")
            or item.get("strategy_family"),
            instrument=study.get("symbol") or item.get("symbol"),
            strategy_family=study.get("strategy_family") or item.get("strategy_family"),
        )
        lane_id = f"replay:{item.get('study_key') or meta.get('study_id') or len(lanes)}"
        lifecycle_truth = _replay_lifecycle_truth(
            meta=meta,
            lane_type=lane_type,
            study_mode=study_mode,
        )
        lanes.append(
            {
                "lane_id": lane_id,
                "strategy_key": strategy_key,
                "display_name": item.get("label") or meta.get("strategy_id") or strategy_key,
                "strategy_label": meta.get("strategy_id") or study.get("standalone_strategy_id") or study.get("strategy_family"),
                "standalone_strategy_id": study.get("standalone_strategy_id") or item.get("strategy_id"),
                "strategy_family": study.get("strategy_family") or item.get("strategy_family"),
                "instrument": study.get("symbol") or item.get("symbol"),
                "lane_type": lane_type,
                "lane_label": _lane_type_label(lane_type),
                "source_lane": "historical_playback",
                "source_of_truth": {
                    "primary_artifact": "strategy_study_v3",
                    "artifact_paths": dict(item.get("artifact_paths") or {}),
                    "run_note": historical_playback.get("note"),
                    "truth_provenance": dict(meta.get("truth_provenance") or {}),
                },
                "run_identity": {
                    "run_id": item.get("run_stamp"),
                    "run_timestamp": item.get("run_timestamp"),
                    "study_id": meta.get("study_id") or item.get("study_key"),
                    "study_key": item.get("study_key"),
                    "candidate_id": item.get("candidate_id"),
                    "contract_version": item.get("contract_version") or study.get("contract_version"),
                },
                "config_identity": {
                    "benchmark_label": "Historical playback benchmark"
                    if lane_type == LANE_TYPE_BENCHMARK_REPLAY
                    else "Historical playback study",
                    "config_source": None,
                    "benchmark_designation": None,
                    "entry_model": meta.get("entry_model") or item.get("entry_model"),
                    "pnl_truth_basis": meta.get("pnl_truth_basis") or item.get("pnl_truth_basis"),
                },
                "lifecycle_truth": lifecycle_truth,
                "mode_truth": {
                    "study_mode": study_mode,
                    "mode_label": _study_mode_label(study_mode),
                    "execution_semantics": _replay_execution_semantics(meta, study_mode),
                    "intrabar_execution_authoritative": bool(meta.get("intrabar_execution_authoritative")),
                },
                "timeframe_truth": {
                    "structural_signal_timeframe": timeframe_truth.get("structural_signal_timeframe") or item.get("context_resolution"),
                    "execution_timeframe": timeframe_truth.get("execution_timeframe") or item.get("execution_resolution"),
                    "artifact_timeframe": timeframe_truth.get("artifact_timeframe") or study.get("timeframe"),
                    "execution_timeframe_role": timeframe_truth.get("execution_timeframe_role"),
                    "explicit": True,
                },
                "date_range": {
                    "start_timestamp": item.get("coverage_start") or meta.get("coverage_start") or _nested_get(meta, "coverage_range", "start_timestamp"),
                    "end_timestamp": item.get("coverage_end") or meta.get("coverage_end") or _nested_get(meta, "coverage_range", "end_timestamp"),
                    "session_window": _replay_session_window(summary),
                },
                "metrics": {
                    "net_pnl": _metric_value(net_pnl),
                    "realized_pnl": _metric_value(realized_pnl),
                    "open_pnl": _metric_value(
                        open_pnl,
                        unavailable_reason=summary.get("pnl_unavailable_reason")
                        if open_pnl in (None, "")
                        else None,
                    ),
                    "trade_count": _metric_value(summary.get("total_trades")),
                    "long_trades": _metric_value(summary.get("long_trades")),
                    "short_trades": _metric_value(summary.get("short_trades")),
                    "winners": _metric_value(winners, unavailable_reason="Replay study summary does not include closed-trade outcome counts." if winners is None else None),
                    "losers": _metric_value(losers, unavailable_reason="Replay study summary does not include closed-trade outcome counts." if losers is None else None),
                    "win_rate": _metric_value(win_rate, unavailable_reason="Replay winners/losers are unavailable for this artifact." if win_rate is None else None),
                    "average_trade": _metric_value(average_trade, unavailable_reason="Average trade requires realized P/L and trade count."),
                    "profit_factor": _metric_value(
                        replay_profit_factor.get("value"),
                        unavailable_reason=replay_profit_factor.get("reason"),
                    ),
                    "max_drawdown": _metric_value(summary.get("max_drawdown")),
                    "session_breakdown": {
                        "available": bool(session_breakdown),
                        "value": session_breakdown,
                        "reason": None if session_breakdown else "Replay artifact does not expose enough closed-trade or session-behavior detail for a session breakdown.",
                    },
                    "trade_family_breakdown": {
                        "available": bool(trade_family_breakdown),
                        "value": trade_family_breakdown,
                        "reason": None if trade_family_breakdown else "Replay artifact did not expose enough closed-trade family labels for a family breakdown.",
                    },
                    "latest_trade_summary": {
                        "available": bool(latest_trade_summary),
                        "value": latest_trade_summary,
                        "reason": None if latest_trade_summary else "Replay study does not expose a complete latest-trade path yet.",
                    },
                    "latest_status": _metric_value(
                        latest_bar.get("strategy_status") or latest_bar.get("position_side") or meta.get("study_mode"),
                    ),
                    "latest_update_timestamp": _metric_value(
                        item.get("run_timestamp")
                        or item.get("coverage_end")
                        or meta.get("coverage_end")
                        or study.get("generated_at")
                    ),
                },
                "runtime_health": {
                    "label": "Replay artifact loaded",
                    "attached": False,
                    "stale": False,
                    "reconciling": False,
                    "healthy": True,
                    "status_reason": "Historical playback artifacts are static replay truth, not attached runtime state.",
                },
                "evidence": {
                    "bars": _evidence_ref(
                        available=bool(bars),
                        count=len(bars),
                        preview_rows=bars[-_PREVIEW_ROW_LIMIT:],
                        ref={"study_key": item.get("study_key")},
                        unavailable_reason="Replay strategy study does not contain bar rows." if not bars else None,
                    ),
                    "signals": _evidence_ref(
                        available=True,
                        count=_count_trade_events(trade_events, "signal"),
                        preview_rows=[event for event in trade_events if "ENTRY" in str(event.get("event_type") or "")][: _PREVIEW_ROW_LIMIT],
                        ref={"study_key": item.get("study_key")},
                    ),
                    "order_intents": _evidence_ref(
                        available=True,
                        count=_count_trade_events(trade_events, "intent"),
                        preview_rows=[event for event in trade_events if "INTENT" in str(event.get("event_type") or "")][: _PREVIEW_ROW_LIMIT],
                        ref={"study_key": item.get("study_key")},
                    ),
                    "fills": _evidence_ref(
                        available=True,
                        count=_count_trade_events(trade_events, "fill"),
                        preview_rows=[event for event in trade_events if "FILL" in str(event.get("event_type") or "")][: _PREVIEW_ROW_LIMIT],
                        ref={"study_key": item.get("study_key")},
                    ),
                    "state_snapshots": _evidence_ref(
                        available=False,
                        count=0,
                        preview_rows=[],
                        ref={"study_key": item.get("study_key")},
                        unavailable_reason="Replay strategy-study bars embed bar-context state but do not publish standalone state snapshot rows.",
                    ),
                    "execution_slices": _evidence_ref(
                        available=bool(execution_slices),
                        count=len(execution_slices),
                        preview_rows=execution_slices[-_PREVIEW_ROW_LIMIT:],
                        ref={"study_key": item.get("study_key")},
                        unavailable_reason="This replay artifact does not include execution slices." if not execution_slices else None,
                    ),
                    "session_evidence": _evidence_ref(
                        available=bool(summary.get("session_level_behavior")),
                        count=len(list(summary.get("session_level_behavior") or [])),
                        preview_rows=list(summary.get("session_level_behavior") or []),
                        ref={"study_key": item.get("study_key")},
                        unavailable_reason="Replay summary did not publish session-level behavior rows." if not summary.get("session_level_behavior") else None,
                    ),
                    "readiness_artifacts": _evidence_ref(
                        available=bool(_nested_get(summary, "atp_summary", "available")),
                        count=len(list(_nested_get(summary, "atp_summary", "top_atp_blocker_codes") or [])),
                        preview_rows=list(_nested_get(summary, "atp_summary", "top_atp_blocker_codes") or []),
                        ref={"study_key": item.get("study_key")},
                        unavailable_reason=_nested_get(summary, "atp_summary", "unavailable_reason"),
                    ),
                },
                "provenance": {
                    "summary": "Replay metrics come from persisted historical-playback strategy-study artifacts and remain separate from paper-runtime truth.",
                    "metric_sources": {
                        "pnl": "strategy_study.summary and strategy_study.pnl_points",
                        "trade_counts": "strategy_study.summary",
                        "event_counts": "strategy_study.trade_events",
                        "trade_breakdowns": "Derived from persisted replay trade events or authoritative trade lifecycle records when those remain complete enough to price closed trades truthfully.",
                    },
                    "lifecycle_truth_source": "strategy_study.meta.pnl_truth_basis plus entry_model_capabilities and authoritative execution-truth records where published",
                },
            }
        )
    return lanes


def _paper_strategy_lanes(paper: dict[str, Any], *, generated_at: str) -> list[dict[str, Any]]:
    performance_rows = [dict(row) for row in list(((paper.get("strategy_performance") or {}).get("rows") or []))]
    trade_log_rows = [dict(row) for row in list(((paper.get("strategy_performance") or {}).get("trade_log") or []))]
    attribution_rows = [dict(row) for row in list((((paper.get("strategy_performance") or {}).get("attribution") or {}).get("rows") or []))]
    trade_log_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trade_log_rows:
        key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if key:
            trade_log_by_strategy[key].append(row)
    lane_universe = {
        str(row.get("lane_id") or ""): dict(row)
        for row in list(((paper.get("raw_operator_status") or {}).get("lanes") or []))
        if row.get("lane_id")
    }
    tracked_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(((paper.get("tracked_strategies") or {}).get("rows") or []))
        if row.get("strategy_id")
    }
    tracked_detail_by_lane_id: dict[str, dict[str, Any]] = {}
    tracked_summary_by_lane_id: dict[str, dict[str, Any]] = {}
    tracked_details_by_strategy = dict(((paper.get("tracked_strategies") or {}).get("details_by_strategy_id") or {}))
    for strategy_id, detail in tracked_details_by_strategy.items():
        detail_dict = dict(detail or {})
        summary_row = tracked_rows.get(str(strategy_id), {})
        for lane in list(detail_dict.get("constituent_lanes") or []):
            lane_id = str(lane.get("lane_id") or "").strip()
            if not lane_id:
                continue
            tracked_detail_by_lane_id[lane_id] = detail_dict
            tracked_summary_by_lane_id[lane_id] = summary_row

    lanes: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in performance_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        standalone_strategy_id = row.get("standalone_strategy_id") or row.get("strategy_key")
        strategy_key = _canonical_strategy_key(
            standalone_strategy_id,
            instrument=row.get("instrument"),
            strategy_family=row.get("strategy_family") or row.get("source_family"),
        )
        seen_keys.add(strategy_key)
        lane_row = lane_universe.get(lane_id, {})
        tracked_detail = tracked_detail_by_lane_id.get(lane_id, {})
        tracked_summary = tracked_summary_by_lane_id.get(lane_id, {})
        trade_rows = sorted(
            _normalize_paper_trade_rows(trade_log_by_strategy.get(strategy_key, [])),
            key=lambda trade: str(trade.get("exit_timestamp") or trade.get("entry_timestamp") or ""),
            reverse=True,
        )
        tracked_trade_rows = _normalize_paper_trade_rows(tracked_detail.get("recent_trades") or [])
        full_history_trade_rows = _complete_trade_rows(
            primary_rows=trade_rows,
            fallback_rows=tracked_trade_rows,
            expected_trade_count=row.get("trade_count") or tracked_summary.get("trade_count"),
        )
        strategy_attribution_rows = _paper_strategy_attribution_rows(
            attribution_rows=attribution_rows,
            strategy_keys=(strategy_key, standalone_strategy_id, lane_id),
        )
        db_path = _resolve_sqlite_database_path(lane_row.get("database_url"))
        evidence = _paper_lane_evidence(
            db_path=db_path,
            lane_id=lane_id,
            standalone_strategy_id=standalone_strategy_id,
            tracked_detail=tracked_detail,
        )
        tracked_status = str(tracked_summary.get("status") or "").upper()
        runtime_attached = bool(tracked_summary.get("runtime_attached"))
        data_stale = bool(tracked_summary.get("data_stale") or paper.get("status", {}).get("stale"))
        reconciling = tracked_status == "RECONCILING" or "RECONCILING" in str(row.get("status") or "").upper()
        healthy = runtime_attached and not data_stale and not reconciling and tracked_status not in {"FAULT", ""}
        timeframe_truth = _paper_timeframe_truth(lane_row, tracked_detail)
        date_range = {
            "start_timestamp": row.get("history_start_timestamp") or _latest_or_earliest(evidence.get("bars", {}).get("preview_rows") or [], "start_ts", earliest=True),
            "end_timestamp": row.get("history_end_timestamp")
            or tracked_summary.get("last_update_timestamp")
            or _latest_or_earliest(evidence.get("bars", {}).get("preview_rows") or [], "end_ts"),
            "session_window": _paper_session_window(row, tracked_summary),
        }
        derived_trade_metrics = _trade_row_metrics(full_history_trade_rows)
        summary_family_breakdown = list(tracked_summary.get("trade_family_breakdown") or [])
        summary_session_breakdown = list(tracked_summary.get("session_breakdown") or [])
        trade_family_breakdown = (
            [dict(item) for item in summary_family_breakdown]
            if summary_family_breakdown
            else _paper_trade_family_breakdown(
                trade_rows=trade_rows or list(full_history_trade_rows.get("rows") or []),
                attribution_rows=strategy_attribution_rows,
            )
        )
        session_breakdown = (
            [dict(item) for item in summary_session_breakdown]
            if summary_session_breakdown
            else _paper_session_breakdown(
                row,
                trade_rows=list(full_history_trade_rows.get("rows") or []),
            )
        )
        latest_trade_summary = _paper_latest_trade_summary(
            summary_row=tracked_summary.get("last_trade_summary"),
            tracked_trade_rows=tracked_trade_rows,
            trade_rows=trade_rows,
        )
        runtime_health_label = (
            "RECONCILING"
            if reconciling
            else "STALE"
            if data_stale
            else "ATTACHED"
            if runtime_attached and not healthy
            else "HEALTHY"
            if healthy
            else "DETACHED"
        )
        lifecycle_truth = _paper_lifecycle_truth(
            evidence=evidence,
            runtime_attached=runtime_attached,
            tracked_detail=tracked_detail,
            tracked_summary=tracked_summary,
        )
        lanes.append(
            {
                "lane_id": f"paper:{lane_id or strategy_key}",
                "paper_lane_id": lane_id or None,
                "strategy_key": strategy_key,
                "display_name": row.get("strategy_name") or tracked_summary.get("display_name") or strategy_key,
                "strategy_label": row.get("strategy_name") or tracked_summary.get("display_name") or strategy_key,
                "standalone_strategy_id": standalone_strategy_id,
                "strategy_family": row.get("strategy_family") or row.get("source_family"),
                "instrument": row.get("instrument"),
                "lane_type": LANE_TYPE_PAPER_RUNTIME,
                "lane_label": _lane_type_label(LANE_TYPE_PAPER_RUNTIME),
                "source_lane": "paper_runtime",
                "source_of_truth": {
                    "primary_artifact": "paper_strategy_performance_snapshot",
                    "lane_database_url": lane_row.get("database_url"),
                    "tracked_strategy_id": tracked_summary.get("strategy_id"),
                    "tracked_artifacts": dict(tracked_detail.get("artifacts") or {}),
                    "truth_provenance": dict(tracked_summary.get("truth_provenance") or tracked_detail.get("truth_provenance") or {}),
                },
                "run_identity": {
                    "run_id": paper.get("status", {}).get("session_date"),
                    "run_timestamp": generated_at,
                    "study_id": lane_id or strategy_key,
                    "study_key": lane_id or strategy_key,
                    "candidate_id": None,
                    "contract_version": "paper_runtime_v1",
                },
                "config_identity": {
                    "benchmark_label": tracked_summary.get("benchmark_designation"),
                    "config_source": tracked_summary.get("config_source"),
                    "benchmark_designation": tracked_summary.get("benchmark_designation"),
                    "entry_model": None,
                    "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                },
                "lifecycle_truth": lifecycle_truth,
                "mode_truth": {
                    "study_mode": "paper_runtime",
                    "mode_label": "Paper Runtime",
                    "execution_semantics": "Tracked paper-runtime execution truth from lane-local order intents, fills, and runtime state.",
                    "intrabar_execution_authoritative": True,
                },
                "timeframe_truth": timeframe_truth,
                "date_range": date_range,
                "metrics": {
                    "net_pnl": _metric_value(row.get("cumulative_pnl")),
                    "realized_pnl": _metric_value(row.get("realized_pnl")),
                    "open_pnl": _metric_value(
                        tracked_summary.get("open_pnl") or row.get("unrealized_pnl"),
                        unavailable_reason=tracked_summary.get("open_pnl_unavailable_reason") or row.get("pnl_unavailable_reason"),
                    ),
                    "trade_count": _metric_value(row.get("trade_count")),
                    "long_trades": _metric_value(
                        tracked_summary.get("long_trade_count") if tracked_summary.get("long_trade_count") is not None else derived_trade_metrics.get("long_trades"),
                        unavailable_reason="Paper trade direction counts are unavailable for this lane." if tracked_summary.get("long_trade_count") is None and derived_trade_metrics.get("long_trades") is None else None,
                    ),
                    "short_trades": _metric_value(
                        tracked_summary.get("short_trade_count") if tracked_summary.get("short_trade_count") is not None else derived_trade_metrics.get("short_trades"),
                        unavailable_reason="Paper trade direction counts are unavailable for this lane." if tracked_summary.get("short_trade_count") is None and derived_trade_metrics.get("short_trades") is None else None,
                    ),
                    "winners": _metric_value(
                        tracked_summary.get("winner_count") if tracked_summary.get("winner_count") is not None else derived_trade_metrics.get("winners"),
                        unavailable_reason="Paper winner counts are unavailable for this lane." if tracked_summary.get("winner_count") is None and derived_trade_metrics.get("winners") is None else None,
                    ),
                    "losers": _metric_value(
                        tracked_summary.get("loser_count") if tracked_summary.get("loser_count") is not None else derived_trade_metrics.get("losers"),
                        unavailable_reason="Paper loser counts are unavailable for this lane." if tracked_summary.get("loser_count") is None and derived_trade_metrics.get("losers") is None else None,
                    ),
                    "win_rate": _metric_value(
                        tracked_summary.get("win_rate") if tracked_summary.get("win_rate") is not None else derived_trade_metrics.get("win_rate"),
                        unavailable_reason="Paper win rate requires closed-trade outcome counts." if tracked_summary.get("win_rate") is None and derived_trade_metrics.get("win_rate") is None else None,
                    ),
                    "average_trade": _metric_value(
                        tracked_summary.get("average_trade_pnl") if tracked_summary.get("average_trade_pnl") is not None else derived_trade_metrics.get("average_trade"),
                        unavailable_reason="Paper average trade is unavailable for this lane." if tracked_summary.get("average_trade_pnl") is None and derived_trade_metrics.get("average_trade") is None else None,
                    ),
                    "profit_factor": _metric_value(
                        tracked_summary.get("profit_factor") if tracked_summary.get("profit_factor") is not None else derived_trade_metrics.get("profit_factor"),
                        unavailable_reason="Paper profit factor is unavailable for this lane." if tracked_summary.get("profit_factor") is None and derived_trade_metrics.get("profit_factor") is None else None,
                    ),
                    "max_drawdown": _metric_value(
                        tracked_summary.get("max_drawdown") or row.get("max_drawdown")
                    ),
                    "session_breakdown": {
                        "available": bool(session_breakdown),
                        "value": session_breakdown,
                        "reason": None if session_breakdown else full_history_trade_rows.get("reason") or "Paper strategy performance did not expose session breakdown counts.",
                    },
                    "trade_family_breakdown": {
                        "available": bool(trade_family_breakdown),
                        "value": trade_family_breakdown,
                        "reason": None if trade_family_breakdown else full_history_trade_rows.get("reason") or "Paper closed-trade attribution is unavailable because persisted family labels were not present for this lane.",
                    },
                    "latest_trade_summary": {
                        "available": bool(latest_trade_summary),
                        "value": latest_trade_summary,
                        "reason": None if latest_trade_summary else "Paper lane has no latest trade summary yet.",
                    },
                    "latest_status": _metric_value(
                        tracked_summary.get("status") or row.get("status"),
                    ),
                    "latest_update_timestamp": _metric_value(
                        tracked_summary.get("last_update_timestamp") or row.get("latest_activity_timestamp")
                    ),
                },
                "runtime_health": {
                    "label": runtime_health_label,
                    "attached": runtime_attached,
                    "stale": data_stale,
                    "reconciling": reconciling,
                    "healthy": healthy,
                    "status_reason": tracked_summary.get("status_reason")
                    or ("Paper runtime status is stale." if data_stale else None)
                    or ("Runtime is not currently attached." if not runtime_attached else "Paper runtime is attached."),
                },
                "evidence": evidence,
                "provenance": {
                    "summary": "Paper metrics come from strategy-performance snapshots, tracked paper lane detail, and lane-local SQLite truth.",
                    "metric_sources": {
                        "pnl": "paper.strategy_performance.rows plus tracked paper open-position state where available",
                        "trade_counts": "paper.strategy_performance.rows, tracked paper summaries, and full-history closed-trade rows when summaries omit breakdown metrics",
                        "drilldown": "lane-local SQLite bars/signals/order_intents/fills/state_snapshots/reconciliation_events",
                        "family_breakdown": "paper.strategy_performance.attribution rows filtered to this standalone strategy, tracked-strategy exact closed-trade breakdown rows when published, or trade-log / complete tracked-trade fallback when those are absent",
                    },
                    "lifecycle_truth_source": "paper runtime lane contract plus lane-local order-intent/fill/state evidence where available",
                },
            }
        )

    for strategy_id, summary in tracked_rows.items():
        if not summary:
            continue
        detail = dict(tracked_details_by_strategy.get(strategy_id) or {})
        if not detail:
            continue
        if any(lane_id for lane_id in [str(row.get("lane_id") or "") for row in list(detail.get("constituent_lanes") or [])] if f"paper:{lane_id}" in {lane["lane_id"] for lane in lanes}):
            continue
        observed_instruments = list(summary.get("observed_instruments") or [])
        instrument = observed_instruments[0] if observed_instruments else None
        strategy_key = _canonical_strategy_key(summary.get("strategy_id"), instrument=instrument, strategy_family=None)
        if strategy_key in seen_keys:
            continue
        tracked_trade_rows = _normalize_paper_trade_rows(detail.get("recent_trades") or [])
        full_history_trade_rows = _complete_trade_rows(
            primary_rows=[],
            fallback_rows=tracked_trade_rows,
            expected_trade_count=summary.get("trade_count"),
        )
        derived_trade_metrics = _trade_row_metrics(full_history_trade_rows)
        session_breakdown = (
            [dict(item) for item in list(summary.get("session_breakdown") or [])]
            if summary.get("session_breakdown")
            else _paper_session_breakdown({}, trade_rows=list(full_history_trade_rows.get("rows") or []))
        )
        trade_family_breakdown = (
            [dict(item) for item in list(summary.get("trade_family_breakdown") or [])]
            if summary.get("trade_family_breakdown")
            else _paper_trade_family_breakdown(
                trade_rows=list(full_history_trade_rows.get("rows") or []),
                attribution_rows=[],
            )
        )
        latest_trade_summary = _paper_latest_trade_summary(
            summary_row=summary.get("last_trade_summary"),
            tracked_trade_rows=tracked_trade_rows,
            trade_rows=[],
        )
        lanes.append(
            {
                "lane_id": f"paper:{summary.get('strategy_id')}",
                "paper_lane_id": None,
                "strategy_key": strategy_key,
                "display_name": summary.get("display_name") or strategy_key,
                "strategy_label": summary.get("display_name") or strategy_key,
                "standalone_strategy_id": None,
                "strategy_family": None,
                "instrument": instrument,
                "lane_type": LANE_TYPE_PAPER_RUNTIME,
                "lane_label": _lane_type_label(LANE_TYPE_PAPER_RUNTIME),
                "source_lane": "paper_runtime",
                "source_of_truth": {
                    "primary_artifact": "paper_tracked_strategies_snapshot",
                    "tracked_strategy_id": summary.get("strategy_id"),
                    "tracked_artifacts": dict(detail.get("artifacts") or {}),
                    "truth_provenance": dict(summary.get("truth_provenance") or detail.get("truth_provenance") or {}),
                },
                "run_identity": {
                    "run_id": paper.get("status", {}).get("session_date"),
                    "run_timestamp": generated_at,
                    "study_id": summary.get("strategy_id"),
                    "study_key": summary.get("strategy_id"),
                    "candidate_id": None,
                    "contract_version": "paper_runtime_v1",
                },
                "config_identity": {
                    "benchmark_label": summary.get("benchmark_designation"),
                    "config_source": summary.get("config_source"),
                    "benchmark_designation": summary.get("benchmark_designation"),
                    "entry_model": None,
                    "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
                },
                "lifecycle_truth": _paper_lifecycle_truth(
                    evidence={},
                    runtime_attached=bool(summary.get("runtime_attached")),
                    tracked_detail=detail,
                    tracked_summary=summary,
                ),
                "mode_truth": {
                    "study_mode": "paper_runtime",
                    "mode_label": "Paper Runtime",
                    "execution_semantics": "Tracked paper-runtime execution truth from tracked strategy summary/detail artifacts.",
                    "intrabar_execution_authoritative": True,
                },
                "timeframe_truth": {
                    "structural_signal_timeframe": None,
                    "execution_timeframe": None,
                    "artifact_timeframe": None,
                    "execution_timeframe_role": None,
                    "explicit": False,
                    "unavailable_reason": "Tracked strategy detail does not currently publish explicit timeframe truth.",
                },
                "date_range": {
                    "start_timestamp": None,
                    "end_timestamp": summary.get("last_update_timestamp"),
                    "session_window": summary.get("current_session_segment"),
                },
                "metrics": {
                    "net_pnl": _metric_value(summary.get("cumulative_pnl")),
                    "realized_pnl": _metric_value(summary.get("realized_pnl")),
                    "open_pnl": _metric_value(
                        summary.get("open_pnl"),
                        unavailable_reason=summary.get("open_pnl_unavailable_reason"),
                    ),
                    "trade_count": _metric_value(summary.get("trade_count")),
                    "long_trades": _metric_value(summary.get("long_trade_count") if summary.get("long_trade_count") is not None else derived_trade_metrics.get("long_trades")),
                    "short_trades": _metric_value(summary.get("short_trade_count") if summary.get("short_trade_count") is not None else derived_trade_metrics.get("short_trades")),
                    "winners": _metric_value(summary.get("winner_count") if summary.get("winner_count") is not None else derived_trade_metrics.get("winners")),
                    "losers": _metric_value(summary.get("loser_count") if summary.get("loser_count") is not None else derived_trade_metrics.get("losers")),
                    "win_rate": _metric_value(summary.get("win_rate") if summary.get("win_rate") is not None else derived_trade_metrics.get("win_rate")),
                    "average_trade": _metric_value(summary.get("average_trade_pnl") if summary.get("average_trade_pnl") is not None else derived_trade_metrics.get("average_trade")),
                    "profit_factor": _metric_value(summary.get("profit_factor") if summary.get("profit_factor") is not None else derived_trade_metrics.get("profit_factor")),
                    "max_drawdown": _metric_value(summary.get("max_drawdown")),
                    "session_breakdown": {
                        "available": bool(session_breakdown),
                        "value": session_breakdown,
                        "reason": None if session_breakdown else full_history_trade_rows.get("reason") or "Tracked strategy summaries do not yet publish per-session breakdown rows.",
                    },
                    "trade_family_breakdown": {
                        "available": bool(trade_family_breakdown),
                        "value": trade_family_breakdown,
                        "reason": None if trade_family_breakdown else full_history_trade_rows.get("reason") or "Tracked strategy summaries do not yet publish full-history per-family breakdown rows for this strategy.",
                    },
                    "latest_trade_summary": {
                        "available": bool(latest_trade_summary),
                        "value": latest_trade_summary,
                        "reason": None if latest_trade_summary else "No tracked trade summary is available yet.",
                    },
                    "latest_status": _metric_value(summary.get("status")),
                    "latest_update_timestamp": _metric_value(summary.get("last_update_timestamp")),
                },
                "runtime_health": {
                    "label": str(summary.get("status") or "UNKNOWN"),
                    "attached": bool(summary.get("runtime_attached")),
                    "stale": bool(summary.get("data_stale")),
                    "reconciling": str(summary.get("status") or "").upper() == "RECONCILING",
                    "healthy": str(summary.get("status") or "").upper() in {"READY", "IN_POSITION"},
                    "status_reason": summary.get("status_reason"),
                },
                "evidence": {
                    "bars": _evidence_ref(
                        available=bool(detail.get("recent_bars")),
                        count=len(list(detail.get("recent_bars") or [])),
                        preview_rows=list(detail.get("recent_bars") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent bars." if not detail.get("recent_bars") else None,
                    ),
                    "signals": _evidence_ref(
                        available=bool(detail.get("recent_signals")),
                        count=len(list(detail.get("recent_signals") or [])),
                        preview_rows=list(detail.get("recent_signals") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent signals." if not detail.get("recent_signals") else None,
                    ),
                    "order_intents": _evidence_ref(
                        available=bool(detail.get("recent_order_intents")),
                        count=len(list(detail.get("recent_order_intents") or [])),
                        preview_rows=list(detail.get("recent_order_intents") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent order intents." if not detail.get("recent_order_intents") else None,
                    ),
                    "fills": _evidence_ref(
                        available=bool(detail.get("recent_fills")),
                        count=len(list(detail.get("recent_fills") or [])),
                        preview_rows=list(detail.get("recent_fills") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent fills." if not detail.get("recent_fills") else None,
                    ),
                    "state_snapshots": _evidence_ref(
                        available=bool(detail.get("recent_state_snapshots")),
                        count=len(list(detail.get("recent_state_snapshots") or [])),
                        preview_rows=list(detail.get("recent_state_snapshots") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent state snapshots." if not detail.get("recent_state_snapshots") else None,
                    ),
                    "execution_slices": _evidence_ref(
                        available=False,
                        count=0,
                        preview_rows=[],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Paper runtime detail does not expose execution slices as a separate artifact.",
                    ),
                    "session_evidence": _evidence_ref(
                        available=bool(summary.get("current_session_segment")),
                        count=1 if summary.get("current_session_segment") else 0,
                        preview_rows=[{"current_session_segment": summary.get("current_session_segment"), "session_allowed": summary.get("session_allowed")}],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy summary has no current session segment." if not summary.get("current_session_segment") else None,
                    ),
                    "readiness_artifacts": _evidence_ref(
                        available=bool(summary.get("health_flags")),
                        count=len(dict(summary.get("health_flags") or {})),
                        preview_rows=[dict(summary.get("health_flags") or {})],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy summary has no readiness/health flags." if not summary.get("health_flags") else None,
                    ),
                    "trade_lifecycle": _evidence_ref(
                        available=bool(detail.get("recent_trades")),
                        count=len(list(detail.get("recent_trades") or [])),
                        preview_rows=list(detail.get("recent_trades") or [])[:_PREVIEW_ROW_LIMIT],
                        ref={"tracked_strategy_id": summary.get("strategy_id")},
                        unavailable_reason="Tracked strategy detail has no recent trade lifecycle preview." if not detail.get("recent_trades") else None,
                    ),
                },
                "provenance": {
                    "summary": "Tracked paper metrics come from tracked strategy summaries/details and remain separate from replay artifacts.",
                    "metric_sources": {
                        "pnl": "tracked_strategies summary rows",
                        "trade_metrics": "tracked_strategies summary rows, with complete recent trade-lifecycle preview fallback when the preview covers the full persisted trade count",
                        "drilldown": "tracked_strategies detail rows",
                    },
                    "lifecycle_truth_source": "tracked paper strategy contract; drill-down completeness depends on tracked detail availability",
                },
            }
        )

    return lanes


def _paper_lane_evidence(
    *,
    db_path: Path | None,
    lane_id: str | None,
    standalone_strategy_id: Any,
    tracked_detail: dict[str, Any],
) -> dict[str, Any]:
    recent_bars = _merge_preview_rows(
        _latest_rows_from_table(db_path, "bars", "end_ts", limit=_PREVIEW_ROW_LIMIT),
        list(tracked_detail.get("recent_bars") or []),
        key_fields=("bar_id", "end_ts", "timestamp"),
    )[:_PREVIEW_ROW_LIMIT]
    recent_signals = _merge_preview_rows(
        _latest_payload_rows_from_table(db_path, "signals", "created_at", limit=_PREVIEW_ROW_LIMIT),
        list(tracked_detail.get("recent_signals") or []),
        key_fields=("signal_id", "signal_timestamp", "created_at", "timestamp"),
    )[:_PREVIEW_ROW_LIMIT]
    recent_order_intents = _merge_preview_rows(
        _filter_strategy_rows(
        _latest_rows_from_table(db_path, "order_intents", "created_at", limit=_PREVIEW_ROW_LIMIT * 4),
        lane_id=lane_id,
        standalone_strategy_id=standalone_strategy_id,
        ),
        list(tracked_detail.get("recent_order_intents") or []),
        key_fields=("order_intent_id", "created_at"),
    )[:_PREVIEW_ROW_LIMIT]
    recent_fills = _merge_preview_rows(
        _filter_strategy_rows(
        _latest_rows_from_table(db_path, "fills", "fill_timestamp", limit=_PREVIEW_ROW_LIMIT * 4),
        lane_id=lane_id,
        standalone_strategy_id=standalone_strategy_id,
        ),
        list(tracked_detail.get("recent_fills") or []),
        key_fields=("fill_id", "order_intent_id", "fill_timestamp"),
    )[:_PREVIEW_ROW_LIMIT]
    recent_snapshots = _normalize_state_snapshot_rows(
        _merge_preview_rows(
            _filter_strategy_rows(
                _latest_rows_from_table(db_path, "strategy_state_snapshots", "updated_at", limit=_PREVIEW_ROW_LIMIT * 2),
                lane_id=lane_id,
                standalone_strategy_id=standalone_strategy_id,
            ),
            list(tracked_detail.get("recent_state_snapshots") or []),
            key_fields=("snapshot_id", "updated_at", "timestamp"),
        )[:_PREVIEW_ROW_LIMIT]
    )
    recent_reconciliation = _merge_preview_rows(
        _filter_strategy_rows(
            _latest_rows_from_table(db_path, "reconciliation_events", "occurred_at", limit=_PREVIEW_ROW_LIMIT * 2),
            lane_id=lane_id,
            standalone_strategy_id=standalone_strategy_id,
        ),
        list(tracked_detail.get("recent_reconciliation_events") or []),
        key_fields=("event_id", "occurred_at", "timestamp"),
    )[:_PREVIEW_ROW_LIMIT]
    recent_trades = _merge_preview_rows(
        list(tracked_detail.get("recent_trades") or []),
        [],
        key_fields=("trade_id", "exit_timestamp", "entry_timestamp"),
    )[:_PREVIEW_ROW_LIMIT]
    readiness_rows = []
    if tracked_detail:
        readiness_rows.append(
            {
                "current_session_segment": tracked_detail.get("current_session_segment"),
                "status": tracked_detail.get("status"),
                "status_reason": tracked_detail.get("status_reason"),
                "health_flags": tracked_detail.get("health_flags"),
                "latest_signal_summary": tracked_detail.get("latest_signal_summary"),
            }
        )
    return {
        "bars": _evidence_ref(
            available=bool(recent_bars),
            count=len(recent_bars),
            preview_rows=recent_bars,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="No recent bars were available in the lane-local SQLite store." if not recent_bars else None,
        ),
        "signals": _evidence_ref(
            available=bool(recent_signals),
            count=len(recent_signals),
            preview_rows=recent_signals,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="Signals are not available in the lane-local SQLite store." if not recent_signals else None,
        ),
        "order_intents": _evidence_ref(
            available=bool(recent_order_intents),
            count=len(recent_order_intents),
            preview_rows=recent_order_intents,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="Order intents are not available in the lane-local SQLite store." if not recent_order_intents else None,
        ),
        "fills": _evidence_ref(
            available=bool(recent_fills),
            count=len(recent_fills),
            preview_rows=recent_fills,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="Fills are not available in the lane-local SQLite store." if not recent_fills else None,
        ),
        "state_snapshots": _evidence_ref(
            available=bool(recent_snapshots),
            count=len(recent_snapshots),
            preview_rows=recent_snapshots,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="State snapshots are not available in the lane-local SQLite store." if not recent_snapshots else None,
        ),
        "execution_slices": _evidence_ref(
            available=False,
            count=0,
            preview_rows=[],
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="Paper runtime evidence does not publish execution slices as a separate artifact.",
        ),
        "session_evidence": _evidence_ref(
            available=bool(recent_reconciliation),
            count=len(recent_reconciliation),
            preview_rows=recent_reconciliation,
            ref={"database_path": str(db_path) if db_path is not None else None},
            unavailable_reason="No recent reconciliation/session evidence rows are available." if not recent_reconciliation else None,
        ),
        "readiness_artifacts": _evidence_ref(
            available=bool(readiness_rows),
            count=len(readiness_rows),
            preview_rows=readiness_rows,
            ref={"tracked_detail": bool(tracked_detail)},
            unavailable_reason="No tracked readiness artifact is available for this paper lane." if not readiness_rows else None,
        ),
        "trade_lifecycle": _evidence_ref(
            available=bool(recent_trades),
            count=len(recent_trades),
            preview_rows=recent_trades,
            ref={"database_path": str(db_path) if db_path is not None else None, "tracked_detail": bool(tracked_detail)},
            unavailable_reason="No recent trade lifecycle preview is available for this paper lane." if not recent_trades else None,
        ),
    }


def _comparison_presets_for_strategy(strategy_key: str, lanes: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    presets: list[dict[str, Any]] = []
    benchmark_lane = next((lane for lane in lanes if lane.get("lane_type") == LANE_TYPE_BENCHMARK_REPLAY), None)
    paper_lane = next((lane for lane in lanes if lane.get("lane_type") == LANE_TYPE_PAPER_RUNTIME), None)
    research_lane = next((lane for lane in lanes if lane.get("lane_type") == LANE_TYPE_RESEARCH_EXECUTION), None)
    if benchmark_lane is not None and paper_lane is not None:
        presets.append(
            _build_comparison_preset(
                comparison_id=f"{strategy_key}:benchmark_vs_paper",
                comparison_type="benchmark_vs_paper_runtime",
                left_lane=benchmark_lane,
                right_lane=paper_lane,
            )
        )
    if benchmark_lane is not None and research_lane is not None:
        presets.append(
            _build_comparison_preset(
                comparison_id=f"{strategy_key}:baseline_vs_research",
                comparison_type="baseline_parity_vs_research_execution",
                left_lane=benchmark_lane,
                right_lane=research_lane,
            )
        )
    return presets


def _build_comparison_preset(
    *,
    comparison_id: str,
    comparison_type: str,
    left_lane: dict[str, Any],
    right_lane: dict[str, Any],
) -> dict[str, Any]:
    metric_keys = (
        "net_pnl",
        "realized_pnl",
        "open_pnl",
        "trade_count",
        "long_trades",
        "short_trades",
        "winners",
        "losers",
        "win_rate",
        "average_trade",
        "profit_factor",
        "max_drawdown",
    )
    metric_rows: list[dict[str, Any]] = []
    for metric_key in metric_keys:
        left_metric = dict(_nested_get(left_lane, "metrics", metric_key) or {})
        right_metric = dict(_nested_get(right_lane, "metrics", metric_key) or {})
        delta = _metric_delta(left_metric.get("value"), right_metric.get("value"))
        metric_rows.append(
            {
                "metric_key": metric_key,
                "left": left_metric,
                "right": right_metric,
                "delta": delta,
            }
        )
    return {
        "comparison_id": comparison_id,
        "comparison_type": comparison_type,
        "label": comparison_type.replace("_", " ").upper(),
        "left_lane_id": left_lane.get("lane_id"),
        "right_lane_id": right_lane.get("lane_id"),
        "left_lane": _comparison_lane_summary(left_lane),
        "right_lane": _comparison_lane_summary(right_lane),
        "metrics": metric_rows,
    }


def _comparison_lane_summary(lane: dict[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": lane.get("lane_id"),
        "source_lane": lane.get("source_lane"),
        "lane_label": lane.get("lane_label"),
        "lane_type": lane.get("lane_type"),
        "display_name": lane.get("display_name"),
        "primary_truth_source": _nested_get(lane, "source_of_truth", "primary_artifact"),
        "truth_provenance": dict(_nested_get(lane, "source_of_truth", "truth_provenance") or {}),
        "lifecycle_truth": dict(lane.get("lifecycle_truth") or {}),
        "timeframe_truth": dict(lane.get("timeframe_truth") or {}),
        "mode_truth": dict(lane.get("mode_truth") or {}),
        "date_range": dict(lane.get("date_range") or {}),
        "execution_semantics": _nested_get(lane, "mode_truth", "execution_semantics"),
    }


def _metric_delta(left: Any, right: Any) -> dict[str, Any] | None:
    left_decimal = _decimal_or_none(left)
    right_decimal = _decimal_or_none(right)
    if left_decimal is None or right_decimal is None:
        return None
    return {"value": _decimal_to_string(right_decimal - left_decimal)}


def _replay_lane_type(study_mode: str, summary: dict[str, Any], meta: dict[str, Any], *, explicit_study_mode: bool = True) -> str:
    if not explicit_study_mode and not str(meta.get("pnl_truth_basis") or "").strip() and summary.get("bar_count") is not None:
        return LANE_TYPE_HISTORICAL_PLAYBACK
    if study_mode == "research_execution_mode":
        return LANE_TYPE_RESEARCH_EXECUTION
    if study_mode == "baseline_parity_mode":
        return LANE_TYPE_BENCHMARK_REPLAY
    if str(meta.get("pnl_truth_basis") or "").upper() == "BASELINE_FILL_TRUTH":
        return LANE_TYPE_BENCHMARK_REPLAY
    if summary.get("bar_count") is not None:
        return LANE_TYPE_HISTORICAL_PLAYBACK
    return LANE_TYPE_HISTORICAL_PLAYBACK


def _replay_execution_semantics(meta: dict[str, Any], study_mode: str) -> str:
    entry_model = str(meta.get("entry_model") or "BASELINE_NEXT_BAR_OPEN")
    if study_mode == "research_execution_mode":
        return f"Research execution study using {entry_model} with explicit execution detail separation."
    return f"Replay benchmark semantics using {entry_model} on completed-bar evaluation."


def _replay_lifecycle_truth(*, meta: dict[str, Any], lane_type: str, study_mode: str) -> dict[str, Any]:
    pnl_truth_basis = str(meta.get("pnl_truth_basis") or "").strip().upper()
    raw_lifecycle_truth_class = str(meta.get("lifecycle_truth_class") or "").strip().upper()
    capability_rows = list(meta.get("entry_model_capabilities") or [])
    authoritative_execution_events = list(meta.get("authoritative_execution_events") or [])
    lifecycle_records = list(meta.get("authoritative_trade_lifecycle_records") or [])
    unsupported_reason = str(meta.get("unsupported_reason") or "").strip() or None

    if raw_lifecycle_truth_class == "FULL_AUTHORITATIVE_LIFECYCLE":
        lifecycle_class = LIFECYCLE_TRUTH_FULL
        reason = "Study publishes full authoritative lifecycle truth for this replay lane."
    elif raw_lifecycle_truth_class in {"HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT", "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"}:
        lifecycle_class = LIFECYCLE_TRUTH_HYBRID
        reason = (
            "Study publishes authoritative entry truth, but lifecycle truth remains partial or hybrid and should not be treated as full lifecycle authority."
        )
    elif raw_lifecycle_truth_class == "BASELINE_PARITY_ONLY":
        lifecycle_class = LIFECYCLE_TRUTH_BASELINE_ONLY
        reason = "Replay artifact exposes legacy benchmark truth only; it remains useful benchmark evidence but not full lifecycle execution truth."
    elif raw_lifecycle_truth_class == "UNSUPPORTED_ENTRY_MODEL":
        lifecycle_class = LIFECYCLE_TRUTH_UNSUPPORTED
        reason = unsupported_reason or "Requested execution semantics are unsupported for this strategy family or artifact."
    elif pnl_truth_basis == PNL_TRUTH_BASIS_ENRICHED:
        lifecycle_class = LIFECYCLE_TRUTH_FULL
        reason = "Study publishes enriched execution truth with authoritative lifecycle detail for this replay lane."
    elif pnl_truth_basis == PNL_TRUTH_BASIS_HYBRID:
        lifecycle_class = LIFECYCLE_TRUTH_HYBRID
        reason = "Study publishes hybrid entry truth with baseline-style exit truth; it should not be treated as full lifecycle authority."
    elif pnl_truth_basis == PNL_TRUTH_BASIS_UNSUPPORTED:
        lifecycle_class = LIFECYCLE_TRUTH_UNSUPPORTED
        reason = unsupported_reason or "Requested execution semantics are unsupported for this strategy family or artifact."
    elif pnl_truth_basis == PNL_TRUTH_BASIS_BASELINE:
        lifecycle_class = LIFECYCLE_TRUTH_BASELINE_ONLY
        reason = "Replay artifact exposes baseline fill truth only; it remains useful benchmark evidence but not full lifecycle execution truth."
    else:
        lifecycle_class = LIFECYCLE_TRUTH_BASELINE_ONLY
        reason = (
            "Legacy playback artifact does not publish explicit lifecycle-truth metadata; compatibility classification defaults to baseline-only."
            if lane_type == LANE_TYPE_HISTORICAL_PLAYBACK
            else "Replay artifact does not publish explicit lifecycle-truth metadata; classification defaults to baseline-only."
        )

    return {
        "class": lifecycle_class,
        "label": _lifecycle_truth_label(lifecycle_class),
        "reason": reason,
        "study_mode": study_mode,
        "raw_lifecycle_truth_class": raw_lifecycle_truth_class or None,
        "pnl_truth_basis": pnl_truth_basis or None,
        "unsupported_reason": unsupported_reason,
        "authoritative_intrabar_available": bool(meta.get("authoritative_intrabar_available") or meta.get("intrabar_execution_authoritative")),
        "authoritative_execution_event_count": len(authoritative_execution_events),
        "authoritative_trade_lifecycle_record_count": len(lifecycle_records),
        "capability_rows": capability_rows,
        "classification_source": "strategy_study.meta",
        "truth_provenance": dict(meta.get("truth_provenance") or {}),
    }


def _paper_lifecycle_truth(
    *,
    evidence: dict[str, Any],
    runtime_attached: bool,
    tracked_detail: dict[str, Any],
    tracked_summary: dict[str, Any],
) -> dict[str, Any]:
    raw_lifecycle_truth_class = str(tracked_summary.get("lifecycle_truth_class") or tracked_detail.get("lifecycle_truth_class") or "").strip().upper()
    truth_provenance = dict(tracked_summary.get("truth_provenance") or tracked_detail.get("truth_provenance") or {})
    if raw_lifecycle_truth_class:
        lifecycle_class, reason = _normalized_lifecycle_truth_class(
            raw_lifecycle_truth_class=raw_lifecycle_truth_class,
            fallback_reason=(
                "Paper-runtime lifecycle truth comes from the tracked strategy lifecycle contract and remains provenance-separated from replay evidence."
            ),
        )
    else:
        lifecycle_class = LIFECYCLE_TRUTH_FULL
        reason = ""
    evidence_rows_available = any(
        bool(_nested_get(evidence, evidence_key, "available"))
        for evidence_key in ("order_intents", "fills", "state_snapshots", "signals")
    )
    tracked_detail_available = bool(tracked_detail)
    has_latest_trade = bool(tracked_summary.get("last_trade_summary"))
    if not reason:
        if evidence_rows_available:
            reason = "Paper-runtime lifecycle truth is derived from lane-local intents, fills, signals, and state snapshots for this tracked lane."
        elif tracked_detail_available or has_latest_trade:
            reason = "Paper-runtime lifecycle truth is derived from tracked runtime summaries/details, but drill-down evidence previews are partial for this lane."
        else:
            reason = "Paper-runtime result is sourced from tracked runtime snapshots only; lifecycle drill-down evidence is currently limited even though replay truth remains separate."
    return {
        "class": lifecycle_class,
        "label": _lifecycle_truth_label(lifecycle_class),
        "reason": reason,
        "study_mode": "paper_runtime",
        "raw_lifecycle_truth_class": raw_lifecycle_truth_class or None,
        "pnl_truth_basis": "PAPER_RUNTIME_LEDGER",
        "unsupported_reason": None,
        "authoritative_intrabar_available": True,
        "authoritative_execution_event_count": _paper_lifecycle_event_count(evidence),
        "authoritative_trade_lifecycle_record_count": _paper_lifecycle_record_count(evidence, tracked_summary),
        "capability_rows": [],
        "classification_source": "tracked_paper_strategy_contract" if raw_lifecycle_truth_class else "paper_runtime_lane_contract",
        "runtime_attached": runtime_attached,
        "truth_provenance": truth_provenance,
    }


def _paper_lifecycle_event_count(evidence: dict[str, Any]) -> int:
    return sum(
        int(_nested_get(evidence, evidence_key, "count") or 0)
        for evidence_key in ("signals", "order_intents", "fills")
    )


def _paper_lifecycle_record_count(evidence: dict[str, Any], tracked_summary: dict[str, Any]) -> int:
    state_snapshot_count = int(_nested_get(evidence, "state_snapshots", "count") or 0)
    trade_lifecycle_count = int(_nested_get(evidence, "trade_lifecycle", "count") or 0)
    latest_trade_count = 1 if tracked_summary.get("last_trade_summary") else 0
    return state_snapshot_count + trade_lifecycle_count + latest_trade_count


def _lifecycle_truth_label(lifecycle_class: str) -> str:
    mapping = {
        LIFECYCLE_TRUTH_FULL: "Full Lifecycle Truth",
        LIFECYCLE_TRUTH_HYBRID: "Hybrid Entry / Baseline Exit",
        LIFECYCLE_TRUTH_BASELINE_ONLY: "Baseline Only",
        LIFECYCLE_TRUTH_UNSUPPORTED: "Unsupported",
    }
    return mapping.get(lifecycle_class, lifecycle_class.replace("_", " ").title())


def _normalized_lifecycle_truth_class(*, raw_lifecycle_truth_class: str, fallback_reason: str) -> tuple[str, str]:
    mapping = {
        "FULL_AUTHORITATIVE_LIFECYCLE": (
            LIFECYCLE_TRUTH_FULL,
            fallback_reason,
        ),
        "HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT": (
            LIFECYCLE_TRUTH_HYBRID,
            "Lifecycle truth remains hybrid: authoritative entry truth is available, but exit or full lifecycle authority stays partial.",
        ),
        "AUTHORITATIVE_INTRABAR_ENTRY_ONLY": (
            LIFECYCLE_TRUTH_HYBRID,
            "Lifecycle truth remains partial: authoritative intrabar entry detail exists, but full entry-to-exit lifecycle authority is not complete.",
        ),
        "BASELINE_PARITY_ONLY": (
            LIFECYCLE_TRUTH_BASELINE_ONLY,
            "Lifecycle truth is baseline-only for this lane contract.",
        ),
        "UNSUPPORTED_ENTRY_MODEL": (
            LIFECYCLE_TRUTH_UNSUPPORTED,
            "Requested execution semantics are unsupported for this lane contract.",
        ),
    }
    return mapping.get(raw_lifecycle_truth_class, (LIFECYCLE_TRUTH_BASELINE_ONLY, fallback_reason))


def _study_mode_label(study_mode: str) -> str:
    mapping = {
        "baseline_parity_mode": "Legacy Benchmark",
        "research_execution_mode": "Research Execution",
        "live_execution_mode": "Live Execution",
        "paper_runtime": "Paper Runtime",
    }
    return mapping.get(study_mode, study_mode.replace("_", " ").title())


def _lane_type_label(lane_type: str) -> str:
    mapping = {
        LANE_TYPE_BENCHMARK_REPLAY: "Benchmark Replay",
        LANE_TYPE_PAPER_RUNTIME: "Paper Runtime",
        LANE_TYPE_HISTORICAL_PLAYBACK: "Historical Playback",
        LANE_TYPE_RESEARCH_EXECUTION: "Research Execution",
    }
    return mapping.get(lane_type, lane_type.replace("_", " ").title())


def _lane_priority(lane_type: str) -> int:
    order = {
        LANE_TYPE_BENCHMARK_REPLAY: 0,
        LANE_TYPE_PAPER_RUNTIME: 1,
        LANE_TYPE_RESEARCH_EXECUTION: 2,
        LANE_TYPE_HISTORICAL_PLAYBACK: 3,
    }
    return order.get(lane_type, 99)


def _default_lane_id(lanes: Sequence[dict[str, Any]]) -> str | None:
    if not lanes:
        return None
    for desired in (LANE_TYPE_BENCHMARK_REPLAY, LANE_TYPE_PAPER_RUNTIME, LANE_TYPE_RESEARCH_EXECUTION, LANE_TYPE_HISTORICAL_PLAYBACK):
        match = next((lane for lane in lanes if lane.get("lane_type") == desired), None)
        if match is not None:
            return str(match.get("lane_id") or "")
    return str(lanes[0].get("lane_id") or "")


def _paper_timeframe_truth(lane_row: dict[str, Any], tracked_detail: dict[str, Any]) -> dict[str, Any]:
    artifact_rows = list(_nested_get(tracked_detail, "artifacts", "lane_artifacts") or [])
    timeframe = lane_row.get("timeframe") or None
    for row in artifact_rows:
        artifacts = dict(row.get("artifacts") or {})
        lane_dir = str(artifacts.get("lane_dir") or "")
        if "1m" in lane_dir or "5m" in lane_dir:
            timeframe = timeframe or ("1m" if "1m" in lane_dir else "5m")
            break
    if timeframe:
        return {
            "structural_signal_timeframe": timeframe,
            "execution_timeframe": timeframe,
            "artifact_timeframe": timeframe,
            "execution_timeframe_role": "matches_signal_evaluation",
            "explicit": False,
            "unavailable_reason": "Paper runtime timeframe truth is inferred from current lane metadata rather than published as an explicit contract.",
        }
    return {
        "structural_signal_timeframe": None,
        "execution_timeframe": None,
        "artifact_timeframe": None,
        "execution_timeframe_role": None,
        "explicit": False,
        "unavailable_reason": "Current paper runtime payload does not publish explicit timeframe truth for this lane yet.",
    }


def _paper_session_window(row: dict[str, Any], tracked_summary: dict[str, Any]) -> str | None:
    current_session = tracked_summary.get("current_session_segment") or row.get("current_session")
    if current_session:
        return str(current_session)
    session_breakdown = row.get("entries_by_session_bucket") or {}
    if isinstance(session_breakdown, dict):
        active = [key for key, value in session_breakdown.items() if int(value or 0) > 0]
        if active:
            return ", ".join(active)
    return None


def _paper_session_breakdown(row: dict[str, Any], *, trade_rows: Sequence[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if row.get("entries_by_session_bucket"):
        rows: list[dict[str, Any]] = []
        for session_name, count in dict(row.get("entries_by_session_bucket") or {}).items():
            rows.append({"session": session_name, "entry_count": count})
        return rows
    if trade_rows:
        return _group_trade_rows(trade_rows, key_name="entry_session_phase")
    rows: list[dict[str, Any]] = []
    return rows


def _replay_session_window(summary: dict[str, Any]) -> str | None:
    phases = [str(row.get("session_phase") or "") for row in list(summary.get("session_level_behavior") or []) if row.get("session_phase")]
    return ", ".join(phases) if phases else None


def _replay_closed_trade_rows(
    *,
    study: dict[str, Any],
    summary: dict[str, Any],
    meta: dict[str, Any],
    trade_events: Sequence[dict[str, Any]],
    bars: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    summary_trade_rows = list(summary.get("closed_trade_breakdown") or [])
    if summary_trade_rows:
        return _normalize_replay_closed_trade_summary_rows(
            summary_trade_rows,
            bars=bars,
            point_value=_decimal_or_none(study.get("point_value")),
        )
    authoritative_trade_rows = list(meta.get("authoritative_trade_lifecycle_records") or [])
    if authoritative_trade_rows:
        return _normalize_authoritative_trade_rows(
            authoritative_trade_rows,
            bars=bars,
            point_value=_decimal_or_none(study.get("point_value")),
        )
    return _reconstruct_replay_trade_rows_from_events(
        trade_events=trade_events,
        bars=bars,
        point_value=_decimal_or_none(study.get("point_value")),
        expected_trade_count=summary.get("total_trades"),
    )


def _normalize_authoritative_trade_rows(
    authoritative_trade_rows: Sequence[dict[str, Any]],
    *,
    bars: Sequence[dict[str, Any]],
    point_value: Decimal | None,
) -> list[dict[str, Any]]:
    bars_by_id = {str(row.get("bar_id") or ""): dict(row) for row in bars if row.get("bar_id")}
    normalized: list[dict[str, Any]] = []
    for row in authoritative_trade_rows:
        trade = dict(row)
        realized_pnl = _authoritative_trade_pnl(trade, point_value=point_value)
        normalized.append(
            {
                "trade_id": trade.get("trade_id") or trade.get("decision_id") or trade.get("entry_ts"),
                "family": trade.get("family") or trade.get("family_name"),
                "side": trade.get("side"),
                "entry_timestamp": trade.get("entry_ts"),
                "exit_timestamp": trade.get("exit_ts"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "realized_pnl": _decimal_to_string(realized_pnl),
                "exit_reason": trade.get("exit_reason"),
                "entry_session_phase": _session_phase_for_replay_timestamp(
                    bars_by_id=bars_by_id,
                    timestamp=trade.get("entry_ts"),
                ),
                "exit_session_phase": _session_phase_for_replay_timestamp(
                    bars_by_id=bars_by_id,
                    timestamp=trade.get("exit_ts"),
                ),
                "truth_source": "authoritative_trade_lifecycle_records",
            }
        )
    normalized.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
    return normalized


def _authoritative_trade_pnl(trade: dict[str, Any], *, point_value: Decimal | None) -> Decimal | None:
    if trade.get("pnl_cash") not in (None, ""):
        return _decimal_or_none(trade.get("pnl_cash"))
    trade_points = _decimal_or_none(trade.get("pnl_points"))
    if trade_points is None:
        return None
    return trade_points * point_value if point_value is not None else trade_points


def _reconstruct_replay_trade_rows_from_events(
    *,
    trade_events: Sequence[dict[str, Any]],
    bars: Sequence[dict[str, Any]],
    point_value: Decimal | None,
    expected_trade_count: Any,
) -> list[dict[str, Any]]:
    bars_by_id = {str(row.get("bar_id") or ""): dict(row) for row in bars if row.get("bar_id")}
    sorted_events = sorted(
        [dict(event) for event in trade_events],
        key=lambda event: (
            str(event.get("event_timestamp") or event.get("decision_context_timestamp") or ""),
            str(event.get("event_id") or ""),
        ),
    )
    closed_trades: list[dict[str, Any]] = []
    open_trade: dict[str, Any] | None = None
    for event in sorted_events:
        event_kind = _replay_trade_event_kind(event)
        if event_kind is None:
            continue
        if event_kind == "entry":
            open_trade = {
                "trade_id": event.get("event_id"),
                "family": event.get("family"),
                "side": event.get("side"),
                "entry_timestamp": event.get("event_timestamp") or event.get("decision_context_timestamp"),
                "entry_price": event.get("event_price"),
                "entry_session_phase": _session_phase_for_replay_event(bars_by_id=bars_by_id, event=event),
                "truth_source": "trade_events",
            }
            continue
        if open_trade is None:
            continue
        entry_price = _decimal_or_none(open_trade.get("entry_price"))
        exit_price = _decimal_or_none(event.get("event_price"))
        realized_pnl = None
        if entry_price is not None and exit_price is not None and point_value is not None:
            side = str(open_trade.get("side") or "").upper()
            if side == "SHORT":
                realized_pnl = (entry_price - exit_price) * point_value
            else:
                realized_pnl = (exit_price - entry_price) * point_value
        closed_trades.append(
            {
                "trade_id": open_trade.get("trade_id"),
                "family": open_trade.get("family") or event.get("family"),
                "side": open_trade.get("side") or event.get("side"),
                "entry_timestamp": open_trade.get("entry_timestamp"),
                "exit_timestamp": event.get("event_timestamp") or event.get("decision_context_timestamp"),
                "entry_price": open_trade.get("entry_price"),
                "exit_price": event.get("event_price"),
                "realized_pnl": _decimal_to_string(realized_pnl),
                "exit_reason": event.get("reason"),
                "entry_session_phase": open_trade.get("entry_session_phase"),
                "exit_session_phase": _session_phase_for_replay_event(bars_by_id=bars_by_id, event=event),
                "truth_source": open_trade.get("truth_source"),
            }
        )
        open_trade = None
    expected_count = int(expected_trade_count or 0) if expected_trade_count not in (None, "") else None
    if expected_count is not None and expected_count > 0 and len(closed_trades) != expected_count:
        return []
    closed_trades.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
    return closed_trades


def _replay_trade_event_kind(event: dict[str, Any]) -> str | None:
    event_type = str(event.get("event_type") or "").upper()
    execution_event_type = str(event.get("execution_event_type") or "").upper()
    if event_type == "ENTRY_FILL" or execution_event_type == "ENTRY_EXECUTED":
        return "entry"
    if event_type == "EXIT_FILL" or execution_event_type == "EXIT_TRIGGERED":
        return "exit"
    return None


def _session_phase_for_replay_event(*, bars_by_id: dict[str, dict[str, Any]], event: dict[str, Any]) -> str | None:
    linked_bar_id = str(event.get("linked_bar_id") or "")
    if linked_bar_id and linked_bar_id in bars_by_id:
        return bars_by_id[linked_bar_id].get("session_phase")
    return _session_phase_for_replay_timestamp(
        bars_by_id=bars_by_id,
        timestamp=event.get("event_timestamp") or event.get("decision_context_timestamp"),
    )


def _session_phase_for_replay_timestamp(*, bars_by_id: dict[str, dict[str, Any]], timestamp: Any) -> str | None:
    timestamp_text = str(timestamp or "")
    for row in bars_by_id.values():
        start_timestamp = str(row.get("start_timestamp") or "")
        end_timestamp = str(row.get("end_timestamp") or row.get("timestamp") or "")
        if start_timestamp and end_timestamp and start_timestamp < timestamp_text <= end_timestamp:
            return row.get("session_phase")
    return None


def _replay_profit_factor(summary: dict[str, Any], trade_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if summary.get("profit_factor") not in (None, ""):
        return {"value": summary.get("profit_factor"), "reason": None}
    if not trade_rows:
        return {
            "value": None,
            "reason": "Replay artifact does not expose a complete closed-trade path that can be priced for profit factor.",
        }
    pnl_values = [_decimal_or_none(row.get("realized_pnl")) for row in trade_rows]
    if any(value is None for value in pnl_values):
        return {
            "value": None,
            "reason": "Replay closed trades are missing priced realized P/L, so profit factor remains unavailable.",
        }
    winners = [value for value in pnl_values if value and value > 0]
    losers = [(-value) for value in pnl_values if value and value < 0]
    return _profit_factor_from_pnl_values(
        winners=winners,
        losers=losers,
        has_rows=bool(trade_rows),
        unavailable_reason="Replay artifact has no losing closed trades yet, so profit factor is not informative for this run.",
    )


def _replay_trade_family_breakdown(
    *,
    summary: dict[str, Any],
    trade_rows: Sequence[dict[str, Any]],
    trade_events: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    if summary.get("trade_family_breakdown"):
        return _normalize_replay_group_rows(
            rows=list(summary.get("trade_family_breakdown") or []),
            label_kind="family",
        )
    grouped = _group_trade_rows(trade_rows, key_name="family")
    if grouped:
        return grouped
    counter: Counter[str] = Counter()
    for event in trade_events:
        if _replay_trade_event_kind(dict(event)) != "entry":
            continue
        family = str(event.get("family") or "").strip()
        if family:
            counter[family] += 1
    return [{"family": family, "count": count} for family, count in counter.most_common()]


def _replay_session_breakdown(*, summary: dict[str, Any], trade_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    if summary.get("session_trade_breakdown"):
        return _normalize_replay_group_rows(
            rows=list(summary.get("session_trade_breakdown") or []),
            label_kind="session",
        )
    grouped = _group_trade_rows(trade_rows, key_name="entry_session_phase")
    if grouped:
        return grouped
    return [dict(row) for row in list(summary.get("session_level_behavior") or [])]


def _group_trade_rows(trade_rows: Sequence[dict[str, Any]], *, key_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for trade in trade_rows:
        key = str(trade.get(key_name) or "").strip()
        if not key:
            continue
        payload = grouped.setdefault(
            key,
            {
                "group": key,
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "realized_pnl_available": True,
                "latest_trade_timestamp": None,
            },
        )
        pnl = _decimal_or_none(trade.get("realized_pnl"))
        payload["trade_count"] += 1
        if pnl is None:
            payload["realized_pnl_available"] = False
        else:
            payload["realized_pnl"] += pnl
            if pnl > 0:
                payload["wins"] += 1
            elif pnl < 0:
                payload["losses"] += 1
        latest_trade_timestamp = str(trade.get("exit_timestamp") or trade.get("entry_timestamp") or "")
        if latest_trade_timestamp and (
            payload["latest_trade_timestamp"] is None
            or latest_trade_timestamp > payload["latest_trade_timestamp"]
        ):
            payload["latest_trade_timestamp"] = latest_trade_timestamp
    rows: list[dict[str, Any]] = []
    for key, payload in grouped.items():
        label_key = "family" if key_name == "family" else "session"
        rows.append(
            {
                label_key: key,
                "trade_count": payload["trade_count"],
                "wins": payload["wins"],
                "losses": payload["losses"],
                "realized_pnl": _decimal_to_string(payload["realized_pnl"]) if payload["realized_pnl_available"] else None,
                "latest_trade_timestamp": payload["latest_trade_timestamp"],
            }
        )
    rows.sort(
        key=lambda row: (
            _sort_decimal_for_rows(row.get("realized_pnl")),
            str(row.get("family") or row.get("session") or ""),
        ),
        reverse=True,
    )
    return rows


def _replay_latest_trade_summary(
    *,
    summary: dict[str, Any],
    trade_rows: Sequence[dict[str, Any]],
    trade_events: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    if summary.get("latest_trade_summary"):
        return dict(summary.get("latest_trade_summary") or {})
    if trade_rows:
        latest_trade = sorted(
            [dict(row) for row in trade_rows],
            key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""),
            reverse=True,
        )[0]
        return {
            "trade_id": latest_trade.get("trade_id"),
            "entry_timestamp": latest_trade.get("entry_timestamp"),
            "exit_timestamp": latest_trade.get("exit_timestamp"),
            "side": latest_trade.get("side"),
            "family": latest_trade.get("family"),
            "realized_pnl": latest_trade.get("realized_pnl"),
            "exit_reason": latest_trade.get("exit_reason"),
            "entry_price": latest_trade.get("entry_price"),
            "exit_price": latest_trade.get("exit_price"),
            "truth_source": latest_trade.get("truth_source"),
        }
    relevant = [
        dict(event)
        for event in trade_events
        if _replay_trade_event_kind(dict(event)) in {"entry", "exit"}
    ]
    if not relevant:
        return None
    relevant.sort(key=lambda event: str(event.get("event_timestamp") or event.get("decision_context_timestamp") or ""), reverse=True)
    latest = relevant[0]
    return {
        "event_type": latest.get("event_type"),
        "event_timestamp": latest.get("event_timestamp"),
        "side": latest.get("side"),
        "family": latest.get("family"),
        "reason": latest.get("reason"),
        "event_price": latest.get("event_price"),
        "source_resolution": latest.get("source_resolution"),
    }


def _paper_strategy_attribution_rows(
    *,
    attribution_rows: Sequence[dict[str, Any]],
    strategy_keys: Sequence[Any],
) -> list[dict[str, Any]]:
    normalized_strategy_keys = {str(value or "").strip() for value in strategy_keys if str(value or "").strip()}
    rows: list[dict[str, Any]] = []
    for row in attribution_rows:
        standalone_ids = {str(value or "").strip() for value in list(row.get("standalone_strategy_ids") or []) if str(value or "").strip()}
        if normalized_strategy_keys & standalone_ids:
            rows.append(dict(row))
    rows.sort(key=lambda row: (_sort_decimal_for_rows(row.get("realized_pnl")), str(row.get("family_label") or "")), reverse=True)
    return rows


def _paper_trade_family_breakdown(
    *,
    trade_rows: Sequence[dict[str, Any]],
    attribution_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    if attribution_rows:
        return [
            {
                "family": row.get("family_label"),
                "trade_count": row.get("trade_count"),
                "wins": row.get("wins"),
                "losses": row.get("losses"),
                "realized_pnl": row.get("realized_pnl"),
                "latest_trade_timestamp": row.get("latest_trade_timestamp"),
                "source_families": list(row.get("source_families") or []),
            }
            for row in attribution_rows
            if row.get("family_label")
        ]
    grouped = _group_trade_rows(
        [
            {
                "family": row.get("signal_family_label") or row.get("signal_family") or row.get("family"),
                "realized_pnl": row.get("realized_pnl"),
                "entry_timestamp": row.get("entry_timestamp"),
                "exit_timestamp": row.get("exit_timestamp"),
            }
            for row in trade_rows
        ],
        key_name="family",
    )
    return grouped


def _normalize_replay_closed_trade_summary_rows(
    rows: Sequence[dict[str, Any]],
    *,
    bars: Sequence[dict[str, Any]],
    point_value: Decimal | None,
) -> list[dict[str, Any]]:
    bars_by_id = {str(row.get("bar_id") or ""): dict(row) for row in bars if row.get("bar_id")}
    normalized: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        entry_timestamp = row.get("entry_timestamp") or row.get("entry_ts")
        exit_timestamp = row.get("exit_timestamp") or row.get("exit_ts")
        pnl_value = (
            _decimal_or_none(row.get("realized_pnl"))
            or _decimal_or_none(row.get("pnl_cash"))
            or _authoritative_trade_pnl(row, point_value=point_value)
        )
        normalized.append(
            {
                "trade_id": row.get("trade_id") or row.get("decision_id") or entry_timestamp,
                "family": row.get("family") or row.get("family_name") or row.get("family_label") or row.get("signal_family_label"),
                "side": row.get("side") or row.get("direction"),
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": exit_timestamp,
                "entry_price": row.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "realized_pnl": _decimal_to_string(pnl_value),
                "exit_reason": row.get("exit_reason") or row.get("primary_exit_reason"),
                "entry_session_phase": row.get("entry_session_phase")
                or _session_phase_for_replay_timestamp(bars_by_id=bars_by_id, timestamp=entry_timestamp),
                "exit_session_phase": row.get("exit_session_phase")
                or _session_phase_for_replay_timestamp(bars_by_id=bars_by_id, timestamp=exit_timestamp),
                "truth_source": "closed_trade_breakdown",
            }
        )
    normalized.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
    return normalized


def _normalize_replay_group_rows(*, rows: Sequence[dict[str, Any]], label_kind: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    label_candidates = ("family", "family_label", "group", "name") if label_kind == "family" else ("session", "session_phase", "group", "name")
    for raw_row in rows:
        row = dict(raw_row)
        label = next((row.get(key) for key in label_candidates if row.get(key) not in (None, "")), None)
        if label in (None, ""):
            continue
        normalized_row = {
            label_kind: label,
            "trade_count": row.get("trade_count") if row.get("trade_count") is not None else row.get("count") if row.get("count") is not None else row.get("entry_count"),
            "wins": row.get("wins"),
            "losses": row.get("losses"),
            "realized_pnl": row.get("realized_pnl"),
            "latest_trade_timestamp": row.get("latest_trade_timestamp") or row.get("last_trade_timestamp"),
        }
        normalized.append({key: value for key, value in normalized_row.items() if value is not None})
    return normalized


def _normalize_paper_trade_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        entry_timestamp = row.get("entry_timestamp") or row.get("entry_ts")
        exit_timestamp = row.get("exit_timestamp") or row.get("exit_ts")
        normalized.append(
            {
                "trade_id": row.get("trade_id") or row.get("decision_id") or entry_timestamp,
                "family": row.get("signal_family_label") or row.get("signal_family") or row.get("family"),
                "side": row.get("side") or row.get("direction"),
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": exit_timestamp,
                "entry_price": row.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "realized_pnl": row.get("realized_pnl") if row.get("realized_pnl") is not None else row.get("pnl_cash"),
                "exit_reason": row.get("exit_reason") or row.get("primary_exit_reason"),
                "entry_session_phase": row.get("entry_session_phase") or _paper_session_phase_from_timestamp(entry_timestamp),
                "exit_session_phase": row.get("exit_session_phase") or _paper_session_phase_from_timestamp(exit_timestamp),
                "truth_source": row.get("truth_source") or row.get("record_source") or ("trade_log" if row.get("signal_family_label") is not None else "tracked_recent_trades"),
            }
        )
    normalized.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
    return normalized


def _complete_trade_rows(
    *,
    primary_rows: Sequence[dict[str, Any]],
    fallback_rows: Sequence[dict[str, Any]],
    expected_trade_count: Any,
) -> dict[str, Any]:
    expected_count = int(expected_trade_count or 0) if expected_trade_count not in (None, "") else None
    primary_closed = [dict(row) for row in primary_rows if row.get("exit_timestamp")]
    fallback_closed = [dict(row) for row in fallback_rows if row.get("exit_timestamp")]
    if expected_count is not None and expected_count <= 0:
        return {"rows": [], "source": None, "reason": None}
    if expected_count is not None and primary_closed and len(primary_closed) == expected_count:
        return {"rows": primary_closed, "source": "paper_trade_log", "reason": None}
    if expected_count is not None and fallback_closed and len(fallback_closed) == expected_count:
        return {"rows": fallback_closed, "source": "tracked_recent_trades", "reason": None}
    if primary_closed:
        return {
            "rows": [],
            "source": None,
            "reason": "Paper trade rows are present, but they do not cover the full persisted closed-trade count for this strategy yet.",
        }
    if fallback_closed:
        return {
            "rows": [],
            "source": None,
            "reason": "Tracked paper detail only publishes a recent-trade preview, and it does not cover the full persisted trade count for this strategy.",
        }
    return {"rows": [], "source": None, "reason": "No complete closed-trade history is available for this strategy yet."}


def _trade_row_metrics(trade_payload: dict[str, Any]) -> dict[str, Any]:
    trade_rows = list(trade_payload.get("rows") or [])
    if not trade_rows:
        return {}
    pnl_values = [_decimal_or_none(row.get("realized_pnl")) for row in trade_rows]
    long_trades = sum(1 for row in trade_rows if str(row.get("side") or "").upper() == "LONG")
    short_trades = sum(1 for row in trade_rows if str(row.get("side") or "").upper() == "SHORT")
    winners = None
    losers = None
    win_rate = None
    average_trade = None
    profit_factor = None
    if all(value is not None for value in pnl_values):
        winners = sum(1 for value in pnl_values if value and value > 0)
        losers = sum(1 for value in pnl_values if value and value < 0)
        trade_count = len(trade_rows)
        total_realized = sum((value or Decimal("0")) for value in pnl_values)
        average_trade = _decimal_to_string(total_realized / Decimal(str(trade_count))) if trade_count > 0 else None
        win_rate = _decimal_to_string((Decimal(winners) / Decimal(str(trade_count))) * Decimal("100")) if trade_count > 0 else None
        profit_factor = _profit_factor_from_pnl_values(
            winners=[value for value in pnl_values if value and value > 0],
            losers=[(-value) for value in pnl_values if value and value < 0],
            has_rows=bool(trade_rows),
            unavailable_reason="Closed-trade history has no losing trades yet, so profit factor is not informative for this run.",
        ).get("value")
    return {
        "long_trades": long_trades,
        "short_trades": short_trades,
        "winners": winners,
        "losers": losers,
        "win_rate": win_rate,
        "average_trade": average_trade,
        "profit_factor": profit_factor,
    }


def _paper_latest_trade_summary(
    *,
    summary_row: dict[str, Any] | None,
    tracked_trade_rows: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    summary = _normalize_trade_summary_row(summary_row, truth_source="tracked_strategy_summary")
    latest_trade = _normalize_trade_summary_row(
        (list(tracked_trade_rows or []) or list(trade_rows or []) or [None])[0],
        truth_source="trade_rows",
    )
    if summary and latest_trade:
        merged = dict(latest_trade)
        merged.update({key: value for key, value in summary.items() if value not in (None, "")})
        return merged
    return summary or latest_trade


def _normalize_trade_summary_row(row: dict[str, Any] | None, *, truth_source: str) -> dict[str, Any] | None:
    if not row:
        return None
    payload = dict(row)
    normalized = {
        "trade_id": payload.get("trade_id") or payload.get("decision_id"),
        "entry_timestamp": payload.get("entry_timestamp") or payload.get("entry_ts"),
        "exit_timestamp": payload.get("exit_timestamp") or payload.get("exit_ts"),
        "side": payload.get("side") or payload.get("direction"),
        "family": payload.get("signal_family_label") or payload.get("signal_family") or payload.get("family"),
        "realized_pnl": payload.get("realized_pnl") if payload.get("realized_pnl") is not None else payload.get("pnl_cash"),
        "exit_reason": payload.get("exit_reason") or payload.get("primary_exit_reason"),
        "entry_price": payload.get("entry_price"),
        "exit_price": payload.get("exit_price"),
        "truth_source": payload.get("truth_source") or payload.get("record_source") or truth_source,
    }
    if not any(value not in (None, "") for value in normalized.values()):
        return None
    return normalized


def _paper_session_phase_from_timestamp(timestamp: Any) -> str | None:
    timestamp_text = str(timestamp or "").strip()
    if not timestamp_text:
        return None
    try:
        return label_session_phase(datetime.fromisoformat(timestamp_text))
    except ValueError:
        return None


def _profit_factor_from_pnl_values(
    *,
    winners: Sequence[Decimal],
    losers: Sequence[Decimal],
    has_rows: bool,
    unavailable_reason: str,
) -> dict[str, Any]:
    gross_profit = sum(winners, Decimal("0"))
    gross_loss = sum(losers, Decimal("0"))
    if gross_loss > 0:
        return {"value": _decimal_to_string(gross_profit / gross_loss), "reason": None}
    if gross_profit > 0 and has_rows:
        return {"value": "999", "reason": None}
    return {"value": None, "reason": unavailable_reason}


def _count_trade_events(trade_events: Sequence[dict[str, Any]], event_kind: str) -> int:
    if event_kind == "intent":
        return sum(1 for event in trade_events if "INTENT" in str(event.get("event_type") or ""))
    if event_kind == "fill":
        return sum(1 for event in trade_events if "FILL" in str(event.get("event_type") or ""))
    return sum(1 for event in trade_events if "ENTRY" in str(event.get("event_type") or "") or "ATP_" in str(event.get("event_type") or ""))


def _canonical_strategy_key(value: Any, *, instrument: Any, strategy_family: Any) -> str:
    raw = str(value or "").strip()
    if raw:
        return raw
    identity = build_standalone_strategy_identity(
        instrument=instrument,
        source_family=strategy_family,
        lane_id=None,
        strategy_name=strategy_family,
    )
    return identity["standalone_strategy_id"]


def _merge_preview_rows(
    primary_rows: Sequence[dict[str, Any]],
    fallback_rows: Sequence[dict[str, Any]],
    *,
    key_fields: Sequence[str],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for row in [*list(primary_rows), *list(fallback_rows)]:
        row_dict = dict(row)
        row_key = tuple(row_dict.get(field) for field in key_fields)
        if any(value not in (None, "") for value in row_key):
            if row_key in seen_keys:
                continue
            seen_keys.add(row_key)
        merged.append(row_dict)
    merged.sort(key=lambda row: _latest_preview_sort_key(row), reverse=True)
    return merged


def _latest_preview_sort_key(row: dict[str, Any]) -> str:
    for key in (
        "updated_at",
        "occurred_at",
        "fill_timestamp",
        "created_at",
        "signal_timestamp",
        "exit_timestamp",
        "entry_timestamp",
        "end_ts",
        "timestamp",
    ):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _sort_decimal_for_rows(value: Any) -> Decimal:
    return _decimal_or_none(value) or Decimal("-999999999")


def _metric_value(value: Any, *, unavailable_reason: str | None = None) -> dict[str, Any]:
    normalized_value = _json_metric_value(value)
    available = normalized_value not in (None, "")
    return {
        "available": available,
        "value": normalized_value if available else None,
        "reason": None if available else unavailable_reason,
    }


def _evidence_ref(
    *,
    available: bool,
    count: int,
    preview_rows: Sequence[dict[str, Any]],
    ref: dict[str, Any] | None = None,
    unavailable_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "available": available,
        "count": count,
        "preview_rows": list(preview_rows),
        "ref": dict(ref or {}),
        "reason": None if available else unavailable_reason,
    }


def _average_metric(total: Any, count: Any) -> str | None:
    total_decimal = _decimal_or_none(total)
    try:
        count_int = int(count)
    except (TypeError, ValueError):
        return None
    if total_decimal is None or count_int <= 0:
        return None
    return _decimal_to_string(total_decimal / Decimal(count_int))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not result.is_finite():
        return None
    return result


def _json_metric_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_string(value) if value.is_finite() else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize() if value == value.to_integral() else value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _latest_rows_from_table(db_path: Path | None, table_name: str, order_column: str, *, limit: int) -> list[dict[str, Any]]:
    if db_path is None or not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"select * from {table_name} order by {order_column} desc limit ?",
            (limit,),
        ).fetchall()
    except sqlite3.Error:
        connection.close()
        return []
    connection.close()
    return [dict(row) for row in rows]


def _latest_payload_rows_from_table(db_path: Path | None, table_name: str, order_column: str, *, limit: int) -> list[dict[str, Any]]:
    rows = _latest_rows_from_table(db_path, table_name, order_column, limit=limit)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(str(row.get("payload_json") or "")) if row.get("payload_json") else {}
        normalized.append({**row, "payload": payload})
    return normalized


def _filter_strategy_rows(
    rows: Sequence[dict[str, Any]],
    *,
    lane_id: str | None,
    standalone_strategy_id: Any,
) -> list[dict[str, Any]]:
    standalone_label = str(standalone_strategy_id or "").strip()
    lane_label = str(lane_id or "").strip()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_lane = str(row.get("lane_id") or "").strip()
        row_strategy = str(row.get("standalone_strategy_id") or "").strip()
        if lane_label and row_lane == lane_label:
            filtered.append(dict(row))
            continue
        if standalone_label and row_strategy == standalone_label:
            filtered.append(dict(row))
            continue
        if not lane_label and not standalone_label:
            filtered.append(dict(row))
    return filtered or [dict(row) for row in rows[:_PREVIEW_ROW_LIMIT]]


def _normalize_state_snapshot_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(str(row.get("payload_json") or "")) if row.get("payload_json") else {}
        normalized.append(
            {
                **dict(row),
                "payload": payload,
                "position_quantity": payload.get("position_quantity"),
                "position_average_price": payload.get("position_average_price"),
                "latest_order_intent": payload.get("latest_order_intent"),
                "latest_fill": payload.get("latest_fill"),
            }
        )
    return normalized


def _load_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _nested_get(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _latest_timestamp_value(values: Iterable[Any]) -> str | None:
    normalized = [str(value) for value in values if value not in (None, "")]
    return max(normalized) if normalized else None


def _latest_or_earliest(rows: Sequence[dict[str, Any]], field: str, *, earliest: bool = False) -> str | None:
    values = [str(row.get(field) or "") for row in rows if row.get(field)]
    if not values:
        return None
    return min(values) if earliest else max(values)


def _resolve_sqlite_database_path(value: Any) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.startswith("sqlite:///"):
        return Path(raw.replace("sqlite:///", "", 1))
    return Path(raw)
