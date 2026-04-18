"""Thin warehouse-native compact evaluator built on shared substrate tables."""

from __future__ import annotations

import json
from bisect import bisect_left
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

from ..trend_participation.storage import materialize_parquet_dataset
from ._warehouse_common import (
    now_utc,
    read_parquet_rows,
    read_stage_cache_manifest,
    stable_cache_key,
    write_stage_cache_manifest,
)
from .family_events import PROBATIONARY_EXECUTION_MODEL
from .layout import build_layout
from .raw_materializer import build_dataset_partition_path, coverage_range


@dataclass(frozen=True)
class LaneDefinition:
    lane_id: str
    strategy_key: str
    family: str
    symbol: str
    source_event_family: str
    side: str
    execution_model: str
    hold_minutes: int


LANE_FAMILY_RULES: tuple[tuple[str, dict[str, Any]], ...] = (
    (
        "us_late_pause_resume_long_turn",
        {
            "family": "usLatePauseResumeLongTurn",
            "source_event_family": "usLatePauseResumeLongTurn",
            "side": "LONG",
            "hold_minutes": 15,
        },
    ),
    (
        "asia_early_normal_breakout_retest_hold_turn",
        {
            "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "source_event_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "side": "LONG",
            "hold_minutes": 20,
        },
    ),
    (
        "asia_early_pause_resume_short_turn",
        {
            "family": "asiaEarlyPauseResumeShortTurn",
            "source_event_family": "asiaEarlyPauseResumeShortTurn",
            "side": "SHORT",
            "hold_minutes": 15,
        },
    ),
)

LANE_FAMILY_FEATURE_FIELDS: dict[str, str] = {
    "usLatePauseResumeLongTurn": "us_late_pause_resume_long_turn_candidate",
    "asiaEarlyNormalBreakoutRetestHoldTurn": "asia_early_normal_breakout_retest_hold_long_turn_candidate",
    "asiaEarlyPauseResumeShortTurn": "asia_early_pause_resume_short_turn_candidate",
}

ENTRY_SEARCH_WINDOW_MINUTES = 10
WAREHOUSE_CONTRACT_POINT_VALUES: dict[str, float] = {
    "MGC": 10.0,
    "GC": 100.0,
    "PL": 50.0,
    "HG": 25000.0,
    "QC": 25000.0,
    "CL": 1000.0,
    "ES": 50.0,
    "6E": 125000.0,
    "6J": 12500000.0,
    "NG": 10000.0,
    "6B": 62500.0,
    "MBT": 0.1,
}
LANE_CANDIDATES_CACHE_VERSION = "warehouse_lane_candidates_v2"
LANE_ENTRIES_CACHE_VERSION = "warehouse_lane_entries_v2"
LANE_CLOSED_TRADES_CACHE_VERSION = "warehouse_lane_closed_trades_v2"
LANE_COMPACT_RESULTS_CACHE_VERSION = "warehouse_lane_compact_results_v2"


def resolve_lane_definition(*, lane_id: str, symbol: str) -> LaneDefinition:
    normalized_lane_id = lane_id.strip()
    normalized_symbol = symbol.upper()
    lane_lower = normalized_lane_id.lower()
    if not lane_lower.endswith(f"__{normalized_symbol.lower()}"):
        raise RuntimeError(f"Lane {lane_id} does not match symbol {symbol}.")
    for token, payload in LANE_FAMILY_RULES:
        if token in lane_lower:
            return LaneDefinition(
                lane_id=normalized_lane_id,
                strategy_key=normalized_lane_id,
                family=str(payload["family"]),
                symbol=normalized_symbol,
                source_event_family=str(payload["source_event_family"]),
                side=str(payload["side"]),
                execution_model=PROBATIONARY_EXECUTION_MODEL,
                hold_minutes=int(payload["hold_minutes"]),
            )
    raise RuntimeError(f"Unsupported warehouse lane family for {lane_id}.")


def materialize_lane_candidates_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    lane_ids: list[str],
    family_event_tables_partition_path: Path,
    shared_features_5m_partition_path: Path,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    lane_definitions = [resolve_lane_definition(lane_id=lane_id, symbol=symbol) for lane_id in lane_ids]
    output_path = build_dataset_partition_path(
        dataset_root=layout["lane_candidates"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="candidates.parquet",
    )
    serialized_definitions = [_serialize_lane_definition(item) for item in lane_definitions]
    cache_key = stable_cache_key(
        {
            "cache_version": LANE_CANDIDATES_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "lane_ids": lane_ids,
            "family_event_tables_partition_path": str(family_event_tables_partition_path.resolve()),
            "shared_features_5m_partition_path": str(shared_features_5m_partition_path.resolve()),
            "lane_definitions": serialized_definitions,
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        candidate_rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        available_families = sorted({str(row["family"]) for row in read_parquet_rows(family_event_tables_partition_path)})
        return {
            "dataset_name": "lane_candidates",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "partition_path": output_path,
            "row_count": len(candidate_rows),
            "coverage": coverage_range(candidate_rows, timestamp_key="candidate_ts"),
            "provenance_tag": candidate_rows[0]["provenance_tag"] if candidate_rows else None,
            "rows": candidate_rows,
            "available_families": available_families,
            "lane_definitions": lane_definitions,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    family_read_started = perf_counter()
    family_rows = read_parquet_rows(family_event_tables_partition_path)
    family_read_seconds = perf_counter() - family_read_started
    feature_read_started = perf_counter()
    feature_rows = read_parquet_rows(shared_features_5m_partition_path)
    feature_read_seconds = perf_counter() - feature_read_started
    lookup_started = perf_counter()
    family_event_lookup = {
        (str(row["family"]), row["decision_ts"]): row
        for row in family_rows
    }
    available_families = {str(row["family"]) for row in family_rows}
    lookup_seconds = perf_counter() - lookup_started
    candidate_rows: list[dict[str, Any]] = []
    loop_started = perf_counter()
    for lane in lane_definitions:
        lane_feature_field = LANE_FAMILY_FEATURE_FIELDS[lane.family]
        for feature_row in feature_rows:
            if not bool(feature_row.get(lane_feature_field)):
                continue
            event_row = family_event_lookup.get((lane.source_event_family, feature_row["decision_ts"]))
            feature_refs = json.loads(str(event_row.get("feature_refs") or "{}")) if event_row else {}
            event_id = (
                str(event_row["event_id"])
                if event_row is not None
                else f"{symbol}:{shard_id}:{lane.source_event_family}:feature:{feature_row['decision_ts'].isoformat()}"
            )
            candidate_rows.append(
                {
                    "candidate_id": f"{lane.lane_id}:{event_id}",
                    "event_id": event_id,
                    "source_event_family": lane.source_event_family,
                    "lane_id": lane.lane_id,
                    "strategy_key": lane.strategy_key,
                    "family": lane.family,
                    "symbol": symbol,
                    "shard_id": shard_id,
                    "candidate_ts": feature_row["decision_ts"],
                    "decision_ts": feature_row["decision_ts"],
                    "timing_ts": event_row["timing_ts"] if event_row is not None else None,
                    "side": lane.side,
                    "execution_model": lane.execution_model,
                    "eligibility_label": "lane_feature_flag_true",
                    "blocker_label": event_row.get("blocker_label") if event_row is not None else None,
                    "feature_bar_id": feature_row["bar_id"],
                    "timing_bar_id": feature_refs.get("shared_features_1m_timing_bar_id"),
                    "materialized_ts": materialized_ts,
                    "provenance_tag": f"lane_candidates:{lane.lane_id}:{shard_id}:{lane.source_event_family}",
                }
            )
    loop_seconds = perf_counter() - loop_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, candidate_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="lane_candidates",
        cache_key=cache_key,
        extra={"lane_definitions": serialized_definitions},
    )
    return {
        "dataset_name": "lane_candidates",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "partition_path": output_path,
        "row_count": len(candidate_rows),
        "coverage": coverage_range(candidate_rows, timestamp_key="candidate_ts"),
        "provenance_tag": candidate_rows[0]["provenance_tag"] if candidate_rows else None,
        "rows": candidate_rows,
        "available_families": sorted(available_families),
        "lane_definitions": lane_definitions,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "family_event_read_seconds": round(family_read_seconds, 6),
            "feature_read_seconds": round(feature_read_seconds, 6),
            "lookup_build_seconds": round(lookup_seconds, 6),
            "candidate_loop_seconds": round(loop_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def materialize_lane_entries_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    lane_definitions: list[LaneDefinition],
    lane_candidates_partition_path: Path,
    shared_features_1m_timing_partition_path: Path,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    output_path = build_dataset_partition_path(
        dataset_root=layout["lane_entries"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="entries.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": LANE_ENTRIES_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "lane_definitions": [_serialize_lane_definition(item) for item in lane_definitions],
            "lane_candidates_partition_path": str(lane_candidates_partition_path.resolve()),
            "shared_features_1m_timing_partition_path": str(shared_features_1m_timing_partition_path.resolve()),
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        entry_rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "lane_entries",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "partition_path": output_path,
            "row_count": len(entry_rows),
            "coverage": coverage_range(entry_rows, timestamp_key="entry_ts"),
            "provenance_tag": entry_rows[0]["provenance_tag"] if entry_rows else None,
            "rows": entry_rows,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    candidate_read_started = perf_counter()
    candidate_rows = read_parquet_rows(lane_candidates_partition_path)
    candidate_read_seconds = perf_counter() - candidate_read_started
    timing_read_started = perf_counter()
    timing_rows = read_parquet_rows(shared_features_1m_timing_partition_path)
    timing_read_seconds = perf_counter() - timing_read_started
    index_started = perf_counter()
    timing_by_ts = {row["timing_ts"]: row for row in timing_rows}
    timing_ts_values = [row["timing_ts"] for row in timing_rows]
    definitions_by_lane = {lane.lane_id: lane for lane in lane_definitions}
    index_seconds = perf_counter() - index_started
    entry_rows: list[dict[str, Any]] = []
    loop_started = perf_counter()
    for candidate_row in candidate_rows:
        lane = definitions_by_lane[candidate_row["lane_id"]]
        timing_row = _select_entry_timing_row(
            side=lane.side,
            decision_ts=candidate_row["decision_ts"],
            explicit_timing_ts=candidate_row["timing_ts"],
            timing_rows=timing_rows,
            timing_ts_values=timing_ts_values,
        )
        if timing_row is None:
            continue
        timing_ts = timing_row["timing_ts"]
        quality, _quality_allowed = _entry_quality_for_side(side=lane.side, timing_row=timing_row)
        entry_rows.append(
            {
                "entry_id": f"{candidate_row['candidate_id']}:entry",
                "candidate_id": candidate_row["candidate_id"],
                "event_id": candidate_row["event_id"],
                "source_event_family": lane.source_event_family,
                "lane_id": lane.lane_id,
                "strategy_key": lane.strategy_key,
                "family": lane.family,
                "symbol": symbol,
                "shard_id": shard_id,
                "entry_ts": timing_ts,
                "side": lane.side,
                "execution_model": lane.execution_model,
                "entry_price": timing_row["close_price"],
                "bar_vwap": timing_row["bar_vwap"],
                "vwap_quality": quality,
                "quality_allowed": True,
                "hold_minutes": lane.hold_minutes,
                "materialized_ts": materialized_ts,
                "provenance_tag": f"lane_entries:{lane.lane_id}:{shard_id}:{lane.execution_model}",
            }
        )
    loop_seconds = perf_counter() - loop_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, entry_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="lane_entries",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "lane_entries",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "partition_path": output_path,
        "row_count": len(entry_rows),
        "coverage": coverage_range(entry_rows, timestamp_key="entry_ts"),
        "provenance_tag": entry_rows[0]["provenance_tag"] if entry_rows else None,
        "rows": entry_rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "candidate_read_seconds": round(candidate_read_seconds, 6),
            "timing_read_seconds": round(timing_read_seconds, 6),
            "index_build_seconds": round(index_seconds, 6),
            "entry_loop_seconds": round(loop_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def materialize_lane_closed_trades_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    lane_definitions: list[LaneDefinition],
    lane_entries_partition_path: Path,
    shared_features_1m_timing_partition_path: Path,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    output_path = build_dataset_partition_path(
        dataset_root=layout["lane_closed_trades"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="closed_trades.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": LANE_CLOSED_TRADES_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "lane_definitions": [_serialize_lane_definition(item) for item in lane_definitions],
            "lane_entries_partition_path": str(lane_entries_partition_path.resolve()),
            "shared_features_1m_timing_partition_path": str(shared_features_1m_timing_partition_path.resolve()),
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        trade_rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "lane_closed_trades",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "partition_path": output_path,
            "row_count": len(trade_rows),
            "coverage": coverage_range(trade_rows, timestamp_key="exit_ts"),
            "provenance_tag": trade_rows[0]["provenance_tag"] if trade_rows else None,
            "rows": trade_rows,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    entry_read_started = perf_counter()
    entry_rows = read_parquet_rows(lane_entries_partition_path)
    entry_read_seconds = perf_counter() - entry_read_started
    timing_read_started = perf_counter()
    timing_rows = read_parquet_rows(shared_features_1m_timing_partition_path)
    timing_read_seconds = perf_counter() - timing_read_started
    index_started = perf_counter()
    timing_ts_index = [row["timing_ts"] for row in timing_rows]
    definitions_by_lane = {lane.lane_id: lane for lane in lane_definitions}
    index_seconds = perf_counter() - index_started
    trade_rows: list[dict[str, Any]] = []
    loop_started = perf_counter()
    for lane_id in sorted(definitions_by_lane):
        lane = definitions_by_lane[lane_id]
        lane_entries = sorted(
            [row for row in entry_rows if row["lane_id"] == lane_id],
            key=lambda item: item["entry_ts"],
        )
        last_exit_ts: datetime | None = None
        for entry_row in lane_entries:
            entry_ts = entry_row["entry_ts"]
            if last_exit_ts is not None and entry_ts <= last_exit_ts:
                continue
            exit_row, exit_reason, exit_price = _select_exit_timing_row(
                side=lane.side,
                entry_ts=entry_ts,
                hold_minutes=int(entry_row["hold_minutes"]),
                timing_rows=timing_rows,
                timing_ts_index=timing_ts_index,
            )
            if exit_row is None:
                continue
            exit_ts = exit_row["timing_ts"]
            point_value = _contract_point_value(symbol)
            pnl_points = _closed_trade_pnl_points(
                side=lane.side,
                entry_price=float(entry_row["entry_price"]),
                exit_price=float(exit_price),
            )
            pnl_cash = pnl_points * point_value
            trade_rows.append(
                {
                    "trade_id": f"{entry_row['entry_id']}:closed",
                    "entry_id": entry_row["entry_id"],
                    "candidate_id": entry_row["candidate_id"],
                    "lane_id": lane.lane_id,
                    "strategy_key": lane.strategy_key,
                    "family": lane.family,
                    "symbol": symbol,
                    "shard_id": shard_id,
                    "side": lane.side,
                    "execution_model": lane.execution_model,
                    "entry_ts": entry_ts,
                    "exit_ts": exit_ts,
                    "entry_price": entry_row["entry_price"],
                    "exit_price": exit_price,
                    "pnl": pnl_cash,
                    "pnl_points": pnl_points,
                    "point_value": point_value,
                    "hold_minutes": int(entry_row["hold_minutes"]),
                    "vwap_quality": entry_row["vwap_quality"],
                    "exit_reason": exit_reason,
                    "win_flag": pnl_cash > 0.0,
                    "materialized_ts": materialized_ts,
                    "provenance_tag": f"lane_closed_trades:{lane.lane_id}:{shard_id}:{lane.execution_model}",
                }
            )
            last_exit_ts = exit_ts
    loop_seconds = perf_counter() - loop_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, trade_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="lane_closed_trades",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "lane_closed_trades",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "partition_path": output_path,
        "row_count": len(trade_rows),
        "coverage": coverage_range(trade_rows, timestamp_key="exit_ts"),
        "provenance_tag": trade_rows[0]["provenance_tag"] if trade_rows else None,
        "rows": trade_rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "entry_read_seconds": round(entry_read_seconds, 6),
            "timing_read_seconds": round(timing_read_seconds, 6),
            "index_build_seconds": round(index_seconds, 6),
            "closed_trade_loop_seconds": round(loop_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def materialize_lane_compact_results_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    lane_definitions: list[LaneDefinition],
    available_families: set[str],
    canonical_input_range: dict[str, Any],
    lane_candidates_partition_path: Path,
    lane_entries_partition_path: Path,
    lane_closed_trades_partition_path: Path,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    output_path = build_dataset_partition_path(
        dataset_root=layout["lane_compact_results"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="results.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": LANE_COMPACT_RESULTS_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "lane_definitions": [_serialize_lane_definition(item) for item in lane_definitions],
            "available_families": sorted(available_families),
            "canonical_input_range": canonical_input_range,
            "lane_candidates_partition_path": str(lane_candidates_partition_path.resolve()),
            "lane_entries_partition_path": str(lane_entries_partition_path.resolve()),
            "lane_closed_trades_partition_path": str(lane_closed_trades_partition_path.resolve()),
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        compact_rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "lane_compact_results",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "partition_path": output_path,
            "row_count": len(compact_rows),
            "coverage": coverage_range(
                [row for row in compact_rows if row["emitted_compact_start"] is not None],
                timestamp_key="emitted_compact_start",
            ),
            "provenance_tag": compact_rows[0]["execution_model"] if compact_rows else None,
            "rows": compact_rows,
            "cache": {
                "cache_hit": True,
                "cache_key": cache_key,
            },
            "timing": {
                "cache_hit": True,
                "parquet_read_seconds": round(read_seconds, 6),
                "total_seconds": round(perf_counter() - stage_started, 6),
            },
        }

    candidate_read_started = perf_counter()
    candidate_rows = read_parquet_rows(lane_candidates_partition_path)
    candidate_read_seconds = perf_counter() - candidate_read_started
    entry_read_started = perf_counter()
    entry_rows = read_parquet_rows(lane_entries_partition_path)
    entry_read_seconds = perf_counter() - entry_read_started
    trade_read_started = perf_counter()
    trade_rows = read_parquet_rows(lane_closed_trades_partition_path)
    trade_read_seconds = perf_counter() - trade_read_started
    compact_rows: list[dict[str, Any]] = []
    aggregate_started = perf_counter()
    for lane in lane_definitions:
        lane_candidates = [row for row in candidate_rows if row["lane_id"] == lane.lane_id]
        lane_entries = [row for row in entry_rows if row["lane_id"] == lane.lane_id]
        lane_trades = [row for row in trade_rows if row["lane_id"] == lane.lane_id]
        compact_rows.append(
            _compact_row_from_warehouse_rows(
                lane=lane,
                available_families=available_families,
                canonical_input_range=canonical_input_range,
                candidate_rows=lane_candidates,
                entry_rows=lane_entries,
                trade_rows=lane_trades,
            )
        )
    aggregate_seconds = perf_counter() - aggregate_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, compact_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="lane_compact_results",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "lane_compact_results",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "partition_path": output_path,
        "row_count": len(compact_rows),
        "coverage": coverage_range(
            [row for row in compact_rows if row["emitted_compact_start"] is not None],
            timestamp_key="emitted_compact_start",
        ),
        "provenance_tag": compact_rows[0]["execution_model"] if compact_rows else None,
        "rows": compact_rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "candidate_read_seconds": round(candidate_read_seconds, 6),
            "entry_read_seconds": round(entry_read_seconds, 6),
            "trade_read_seconds": round(trade_read_seconds, 6),
            "aggregate_seconds": round(aggregate_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def _compact_row_from_warehouse_rows(
    *,
    lane: LaneDefinition,
    available_families: set[str],
    canonical_input_range: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    entry_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if trade_rows:
        result_classification = "nonzero_trade"
        eligibility_status = "eligible_nonzero_trade"
    elif candidate_rows or entry_rows or lane.source_event_family in available_families:
        result_classification = "zero_trade"
        if candidate_rows or entry_rows:
            eligibility_status = "eligible_no_closed_trades"
        else:
            eligibility_status = f"eligible_no_family_events_in_window:{lane.source_event_family}"
    else:
        result_classification = "zero_trade"
        eligibility_status = f"eligible_no_family_events_in_window:{lane.source_event_family}"
    emitted_range = (
        coverage_range(entry_rows, timestamp_key="entry_ts")
        if entry_rows
        else coverage_range(candidate_rows, timestamp_key="candidate_ts")
    )
    closed_range = coverage_range(trade_rows, timestamp_key="exit_ts")
    winners = sum(1 for trade in trade_rows if float(trade["pnl"]) > 0.0)
    losers = sum(1 for trade in trade_rows if float(trade["pnl"]) < 0.0)
    total_positive = sum(float(trade["pnl"]) for trade in trade_rows if float(trade["pnl"]) > 0.0)
    total_negative = sum(float(trade["pnl"]) for trade in trade_rows if float(trade["pnl"]) < 0.0)
    trade_count = len(trade_rows)
    profit_factor = None
    if total_negative < 0:
        profit_factor = total_positive / abs(total_negative)
    elif trade_count > 0 and total_positive > 0:
        profit_factor = None
    return {
        "lane_id": lane.lane_id,
        "strategy_key": lane.strategy_key,
        "family": lane.family,
        "symbol": lane.symbol,
        "execution_model": lane.execution_model,
        "shard_id": canonical_input_range.get("shard_id") or "",
        "artifact_class": "compact_summary",
        "result_classification": result_classification,
        "trade_count": trade_count,
        "net_pnl": sum(float(trade["pnl"]) for trade in trade_rows),
        "profit_factor": profit_factor,
        "win_rate": (winners / trade_count) if trade_count else 0.0,
        "winners": winners,
        "losers": losers,
        "canonical_input_start": canonical_input_range["start"],
        "canonical_input_end": canonical_input_range["end"],
        "emitted_compact_start": emitted_range["start"],
        "emitted_compact_end": emitted_range["end"],
        "closed_trade_start": closed_range["start"],
        "closed_trade_end": closed_range["end"],
        "eligibility_status": eligibility_status,
        "zero_trade_flag": result_classification == "zero_trade",
        "reference_lane": False,
        "bucket": None,
        "status": None,
        "cohort": "WAREHOUSE_THIN_PROOF",
    }


def _entry_quality_for_side(*, side: str, timing_row: dict[str, Any]) -> tuple[str, bool]:
    if side == "LONG":
        quality = str(timing_row["long_close_quality"])
        allowed = quality == "VWAP_FAVORABLE" or (quality == "VWAP_NEUTRAL" and bool(timing_row["long_neutral_tight_ok"]))
        return quality, allowed
    quality = str(timing_row["short_close_quality"])
    allowed = quality == "VWAP_FAVORABLE" or (quality == "VWAP_NEUTRAL" and bool(timing_row["short_neutral_tight_ok"]))
    return quality, allowed


def _select_entry_timing_row(
    *,
    side: str,
    decision_ts: datetime,
    explicit_timing_ts: datetime | None,
    timing_rows: list[dict[str, Any]],
    timing_ts_values: list[datetime],
    search_window_minutes: int = ENTRY_SEARCH_WINDOW_MINUTES,
) -> dict[str, Any] | None:
    start_ts = explicit_timing_ts or _first_timing_ts_on_or_after(
        decision_ts=decision_ts,
        timing_ts_values=timing_ts_values,
    )
    if start_ts is None:
        return None
    start_index = bisect_left(timing_ts_values, start_ts)
    window_end_ts = start_ts + timedelta(minutes=search_window_minutes)
    for timing_row in timing_rows[start_index:]:
        timing_ts = timing_row["timing_ts"]
        if timing_ts > window_end_ts:
            break
        _quality, quality_allowed = _entry_quality_for_side(side=side, timing_row=timing_row)
        if quality_allowed:
            return timing_row
    return None


def _select_exit_timing_row(
    *,
    side: str,
    entry_ts: datetime,
    hold_minutes: int,
    timing_rows: list[dict[str, Any]],
    timing_ts_index: list[datetime],
) -> tuple[dict[str, Any] | None, str | None, float | None]:
    hold_target_ts = entry_ts + timedelta(minutes=hold_minutes)
    start_index = bisect_left(timing_ts_index, entry_ts)
    target_index = bisect_left(timing_ts_index, hold_target_ts)
    if target_index >= len(timing_rows):
        return None, None, None
    integrity_fail_reason = "LONG_INTEGRITY_FAIL" if side == "LONG" else "SHORT_INTEGRITY_FAIL"
    time_exit_reason = "LONG_TIME_EXIT" if side == "LONG" else "SHORT_TIME_EXIT"
    for timing_row in timing_rows[start_index + 1 : target_index + 1]:
        _quality, quality_allowed = _entry_quality_for_side(side=side, timing_row=timing_row)
        if not quality_allowed:
            boundary_row = _select_structural_exit_row_after_fail(
                failed_timing_ts=timing_row["timing_ts"],
                hold_target_ts=hold_target_ts,
                timing_rows=timing_rows,
                timing_ts_index=timing_ts_index,
            )
            exit_price = _resolve_integrity_fail_exit_price(
                side=side,
                failed_timing_ts=timing_row["timing_ts"],
                boundary_timing_ts=boundary_row["timing_ts"],
                timing_rows=timing_rows,
                timing_ts_index=timing_ts_index,
            )
            return boundary_row, integrity_fail_reason, exit_price
    return timing_rows[target_index], time_exit_reason, float(timing_rows[target_index]["close_price"])


def _contract_point_value(symbol: str) -> float:
    return float(WAREHOUSE_CONTRACT_POINT_VALUES.get(symbol.upper(), 1.0))


def _closed_trade_pnl_points(*, side: str, entry_price: float, exit_price: float) -> float:
    if side == "LONG":
        return exit_price - entry_price
    return entry_price - exit_price


def _first_timing_ts_on_or_after(*, decision_ts: datetime, timing_ts_values: list[datetime]) -> datetime | None:
    index = bisect_left(timing_ts_values, decision_ts)
    if index >= len(timing_ts_values):
        return None
    return timing_ts_values[index]


def _select_structural_exit_row_after_fail(
    *,
    failed_timing_ts: datetime,
    hold_target_ts: datetime,
    timing_rows: list[dict[str, Any]],
    timing_ts_index: list[datetime],
) -> dict[str, Any]:
    start_index = bisect_left(timing_ts_index, failed_timing_ts)
    target_index = bisect_left(timing_ts_index, hold_target_ts)
    for timing_row in timing_rows[start_index : target_index + 1]:
        ts = timing_row["timing_ts"]
        if _is_structural_boundary(ts):
            return timing_row
    return timing_rows[target_index]


def _is_structural_boundary(ts: datetime) -> bool:
    return ts.minute % 5 == 0 and ts.second == 0 and ts.microsecond == 0


def _resolve_integrity_fail_exit_price(
    *,
    side: str,
    failed_timing_ts: datetime,
    boundary_timing_ts: datetime,
    timing_rows: list[dict[str, Any]],
    timing_ts_index: list[datetime],
) -> float:
    start_index = bisect_left(timing_ts_index, failed_timing_ts)
    end_index = bisect_left(timing_ts_index, boundary_timing_ts)
    window = timing_rows[start_index : end_index + 1]
    if not window:
        raise RuntimeError("Integrity-fail exit window cannot be empty.")
    if side == "LONG":
        return min(float(row.get("low_price", row["close_price"])) for row in window)
    return max(float(row.get("high_price", row["close_price"])) for row in window)


def _serialize_lane_definition(lane: LaneDefinition) -> dict[str, Any]:
    return {
        "lane_id": lane.lane_id,
        "strategy_key": lane.strategy_key,
        "family": lane.family,
        "symbol": lane.symbol,
        "source_event_family": lane.source_event_family,
        "side": lane.side,
        "execution_model": lane.execution_model,
        "hold_minutes": lane.hold_minutes,
    }
