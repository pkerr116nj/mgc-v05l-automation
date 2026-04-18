"""Reusable family-event table materializer built from shared warehouse features."""

from __future__ import annotations

from bisect import bisect_left
import json
from datetime import datetime
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
from .layout import build_layout
from .raw_materializer import build_dataset_partition_path, coverage_range

PROBATIONARY_EXECUTION_MODEL = "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP"
FAMILY_FEATURE_MAP: dict[str, tuple[str, str]] = {
    "bullSnapLongBase": ("bull_snap_turn_candidate", "LONG"),
    "bearSnapShortBase": ("bear_snap_turn_candidate", "SHORT"),
    "asiaVwapLongBase": ("asia_vwap_long_signal", "LONG"),
    "usLatePauseResumeLongTurn": ("us_late_pause_resume_long_turn_candidate", "LONG"),
    "asiaEarlyNormalBreakoutRetestHoldTurn": ("asia_early_normal_breakout_retest_hold_long_turn_candidate", "LONG"),
    "asiaEarlyPauseResumeShortTurn": ("asia_early_pause_resume_short_turn_candidate", "SHORT"),
}
FAMILY_EVENT_TABLES_CACHE_VERSION = "warehouse_family_event_tables_v3"


def materialize_family_event_tables_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    shared_features_5m_partition_path: Path,
    shared_features_1m_timing_partition_path: Path,
) -> dict[str, Any]:
    layout = build_layout(root_dir.resolve())
    symbol = symbol.upper()
    materialized_ts = now_utc()
    output_path = build_dataset_partition_path(
        dataset_root=layout["family_event_tables"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="events.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": FAMILY_EVENT_TABLES_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "shared_features_5m_partition_path": str(shared_features_5m_partition_path.resolve()),
            "shared_features_1m_timing_partition_path": str(shared_features_1m_timing_partition_path.resolve()),
            "family_feature_map": FAMILY_FEATURE_MAP,
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        event_rows = read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": "family_event_tables",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "partition_path": output_path,
            "row_count": len(event_rows),
            "coverage": coverage_range(event_rows, timestamp_key="candidate_ts"),
            "provenance_tag": event_rows[0]["provenance_tag"] if event_rows else None,
            "rows": event_rows,
            "families": sorted({row["family"] for row in event_rows}),
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

    feature_read_started = perf_counter()
    feature_rows = read_parquet_rows(shared_features_5m_partition_path)
    feature_read_seconds = perf_counter() - feature_read_started
    timing_read_started = perf_counter()
    timing_rows = read_parquet_rows(shared_features_1m_timing_partition_path)
    timing_read_seconds = perf_counter() - timing_read_started
    timing_by_minute = {row["timing_ts"]: row for row in timing_rows}
    timing_ts_values = [row["timing_ts"] for row in timing_rows]
    event_rows: list[dict[str, Any]] = []
    mapping_started = perf_counter()
    for feature_row in feature_rows:
        for family, (field_name, side) in FAMILY_FEATURE_MAP.items():
            if not feature_row.get(field_name):
                continue
            decision_ts = feature_row["decision_ts"]
            timing_ts = _first_timing_ts_on_or_after(decision_ts=decision_ts, timing_ts_values=timing_ts_values)
            event_rows.append(
                {
                    "event_id": f"{symbol}:{shard_id}:{family}:{decision_ts.isoformat()}",
                    "symbol": symbol,
                    "family": family,
                    "shard_id": shard_id,
                    "candidate_ts": decision_ts,
                    "event_side": side,
                    "event_phase": feature_row.get("session_phase"),
                    "eligibility_label": "candidate_flag_true",
                    "blocker_label": None,
                    "execution_model": PROBATIONARY_EXECUTION_MODEL,
                    "decision_ts": decision_ts,
                    "feature_bar_id": feature_row["bar_id"],
                    "timing_ts": timing_ts,
                    "feature_refs": json.dumps(
                        {
                            "shared_features_5m_bar_id": feature_row["bar_id"],
                            "shared_features_1m_timing_bar_id": timing_by_minute.get(timing_ts, {}).get("bar_id")
                            if timing_ts
                            else None,
                        },
                        sort_keys=True,
                    ),
                    "materialized_ts": materialized_ts,
                    "provenance_tag": f"family_event_tables:{symbol}:{shard_id}:{family}",
                }
            )
    mapping_seconds = perf_counter() - mapping_started
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, event_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name="family_event_tables",
        cache_key=cache_key,
    )
    return {
        "dataset_name": "family_event_tables",
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "partition_path": output_path,
        "row_count": len(event_rows),
        "coverage": coverage_range(event_rows, timestamp_key="candidate_ts"),
        "provenance_tag": event_rows[0]["provenance_tag"] if event_rows else None,
        "rows": event_rows,
        "families": sorted({row["family"] for row in event_rows}),
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "feature_read_seconds": round(feature_read_seconds, 6),
            "timing_read_seconds": round(timing_read_seconds, 6),
            "event_mapping_seconds": round(mapping_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def summarize_family_event_counts(event_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in event_rows:
        family = str(row["family"])
        counts[family] = counts.get(family, 0) + 1
    return counts


def _first_timing_ts_on_or_after(*, decision_ts: datetime, timing_ts_values: list[datetime]) -> datetime | None:
    index = bisect_left(timing_ts_values, decision_ts)
    if index >= len(timing_ts_values):
        return None
    return timing_ts_values[index]
