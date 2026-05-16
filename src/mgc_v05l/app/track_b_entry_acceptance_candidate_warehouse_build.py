"""Build a narrow offline Entry Acceptance candidate warehouse partition.

This command is research-only. It reads canonical historical 1m replay rows,
derives completed 5m bars, materializes shared feature and candidate surfaces,
and stops before entry, trade, compact-result, or backtest materialization.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.research.trend_participation.storage import write_storage_manifest
from mgc_v05l.research.warehouse_historical_evaluator._warehouse_common import json_range, read_parquet_rows
from mgc_v05l.research.warehouse_historical_evaluator.bootstrap import bootstrap_storage_skeleton
from mgc_v05l.research.warehouse_historical_evaluator.derived_materializer import materialize_derived_timeframe_partition
from mgc_v05l.research.warehouse_historical_evaluator.family_events import materialize_family_event_tables_partition
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout
from mgc_v05l.research.warehouse_historical_evaluator.raw_materializer import export_canonical_1m_partition
from mgc_v05l.research.warehouse_historical_evaluator.shared_features import (
    materialize_shared_features_1m_timing_partition,
    materialize_shared_features_5m_partition,
)
from mgc_v05l.research.warehouse_historical_evaluator.warehouse_evaluator import (
    materialize_lane_candidates_partition,
)


CANONICAL_1M_SOURCE = "historical_1m_canonical"
DEFAULT_FAMILY = "asiaEarlyNormalBreakoutRetestHoldLong"
LEGACY_SOURCE_EVENT_FAMILY = "asiaEarlyNormalBreakoutRetestHoldTurn"
ASIA_BREAKOUT_RETEST_HOLD_FLAG = "asia_early_normal_breakout_retest_hold_long_turn_candidate"
RESEARCH_EXECUTION_MODE = "ENTRY_ACCEPTANCE_CANDIDATE_WAREHOUSE_DRY_RUN"
SCHEMA_VERSION = "track_b_entry_acceptance_candidate_warehouse_build_v1"
SUPPORTED_DERIVED_TIMEFRAMES = frozenset({"5m"})


@dataclass(frozen=True)
class CandidateWarehouseBuildConfig:
    warehouse_root: Path
    replay_db: Path
    symbol: str
    shard_id: str
    start: datetime
    end: datetime
    lane_id: str
    family: str = DEFAULT_FAMILY
    source: str = CANONICAL_1M_SOURCE
    derived_timeframe: str = "5m"
    mode: str = "dry-run"


def build_candidate_warehouse_partition(config: CandidateWarehouseBuildConfig) -> dict[str, Any]:
    """Build one research-only candidate warehouse partition."""

    _validate_config(config)
    generated_at = datetime.now(UTC)
    source_audit = _audit_source_rows(config)
    bootstrap = _bootstrap_candidate_layout(config.warehouse_root)
    layout = build_layout(config.warehouse_root)

    raw_result = export_canonical_1m_partition(
        root_dir=config.warehouse_root,
        sqlite_path=config.replay_db,
        symbol=config.symbol,
        shard_id=config.shard_id,
        start_ts=config.start,
        end_ts=config.end,
        data_source=config.source,
    )
    raw_rows = list(raw_result.get("rows") or read_parquet_rows(Path(raw_result["partition_path"])))
    _validate_ohlc_rows(raw_rows, label="raw_bars_1m")

    derived_result = materialize_derived_timeframe_partition(
        root_dir=config.warehouse_root,
        symbol=config.symbol,
        shard_id=config.shard_id,
        year=config.start.year,
        timeframe=config.derived_timeframe,
        raw_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
        materialized_ts=generated_at,
    )
    derived_rows = list(derived_result.get("rows") or read_parquet_rows(Path(derived_result["partition_path"])))
    _validate_derived_rows(derived_rows, timeframe=config.derived_timeframe)

    shared_5m_result = materialize_shared_features_5m_partition(
        root_dir=config.warehouse_root,
        symbol=config.symbol,
        shard_id=config.shard_id,
        year=config.start.year,
        derived_5m_partition_path=Path(derived_result["partition_path"]),
        derived_from_version=str(raw_result["raw_version"]),
    )
    shared_5m_rows = list(shared_5m_result.get("rows") or read_parquet_rows(Path(shared_5m_result["partition_path"])))
    _validate_shared_features(shared_5m_rows, derived_rows=derived_rows)

    shared_1m_timing_result = materialize_shared_features_1m_timing_partition(
        root_dir=config.warehouse_root,
        symbol=config.symbol,
        shard_id=config.shard_id,
        year=config.start.year,
        raw_1m_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
    )

    family_event_result = materialize_family_event_tables_partition(
        root_dir=config.warehouse_root,
        symbol=config.symbol,
        shard_id=config.shard_id,
        year=config.start.year,
        shared_features_5m_partition_path=Path(shared_5m_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_1m_timing_result["partition_path"]),
    )

    lane_candidate_result = materialize_lane_candidates_partition(
        root_dir=config.warehouse_root,
        symbol=config.symbol,
        shard_id=config.shard_id,
        year=config.start.year,
        lane_ids=[config.lane_id],
        family_event_tables_partition_path=Path(family_event_result["partition_path"]),
        shared_features_5m_partition_path=Path(shared_5m_result["partition_path"]),
    )
    lane_candidate_rows = list(
        lane_candidate_result.get("rows") or read_parquet_rows(Path(lane_candidate_result["partition_path"]))
    )

    validation_summary = _validation_summary(
        config=config,
        source_audit=source_audit,
        raw_rows=raw_rows,
        derived_rows=derived_rows,
        shared_5m_rows=shared_5m_rows,
        lane_candidate_rows=lane_candidate_rows,
    )
    manifest = _manifest(
        config=config,
        generated_at=generated_at,
        bootstrap=bootstrap,
        source_audit=source_audit,
        validation_summary=validation_summary,
        raw_result=raw_result,
        derived_result=derived_result,
        shared_5m_result=shared_5m_result,
        shared_1m_timing_result=shared_1m_timing_result,
        family_event_result=family_event_result,
        lane_candidate_result=lane_candidate_result,
        layout=layout,
    )
    manifest_path = layout["root"] / "manifest.json"
    validation_path = layout["root"] / "validation_summary.json"
    write_storage_manifest(manifest_path, manifest)
    write_storage_manifest(validation_path, validation_summary)
    manifest["manifest_path"] = str(manifest_path)
    manifest["validation_summary_path"] = str(validation_path)
    return manifest


def _validate_config(config: CandidateWarehouseBuildConfig) -> None:
    if config.mode != "dry-run":
        raise ValueError("Entry Acceptance candidate warehouse builder supports dry-run mode only.")
    if config.source != CANONICAL_1M_SOURCE:
        raise ValueError("Entry Acceptance candidate warehouse builder requires historical_1m_canonical source.")
    if config.derived_timeframe not in SUPPORTED_DERIVED_TIMEFRAMES:
        supported = ", ".join(sorted(SUPPORTED_DERIVED_TIMEFRAMES))
        raise ValueError(f"Entry Acceptance candidate warehouse builder v1 supports derived_timeframe in {{{supported}}}.")
    if config.family != DEFAULT_FAMILY:
        raise ValueError(f"Unsupported Entry Acceptance family: {config.family}")
    symbol = config.symbol.upper()
    if not config.lane_id.lower().endswith(f"__{symbol.lower()}"):
        raise ValueError(f"lane_id must match symbol {symbol}: {config.lane_id}")
    if "asia_early_normal_breakout_retest_hold_turn" not in config.lane_id.lower():
        raise ValueError("lane_id must be the Asia normal breakout/retest/hold pilot lane.")
    if config.start >= config.end:
        raise ValueError("start must be before end.")


def _bootstrap_candidate_layout(warehouse_root: Path) -> dict[str, Any]:
    try:
        return bootstrap_storage_skeleton(warehouse_root)
    except RuntimeError as exc:
        if "duckdb" not in str(exc).lower():
            raise
        layout = build_layout(warehouse_root)
        return {
            "root": str(layout["root"]),
            "duckdb": None,
            "storage_manifest": None,
            "contracts_catalog": None,
            "shard_stitch_contract": None,
            "dataset_count": None,
            "duckdb_table_count": 0,
            "catalog_bootstrap_skipped": True,
            "catalog_bootstrap_skip_reason": str(exc),
        }


def _audit_source_rows(config: CandidateWarehouseBuildConfig) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{config.replay_db.resolve()}?mode=ro", uri=True)
    try:
        selected = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where symbol = ?
              and timeframe = '1m'
              and data_source = ?
              and end_ts >= ?
              and end_ts <= ?
            """,
            [config.symbol.upper(), config.source, config.start.isoformat(), config.end.isoformat()],
        ).fetchone()
        selected_count = int(selected[0] or 0)
        if selected_count <= 0:
            raise ValueError("No historical canonical 1m source rows found for requested partition.")

        duplicate_rows = connection.execute(
            """
            select end_ts, count(*) as count
            from bars
            where symbol = ?
              and timeframe = '1m'
              and data_source = ?
              and end_ts >= ?
              and end_ts <= ?
            group by end_ts
            having count(*) > 1
            order by end_ts
            limit 10
            """,
            [config.symbol.upper(), config.source, config.start.isoformat(), config.end.isoformat()],
        ).fetchall()
        if duplicate_rows:
            raise ValueError(f"Duplicate canonical 1m timestamps found: {duplicate_rows[:3]}")

        mixed_timeframe_rows = connection.execute(
            """
            select timeframe, count(*)
            from bars
            where symbol = ?
              and data_source = ?
              and end_ts >= ?
              and end_ts <= ?
              and timeframe != '1m'
            group by timeframe
            order by timeframe
            """,
            [config.symbol.upper(), config.source, config.start.isoformat(), config.end.isoformat()],
        ).fetchall()
        if mixed_timeframe_rows:
            raise ValueError(f"Mixed timeframe source rows found for canonical source: {mixed_timeframe_rows}")

        source_breakdown = connection.execute(
            """
            select data_source, timeframe, count(*)
            from bars
            where symbol = ?
              and end_ts >= ?
              and end_ts <= ?
            group by data_source, timeframe
            order by data_source, timeframe
            """,
            [config.symbol.upper(), config.start.isoformat(), config.end.isoformat()],
        ).fetchall()
    finally:
        connection.close()

    return {
        "source_rows_exist": True,
        "selected_row_count": selected_count,
        "source_start": selected[1],
        "source_end": selected[2],
        "duplicate_1m_timestamps": 0,
        "mixed_timeframe_rows": [],
        "source_breakdown": [
            {"data_source": str(row[0]), "timeframe": str(row[1]), "row_count": int(row[2])}
            for row in source_breakdown
        ],
    }


def _validate_ohlc_rows(rows: Sequence[Mapping[str, Any]], *, label: str) -> None:
    for index, row in enumerate(rows):
        open_price = float(row["open"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        close_price = float(row["close"])
        if high_price < max(open_price, close_price, low_price) or low_price > min(open_price, close_price, high_price):
            raise ValueError(f"{label} OHLC validity failed at row {index}.")


def _validate_derived_rows(rows: Sequence[Mapping[str, Any]], *, timeframe: str) -> None:
    if timeframe != "5m":
        raise ValueError("Only completed 5m derived rows are valid for this builder.")
    if not rows:
        raise ValueError("No completed derived 5m rows were produced.")
    _validate_ohlc_rows(rows, label="derived_bars_5m")
    timestamps = [row.get("bar_ts") for row in rows]
    duplicates = [value for value, count in Counter(timestamps).items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate derived 5m timestamps found: {duplicates[:3]}")
    if any(str(row.get("timeframe")) != "5m" for row in rows):
        raise ValueError("Derived rows contain a non-5m timeframe.")


def _validate_shared_features(
    rows: Sequence[Mapping[str, Any]],
    *,
    derived_rows: Sequence[Mapping[str, Any]],
) -> None:
    if len(rows) != len(derived_rows):
        raise ValueError("shared_features_5m row count must equal derived_bars_5m row count.")
    if rows and ASIA_BREAKOUT_RETEST_HOLD_FLAG not in rows[0]:
        raise ValueError(f"Candidate flag field missing from shared_features_5m: {ASIA_BREAKOUT_RETEST_HOLD_FLAG}")


def _validation_summary(
    *,
    config: CandidateWarehouseBuildConfig,
    source_audit: Mapping[str, Any],
    raw_rows: Sequence[Mapping[str, Any]],
    derived_rows: Sequence[Mapping[str, Any]],
    shared_5m_rows: Sequence[Mapping[str, Any]],
    lane_candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_flag_true_count = sum(1 for row in shared_5m_rows if bool(row.get(ASIA_BREAKOUT_RETEST_HOLD_FLAG)))
    return {
        "schema_version": f"{SCHEMA_VERSION}.validation_summary",
        "research_execution_mode": RESEARCH_EXECUTION_MODE,
        "symbol": config.symbol.upper(),
        "shard_id": config.shard_id,
        "source_rows_exist": bool(source_audit.get("source_rows_exist")),
        "source": config.source,
        "source_start": source_audit.get("source_start"),
        "source_end": source_audit.get("source_end"),
        "source_row_count": int(source_audit.get("selected_row_count") or 0),
        "duplicate_1m_timestamps": 0,
        "base_timeframe": "1m",
        "derived_timeframe": config.derived_timeframe,
        "derived_completed_bar_only": True,
        "duplicate_derived_timestamps": 0,
        "ohlc_valid": True,
        "raw_1m_row_count": len(raw_rows),
        "derived_5m_row_count": len(derived_rows),
        "shared_features_5m_row_count": len(shared_5m_rows),
        "shared_features_matches_derived": len(shared_5m_rows) == len(derived_rows),
        "candidate_family": config.family,
        "source_event_family": LEGACY_SOURCE_EVENT_FAMILY,
        "candidate_flag_field": ASIA_BREAKOUT_RETEST_HOLD_FLAG,
        "candidate_flag_true_count": candidate_flag_true_count,
        "candidate_flags_can_be_produced": ASIA_BREAKOUT_RETEST_HOLD_FLAG in shared_5m_rows[0] if shared_5m_rows else False,
        "lane_candidate_row_count": len(lane_candidate_rows),
        "provenance_fields_written": True,
        "strategy_behavior_changed": False,
        "lane_execution_performed": False,
        "trade_outputs_materialized": False,
        "broker_state_mutated": False,
        "runtime_artifacts_used_as_input": False,
    }


def _manifest(
    *,
    config: CandidateWarehouseBuildConfig,
    generated_at: datetime,
    bootstrap: Mapping[str, Any],
    source_audit: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    raw_result: Mapping[str, Any],
    derived_result: Mapping[str, Any],
    shared_5m_result: Mapping[str, Any],
    shared_1m_timing_result: Mapping[str, Any],
    family_event_result: Mapping[str, Any],
    lane_candidate_result: Mapping[str, Any],
    layout: Mapping[str, Path],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "research_execution_mode": RESEARCH_EXECUTION_MODE,
        "mode": config.mode,
        "generated_at": generated_at.isoformat(),
        "git_head": _git_head(),
        "warehouse_root": str(config.warehouse_root.resolve()),
        "replay_db": str(config.replay_db.resolve()),
        "symbol": config.symbol.upper(),
        "shard_id": config.shard_id,
        "lane_id": config.lane_id,
        "candidate_family": config.family,
        "source_event_family": LEGACY_SOURCE_EVENT_FAMILY,
        "source": config.source,
        "source_start": source_audit.get("source_start"),
        "source_end": source_audit.get("source_end"),
        "base_timeframe": "1m",
        "derived_timeframe": config.derived_timeframe,
        "timeframe_source": "DERIVED",
        "base_timeframe_if_derived": "1m",
        "aggregation_method": "complete_bucket_resample_from_canonical_1m",
        "anchor_rule": "UTC epoch-aligned 5-minute bucket end; incomplete buckets dropped",
        "completed_bar_only": True,
        "bootstrap": dict(bootstrap),
        "validation_summary": dict(validation_summary),
        "safety_flags": {
            "strategy_behavior_changed": False,
            "lane_execution_performed": False,
            "trade_outputs_materialized": False,
            "broker_state_mutated": False,
            "runtime_artifacts_used_as_input": False,
        },
        "outputs": {
            "raw_bars_1m": str(raw_result["partition_path"]),
            "derived_bars_5m": str(derived_result["partition_path"]),
            "shared_features_5m": str(shared_5m_result["partition_path"]),
            "shared_features_1m_timing": str(shared_1m_timing_result["partition_path"]),
            "family_event_tables": str(family_event_result["partition_path"]),
            "lane_candidates": str(lane_candidate_result["partition_path"]),
        },
        "row_counts": {
            "raw_bars_1m": int(raw_result["row_count"]),
            "derived_bars_5m": int(derived_result["row_count"]),
            "shared_features_5m": int(shared_5m_result["row_count"]),
            "shared_features_1m_timing": int(shared_1m_timing_result["row_count"]),
            "family_event_tables": int(family_event_result["row_count"]),
            "lane_candidates": int(lane_candidate_result["row_count"]),
        },
        "coverage": {
            "raw_bars_1m": json_range(raw_result["coverage"]),
            "derived_bars_5m": json_range(derived_result["coverage"]),
            "shared_features_5m": json_range(shared_5m_result["coverage"]),
            "shared_features_1m_timing": json_range(shared_1m_timing_result["coverage"]),
            "family_event_tables": json_range(family_event_result["coverage"]),
            "lane_candidates": json_range(lane_candidate_result["coverage"]),
        },
        "layout": {key: str(value) for key, value in layout.items()},
    }


def _git_head() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", required=True, type=Path)
    parser.add_argument("--replay-db", required=True, type=Path)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--shard-id", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--family", default=DEFAULT_FAMILY)
    parser.add_argument("--source", default=CANONICAL_1M_SOURCE)
    parser.add_argument("--mode", default="dry-run", choices=("dry-run",))
    args = parser.parse_args(argv)
    manifest = build_candidate_warehouse_partition(
        CandidateWarehouseBuildConfig(
            warehouse_root=args.warehouse_root,
            replay_db=args.replay_db,
            symbol=str(args.symbol).upper(),
            shard_id=str(args.shard_id),
            start=_parse_timestamp(args.start),
            end=_parse_timestamp(args.end),
            derived_timeframe=str(args.timeframe),
            lane_id=str(args.lane_id),
            family=str(args.family),
            source=str(args.source),
            mode=str(args.mode),
        )
    )
    print(
        json.dumps(
            {
                "manifest_path": manifest["manifest_path"],
                "validation_summary_path": manifest["validation_summary_path"],
                "outputs": manifest["outputs"],
                "row_counts": manifest["row_counts"],
                "validation_summary": manifest["validation_summary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
