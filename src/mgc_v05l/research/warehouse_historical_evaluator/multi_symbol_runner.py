"""Reusable multi-symbol shard runner for the warehouse historical evaluator."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from ..trend_participation.storage import write_storage_manifest
from .bootstrap import bootstrap_storage_skeleton
from .catalog import build_coverage_row, build_partition_row, register_catalog_metadata
from .derived_materializer import materialize_derived_timeframe_partition
from .family_events import materialize_family_event_tables_partition, summarize_family_event_counts
from .layout import build_layout
from .raw_materializer import coerce_timestamp, export_canonical_1m_partition
from .shared_features import (
    materialize_shared_features_1m_timing_partition,
    materialize_shared_features_5m_partition,
)
from .warehouse_evaluator import (
    materialize_lane_candidates_partition,
    materialize_lane_closed_trades_partition,
    materialize_lane_compact_results_partition,
    materialize_lane_entries_partition,
)


DEFAULT_SQLITE_PATH = Path("mgc_v05l.replay.sqlite3")
DEFAULT_BASELINE_REPORT_PATH = Path(
    "outputs/reports/strategy_universe_q1_2024_exec_vwap_compact/strategy_universe_retest.json"
)
DEFAULT_SHARD_ID = "2024Q1"
DEFAULT_START = "2024-01-01T00:00:00-05:00"
DEFAULT_END = "2024-03-31T23:59:00-04:00"
DEFAULT_BASKET: dict[str, list[str]] = {
    "MGC": [
        "mgc_us_late_pause_resume_long_turn__MGC",
        "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
        "mgc_asia_early_pause_resume_short_turn__MGC",
    ],
    "GC": [
        "gc_us_late_pause_resume_long_turn__GC",
        "gc_asia_early_normal_breakout_retest_hold_turn__GC",
    ],
}


def run_multi_symbol_warehouse_shard(
    *,
    root_dir: Path,
    sqlite_path: Path,
    symbol_lane_map: dict[str, list[str]],
    shard_id: str,
    start_ts: datetime,
    end_ts: datetime,
    baseline_report_path: Path,
) -> dict[str, Any]:
    root_dir = root_dir.resolve()
    sqlite_path = sqlite_path.resolve()
    baseline_report_path = baseline_report_path.resolve()
    bootstrap_storage_skeleton(root_dir)
    layout = build_layout(root_dir)
    generated_at = datetime.now(UTC)

    partition_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    symbol_payloads: list[dict[str, Any]] = []
    all_compact_rows: list[dict[str, Any]] = []
    stage_timing_rollup: dict[str, float] = {}
    cache_rollup: dict[str, dict[str, int]] = {}
    symbol_stage_timings: dict[str, dict[str, Any]] = {}

    for symbol, lane_ids in symbol_lane_map.items():
        symbol_payload = _materialize_symbol_shard(
            root_dir=root_dir,
            sqlite_path=sqlite_path,
            symbol=symbol,
            lane_ids=lane_ids,
            shard_id=shard_id,
            start_ts=start_ts,
            end_ts=end_ts,
            generated_at=generated_at,
        )
        partition_rows.extend(symbol_payload["partition_rows"])
        coverage_rows.extend(symbol_payload["coverage_rows"])
        symbol_payloads.append(symbol_payload["proof"])
        all_compact_rows.extend(symbol_payload["compact_rows"])
        symbol_stage_timings[symbol] = symbol_payload["stage_timing"]
        for stage_name, seconds in symbol_payload["stage_timing"]["aggregate_seconds"].items():
            stage_timing_rollup[stage_name] = round(stage_timing_rollup.get(stage_name, 0.0) + float(seconds), 6)
        for stage_name, cache_payload in symbol_payload["stage_timing"]["cache"].items():
            target = cache_rollup.setdefault(stage_name, {"hits": 0, "misses": 0})
            if cache_payload.get("cache_hit"):
                target["hits"] += 1
            else:
                target["misses"] += 1

    catalog_started = perf_counter()
    register_catalog_metadata(
        duckdb_path=layout["duckdb"],
        dataset_root=layout["root"],
        generated_at=generated_at,
        partition_rows=partition_rows,
        coverage_rows=coverage_rows,
        compact_run_row={
            "run_id": f"warehouse_basket_{shard_id.lower()}",
            "run_ts": generated_at.isoformat(),
            "artifact_mode": "compact_only",
            "window_start": start_ts.isoformat(),
            "window_end": end_ts.isoformat(),
            "notes": f"Tiny-basket warehouse shard proof for {','.join(sorted(symbol_lane_map))} {shard_id}.",
        },
    )
    catalog_seconds = perf_counter() - catalog_started
    stitched_started = perf_counter()
    stitched = _build_duckdb_stitched_summary(duckdb_path=layout["duckdb"])
    stitched_seconds = perf_counter() - stitched_started
    baseline_started = perf_counter()
    baseline_rows = _load_baseline_rows(baseline_report_path=baseline_report_path, lane_ids=[lane for lanes in symbol_lane_map.values() for lane in lanes])
    comparisons = _compare_against_baseline(warehouse_rows=all_compact_rows, baseline_rows=baseline_rows)
    baseline_seconds = perf_counter() - baseline_started

    proof_payload = {
        "generated_at": generated_at.isoformat(),
        "shard_id": shard_id,
        "basket_symbols": sorted(symbol_lane_map),
        "symbol_payloads": symbol_payloads,
        "stitched_summary": stitched,
        "baseline_report_path": str(baseline_report_path),
        "comparisons": comparisons,
        "timing": {
            "stages_by_symbol": symbol_stage_timings,
            "aggregate_stage_seconds": stage_timing_rollup,
            "cache": cache_rollup,
            "catalog_registration_seconds": round(catalog_seconds, 6),
            "stitched_summary_seconds": round(stitched_seconds, 6),
            "baseline_compare_seconds": round(baseline_seconds, 6),
            "total_materialization_seconds": round(
                sum(stage_timing_rollup.values()) + catalog_seconds + stitched_seconds + baseline_seconds,
                6,
            ),
        },
        "warehouse_paths": {
            "duckdb": str(layout["duckdb"]),
        },
        "catalog_views": [
            "raw_bars_1m",
            "derived_bars_5m",
            "derived_bars_10m",
            "shared_features_5m",
            "shared_features_1m_timing",
            "family_event_tables",
            "lane_candidates",
            "lane_entries",
            "lane_closed_trades",
            "lane_compact_results",
            "stitched_compact_by_symbol",
            "stitched_compact_basket",
        ],
        "dataset_partition_count": len(partition_rows),
    }
    proof_path = layout["manifests"] / "multi_symbol_quarter_proof.json"
    write_storage_manifest(proof_path, proof_payload)
    proof_md_path = layout["manifests"] / "multi_symbol_quarter_proof.md"
    proof_md_path.write_text(_build_proof_markdown(proof_payload), encoding="utf-8")
    return {
        "proof_path": str(proof_path),
        "proof_markdown_path": str(proof_md_path),
        "duckdb_path": str(layout["duckdb"]),
        "basket_symbols": sorted(symbol_lane_map),
        "shard_id": shard_id,
        "timing": proof_payload["timing"],
    }


def _materialize_symbol_shard(
    *,
    root_dir: Path,
    sqlite_path: Path,
    symbol: str,
    lane_ids: list[str],
    shard_id: str,
    start_ts: datetime,
    end_ts: datetime,
    generated_at: datetime,
) -> dict[str, Any]:
    symbol = symbol.upper()
    stage_details: dict[str, dict[str, Any]] = {}
    raw_result = export_canonical_1m_partition(
        root_dir=root_dir,
        sqlite_path=sqlite_path,
        symbol=symbol,
        shard_id=shard_id,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    stage_details["raw_bars_1m"] = _stage_detail(raw_result)
    derived_5m_result = materialize_derived_timeframe_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        timeframe="5m",
        raw_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
        materialized_ts=generated_at,
    )
    stage_details["derived_bars_5m"] = _stage_detail(derived_5m_result)
    derived_10m_result = materialize_derived_timeframe_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        timeframe="10m",
        raw_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
        materialized_ts=generated_at,
    )
    stage_details["derived_bars_10m"] = _stage_detail(derived_10m_result)
    shared_features_5m_result = materialize_shared_features_5m_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        derived_5m_partition_path=Path(derived_5m_result["partition_path"]),
        derived_from_version=str(raw_result["raw_version"]),
    )
    stage_details["shared_features_5m"] = _stage_detail(shared_features_5m_result)
    shared_features_1m_timing_result = materialize_shared_features_1m_timing_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        raw_1m_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
    )
    stage_details["shared_features_1m_timing"] = _stage_detail(shared_features_1m_timing_result)
    family_event_result = materialize_family_event_tables_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        shared_features_5m_partition_path=Path(shared_features_5m_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
    stage_details["family_event_tables"] = _stage_detail(family_event_result)
    lane_candidate_result = materialize_lane_candidates_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_ids=lane_ids,
        family_event_tables_partition_path=Path(family_event_result["partition_path"]),
        shared_features_5m_partition_path=Path(shared_features_5m_result["partition_path"]),
    )
    stage_details["lane_candidates"] = _stage_detail(lane_candidate_result)
    lane_entry_result = materialize_lane_entries_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_definitions=lane_candidate_result["lane_definitions"],
        lane_candidates_partition_path=Path(lane_candidate_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
    stage_details["lane_entries"] = _stage_detail(lane_entry_result)
    lane_closed_trade_result = materialize_lane_closed_trades_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_definitions=lane_candidate_result["lane_definitions"],
        lane_entries_partition_path=Path(lane_entry_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
    stage_details["lane_closed_trades"] = _stage_detail(lane_closed_trade_result)
    compact_result = materialize_lane_compact_results_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_definitions=lane_candidate_result["lane_definitions"],
        available_families=set(family_event_result["families"]),
        canonical_input_range={"start": raw_result["coverage"]["start"], "end": raw_result["coverage"]["end"], "shard_id": shard_id},
        lane_candidates_partition_path=Path(lane_candidate_result["partition_path"]),
        lane_entries_partition_path=Path(lane_entry_result["partition_path"]),
        lane_closed_trades_partition_path=Path(lane_closed_trade_result["partition_path"]),
    )
    stage_details["lane_compact_results"] = _stage_detail(compact_result)

    partition_rows = [
        build_partition_row(dataset_name=str(raw_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="1m", partition_path=Path(raw_result["partition_path"]), row_count=int(raw_result["row_count"]), coverage=raw_result["coverage"], provenance_tag=raw_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(derived_5m_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="5m", partition_path=Path(derived_5m_result["partition_path"]), row_count=int(derived_5m_result["row_count"]), coverage=derived_5m_result["coverage"], provenance_tag=derived_5m_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(derived_10m_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="10m", partition_path=Path(derived_10m_result["partition_path"]), row_count=int(derived_10m_result["row_count"]), coverage=derived_10m_result["coverage"], provenance_tag=derived_10m_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(shared_features_5m_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="5m", partition_path=Path(shared_features_5m_result["partition_path"]), row_count=int(shared_features_5m_result["row_count"]), coverage=shared_features_5m_result["coverage"], provenance_tag=shared_features_5m_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(shared_features_1m_timing_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="1m", partition_path=Path(shared_features_1m_timing_result["partition_path"]), row_count=int(shared_features_1m_timing_result["row_count"]), coverage=shared_features_1m_timing_result["coverage"], provenance_tag=shared_features_1m_timing_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(family_event_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe=None, partition_path=Path(family_event_result["partition_path"]), row_count=int(family_event_result["row_count"]), coverage=family_event_result["coverage"], provenance_tag=family_event_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(lane_candidate_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe=None, partition_path=Path(lane_candidate_result["partition_path"]), row_count=int(lane_candidate_result["row_count"]), coverage=lane_candidate_result["coverage"], provenance_tag=lane_candidate_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(lane_entry_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="1m", partition_path=Path(lane_entry_result["partition_path"]), row_count=int(lane_entry_result["row_count"]), coverage=lane_entry_result["coverage"], provenance_tag=lane_entry_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(lane_closed_trade_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe="1m", partition_path=Path(lane_closed_trade_result["partition_path"]), row_count=int(lane_closed_trade_result["row_count"]), coverage=lane_closed_trade_result["coverage"], provenance_tag=lane_closed_trade_result["provenance_tag"], generated_at=generated_at),
        build_partition_row(dataset_name=str(compact_result["dataset_name"]), symbol=symbol, year=start_ts.year, shard_id=shard_id, timeframe=None, partition_path=Path(compact_result["partition_path"]), row_count=int(compact_result["row_count"]), coverage=compact_result["coverage"], provenance_tag=compact_result["provenance_tag"], generated_at=generated_at),
    ]
    coverage_rows = [
        build_coverage_row(layer="raw_1m_warehouse", symbol=symbol, strategy_id=None, coverage=raw_result["coverage"], provenance_tag=raw_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="derived_5m_warehouse", symbol=symbol, strategy_id=None, coverage=derived_5m_result["coverage"], provenance_tag=derived_5m_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="derived_10m_warehouse", symbol=symbol, strategy_id=None, coverage=derived_10m_result["coverage"], provenance_tag=derived_10m_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="shared_features_5m_warehouse", symbol=symbol, strategy_id=None, coverage=shared_features_5m_result["coverage"], provenance_tag=shared_features_5m_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="shared_features_1m_timing_warehouse", symbol=symbol, strategy_id=None, coverage=shared_features_1m_timing_result["coverage"], provenance_tag=shared_features_1m_timing_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="family_event_tables_warehouse", symbol=symbol, strategy_id=None, coverage=family_event_result["coverage"], provenance_tag=family_event_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="lane_candidates_warehouse", symbol=symbol, strategy_id=None, coverage=lane_candidate_result["coverage"], provenance_tag=lane_candidate_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="lane_entries_warehouse", symbol=symbol, strategy_id=None, coverage=lane_entry_result["coverage"], provenance_tag=lane_entry_result["provenance_tag"], generated_at=generated_at),
        build_coverage_row(layer="lane_closed_trades_warehouse", symbol=symbol, strategy_id=None, coverage=lane_closed_trade_result["coverage"], provenance_tag=lane_closed_trade_result["provenance_tag"], generated_at=generated_at),
    ]
    for row in compact_result["rows"]:
        coverage_rows.append(
            build_coverage_row(
                layer="compact_result_warehouse",
                symbol=symbol,
                strategy_id=row["lane_id"],
                coverage={"start": row["emitted_compact_start"], "end": row["emitted_compact_end"]},
                provenance_tag=row["execution_model"],
                generated_at=generated_at,
            )
        )

    proof = {
        "symbol": symbol,
        "lane_ids": lane_ids,
        "canonical_raw_input_range": _json_range(raw_result["coverage"]),
        "derived_5m_range": _json_range(derived_5m_result["coverage"]),
        "derived_10m_range": _json_range(derived_10m_result["coverage"]),
        "shared_features_5m_range": _json_range(shared_features_5m_result["coverage"]),
        "shared_features_1m_timing_range": _json_range(shared_features_1m_timing_result["coverage"]),
        "family_event_table_range": _json_range(family_event_result["coverage"]),
        "lane_candidate_range": _json_range(lane_candidate_result["coverage"]),
        "lane_entry_range": _json_range(lane_entry_result["coverage"]),
        "lane_closed_trade_range": _json_range(lane_closed_trade_result["coverage"]),
        "compact_result_range": _json_range(compact_result["coverage"]),
        "family_event_counts": summarize_family_event_counts(family_event_result["rows"]),
        "compact_rows": [_serialize_compact_row(row) for row in compact_result["rows"]],
        "stage_timing": stage_details,
    }
    return {
        "partition_rows": partition_rows,
        "coverage_rows": coverage_rows,
        "compact_rows": compact_result["rows"],
        "proof": proof,
        "stage_timing": {
            "details": stage_details,
            "aggregate_seconds": {
                stage_name: float(stage_payload["timing"].get("total_seconds") or 0.0)
                for stage_name, stage_payload in stage_details.items()
            },
            "cache": {
                stage_name: {"cache_hit": bool(stage_payload["cache"].get("cache_hit"))}
                for stage_name, stage_payload in stage_details.items()
            },
        },
    }


def _load_baseline_rows(*, baseline_report_path: Path, lane_ids: list[str]) -> dict[str, dict[str, Any]]:
    payload = json.loads(baseline_report_path.read_text(encoding="utf-8"))
    by_lane = {str(row["strategy_id"]): row for row in payload.get("results", [])}
    loaded: dict[str, dict[str, Any]] = {}
    for lane_id in lane_ids:
        row = by_lane.get(lane_id)
        if row is None:
            continue
        eligibility_status = str(row.get("eligibility_status") or "unknown")
        trade_count = int(row.get("metrics", {}).get("trade_count") or 0)
        if eligibility_status.startswith("missing"):
            result_classification = "missing"
        elif trade_count == 0:
            result_classification = "zero_trade"
        else:
            result_classification = "nonzero_trade"
        loaded[lane_id] = {
            "lane_id": lane_id,
            "family": row.get("family"),
            "execution_model": row.get("execution_model"),
            "result_classification": result_classification,
            "trade_count": trade_count,
            "net_pnl": float(row.get("metrics", {}).get("net_pnl") or 0.0),
            "win_rate": row.get("metrics", {}).get("win_rate"),
            "canonical_input_range": row.get("coverage", {}).get("raw_market_data") or {},
            "emitted_compact_range": row.get("coverage", {}).get("derived_playback") or {},
            "closed_trade_range": row.get("coverage", {}).get("closed_trade_economics") or {},
        }
    return loaded


def _compare_against_baseline(*, warehouse_rows: list[dict[str, Any]], baseline_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for row in sorted(warehouse_rows, key=lambda item: str(item["lane_id"])):
        lane_id = str(row["lane_id"])
        baseline = baseline_rows.get(lane_id)
        comparisons.append(
            {
                "lane_id": lane_id,
                "family": row["family"],
                "warehouse": _serialize_compact_row(row),
                "baseline": baseline,
                "deltas": {
                    "trade_count": None if baseline is None else int(row["trade_count"]) - int(baseline["trade_count"]),
                    "net_pnl": None if baseline is None else round(float(row["net_pnl"]) - float(baseline["net_pnl"]), 6),
                    "win_rate": None
                    if baseline is None or baseline["win_rate"] is None
                    else round(float(row["win_rate"]) - (float(baseline["win_rate"]) / 100.0), 6),
                },
                "matches": {
                    "execution_model": False if baseline is None else str(row["execution_model"]) == str(baseline["execution_model"]),
                    "classification": False if baseline is None else str(row["result_classification"]) == str(baseline["result_classification"]),
                },
            }
        )
    return comparisons


def _build_duckdb_stitched_summary(*, duckdb_path: Path) -> dict[str, Any]:
    import duckdb

    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            create or replace view stitched_compact_by_symbol as
            select
                symbol,
                shard_id,
                count(*) as lane_count,
                sum(trade_count) as trade_count,
                sum(net_pnl) as net_pnl,
                sum(winners) as winners,
                sum(losers) as losers,
                case when sum(trade_count) > 0 then cast(sum(winners) as double) / sum(trade_count) else 0.0 end as win_rate
            from lane_compact_results
            group by symbol, shard_id
            order by symbol, shard_id
            """
        )
        connection.execute(
            """
            create or replace view stitched_compact_basket as
            select
                shard_id,
                count(*) as lane_count,
                sum(trade_count) as trade_count,
                sum(net_pnl) as net_pnl,
                sum(winners) as winners,
                sum(losers) as losers,
                case when sum(trade_count) > 0 then cast(sum(winners) as double) / sum(trade_count) else 0.0 end as win_rate
            from lane_compact_results
            group by shard_id
            order by shard_id
            """
        )
        by_symbol = connection.execute(
            "select symbol, shard_id, lane_count, trade_count, round(net_pnl, 6), round(win_rate, 6) from stitched_compact_by_symbol"
        ).fetchall()
        basket = connection.execute(
            "select shard_id, lane_count, trade_count, round(net_pnl, 6), round(win_rate, 6) from stitched_compact_basket"
        ).fetchall()
    finally:
        connection.close()
    return {
        "by_symbol": by_symbol,
        "basket": basket,
    }


def _serialize_compact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": row["lane_id"],
        "family": row["family"],
        "symbol": row["symbol"],
        "execution_model": row["execution_model"],
        "result_classification": row["result_classification"],
        "trade_count": int(row["trade_count"]),
        "net_pnl": float(row["net_pnl"]),
        "win_rate": float(row["win_rate"]) if row["win_rate"] is not None else None,
        "canonical_input_range": _json_range({"start": row["canonical_input_start"], "end": row["canonical_input_end"]}),
        "emitted_compact_range": _json_range({"start": row["emitted_compact_start"], "end": row["emitted_compact_end"]}),
        "closed_trade_range": _json_range({"start": row["closed_trade_start"], "end": row["closed_trade_end"]}),
    }


def _json_range(coverage: dict[str, Any]) -> dict[str, str | None]:
    return {
        "start": coverage["start"].isoformat() if coverage.get("start") else None,
        "end": coverage["end"].isoformat() if coverage.get("end") else None,
    }


def _build_proof_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Multi-Symbol Warehouse Quarter Proof",
        "",
        f"- Shard: `{payload['shard_id']}`",
        f"- Symbols: `{', '.join(payload['basket_symbols'])}`",
        f"- Dataset partitions registered: `{payload['dataset_partition_count']}`",
        "",
        "## Stitched Summary",
        "",
    ]
    for row in payload["stitched_summary"]["by_symbol"]:
        lines.append(
            f"- `{row[0]}` lane_count=`{row[2]}` trade_count=`{row[3]}` net_pnl=`{row[4]}` win_rate=`{row[5]}`"
        )
    lines.extend(["", "## Baseline Comparison", ""])
    for row in payload["comparisons"]:
        lines.append(
            f"- `{row['lane_id']}` warehouse_trade_count=`{row['warehouse']['trade_count']}` "
            f"baseline_trade_count=`{None if row['baseline'] is None else row['baseline']['trade_count']}` "
            f"delta=`{row['deltas']['trade_count']}` exec_match=`{row['matches']['execution_model']}` "
            f"class_match=`{row['matches']['classification']}`"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def _parse_symbol_lanes(specs: list[str] | None) -> dict[str, list[str]]:
    if not specs:
        return {symbol: list(lanes) for symbol, lanes in DEFAULT_BASKET.items()}
    parsed: dict[str, list[str]] = {}
    for spec in specs:
        symbol_part, lane_part = spec.split("=", 1)
        parsed[symbol_part.upper()] = [item.strip() for item in lane_part.split(",") if item.strip()]
    return parsed


def _stage_detail(stage_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_count": int(stage_result.get("row_count") or 0),
        "timing": dict(stage_result.get("timing") or {}),
        "cache": dict(stage_result.get("cache") or {}),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a tiny-basket quarter warehouse evaluator proof.")
    parser.add_argument("--root", default="outputs/warehouse_historical_evaluator", help="Warehouse root directory.")
    parser.add_argument("--sqlite", default=str(DEFAULT_SQLITE_PATH), help="Canonical replay SQLite path.")
    parser.add_argument("--baseline-report", default=str(DEFAULT_BASELINE_REPORT_PATH), help="Frozen baseline compact report JSON path.")
    parser.add_argument("--symbol-lanes", action="append", help="Symbol-to-lanes spec like MGC=lane1,lane2")
    parser.add_argument("--shard-id", default=DEFAULT_SHARD_ID, help="Shard id, e.g. 2024Q1.")
    parser.add_argument("--start", default=DEFAULT_START, help="Shard start timestamp (ISO 8601).")
    parser.add_argument("--end", default=DEFAULT_END, help="Shard end timestamp (ISO 8601).")
    args = parser.parse_args(argv)
    payload = run_multi_symbol_warehouse_shard(
        root_dir=Path(args.root),
        sqlite_path=Path(args.sqlite),
        symbol_lane_map=_parse_symbol_lanes(args.symbol_lanes),
        shard_id=args.shard_id,
        start_ts=coerce_timestamp(args.start),
        end_ts=coerce_timestamp(args.end),
        baseline_report_path=Path(args.baseline_report),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
