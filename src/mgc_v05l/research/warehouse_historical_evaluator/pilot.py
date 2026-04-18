"""Thin reusable proof slice for the warehouse historical evaluator substrate."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
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
DEFAULT_SYMBOL = "MGC"
DEFAULT_LANE_IDS = [
    "mgc_us_late_pause_resume_long_turn__MGC",
    "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
    "mgc_asia_early_pause_resume_short_turn__MGC",
]
DEFAULT_SHARD_ID = "2024Q1"
DEFAULT_START = "2024-01-01T00:00:00-05:00"
DEFAULT_END = "2024-03-31T23:59:00-04:00"


def run_thin_warehouse_slice(
    *,
    root_dir: Path,
    sqlite_path: Path,
    symbol: str,
    lane_ids: list[str],
    shard_id: str,
    start_ts: datetime,
    end_ts: datetime,
) -> dict[str, Any]:
    root_dir = root_dir.resolve()
    sqlite_path = sqlite_path.resolve()
    bootstrap_storage_skeleton(root_dir)
    layout = build_layout(root_dir)
    generated_at = datetime.now(UTC)
    symbol = symbol.upper()

    raw_result = export_canonical_1m_partition(
        root_dir=root_dir,
        sqlite_path=sqlite_path,
        symbol=symbol,
        shard_id=shard_id,
        start_ts=start_ts,
        end_ts=end_ts,
    )
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
    shared_features_5m_result = materialize_shared_features_5m_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        derived_5m_partition_path=Path(derived_5m_result["partition_path"]),
        derived_from_version=str(raw_result["raw_version"]),
    )
    shared_features_1m_timing_result = materialize_shared_features_1m_timing_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        raw_1m_partition_path=Path(raw_result["partition_path"]),
        raw_version=str(raw_result["raw_version"]),
    )
    family_event_result = materialize_family_event_tables_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        shared_features_5m_partition_path=Path(shared_features_5m_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
    lane_candidate_result = materialize_lane_candidates_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_ids=lane_ids,
        family_event_tables_partition_path=Path(family_event_result["partition_path"]),
        shared_features_5m_partition_path=Path(shared_features_5m_result["partition_path"]),
    )
    lane_entry_result = materialize_lane_entries_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_definitions=lane_candidate_result["lane_definitions"],
        lane_candidates_partition_path=Path(lane_candidate_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
    lane_closed_trade_result = materialize_lane_closed_trades_partition(
        root_dir=root_dir,
        symbol=symbol,
        shard_id=shard_id,
        year=start_ts.year,
        lane_definitions=lane_candidate_result["lane_definitions"],
        lane_entries_partition_path=Path(lane_entry_result["partition_path"]),
        shared_features_1m_timing_partition_path=Path(shared_features_1m_timing_result["partition_path"]),
    )
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

    partition_rows = [
        build_partition_row(
            dataset_name=str(raw_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="1m",
            partition_path=Path(raw_result["partition_path"]),
            row_count=int(raw_result["row_count"]),
            coverage=raw_result["coverage"],
            provenance_tag=raw_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(derived_5m_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="5m",
            partition_path=Path(derived_5m_result["partition_path"]),
            row_count=int(derived_5m_result["row_count"]),
            coverage=derived_5m_result["coverage"],
            provenance_tag=derived_5m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(derived_10m_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="10m",
            partition_path=Path(derived_10m_result["partition_path"]),
            row_count=int(derived_10m_result["row_count"]),
            coverage=derived_10m_result["coverage"],
            provenance_tag=derived_10m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(shared_features_5m_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="5m",
            partition_path=Path(shared_features_5m_result["partition_path"]),
            row_count=int(shared_features_5m_result["row_count"]),
            coverage=shared_features_5m_result["coverage"],
            provenance_tag=shared_features_5m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(shared_features_1m_timing_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="1m",
            partition_path=Path(shared_features_1m_timing_result["partition_path"]),
            row_count=int(shared_features_1m_timing_result["row_count"]),
            coverage=shared_features_1m_timing_result["coverage"],
            provenance_tag=shared_features_1m_timing_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(family_event_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe=None,
            partition_path=Path(family_event_result["partition_path"]),
            row_count=int(family_event_result["row_count"]),
            coverage=family_event_result["coverage"],
            provenance_tag=family_event_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(lane_candidate_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe=None,
            partition_path=Path(lane_candidate_result["partition_path"]),
            row_count=int(lane_candidate_result["row_count"]),
            coverage=lane_candidate_result["coverage"],
            provenance_tag=lane_candidate_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(lane_entry_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="1m",
            partition_path=Path(lane_entry_result["partition_path"]),
            row_count=int(lane_entry_result["row_count"]),
            coverage=lane_entry_result["coverage"],
            provenance_tag=lane_entry_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(lane_closed_trade_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe="1m",
            partition_path=Path(lane_closed_trade_result["partition_path"]),
            row_count=int(lane_closed_trade_result["row_count"]),
            coverage=lane_closed_trade_result["coverage"],
            provenance_tag=lane_closed_trade_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_partition_row(
            dataset_name=str(compact_result["dataset_name"]),
            symbol=symbol,
            year=start_ts.year,
            shard_id=shard_id,
            timeframe=None,
            partition_path=Path(compact_result["partition_path"]),
            row_count=int(compact_result["row_count"]),
            coverage=compact_result["coverage"],
            provenance_tag=compact_result["provenance_tag"],
            generated_at=generated_at,
        ),
    ]
    coverage_rows = [
        build_coverage_row(
            layer="raw_1m_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=raw_result["coverage"],
            provenance_tag=raw_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="derived_5m_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=derived_5m_result["coverage"],
            provenance_tag=derived_5m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="derived_10m_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=derived_10m_result["coverage"],
            provenance_tag=derived_10m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="shared_features_5m_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=shared_features_5m_result["coverage"],
            provenance_tag=shared_features_5m_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="shared_features_1m_timing_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=shared_features_1m_timing_result["coverage"],
            provenance_tag=shared_features_1m_timing_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="family_event_tables_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=family_event_result["coverage"],
            provenance_tag=family_event_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="lane_candidates_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=lane_candidate_result["coverage"],
            provenance_tag=lane_candidate_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="lane_entries_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=lane_entry_result["coverage"],
            provenance_tag=lane_entry_result["provenance_tag"],
            generated_at=generated_at,
        ),
        build_coverage_row(
            layer="lane_closed_trades_warehouse",
            symbol=symbol,
            strategy_id=None,
            coverage=lane_closed_trade_result["coverage"],
            provenance_tag=lane_closed_trade_result["provenance_tag"],
            generated_at=generated_at,
        ),
    ]
    compact_catalog_rows: list[dict[str, Any]] = []
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
        compact_catalog_rows.append(
            {
                "lane_id": row["lane_id"],
                "family": row["family"],
                "execution_model": row["execution_model"],
                "trade_count": row["trade_count"],
                "net_pnl": row["net_pnl"],
                "win_rate": row["win_rate"],
                "result_classification": row["result_classification"],
                "partition_path": str(compact_result["partition_path"]),
                "emitted_compact_range": {
                    "start": row["emitted_compact_start"].isoformat() if row["emitted_compact_start"] else None,
                    "end": row["emitted_compact_end"].isoformat() if row["emitted_compact_end"] else None,
                },
                "closed_trade_range": {
                    "start": row["closed_trade_start"].isoformat() if row["closed_trade_start"] else None,
                    "end": row["closed_trade_end"].isoformat() if row["closed_trade_end"] else None,
                },
            }
        )

    register_catalog_metadata(
        duckdb_path=layout["duckdb"],
        dataset_root=layout["root"],
        generated_at=generated_at,
        partition_rows=partition_rows,
        coverage_rows=coverage_rows,
        compact_run_row={
            "run_id": f"warehouse_substrate_{symbol.lower()}_{shard_id.lower()}",
            "run_ts": generated_at.isoformat(),
            "artifact_mode": "compact_only",
            "window_start": start_ts.isoformat(),
            "window_end": end_ts.isoformat(),
            "notes": f"Thin reusable warehouse substrate proof for {symbol} {shard_id}.",
        },
    )

    proof_payload = {
        "generated_at": generated_at.isoformat(),
        "symbol": symbol,
        "shard_id": shard_id,
        "lane_ids": lane_ids,
        "canonical_raw_input_range": json_range(raw_result["coverage"]),
        "derived_5m_range": json_range(derived_5m_result["coverage"]),
        "derived_10m_range": json_range(derived_10m_result["coverage"]),
        "shared_features_5m_range": json_range(shared_features_5m_result["coverage"]),
        "shared_features_1m_timing_range": json_range(shared_features_1m_timing_result["coverage"]),
        "family_event_table_range": json_range(family_event_result["coverage"]),
        "lane_candidate_range": json_range(lane_candidate_result["coverage"]),
        "lane_entry_range": json_range(lane_entry_result["coverage"]),
        "lane_closed_trade_range": json_range(lane_closed_trade_result["coverage"]),
        "compact_result_range": json_range(compact_result["coverage"]),
        "family_event_counts": summarize_family_event_counts(family_event_result["rows"]),
        "compact_rows": compact_catalog_rows,
        "warehouse_paths": {
            "raw_bars_1m": str(raw_result["partition_path"]),
            "derived_bars_5m": str(derived_5m_result["partition_path"]),
            "derived_bars_10m": str(derived_10m_result["partition_path"]),
            "shared_features_5m": str(shared_features_5m_result["partition_path"]),
            "shared_features_1m_timing": str(shared_features_1m_timing_result["partition_path"]),
            "family_event_tables": str(family_event_result["partition_path"]),
            "lane_candidates": str(lane_candidate_result["partition_path"]),
            "lane_entries": str(lane_entry_result["partition_path"]),
            "lane_closed_trades": str(lane_closed_trade_result["partition_path"]),
            "lane_compact_results": str(compact_result["partition_path"]),
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
        ],
        "dataset_partition_count": len(partition_rows),
    }
    proof_path = layout["manifests"] / "substrate_proof.json"
    write_storage_manifest(proof_path, proof_payload)
    proof_markdown_path = layout["manifests"] / "substrate_proof.md"
    proof_markdown_path.write_text(build_proof_markdown(proof_payload), encoding="utf-8")
    return {
        "proof_path": str(proof_path),
        "proof_markdown_path": str(proof_markdown_path),
        "duckdb_path": str(layout["duckdb"]),
        "raw_partition_path": str(raw_result["partition_path"]),
        "derived_5m_path": str(derived_5m_result["partition_path"]),
        "derived_10m_path": str(derived_10m_result["partition_path"]),
        "shared_features_5m_path": str(shared_features_5m_result["partition_path"]),
        "shared_features_1m_timing_path": str(shared_features_1m_timing_result["partition_path"]),
        "family_event_tables_path": str(family_event_result["partition_path"]),
        "lane_candidates_path": str(lane_candidate_result["partition_path"]),
        "lane_entries_path": str(lane_entry_result["partition_path"]),
        "lane_closed_trades_path": str(lane_closed_trade_result["partition_path"]),
        "compact_partition_paths": [str(compact_result["partition_path"])],
        "symbol": symbol,
        "shard_id": shard_id,
        "lane_ids": lane_ids,
    }


def json_range(coverage: dict[str, Any]) -> dict[str, str | None]:
    return {
        "start": coverage["start"].isoformat() if coverage["start"] else None,
        "end": coverage["end"].isoformat() if coverage["end"] else None,
    }


def build_proof_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Warehouse Substrate Proof",
        "",
        f"- Symbol: `{payload['symbol']}`",
        f"- Shard: `{payload['shard_id']}`",
        f"- Canonical raw range: `{payload['canonical_raw_input_range']['start']}` -> `{payload['canonical_raw_input_range']['end']}`",
        f"- Derived 5m range: `{payload['derived_5m_range']['start']}` -> `{payload['derived_5m_range']['end']}`",
        f"- Derived 10m range: `{payload['derived_10m_range']['start']}` -> `{payload['derived_10m_range']['end']}`",
        f"- Shared features 5m range: `{payload['shared_features_5m_range']['start']}` -> `{payload['shared_features_5m_range']['end']}`",
        f"- Shared features 1m timing range: `{payload['shared_features_1m_timing_range']['start']}` -> `{payload['shared_features_1m_timing_range']['end']}`",
        f"- Family event table range: `{payload['family_event_table_range']['start']}` -> `{payload['family_event_table_range']['end']}`",
        f"- Lane candidate range: `{payload['lane_candidate_range']['start']}` -> `{payload['lane_candidate_range']['end']}`",
        f"- Lane entry range: `{payload['lane_entry_range']['start']}` -> `{payload['lane_entry_range']['end']}`",
        f"- Lane closed-trade range: `{payload['lane_closed_trade_range']['start']}` -> `{payload['lane_closed_trade_range']['end']}`",
        f"- Compact result range: `{payload['compact_result_range']['start']}` -> `{payload['compact_result_range']['end']}`",
        f"- Lane candidate table path: `{payload['warehouse_paths']['lane_candidates']}`",
        f"- Lane entry table path: `{payload['warehouse_paths']['lane_entries']}`",
        f"- Lane closed-trade table path: `{payload['warehouse_paths']['lane_closed_trades']}`",
        f"- Dataset partitions registered: `{payload['dataset_partition_count']}`",
        "",
        "## Family Event Counts",
        "",
    ]
    for family, count in sorted(payload["family_event_counts"].items()):
        lines.append(f"- `{family}` -> `{count}`")
    lines.extend(["", "## Compact Rows", ""])
    for row in payload["compact_rows"]:
        lines.extend(
            [
                f"- `{row['lane_id']}` family=`{row['family']}`",
                f"  execution_model=`{row['execution_model']}` trade_count=`{row['trade_count']}` "
                f"net_pnl=`{row['net_pnl']}` win_rate=`{row['win_rate']}` "
                f"classification=`{row['result_classification']}`",
            ]
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a thin reusable warehouse substrate proof slice.")
    parser.add_argument("--root", default="outputs/warehouse_historical_evaluator", help="Warehouse root directory.")
    parser.add_argument("--sqlite", default=str(DEFAULT_SQLITE_PATH), help="Canonical replay SQLite path.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Pilot symbol.")
    parser.add_argument("--lane-id", dest="lane_ids", action="append", help="Lane id to evaluate in the warehouse proof.")
    parser.add_argument("--shard-id", default=DEFAULT_SHARD_ID, help="Shard id, e.g. 2024Q1.")
    parser.add_argument("--start", default=DEFAULT_START, help="Shard start timestamp (ISO 8601).")
    parser.add_argument("--end", default=DEFAULT_END, help="Shard end timestamp (ISO 8601).")
    args = parser.parse_args(argv)
    payload = run_thin_warehouse_slice(
        root_dir=Path(args.root),
        sqlite_path=Path(args.sqlite),
        symbol=args.symbol,
        lane_ids=args.lane_ids or list(DEFAULT_LANE_IDS),
        shard_id=args.shard_id,
        start_ts=coerce_timestamp(args.start),
        end_ts=coerce_timestamp(args.end),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
