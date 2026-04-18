"""DuckDB catalog registration and queryable metadata for the warehouse substrate."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def register_catalog_metadata(
    *,
    duckdb_path: Path,
    dataset_root: Path,
    generated_at: datetime,
    partition_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    compact_run_row: dict[str, Any] | None = None,
) -> None:
    duckdb = _require_duckdb()
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute("delete from dataset_partitions")
        connection.execute("delete from coverage_audit")
        if compact_run_row is not None:
            connection.execute("delete from compact_run_registry")
            connection.execute(
                """
                insert into compact_run_registry (run_id, run_ts, artifact_mode, window_start, window_end, notes)
                values (?, ?, ?, ?, ?, ?)
                """,
                [
                    compact_run_row["run_id"],
                    compact_run_row["run_ts"],
                    compact_run_row["artifact_mode"],
                    compact_run_row["window_start"],
                    compact_run_row["window_end"],
                    compact_run_row["notes"],
                ],
            )
        for row in partition_rows:
            connection.execute(
                """
                insert into dataset_partitions (
                    dataset_name, symbol, year, shard_id, timeframe, partition_path,
                    row_count, coverage_start, coverage_end, provenance_tag, registered_ts
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["dataset_name"],
                    row["symbol"],
                    row["year"],
                    row["shard_id"],
                    row["timeframe"],
                    row["partition_path"],
                    row["row_count"],
                    row["coverage_start"],
                    row["coverage_end"],
                    row["provenance_tag"],
                    row["registered_ts"],
                ],
            )
        for row in coverage_rows:
            connection.execute(
                """
                insert into coverage_audit (
                    layer, symbol, strategy_id, coverage_start, coverage_end, recorded_ts, provenance_tag
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["layer"],
                    row["symbol"],
                    row["strategy_id"],
                    row["coverage_start"],
                    row["coverage_end"],
                    row["recorded_ts"],
                    row["provenance_tag"],
                ],
            )
        refresh_query_views(connection=connection, dataset_root=dataset_root)
    finally:
        connection.close()


def refresh_query_views(*, connection, dataset_root: Path) -> None:
    _register_view(connection, "raw_bars_1m", dataset_root / "datasets" / "raw_bars_1m", dataset_name="raw_bars_1m")
    _register_view(connection, "derived_bars_5m", dataset_root / "datasets" / "derived_bars_5m", dataset_name="derived_bars_5m")
    _register_view(connection, "derived_bars_10m", dataset_root / "datasets" / "derived_bars_10m", dataset_name="derived_bars_10m")
    _register_view(
        connection,
        "shared_features_5m",
        dataset_root / "datasets" / "shared_features_5m",
        dataset_name="shared_features_5m",
    )
    _register_view(
        connection,
        "shared_features_1m_timing",
        dataset_root / "datasets" / "shared_features_1m_timing",
        dataset_name="shared_features_1m_timing",
    )
    _register_view(
        connection,
        "family_event_tables",
        dataset_root / "datasets" / "family_event_tables",
        dataset_name="family_event_tables",
    )
    _register_view(connection, "lane_candidates", dataset_root / "datasets" / "lane_candidates", dataset_name="lane_candidates")
    _register_view(connection, "lane_entries", dataset_root / "datasets" / "lane_entries", dataset_name="lane_entries")
    _register_view(
        connection,
        "lane_closed_trades",
        dataset_root / "datasets" / "lane_closed_trades",
        dataset_name="lane_closed_trades",
    )
    _register_view(
        connection,
        "lane_compact_results",
        dataset_root / "datasets" / "lane_compact_results",
        dataset_name="lane_compact_results",
    )


def build_partition_row(
    *,
    dataset_name: str,
    symbol: str | None,
    year: int | None,
    shard_id: str | None,
    timeframe: str | None,
    partition_path: Path,
    row_count: int,
    coverage: dict[str, Any],
    provenance_tag: str | None,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "timeframe": timeframe,
        "partition_path": str(partition_path),
        "row_count": row_count,
        "coverage_start": coverage["start"],
        "coverage_end": coverage["end"],
        "provenance_tag": provenance_tag,
        "registered_ts": generated_at.isoformat(),
    }


def build_coverage_row(
    *,
    layer: str,
    symbol: str | None,
    strategy_id: str | None,
    coverage: dict[str, Any],
    provenance_tag: str | None,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "layer": layer,
        "symbol": symbol,
        "strategy_id": strategy_id,
        "coverage_start": coverage["start"],
        "coverage_end": coverage["end"],
        "recorded_ts": generated_at.isoformat(),
        "provenance_tag": provenance_tag,
    }


def _register_view(connection, view_name: str, dataset_path: Path, *, dataset_name: str) -> None:
    registered_paths = [
        str(row[0])
        for row in connection.execute(
            "select partition_path from dataset_partitions where dataset_name = ? order by partition_path",
            [dataset_name],
        ).fetchall()
        if row[0]
    ]
    if registered_paths:
        escaped_paths = ", ".join("'" + path.replace("'", "''") + "'" for path in registered_paths)
        connection.execute(
            f"create or replace view {view_name} as "
            f"select * from read_parquet([{escaped_paths}], union_by_name=true, filename=true)"
        )
        return
    escaped_path = str(dataset_path / "**" / "*.parquet").replace("'", "''")
    connection.execute(
        f"create or replace view {view_name} as "
        f"select * from read_parquet('{escaped_path}', union_by_name=true, filename=true)"
    )


def _require_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Warehouse catalog registration requires `duckdb`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return duckdb
