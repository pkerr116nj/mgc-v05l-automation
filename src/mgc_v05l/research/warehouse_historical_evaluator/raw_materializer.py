"""Reusable raw canonical 1m export into warehouse Parquet partitions."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from ..trend_participation.storage import materialize_parquet_dataset
from ._warehouse_common import (
    file_signature,
    read_parquet_rows,
    read_stage_cache_manifest,
    stable_cache_key,
    write_stage_cache_manifest,
)
from .layout import build_layout

RAW_PARTITION_CACHE_VERSION = "warehouse_raw_bars_1m_v2"


def export_canonical_1m_partition(
    *,
    root_dir: Path,
    sqlite_path: Path,
    symbol: str,
    shard_id: str,
    start_ts: datetime,
    end_ts: datetime,
    data_source: str = "historical_1m_canonical",
) -> dict[str, Any]:
    root_dir = root_dir.resolve()
    sqlite_path = sqlite_path.resolve()
    symbol = symbol.upper()
    layout = build_layout(root_dir)
    partition_path = build_dataset_partition_path(
        dataset_root=layout["raw_bars_1m"],
        symbol=symbol,
        year=start_ts.year,
        shard_id=shard_id,
        filename="bars.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": RAW_PARTITION_CACHE_VERSION,
            "sqlite_signature": file_signature(sqlite_path),
            "symbol": symbol,
            "shard_id": shard_id,
            "data_source": data_source,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=partition_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        rows = read_parquet_rows(partition_path)
        read_seconds = perf_counter() - read_started
        coverage = coverage_range(rows, timestamp_key="bar_ts")
        raw_version = raw_version_fingerprint(symbol=symbol, shard_id=shard_id, raw_rows=rows)
        return {
            "dataset_name": "raw_bars_1m",
            "symbol": symbol,
            "year": start_ts.year,
            "shard_id": shard_id,
            "timeframe": "1m",
            "partition_path": partition_path,
            "row_count": len(rows),
            "coverage": coverage,
            "provenance_tag": rows[0]["provenance_tag"] if rows else None,
            "raw_version": raw_version,
            "rows": rows,
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

    sqlite_started = perf_counter()
    rows = load_canonical_1m_rows(
        sqlite_path=sqlite_path,
        symbol=symbol,
        start_ts=start_ts,
        end_ts=end_ts,
        data_source=data_source,
    )
    sqlite_seconds = perf_counter() - sqlite_started
    if not rows:
        raise RuntimeError(f"No canonical 1m rows found for {symbol} in shard {shard_id}.")
    coverage = coverage_range(rows, timestamp_key="bar_ts")
    write_started = perf_counter()
    materialize_parquet_dataset(partition_path, rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=partition_path,
        stage_name="raw_bars_1m",
        cache_key=cache_key,
    )
    raw_version = raw_version_fingerprint(symbol=symbol, shard_id=shard_id, raw_rows=rows)
    return {
        "dataset_name": "raw_bars_1m",
        "symbol": symbol,
        "year": start_ts.year,
        "shard_id": shard_id,
        "timeframe": "1m",
        "partition_path": partition_path,
        "row_count": len(rows),
        "coverage": coverage,
        "provenance_tag": rows[0]["provenance_tag"],
        "raw_version": raw_version,
        "rows": rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "sqlite_read_seconds": round(sqlite_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def load_canonical_1m_rows(
    *,
    sqlite_path: Path,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    data_source: str,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            with ranked_provenance as (
                select
                    p.*,
                    row_number() over (
                        partition by p.bar_id
                        order by p.ingest_time desc, p.provenance_id desc
                    ) as rn
                from market_data_bar_provenance p
                where p.data_source = ?
            ),
            latest_provenance as (
                select * from ranked_provenance where rn = 1
            )
            select
                b.symbol,
                b.end_ts as bar_ts,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                coalesce(lp.provider, 'unknown') as provider,
                lp.dataset as dataset,
                lp.schema_name as schema_name,
                coalesce(lp.raw_symbol, lp.request_symbol, lp.internal_symbol, b.symbol) as instrument_identity,
                b.data_source,
                coalesce(lp.ingest_time, b.created_at) as ingest_ts,
                lp.coverage_start as coverage_window_start,
                lp.coverage_end as coverage_window_end,
                coalesce(lp.provenance_tag, b.data_source || ':' || b.symbol || ':' || b.timeframe) as provenance_tag
            from bars b
            left join latest_provenance lp on lp.bar_id = b.bar_id
            where
                b.symbol = ?
                and b.timeframe = '1m'
                and b.data_source = ?
                and b.end_ts >= ?
                and b.end_ts <= ?
            order by b.end_ts asc
            """,
            [
                data_source,
                symbol,
                data_source,
                start_ts.isoformat(),
                end_ts.isoformat(),
            ],
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            "symbol": str(row["symbol"]).upper(),
            "bar_ts": coerce_timestamp(row["bar_ts"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "provider": str(row["provider"]),
            "dataset": row["dataset"],
            "schema": row["schema_name"],
            "instrument_identity": str(row["instrument_identity"]),
            "data_source": str(row["data_source"]),
            "ingest_ts": coerce_timestamp(row["ingest_ts"]),
            "coverage_window_start": coerce_timestamp(row["coverage_window_start"]) if row["coverage_window_start"] else None,
            "coverage_window_end": coerce_timestamp(row["coverage_window_end"]) if row["coverage_window_end"] else None,
            "provenance_tag": str(row["provenance_tag"]),
        }
        for row in rows
    ]


def build_dataset_partition_path(
    *,
    dataset_root: Path,
    symbol: str,
    year: int,
    shard_id: str,
    filename: str,
) -> Path:
    return dataset_root / f"symbol={symbol}" / f"year={year}" / f"shard_id={shard_id}" / filename


def raw_version_fingerprint(*, symbol: str, shard_id: str, raw_rows: list[dict[str, Any]]) -> str:
    start = raw_rows[0]["bar_ts"].isoformat()
    end = raw_rows[-1]["bar_ts"].isoformat()
    seed = f"{symbol}|{shard_id}|{len(raw_rows)}|{start}|{end}|{raw_rows[0]['provenance_tag']}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def coverage_range(rows: list[dict[str, Any]], *, timestamp_key: str) -> dict[str, datetime | None]:
    if not rows:
        return {"start": None, "end": None}
    ordered = sorted(rows, key=lambda item: item[timestamp_key])
    return {"start": ordered[0][timestamp_key], "end": ordered[-1][timestamp_key]}


def coerce_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
