"""Reusable derived timeframe materializers on top of warehouse 1m Parquet."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

from ..trend_participation.storage import materialize_parquet_dataset
from ._warehouse_common import read_stage_cache_manifest, stable_cache_key, write_stage_cache_manifest
from .layout import build_layout
from .raw_materializer import build_dataset_partition_path, coverage_range

DERIVED_PARTITION_CACHE_VERSION = "warehouse_derived_partition_v2"


def materialize_derived_timeframe_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    timeframe: str,
    raw_partition_path: Path,
    raw_version: str,
    materialized_ts: datetime | None = None,
) -> dict[str, Any]:
    root_dir = root_dir.resolve()
    symbol = symbol.upper()
    materialized_ts = materialized_ts or datetime.now(UTC)
    layout = build_layout(root_dir)
    minutes = _timeframe_minutes(timeframe)
    output_path = build_dataset_partition_path(
        dataset_root=layout[f"derived_bars_{timeframe}"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="bars.parquet",
    )
    cache_key = stable_cache_key(
        {
            "cache_version": DERIVED_PARTITION_CACHE_VERSION,
            "symbol": symbol,
            "shard_id": shard_id,
            "year": year,
            "timeframe": timeframe,
            "raw_version": raw_version,
        }
    )
    stage_started = perf_counter()
    cache_manifest = read_stage_cache_manifest(partition_path=output_path, cache_key=cache_key)
    if cache_manifest is not None:
        read_started = perf_counter()
        derived_rows = _read_parquet_rows(output_path)
        read_seconds = perf_counter() - read_started
        return {
            "dataset_name": f"derived_bars_{timeframe}",
            "symbol": symbol,
            "year": year,
            "shard_id": shard_id,
            "timeframe": timeframe,
            "partition_path": output_path,
            "row_count": len(derived_rows),
            "coverage": coverage_range(derived_rows, timestamp_key="bar_ts"),
            "provenance_tag": derived_rows[0]["provenance_tag"] if derived_rows else None,
            "rows": derived_rows,
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

    read_started = perf_counter()
    raw_rows = _read_parquet_rows(raw_partition_path)
    read_seconds = perf_counter() - read_started
    derive_started = perf_counter()
    derived_rows = _derive_timeframe_rows(
        raw_rows=raw_rows,
        timeframe=timeframe,
        minutes=minutes,
        raw_version=raw_version,
        materialized_ts=materialized_ts,
    )
    derive_seconds = perf_counter() - derive_started
    dataset_key = f"derived_bars_{timeframe}"
    write_started = perf_counter()
    materialize_parquet_dataset(output_path, derived_rows)
    write_seconds = perf_counter() - write_started
    write_stage_cache_manifest(
        partition_path=output_path,
        stage_name=dataset_key,
        cache_key=cache_key,
    )
    return {
        "dataset_name": dataset_key,
        "symbol": symbol,
        "year": year,
        "shard_id": shard_id,
        "timeframe": timeframe,
        "partition_path": output_path,
        "row_count": len(derived_rows),
        "coverage": coverage_range(derived_rows, timestamp_key="bar_ts"),
        "provenance_tag": derived_rows[0]["provenance_tag"] if derived_rows else None,
        "rows": derived_rows,
        "cache": {
            "cache_hit": False,
            "cache_key": cache_key,
        },
        "timing": {
            "cache_hit": False,
            "parquet_read_seconds": round(read_seconds, 6),
            "derive_seconds": round(derive_seconds, 6),
            "parquet_write_seconds": round(write_seconds, 6),
            "total_seconds": round(perf_counter() - stage_started, 6),
        },
    }


def _read_parquet_rows(parquet_path: Path) -> list[dict[str, Any]]:
    pyarrow = _require_pyarrow()
    table = pyarrow.parquet.ParquetFile(parquet_path).read()
    rows = table.to_pylist()
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                **row,
                "bar_ts": _normalize_timestamp(row["bar_ts"]),
                "ingest_ts": _normalize_timestamp(row["ingest_ts"]) if row.get("ingest_ts") else None,
                "coverage_window_start": _normalize_timestamp(row["coverage_window_start"]) if row.get("coverage_window_start") else None,
                "coverage_window_end": _normalize_timestamp(row["coverage_window_end"]) if row.get("coverage_window_end") else None,
            }
        )
    return normalized


def _derive_timeframe_rows(
    *,
    raw_rows: list[dict[str, Any]],
    timeframe: str,
    minutes: int,
    raw_version: str,
    materialized_ts: datetime,
) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[dict[str, Any]]] = {}
    for row in raw_rows:
        bucket_end = _bucket_end(row["bar_ts"], minutes)
        buckets.setdefault(bucket_end, []).append(row)
    derived_rows: list[dict[str, Any]] = []
    for bucket_end, bucket_rows in sorted(buckets.items()):
        bucket_rows = sorted(bucket_rows, key=lambda item: item["bar_ts"])
        if not _is_complete_bucket(bucket_rows, bucket_end=bucket_end, minutes=minutes):
            continue
        first = bucket_rows[0]
        last = bucket_rows[-1]
        derived_rows.append(
            {
                "symbol": first["symbol"],
                "timeframe": timeframe,
                "bar_ts": bucket_end,
                "open": first["open"],
                "high": max(row["high"] for row in bucket_rows),
                "low": min(row["low"] for row in bucket_rows),
                "close": last["close"],
                "volume": sum(row["volume"] for row in bucket_rows),
                "source_data_source": first["data_source"],
                "derived_rule": "complete_bucket_resample_from_canonical_1m",
                "materialized_from_raw_version": raw_version,
                "materialized_ts": materialized_ts,
                "provenance_tag": f"derived:{timeframe}:{raw_version}",
            }
        )
    return derived_rows


def _normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _bucket_end(timestamp: datetime, minutes: int) -> datetime:
    utc_ts = timestamp.astimezone(UTC)
    epoch_minutes = int(utc_ts.timestamp() // 60)
    bucket = ((epoch_minutes + minutes - 1) // minutes) * minutes
    return datetime.fromtimestamp(bucket * 60, tz=UTC)


def _is_complete_bucket(bucket_rows: list[dict[str, Any]], *, bucket_end: datetime, minutes: int) -> bool:
    if len(bucket_rows) != minutes:
        return False
    expected_start = bucket_end - timedelta(minutes=minutes - 1)
    timestamps = [row["bar_ts"].astimezone(UTC) for row in bucket_rows]
    if timestamps[0] != expected_start.astimezone(UTC):
        return False
    for previous, current in zip(timestamps, timestamps[1:], strict=False):
        if current - previous != timedelta(minutes=1):
            return False
    return True


def _timeframe_minutes(timeframe: str) -> int:
    mapping = {"5m": 5, "10m": 10}
    if timeframe not in mapping:
        raise RuntimeError(f"Unsupported derived timeframe: {timeframe}")
    return mapping[timeframe]


def _require_pyarrow():
    try:
        import pyarrow  # type: ignore
        import pyarrow.parquet  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Derived warehouse materialization requires `pyarrow`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return pyarrow
