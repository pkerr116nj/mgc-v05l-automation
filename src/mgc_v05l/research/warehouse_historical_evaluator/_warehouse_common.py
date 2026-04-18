"""Shared helpers for warehouse Parquet and domain-row conversions."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from ...domain.models import Bar

WAREHOUSE_STAGE_CACHE_ARTIFACT_VERSION = "warehouse_stage_cache_v1"


def read_parquet_rows(parquet_path: Path) -> list[dict[str, Any]]:
    pyarrow = require_pyarrow()
    table = pyarrow.parquet.ParquetFile(parquet_path).read()
    rows = table.to_pylist()
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if set(row.keys()) == {"status"} and row.get("status") == "empty":
            continue
        normalized_row = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                normalized_row[key] = value
            elif key.endswith("_ts") or key.endswith("_start") or key.endswith("_end") or key in {"bar_ts", "decision_ts", "timing_ts"}:
                normalized_row[key] = normalize_timestamp(value) if value is not None else None
            else:
                normalized_row[key] = value
        normalized.append(normalized_row)
    return normalized


def stable_cache_key(payload: Any, *, length: int = 24) -> str:
    encoded = json.dumps(_json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:length]


def file_signature(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def stage_manifest_path(partition_path: Path) -> Path:
    return partition_path.with_suffix(".manifest.json")


def read_stage_cache_manifest(*, partition_path: Path, cache_key: str) -> dict[str, Any] | None:
    manifest_path = stage_manifest_path(partition_path)
    if not partition_path.exists() or not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(payload.get("artifact_version") or "") != WAREHOUSE_STAGE_CACHE_ARTIFACT_VERSION:
        return None
    if str(payload.get("cache_key") or "") != cache_key:
        return None
    return payload


def write_stage_cache_manifest(
    *,
    partition_path: Path,
    stage_name: str,
    cache_key: str,
    extra: dict[str, Any] | None = None,
) -> Path:
    manifest_path = stage_manifest_path(partition_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_version": WAREHOUSE_STAGE_CACHE_ARTIFACT_VERSION,
        "stage_name": stage_name,
        "cache_key": cache_key,
        "partition_path": str(partition_path.resolve()),
        "generated_at": now_utc().isoformat(),
        "extra": _json_ready(extra or {}),
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def row_to_domain_bar(*, row: dict[str, Any], timeframe: str) -> Bar:
    end_ts = normalize_timestamp(row["bar_ts"])
    start_ts = end_ts - _timeframe_delta(timeframe)
    symbol = str(row["symbol"]).upper()
    bar_id = f"{symbol}:{timeframe}:{end_ts.isoformat()}"
    return Bar(
        bar_id=bar_id,
        symbol=symbol,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=int(row["volume"]),
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


def normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def json_range(coverage: dict[str, datetime | None]) -> dict[str, str | None]:
    return {
        "start": coverage["start"].isoformat() if coverage["start"] else None,
        "end": coverage["end"].isoformat() if coverage["end"] else None,
    }


def require_pyarrow():
    try:
        import pyarrow  # type: ignore
        import pyarrow.parquet  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Warehouse materialization requires `pyarrow`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return pyarrow


def now_utc() -> datetime:
    return datetime.now(UTC)


def _timeframe_delta(timeframe: str):
    mapping = {
        "1m": 60,
        "5m": 300,
        "10m": 600,
    }
    if timeframe not in mapping:
        raise RuntimeError(f"Unsupported timeframe delta: {timeframe}")
    from datetime import timedelta

    return timedelta(seconds=mapping[timeframe])


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
