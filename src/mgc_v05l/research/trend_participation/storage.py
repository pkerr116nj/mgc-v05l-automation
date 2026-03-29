"""Durable storage helpers for Active Trend Participation Engine research."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from ...app.session_phase_labels import label_session_phase
from ...market_data.timeframes import timeframe_minutes
from .models import DataQualityIssue, ResearchBar


def build_layout(root_dir: Path) -> dict[str, Path]:
    root = root_dir.resolve()
    layout = {
        "root": root,
        "raw": root / "raw_bars",
        "features": root / "features",
        "signals": root / "signals",
        "trades": root / "trades",
        "reports": root / "reports",
        "manifests": root / "manifests",
        "warehouse": root / "warehouse",
        "duckdb": root / "warehouse" / "trend_participation.duckdb",
        "storage_manifest": root / "manifests" / "storage_manifest.json",
    }
    for key, value in layout.items():
        if key in {"duckdb", "storage_manifest"}:
            continue
        value.mkdir(parents=True, exist_ok=True)
    return layout


def load_sqlite_bars(
    *,
    sqlite_path: Path,
    instrument: str,
    timeframe: str,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> list[ResearchBar]:
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        query = """
            select symbol, timeframe, start_ts, end_ts, open, high, low, close, volume
            from bars
            where symbol = ? and timeframe = ?
        """
        params: list[Any] = [instrument, timeframe]
        if start_ts is not None:
            query += " and end_ts >= ?"
            params.append(start_ts.isoformat())
        if end_ts is not None:
            query += " and end_ts <= ?"
            params.append(end_ts.isoformat())
        query += " order by end_ts asc"
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        ResearchBar(
            instrument=str(row["symbol"]).upper(),
            timeframe=str(row["timeframe"]).lower(),
            start_ts=_coerce_timestamp(row["start_ts"]),
            end_ts=_coerce_timestamp(row["end_ts"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row["volume"]),
            session_label=label_session_phase(_coerce_timestamp(row["end_ts"])),
            session_segment=_base_session_segment(label_session_phase(_coerce_timestamp(row["end_ts"]))),
            source="sqlite",
        )
        for row in rows
    ]


def normalize_and_check_bars(
    *,
    bars: Iterable[ResearchBar],
    timeframe: str,
) -> tuple[list[ResearchBar], list[DataQualityIssue]]:
    issues: list[DataQualityIssue] = []
    deduped: dict[tuple[str, datetime], ResearchBar] = {}
    for bar in bars:
        deduped[(bar.instrument, bar.end_ts)] = bar
    normalized = sorted(deduped.values(), key=lambda item: (item.instrument, item.end_ts))
    expected_delta = timedelta(minutes=timeframe_minutes(timeframe))
    for previous, current in zip(normalized, normalized[1:], strict=False):
        if previous.instrument != current.instrument:
            continue
        if current.end_ts <= previous.end_ts:
            issues.append(
                DataQualityIssue(
                    instrument=current.instrument,
                    timeframe=current.timeframe,
                    issue_type="non_monotonic_timestamp",
                    severity="ERROR",
                    message="Bars are not strictly increasing after normalization.",
                    bar_end_ts=current.end_ts,
                )
            )
        if current.end_ts - previous.end_ts > expected_delta:
            issues.append(
                DataQualityIssue(
                    instrument=current.instrument,
                    timeframe=current.timeframe,
                    issue_type="gap_detected",
                    severity="WARNING",
                    message=(
                        f"Gap detected between {previous.end_ts.isoformat()} and {current.end_ts.isoformat()} "
                        f"for expected delta {expected_delta}."
                    ),
                    bar_end_ts=current.end_ts,
                )
            )
    return normalized, issues


def resample_bars_from_1m(*, bars_1m: Iterable[ResearchBar], target_timeframe: str) -> list[ResearchBar]:
    target_minutes = timeframe_minutes(target_timeframe)
    if target_minutes <= 1:
        return list(sorted(bars_1m, key=lambda item: item.end_ts))
    grouped: dict[tuple[str, datetime], list[ResearchBar]] = {}
    for bar in sorted(bars_1m, key=lambda item: (item.instrument, item.end_ts)):
        bucket_end = _bucket_end(bar.end_ts, target_minutes)
        grouped.setdefault((bar.instrument, bucket_end), []).append(bar)

    resampled: list[ResearchBar] = []
    for (instrument, bucket_end), bucket in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        first = bucket[0]
        last = bucket[-1]
        resampled.append(
            ResearchBar(
                instrument=instrument,
                timeframe=target_timeframe,
                start_ts=bucket_end - timedelta(minutes=target_minutes),
                end_ts=bucket_end,
                open=first.open,
                high=max(item.high for item in bucket),
                low=min(item.low for item in bucket),
                close=last.close,
                volume=sum(item.volume for item in bucket),
                session_label=label_session_phase(bucket_end),
                session_segment=_base_session_segment(label_session_phase(bucket_end)),
                source="resampled_from_1m",
            )
        )
    return resampled


def write_storage_manifest(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_ready), encoding="utf-8")
    return path


def materialize_parquet_dataset(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    pyarrow = _require_module("pyarrow")
    table = pyarrow.table(_columnar_rows(list(rows)))
    path.parent.mkdir(parents=True, exist_ok=True)
    pyarrow.parquet.write_table(table, path)
    return path


def register_duckdb_views(*, duckdb_path: Path, parquet_map: dict[str, Path]) -> None:
    duckdb = _require_module("duckdb")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(duckdb_path))
    try:
        for view_name, parquet_path in parquet_map.items():
            escaped_path = str(parquet_path).replace("'", "''")
            connection.execute(
                f"create or replace view {view_name} as select * from read_parquet('{escaped_path}')"
            )
    finally:
        connection.close()


def serialize_rows(rows: Iterable[Any]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        payload = asdict(row) if is_dataclass(row) else dict(row)
        serialized.append({key: _json_ready(value) for key, value in payload.items()})
    return serialized


def _bucket_end(timestamp: datetime, target_minutes: int) -> datetime:
    utc_ts = timestamp.astimezone(UTC)
    epoch_minutes = int(utc_ts.timestamp() // 60)
    bucket = ((epoch_minutes + target_minutes - 1) // target_minutes) * target_minutes
    return datetime.fromtimestamp(bucket * 60, tz=UTC)


def _coerce_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _base_session_segment(label: str) -> str:
    if label.startswith("ASIA"):
        return "ASIA"
    if label.startswith("LONDON"):
        return "LONDON"
    if label.startswith("US"):
        return "US"
    return "UNKNOWN"


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _columnar_rows(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {"status": ["empty"]}
    columns = {key: [] for key in rows[0]}
    for row in rows:
        for key, value in row.items():
            columns[key].append(value)
    return columns


def _require_module(name: str):
    if name == "pyarrow":
        try:
            import pyarrow  # type: ignore
            import pyarrow.parquet  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Active Trend Participation Engine storage materialization requires `pyarrow`. "
                "Install the research extras with `pip install -e \".[research]\"`."
            ) from exc
        return pyarrow
    if name == "duckdb":
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Active Trend Participation Engine query registration requires `duckdb`. "
                "Install the research extras with `pip install -e \".[research]\"`."
            ) from exc
        return duckdb
    raise RuntimeError(f"Unsupported optional module request: {name}")
