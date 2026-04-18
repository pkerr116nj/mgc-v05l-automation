"""Project-level helpers for durable research datasets and manifests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping


def json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def stable_hash(payload: Any, *, length: int = 16) -> str:
    normalized = json.dumps(json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()[:length]


def write_json_manifest(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_ready(dict(payload)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_dataset_bundle(
    *,
    bundle_dir: Path,
    dataset_name: str,
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    prepared_rows = [dict(json_ready(dict(row))) for row in rows]
    dataset_dir = bundle_dir / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = dataset_dir / f"{dataset_name}.jsonl"
    parquet_path = dataset_dir / f"{dataset_name}.parquet"

    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in prepared_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    _write_parquet_dataset(parquet_path, prepared_rows)
    return {
        "dataset_name": dataset_name,
        "jsonl_path": str(jsonl_path.resolve()),
        "parquet_path": str(parquet_path.resolve()),
        "row_count": len(prepared_rows),
        "columns": sorted(prepared_rows[0].keys()) if prepared_rows else [],
    }


def read_jsonl_dataset(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def register_duckdb_catalog(*, duckdb_path: Path, view_to_parquet: Mapping[str, Path]) -> Path:
    duckdb = _require_module("duckdb")
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(duckdb_path))
    try:
        for view_name, parquet_path in view_to_parquet.items():
            escaped = str(parquet_path).replace("'", "''")
            connection.execute(
                f"create or replace view {view_name} as select * from read_parquet('{escaped}', union_by_name=true)"
            )
    finally:
        connection.close()
    return duckdb_path


def _write_parquet_dataset(path: Path, rows: list[dict[str, Any]]) -> None:
    pyarrow = _require_module("pyarrow")
    parquet = __import__("pyarrow.parquet", fromlist=["write_table"])
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pyarrow.table(_columnar_rows(rows))
    parquet.write_table(table, path)


def _columnar_rows(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not rows:
        return {"__empty_dataset__": []}
    columns = sorted({key for row in rows for key in row.keys()})
    return {column: [row.get(column) for row in rows] for column in columns}


def _require_module(name: str):
    module = __import__(name)
    if module is None:
        raise RuntimeError(f"Required module not available: {name}")
    return module
