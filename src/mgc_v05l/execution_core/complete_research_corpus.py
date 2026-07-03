"""Complete historical research corpus writers.

This writer is intentionally separate from bounded_jsonl. Hot-path JSONL
artifacts should stay bounded; historical research corpora need complete,
auditable row persistence.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable


COMPLETE_CORPUS_CONTRACT = "complete_research_corpus_jsonl_v1"


def write_complete_research_jsonl(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    generated_at: datetime | str | None = None,
    corpus_name: str,
    timestamp_keys: Sequence[str] = ("observation_time", "gre_generated_at", "generated_at"),
) -> dict[str, Any]:
    """Atomically write every JSONL row and publish a manifest beside it."""

    if path.suffix != ".jsonl":
        raise ValueError("complete research corpus path must end in .jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = [to_jsonable(dict(row)) for row in rows]
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            for row in normalized_rows:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str))
                handle.write("\n")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)

    payload = path.read_bytes()
    timestamps = _extract_timestamps(normalized_rows, timestamp_keys=timestamp_keys)
    manifest = {
        "schema_version": COMPLETE_CORPUS_CONTRACT,
        "generated_at": _iso_or_now(generated_at),
        "corpus_name": corpus_name,
        "path": str(path),
        "manifest_path": str(manifest_path_for(path)),
        "row_count": len(normalized_rows),
        "file_size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "first_timestamp": min(timestamps) if timestamps else None,
        "latest_timestamp": max(timestamps) if timestamps else None,
        "timestamp_keys": list(timestamp_keys),
        "complete_corpus": True,
        "hot_path_bounded": False,
        "diagnostic_only": True,
        "broker_authority": False,
        "runtime_authority": False,
        "strategy_authority": False,
        "managed_exit_authority": False,
    }
    _write_manifest(manifest_path_for(path), manifest)
    return manifest


def manifest_path_for(path: Path) -> Path:
    return path.with_name(f"{path.name}.manifest.json")


def _extract_timestamps(rows: Sequence[Mapping[str, Any]], *, timestamp_keys: Sequence[str]) -> list[str]:
    timestamps: list[str] = []
    for row in rows:
        for key in timestamp_keys:
            value = row.get(key)
            if value:
                timestamps.append(str(value))
                break
    return timestamps


def _write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(to_jsonable(dict(manifest)), handle, indent=2, sort_keys=True)
            handle.write("\n")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _iso_or_now(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(UTC).isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
