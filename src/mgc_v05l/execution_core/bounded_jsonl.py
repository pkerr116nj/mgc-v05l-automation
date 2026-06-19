"""Bounded JSONL artifact writers for hot runtime paths."""

from __future__ import annotations

import gzip
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from mgc_v05l.execution_core.models import to_jsonable


DEFAULT_MAX_ROW_BYTES = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_ROW_BYTES", "65536"))
DEFAULT_MAX_FILE_BYTES = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_FILE_BYTES", str(8 * 1024 * 1024)))
DEFAULT_MAX_ARCHIVES = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_ARCHIVES", "5"))
DEFAULT_MAX_DEPTH = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_DEPTH", "4"))
DEFAULT_MAX_ITEMS = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_ITEMS", "32"))
DEFAULT_MAX_STRING_CHARS = int(os.environ.get("MGC_HOT_PATH_JSONL_MAX_STRING_CHARS", "2048"))


@dataclass(frozen=True)
class BoundedJsonlConfig:
    max_row_bytes: int = DEFAULT_MAX_ROW_BYTES
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    max_archives: int = DEFAULT_MAX_ARCHIVES
    max_depth: int = DEFAULT_MAX_DEPTH
    max_items: int = DEFAULT_MAX_ITEMS
    max_string_chars: int = DEFAULT_MAX_STRING_CHARS


def append_bounded_jsonl(
    path: Path,
    payload: Mapping[str, Any],
    *,
    config: BoundedJsonlConfig | None = None,
) -> Path:
    """Append one compacted JSONL row and rotate compressed logs by size."""

    actual_config = config or BoundedJsonlConfig()
    _require_jsonl_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = _bounded_json_line(payload, config=actual_config)
    _rotate_if_needed(path, incoming_bytes=len(line.encode("utf-8")), config=actual_config)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
    return path


def write_bounded_jsonl(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    config: BoundedJsonlConfig | None = None,
) -> Path:
    """Rewrite a JSONL artifact with capped rows and total file size."""

    actual_config = config or BoundedJsonlConfig()
    _require_jsonl_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    bounded_lines: list[str] = []
    total_bytes = 0
    for row in reversed([dict(item) for item in rows]):
        line = _bounded_json_line(row, config=actual_config)
        row_bytes = len(line.encode("utf-8"))
        if bounded_lines and total_bytes + row_bytes > actual_config.max_file_bytes:
            break
        if row_bytes > actual_config.max_file_bytes:
            continue
        bounded_lines.append(line)
        total_bytes += row_bytes
    bounded_lines.reverse()
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.writelines(bounded_lines)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def compact_for_jsonl(payload: Any, *, config: BoundedJsonlConfig | None = None) -> Any:
    actual_config = config or BoundedJsonlConfig()
    return _compact_value(
        to_jsonable(payload),
        depth=0,
        max_depth=actual_config.max_depth,
        max_items=actual_config.max_items,
        max_string_chars=actual_config.max_string_chars,
    )


def _bounded_json_line(payload: Mapping[str, Any], *, config: BoundedJsonlConfig) -> str:
    original = to_jsonable(dict(payload))
    line = _json_line(original)
    original_bytes = len(line.encode("utf-8"))
    if original_bytes <= config.max_row_bytes:
        return line

    compact = compact_for_jsonl(original, config=config)
    if isinstance(compact, dict):
        compact["_bounded_jsonl_truncated"] = True
        compact["_bounded_jsonl_original_bytes"] = original_bytes
    line = _json_line(compact)
    if len(line.encode("utf-8")) <= config.max_row_bytes:
        return line

    summary = _summary_record(original, original_bytes=original_bytes, config=config)
    line = _json_line(summary)
    if len(line.encode("utf-8")) <= config.max_row_bytes:
        return line

    summary["payload_excerpt"] = _truncate_text(
        json.dumps(compact, sort_keys=True, default=str),
        max(config.max_row_bytes // 2, 256),
    )
    line = _json_line(summary)
    while len(line.encode("utf-8")) > config.max_row_bytes and len(str(summary.get("payload_excerpt") or "")) > 32:
        summary["payload_excerpt"] = _truncate_text(str(summary["payload_excerpt"]), len(str(summary["payload_excerpt"])) // 2)
        line = _json_line(summary)
    return line


def _summary_record(payload: Any, *, original_bytes: int, config: BoundedJsonlConfig) -> dict[str, Any]:
    row = payload if isinstance(payload, Mapping) else {}
    keep_keys = (
        "schema_version",
        "generated_at",
        "timestamp",
        "logged_at",
        "classification",
        "status",
        "stage",
        "pass_fail",
        "lane_id",
        "strategy_id",
        "symbol",
        "local_symbol",
        "con_id",
        "order_intent_id",
        "lifecycle_id",
        "trade_id",
        "reason",
        "blocker",
        "blocker_classification",
    )
    summary = {key: _compact_value(row.get(key), depth=0, max_depth=1, max_items=8, max_string_chars=512) for key in keep_keys if key in row}
    summary.update(
        {
            "_bounded_jsonl_truncated": True,
            "_bounded_jsonl_original_bytes": original_bytes,
            "payload_excerpt": _truncate_text(json.dumps(compact_for_jsonl(payload, config=config), sort_keys=True, default=str), 4096),
        }
    )
    return summary


def _compact_value(value: Any, *, depth: int, max_depth: int, max_items: int, max_string_chars: int) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_text(value, max_string_chars)
    if depth >= max_depth:
        return _summarize_nested(value)
    if isinstance(value, Mapping):
        items = list(value.items())
        result: dict[str, Any] = {}
        for key, nested in items[:max_items]:
            result[str(key)] = _compact_value(
                nested,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
        if len(items) > max_items:
            result["_truncated_items"] = len(items) - max_items
        return result
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        result = [
            _compact_value(
                nested,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
            for nested in items[:max_items]
        ]
        if len(items) > max_items:
            result.append({"_truncated_items": len(items) - max_items})
        return result
    return _truncate_text(str(value), max_string_chars)


def _summarize_nested(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {"_truncated_mapping_keys": len(value)}
    if isinstance(value, (list, tuple, set)):
        return {"_truncated_sequence_items": len(value)}
    return _truncate_text(str(value), 256)


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    omitted = len(value) - max_chars
    return f"{value[:max_chars]}...<truncated {omitted} chars>"


def _json_line(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n"


def _rotate_if_needed(path: Path, *, incoming_bytes: int, config: BoundedJsonlConfig) -> None:
    if config.max_file_bytes <= 0 or not path.exists():
        return
    try:
        current_size = path.stat().st_size
    except OSError:
        return
    if current_size <= 0 or current_size + incoming_bytes <= config.max_file_bytes:
        return
    timestamp = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.{time.time_ns()}"
    archive = path.with_name(f"{path.stem}.{timestamp}{path.suffix}.gz")
    with path.open("rb") as source, gzip.open(archive, "wb") as target:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            target.write(chunk)
    path.unlink(missing_ok=True)
    _prune_archives(path, config=config)


def _prune_archives(path: Path, *, config: BoundedJsonlConfig) -> None:
    if config.max_archives < 0:
        return
    archives = sorted(path.parent.glob(f"{path.stem}.*{path.suffix}.gz"), key=lambda item: item.stat().st_mtime, reverse=True)
    for archive in archives[config.max_archives :]:
        archive.unlink(missing_ok=True)


def _require_jsonl_path(path: Path) -> None:
    if path.suffix != ".jsonl":
        raise ValueError(f"bounded JSONL writer only accepts .jsonl paths, got {path}")
