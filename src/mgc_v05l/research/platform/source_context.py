"""Project-level source discovery and symbol-context materialization."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Sequence

from ...market_data.provider_config import load_market_data_providers_config
from ...market_data.provider_models import MarketDataUseCase
from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import (
    load_sqlite_bars,
    normalize_and_check_bars,
    resample_bars_from_1m,
    rolling_window_bars_from_1m,
)
from .datasets import read_jsonl_dataset, register_duckdb_catalog, stable_hash, write_dataset_bundle, write_json_manifest

_SYMBOL_CONTEXT_CACHE: dict[tuple[Any, ...], dict[str, Any] | None] = {}
_LAST_SOURCE_DISCOVERY_METADATA: dict[str, Any] = {}
SOURCE_INVENTORY_ARTIFACT_VERSION = "source_inventory_v1"


@dataclass(frozen=True)
class SourceSelection:
    symbol: str
    timeframe: str
    data_source: str
    sqlite_path: Path
    row_count: int
    start_ts: str | None
    end_ts: str | None


@dataclass(frozen=True)
class SourceInventoryConfig:
    inventory_enabled: bool = True
    provider_use_case: str = "historical_research"


def discover_best_sources(
    *,
    symbols: set[str],
    timeframes: set[str],
    sqlite_paths: Sequence[str | Path] | None = None,
) -> dict[str, dict[str, SourceSelection]]:
    started = perf_counter()
    selections: dict[str, dict[str, SourceSelection]] = {}
    provider_config = load_market_data_providers_config()
    candidate_paths = (
        [Path(path).resolve() for path in sqlite_paths]
        if sqlite_paths is not None
        else sorted(Path.cwd().glob("*.sqlite3"))
    )
    inventory_result = ensure_source_inventory(
        inventory_root=_default_source_inventory_root(),
        sqlite_paths=candidate_paths,
        symbols=symbols,
        timeframes=timeframes,
    )
    filtered_rows_started = perf_counter()
    for row in inventory_result["rows"]:
        sqlite_path = Path(str(row.get("sqlite_path") or ""))
        if not sqlite_path.exists():
            continue
        symbol = row.get("symbol")
        timeframe = row.get("timeframe")
        data_source = row.get("data_source")
        row_count = row.get("row_count")
        start_ts = row.get("start_ts")
        end_ts = row.get("end_ts")
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_timeframe = str(timeframe or "").strip().lower()
        normalized_data_source = str(data_source or "").strip()
        if normalized_symbol not in symbols or normalized_timeframe not in timeframes:
            continue
        candidate = SourceSelection(
            symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            data_source=normalized_data_source,
            sqlite_path=sqlite_path.resolve(),
            row_count=int(row_count or 0),
            start_ts=str(start_ts) if start_ts else None,
            end_ts=str(end_ts) if end_ts else None,
        )
        current = selections.get(normalized_symbol, {}).get(normalized_timeframe)
        if current is None or _source_selection_key(candidate, provider_config) > _source_selection_key(current, provider_config):
            selections.setdefault(normalized_symbol, {})[normalized_timeframe] = candidate
    filtered_rows_seconds = perf_counter() - filtered_rows_started
    _LAST_SOURCE_DISCOVERY_METADATA.clear()
    _LAST_SOURCE_DISCOVERY_METADATA.update(
        {
            "artifact_version": SOURCE_INVENTORY_ARTIFACT_VERSION,
            "generated_at": datetime.now().astimezone().isoformat(),
            "inventory_root": inventory_result["inventory_root"],
            "inventory_scope": inventory_result.get("inventory_scope"),
            "inventory_manifest_path": inventory_result["manifest_path"],
            "inventory_timing": dict(inventory_result["timing"]),
            "candidate_path_count": len(candidate_paths),
            "inventory_row_count": len(inventory_result["rows"]),
            "filtered_row_seconds": round(filtered_rows_seconds, 6),
            "selection_count": sum(len(by_timeframe) for by_timeframe in selections.values()),
            "total_seconds": round(perf_counter() - started, 6),
        }
    )
    return {symbol: dict(by_timeframe) for symbol, by_timeframe in selections.items()}


def ensure_source_inventory(
    *,
    inventory_root: Path,
    sqlite_paths: Sequence[str | Path],
    symbols: set[str] | None = None,
    timeframes: set[str] | None = None,
) -> dict[str, Any]:
    started = perf_counter()
    inventory_root = inventory_root.resolve()
    inventory_root.mkdir(parents=True, exist_ok=True)
    inventory_scope = {
        "symbols": sorted(str(symbol).strip().upper() for symbol in (symbols or set()) if str(symbol).strip()),
        "timeframes": sorted(str(timeframe).strip().lower() for timeframe in (timeframes or set()) if str(timeframe).strip()),
    }
    scoped_root = _inventory_scope_root(inventory_root=inventory_root, inventory_scope=inventory_scope)
    scoped_root.mkdir(parents=True, exist_ok=True)
    candidate_paths = [Path(path).resolve() for path in sqlite_paths if Path(path).exists()]
    signatures = [_sqlite_signature(path) for path in candidate_paths]
    signature_keys = {
        (str(signature["sqlite_path"]), int(signature["file_size_bytes"]), int(signature["file_mtime_ns"]))
        for signature in signatures
    }
    rows_jsonl = scoped_root / "source_inventory_rows.jsonl"
    manifest_path = scoped_root / "manifest.json"
    previous_rows = read_jsonl_dataset(rows_jsonl)
    rows_by_signature: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for row in previous_rows:
        key = (
            str(row.get("sqlite_path") or ""),
            int(row.get("file_size_bytes") or 0),
            int(row.get("file_mtime_ns") or 0),
        )
        rows_by_signature.setdefault(key, []).append(row)

    previous_manifest = _read_manifest(manifest_path) if manifest_path.exists() else None
    if _inventory_manifest_matches(previous_manifest, signatures) and rows_jsonl.exists():
        return {
            "inventory_root": str(scoped_root),
            "inventory_scope": inventory_scope,
            "manifest_path": str(manifest_path.resolve()),
            "rows": previous_rows,
            "timing": {
                "scanned_files": 0,
                "reused_files": len(signatures),
                "scan_seconds": 0.0,
                "dataset_bundle_seconds": 0.0,
                "duckdb_seconds": 0.0,
                "total_seconds": round(perf_counter() - started, 6),
                "inventory_cache_hit": True,
            },
        }

    inventory_rows: list[dict[str, Any]] = []
    scanned_files = 0
    reused_files = 0
    scan_seconds = 0.0
    for signature in signatures:
        key = (
            str(signature["sqlite_path"]),
            int(signature["file_size_bytes"]),
            int(signature["file_mtime_ns"]),
        )
        cached_rows = rows_by_signature.get(key)
        if cached_rows:
            reused_files += 1
            inventory_rows.extend(cached_rows)
            continue
        scanned_files += 1
        scan_started = perf_counter()
        inventory_rows.extend(
            _scan_source_inventory_rows(
                signature,
                symbols=inventory_scope["symbols"],
                timeframes=inventory_scope["timeframes"],
            )
        )
        scan_seconds += perf_counter() - scan_started

    inventory_rows = [
        row
        for row in inventory_rows
        if (
            str(row.get("sqlite_path") or ""),
            int(row.get("file_size_bytes") or 0),
            int(row.get("file_mtime_ns") or 0),
        ) in signature_keys
    ]
    inventory_rows.sort(
        key=lambda row: (
            str(row.get("sqlite_path") or ""),
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("data_source") or ""),
        )
    )
    rows_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with rows_jsonl.open("w", encoding="utf-8") as handle:
        for row in inventory_rows:
            handle.write(f"{json.dumps(row, sort_keys=True)}\n")

    dataset_started = perf_counter()
    dataset_spec = write_dataset_bundle(
        bundle_dir=scoped_root / "datasets",
        dataset_name="source_inventory_rows",
        rows=inventory_rows,
    )
    dataset_seconds = perf_counter() - dataset_started
    duckdb_started = perf_counter()
    duckdb_path = register_duckdb_catalog(
        duckdb_path=scoped_root / "catalog.duckdb",
        view_to_parquet={"source_inventory_rows": Path(dataset_spec["parquet_path"])},
    )
    duckdb_seconds = perf_counter() - duckdb_started
    inventory_config = SourceInventoryConfig()
    manifest = {
        "artifact_version": SOURCE_INVENTORY_ARTIFACT_VERSION,
        "generated_at": datetime.now().astimezone().isoformat(),
        "inventory_config": {
            "artifact_version": SOURCE_INVENTORY_ARTIFACT_VERSION,
            "config": {
                "inventory_enabled": inventory_config.inventory_enabled,
                "provider_use_case": inventory_config.provider_use_case,
            },
            "config_hash": stable_hash(
                {
                    "inventory_enabled": inventory_config.inventory_enabled,
                    "provider_use_case": inventory_config.provider_use_case,
                },
                length=24,
            ),
        },
        "candidate_paths": [str(path) for path in candidate_paths],
        "inventory_scope": inventory_scope,
        "file_signatures": signatures,
        "datasets": {"source_inventory_rows": dataset_spec},
        "duckdb_catalog_path": str(duckdb_path.resolve()),
        "timing": {
            "scanned_files": scanned_files,
            "reused_files": reused_files,
            "scan_seconds": round(scan_seconds, 6),
            "dataset_bundle_seconds": round(dataset_seconds, 6),
            "duckdb_seconds": round(duckdb_seconds, 6),
            "total_seconds": round(perf_counter() - started, 6),
            "inventory_cache_hit": False,
        },
    }
    manifest_path = write_json_manifest(manifest_path, manifest)
    return {
        "inventory_root": str(scoped_root),
        "inventory_scope": inventory_scope,
        "manifest_path": str(manifest_path.resolve()),
        "rows": inventory_rows,
        "timing": dict(manifest["timing"]),
    }


def last_source_discovery_metadata() -> dict[str, Any]:
    return dict(_LAST_SOURCE_DISCOVERY_METADATA)


def _default_source_inventory_root() -> Path:
    return (Path.cwd() / "outputs" / "research_platform" / "source_context" / "inventory").resolve()


def _sqlite_signature(sqlite_path: Path) -> dict[str, Any]:
    stat = sqlite_path.stat()
    return {
        "sqlite_path": str(sqlite_path.resolve()),
        "file_size_bytes": int(stat.st_size),
        "file_mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
    }


def _scan_source_inventory_rows(
    signature: Mapping[str, Any],
    *,
    symbols: Sequence[str] | None = None,
    timeframes: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    sqlite_path = Path(str(signature["sqlite_path"]))
    try:
        connection = sqlite3.connect(sqlite_path)
        query = [
            "select symbol, timeframe, data_source, count(*) as row_count, min(end_ts) as start_ts, max(end_ts) as end_ts",
            "from bars",
        ]
        conditions: list[str] = []
        parameters: list[str] = []
        if symbols:
            placeholders = ", ".join("?" for _ in symbols)
            conditions.append(f"upper(symbol) in ({placeholders})")
            parameters.extend(str(symbol).strip().upper() for symbol in symbols)
        if timeframes:
            placeholders = ", ".join("?" for _ in timeframes)
            conditions.append(f"lower(timeframe) in ({placeholders})")
            parameters.extend(str(timeframe).strip().lower() for timeframe in timeframes)
        if conditions:
            query.append("where " + " and ".join(conditions))
        query.append("group by symbol, timeframe, data_source")
        rows = connection.execute("\n".join(query), parameters).fetchall()
        connection.close()
    except sqlite3.Error:
        return []
    refreshed_at = datetime.now().astimezone().isoformat()
    return [
        {
            "sqlite_path": str(sqlite_path.resolve()),
            "file_size_bytes": int(signature["file_size_bytes"]),
            "file_mtime_ns": int(signature["file_mtime_ns"]),
            "symbol": str(symbol or "").strip().upper(),
            "timeframe": str(timeframe or "").strip().lower(),
            "data_source": str(data_source or "").strip(),
            "row_count": int(row_count or 0),
            "start_ts": str(start_ts) if start_ts else None,
            "end_ts": str(end_ts) if end_ts else None,
            "refreshed_at": refreshed_at,
        }
        for symbol, timeframe, data_source, row_count, start_ts, end_ts in rows
    ]


def _inventory_scope_root(*, inventory_root: Path, inventory_scope: Mapping[str, Sequence[str]]) -> Path:
    symbols = list(inventory_scope.get("symbols") or [])
    timeframes = list(inventory_scope.get("timeframes") or [])
    if not symbols and not timeframes:
        return inventory_root
    scope_hash = stable_hash(
        {
            "symbols": symbols,
            "timeframes": timeframes,
        },
        length=24,
    )
    return inventory_root / "slices" / scope_hash


def _inventory_manifest_matches(manifest: Mapping[str, Any] | None, signatures: Sequence[Mapping[str, Any]]) -> bool:
    if not isinstance(manifest, Mapping):
        return False
    existing = manifest.get("file_signatures")
    if not isinstance(existing, Sequence):
        return False
    return _normalized_signature_rows(existing) == _normalized_signature_rows(signatures)


def _normalized_signature_rows(rows: Sequence[Mapping[str, Any]]) -> list[tuple[str, int, int]]:
    normalized: list[tuple[str, int, int]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            (
                str(row.get("sqlite_path") or ""),
                int(row.get("file_size_bytes") or 0),
                int(row.get("file_mtime_ns") or 0),
            )
        )
    normalized.sort()
    return normalized


def ensure_symbol_context_bundle(
    *,
    bundle_root: Path,
    symbol: str,
    bar_source_index: Mapping[str, Mapping[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return None
    identity = {
        "bundle_type": "symbol_context",
        "symbol": symbol,
        "minute_source": _selection_payload(minute_source),
        "completed_source": _selection_payload(completed_source) if completed_source is not None else None,
        "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
        "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
    }
    bundle_id = stable_hash(identity)
    bundle_dir = bundle_root.resolve() / "context_bundles" / bundle_id
    manifest_path = bundle_dir / "manifest.json"
    if manifest_path.exists():
        manifest = _read_manifest(manifest_path)
        return _load_context_bundle(manifest)

    payload = load_symbol_context(
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if payload is None:
        return None

    datasets_dir = bundle_dir / "datasets"
    specs = {
        "bars_1m": write_dataset_bundle(
            bundle_dir=datasets_dir,
            dataset_name="bars_1m",
            rows=[_research_bar_row(bar) for bar in payload["bars_1m"]],
        ),
        "completed_5m_history": write_dataset_bundle(
            bundle_dir=datasets_dir,
            dataset_name="completed_5m_history",
            rows=[_research_bar_row(bar) for bar in payload["completed_5m_history"]],
        ),
        "rolling_5m": write_dataset_bundle(
            bundle_dir=datasets_dir,
            dataset_name="rolling_5m",
            rows=[_research_bar_row(bar) for bar in payload["rolling_5m"]],
        ),
        "combined_rolling_5m": write_dataset_bundle(
            bundle_dir=datasets_dir,
            dataset_name="combined_rolling_5m",
            rows=[_research_bar_row(bar) for bar in payload["combined_rolling_5m"]],
        ),
        "window_completed_5m": write_dataset_bundle(
            bundle_dir=datasets_dir,
            dataset_name="window_completed_5m",
            rows=[_research_bar_row(bar) for bar in payload["window_completed_5m"]],
        ),
    }
    duckdb_path = register_duckdb_catalog(
        duckdb_path=bundle_dir / "catalog.duckdb",
        view_to_parquet={name: Path(spec["parquet_path"]) for name, spec in specs.items()},
    )
    manifest = {
        "bundle_type": "symbol_context",
        "bundle_id": bundle_id,
        "generated_at": datetime.now().astimezone().isoformat(),
        "symbol": symbol,
        "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
        "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
        "minute_source": _selection_payload(minute_source),
        "completed_source": _selection_payload(completed_source) if completed_source is not None else None,
        "datasets": specs,
        "duckdb_catalog_path": str(duckdb_path.resolve()),
    }
    write_json_manifest(manifest_path, manifest)
    loaded = _load_context_bundle(manifest)
    loaded["context_bundle_id"] = bundle_id
    return loaded


def load_symbol_context(
    *,
    symbol: str,
    bar_source_index: Mapping[str, Mapping[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return None
    cache_key = (
        symbol,
        str(minute_source.sqlite_path),
        minute_source.data_source,
        str(completed_source.sqlite_path) if completed_source is not None else None,
        completed_source.data_source if completed_source is not None else None,
        start_timestamp.isoformat() if start_timestamp is not None else None,
        end_timestamp.isoformat() if end_timestamp is not None else None,
    )
    cached = _SYMBOL_CONTEXT_CACHE.get(cache_key)
    if cached is not None or cache_key in _SYMBOL_CONTEXT_CACHE:
        return cached
    raw_bars_1m, _ = normalize_and_check_bars(
        bars=load_sqlite_bars(
            sqlite_path=minute_source.sqlite_path,
            instrument=symbol,
            timeframe="1m",
            data_source=minute_source.data_source,
            start_ts=start_timestamp,
            end_ts=end_timestamp,
        ),
        timeframe="1m",
    )
    bars_1m = _clip_research_bars_to_exact_window(
        raw_bars_1m,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    native_completed_5m_history: list[ResearchBar] = []
    if completed_source is not None:
        native_completed_5m_history, _ = normalize_and_check_bars(
            bars=load_sqlite_bars(
                sqlite_path=completed_source.sqlite_path,
                instrument=symbol,
                timeframe="5m",
                data_source=completed_source.data_source,
                start_ts=start_timestamp,
                end_ts=end_timestamp,
            ),
            timeframe="5m",
        )
        native_completed_5m_history = _clip_research_bars_to_exact_window(
            native_completed_5m_history,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    derived_completed_5m_history = resample_bars_from_1m(bars_1m=bars_1m, target_timeframe="5m")
    completed_5m_history = (
        derived_completed_5m_history
        if len(derived_completed_5m_history) >= len(native_completed_5m_history)
        else native_completed_5m_history
    )
    rolling_5m = rolling_window_bars_from_1m(bars_1m=bars_1m)
    if not bars_1m or not completed_5m_history or not rolling_5m:
        _SYMBOL_CONTEXT_CACHE[cache_key] = None
        return None
    first_rolling_ts = rolling_5m[0].end_ts
    last_minute_ts = bars_1m[-1].end_ts
    payload = {
        "bars_1m": bars_1m,
        "completed_5m_history": completed_5m_history,
        "rolling_5m": rolling_5m,
        "combined_rolling_5m": [bar for bar in completed_5m_history if bar.end_ts < first_rolling_ts] + rolling_5m,
        "window_completed_5m": [bar for bar in completed_5m_history if bars_1m[0].end_ts <= bar.end_ts <= last_minute_ts],
    }
    _SYMBOL_CONTEXT_CACHE[cache_key] = payload
    return payload


def _load_context_bundle(manifest: dict[str, Any]) -> dict[str, Any]:
    datasets = manifest["datasets"]
    return {
        "bars_1m": [_research_bar_from_row(row) for row in read_jsonl_dataset(Path(datasets["bars_1m"]["jsonl_path"]))],
        "completed_5m_history": [_research_bar_from_row(row) for row in read_jsonl_dataset(Path(datasets["completed_5m_history"]["jsonl_path"]))],
        "rolling_5m": [_research_bar_from_row(row) for row in read_jsonl_dataset(Path(datasets["rolling_5m"]["jsonl_path"]))],
        "combined_rolling_5m": [_research_bar_from_row(row) for row in read_jsonl_dataset(Path(datasets["combined_rolling_5m"]["jsonl_path"]))],
        "window_completed_5m": [_research_bar_from_row(row) for row in read_jsonl_dataset(Path(datasets["window_completed_5m"]["jsonl_path"]))],
        "context_bundle_id": manifest["bundle_id"],
        "selected_sources": {
            "1m": manifest["minute_source"],
            "5m": manifest.get("completed_source"),
        },
    }


def _source_selection_key(selection: SourceSelection, provider_config) -> tuple[int, int, str, str]:
    preferred_sources = list(
        provider_config.preferred_data_sources(MarketDataUseCase.HISTORICAL_RESEARCH, selection.timeframe)
    )
    try:
        precedence = len(preferred_sources) - preferred_sources.index(selection.data_source)
    except ValueError:
        precedence = 0
    return (precedence, selection.row_count, selection.end_ts or "", str(selection.sqlite_path))


def _clip_research_bars_to_exact_window(
    bars: Sequence[ResearchBar],
    *,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> list[ResearchBar]:
    clipped = list(bars)
    if start_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts >= start_timestamp]
    if end_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts <= end_timestamp]
    return clipped


def _selection_payload(selection: SourceSelection | None) -> dict[str, Any] | None:
    if selection is None:
        return None
    return {
        "symbol": selection.symbol,
        "timeframe": selection.timeframe,
        "data_source": selection.data_source,
        "sqlite_path": str(selection.sqlite_path),
        "row_count": selection.row_count,
        "start_ts": selection.start_ts,
        "end_ts": selection.end_ts,
    }


def _research_bar_row(bar: ResearchBar) -> dict[str, Any]:
    return {
        "instrument": bar.instrument,
        "timeframe": bar.timeframe,
        "start_ts": bar.start_ts.isoformat(),
        "end_ts": bar.end_ts.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "session_label": bar.session_label,
        "session_segment": bar.session_segment,
        "source": bar.source,
        "provenance": bar.provenance,
        "trading_calendar": bar.trading_calendar,
    }


def _research_bar_from_row(row: dict[str, Any]) -> ResearchBar:
    payload = dict(row)
    payload["start_ts"] = datetime.fromisoformat(str(payload["start_ts"]))
    payload["end_ts"] = datetime.fromisoformat(str(payload["end_ts"]))
    return ResearchBar(**payload)


def _read_manifest(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))
