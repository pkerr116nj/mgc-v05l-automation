"""Generic strategy scope bundles for analytics-ready trade truth."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .datasets import read_jsonl_dataset, register_duckdb_catalog, stable_hash, write_dataset_bundle, write_json_manifest

GENERIC_SCOPE_BUNDLE_ARTIFACT_VERSION = "generic_scope_bundle_v1"


@dataclass(frozen=True)
class GenericTradeScopeBundle:
    bundle_id: str
    bundle_dir: Path
    manifest_path: Path
    dataset_paths: dict[str, str]
    trade_records: list[dict[str, Any]]


def ensure_trade_scope_bundle(
    *,
    bundle_root: Path,
    strategy_family: str,
    strategy_variant: str,
    symbol: str,
    selected_sources: Mapping[str, Any],
    start_timestamp: str | None,
    end_timestamp: str | None,
    allowed_sessions: Sequence[str],
    execution_model: str,
    trade_records: Sequence[Mapping[str, Any]],
    point_value: float | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> GenericTradeScopeBundle:
    bundle_root = bundle_root.resolve()
    normalized_records = [dict(record) for record in trade_records]
    identity = {
        "artifact_version": GENERIC_SCOPE_BUNDLE_ARTIFACT_VERSION,
        "strategy_family": strategy_family,
        "strategy_variant": strategy_variant,
        "symbol": symbol,
        "selected_sources": dict(selected_sources),
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "allowed_sessions": list(allowed_sessions),
        "execution_model": execution_model,
        "point_value": point_value,
        "trade_records_hash": stable_hash(normalized_records, length=24),
        "metadata": dict(metadata or {}),
    }
    bundle_id = stable_hash(identity)
    bundle_dir = bundle_root / "scope_bundles" / bundle_id
    manifest_path = bundle_dir / "manifest.json"
    if manifest_path.exists():
        manifest = _read_manifest(manifest_path)
        trade_dataset = ((manifest.get("datasets") or {}).get("trade_records") or {})
        rows = read_jsonl_dataset(Path(str(trade_dataset.get("jsonl_path") or "")))
        return GenericTradeScopeBundle(
            bundle_id=bundle_id,
            bundle_dir=bundle_dir,
            manifest_path=manifest_path,
            dataset_paths={name: spec["parquet_path"] for name, spec in (manifest.get("datasets") or {}).items()},
            trade_records=rows,
        )

    trade_spec = write_dataset_bundle(
        bundle_dir=bundle_dir / "datasets",
        dataset_name="trade_records",
        rows=normalized_records,
    )
    duckdb_path = register_duckdb_catalog(
        duckdb_path=bundle_dir / "catalog.duckdb",
        view_to_parquet={"trade_records": Path(trade_spec["parquet_path"])},
    )
    manifest = {
        "artifact_version": GENERIC_SCOPE_BUNDLE_ARTIFACT_VERSION,
        "bundle_type": "generic_trade_scope_bundle",
        "bundle_id": bundle_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "strategy_family": strategy_family,
        "strategy_variant": strategy_variant,
        "symbol": symbol,
        "selected_sources": dict(selected_sources),
        "source_date_span": {
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
        },
        "allowed_sessions": list(allowed_sessions),
        "execution_model": execution_model,
        "point_value": point_value,
        "metadata": dict(metadata or {}),
        "datasets": {"trade_records": trade_spec},
        "duckdb_catalog_path": str(duckdb_path.resolve()),
    }
    write_json_manifest(manifest_path, manifest)
    return GenericTradeScopeBundle(
        bundle_id=bundle_id,
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        dataset_paths={"trade_records": trade_spec["parquet_path"]},
        trade_records=normalized_records,
    )


def _read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
