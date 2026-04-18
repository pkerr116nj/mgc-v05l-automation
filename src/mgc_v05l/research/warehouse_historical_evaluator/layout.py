"""Filesystem layout for the warehouse historical evaluator."""

from __future__ import annotations

from pathlib import Path


def build_layout(root_dir: Path) -> dict[str, Path]:
    root = root_dir.resolve()
    layout = {
        "root": root,
        "datasets": root / "datasets",
        "raw_bars_1m": root / "datasets" / "raw_bars_1m",
        "derived_bars_5m": root / "datasets" / "derived_bars_5m",
        "derived_bars_10m": root / "datasets" / "derived_bars_10m",
        "shared_features_5m": root / "datasets" / "shared_features_5m",
        "shared_features_1m_timing": root / "datasets" / "shared_features_1m_timing",
        "family_event_tables": root / "datasets" / "family_event_tables",
        "lane_candidates": root / "datasets" / "lane_candidates",
        "lane_entries": root / "datasets" / "lane_entries",
        "lane_closed_trades": root / "datasets" / "lane_closed_trades",
        "lane_compact_results": root / "datasets" / "lane_compact_results",
        "rich_publication_artifacts": root / "datasets" / "rich_publication_artifacts",
        "catalogs": root / "catalogs",
        "schemas": root / "catalogs" / "schemas",
        "manifests": root / "manifests",
        "duckdb": root / "catalogs" / "warehouse_historical_evaluator.duckdb",
        "storage_manifest": root / "manifests" / "storage_manifest.json",
        "contracts_catalog": root / "manifests" / "contracts_catalog.json",
        "shard_stitch_contract": root / "manifests" / "shard_stitch_contract.json",
    }
    for key, value in layout.items():
        if key in {"duckdb", "storage_manifest", "contracts_catalog", "shard_stitch_contract"}:
            continue
        value.mkdir(parents=True, exist_ok=True)
    return layout
