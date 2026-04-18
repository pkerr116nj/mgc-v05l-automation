from __future__ import annotations

import json
from pathlib import Path

import duckdb

from mgc_v05l.research.warehouse_historical_evaluator import bootstrap_storage_skeleton


def test_bootstrap_storage_skeleton_creates_layout_and_catalog(tmp_path: Path) -> None:
    root = tmp_path / "warehouse_eval"
    payload = bootstrap_storage_skeleton(root)

    assert Path(payload["root"]) == root.resolve()
    assert (root / "manifests" / "storage_manifest.json").exists()
    assert (root / "manifests" / "contracts_catalog.json").exists()
    assert (root / "manifests" / "shard_stitch_contract.json").exists()
    assert (root / "catalogs" / "warehouse_historical_evaluator.duckdb").exists()

    contracts_catalog = json.loads((root / "manifests" / "contracts_catalog.json").read_text())
    dataset_names = {item["dataset_name"] for item in contracts_catalog["dataset_contracts"]}
    assert "raw_bars_1m" in dataset_names
    assert "lane_candidates" in dataset_names
    assert "lane_entries" in dataset_names
    assert "lane_closed_trades" in dataset_names
    assert "lane_compact_results" in dataset_names

    schema_path = root / "catalogs" / "schemas" / "raw_bars_1m.schema.json"
    schema_payload = json.loads(schema_path.read_text())
    assert schema_payload["dataset_name"] == "raw_bars_1m"
    assert any(column["name"] == "bar_ts" for column in schema_payload["columns"])

    schema_parquet = root / "datasets" / "raw_bars_1m" / "_schema.parquet"
    assert schema_parquet.exists()

    connection = duckdb.connect(str(root / "catalogs" / "warehouse_historical_evaluator.duckdb"))
    try:
        dataset_contract_count = connection.execute("select count(*) from dataset_contracts").fetchone()[0]
        shard_contract_count = connection.execute("select count(*) from shard_contracts").fetchone()[0]
    finally:
        connection.close()

    assert dataset_contract_count >= 10
    assert shard_contract_count == 1
