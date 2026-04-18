"""Bootstrap a runnable Parquet/DuckDB storage skeleton for bulk historical evaluation."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..trend_participation.storage import write_storage_manifest
from .contracts import (
    DEFAULT_DATASET_CONTRACTS,
    DEFAULT_DUCKDB_TABLE_CONTRACTS,
    DEFAULT_SHARD_STITCH_CONTRACT,
    DatasetContract,
)
from .layout import build_layout


def bootstrap_storage_skeleton(root_dir: Path) -> dict[str, Any]:
    layout = build_layout(root_dir)
    generated_at = datetime.now(UTC).isoformat()
    schema_paths: dict[str, str] = {}

    for contract in DEFAULT_DATASET_CONTRACTS:
        schema_path = layout["schemas"] / f"{contract.dataset_name}.schema.json"
        schema_path.write_text(json.dumps(contract.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        schema_paths[contract.dataset_name] = str(schema_path)
        _write_empty_parquet_schema(
            dataset_path=layout[contract.dataset_name] / "_schema.parquet",
            contract=contract,
        )

    contracts_catalog_payload = {
        "generated_at": generated_at,
        "dataset_contracts": [contract.to_dict() for contract in DEFAULT_DATASET_CONTRACTS],
        "duckdb_table_contracts": [contract.to_dict() for contract in DEFAULT_DUCKDB_TABLE_CONTRACTS],
    }
    write_storage_manifest(layout["contracts_catalog"], contracts_catalog_payload)
    write_storage_manifest(
        layout["shard_stitch_contract"],
        {
            "generated_at": generated_at,
            "contract": DEFAULT_SHARD_STITCH_CONTRACT,
        },
    )
    _bootstrap_duckdb_catalog(layout=layout, schema_paths=schema_paths, generated_at=generated_at)

    storage_manifest = {
        "generated_at": generated_at,
        "layout": {key: str(path) for key, path in layout.items()},
        "dataset_contracts": [
            {
                "dataset_name": contract.dataset_name,
                "truth_class": contract.truth_class,
                "storage_path": str(layout[contract.dataset_name]),
                "schema_path": schema_paths[contract.dataset_name],
            }
            for contract in DEFAULT_DATASET_CONTRACTS
        ],
        "duckdb_catalog": str(layout["duckdb"]),
    }
    write_storage_manifest(layout["storage_manifest"], storage_manifest)
    return {
        "root": str(layout["root"]),
        "duckdb": str(layout["duckdb"]),
        "storage_manifest": str(layout["storage_manifest"]),
        "contracts_catalog": str(layout["contracts_catalog"]),
        "shard_stitch_contract": str(layout["shard_stitch_contract"]),
        "dataset_count": len(DEFAULT_DATASET_CONTRACTS),
        "duckdb_table_count": len(DEFAULT_DUCKDB_TABLE_CONTRACTS),
    }


def _write_empty_parquet_schema(*, dataset_path: Path, contract: DatasetContract) -> None:
    pyarrow = _require_pyarrow()
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    schema = pyarrow.schema(
        [(column.name, _pyarrow_type(pyarrow, column.logical_type)) for column in contract.columns]
    )
    pyarrow.parquet.write_table(pyarrow.Table.from_pylist([], schema=schema), dataset_path)


def _bootstrap_duckdb_catalog(*, layout: dict[str, Path], schema_paths: dict[str, str], generated_at: str) -> None:
    duckdb = _require_duckdb()
    connection = duckdb.connect(str(layout["duckdb"]))
    try:
        for table_contract in DEFAULT_DUCKDB_TABLE_CONTRACTS:
            columns_sql = ", ".join(
                f"{column.name} {_duckdb_type(column.logical_type)}"
                for column in table_contract.columns
            )
            connection.execute(f"create table if not exists {table_contract.table_name} ({columns_sql})")
            connection.execute(f"delete from {table_contract.table_name}")

        for contract in DEFAULT_DATASET_CONTRACTS:
            connection.execute(
                """
                insert into dataset_contracts (dataset_name, truth_class, storage_path, schema_path, registered_ts)
                values (?, ?, ?, ?, ?)
                """,
                [
                    contract.dataset_name,
                    contract.truth_class,
                    str(layout[contract.dataset_name]),
                    schema_paths[contract.dataset_name],
                    generated_at,
                ],
            )
        connection.execute(
            """
            insert into shard_contracts (contract_id, shard_unit, warmup_policy, stitch_policy, registered_ts)
            values (?, ?, ?, ?, ?)
            """,
            [
                "default_quarter_symbol_contract",
                str(DEFAULT_SHARD_STITCH_CONTRACT["default_shard_unit"]),
                json.dumps(DEFAULT_SHARD_STITCH_CONTRACT["warmup_policy"], sort_keys=True),
                json.dumps(DEFAULT_SHARD_STITCH_CONTRACT["stitch_policy"], sort_keys=True),
                generated_at,
            ],
        )
    finally:
        connection.close()


def _pyarrow_type(pyarrow, logical_type: str):
    mapping = {
        "string": pyarrow.string(),
        "double": pyarrow.float64(),
        "bigint": pyarrow.int64(),
        "boolean": pyarrow.bool_(),
        "json": pyarrow.string(),
        "timestamp_tz": pyarrow.timestamp("us", tz="UTC"),
    }
    if logical_type not in mapping:
        raise RuntimeError(f"Unsupported pyarrow logical type: {logical_type}")
    return mapping[logical_type]


def _duckdb_type(logical_type: str) -> str:
    mapping = {
        "string": "varchar",
        "double": "double",
        "bigint": "bigint",
        "boolean": "boolean",
        "json": "varchar",
        "timestamp_tz": "timestamptz",
    }
    if logical_type not in mapping:
        raise RuntimeError(f"Unsupported DuckDB logical type: {logical_type}")
    return mapping[logical_type]


def _require_pyarrow():
    try:
        import pyarrow  # type: ignore
        import pyarrow.parquet  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Warehouse storage bootstrap requires `pyarrow`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return pyarrow


def _require_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Warehouse storage bootstrap requires `duckdb`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return duckdb


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bootstrap the warehouse historical evaluator storage skeleton.")
    parser.add_argument(
        "--root",
        default="outputs/warehouse_historical_evaluator",
        help="Root directory for the storage skeleton.",
    )
    args = parser.parse_args(argv)
    payload = bootstrap_storage_skeleton(Path(args.root))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
