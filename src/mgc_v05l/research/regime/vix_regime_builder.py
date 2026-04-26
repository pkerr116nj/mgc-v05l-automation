"""Build a daily VIX regime table from raw Cboe VIX history."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from ..trend_participation.storage import materialize_parquet_dataset, write_storage_manifest
from ..warehouse_historical_evaluator.catalog import refresh_query_views
from ..warehouse_historical_evaluator.layout import build_layout


def load_vix_bucket_config(config_path: Path) -> dict[str, Any]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return dict(payload["vix_daily"])


def build_vol_regime_daily(
    *,
    warehouse_root: Path,
    bucket_config_path: Path,
) -> dict[str, Any]:
    warehouse_root = warehouse_root.resolve()
    bucket_config_path = bucket_config_path.resolve()
    layout = build_layout(warehouse_root)
    config = load_vix_bucket_config(bucket_config_path)
    loaded_at = datetime.now(UTC).isoformat()

    raw_rows = load_vix_daily_rows(warehouse_root)
    ordered_rows = sorted(raw_rows, key=lambda row: str(row["vix_trade_date"]))

    regime_rows: list[dict[str, Any]] = []
    prior_close: float | None = None
    for row in ordered_rows:
        close = float(row["vix_close"])
        change_abs = close - prior_close if prior_close is not None else None
        change_pct = (change_abs / prior_close) if prior_close not in {None, 0.0} else None
        level_bucket = _bucket_label(close, config["level_buckets"])
        change_bucket = _bucket_label(change_pct, config["change_pct_buckets"]) if change_pct is not None else "UNKNOWN"
        regime_rows.append(
            {
                "vix_trade_date": row["vix_trade_date"],
                "vix_asof_ts": _coerce_ts(row["vix_asof_ts"]).astimezone(UTC).isoformat(),
                "vix_close": close,
                "vix_change_abs": None if change_abs is None else round(change_abs, 6),
                "vix_change_pct": None if change_pct is None else round(change_pct, 6),
                "vix_level_bucket": level_bucket,
                "vix_change_bucket": change_bucket,
                "vix_combined_bucket": f"{level_bucket}_{change_bucket}",
            }
        )
        prior_close = close

    artifact_path = layout["vol_regime_daily"] / "vol_regime_daily.parquet"
    materialize_parquet_dataset(artifact_path, regime_rows)
    manifest_path = layout["manifests"] / "vol_regime_daily_manifest.json"
    write_storage_manifest(
        manifest_path,
        {
            "module": "vix_regime_build",
            "warehouse_root": str(warehouse_root),
            "bucket_config_path": str(bucket_config_path),
            "bucket_config_version": config.get("version"),
            "row_count": len(regime_rows),
            "artifact_path": str(artifact_path),
            "generated_at": loaded_at,
        },
    )
    _refresh_duckdb_views_if_available(layout)
    return {
        "artifact_path": str(artifact_path),
        "manifest_path": str(manifest_path),
        "row_count": len(regime_rows),
        "bucket_config_version": config.get("version"),
    }


def load_vix_daily_rows(warehouse_root: Path) -> list[dict[str, Any]]:
    dataset_root = build_layout(warehouse_root.resolve())["vix_daily"]
    rows = _read_parquet_rows(dataset_root)
    rows.sort(key=lambda row: str(row["vix_trade_date"]))
    return rows


def load_vol_regime_rows(warehouse_root: Path) -> list[dict[str, Any]]:
    dataset_root = build_layout(warehouse_root.resolve())["vol_regime_daily"]
    rows = _read_parquet_rows(dataset_root)
    rows.sort(key=lambda row: _coerce_ts(row["vix_asof_ts"]))
    return rows


def _read_parquet_rows(dataset_root: Path) -> list[dict[str, Any]]:
    parquet_files = sorted(
        path
        for path in dataset_root.rglob("*.parquet")
        if path.name != "_schema.parquet"
    )
    if not parquet_files:
        return []
    pyarrow_dataset = _require_pyarrow_dataset()
    dataset = pyarrow_dataset.dataset([str(path) for path in parquet_files], format="parquet")
    return list(dataset.to_table().to_pylist())


def _bucket_label(value: float | None, bucket_specs: Sequence[dict[str, Any]]) -> str:
    if value is None:
        return "UNKNOWN"
    for spec in bucket_specs:
        minimum = spec.get("min")
        maximum = spec.get("max")
        if minimum is not None and value < float(minimum):
            continue
        if maximum is not None and value >= float(maximum):
            continue
        return str(spec["label"])
    return str(bucket_specs[-1]["label"]) if bucket_specs else "UNKNOWN"


def _coerce_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _refresh_duckdb_views_if_available(layout: dict[str, Path]) -> None:
    if not layout["duckdb"].exists():
        return
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        return
    connection = duckdb.connect(str(layout["duckdb"]))
    try:
        try:
            refresh_query_views(connection=connection, dataset_root=layout["root"])
        except Exception:
            pass
    finally:
        connection.close()


def _require_pyarrow_dataset():
    try:
        import pyarrow.dataset as dataset  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "VIX regime build requires `pyarrow`. Install the research extras with `pip install -e \".[research]\"`."
        ) from exc
    return dataset
