"""Export canonical SQLite 1m bars to a partitioned Parquet research warehouse.

The exporter is research/offline only. It opens SQLite read-only, never deletes
existing files, and has no broker, runtime, lane, paper, order, or strategy
authority.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset, write_storage_manifest


SCHEMA_VERSION = "canonical_1m_sqlite_to_parquet_export_v1"
DEFAULT_DATA_SOURCE = "historical_1m_canonical"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_PARTITION = "year-quarter"
BASE_DATASET_NAME = "base_1m"
FUTURES_ASSET_CLASS = "futures"

VALIDATION_ROW_COUNTS = "row-counts"
VALIDATION_MIN_MAX_TIMESTAMPS = "min-max-timestamps"
VALIDATION_NO_DUPLICATES = "no-duplicates"
VALIDATION_OHLC = "ohlc"
VALIDATION_SOURCE = "source"
VALIDATION_TIMEZONE = "timezone"
ALL_VALIDATIONS = (
    VALIDATION_ROW_COUNTS,
    VALIDATION_MIN_MAX_TIMESTAMPS,
    VALIDATION_NO_DUPLICATES,
    VALIDATION_OHLC,
    VALIDATION_SOURCE,
    VALIDATION_TIMEZONE,
)


@dataclass(frozen=True)
class ExportConfig:
    mode: str
    source_sqlite: Path
    asset_class: str
    symbols: tuple[str, ...]
    timeframe: str
    data_source: str
    start: datetime
    end: datetime
    output_root: Path
    partition: str
    validations: tuple[str, ...]
    no_delete: bool
    no_source_mutation: bool
    skip_existing: bool


@dataclass(frozen=True)
class PartitionSpec:
    symbol: str
    year: int
    quarter: int
    start: datetime
    end: datetime

    @property
    def partition_id(self) -> str:
        return f"{self.symbol}_{self.year}Q{self.quarter}"


def main(argv: Sequence[str] | None = None) -> int:
    config = _parse_args(argv)
    result = run_export(config)
    print(json.dumps(_summary(result), indent=2, sort_keys=True))
    return 0


def run_export(config: ExportConfig) -> dict[str, Any]:
    _validate_config(config)
    run_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}__canonical_1m_sqlite_to_parquet"
    partition_specs = _partition_specs(config)
    partition_results: list[dict[str, Any]] = []
    validation_failures: list[dict[str, Any]] = []

    with _connect_readonly(config.source_sqlite) as connection:
        for spec in partition_specs:
            rows = _load_rows(connection=connection, config=config, spec=spec)
            validation = _validate_partition(rows=rows, config=config, spec=spec)
            if validation["failure_reasons"]:
                validation_failures.append(
                    {
                        "partition_id": spec.partition_id,
                        "symbol": spec.symbol,
                        "failure_reasons": validation["failure_reasons"],
                    }
                )
            partition_path = _partition_path(config, spec)
            partition_manifest_path = _partition_manifest_path(config, spec)
            existing_validation: dict[str, Any] | None = None
            partition_action = "write"
            if partition_path.exists() or partition_manifest_path.exists():
                if config.skip_existing:
                    existing_validation = _validate_existing_partition(
                        config=config,
                        spec=spec,
                        expected_validation=validation,
                        partition_path=partition_path,
                        partition_manifest_path=partition_manifest_path,
                    )
                    partition_action = "skip_existing"
                else:
                    partition_action = "would_refuse_existing"
            partition_results.append(
                {
                    "spec": spec,
                    "rows": rows,
                    "validation": validation,
                    "existing_validation": existing_validation,
                    "partition_action": partition_action,
                    "partition_path": partition_path,
                    "partition_manifest_path": partition_manifest_path,
                }
            )

    if validation_failures:
        raise SystemExit(json.dumps({"validation_failed": validation_failures}, sort_keys=True))

    manifest = _build_manifest(
        config=config,
        run_id=run_id,
        partition_results=partition_results,
        write_mode=config.mode == "apply",
    )
    validation_summary = _build_validation_summary(
        config=config,
        run_id=run_id,
        partition_results=partition_results,
    )

    if config.mode == "apply":
        _assert_targets_do_not_exist(partition_results, config=config, run_id=run_id)
        for result in partition_results:
            if result["partition_action"] == "skip_existing":
                continue
            materialize_parquet_dataset(result["partition_path"], result["rows"])
            write_storage_manifest(
                result["partition_manifest_path"],
                _build_partition_manifest(config=config, run_id=run_id, partition_result=result),
            )
        write_storage_manifest(_manifest_path(config, run_id), manifest)
        write_storage_manifest(_validation_summary_path(config, run_id), validation_summary)

    return {
        "schema_version": SCHEMA_VERSION,
        "mode": config.mode,
        "run_id": run_id,
        "source_sqlite": str(config.source_sqlite.resolve()),
        "output_root": str(config.output_root.resolve()),
        "partition_count": len(partition_results),
        "written_partition_count": sum(1 for result in partition_results if result["partition_action"] == "write"),
        "skipped_partition_count": sum(1 for result in partition_results if result["partition_action"] == "skip_existing"),
        "row_count": sum(int(result["validation"]["row_count"]) for result in partition_results),
        "written_row_count": sum(
            int(result["validation"]["row_count"]) for result in partition_results if result["partition_action"] == "write"
        ),
        "skipped_row_count": sum(
            int(result["validation"]["row_count"])
            for result in partition_results
            if result["partition_action"] == "skip_existing"
        ),
        "partitions": [
            {
                "partition_id": result["spec"].partition_id,
                "symbol": result["spec"].symbol,
                "year": result["spec"].year,
                "quarter": f"Q{result['spec'].quarter}",
                "partition_action": result["partition_action"],
                "row_count": result["validation"]["row_count"],
                "partition_path": str(result["partition_path"]),
                "partition_manifest_path": str(result["partition_manifest_path"]),
            }
            for result in partition_results
        ],
        "manifest_path": str(_manifest_path(config, run_id)),
        "validation_summary_path": str(_validation_summary_path(config, run_id)),
        "validation_summary": validation_summary,
        "wrote_outputs": config.mode == "apply",
        "source_mutated": False,
        "deleted_outputs": False,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
    }


def _parse_args(argv: Sequence[str] | None) -> ExportConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("dry-run", "apply"), required=True)
    parser.add_argument("--source-sqlite", required=True, type=Path)
    parser.add_argument("--asset-class", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, for example GC,MGC.")
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--data-source", default=DEFAULT_DATA_SOURCE)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--partition", default=DEFAULT_PARTITION, choices=(DEFAULT_PARTITION,))
    parser.add_argument("--validate", default=",".join(ALL_VALIDATIONS))
    parser.add_argument("--no-delete", action="store_true")
    parser.add_argument("--no-source-mutation", action="store_true")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Validate existing partition artifacts and skip them instead of overwriting.",
    )
    args = parser.parse_args(argv)
    return ExportConfig(
        mode=str(args.mode),
        source_sqlite=args.source_sqlite,
        asset_class=str(args.asset_class),
        symbols=tuple(_parse_symbols(str(args.symbols))),
        timeframe=str(args.timeframe),
        data_source=str(args.data_source),
        start=_parse_timestamp(str(args.start)),
        end=_parse_timestamp(str(args.end)),
        output_root=args.output_root,
        partition=str(args.partition),
        validations=tuple(_parse_validations(str(args.validate))),
        no_delete=bool(args.no_delete),
        no_source_mutation=bool(args.no_source_mutation),
        skip_existing=bool(args.skip_existing),
    )


def _validate_config(config: ExportConfig) -> None:
    if not config.source_sqlite.exists():
        raise SystemExit(f"source SQLite does not exist: {config.source_sqlite}")
    if config.asset_class != FUTURES_ASSET_CLASS:
        raise SystemExit(f"unsupported asset class: {config.asset_class}")
    if config.timeframe != DEFAULT_TIMEFRAME:
        raise SystemExit("only timeframe 1m is supported")
    if config.data_source != DEFAULT_DATA_SOURCE:
        raise SystemExit("only data_source historical_1m_canonical is supported")
    if config.end < config.start:
        raise SystemExit("end must be greater than or equal to start")
    if not config.no_delete:
        raise SystemExit("--no-delete is required")
    if not config.no_source_mutation:
        raise SystemExit("--no-source-mutation is required")
    unknown = sorted(set(config.validations) - set(ALL_VALIDATIONS))
    if unknown:
        raise SystemExit(f"unknown validation(s): {','.join(unknown)}")


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("pragma query_only = true")
    return connection


def _load_rows(*, connection: sqlite3.Connection, config: ExportConfig, spec: PartitionSpec) -> list[dict[str, Any]]:
    sqlite_rows = connection.execute(
        """
        select
            bar_id,
            coalesce(symbol, ticker) as symbol,
            ticker,
            asset_class,
            timeframe,
            data_source,
            start_ts,
            end_ts,
            open,
            high,
            low,
            close,
            volume,
            is_final
        from bars
        where coalesce(symbol, ticker) = ?
          and timeframe = ?
          and data_source = ?
          and end_ts >= ?
          and end_ts <= ?
        order by end_ts asc, bar_id asc
        """,
        [
            spec.symbol,
            config.timeframe,
            config.data_source,
            spec.start.isoformat(),
            spec.end.isoformat(),
        ],
    ).fetchall()
    return [_export_row(row=row, config=config, spec=spec) for row in sqlite_rows]


def _export_row(*, row: sqlite3.Row, config: ExportConfig, spec: PartitionSpec) -> dict[str, Any]:
    start_utc = _to_utc(_parse_timestamp(str(row["start_ts"])))
    end_utc = _to_utc(_parse_timestamp(str(row["end_ts"])))
    symbol = str(row["symbol"]).upper()
    return {
        "instrument": symbol,
        "symbol_root": symbol,
        "ticker": str(row["ticker"] or symbol).upper(),
        "asset_class": config.asset_class,
        "timestamp_utc": end_utc,
        "start_ts_utc": start_utc,
        "end_ts_utc": end_utc,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row["volume"] or 0),
        "timeframe": str(row["timeframe"]),
        "source": str(row["data_source"]),
        "data_source": str(row["data_source"]),
        "sqlite_path": str(config.source_sqlite.resolve()),
        "sqlite_table": "bars",
        "bar_id": str(row["bar_id"]),
        "is_final": bool(row["is_final"]),
        "export_partition_id": spec.partition_id,
    }


def _validate_partition(*, rows: list[dict[str, Any]], config: ExportConfig, spec: PartitionSpec) -> dict[str, Any]:
    failures: list[str] = []
    timestamps = [row["timestamp_utc"] for row in rows]
    distinct_timestamps = set(timestamps)
    duplicate_count = len(timestamps) - len(distinct_timestamps)
    ohlc_invalid_count = sum(
        1
        for row in rows
        if row["high"] < row["open"]
        or row["high"] < row["close"]
        or row["low"] > row["open"]
        or row["low"] > row["close"]
        or row["high"] < row["low"]
    )
    negative_volume_count = sum(1 for row in rows if row["volume"] < 0)
    wrong_source_count = sum(1 for row in rows if row["data_source"] != config.data_source)
    wrong_timeframe_count = sum(1 for row in rows if row["timeframe"] != config.timeframe)
    non_utc_count = sum(1 for timestamp in timestamps if not _is_utc_timestamp(timestamp))

    if VALIDATION_ROW_COUNTS in config.validations and not rows:
        failures.append("NO_ROWS_FOR_PARTITION")
    if VALIDATION_NO_DUPLICATES in config.validations and duplicate_count:
        failures.append("DUPLICATE_TIMESTAMPS")
    if VALIDATION_OHLC in config.validations and ohlc_invalid_count:
        failures.append("OHLC_INVALID")
    if VALIDATION_OHLC in config.validations and negative_volume_count:
        failures.append("NEGATIVE_VOLUME")
    if VALIDATION_SOURCE in config.validations and wrong_source_count:
        failures.append("WRONG_DATA_SOURCE")
    if VALIDATION_SOURCE in config.validations and wrong_timeframe_count:
        failures.append("WRONG_TIMEFRAME")
    if VALIDATION_TIMEZONE in config.validations and non_utc_count:
        failures.append("NON_UTC_TIMESTAMP")

    return {
        "partition_id": spec.partition_id,
        "symbol": spec.symbol,
        "year": spec.year,
        "quarter": f"Q{spec.quarter}",
        "query_bounds": {"start": spec.start.isoformat(), "end": spec.end.isoformat()},
        "row_count": len(rows),
        "distinct_timestamp_count": len(distinct_timestamps),
        "duplicate_timestamp_count": duplicate_count,
        "ohlc_invalid_count": ohlc_invalid_count,
        "negative_volume_count": negative_volume_count,
        "wrong_source_count": wrong_source_count,
        "wrong_timeframe_count": wrong_timeframe_count,
        "non_utc_timestamp_count": non_utc_count,
        "min_timestamp_utc": min(timestamps).isoformat() if timestamps else None,
        "max_timestamp_utc": max(timestamps).isoformat() if timestamps else None,
        "failure_reasons": failures,
    }


def _validate_existing_partition(
    *,
    config: ExportConfig,
    spec: PartitionSpec,
    expected_validation: Mapping[str, Any],
    partition_path: Path,
    partition_manifest_path: Path,
) -> dict[str, Any]:
    if not partition_path.exists() or not partition_manifest_path.exists():
        raise SystemExit(
            json.dumps(
                {
                    "invalid_existing_partition": {
                        "partition_id": spec.partition_id,
                        "reason": "MISSING_PARTITION_FILE_OR_MANIFEST",
                        "partition_path": str(partition_path),
                        "partition_manifest_path": str(partition_manifest_path),
                    }
                },
                sort_keys=True,
            )
        )
    try:
        existing_rows = _read_parquet_rows(partition_path)
        existing_manifest = json.loads(partition_manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - surface exact corrupted artifact reason in CLI failure.
        raise SystemExit(
            json.dumps(
                {
                    "invalid_existing_partition": {
                        "partition_id": spec.partition_id,
                        "reason": "UNREADABLE_EXISTING_PARTITION",
                        "detail": str(exc),
                    }
                },
                sort_keys=True,
            )
        ) from exc
    existing_validation = _validate_partition(rows=existing_rows, config=config, spec=spec)
    mismatch_reasons = _existing_partition_mismatches(
        expected_validation=expected_validation,
        existing_validation=existing_validation,
        existing_manifest=existing_manifest,
        config=config,
        spec=spec,
    )
    if mismatch_reasons or existing_validation["failure_reasons"]:
        raise SystemExit(
            json.dumps(
                {
                    "invalid_existing_partition": {
                        "partition_id": spec.partition_id,
                        "failure_reasons": [*existing_validation["failure_reasons"], *mismatch_reasons],
                    }
                },
                sort_keys=True,
            )
        )
    return existing_validation


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("skip-existing validation requires pyarrow to read existing Parquet partitions.") from exc
    return list(pq.read_table(path).to_pylist())


def _existing_partition_mismatches(
    *,
    expected_validation: Mapping[str, Any],
    existing_validation: Mapping[str, Any],
    existing_manifest: Mapping[str, Any],
    config: ExportConfig,
    spec: PartitionSpec,
) -> list[str]:
    mismatch_reasons: list[str] = []
    for key in ("row_count", "min_timestamp_utc", "max_timestamp_utc"):
        if expected_validation.get(key) != existing_validation.get(key):
            mismatch_reasons.append(f"EXISTING_{key.upper()}_MISMATCH")
    if existing_manifest.get("source_sqlite_path") != str(config.source_sqlite.resolve()):
        mismatch_reasons.append("EXISTING_SOURCE_SQLITE_PATH_MISMATCH")
    if existing_manifest.get("symbol") != spec.symbol:
        mismatch_reasons.append("EXISTING_SYMBOL_MISMATCH")
    if existing_manifest.get("timeframe") != config.timeframe:
        mismatch_reasons.append("EXISTING_TIMEFRAME_MISMATCH")
    if existing_manifest.get("data_source") != config.data_source:
        mismatch_reasons.append("EXISTING_DATA_SOURCE_MISMATCH")
    return mismatch_reasons


def _assert_targets_do_not_exist(partition_results: list[dict[str, Any]], *, config: ExportConfig, run_id: str) -> None:
    targets = [_manifest_path(config, run_id), _validation_summary_path(config, run_id)]
    for result in partition_results:
        if result["partition_action"] == "skip_existing":
            continue
        targets.append(result["partition_path"])
        targets.append(result["partition_manifest_path"])
    existing = [str(path) for path in targets if path.exists()]
    if existing:
        raise SystemExit(json.dumps({"refusing_to_overwrite_without_delete": existing}, sort_keys=True))


def _build_manifest(
    *,
    config: ExportConfig,
    run_id: str,
    partition_results: list[dict[str, Any]],
    write_mode: bool,
) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_VERSION}.manifest",
        "run_id": run_id,
        "mode": config.mode,
        "write_mode": write_mode,
        "source_sqlite_path": str(config.source_sqlite.resolve()),
        "sqlite_table": "bars",
        "asset_class": config.asset_class,
        "symbols": list(config.symbols),
        "timeframe": config.timeframe,
        "data_source": config.data_source,
        "query_bounds": {"start": config.start.isoformat(), "end": config.end.isoformat()},
        "output_root": str(config.output_root.resolve()),
        "layout": "outputs/research_warehouse/base_1m/futures/<SYMBOL>/<YEAR>/Q<q>/bars.parquet",
        "partition_count": len(partition_results),
        "written_partition_count": sum(1 for result in partition_results if result["partition_action"] == "write"),
        "skipped_partition_count": sum(1 for result in partition_results if result["partition_action"] == "skip_existing"),
        "row_count": sum(int(result["validation"]["row_count"]) for result in partition_results),
        "written_row_count": sum(
            int(result["validation"]["row_count"]) for result in partition_results if result["partition_action"] == "write"
        ),
        "skipped_row_count": sum(
            int(result["validation"]["row_count"])
            for result in partition_results
            if result["partition_action"] == "skip_existing"
        ),
        "partitions": [
            {
                "partition_id": result["spec"].partition_id,
                "partition_action": result["partition_action"],
                "partition_path": str(result["partition_path"]),
                "partition_manifest_path": str(result["partition_manifest_path"]),
                "validation": result["validation"],
                "existing_validation": result["existing_validation"],
            }
            for result in partition_results
        ],
        "no_delete": config.no_delete,
        "no_source_mutation": config.no_source_mutation,
        "skip_existing": config.skip_existing,
        "source_mutated": False,
        "deleted_outputs": False,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
    }


def _build_validation_summary(
    *,
    config: ExportConfig,
    run_id: str,
    partition_results: list[dict[str, Any]],
) -> dict[str, Any]:
    validations = [result["validation"] for result in partition_results]
    return {
        "schema_version": f"{SCHEMA_VERSION}.validation_summary",
        "run_id": run_id,
        "source_sqlite_path": str(config.source_sqlite.resolve()),
        "query_bounds": {"start": config.start.isoformat(), "end": config.end.isoformat()},
        "validation_set": list(config.validations),
        "partition_count": len(validations),
        "written_partition_count": sum(1 for result in partition_results if result["partition_action"] == "write"),
        "skipped_partition_count": sum(1 for result in partition_results if result["partition_action"] == "skip_existing"),
        "row_count": sum(int(item["row_count"]) for item in validations),
        "written_row_count": sum(
            int(result["validation"]["row_count"]) for result in partition_results if result["partition_action"] == "write"
        ),
        "skipped_row_count": sum(
            int(result["validation"]["row_count"])
            for result in partition_results
            if result["partition_action"] == "skip_existing"
        ),
        "duplicate_timestamp_count": sum(int(item["duplicate_timestamp_count"]) for item in validations),
        "ohlc_invalid_count": sum(int(item["ohlc_invalid_count"]) for item in validations),
        "negative_volume_count": sum(int(item["negative_volume_count"]) for item in validations),
        "wrong_source_count": sum(int(item["wrong_source_count"]) for item in validations),
        "wrong_timeframe_count": sum(int(item["wrong_timeframe_count"]) for item in validations),
        "non_utc_timestamp_count": sum(int(item["non_utc_timestamp_count"]) for item in validations),
        "failure_reasons": sorted({reason for item in validations for reason in item["failure_reasons"]}),
        "partitions": validations,
    }


def _build_partition_manifest(
    *,
    config: ExportConfig,
    run_id: str,
    partition_result: dict[str, Any],
) -> dict[str, Any]:
    spec = partition_result["spec"]
    return {
        "schema_version": f"{SCHEMA_VERSION}.partition_manifest",
        "run_id": run_id,
        "partition_id": spec.partition_id,
        "source_sqlite_path": str(config.source_sqlite.resolve()),
        "sqlite_table": "bars",
        "asset_class": config.asset_class,
        "symbol": spec.symbol,
        "timeframe": config.timeframe,
        "data_source": config.data_source,
        "query_bounds": {"start": spec.start.isoformat(), "end": spec.end.isoformat()},
        "partition_path": str(partition_result["partition_path"]),
        "validation": partition_result["validation"],
        "source_mutated": False,
        "deleted_outputs": False,
    }


def _partition_specs(config: ExportConfig) -> list[PartitionSpec]:
    specs: list[PartitionSpec] = []
    for symbol in config.symbols:
        cursor = config.start
        while cursor <= config.end:
            q_start, q_end = _quarter_bounds(cursor)
            part_start = max(cursor, q_start)
            part_end = min(config.end, q_end)
            specs.append(
                PartitionSpec(
                    symbol=symbol,
                    year=part_start.year,
                    quarter=((part_start.month - 1) // 3) + 1,
                    start=part_start,
                    end=part_end,
                )
            )
            cursor = q_end + timedelta(microseconds=1)
    return specs


def _quarter_bounds(value: datetime) -> tuple[datetime, datetime]:
    quarter = ((value.month - 1) // 3) + 1
    first_month = (quarter - 1) * 3 + 1
    q_start = value.replace(month=first_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    if first_month == 10:
        next_q_start = value.replace(year=value.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        next_q_start = value.replace(month=first_month + 3, day=1, hour=0, minute=0, second=0, microsecond=0)
    return q_start, next_q_start - timedelta(microseconds=1)


def _partition_path(config: ExportConfig, spec: PartitionSpec) -> Path:
    return (
        config.output_root
        / BASE_DATASET_NAME
        / config.asset_class
        / spec.symbol
        / str(spec.year)
        / f"Q{spec.quarter}"
        / "bars.parquet"
    )


def _partition_manifest_path(config: ExportConfig, spec: PartitionSpec) -> Path:
    return _partition_path(config, spec).parent / "partition_manifest.json"


def _manifest_path(config: ExportConfig, run_id: str) -> Path:
    return config.output_root / BASE_DATASET_NAME / "manifests" / run_id / "manifest.json"


def _validation_summary_path(config: ExportConfig, run_id: str) -> Path:
    return config.output_root / BASE_DATASET_NAME / "manifests" / run_id / "validation_summary.json"


def _summary(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": result.get("schema_version"),
        "mode": result.get("mode"),
        "run_id": result.get("run_id"),
        "source_sqlite": result.get("source_sqlite"),
        "output_root": result.get("output_root"),
        "partition_count": result.get("partition_count"),
        "row_count": result.get("row_count"),
        "manifest_path": result.get("manifest_path"),
        "validation_summary_path": result.get("validation_summary_path"),
        "validation_failure_reasons": result.get("validation_summary", {}).get("failure_reasons", []),
        "written_partition_count": result.get("written_partition_count"),
        "skipped_partition_count": result.get("skipped_partition_count"),
        "written_row_count": result.get("written_row_count"),
        "skipped_row_count": result.get("skipped_row_count"),
        "wrote_outputs": result.get("wrote_outputs"),
        "source_mutated": result.get("source_mutated"),
        "deleted_outputs": result.get("deleted_outputs"),
        "runtime_activity": result.get("runtime_activity"),
        "strategy_behavior_changed": result.get("strategy_behavior_changed"),
    }


def _parse_symbols(value: str) -> list[str]:
    symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not symbols:
        raise SystemExit("--symbols must include at least one symbol")
    return symbols


def _parse_validations(value: str) -> list[str]:
    if value.strip().lower() in {"all", "*"}:
        return list(ALL_VALIDATIONS)
    validations = [item.strip() for item in value.split(",") if item.strip()]
    return validations or list(ALL_VALIDATIONS)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise SystemExit(f"timestamp must include timezone: {value}")
    return parsed


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _is_utc_timestamp(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
