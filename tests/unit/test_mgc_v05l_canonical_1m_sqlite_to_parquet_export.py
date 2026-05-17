from __future__ import annotations

import ast
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app.canonical_1m_sqlite_to_parquet_export import (
    DEFAULT_DATA_SOURCE,
    ExportConfig,
    main,
    run_export,
)


def test_dry_run_validates_without_writing_bars(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    output_root = tmp_path / "warehouse"

    result = run_export(_config(source=source, output_root=output_root, mode="dry-run", symbols=("GC",)))

    assert result["mode"] == "dry-run"
    assert result["row_count"] == 5
    assert result["wrote_outputs"] is False
    assert result["source_mutated"] is False
    assert result["deleted_outputs"] is False
    assert not (output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet").exists()
    assert not (output_root / "base_1m" / "manifests").exists()


def test_apply_writes_partitioned_parquet_and_manifests(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    output_root = tmp_path / "warehouse"

    result = run_export(_config(source=source, output_root=output_root, mode="apply", symbols=("GC",)))
    bars_path = output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet"
    partition_manifest_path = bars_path.parent / "partition_manifest.json"
    manifest_path = Path(result["manifest_path"])
    validation_summary_path = Path(result["validation_summary_path"])

    assert bars_path.exists()
    assert partition_manifest_path.exists()
    assert manifest_path.exists()
    assert validation_summary_path.exists()

    import pyarrow.parquet as pq

    table = pq.read_table(bars_path)
    rows = table.to_pylist()
    partition_manifest = json.loads(partition_manifest_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validation_summary = json.loads(validation_summary_path.read_text(encoding="utf-8"))

    assert table.num_rows == 5
    assert rows[0]["instrument"] == "GC"
    assert rows[0]["data_source"] == DEFAULT_DATA_SOURCE
    assert rows[0]["timestamp_utc"].tzinfo is not None
    assert partition_manifest["source_sqlite_path"] == str(source.resolve())
    assert partition_manifest["query_bounds"]["start"] == "2024-01-01T00:00:00+00:00"
    assert manifest["row_count"] == 5
    assert validation_summary["failure_reasons"] == []
    assert validation_summary["row_count"] == 5


def test_apply_multiple_symbols_writes_expected_layout(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    _append_rows(source, symbol="MGC")
    output_root = tmp_path / "warehouse"

    result = run_export(_config(source=source, output_root=output_root, mode="apply", symbols=("GC", "MGC")))

    assert result["row_count"] == 10
    assert (output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet").exists()
    assert (output_root / "base_1m" / "futures" / "MGC" / "2024" / "Q1" / "bars.parquet").exists()


def test_duplicate_timestamps_are_rejected_before_writing(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    _insert_bar(source, symbol="GC", offset=0, bar_id="duplicate-ts")
    output_root = tmp_path / "warehouse"

    with pytest.raises(SystemExit) as excinfo:
        run_export(_config(source=source, output_root=output_root, mode="apply", symbols=("GC",)))

    assert "DUPLICATE_TIMESTAMPS" in str(excinfo.value)
    assert not (output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet").exists()


def test_bad_ohlc_is_rejected_before_writing(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC", bad_ohlc=True)
    output_root = tmp_path / "warehouse"

    with pytest.raises(SystemExit) as excinfo:
        run_export(_config(source=source, output_root=output_root, mode="apply", symbols=("GC",)))

    assert "OHLC_INVALID" in str(excinfo.value)
    assert not (output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet").exists()


def test_wrong_data_source_or_timeframe_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    output_root = tmp_path / "warehouse"

    with pytest.raises(SystemExit) as source_exc:
        run_export(
            _config(
                source=source,
                output_root=output_root,
                mode="dry-run",
                symbols=("GC",),
                data_source="schwab_history",
            )
        )
    with pytest.raises(SystemExit) as timeframe_exc:
        run_export(_config(source=source, output_root=output_root, mode="dry-run", symbols=("GC",), timeframe="5m"))

    assert "historical_1m_canonical" in str(source_exc.value)
    assert "only timeframe 1m" in str(timeframe_exc.value)


def test_no_delete_and_no_source_mutation_are_required(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    output_root = tmp_path / "warehouse"

    with pytest.raises(SystemExit) as delete_exc:
        run_export(_config(source=source, output_root=output_root, mode="dry-run", symbols=("GC",), no_delete=False))
    with pytest.raises(SystemExit) as mutation_exc:
        run_export(
            _config(
                source=source,
                output_root=output_root,
                mode="dry-run",
                symbols=("GC",),
                no_source_mutation=False,
            )
        )

    assert "--no-delete is required" in str(delete_exc.value)
    assert "--no-source-mutation is required" in str(mutation_exc.value)


def test_apply_refuses_to_overwrite_existing_partition(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    output_root = tmp_path / "warehouse"
    target = output_root / "base_1m" / "futures" / "GC" / "2024" / "Q1" / "bars.parquet"
    target.parent.mkdir(parents=True)
    target.write_text("existing", encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        run_export(_config(source=source, output_root=output_root, mode="apply", symbols=("GC",)))

    assert "refusing_to_overwrite_without_delete" in str(excinfo.value)
    assert target.read_text(encoding="utf-8") == "existing"


def test_source_sqlite_is_not_mutated(tmp_path: Path) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")
    before = _source_counts(source)

    run_export(_config(source=source, output_root=tmp_path / "warehouse", mode="apply", symbols=("GC",)))

    assert _source_counts(source) == before


def test_cli_entrypoint_prints_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source = tmp_path / "canonical.sqlite3"
    _create_sqlite(source, symbol="GC")

    exit_code = main(
        [
            "--mode",
            "dry-run",
            "--source-sqlite",
            str(source),
            "--asset-class",
            "futures",
            "--symbols",
            "GC",
            "--timeframe",
            "1m",
            "--data-source",
            DEFAULT_DATA_SOURCE,
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:05:00+00:00",
            "--output-root",
            str(tmp_path / "warehouse"),
            "--partition",
            "year-quarter",
            "--validate",
            "all",
            "--no-delete",
            "--no-source-mutation",
        ]
    )
    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["row_count"] == 5
    assert captured["wrote_outputs"] is False


def test_forbidden_imports_and_authority_calls_absent() -> None:
    path = Path("src/mgc_v05l/app/canonical_1m_sqlite_to_parquet_export.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    allowed_from_imports = {"mgc_v05l.research.trend_participation.storage"}
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "placeOrder",
        "create_order_intent",
        "mutate_lifecycle",
    }
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module not in allowed_from_imports and node.module.startswith(forbidden_import_roots):
                violations.append(node.module)
        elif isinstance(node, ast.Call):
            call_name: str | None = None
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            if call_name in forbidden_call_names:
                violations.append(call_name)

    assert violations == []


def _config(
    *,
    source: Path,
    output_root: Path,
    mode: str,
    symbols: tuple[str, ...],
    data_source: str = DEFAULT_DATA_SOURCE,
    timeframe: str = "1m",
    no_delete: bool = True,
    no_source_mutation: bool = True,
) -> ExportConfig:
    return ExportConfig(
        mode=mode,
        source_sqlite=source,
        asset_class="futures",
        symbols=symbols,
        timeframe=timeframe,
        data_source=data_source,
        start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        end=datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
        output_root=output_root,
        partition="year-quarter",
        validations=("row-counts", "min-max-timestamps", "no-duplicates", "ohlc", "source", "timezone"),
        no_delete=no_delete,
        no_source_mutation=no_source_mutation,
    )


def _create_sqlite(path: Path, *, symbol: str, bad_ohlc: bool = False) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            create table bars (
                bar_id text primary key,
                ticker text,
                asset_class text,
                data_source text,
                timestamp text,
                symbol text,
                timeframe text,
                start_ts text,
                end_ts text,
                open real,
                high real,
                low real,
                close real,
                volume integer,
                is_final integer,
                created_at text
            )
            """
        )
        for offset in range(5):
            _insert_bar(path, symbol=symbol, offset=offset, bad_ohlc=(bad_ohlc and offset == 2), connection=connection)
        connection.commit()
    finally:
        connection.close()


def _append_rows(path: Path, *, symbol: str) -> None:
    connection = sqlite3.connect(path)
    try:
        for offset in range(5):
            _insert_bar(path, symbol=symbol, offset=offset, connection=connection)
        connection.commit()
    finally:
        connection.close()


def _insert_bar(
    path: Path,
    *,
    symbol: str,
    offset: int,
    bar_id: str | None = None,
    bad_ohlc: bool = False,
    connection: sqlite3.Connection | None = None,
) -> None:
    owns_connection = connection is None
    active_connection = connection or sqlite3.connect(path)
    start = datetime(2024, 1, 1, 0, offset, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    open_px = 100.0 + offset
    close = open_px + 0.25
    high = min(open_px, close) - 0.1 if bad_ohlc else max(open_px, close) + 0.1
    low = min(open_px, close) - 0.1
    try:
        active_connection.execute(
            """
            insert into bars (
                bar_id, ticker, asset_class, data_source, timestamp, symbol, timeframe,
                start_ts, end_ts, open, high, low, close, volume, is_final, created_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                bar_id or f"{DEFAULT_DATA_SOURCE}::{symbol}|1m|{end.isoformat()}|{offset}",
                symbol,
                "futures",
                DEFAULT_DATA_SOURCE,
                end.isoformat(),
                symbol,
                "1m",
                start.isoformat(),
                end.isoformat(),
                open_px,
                high,
                low,
                close,
                10 + offset,
                1,
                datetime(2024, 1, 1, 1, 0, tzinfo=UTC).isoformat(),
            ],
        )
        if owns_connection:
            active_connection.commit()
    finally:
        if owns_connection:
            active_connection.close()


def _source_counts(path: Path) -> tuple[int, int]:
    connection = sqlite3.connect(path)
    try:
        table_count = connection.execute("select count(*) from sqlite_master where type='table'").fetchone()[0]
        row_count = connection.execute("select count(*) from bars").fetchone()[0]
    finally:
        connection.close()
    return int(table_count), int(row_count)
