"""Schema-preserving canonical subset builder for verification workflows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import select

from ..persistence import build_engine
from ..persistence.db import create_schema
from ..persistence.tables import (
    bars_table,
    instruments_table,
    market_data_bar_provenance_table,
    market_data_ingest_runs_table,
)


@dataclass(frozen=True)
class CanonicalSubsetBuildResult:
    source_db_path: str
    target_db_path: str
    symbols: list[str]
    timeframes: list[str]
    data_sources: list[str]
    instrument_count: int
    bar_count: int
    provenance_row_count: int
    ingest_run_count: int
    generated_at: str
    report_path: str | None = None


def build_schema_preserving_canonical_subset(
    *,
    source_db_path: str | Path,
    target_db_path: str | Path,
    symbols: Sequence[str],
    timeframes: Sequence[str] | None = None,
    data_sources: Sequence[str] | None = None,
    reset_target: bool = True,
    report_path: str | Path | None = None,
) -> CanonicalSubsetBuildResult:
    source_path = Path(source_db_path).resolve()
    target_path = Path(target_db_path).resolve()
    if reset_target and target_path.exists():
        target_path.unlink()

    source_engine = build_engine(f"sqlite:///{source_path}")
    target_engine = build_engine(f"sqlite:///{target_path}")
    create_schema(target_engine)

    normalized_symbols = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    normalized_timeframes = sorted({str(timeframe).strip().lower() for timeframe in (timeframes or []) if str(timeframe).strip()})
    normalized_data_sources = sorted({str(source).strip() for source in (data_sources or []) if str(source).strip()})

    bar_statement = select(bars_table).where(bars_table.c.ticker.in_(normalized_symbols))
    if normalized_timeframes:
        bar_statement = bar_statement.where(bars_table.c.timeframe.in_(normalized_timeframes))
    if normalized_data_sources:
        bar_statement = bar_statement.where(bars_table.c.data_source.in_(normalized_data_sources))

    with source_engine.begin() as source_connection:
        bar_rows = source_connection.execute(bar_statement).mappings().all()
        instrument_ids = sorted({int(row["instrument_id"]) for row in bar_rows if row.get("instrument_id") is not None})
        instrument_rows = _load_rows_for_ids(
            source_connection,
            instruments_table,
            instruments_table.c.instrument_id,
            instrument_ids,
        )
        bar_ids = [str(row["bar_id"]) for row in bar_rows]
        provenance_rows = _load_rows_for_ids(
            source_connection,
            market_data_bar_provenance_table,
            market_data_bar_provenance_table.c.bar_id,
            bar_ids,
        )
        ingest_run_ids = sorted({str(row["ingest_run_id"]) for row in provenance_rows if row.get("ingest_run_id")})
        ingest_run_rows = _load_rows_for_ids(
            source_connection,
            market_data_ingest_runs_table,
            market_data_ingest_runs_table.c.ingest_run_id,
            ingest_run_ids,
        )

    with target_engine.begin() as target_connection:
        _bulk_insert_rows(target_connection, instruments_table, instrument_rows)
        _bulk_insert_rows(target_connection, market_data_ingest_runs_table, ingest_run_rows)
        _bulk_insert_rows(target_connection, bars_table, bar_rows)
        _bulk_insert_rows(target_connection, market_data_bar_provenance_table, provenance_rows)

    result = CanonicalSubsetBuildResult(
        source_db_path=str(source_path),
        target_db_path=str(target_path),
        symbols=normalized_symbols,
        timeframes=normalized_timeframes,
        data_sources=normalized_data_sources,
        instrument_count=len(instrument_rows),
        bar_count=len(bar_rows),
        provenance_row_count=len(provenance_rows),
        ingest_run_count=len(ingest_run_rows),
        generated_at=datetime.now(UTC).isoformat(),
    )
    resolved_report_path = Path(report_path).resolve() if report_path is not None else None
    if resolved_report_path is not None:
        resolved_report_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_report_path.write_text(json.dumps(asdict(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return CanonicalSubsetBuildResult(**{**asdict(result), "report_path": str(resolved_report_path)})
    return result


def _bulk_insert_rows(connection: Any, table: Any, rows: Sequence[dict[str, Any] | Any], *, chunk_size: int = 5000) -> None:
    if not rows:
        return
    insert_statement = table.insert().prefix_with("OR REPLACE")
    for start in range(0, len(rows), chunk_size):
        chunk = [dict(row) for row in rows[start : start + chunk_size]]
        connection.execute(insert_statement, chunk)


def _load_rows_for_ids(
    connection: Any,
    table: Any,
    id_column: Any,
    values: Sequence[Any],
    *,
    chunk_size: int = 900,
) -> list[dict[str, Any]]:
    if not values:
        return []
    rows: list[dict[str, Any]] = []
    for start in range(0, len(values), chunk_size):
        chunk = list(values[start : start + chunk_size])
        rows.extend(connection.execute(select(table).where(id_column.in_(chunk))).mappings().all())
    return rows
