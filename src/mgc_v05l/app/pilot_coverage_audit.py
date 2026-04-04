"""Operator-grade coverage audit for canonical and derived market-data surfaces."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from ..market_data.canonical_maintenance import GapRange, _is_expected_maintenance_gap

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATABASE_PATH = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "canonical_market_data"
DEFAULT_SYMBOLS = ("MGC", "GC", "MES", "ES", "MNQ", "NQ", "CL", "6E", "ZN")


def generate_pilot_coverage_audit(
    *,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Path]:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    symbol_rows: list[dict[str, Any]] = []
    with sqlite3.connect(resolved_database_path) as connection:
        connection.row_factory = sqlite3.Row
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            canonical_1m = _coverage_audit(connection, symbol=symbol, timeframe="1m", data_source="historical_1m_canonical")
            derived_5m = _coverage_audit(connection, symbol=symbol, timeframe="5m", data_source="historical_5m_canonical")
            derived_10m = _coverage_audit(connection, symbol=symbol, timeframe="10m", data_source="historical_10m_canonical")
            if int(derived_5m.get("bar_count") or 0) == 0 and int(canonical_1m.get("bar_count") or 0) > 0:
                derived_5m = _derived_coverage_from_canonical_1m(connection, symbol=symbol, target_minutes=5)
            if int(derived_10m.get("bar_count") or 0) == 0 and int(canonical_1m.get("bar_count") or 0) > 0:
                derived_10m = _derived_coverage_from_canonical_1m(connection, symbol=symbol, target_minutes=10)
            audits = {
                "canonical_1m": canonical_1m,
                "derived_5m": derived_5m,
                "derived_10m": derived_10m,
            }
            provenance_rows = _provenance_summary(connection, symbol=symbol)
            any_gaps = any(int(audit.get("gap_count") or 0) > 0 for audit in audits.values())
            symbol_rows.append(
                {
                    "symbol": symbol,
                    **audits,
                    "gaps_detected": any_gaps,
                    "gaps_repaired": not any_gaps,
                    "provider_provenance_summary": provenance_rows,
                }
            )

    generated_at = datetime.now(UTC).isoformat()
    json_path = resolved_output_dir / "pilot_coverage_audit.json"
    markdown_path = resolved_output_dir / "pilot_coverage_audit.md"
    payload = {
        "generated_at": generated_at,
        "database_path": str(resolved_database_path),
        "symbols": symbol_rows,
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _coverage_audit(connection: sqlite3.Connection, *, symbol: str, timeframe: str, data_source: str) -> dict[str, Any]:
    summary_row = connection.execute(
        """
        select
          count(*) as bar_count,
          min(end_ts) as earliest,
          max(end_ts) as latest
        from bars
        where ticker = ?
          and timeframe = ?
          and data_source = ?
          and is_final = 1
        """,
        (symbol, timeframe, data_source),
    ).fetchone()
    gap_rows = connection.execute(
        """
        with ordered as (
          select
            end_ts,
            lag(end_ts) over (order by end_ts asc) as prev_end_ts
          from bars
          where ticker = ?
            and timeframe = ?
            and data_source = ?
            and is_final = 1
        )
        select
          prev_end_ts,
          end_ts,
          cast((unixepoch(end_ts) - unixepoch(prev_end_ts)) / 60 as integer) as gap_minutes
        from ordered
        where prev_end_ts is not null
          and cast((unixepoch(end_ts) - unixepoch(prev_end_ts)) / 60 as integer) > 1
        order by prev_end_ts asc
        """,
        (symbol, timeframe, data_source),
    ).fetchall()
    gaps: list[GapRange] = []
    for row in gap_rows:
        left = datetime.fromisoformat(str(row["prev_end_ts"]))
        right = datetime.fromisoformat(str(row["end_ts"]))
        gap_minutes = int(row["gap_minutes"] or 0)
        if gap_minutes <= 1 or _is_expected_maintenance_gap(left, right):
            continue
        gaps.append(
            GapRange(
                start=(left + timedelta(minutes=1)).isoformat(),
                end=right.isoformat(),
                missing_minutes=gap_minutes - 1,
                expected_maintenance_gap=False,
            )
        )
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "data_source": data_source,
        "bar_count": int(summary_row["bar_count"] or 0),
        "earliest": summary_row["earliest"],
        "latest": summary_row["latest"],
        "gap_count": len(gaps),
        "gaps": [asdict(gap) for gap in gaps],
        "report_path": None,
    }


def _derived_coverage_from_canonical_1m(
    connection: sqlite3.Connection,
    *,
    symbol: str,
    target_minutes: int,
) -> dict[str, Any]:
    bucket_summary = connection.execute(
        """
        with complete_buckets as (
          select
            cast(unixepoch(end_ts) / (? * 60) as integer) as bucket_id,
            count(*) as minute_count,
            max(end_ts) as bucket_end_ts
          from bars
          where ticker = ?
            and timeframe = '1m'
            and data_source = 'historical_1m_canonical'
            and is_final = 1
          group by bucket_id
          having count(*) = ?
        )
        select
          count(*) as bar_count,
          min(bucket_end_ts) as earliest,
          max(bucket_end_ts) as latest
        from complete_buckets
        """,
        (target_minutes, symbol, target_minutes),
    ).fetchone()
    bucket_gap_rows = connection.execute(
        """
        with complete_buckets as (
          select
            cast(unixepoch(end_ts) / (? * 60) as integer) as bucket_id,
            count(*) as minute_count,
            max(end_ts) as bucket_end_ts
          from bars
          where ticker = ?
            and timeframe = '1m'
            and data_source = 'historical_1m_canonical'
            and is_final = 1
          group by bucket_id
          having count(*) = ?
        ),
        ordered as (
          select
            bucket_id,
            bucket_end_ts,
            lag(bucket_id) over (order by bucket_id asc) as prev_bucket_id,
            lag(bucket_end_ts) over (order by bucket_id asc) as prev_bucket_end_ts
          from complete_buckets
        )
        select
          prev_bucket_id,
          bucket_id,
          prev_bucket_end_ts,
          bucket_end_ts
        from ordered
        where prev_bucket_id is not null
          and bucket_id - prev_bucket_id > 1
        order by prev_bucket_id asc
        """,
        (target_minutes, symbol, target_minutes),
    ).fetchall()
    gaps: list[GapRange] = []
    for row in bucket_gap_rows:
        left = datetime.fromisoformat(str(row["prev_bucket_end_ts"]))
        right = datetime.fromisoformat(str(row["bucket_end_ts"]))
        if _is_expected_maintenance_gap(left, right):
            continue
        missing_bucket_count = int(row["bucket_id"] - row["prev_bucket_id"] - 1)
        if missing_bucket_count <= 0:
            continue
        gaps.append(
            GapRange(
                start=(left + timedelta(minutes=target_minutes)).isoformat(),
                end=right.isoformat(),
                missing_minutes=missing_bucket_count * target_minutes,
                expected_maintenance_gap=False,
            )
        )
    timeframe_label = f"{target_minutes}m"
    return {
        "symbol": symbol,
        "timeframe": timeframe_label,
        "data_source": f"derived_on_demand_from_historical_1m_canonical[{timeframe_label}]",
        "bar_count": int(bucket_summary["bar_count"] or 0),
        "earliest": bucket_summary["earliest"],
        "latest": bucket_summary["latest"],
        "gap_count": len(gaps),
        "gaps": [asdict(gap) for gap in gaps],
        "report_path": None,
    }


def _provenance_summary(connection: sqlite3.Connection, *, symbol: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        select
          provider,
          dataset,
          schema_name,
          data_source,
          interval,
          count(*) as bar_count,
          count(distinct ingest_run_id) as ingest_run_count,
          min(source_timestamp) as earliest_source_timestamp,
          max(source_timestamp) as latest_source_timestamp,
          min(ingest_time) as earliest_ingest_time,
          max(ingest_time) as latest_ingest_time
        from market_data_bar_provenance
        where internal_symbol = ?
        group by provider, dataset, schema_name, data_source, interval
        order by interval asc, data_source asc, provider asc
        """,
        (symbol,),
    ).fetchall()
    return [dict(row) for row in rows]


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Pilot Coverage Audit",
        "",
        f"- Generated At: {payload.get('generated_at')}",
        f"- Database: {payload.get('database_path')}",
        "",
        "| Symbol | 1m Earliest | 1m Latest | 5m Earliest | 5m Latest | 10m Earliest | 10m Latest | Remaining Gaps | Provenance |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in payload.get("symbols") or []:
        provenance = ", ".join(
            f"{entry.get('provider')}/{entry.get('data_source')}[{entry.get('interval')}]"
            for entry in row.get("provider_provenance_summary") or []
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("symbol") or "-"),
                    str(((row.get("canonical_1m") or {}).get("earliest")) or "-"),
                    str(((row.get("canonical_1m") or {}).get("latest")) or "-"),
                    str(((row.get("derived_5m") or {}).get("earliest")) or "-"),
                    str(((row.get("derived_5m") or {}).get("latest")) or "-"),
                    str(((row.get("derived_10m") or {}).get("earliest")) or "-"),
                    str(((row.get("derived_10m") or {}).get("latest")) or "-"),
                    "YES" if row.get("gaps_detected") else "NO",
                    provenance or "-",
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(prog="pilot-coverage-audit")
    parser.add_argument("--database-path", default=str(DEFAULT_DATABASE_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--symbol", action="append", default=None)
    args = parser.parse_args()
    outputs = generate_pilot_coverage_audit(
        database_path=args.database_path,
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        output_dir=args.output_dir,
    )
    print(json.dumps({key: str(value) for key, value in outputs.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
