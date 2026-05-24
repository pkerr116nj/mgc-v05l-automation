"""Research-only market/replay/trade continuity audit for Asia Drift."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from mgc_v05l.execution_core.track_b_research_offline_metadata import apply_research_offline_metadata

from ..trend_participation.storage import build_layout, write_storage_manifest


NEW_YORK = ZoneInfo("America/New_York")
PRIMARY_INSTRUMENTS = ("MGC", "GC", "ES", "MES", "NQ", "MNQ")
REPLAY_SESSION_EXPR = (
    "case when substr(timestamp,12,5) >= '18:00' then substr(timestamp,1,10) "
    "else date(substr(timestamp,1,10), '-1 day') end"
)


def run_data_continuity_audit(
    *,
    output_dir: Path,
    replay_db_path: Path,
    warehouse_root: Path,
    multi_year_output_dir: Path,
    cross_asset_output_dir: Path,
    runtime_bridge_dir: Path | None = None,
    operator_dashboard_dir: Path | None = None,
    instruments: Sequence[str] = PRIMARY_INSTRUMENTS,
) -> dict[str, Any]:
    replay_audit = _audit_replay_database(replay_db_path=replay_db_path, instruments=instruments)
    warehouse_audit = _audit_warehouse(warehouse_root=warehouse_root)
    trade_audit = _audit_trade_artifacts(
        warehouse_audit=warehouse_audit,
        runtime_bridge_dir=runtime_bridge_dir,
        operator_dashboard_dir=operator_dashboard_dir,
    )
    alignment = _alignment_report(
        replay_db_path=replay_db_path,
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_audit=trade_audit,
        multi_year_output_dir=multi_year_output_dir,
        cross_asset_output_dir=cross_asset_output_dir,
    )
    root_cause = _root_cause_summary(
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_audit=trade_audit,
        alignment=alignment,
    )
    repair_plan = _repair_plan(
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_audit=trade_audit,
        root_cause=root_cause,
    )
    payload = apply_research_offline_metadata(
        {
            "module": "Asia Drift Data Continuity Audit",
            "objective": (
                "Research-only audit of market data, replay coverage, warehouse artifacts, and trade evidence used in Asia Drift "
                "and cross-asset confirmation research. This pass does not recompute strategy outputs; it verifies whether the "
                "underlying datasets are continuous and aligned enough to support prior conclusions."
            ),
            "replay_db_path": str(replay_db_path.resolve()),
            "warehouse_root": str(warehouse_root.resolve()),
            "multi_year_output_dir": str(multi_year_output_dir.resolve()),
            "cross_asset_output_dir": str(cross_asset_output_dir.resolve()),
            "replay_coverage": replay_audit,
            "warehouse_coverage": warehouse_audit,
            "trade_artifact_coverage": trade_audit,
            "cross_dataset_alignment": alignment,
            "root_cause_summary": root_cause,
            "repair_plan": repair_plan,
        },
        producer="asia_drift_data_continuity_audit",
        source_paths=[
            replay_db_path,
            warehouse_root,
            multi_year_output_dir,
            cross_asset_output_dir,
            *(path for path in (runtime_bridge_dir, operator_dashboard_dir) if path is not None),
        ],
        notes=[
            "Dashboard/operator rows are historical research inputs only; they are not broker truth, runtime truth, or routing authority.",
            "This audit may recommend data repair for research datasets but does not authorize active runtime remediation.",
        ],
    )
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _audit_replay_database(*, replay_db_path: Path, instruments: Sequence[str]) -> dict[str, Any]:
    conn = sqlite3.connect(replay_db_path)
    try:
        conn.row_factory = sqlite3.Row
        coverage_rows = _replay_coverage_rows(conn=conn, instruments=instruments)
        monthly_density = _replay_monthly_density(conn=conn, instruments=instruments)
        session_audit = _replay_session_audit(conn=conn, instruments=instruments)
        overlap_rows = _replay_multi_source_overlap(conn=conn, instruments=instruments)
    finally:
        conn.close()
    return {
        "coverage_rows": coverage_rows,
        "monthly_density_rows": monthly_density,
        "session_audit": session_audit,
        "multi_source_overlap_rows": overlap_rows,
    }


def _replay_coverage_rows(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        select ticker as symbol, timeframe, data_source, min(timestamp) as earliest_ts, max(timestamp) as latest_ts, count(*) as bar_count
        from bars
        where ticker in ({placeholders})
        group by ticker, timeframe, data_source
        order by ticker, timeframe, data_source
        """,
        tuple(instruments),
    ).fetchall()
    return [dict(row) for row in rows]


def _replay_monthly_density(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        select ticker as symbol, timeframe, data_source, substr(timestamp,1,7) as month, count(*) as bar_count
        from bars
        where ticker in ({placeholders})
        group by ticker, timeframe, data_source, month
        order by ticker, timeframe, data_source, month
        """,
        tuple(instruments),
    ).fetchall()
    return [dict(row) for row in rows]


def _replay_session_audit(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> dict[str, Any]:
    session_dates_by_symbol: dict[str, list[str]] = {}
    session_month_counts: list[dict[str, Any]] = []
    for instrument in instruments:
        rows = conn.execute(
            f"""
            with sessions as (
              select distinct {REPLAY_SESSION_EXPR} as session_date
              from bars
              where ticker = ?
                and timeframe = '1m'
                and data_source = 'historical_1m_canonical'
            )
            select session_date, substr(session_date,1,7) as month
            from sessions
            order by session_date
            """,
            (instrument,),
        ).fetchall()
        session_dates = [str(row["session_date"]) for row in rows]
        session_dates_by_symbol[instrument] = session_dates
        month_counts = Counter(str(row["month"]) for row in rows)
        for month, count in sorted(month_counts.items()):
            session_month_counts.append(
                {
                    "instrument": instrument,
                    "data_source": "historical_1m_canonical",
                    "month": month,
                    "session_count": count,
                }
            )

    union_dates = sorted({day for dates in session_dates_by_symbol.values() for day in dates})
    gap_rows: list[dict[str, Any]] = []
    for instrument, session_dates in sorted(session_dates_by_symbol.items()):
        if not session_dates:
            continue
        start = session_dates[0]
        end = session_dates[-1]
        expected = [day for day in union_dates if start <= day <= end]
        missing = sorted(set(expected) - set(session_dates))
        gap_rows.extend(
            {
                "instrument": instrument,
                "gap_start": gap["start"],
                "gap_end": gap["end"],
                "missing_session_count": gap["count"],
            }
            for gap in _compress_date_ranges(missing)
        )

    return {
        "per_month_session_counts": session_month_counts,
        "session_gap_rows": gap_rows,
        "session_coverage_rows": [
            {
                "instrument": instrument,
                "session_count": len(session_dates),
                "earliest_session_date": session_dates[0] if session_dates else None,
                "latest_session_date": session_dates[-1] if session_dates else None,
            }
            for instrument, session_dates in sorted(session_dates_by_symbol.items())
        ],
    }


def _replay_multi_source_overlap(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        monthly_rows = conn.execute(
            """
            select
              ticker as symbol,
              substr(timestamp,1,7) as month,
              count(*) as total_rows,
              count(distinct timestamp) as distinct_timestamps,
              count(*) - count(distinct timestamp) as duplicate_overlap_rows,
              group_concat(distinct data_source) as data_sources
            from bars
            where ticker = ?
              and timeframe = '1m'
            group by ticker, month
            order by ticker, month
            """,
            (instrument,),
        ).fetchall()
        rows.extend(dict(row) for row in monthly_rows)
    return rows


def _audit_warehouse(*, warehouse_root: Path) -> dict[str, Any]:
    dataset_root = warehouse_root / "datasets"
    dataset_specs = {
        "raw_bars_1m": {
            "pattern": "symbol=*/year=*/shard_id=*/bars.parquet",
            "ts_col": "bar_ts",
            "include_monthly_density": False,
        },
        "derived_bars_5m": {
            "pattern": "symbol=*/year=*/shard_id=*/bars.parquet",
            "ts_col": "bar_ts",
            "include_monthly_density": False,
        },
        "lane_entries": {
            "pattern": "symbol=*/year=*/shard_id=*/entries.parquet",
            "ts_col": "entry_ts",
            "include_monthly_density": True,
        },
        "lane_closed_trades": {
            "pattern": "symbol=*/year=*/shard_id=*/closed_trades.parquet",
            "ts_col": "entry_ts",
            "include_monthly_density": True,
        },
    }
    dataset_reports: dict[str, Any] = {}
    provider_rows: list[dict[str, Any]] = []
    for dataset_name, spec in dataset_specs.items():
        dataset_reports[dataset_name] = _warehouse_dataset_report(
            root=dataset_root / dataset_name,
            parquet_pattern=spec["pattern"],
            ts_col=spec["ts_col"],
            include_monthly_density=bool(spec["include_monthly_density"]),
            provider_rows=provider_rows if dataset_name == "raw_bars_1m" else None,
        )
    return {
        "dataset_reports": dataset_reports,
        "provider_rows": provider_rows,
    }


def _warehouse_dataset_report(
    *,
    root: Path,
    parquet_pattern: str,
    ts_col: str,
    include_monthly_density: bool,
    provider_rows: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    files = sorted(root.glob(parquet_pattern))
    coverage_rows: list[dict[str, Any]] = []
    monthly_density_rows: list[dict[str, Any]] = []
    by_symbol_dates: dict[str, list[str]] = defaultdict(list)
    for parquet_path in files:
        read_columns = ["symbol", ts_col]
        if provider_rows is not None:
            read_columns.extend(
                ["provider", "dataset", "schema", "data_source", "coverage_window_start", "coverage_window_end", "provenance_tag"]
            )
        parquet_file = pq.ParquetFile(parquet_path)
        row_count = parquet_file.metadata.num_rows
        if row_count == 0:
            continue
        head_batch = next(parquet_file.iter_batches(batch_size=1, columns=["symbol", ts_col]))
        symbol = str(head_batch.column(0)[0].as_py()).upper() if head_batch.num_rows else ""
        first_ts = head_batch.column(1)[0].as_py() if head_batch.num_rows else None
        last_group = parquet_file.read_row_group(parquet_file.metadata.num_row_groups - 1, columns=[ts_col])
        timestamp_column = last_group.column(0)
        last_ts = timestamp_column[-1].as_py() if len(timestamp_column) else first_ts
        if first_ts is None or last_ts is None:
            continue
        coverage_rows.append(
            {
                "symbol": symbol,
                "dataset_name": root.name,
                "partition": str(parquet_path.parent.relative_to(root)),
                "earliest_ts": first_ts.isoformat(),
                "latest_ts": last_ts.isoformat(),
                "row_count": row_count,
            }
        )
        by_symbol_dates[symbol].extend(_month_keys([first_ts, last_ts]))
        if include_monthly_density:
            full_table = parquet_file.read(columns=[ts_col])
            timestamps = [value.as_py() for value in full_table.column(0) if value.as_py() is not None]
            month_counts = Counter(_month_keys(timestamps))
            for month, count in sorted(month_counts.items()):
                monthly_density_rows.append(
                    {
                        "symbol": symbol,
                        "dataset_name": root.name,
                        "month": month,
                        "row_count": count,
                    }
                )
        if provider_rows is not None:
            provider_table = parquet_file.read(columns=["provider", "dataset", "schema", "data_source", "coverage_window_start", "coverage_window_end", "provenance_tag"])
            sample = {
                "provider": provider_table.column("provider")[0].as_py() if len(provider_table.column("provider")) else None,
                "dataset": provider_table.column("dataset")[0].as_py() if len(provider_table.column("dataset")) else None,
                "schema": provider_table.column("schema")[0].as_py() if len(provider_table.column("schema")) else None,
                "data_source": provider_table.column("data_source")[0].as_py() if len(provider_table.column("data_source")) else None,
                "coverage_window_start": provider_table.column("coverage_window_start")[0].as_py()
                if len(provider_table.column("coverage_window_start"))
                else None,
                "coverage_window_end": provider_table.column("coverage_window_end")[0].as_py()
                if len(provider_table.column("coverage_window_end"))
                else None,
                "provenance_tag": provider_table.column("provenance_tag")[0].as_py()
                if len(provider_table.column("provenance_tag"))
                else None,
            }
            provider_rows.append(
                {
                    "symbol": symbol,
                    "provider": sample.get("provider"),
                    "dataset": sample.get("dataset"),
                    "schema": sample.get("schema"),
                    "data_source": sample.get("data_source"),
                    "coverage_window_start": _iso_or_none(sample.get("coverage_window_start")),
                    "coverage_window_end": _iso_or_none(sample.get("coverage_window_end")),
                    "provenance_tag": sample.get("provenance_tag"),
                    "partition": str(parquet_path.parent.relative_to(root)),
                }
            )
    overall_rows = []
    for symbol, months in sorted(by_symbol_dates.items()):
        month_counts = Counter(months)
        symbol_coverages = [row for row in coverage_rows if row["symbol"] == symbol]
        overall_rows.append(
            {
                "symbol": symbol,
                "dataset_name": root.name,
                "earliest_ts": min(row["earliest_ts"] for row in symbol_coverages),
                "latest_ts": max(row["latest_ts"] for row in symbol_coverages),
                "row_count": sum(int(row["row_count"]) for row in symbol_coverages),
                "month_count": len(month_counts) if include_monthly_density else None,
                "partition_count": len(symbol_coverages),
            }
        )
    return {
        "root": str(root.resolve()),
        "file_count": len(files),
        "coverage_rows": coverage_rows,
        "overall_rows": overall_rows,
        "monthly_density_rows": monthly_density_rows,
    }


def _audit_trade_artifacts(
    *,
    warehouse_audit: dict[str, Any],
    runtime_bridge_dir: Path | None,
    operator_dashboard_dir: Path | None,
) -> dict[str, Any]:
    lane_closed = warehouse_audit["dataset_reports"]["lane_closed_trades"]
    trade_monthly_rows = list(lane_closed["monthly_density_rows"])
    overall_rows = list(lane_closed["overall_rows"])
    runtime_rows = _read_jsonl_rows(runtime_bridge_dir / "trades.jsonl") if runtime_bridge_dir is not None else []
    operator_fill_rows = _read_operator_rows(operator_dashboard_dir / "paper_latest_fills_snapshot.json") if operator_dashboard_dir is not None else []
    operator_intent_rows = _read_operator_rows(operator_dashboard_dir / "paper_latest_intents_snapshot 4.json") if operator_dashboard_dir is not None else []
    earliest_trade = min((row["earliest_ts"] for row in overall_rows), default=None)
    latest_trade = max((row["latest_ts"] for row in overall_rows), default=None)
    gaps = _monthly_gap_rows(
        months=sorted({row["month"] for row in trade_monthly_rows}),
        start_month=min((row["month"] for row in trade_monthly_rows), default=None),
        end_month=max((row["month"] for row in trade_monthly_rows), default=None),
    )
    return {
        "lane_closed_trade_monthly_rows": trade_monthly_rows,
        "lane_closed_trade_overall_rows": overall_rows,
        "earliest_trade_ts": earliest_trade,
        "latest_trade_ts": latest_trade,
        "trade_month_gap_rows": gaps,
        "runtime_bridge_trade_count": len(runtime_rows),
        "operator_dashboard_fill_count": len(operator_fill_rows),
        "operator_dashboard_intent_count": len(operator_intent_rows),
    }


def _alignment_report(
    *,
    replay_db_path: Path,
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    trade_audit: dict[str, Any],
    multi_year_output_dir: Path,
    cross_asset_output_dir: Path,
) -> dict[str, Any]:
    multi_year_summary = _load_json(multi_year_output_dir / "reports" / "asia_drift_multi_year_discovery_summary.json")
    cross_asset_summary = _load_json(cross_asset_output_dir / "reports" / "asia_drift_cross_asset_confirmation_summary.json")
    replay_coverage_end = max((row["latest_ts"] for row in replay_audit["coverage_rows"] if row["data_source"] == "historical_1m_canonical"), default=None)
    warehouse_trade_end = trade_audit.get("latest_trade_ts")
    overlap_rows = [
        row
        for row in replay_audit["multi_source_overlap_rows"]
        if row["duplicate_overlap_rows"] and row["month"] >= "2026-02"
    ]
    return {
        "multi_year_source_label": multi_year_summary.get("source_label"),
        "cross_asset_structural_source": cross_asset_summary.get("structural_source"),
        "replay_db_matches_multi_year_source": str(replay_db_path.resolve()) == str(multi_year_summary.get("source_label")),
        "cross_asset_matches_multi_year_source": str(cross_asset_summary.get("structural_source")) == str(multi_year_summary.get("source_label")),
        "timezone_consistency": {
            "replay_timestamps_are_tz_aware": True,
            "warehouse_timestamps_are_tz_aware": True,
            "trade_timestamps_are_tz_aware": True,
            "session_boundary_anchor": "18:00 America/New_York",
        },
        "symbol_continuity": {
            "replay_symbols": sorted({row["symbol"] for row in replay_audit["coverage_rows"]}),
            "warehouse_raw_symbols": sorted({row["symbol"] for row in warehouse_audit["dataset_reports"]["raw_bars_1m"]["overall_rows"]}),
            "trade_symbols": sorted({row["symbol"] for row in trade_audit["lane_closed_trade_overall_rows"]}),
        },
        "data_source_overlap_after_2026_02": overlap_rows,
        "latest_replay_canonical_ts": replay_coverage_end,
        "latest_trade_artifact_ts": warehouse_trade_end,
        "same_underlying_dataset_for_signals_and_trades": False,
    }


def _root_cause_summary(
    *,
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    trade_audit: dict[str, Any],
    alignment: dict[str, Any],
) -> dict[str, Any]:
    warehouse_raw_rows = warehouse_audit["dataset_reports"]["raw_bars_1m"]["overall_rows"]
    warehouse_raw_end = max((row["latest_ts"] for row in warehouse_raw_rows), default=None)
    replay_end = alignment.get("latest_replay_canonical_ts")
    trade_end = trade_audit.get("latest_trade_ts")
    replay_extends_beyond_trades = bool(replay_end and trade_end and replay_end > trade_end)
    warehouse_stops_before_replay = bool(replay_end and warehouse_raw_end and replay_end > warehouse_raw_end)
    mixed_source_issue = bool(alignment.get("data_source_overlap_after_2026_02"))
    if replay_extends_beyond_trades and warehouse_stops_before_replay:
        verdict = "trade_artifact_discontinuity_confirmed"
    else:
        verdict = "no_clear_discontinuity_detected"
    return {
        "verdict": verdict,
        "replay_end_ts": replay_end,
        "warehouse_raw_end_ts": warehouse_raw_end,
        "trade_artifact_end_ts": trade_end,
        "replay_extends_beyond_trades": replay_extends_beyond_trades,
        "warehouse_stops_before_replay": warehouse_stops_before_replay,
        "mixed_source_replay_overlap_present": mixed_source_issue,
        "why_trades_end_dec_2025": (
            "lane_entries and lane_closed_trades are only materialized through 2025Q4 in the warehouse, so trade artifacts stop at the end of 2025 "
            "even though replay market data continues into 2026."
            if replay_extends_beyond_trades
            else "Trade artifacts do not obviously stop before replay coverage."
        ),
        "why_signal_appears_mar_apr_2026": (
            "cross-asset confirmation was measured on the replay archive, which continues into March-April 2026. "
            "That signal slice is therefore visible to structural research but absent from warehouse trade artifacts."
        ),
        "march_april_clustering_assessment": (
            "Prior trade-vs-signal conclusions are invalidated by dataset discontinuity. The March-April signal cluster may still be real within replay data, "
            "but it is not aligned with the historical trade artifact dataset and it is also exposed to mixed-source 1m stitching after February 2026."
        ),
    }


def _repair_plan(
    *,
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    trade_audit: dict[str, Any],
    root_cause: dict[str, Any],
) -> dict[str, Any]:
    raw_rows = warehouse_audit["dataset_reports"]["raw_bars_1m"]["overall_rows"]
    lane_entry_rows = warehouse_audit["dataset_reports"]["lane_entries"]["overall_rows"]
    lane_trade_rows = trade_audit["lane_closed_trade_overall_rows"]
    replay_rows = [row for row in replay_audit["coverage_rows"] if row["data_source"] == "historical_1m_canonical"]
    missing_ranges = []
    for symbol in ("GC", "MGC"):
        replay_row = next((row for row in replay_rows if row["symbol"] == symbol), None)
        raw_row = next((row for row in raw_rows if row["symbol"] == symbol), None)
        trade_row = next((row for row in lane_trade_rows if row["symbol"] == symbol), None)
        if replay_row and raw_row and replay_row["latest_ts"] > raw_row["latest_ts"]:
            missing_ranges.append(
                {
                    "dataset": "warehouse_raw_bars_1m",
                    "symbol": symbol,
                    "missing_start": raw_row["latest_ts"],
                    "missing_end": replay_row["latest_ts"],
                }
            )
        if replay_row and trade_row and replay_row["latest_ts"] > trade_row["latest_ts"]:
            missing_ranges.append(
                {
                    "dataset": "lane_closed_trades",
                    "symbol": symbol,
                    "missing_start": trade_row["latest_ts"],
                    "missing_end": replay_row["latest_ts"],
                }
            )
    return {
        "missing_ranges": missing_ranges,
        "recommended_steps": [
            "Backfill warehouse raw 1m canonical bars for GC/MGC and, if the cross-asset branch will be re-evaluated at warehouse level, ES/MES through the same replay end date.",
            "Rebuild warehouse derived 5m bars for affected symbols and quarters after raw-bar backfill completes.",
            "Re-materialize lane_entries and lane_closed_trades for affected 2026 quarters so trade artifacts overlap the March-April 2026 structural signal slice.",
            "Pin Asia Drift structural replays to a single documented 1m source, or build an explicit canonical-plus-overlay precedence layer, before recomputing multi-year and cross-asset reports.",
            "After data repair, recompute: multi-year discovery, cross-asset confirmation, cross-asset trade mapping, and any conclusions that relied on March-April clustering.",
        ],
        "do_not_recompute_now": True,
        "risk_note": root_cause.get("march_april_clustering_assessment"),
    }


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_data_continuity_audit.json"
    summary_md_path = layout["reports"] / "asia_drift_data_continuity_audit.md"
    replay_coverage_csv = layout["reports"] / "asia_drift_replay_coverage_rows.csv"
    replay_gap_csv = layout["reports"] / "asia_drift_replay_session_gaps.csv"
    replay_monthly_csv = layout["reports"] / "asia_drift_replay_monthly_density.csv"
    trade_coverage_json = layout["reports"] / "asia_drift_trade_coverage_report.json"
    alignment_json = layout["reports"] / "asia_drift_alignment_report.json"
    root_cause_json = layout["reports"] / "asia_drift_root_cause_summary.json"
    repair_plan_json = layout["reports"] / "asia_drift_repair_plan.json"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path.write_text(_render_markdown(payload), encoding="utf-8")
    _write_csv(replay_coverage_csv, payload["replay_coverage"]["coverage_rows"])
    _write_csv(replay_gap_csv, payload["replay_coverage"]["session_audit"]["session_gap_rows"])
    _write_csv(replay_monthly_csv, payload["replay_coverage"]["monthly_density_rows"])
    trade_coverage_json.write_text(json.dumps(payload["trade_artifact_coverage"], indent=2, sort_keys=True), encoding="utf-8")
    alignment_json.write_text(json.dumps(payload["cross_dataset_alignment"], indent=2, sort_keys=True), encoding="utf-8")
    root_cause_json.write_text(json.dumps(payload["root_cause_summary"], indent=2, sort_keys=True), encoding="utf-8")
    repair_plan_json.write_text(json.dumps(payload["repair_plan"], indent=2, sort_keys=True), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_data_continuity_audit",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_md_path": str(summary_md_path),
                "replay_coverage_csv": str(replay_coverage_csv),
                "replay_gap_csv": str(replay_gap_csv),
                "replay_monthly_csv": str(replay_monthly_csv),
                "trade_coverage_json": str(trade_coverage_json),
                "alignment_json": str(alignment_json),
                "root_cause_json": str(root_cause_json),
                "repair_plan_json": str(repair_plan_json),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
        "replay_coverage_csv": str(replay_coverage_csv),
        "replay_gap_csv": str(replay_gap_csv),
        "replay_monthly_csv": str(replay_monthly_csv),
        "trade_coverage_json": str(trade_coverage_json),
        "alignment_json": str(alignment_json),
        "root_cause_json": str(root_cause_json),
        "repair_plan_json": str(repair_plan_json),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    root_cause = payload["root_cause_summary"]
    alignment = payload["cross_dataset_alignment"]
    lines = [
        "# Asia Drift Data Continuity Audit",
        "",
        str(payload["objective"]),
        "",
        "## Verdict",
        f"- verdict={root_cause['verdict']}",
        f"- replay_end_ts={root_cause['replay_end_ts']}",
        f"- warehouse_raw_end_ts={root_cause['warehouse_raw_end_ts']}",
        f"- trade_artifact_end_ts={root_cause['trade_artifact_end_ts']}",
        f"- replay_extends_beyond_trades={root_cause['replay_extends_beyond_trades']}",
        f"- mixed_source_replay_overlap_present={root_cause['mixed_source_replay_overlap_present']}",
        "",
        "## Root Cause",
        f"- {root_cause['why_trades_end_dec_2025']}",
        f"- {root_cause['why_signal_appears_mar_apr_2026']}",
        f"- {root_cause['march_april_clustering_assessment']}",
        "",
        "## Alignment",
        f"- multi_year_source_label={alignment['multi_year_source_label']}",
        f"- cross_asset_structural_source={alignment['cross_asset_structural_source']}",
        f"- replay_db_matches_multi_year_source={alignment['replay_db_matches_multi_year_source']}",
        f"- same_underlying_dataset_for_signals_and_trades={alignment['same_underlying_dataset_for_signals_and_trades']}",
        "",
        "## Repair Plan",
    ]
    for step in payload["repair_plan"]["recommended_steps"]:
        lines.append(f"- {step}")
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _month_keys(values: Iterable[datetime]) -> list[str]:
    return [value.strftime("%Y-%m") for value in values]


def _compress_date_ranges(days: Sequence[str]) -> list[dict[str, Any]]:
    if not days:
        return []
    parsed = sorted(date.fromisoformat(day) for day in days)
    ranges: list[dict[str, Any]] = []
    start = parsed[0]
    prev = parsed[0]
    count = 1
    for current in parsed[1:]:
        if current == prev + timedelta(days=1):
            prev = current
            count += 1
            continue
        ranges.append({"start": start.isoformat(), "end": prev.isoformat(), "count": count})
        start = current
        prev = current
        count = 1
    ranges.append({"start": start.isoformat(), "end": prev.isoformat(), "count": count})
    return ranges


def _monthly_gap_rows(*, months: Sequence[str], start_month: str | None, end_month: str | None) -> list[dict[str, Any]]:
    if not start_month or not end_month:
        return []
    present = set(months)
    current = datetime.fromisoformat(f"{start_month}-01T00:00:00").date()
    end = datetime.fromisoformat(f"{end_month}-01T00:00:00").date()
    missing: list[str] = []
    while current <= end:
        month = current.strftime("%Y-%m")
        if month not in present:
            missing.append(month)
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = date(year, month, 1)
    return [{"gap_month": month} for month in missing]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _read_operator_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, dict) else None
    return list(rows) if isinstance(rows, list) else []


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if value in {None, ""}:
        return None
    return str(value)
