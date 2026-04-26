"""Operational reporting for Asia Drift shadow trading."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
ROLLING_SESSION_WINDOWS = (5, 20)


def run_asia_drift_shadow_report(
    *,
    start_date: date,
    end_date: date,
    output_dir: Path,
    shadow_output_dir: Path,
    warehouse_root: Path,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    shadow_output_dir = shadow_output_dir.resolve()
    warehouse_root = warehouse_root.resolve()
    layout = build_layout(output_dir)

    reports_dir = shadow_output_dir / "reports"
    required_paths = {
        "candidate_csv": reports_dir / "asia_drift_shadow_candidate_log.csv",
        "trade_csv": reports_dir / "asia_drift_shadow_daily_trade_log.csv",
        "source_summary_json": reports_dir / "asia_drift_shadow_summary.json",
    }
    missing_required = [name for name, path in required_paths.items() if not path.exists()]
    if missing_required:
        raise FileNotFoundError(f"Missing shadow artifact(s): {', '.join(missing_required)}")

    candidate_rows = _filter_rows_by_trade_date(_read_csv_rows(required_paths["candidate_csv"]), start_date, end_date)
    trade_rows = _filter_rows_by_trade_date(_read_csv_rows(required_paths["trade_csv"]), start_date, end_date)
    source_summary = json.loads(required_paths["source_summary_json"].read_text(encoding="utf-8"))

    daily_rows = _build_daily_rows(start_date=start_date, end_date=end_date, candidate_rows=candidate_rows, trade_rows=trade_rows)
    weekly_rows = _build_weekly_rows(daily_rows)
    rolling_rows = _build_rolling_rows(daily_rows=daily_rows, trade_rows=trade_rows)
    process_payload = _build_process_quality_payload(
        start_date=start_date,
        end_date=end_date,
        candidate_rows=candidate_rows,
        trade_rows=trade_rows,
        shadow_output_dir=shadow_output_dir,
        warehouse_root=warehouse_root,
        source_summary=source_summary,
        missing_required=missing_required,
    )

    trade_csv = layout["reports"] / "asia_drift_shadow_report_trade_log.csv"
    trade_parquet = layout["reports"] / "asia_drift_shadow_report_trade_log.parquet"
    daily_csv = layout["reports"] / "asia_drift_shadow_report_daily_performance.csv"
    daily_parquet = layout["reports"] / "asia_drift_shadow_report_daily_performance.parquet"
    rolling_csv = layout["reports"] / "asia_drift_shadow_report_rolling_performance.csv"
    rolling_parquet = layout["reports"] / "asia_drift_shadow_report_rolling_performance.parquet"
    status_json = layout["reports"] / "asia_drift_shadow_report_status.json"
    daily_md = layout["reports"] / "asia_drift_shadow_report_daily_summary.md"
    weekly_md = layout["reports"] / "asia_drift_shadow_report_weekly_summary.md"

    for path, rows in [
        (trade_csv, trade_rows),
        (daily_csv, daily_rows),
        (rolling_csv, rolling_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (trade_parquet, trade_rows),
        (daily_parquet, daily_rows),
        (rolling_parquet, rolling_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    status_json.write_text(json.dumps(process_payload, indent=2, sort_keys=True), encoding="utf-8")
    daily_md.write_text(_render_daily_markdown(daily_rows=daily_rows, process_payload=process_payload), encoding="utf-8")
    weekly_md.write_text(_render_weekly_markdown(weekly_rows=weekly_rows, rolling_rows=rolling_rows), encoding="utf-8")

    payload = {
        "module": "asia_drift_shadow_reporting",
        "scope": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "shadow_output_dir": str(shadow_output_dir),
            "warehouse_root": str(warehouse_root),
        },
        "artifacts": {
            "trade_csv": str(trade_csv),
            "trade_parquet": str(trade_parquet),
            "daily_csv": str(daily_csv),
            "daily_parquet": str(daily_parquet),
            "rolling_csv": str(rolling_csv),
            "rolling_parquet": str(rolling_parquet),
            "status_json": str(status_json),
            "daily_markdown": str(daily_md),
            "weekly_markdown": str(weekly_md),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "row_counts": {
            "trade_rows": len(trade_rows),
            "daily_rows": len(daily_rows),
            "weekly_rows": len(weekly_rows),
            "rolling_rows": len(rolling_rows),
        },
        "process_status": process_payload["overall_status"],
    }
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_shadow_reporting",
            "artifact_paths": payload["artifacts"],
            "trade_rows": len(trade_rows),
            "daily_rows": len(daily_rows),
            "rolling_rows": len(rolling_rows),
        },
    )
    return payload


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _filter_rows_by_trade_date(rows: Sequence[dict[str, str]], start_date: date, end_date: date) -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for row in rows:
        trade_date = date.fromisoformat(str(row["trade_date"]))
        if start_date <= trade_date <= end_date:
            payload.append(dict(row))
    return payload


def _build_daily_rows(
    *,
    start_date: date,
    end_date: date,
    candidate_rows: Sequence[dict[str, str]],
    trade_rows: Sequence[dict[str, str]],
) -> list[dict[str, Any]]:
    candidates_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    trades_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidate_rows:
        candidates_by_date[str(row["trade_date"])].append(row)
    for row in trade_rows:
        trades_by_date[str(row["trade_date"])].append(row)

    payload: list[dict[str, Any]] = []
    for session_date in _business_dates(start_date, end_date):
        session_key = session_date.isoformat()
        day_candidates = candidates_by_date.get(session_key, [])
        day_trades = trades_by_date.get(session_key, [])
        state_counts = Counter(str(row["state_classification"]) for row in day_candidates)
        instrument_set = sorted({str(row["instrument"]) for row in day_trades})
        exit_counts = Counter(str(row["exit_reason"]) for row in day_trades)
        total_points = sum(float(row["return_points"]) for row in day_trades)
        total_r = sum(_row_r_multiple(row) for row in day_trades)
        max_adverse = max((float(row["max_adverse_excursion"]) for row in day_trades), default=0.0)
        notes: list[str] = []
        if not day_candidates:
            notes.append("no_candidates")
        elif not day_trades:
            notes.append("no_signal_day")
        payload.append(
            {
                "trade_date": session_key,
                "candidate_count": len(day_candidates),
                "simulated_trade_count": len(day_trades),
                "instruments_triggered": ",".join(instrument_set),
                "favorable_candidates": state_counts.get("TRADE_FAVORABLE", 0),
                "neutral_candidates": state_counts.get("TRADE_NEUTRAL", 0),
                "do_not_trade_candidates": state_counts.get("DO_NOT_TRADE", 0),
                "simulated_entries": len(day_trades),
                "simulated_exits": len(day_trades),
                "exit_stop_count": exit_counts.get("STOP", 0),
                "exit_target_count": exit_counts.get("TARGET", 0),
                "exit_time_count": exit_counts.get("TIME", 0),
                "daily_pl_points": round(total_points, 6),
                "daily_pl_r": round(total_r, 6),
                "max_daily_adverse_excursion": round(max_adverse, 6),
                "notes": ";".join(notes),
            }
        )
    return payload


def _build_weekly_rows(daily_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in daily_rows:
        trade_date = date.fromisoformat(str(row["trade_date"]))
        iso_year, iso_week, _ = trade_date.isocalendar()
        grouped[f"{iso_year}-W{iso_week:02d}"].append(row)
    payload: list[dict[str, Any]] = []
    for week_key in sorted(grouped):
        bucket = grouped[week_key]
        trade_count = sum(int(row["simulated_trade_count"]) for row in bucket)
        total_r = sum(float(row["daily_pl_r"]) for row in bucket)
        total_points = sum(float(row["daily_pl_points"]) for row in bucket)
        no_signal_days = sum(1 for row in bucket if "no_signal_day" in str(row["notes"]).split(";"))
        payload.append(
            {
                "week": week_key,
                "session_count": len(bucket),
                "trade_count": trade_count,
                "cumulative_r": round(total_r, 6),
                "cumulative_points": round(total_points, 6),
                "avg_r_per_session": round(total_r / len(bucket), 6) if bucket else 0.0,
                "no_signal_days": no_signal_days,
            }
        )
    return payload


def _build_rolling_rows(
    *,
    daily_rows: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, str]],
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    trade_rows_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in trade_rows:
        trade_rows_by_date[str(row["trade_date"])].append(row)
    session_dates = [str(row["trade_date"]) for row in daily_rows]

    for window in list(ROLLING_SESSION_WINDOWS) + [None]:
        window_label = f"last_{window}_sessions" if window is not None else "since_start"
        window_dates = session_dates[-window:] if window is not None else session_dates
        payload.append(_rolling_row(window_label=window_label, instrument="ALL", window_dates=window_dates, trade_rows_by_date=trade_rows_by_date))
        for instrument in INDEX_SYMBOLS:
            payload.append(
                _rolling_row(
                    window_label=window_label,
                    instrument=instrument,
                    window_dates=window_dates,
                    trade_rows_by_date=trade_rows_by_date,
                )
            )
    return payload


def _rolling_row(
    *,
    window_label: str,
    instrument: str,
    window_dates: Sequence[str],
    trade_rows_by_date: dict[str, list[dict[str, str]]],
) -> dict[str, Any]:
    bucket: list[dict[str, str]] = []
    for window_date in window_dates:
        for row in trade_rows_by_date.get(window_date, []):
            if instrument == "ALL" or str(row["instrument"]) == instrument:
                bucket.append(row)
    returns_r = [_row_r_multiple(row) for row in bucket]
    wins = [value for value in returns_r if value > 0.0]
    return {
        "window_label": window_label,
        "instrument": instrument,
        "session_count": len(window_dates),
        "trade_count": len(bucket),
        "win_rate": round(len(wins) / len(returns_r), 6) if returns_r else 0.0,
        "avg_r": round(sum(returns_r) / len(returns_r), 6) if returns_r else 0.0,
        "cumulative_r": round(sum(returns_r), 6),
        "max_drawdown_r": round(_max_drawdown_from_returns(returns_r), 6),
        "longest_losing_streak": _longest_losing_streak(returns_r),
        "total_return_points": round(sum(float(row["return_points"]) for row in bucket), 6) if bucket else 0.0,
    }


def _build_process_quality_payload(
    *,
    start_date: date,
    end_date: date,
    candidate_rows: Sequence[dict[str, str]],
    trade_rows: Sequence[dict[str, str]],
    shadow_output_dir: Path,
    warehouse_root: Path,
    source_summary: dict[str, Any],
    missing_required: Sequence[str],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "missing_data",
            "status": "fail" if missing_required else "ok",
            "details": {"missing_required": list(missing_required), "candidate_rows": len(candidate_rows), "trade_rows": len(trade_rows)},
        }
    )
    vol_rows = load_vol_regime_rows(warehouse_root)
    latest_vix_trade_date = str(vol_rows[-1]["vix_trade_date"]) if vol_rows else None
    last_weekday = _last_business_date(start_date, end_date)
    stale_vix = latest_vix_trade_date is None or latest_vix_trade_date < last_weekday.isoformat()
    checks.append(
        {
            "name": "stale_vix_regime_data",
            "status": "warn" if stale_vix else "ok",
            "details": {"latest_vix_trade_date": latest_vix_trade_date, "expected_at_least": last_weekday.isoformat()},
        }
    )
    candidate_symbols = sorted({str(row["instrument"]) for row in candidate_rows})
    incomplete_symbols = [symbol for symbol in INDEX_SYMBOLS if symbol not in candidate_symbols]
    checks.append(
        {
            "name": "incomplete_warehouse_updates",
            "status": "warn" if incomplete_symbols else "ok",
            "details": {"candidate_symbols": candidate_symbols, "missing_symbols": incomplete_symbols},
        }
    )
    candidate_days = {str(row["trade_date"]) for row in candidate_rows}
    trade_days = {str(row["trade_date"]) for row in trade_rows}
    no_signal_days = sorted(day for day in candidate_days if day not in trade_days)
    checks.append(
        {
            "name": "no_signal_days",
            "status": "warn" if no_signal_days else "ok",
            "details": {"count": len(no_signal_days), "days": no_signal_days[:20]},
        }
    )
    zero_trade_symbols = [symbol for symbol in INDEX_SYMBOLS if all(str(row["instrument"]) != symbol for row in trade_rows)]
    checks.append(
        {
            "name": "unexpected_symbol_timeframe_gaps",
            "status": "warn" if zero_trade_symbols else "ok",
            "details": {"zero_trade_symbols": zero_trade_symbols},
        }
    )
    shadow_run_errors = []
    if int(source_summary.get("row_counts", {}).get("candidate_rows", 0)) == 0:
        shadow_run_errors.append("source_shadow_summary_has_zero_candidates")
    checks.append(
        {
            "name": "shadow_run_errors",
            "status": "warn" if shadow_run_errors else "ok",
            "details": {"errors": shadow_run_errors, "source_shadow_output_dir": str(shadow_output_dir)},
        }
    )
    overall_status = "healthy"
    if any(check["status"] == "fail" for check in checks):
        overall_status = "failed"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "warning"
    return {
        "module": "asia_drift_shadow_reporting",
        "overall_status": overall_status,
        "scope": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "shadow_output_dir": str(shadow_output_dir),
            "warehouse_root": str(warehouse_root),
        },
        "checks": checks,
    }


def _row_r_multiple(row: dict[str, str]) -> float:
    stop_points = float(row["stop_points"])
    return float(row["return_points"]) / stop_points if stop_points else 0.0


def _max_drawdown_from_returns(returns_r: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns_r:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _longest_losing_streak(returns_r: Sequence[float]) -> int:
    longest = 0
    current = 0
    for value in returns_r:
        if value < 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _business_dates(start_date: date, end_date: date) -> list[date]:
    payload: list[date] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            payload.append(current)
        current += timedelta(days=1)
    return payload


def _last_business_date(start_date: date, end_date: date) -> date:
    business_days = _business_dates(start_date, end_date)
    return business_days[-1] if business_days else end_date


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _render_daily_markdown(*, daily_rows: Sequence[dict[str, Any]], process_payload: dict[str, Any]) -> str:
    total_candidates = sum(int(row["candidate_count"]) for row in daily_rows)
    total_trades = sum(int(row["simulated_trade_count"]) for row in daily_rows)
    total_r = sum(float(row["daily_pl_r"]) for row in daily_rows)
    lines = [
        "# Asia Drift Shadow Daily Report",
        "",
        f"- Overall status: {process_payload['overall_status']}",
        f"- Total candidates: {total_candidates}",
        f"- Total simulated trades: {total_trades}",
        f"- Total R: {round(total_r, 4)}",
        "",
        "## Daily Sessions",
    ]
    for row in daily_rows[-10:]:
        lines.append(
            f"- {row['trade_date']}: candidates={row['candidate_count']} trades={row['simulated_trade_count']} "
            f"pl_points={row['daily_pl_points']} pl_r={row['daily_pl_r']} exits(stop/target/time)="
            f"{row['exit_stop_count']}/{row['exit_target_count']}/{row['exit_time_count']} notes={row['notes'] or 'none'}"
        )
    return "\n".join(lines) + "\n"


def _render_weekly_markdown(*, weekly_rows: Sequence[dict[str, Any]], rolling_rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# Asia Drift Shadow Weekly Report",
        "",
        "## Weekly Summary",
    ]
    for row in weekly_rows[-8:]:
        lines.append(
            f"- {row['week']}: sessions={row['session_count']} trades={row['trade_count']} "
            f"cum_r={row['cumulative_r']} avg_r_session={row['avg_r_per_session']} no_signal_days={row['no_signal_days']}"
        )
    lines.extend(["", "## Rolling Windows"])
    for row in rolling_rows:
        if row["instrument"] != "ALL":
            continue
        lines.append(
            f"- {row['window_label']}: trades={row['trade_count']} win_rate={row['win_rate']} "
            f"avg_r={row['avg_r']} cum_r={row['cumulative_r']} max_dd_r={row['max_drawdown_r']} "
            f"losing_streak={row['longest_losing_streak']}"
        )
    return "\n".join(lines) + "\n"
