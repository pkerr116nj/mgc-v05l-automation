"""Decision-structure analysis for the Asia Drift continuation evidence stack."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from ...app.session_phase_labels import label_session_phase
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass1 import _coerce_ts, _last_value, _load_raw_outcome_series, _quantile, _signed_return, _try_connect_duckdb
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
DO_NOT_TRUST_MIN_ROWS = 100
HORIZON_MINUTES = {
    "15m": 15,
    "30m": 30,
    "60m": 60,
}
STOP_TARGET_SCENARIOS = {
    "ES": ((4.0, 4.0), (4.0, 8.0)),
    "MES": ((4.0, 4.0), (4.0, 8.0)),
    "NQ": ((15.0, 15.0), (15.0, 30.0)),
    "MNQ": ((15.0, 15.0), (15.0, 30.0)),
}


def run_probabilistic_pass4(
    *,
    pass1_root: Path,
    warehouse_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    warehouse_root = warehouse_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "asia_drift_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "asia_drift_probabilistic_pass1_outcomes.csv")
    merged_rows = _with_stack_flags(_merge_rows(candidate_rows, outcome_rows))
    filtered_rows = [
        row
        for row in merged_rows
        if row["instrument"] in INDEX_SYMBOLS and row["primary_stack_only"] and row["timing_within_asia"] in {"EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"}
    ]

    connection = _try_connect_duckdb()
    raw_series_by_symbol: dict[str, dict[str, Any]] = {}
    try:
        for symbol in INDEX_SYMBOLS:
            raw_series_by_symbol[symbol] = _load_raw_outcome_series(
                connection=connection,
                warehouse_root=warehouse_root,
                symbol=symbol,
                start_ts=datetime.fromisoformat("2020-01-01T18:00:00-05:00"),
                end_ts=datetime.fromisoformat("2026-04-21T23:59:00-04:00"),
            )
    finally:
        if connection is not None:
            connection.close()

    enriched_rows = [_enrich_row(row, raw_series_by_symbol[str(row["instrument"])]) for row in filtered_rows]

    expectancy_rows = _build_expectancy_rows(enriched_rows)
    payoff_rows = _build_payoff_rows(enriched_rows)
    curve_rows = _build_curve_rows(enriched_rows)
    time_rows = _build_time_distribution_rows(enriched_rows)
    holding_rows = _build_holding_window_rows(expectancy_rows)
    scenario_rows = _build_scenario_rows(enriched_rows, raw_series_by_symbol)

    expectancy_csv = layout["reports"] / "asia_drift_probabilistic_pass4_expectancy.csv"
    expectancy_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_expectancy.parquet"
    payoff_csv = layout["reports"] / "asia_drift_probabilistic_pass4_payoff_profile.csv"
    payoff_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_payoff_profile.parquet"
    curve_csv = layout["reports"] / "asia_drift_probabilistic_pass4_mae_mfe_curves.csv"
    curve_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_mae_mfe_curves.parquet"
    time_csv = layout["reports"] / "asia_drift_probabilistic_pass4_time_distributions.csv"
    time_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_time_distributions.parquet"
    holding_csv = layout["reports"] / "asia_drift_probabilistic_pass4_minimum_holding_window.csv"
    holding_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_minimum_holding_window.parquet"
    scenario_csv = layout["reports"] / "asia_drift_probabilistic_pass4_stop_target_analysis.csv"
    scenario_parquet = layout["reports"] / "asia_drift_probabilistic_pass4_stop_target_analysis.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass4_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass4_summary.md"

    for path, rows in [
        (expectancy_csv, expectancy_rows),
        (payoff_csv, payoff_rows),
        (curve_csv, curve_rows),
        (time_csv, time_rows),
        (holding_csv, holding_rows),
        (scenario_csv, scenario_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (expectancy_parquet, expectancy_rows),
        (payoff_parquet, payoff_rows),
        (curve_parquet, curve_rows),
        (time_parquet, time_rows),
        (holding_parquet, holding_rows),
        (scenario_parquet, scenario_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass4",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "assumptions": {
            "do_not_trust_min_rows": DO_NOT_TRUST_MIN_ROWS,
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "stop_target_tie_break": "same_minute_dual_touch_counts_as_stop_first",
            "scenarios": {
                "ES/MES": [{"stop_points": 4.0, "target_points": 4.0}, {"stop_points": 4.0, "target_points": 8.0}],
                "NQ/MNQ": [{"stop_points": 15.0, "target_points": 15.0}, {"stop_points": 15.0, "target_points": 30.0}],
            },
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "filtered_index_stack_rows": len(enriched_rows),
        },
        "artifact_paths": {
            "expectancy_csv": str(expectancy_csv),
            "expectancy_parquet": str(expectancy_parquet),
            "payoff_csv": str(payoff_csv),
            "payoff_parquet": str(payoff_parquet),
            "curve_csv": str(curve_csv),
            "curve_parquet": str(curve_parquet),
            "time_csv": str(time_csv),
            "time_parquet": str(time_parquet),
            "holding_csv": str(holding_csv),
            "holding_parquet": str(holding_parquet),
            "scenario_csv": str(scenario_csv),
            "scenario_parquet": str(scenario_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": {
            "holdout_index_early_avg_return_60m": _find_expectancy(expectancy_rows, "holdout", "INDEX_POOLED", "EARLY_ASIA", "60m"),
            "holdout_index_late_avg_return_60m": _find_expectancy(expectancy_rows, "holdout", "INDEX_POOLED", "LATE_ASIA", "60m"),
            "holdout_index_pre_london_avg_return_60m": _find_expectancy(expectancy_rows, "holdout", "INDEX_POOLED", "PRE_LONDON", "60m"),
            "holdout_index_late_payoff_ratio_60m": _find_payoff_ratio(payoff_rows, "holdout", "INDEX_POOLED", "LATE_ASIA"),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(_render_markdown(summary_payload, expectancy_rows, payoff_rows, holding_rows, scenario_rows), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass4",
            "source_pass1_root": str(pass1_root),
            "warehouse_root": str(warehouse_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "filtered_index_stack_rows": len(enriched_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [_coerce_row(row) for row in reader]


def _enrich_row(row: dict[str, Any], raw_series: dict[str, Any]) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(row["direction"])
    decision_ts = _coerce_ts(row["decision_ts"])
    entry_price = float(row["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    idx_30 = bisect_right(timestamps, decision_ts + timedelta(minutes=30))
    idx_60 = bisect_right(timestamps, decision_ts + timedelta(minutes=60))

    forward_return_30m = _signed_return(direction, entry_price, _last_value(closes, idx_start, idx_30))

    window_ts = timestamps[idx_start:idx_60]
    window_highs = highs[idx_start:idx_60]
    window_lows = lows[idx_start:idx_60]
    favorable_values = [
        (float(high) - entry_price) if direction == "LONG" else (entry_price - float(low))
        for high, low in zip(window_highs, window_lows, strict=False)
    ]
    adverse_values = [
        (entry_price - float(low)) if direction == "LONG" else (float(high) - entry_price)
        for high, low in zip(window_highs, window_lows, strict=False)
    ]
    time_to_first_adverse_minute = _time_to_first_positive(window_ts, decision_ts, adverse_values)

    return {
        **row,
        "forward_return_30m": round(forward_return_30m, 6) if forward_return_30m is not None else None,
        "time_to_first_adverse_minute": time_to_first_adverse_minute,
    }


def _build_expectancy_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            for focus in ("INDEX_POOLED", *INDEX_SYMBOLS):
                bucket = _filter_focus_rows(rows, sample_split=sample_split, timing=timing, focus=focus)
                if not bucket:
                    continue
                for horizon, field in (
                    ("15m", "forward_return_15m"),
                    ("30m", "forward_return_30m"),
                    ("60m", "forward_return_60m"),
                    ("session_end", "session_end_return"),
                ):
                    values = [float(row[field]) for row in bucket if row[field] is not None]
                    payload.append(
                        {
                            "sample_split": sample_split,
                            "focus": focus,
                            "timing_within_asia": timing,
                            "horizon": horizon,
                            "row_count": len(values),
                            "positive_return_probability": round(sum(1 for value in values if value > 0.0) / len(values), 6) if values else 0.0,
                            "avg_return": round(_mean(values), 6),
                            "median_return": round(_quantile(values, 0.50), 6) if values else 0.0,
                            "return_p10": round(_quantile(values, 0.10), 6) if values else 0.0,
                            "return_p25": round(_quantile(values, 0.25), 6) if values else 0.0,
                            "return_p75": round(_quantile(values, 0.75), 6) if values else 0.0,
                            "return_p90": round(_quantile(values, 0.90), 6) if values else 0.0,
                            "do_not_trust": len(values) < DO_NOT_TRUST_MIN_ROWS,
                        }
                    )
    return payload


def _build_payoff_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            for focus in ("INDEX_POOLED", *INDEX_SYMBOLS):
                bucket = _filter_focus_rows(rows, sample_split=sample_split, timing=timing, focus=focus)
                if not bucket:
                    continue
                returns = [float(row["forward_return_60m"]) for row in bucket if row["forward_return_60m"] is not None]
                wins = [value for value in returns if value > 0.0]
                losses = [value for value in returns if value <= 0.0]
                avg_win = _mean(wins)
                avg_loss = _mean(losses)
                avg_loss_abs = abs(avg_loss)
                p10 = _quantile(returns, 0.10) if returns else 0.0
                p90 = _quantile(returns, 0.90) if returns else 0.0
                payload.append(
                    {
                        "sample_split": sample_split,
                        "focus": focus,
                        "timing_within_asia": timing,
                        "row_count": len(returns),
                        "win_rate_60m": round(sum(1 for value in returns if value > 0.0) / len(returns), 6) if returns else 0.0,
                        "avg_win_60m": round(avg_win, 6),
                        "avg_loss_60m": round(avg_loss, 6),
                        "payoff_ratio": round(avg_win / avg_loss_abs, 6) if avg_loss_abs > 0.0 else None,
                        "expectancy_60m": round(_mean(returns), 6),
                        "return_skew_p90_over_p10": round(p90 / abs(p10), 6) if p10 < 0.0 else None,
                        "loss_tail_p10": round(p10, 6),
                        "loss_tail_p25": round(_quantile(returns, 0.25), 6) if returns else 0.0,
                        "do_not_trust": len(returns) < DO_NOT_TRUST_MIN_ROWS,
                    }
                )
    return payload


def _build_curve_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            for focus in ("INDEX_POOLED", "ES", "NQ", "MES", "MNQ"):
                bucket = _filter_focus_rows(rows, sample_split=sample_split, timing=timing, focus=focus)
                if not bucket:
                    continue
                for metric_name, field in (("MFE_60M", "mfe_60m_points"), ("MAE_60M", "mae_60m_points")):
                    values = [float(row[field]) for row in bucket]
                    for quantile in (0.10, 0.25, 0.50, 0.75, 0.90):
                        payload.append(
                            {
                                "sample_split": sample_split,
                                "focus": focus,
                                "timing_within_asia": timing,
                                "metric": metric_name,
                                "quantile": quantile,
                                "row_count": len(values),
                                "value": round(_quantile(values, quantile), 6),
                                "do_not_trust": len(values) < DO_NOT_TRUST_MIN_ROWS,
                            }
                        )
    return payload


def _build_time_distribution_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            for focus in ("INDEX_POOLED", "ES", "NQ", "MES", "MNQ"):
                bucket = _filter_focus_rows(rows, sample_split=sample_split, timing=timing, focus=focus)
                if not bucket:
                    continue
                time_to_mfe = [int(row["time_to_mfe_minutes"]) for row in bucket if row["time_to_mfe_minutes"] is not None]
                time_to_mae = [int(row["time_to_mae_minutes"]) for row in bucket if row["time_to_mae_minutes"] is not None]
                time_to_first_adverse = [int(row["time_to_first_adverse_minute"]) for row in bucket if row["time_to_first_adverse_minute"] is not None]
                resolution_counts = {
                    "favorable_first_rate": sum(1 for row in bucket if row["resolution_proxy"] == "FAVORABLE_FIRST") / len(bucket),
                    "adverse_first_rate": sum(1 for row in bucket if row["resolution_proxy"] == "ADVERSE_FIRST") / len(bucket),
                    "tie_rate": sum(1 for row in bucket if row["resolution_proxy"] == "TIE") / len(bucket),
                }
                payload.append(
                    {
                        "sample_split": sample_split,
                        "focus": focus,
                        "timing_within_asia": timing,
                        "row_count": len(bucket),
                        **{key: round(value, 6) for key, value in resolution_counts.items()},
                        "time_to_mfe_p25": round(_quantile(time_to_mfe, 0.25), 6) if time_to_mfe else None,
                        "time_to_mfe_p50": round(_quantile(time_to_mfe, 0.50), 6) if time_to_mfe else None,
                        "time_to_mfe_p75": round(_quantile(time_to_mfe, 0.75), 6) if time_to_mfe else None,
                        "time_to_mae_p25": round(_quantile(time_to_mae, 0.25), 6) if time_to_mae else None,
                        "time_to_mae_p50": round(_quantile(time_to_mae, 0.50), 6) if time_to_mae else None,
                        "time_to_mae_p75": round(_quantile(time_to_mae, 0.75), 6) if time_to_mae else None,
                        "time_to_first_adverse_p25": round(_quantile(time_to_first_adverse, 0.25), 6) if time_to_first_adverse else None,
                        "time_to_first_adverse_p50": round(_quantile(time_to_first_adverse, 0.50), 6) if time_to_first_adverse else None,
                        "time_to_first_adverse_p75": round(_quantile(time_to_first_adverse, 0.75), 6) if time_to_first_adverse else None,
                        "do_not_trust": len(bucket) < DO_NOT_TRUST_MIN_ROWS,
                    }
                )
    return payload


def _build_holding_window_rows(expectancy_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in expectancy_rows:
        if row["horizon"] == "session_end":
            continue
        grouped.setdefault((str(row["sample_split"]), str(row["focus"]), str(row["timing_within_asia"])), []).append(row)
    horizon_order = {"15m": 0, "30m": 1, "60m": 2}
    for key in sorted(grouped):
        bucket = sorted(grouped[key], key=lambda item: horizon_order[str(item["horizon"])])
        viable = next(
            (
                str(row["horizon"])
                for row in bucket
                if float(row["avg_return"]) > 0.0 and float(row["positive_return_probability"]) >= 0.5
            ),
            "NONE_BY_60M",
        )
        payload.append(
            {
                "sample_split": key[0],
                "focus": key[1],
                "timing_within_asia": key[2],
                "minimum_viable_holding_window": viable,
                "row_count": int(bucket[0]["row_count"]) if bucket else 0,
                "do_not_trust": bool(bucket[0]["do_not_trust"]) if bucket else True,
            }
        )
    return payload


def _build_scenario_rows(rows: Sequence[dict[str, Any]], raw_series_by_symbol: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    instrument_rows: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            for instrument in INDEX_SYMBOLS:
                bucket = _filter_focus_rows(rows, sample_split=sample_split, timing=timing, focus=instrument)
                if not bucket:
                    continue
                for stop_points, target_points in STOP_TARGET_SCENARIOS[instrument]:
                    scenario_values = [
                        _simulate_stop_target(
                            row=row,
                            raw_series=raw_series_by_symbol[instrument],
                            stop_points=stop_points,
                            target_points=target_points,
                        )
                        for row in bucket
                    ]
                    returns = [float(item["scenario_return"]) for item in scenario_values]
                    exit_minutes = [int(item["time_to_exit_minutes"]) for item in scenario_values if item["time_to_exit_minutes"] is not None]
                    instrument_rows.append(
                        {
                            "sample_split": sample_split,
                            "focus": instrument,
                            "timing_within_asia": timing,
                            "stop_points": stop_points,
                            "target_points": target_points,
                            "row_count": len(returns),
                            "avg_scenario_return": round(_mean(returns), 6),
                            "median_scenario_return": round(_quantile(returns, 0.50), 6) if returns else 0.0,
                            "target_hit_rate": round(sum(1 for item in scenario_values if item["exit_type"] == "TARGET") / len(scenario_values), 6),
                            "stop_hit_rate": round(sum(1 for item in scenario_values if item["exit_type"] == "STOP") / len(scenario_values), 6),
                            "close_exit_rate": round(sum(1 for item in scenario_values if item["exit_type"] == "TIME") / len(scenario_values), 6),
                            "time_to_exit_p50": round(_quantile(exit_minutes, 0.50), 6) if exit_minutes else None,
                            "do_not_trust": len(returns) < DO_NOT_TRUST_MIN_ROWS,
                        }
                    )
    return instrument_rows


def _simulate_stop_target(
    *,
    row: dict[str, Any],
    raw_series: dict[str, Any],
    stop_points: float,
    target_points: float,
) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(row["direction"])
    decision_ts = _coerce_ts(row["decision_ts"])
    entry_price = float(row["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    idx_60 = bisect_right(timestamps, decision_ts + timedelta(minutes=60))

    for ts, high, low in zip(timestamps[idx_start:idx_60], highs[idx_start:idx_60], lows[idx_start:idx_60], strict=False):
        favorable = (float(high) - entry_price) if direction == "LONG" else (entry_price - float(low))
        adverse = (entry_price - float(low)) if direction == "LONG" else (float(high) - entry_price)
        if adverse >= stop_points:
            return {
                "scenario_return": -stop_points,
                "exit_type": "STOP",
                "time_to_exit_minutes": int((ts - decision_ts).total_seconds() // 60),
            }
        if favorable >= target_points:
            return {
                "scenario_return": target_points,
                "exit_type": "TARGET",
                "time_to_exit_minutes": int((ts - decision_ts).total_seconds() // 60),
            }

    close_value = _last_value(closes, idx_start, idx_60)
    return {
        "scenario_return": _signed_return(direction, entry_price, close_value) or 0.0,
        "exit_type": "TIME",
        "time_to_exit_minutes": 60,
    }


def _filter_focus_rows(
    rows: Sequence[dict[str, Any]],
    *,
    sample_split: str,
    timing: str,
    focus: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row["sample_split"] == sample_split
        and row["timing_within_asia"] == timing
        and (focus == "INDEX_POOLED" or row["instrument"] == focus)
    ]


def _time_to_first_positive(
    timestamps: Sequence[datetime],
    decision_ts: datetime,
    values: Sequence[float],
) -> int | None:
    for ts, value in zip(timestamps, values, strict=False):
        if value > 0.0:
            return int((ts - decision_ts).total_seconds() // 60)
    return None


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _find_expectancy(rows: Sequence[dict[str, Any]], sample_split: str, focus: str, timing: str, horizon: str) -> float | None:
    for row in rows:
        if row["sample_split"] == sample_split and row["focus"] == focus and row["timing_within_asia"] == timing and row["horizon"] == horizon:
            return float(row["avg_return"])
    return None


def _find_payoff_ratio(rows: Sequence[dict[str, Any]], sample_split: str, focus: str, timing: str) -> float | None:
    for row in rows:
        if row["sample_split"] == sample_split and row["focus"] == focus and row["timing_within_asia"] == timing:
            value = row["payoff_ratio"]
            return None if value is None else float(value)
    return None


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
            writer.writerow(_json_ready(row))


def _json_ready(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        else:
            payload[key] = value
    return payload


def _render_markdown(
    summary_payload: dict[str, Any],
    expectancy_rows: Sequence[dict[str, Any]],
    payoff_rows: Sequence[dict[str, Any]],
    holding_rows: Sequence[dict[str, Any]],
    scenario_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 4",
        "",
        "Index-only decision-structure analysis for the fixed evidence stack:",
        "- 60m agreement",
        "- cross-asset confirmation",
        "- NOT_COMPRESSED",
        "",
        "Headline holdout 60m expectancy:",
    ]
    for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
        row = next(
            (
                item
                for item in expectancy_rows
                if item["sample_split"] == "holdout" and item["focus"] == "INDEX_POOLED" and item["timing_within_asia"] == timing and item["horizon"] == "60m"
            ),
            None,
        )
        if row is None:
            continue
        lines.append(
            f"- {timing}: avg60={row['avg_return']} ppos={row['positive_return_probability']} rows={row['row_count']}"
        )
    lines.extend(["", "Minimum viable holding windows (holdout):"])
    for row in holding_rows:
        if row["sample_split"] == "holdout" and row["focus"] == "INDEX_POOLED":
            lines.append(f"- {row['timing_within_asia']}: {row['minimum_viable_holding_window']}")
    lines.extend(["", "Illustrative stop/target what-if (holdout, NQ and ES):"])
    for row in scenario_rows:
        if row["sample_split"] == "holdout" and row["focus"] in {"ES", "NQ"}:
            lines.append(
                f"- {row['focus']} {row['timing_within_asia']} stop={row['stop_points']} target={row['target_points']} avg={row['avg_scenario_return']}"
            )
    lines.extend(["", f"Summary JSON: `{summary_payload['artifact_paths']['summary_json']}`"])
    return "\n".join(lines) + "\n"
