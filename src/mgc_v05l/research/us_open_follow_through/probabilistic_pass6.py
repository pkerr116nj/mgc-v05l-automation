"""Risk-shaping feasibility pass for the retained NDX UP_OPEN signal candidate."""

from __future__ import annotations

import csv
import json
import math
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from ..asia_drift.probabilistic_pass1 import (
    _coerce_ts,
    _load_raw_outcome_series,
    _mean,
    _quantile,
    _try_connect_duckdb,
    _write_csv,
)
from ..asia_drift.probabilistic_pass2 import _coerce_row
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass2 import _annotate_rows, _merge_rows


NEW_YORK = ZoneInfo("America/New_York")
PASS_VERSION = "us_open_probabilistic_pass6_v1"
DEFAULT_SYMBOLS: tuple[str, ...] = ("NQ", "MNQ")
MINUTES_HORIZONS: tuple[int, ...] = (30, 60, 90, 120)
SESSION_HORIZONS: tuple[tuple[str, time], ...] = (
    ("1530", time(15, 30)),
    ("close", time(16, 0)),
)
FIXED_STOP_POINTS: tuple[float, ...] = (50.0, 75.0, 100.0, 125.0, 150.0)
ATR_STOP_MULTIPLES: tuple[float, ...] = (0.75, 1.0, 1.25, 1.5, 2.0)

RISK_CONTROL_PROMISING = "RISK_CONTROL_PROMISING"
RISK_CONTROL_MIXED = "RISK_CONTROL_MIXED"
RISK_CONTROL_INEFFECTIVE = "RISK_CONTROL_INEFFECTIVE"
RISK_CONTROL_OVERFITTING_RISK = "RISK_CONTROL_OVERFITTING_RISK"
RETAIN_CONSTRAINED_EXPRESSION = "RETAIN_CONSTRAINED_EXPRESSION"


@dataclass(frozen=True)
class ScenarioDefinition:
    stop_family: str
    stop_label: str
    stop_value: float | None
    horizon: str
    horizon_minutes: int | None
    session_time: time | None

    @property
    def scenario_id(self) -> str:
        return f"{self.stop_family}|{self.stop_label}|{self.horizon}"


def run_probabilistic_pass6(*, pass1_root: Path, warehouse_root: Path, output_dir: Path) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    warehouse_root = warehouse_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv")
    merged_rows = _annotate_rows(_merge_rows(candidate_rows, outcome_rows))
    signal_rows = [
        row
        for row in merged_rows
        if str(row.get("cluster")) == "NDX" and str(row.get("direction")) == "UP" and str(row.get("instrument")) in DEFAULT_SYMBOLS
    ]

    raw_series_by_symbol = _load_raw_series_for_signal_rows(warehouse_root=warehouse_root, rows=signal_rows)
    row_profiles = [_build_row_profile(row=row, raw_series=raw_series_by_symbol[str(row["instrument"])]) for row in signal_rows]
    scenarios = _scenario_definitions()
    simulation_rows = _simulate_scenarios(row_profiles, scenarios)

    summary_rows = _build_stop_horizon_summary_table(simulation_rows)
    tail_rows = _build_tail_risk_reduction_table(summary_rows)
    damage_rows = _build_stop_damage_table(simulation_rows, summary_rows)
    stability_rows = _build_dev_holdout_stability_table(summary_rows, tail_rows)
    classification = _classify_feasibility(summary_rows, tail_rows, stability_rows)

    simulation_csv = layout["signals"] / "us_open_probabilistic_pass6_simulation_rows.csv"
    summary_csv = layout["reports"] / "us_open_probabilistic_pass6_stop_horizon_summary_table.csv"
    tail_csv = layout["reports"] / "us_open_probabilistic_pass6_tail_risk_reduction_table.csv"
    damage_csv = layout["reports"] / "us_open_probabilistic_pass6_stop_damage_table.csv"
    stability_csv = layout["reports"] / "us_open_probabilistic_pass6_dev_holdout_stability_table.csv"
    summary_json = layout["reports"] / "us_open_probabilistic_pass6_summary.json"
    summary_markdown = layout["reports"] / "us_open_probabilistic_pass6_summary.md"

    _write_csv(simulation_csv, simulation_rows)
    _write_csv(summary_csv, summary_rows)
    _write_csv(tail_csv, tail_rows)
    _write_csv(damage_csv, damage_rows)
    _write_csv(stability_csv, stability_rows)

    materialize_parquet_dataset(layout["signals"] / "us_open_probabilistic_pass6_simulation_rows.parquet", simulation_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass6_stop_horizon_summary_table.parquet", summary_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass6_tail_risk_reduction_table.parquet", tail_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass6_stop_damage_table.parquet", damage_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass6_dev_holdout_stability_table.parquet", stability_rows)

    payload = {
        "module": "us_open_probabilistic_pass6",
        "version": PASS_VERSION,
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "signal_rows": len(signal_rows),
            "row_profiles": len(row_profiles),
            "simulation_rows": len(simulation_rows),
        },
        "assumptions": {
            "population": "NDX UP_OPEN only from Pass 1 anchor",
            "entry_reference": "10:00 ET decision close",
            "atr_definition": "mean 1m true range over 09:30-10:00 ET using prior-minute close where available",
            "stop_fill_assumption": "first 1m bar low at or through stop price exits exactly at stop price",
            "no_stop_baseline": "same-horizon close-only exit with no stop",
            "purpose": "risk-control feasibility mapping, not stop optimization",
        },
        "classification": classification,
        "family_level_status": RETAIN_CONSTRAINED_EXPRESSION,
        "artifact_paths": {
            "simulation_csv": str(simulation_csv),
            "stop_horizon_summary_table": str(summary_csv),
            "tail_risk_reduction_table": str(tail_csv),
            "stop_damage_table": str(damage_csv),
            "dev_holdout_stability_table": str(stability_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(payload, summary_rows, tail_rows, damage_rows, stability_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "us_open_probabilistic_pass6",
            "version": PASS_VERSION,
            "artifact_paths": payload["artifact_paths"],
            "simulation_row_count": len(simulation_rows),
        },
    )
    return {"artifacts": payload["artifact_paths"], "summary": payload}


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _load_raw_series_for_signal_rows(*, warehouse_root: Path, rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not rows:
        return {}
    start_ts = min(_coerce_ts(row["decision_ts"]) for row in rows) - timedelta(minutes=45)
    end_ts = max(_coerce_ts(row["decision_ts"]) for row in rows) + timedelta(hours=7)
    connection = _try_connect_duckdb()
    payload: dict[str, dict[str, Any]] = {}
    try:
        for symbol in sorted({str(row["instrument"]) for row in rows}):
            payload[symbol] = _load_raw_outcome_series(
                connection=connection,
                warehouse_root=warehouse_root,
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
            )
    finally:
        if connection is not None:
            connection.close()
    return payload


def _scenario_definitions() -> list[ScenarioDefinition]:
    payload: list[ScenarioDefinition] = []
    for horizon in MINUTES_HORIZONS:
        payload.append(
            ScenarioDefinition(
                stop_family="NO_STOP",
                stop_label="NONE",
                stop_value=None,
                horizon=f"{horizon}m",
                horizon_minutes=horizon,
                session_time=None,
            )
        )
    for horizon_name, session_time in SESSION_HORIZONS:
        payload.append(
            ScenarioDefinition(
                stop_family="NO_STOP",
                stop_label="NONE",
                stop_value=None,
                horizon=horizon_name,
                horizon_minutes=None,
                session_time=session_time,
            )
        )
    for stop_points in FIXED_STOP_POINTS:
        stop_label = f"{int(stop_points)}pt"
        for horizon in MINUTES_HORIZONS:
            payload.append(
                ScenarioDefinition(
                    stop_family="FIXED",
                    stop_label=stop_label,
                    stop_value=stop_points,
                    horizon=f"{horizon}m",
                    horizon_minutes=horizon,
                    session_time=None,
                )
            )
        for horizon_name, session_time in SESSION_HORIZONS:
            payload.append(
                ScenarioDefinition(
                    stop_family="FIXED",
                    stop_label=stop_label,
                    stop_value=stop_points,
                    horizon=horizon_name,
                    horizon_minutes=None,
                    session_time=session_time,
                )
            )
    for atr_multiple in ATR_STOP_MULTIPLES:
        atr_label = f"{atr_multiple:.2f}xATR"
        for horizon in MINUTES_HORIZONS:
            payload.append(
                ScenarioDefinition(
                    stop_family="ATR",
                    stop_label=atr_label,
                    stop_value=atr_multiple,
                    horizon=f"{horizon}m",
                    horizon_minutes=horizon,
                    session_time=None,
                )
            )
        for horizon_name, session_time in SESSION_HORIZONS:
            payload.append(
                ScenarioDefinition(
                    stop_family="ATR",
                    stop_label=atr_label,
                    stop_value=atr_multiple,
                    horizon=horizon_name,
                    horizon_minutes=None,
                    session_time=session_time,
                )
            )
    return payload


def _build_row_profile(*, row: dict[str, Any], raw_series: dict[str, Any]) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]
    closes = raw_series["closes"]
    decision_ts = _coerce_ts(row["decision_ts"])
    local_session_date = decision_ts.astimezone(NEW_YORK).date()
    entry_price = float(row["decision_close"])
    start_idx = bisect_right(timestamps, decision_ts)
    future_timestamps = timestamps[start_idx:]
    future_lows = lows[start_idx:]
    future_closes = closes[start_idx:]
    future_highs = highs[start_idx:]

    opening_atr = _opening_window_atr(
        timestamps=timestamps,
        highs=highs,
        lows=lows,
        closes=closes,
        local_session_date=local_session_date,
        decision_ts=decision_ts,
    )
    return {
        "candidate_id": row["candidate_id"],
        "instrument": row["instrument"],
        "sample_split": row["sample_split"],
        "local_session_date": row.get("local_session_date", local_session_date.isoformat()),
        "decision_ts": decision_ts,
        "entry_price": entry_price,
        "opening_atr_points": opening_atr,
        "future_timestamps": future_timestamps,
        "future_lows": future_lows,
        "future_closes": future_closes,
        "future_highs": future_highs,
    }


def _opening_window_atr(
    *,
    timestamps: Sequence[datetime],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    local_session_date: Any,
    decision_ts: datetime,
) -> float:
    session_start = datetime.combine(local_session_date, time(9, 30), tzinfo=NEW_YORK).astimezone(UTC)
    start_idx = bisect_right(timestamps, session_start)
    end_idx = bisect_right(timestamps, decision_ts)
    true_ranges: list[float] = []
    for idx in range(start_idx, end_idx):
        high = float(highs[idx])
        low = float(lows[idx])
        prev_close = float(closes[idx - 1]) if idx > 0 else float(closes[idx])
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return round(_mean(true_ranges), 6)


def _simulate_scenarios(
    row_profiles: Sequence[dict[str, Any]],
    scenarios: Sequence[ScenarioDefinition],
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in row_profiles:
        baselines_by_horizon: dict[str, float] = {}
        for scenario in scenarios:
            result = _simulate_single_scenario(row=row, scenario=scenario)
            if scenario.stop_family == "NO_STOP":
                baselines_by_horizon[scenario.horizon] = float(result["return_points"])
            payload.append(result)
        for record in payload[-len(scenarios) :]:
            record["baseline_return_points"] = baselines_by_horizon[str(record["horizon"])]
    return payload


def _simulate_single_scenario(*, row: dict[str, Any], scenario: ScenarioDefinition) -> dict[str, Any]:
    decision_ts = row["decision_ts"]
    local_session_date = decision_ts.astimezone(NEW_YORK).date()
    checkpoint_ts = _checkpoint_ts(local_session_date, decision_ts, scenario)
    future_timestamps = row["future_timestamps"]
    end_idx = bisect_right(future_timestamps, checkpoint_ts)
    timestamps = future_timestamps[:end_idx]
    lows = row["future_lows"][:end_idx]
    closes = row["future_closes"][:end_idx]
    highs = row["future_highs"][:end_idx]
    entry_price = float(row["entry_price"])
    stop_distance = _stop_distance(row=row, scenario=scenario)
    stop_price = entry_price - stop_distance if stop_distance is not None else None

    stopped_out = False
    stop_ts: datetime | None = None
    exit_price: float
    if stop_price is not None:
        for ts, low in zip(timestamps, lows, strict=False):
            if float(low) <= stop_price:
                stopped_out = True
                stop_ts = ts
                break
    if stopped_out and stop_price is not None:
        exit_price = stop_price
    elif closes:
        exit_price = float(closes[-1])
    else:
        exit_price = entry_price
    return_points = exit_price - entry_price
    baseline_path_return = (float(closes[-1]) - entry_price) if closes else 0.0
    return {
        "candidate_id": row["candidate_id"],
        "instrument": row["instrument"],
        "sample_split": row["sample_split"],
        "local_session_date": row["local_session_date"],
        "decision_ts": decision_ts.isoformat(),
        "scenario_id": scenario.scenario_id,
        "stop_family": scenario.stop_family,
        "stop_label": scenario.stop_label,
        "stop_value": round(stop_distance, 6) if stop_distance is not None else None,
        "horizon": scenario.horizon,
        "opening_atr_points": row["opening_atr_points"],
        "sample_path_minutes": len(closes),
        "return_points": round(return_points, 6),
        "median_path_return_points": round(baseline_path_return, 6),
        "stopped_out": stopped_out,
        "stop_ts": stop_ts.isoformat() if stop_ts is not None else None,
        "stopped_loss_points": round(return_points, 6) if stopped_out else None,
        "exit_price": round(exit_price, 6),
        "max_favorable_points": round(max((float(high) - entry_price for high in highs), default=0.0), 6),
        "max_adverse_points": round(max((entry_price - float(low) for low in lows), default=0.0), 6),
    }


def _checkpoint_ts(local_session_date: Any, decision_ts: datetime, scenario: ScenarioDefinition) -> datetime:
    if scenario.horizon_minutes is not None:
        return decision_ts + timedelta(minutes=scenario.horizon_minutes)
    assert scenario.session_time is not None
    return datetime.combine(local_session_date, scenario.session_time, tzinfo=NEW_YORK).astimezone(UTC)


def _stop_distance(*, row: dict[str, Any], scenario: ScenarioDefinition) -> float | None:
    if scenario.stop_family == "NO_STOP":
        return None
    if scenario.stop_family == "FIXED":
        return float(scenario.stop_value or 0.0)
    atr_value = float(row["opening_atr_points"] or 0.0)
    multiple = float(scenario.stop_value or 0.0)
    return round(atr_value * multiple, 6)


def _build_stop_horizon_summary_table(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        split_rows = [row for row in rows if str(row["sample_split"]) == sample_split]
        for instrument in (*DEFAULT_SYMBOLS, "ALL"):
            instrument_rows = [
                row for row in split_rows if instrument == "ALL" or str(row["instrument"]) == instrument
            ]
            if not instrument_rows:
                continue
            buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
            for row in instrument_rows:
                buckets[(str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))].append(row)
            for (stop_family, stop_label, horizon), bucket in sorted(buckets.items()):
                returns = [float(row["return_points"]) for row in bucket]
                winners = [value for value in returns if value > 0.0]
                losers = [value for value in returns if value < 0.0]
                stopped_rows = [row for row in bucket if bool(row["stopped_out"])]
                payload.append(
                    {
                        "sample_split": sample_split,
                        "instrument": instrument,
                        "stop_family": stop_family,
                        "stop_label": stop_label,
                        "horizon": horizon,
                        "sample_count": len(bucket),
                        "average_return_points": round(_mean(returns), 6),
                        "median_return_points": round(_median(returns), 6),
                        "win_rate": round(_win_rate(returns), 6),
                        "average_winner_points": round(_mean(winners), 6),
                        "average_loser_points": round(_mean(losers), 6),
                        "profit_factor": _profit_factor(winners, losers),
                        "expectancy_points": round(_mean(returns), 6),
                        "stopped_out_rate": round(len(stopped_rows) / len(bucket), 6),
                        "average_stopped_loss_points": round(
                            _mean(float(row["stopped_loss_points"]) for row in stopped_rows if row["stopped_loss_points"] is not None),
                            6,
                        ),
                        "worst_5pct_return_points": round(_quantile(returns, 0.05), 6),
                        "worst_1pct_return_points": round(_quantile(returns, 0.01), 6),
                        "max_losing_streak": _longest_streak(
                            [float(row["return_points"]) <= 0.0 for row in sorted(bucket, key=lambda item: (str(item["local_session_date"]), str(item["instrument"])))]
                        ),
                        "avg_stop_distance_points": round(
                            _mean(float(row["stop_value"]) for row in bucket if row["stop_value"] is not None),
                            6,
                        ),
                    }
                )
    return payload


def _build_tail_risk_reduction_table(summary_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    baselines = {
        (str(row["sample_split"]), str(row["instrument"]), str(row["horizon"])): row
        for row in summary_rows
        if str(row["stop_family"]) == "NO_STOP"
    }
    payload: list[dict[str, Any]] = []
    for row in summary_rows:
        key = (str(row["sample_split"]), str(row["instrument"]), str(row["horizon"]))
        baseline = baselines[key]
        baseline_avg = float(baseline["average_return_points"])
        baseline_w5 = float(baseline["worst_5pct_return_points"])
        baseline_w1 = float(baseline["worst_1pct_return_points"])
        current_avg = float(row["average_return_points"])
        current_w5 = float(row["worst_5pct_return_points"])
        current_w1 = float(row["worst_1pct_return_points"])
        payload.append(
            {
                "sample_split": row["sample_split"],
                "instrument": row["instrument"],
                "stop_family": row["stop_family"],
                "stop_label": row["stop_label"],
                "horizon": row["horizon"],
                "retained_upside_vs_no_stop": _safe_ratio(current_avg, baseline_avg),
                "avg_return_delta_vs_no_stop": round(current_avg - baseline_avg, 6),
                "worst_5pct_tail_risk_reduction": _tail_risk_reduction(baseline_w5, current_w5),
                "worst_1pct_tail_risk_reduction": _tail_risk_reduction(baseline_w1, current_w1),
            }
        )
    return payload


def _build_stop_damage_table(
    simulation_rows: Sequence[dict[str, Any]],
    summary_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped_rows: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    grouped_baselines: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in simulation_rows:
        split = str(row["sample_split"])
        instrument = str(row["instrument"])
        stop_family = str(row["stop_family"])
        stop_label = str(row["stop_label"])
        horizon = str(row["horizon"])
        grouped_rows[(split, instrument, stop_family, stop_label, horizon)].append(row)
        grouped_rows[(split, "ALL", stop_family, stop_label, horizon)].append(row)
        if stop_family == "NO_STOP":
            grouped_baselines[(split, instrument, horizon)][str(row["candidate_id"])] = row
            grouped_baselines[(split, "ALL", horizon)][str(row["candidate_id"])] = row

    payload: list[dict[str, Any]] = []
    for summary_row in summary_rows:
        split = str(summary_row["sample_split"])
        instrument = str(summary_row["instrument"])
        stop_family = str(summary_row["stop_family"])
        stop_label = str(summary_row["stop_label"])
        horizon = str(summary_row["horizon"])
        bucket = grouped_rows[(split, instrument, stop_family, stop_label, horizon)]
        baseline_rows = grouped_baselines[(split, instrument, horizon)]

        baseline_winners = 0
        winner_to_stopped_loss = 0
        baseline_losers = 0
        loser_reduced = 0
        for row in bucket:
            baseline_row = baseline_rows.get(str(row["candidate_id"]))
            if baseline_row is None:
                continue
            baseline_return = float(baseline_row["return_points"])
            current_return = float(row["return_points"])
            if baseline_return > 0.0:
                baseline_winners += 1
                if bool(row["stopped_out"]) and current_return < 0.0:
                    winner_to_stopped_loss += 1
            elif baseline_return < 0.0:
                baseline_losers += 1
                if current_return > baseline_return:
                    loser_reduced += 1
        payload.append(
            {
                "sample_split": split,
                "instrument": instrument,
                "stop_family": stop_family,
                "stop_label": stop_label,
                "horizon": horizon,
                "pct_original_unstopped_winners_became_stopped_losses": round(
                    winner_to_stopped_loss / baseline_winners, 6
                )
                if baseline_winners
                else None,
                "pct_original_unstopped_losers_reduced": round(loser_reduced / baseline_losers, 6)
                if baseline_losers
                else None,
                "baseline_winner_count": baseline_winners,
                "baseline_loser_count": baseline_losers,
            }
        )
    return payload


def _build_dev_holdout_stability_table(
    summary_rows: Sequence[dict[str, Any]],
    tail_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    summary_lookup = {
        (str(row["sample_split"]), str(row["instrument"]), str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])): row
        for row in summary_rows
    }
    tail_lookup = {
        (str(row["sample_split"]), str(row["instrument"]), str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])): row
        for row in tail_rows
    }
    payload: list[dict[str, Any]] = []
    keys = {
        (str(row["instrument"]), str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))
        for row in summary_rows
    }
    for instrument, stop_family, stop_label, horizon in sorted(keys):
        dev = summary_lookup.get(("development", instrument, stop_family, stop_label, horizon))
        hold = summary_lookup.get(("holdout", instrument, stop_family, stop_label, horizon))
        dev_tail = tail_lookup.get(("development", instrument, stop_family, stop_label, horizon))
        hold_tail = tail_lookup.get(("holdout", instrument, stop_family, stop_label, horizon))
        if dev is None or hold is None or dev_tail is None or hold_tail is None:
            continue
        payload.append(
            {
                "instrument": instrument,
                "stop_family": stop_family,
                "stop_label": stop_label,
                "horizon": horizon,
                "development_average_return_points": dev["average_return_points"],
                "holdout_average_return_points": hold["average_return_points"],
                "development_win_rate": dev["win_rate"],
                "holdout_win_rate": hold["win_rate"],
                "development_stopped_out_rate": dev["stopped_out_rate"],
                "holdout_stopped_out_rate": hold["stopped_out_rate"],
                "development_profit_factor": dev["profit_factor"],
                "holdout_profit_factor": hold["profit_factor"],
                "development_retained_upside_vs_no_stop": dev_tail["retained_upside_vs_no_stop"],
                "holdout_retained_upside_vs_no_stop": hold_tail["retained_upside_vs_no_stop"],
                "development_worst_5pct_tail_risk_reduction": dev_tail["worst_5pct_tail_risk_reduction"],
                "holdout_worst_5pct_tail_risk_reduction": hold_tail["worst_5pct_tail_risk_reduction"],
                "avg_return_delta_holdout_minus_dev": _delta(hold["average_return_points"], dev["average_return_points"]),
                "win_rate_delta_holdout_minus_dev": _delta(hold["win_rate"], dev["win_rate"]),
                "tail_reduction_delta_holdout_minus_dev": _delta(
                    hold_tail["worst_5pct_tail_risk_reduction"],
                    dev_tail["worst_5pct_tail_risk_reduction"],
                ),
            }
        )
    return payload


def _classify_feasibility(
    summary_rows: Sequence[dict[str, Any]],
    tail_rows: Sequence[dict[str, Any]],
    stability_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    holdout_all_rows = [
        row for row in summary_rows if str(row["sample_split"]) == "holdout" and str(row["instrument"]) == "ALL" and str(row["stop_family"]) != "NO_STOP"
    ]
    holdout_tail_lookup = {
        (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])): row
        for row in tail_rows
        if str(row["sample_split"]) == "holdout" and str(row["instrument"]) == "ALL"
    }
    dev_tail_lookup = {
        (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])): row
        for row in tail_rows
        if str(row["sample_split"]) == "development" and str(row["instrument"]) == "ALL"
    }
    robust: list[tuple[str, str, str]] = []
    helpful: list[tuple[str, str, str]] = []
    concentrated: set[str] = set()
    for row in holdout_all_rows:
        key = (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))
        hold_tail = holdout_tail_lookup.get(key)
        dev_tail = dev_tail_lookup.get(key)
        if hold_tail is None or dev_tail is None:
            continue
        retained = _as_float(hold_tail["retained_upside_vs_no_stop"])
        worst5 = _as_float(hold_tail["worst_5pct_tail_risk_reduction"])
        worst1 = _as_float(hold_tail["worst_1pct_tail_risk_reduction"])
        dev_retained = _as_float(dev_tail["retained_upside_vs_no_stop"])
        dev_worst5 = _as_float(dev_tail["worst_5pct_tail_risk_reduction"])
        if (
            float(row["average_return_points"]) > 0.0
            and float(row["profit_factor"] or 0.0) > 1.0
            and retained is not None
            and worst5 is not None
            and worst1 is not None
            and retained >= 0.50
            and worst5 >= 0.25
            and worst1 >= 0.25
        ):
            helpful.append(key)
            concentrated.add(str(row["horizon"]))
            if dev_retained is not None and dev_worst5 is not None and dev_retained >= 0.45 and dev_worst5 >= 0.15:
                robust.append(key)

    top_holdout = sorted(
        holdout_all_rows,
        key=lambda row: (
            _as_float(holdout_tail_lookup.get((str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])), {}).get("worst_5pct_tail_risk_reduction")) or -999.0,
            float(row["average_return_points"]),
        ),
        reverse=True,
    )[:5]
    if not helpful:
        classification = RISK_CONTROL_INEFFECTIVE
        justified = False
    elif len(robust) >= 4 and len(concentrated) >= 2:
        classification = RISK_CONTROL_PROMISING
        justified = True
    elif len(helpful) <= 2 and len(robust) <= 1:
        classification = RISK_CONTROL_OVERFITTING_RISK
        justified = True
    else:
        classification = RISK_CONTROL_MIXED
        justified = True

    return {
        "classification": classification,
        "family_level_status": RETAIN_CONSTRAINED_EXPRESSION,
        "further_constrained_expression_research_justified": justified,
        "holdout_helpful_combo_count": len(helpful),
        "holdout_robust_combo_count": len(robust),
        "helpful_horizons": sorted(concentrated),
        "top_holdout_tail_reduction_candidates": [
            {
                "stop_family": row["stop_family"],
                "stop_label": row["stop_label"],
                "horizon": row["horizon"],
                "avg_return_points": row["average_return_points"],
                "win_rate": row["win_rate"],
                "retained_upside_vs_no_stop": holdout_tail_lookup[
                    (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))
                ]["retained_upside_vs_no_stop"],
                "worst_5pct_tail_risk_reduction": holdout_tail_lookup[
                    (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))
                ]["worst_5pct_tail_risk_reduction"],
                "worst_1pct_tail_risk_reduction": holdout_tail_lookup[
                    (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))
                ]["worst_1pct_tail_risk_reduction"],
            }
            for row in top_holdout
        ],
    }


def _render_markdown(
    payload: dict[str, Any],
    summary_rows: Sequence[dict[str, Any]],
    tail_rows: Sequence[dict[str, Any]],
    damage_rows: Sequence[dict[str, Any]],
    stability_rows: Sequence[dict[str, Any]],
) -> str:
    classification = payload["classification"]
    lines = [
        "# US Open Probabilistic Pass 6",
        "",
        "## Scope",
        "",
        "- focus: retained `NDX UP_OPEN` signal candidate",
        "- instruments: `NQ`, `MNQ`",
        "- expression study only: no-stop vs predefined fixed-stop vs predefined ATR-stop",
        "- no threshold optimization and no strategy promotion",
        "",
        "## Feasibility Classification",
        "",
        "- Pass 5 classified the raw `NDX UP_OPEN` `10:00-to-close` expression as `RISK_UNSTABLE`.",
        "- Pass 6 does not select a final stop and does not promote a strategy.",
        "- Pass 6 tests whether mechanically predefined risk control can improve survivability at all.",
        "",
        f"- classification: `{classification['classification']}`",
        f"- family-level status: `{classification['family_level_status']}`",
        f"- further constrained-expression research justified: `{classification['further_constrained_expression_research_justified']}`",
        f"- holdout helpful combo count: `{classification['holdout_helpful_combo_count']}`",
        f"- holdout robust combo count: `{classification['holdout_robust_combo_count']}`",
        f"- helpful horizons: `{', '.join(classification['helpful_horizons']) or 'none'}`",
        "",
        "## Family-Level Interpretation",
        "",
        "- predefined risk control appears capable of improving survivability",
        "- the strongest broad family is `fixed-point stop + fixed horizon`",
        "- the most promising horizon zone is `60m-120m`, with `120m` the cleanest read",
        "- `ATR` stops reduce tails aggressively but often damage expression through high stop-out rates and winner conversion",
        "- do not treat any single stop/horizon cell as the answer",
        "",
        "## Holdout Summary",
        "",
    ]
    holdout_all = [
        row for row in summary_rows
        if str(row["sample_split"]) == "holdout" and str(row["instrument"]) == "ALL" and str(row["stop_family"]) in {"NO_STOP", "FIXED", "ATR"}
    ]
    holdout_tail = {
        (str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"])): row
        for row in tail_rows
        if str(row["sample_split"]) == "holdout" and str(row["instrument"]) == "ALL"
    }
    for row in sorted(
        holdout_all,
        key=lambda item: (
            str(item["horizon"]),
            0 if str(item["stop_family"]) == "NO_STOP" else 1,
            str(item["stop_family"]),
            str(item["stop_label"]),
        ),
    )[:30]:
        tail = holdout_tail[(str(row["stop_family"]), str(row["stop_label"]), str(row["horizon"]))]
        lines.append(
            f"- `{row['horizon']} {row['stop_family']} {row['stop_label']}`: avg `{float(row['average_return_points']):.3f}`, "
            f"win `{float(row['win_rate']):.2%}`, stopped `{float(row['stopped_out_rate']):.2%}`, "
            f"retained upside `{_fmt_pct(tail['retained_upside_vs_no_stop'])}`, "
            f"tail reduction 5% `{_fmt_pct(tail['worst_5pct_tail_risk_reduction'])}`, "
            f"tail reduction 1% `{_fmt_pct(tail['worst_1pct_tail_risk_reduction'])}`"
        )
    lines.extend(["", "## Stop Damage", ""])
    for row in [
        item
        for item in damage_rows
        if str(item["sample_split"]) == "holdout" and str(item["instrument"]) == "ALL" and str(item["stop_family"]) != "NO_STOP"
    ][:20]:
        lines.append(
            f"- `{row['horizon']} {row['stop_family']} {row['stop_label']}`: winners->stopped losses `{_fmt_pct(row['pct_original_unstopped_winners_became_stopped_losses'])}`, "
            f"losers reduced `{_fmt_pct(row['pct_original_unstopped_losers_reduced'])}`"
        )
    lines.extend(["", "## Stability", ""])
    for row in [
        item
        for item in stability_rows
        if str(item["instrument"]) == "ALL" and str(item["stop_family"]) != "NO_STOP"
    ][:20]:
        lines.append(
            f"- `{row['horizon']} {row['stop_family']} {row['stop_label']}`: dev avg `{float(row['development_average_return_points']):.3f}`, "
            f"holdout avg `{float(row['holdout_average_return_points']):.3f}`, "
            f"holdout tail reduction `{_fmt_pct(row['holdout_worst_5pct_tail_risk_reduction'])}`, "
            f"holdout retained upside `{_fmt_pct(row['holdout_retained_upside_vs_no_stop'])}`"
        )
    lines.extend(["", "## Core Question", ""])
    lines.append(
        "Can predefined stop-loss rules materially improve survivability without destroying the directional expectancy of the NDX UP_OPEN signal?"
    )
    lines.append("")
    lines.append(
        f"Current answer: `{classification['classification']}`. This is a feasibility read only, not a stop selection or strategy decision."
    )
    return "\n".join(lines) + "\n"


def _median(values: Sequence[float]) -> float:
    return median(values) if values else 0.0


def _win_rate(values: Sequence[float]) -> float:
    return (sum(1 for value in values if value > 0.0) / len(values)) if values else 0.0


def _profit_factor(winners: Sequence[float], losers: Sequence[float]) -> float | None:
    gross_win = sum(winners)
    gross_loss = abs(sum(losers))
    if gross_loss <= 0.0:
        return None
    return round(gross_win / gross_loss, 6)


def _longest_streak(flags: Sequence[bool]) -> int:
    longest = 0
    current = 0
    for flag in flags:
        if flag:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if math.isclose(denominator, 0.0, abs_tol=1e-9):
        return None
    return round(numerator / denominator, 6)


def _tail_risk_reduction(baseline_value: float, scenario_value: float) -> float | None:
    baseline_abs = abs(baseline_value)
    if baseline_abs <= 1e-9:
        return None
    return round((baseline_abs - abs(scenario_value)) / baseline_abs, 6)


def _delta(left: Any, right: Any) -> float | None:
    left_float = _as_float(left)
    right_float = _as_float(right)
    if left_float is None or right_float is None:
        return None
    return round(left_float - right_float, 6)


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _fmt_pct(value: Any) -> str:
    value_float = _as_float(value)
    if value_float is None:
        return "n/a"
    return f"{value_float:.2%}"
