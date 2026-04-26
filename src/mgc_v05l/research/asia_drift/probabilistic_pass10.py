"""Execution-framework design pass for favorable late-Asia Asia Drift states."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass1 import _coerce_ts, _last_value, _load_raw_outcome_series, _quantile, _signed_return, _try_connect_duckdb
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags
from .probabilistic_pass6 import _enrich_environment_row, _mean
from .probabilistic_pass7 import _load_state_lookup


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
NY = ZoneInfo("America/New_York")
CLUSTER_MAP = {
    "ES": "SPX_CLUSTER",
    "MES": "SPX_CLUSTER",
    "NQ": "NDX_CLUSTER",
    "MNQ": "NDX_CLUSTER",
}
HORIZONS = (
    ("15m", lambda ts: ts + timedelta(minutes=15)),
    ("30m", lambda ts: ts + timedelta(minutes=30)),
    ("60m", lambda ts: ts + timedelta(minutes=60)),
    ("120m", lambda ts: ts + timedelta(minutes=120)),
    ("240m", lambda ts: ts + timedelta(minutes=240)),
    ("rth_open", lambda ts: _checkpoint_on_trade_date(ts, time(9, 30))),
    ("session_end", lambda ts: _checkpoint_on_trade_date(ts, time(16, 0))),
)
STOP_TARGET_SCENARIOS = {
    "ES": ((4.0, 8.0), (6.0, 12.0)),
    "MES": ((4.0, 8.0), (6.0, 12.0)),
    "NQ": ((15.0, 30.0), (20.0, 40.0)),
    "MNQ": ((15.0, 30.0), (20.0, 40.0)),
}
MAX_HOLD_WINDOWS = (
    ("120m", 120),
    ("240m", 240),
    ("session_end", None),
)


def run_probabilistic_pass10(
    *,
    pass1_root: Path,
    warehouse_root: Path,
    pass6_classification_csv: Path,
    output_dir: Path,
) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    warehouse_root = warehouse_root.resolve()
    pass6_classification_csv = pass6_classification_csv.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "asia_drift_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "asia_drift_probabilistic_pass1_outcomes.csv")
    merged_rows = _with_stack_flags(_merge_rows(candidate_rows, outcome_rows))
    filtered_rows = [
        row
        for row in merged_rows
        if row["instrument"] in INDEX_SYMBOLS
        and row["primary_stack_only"]
        and row["timing_within_asia"] == "LATE_ASIA"
    ]

    rows_with_vix = attach_vix_asof(filtered_rows, vix_rows=load_vol_regime_rows(warehouse_root))
    enriched_rows = [_enrich_environment_row(row) for row in rows_with_vix]
    state_lookup = _load_state_lookup(pass6_classification_csv)
    favorable_rows = [
        {
            **row,
            "decision_state": state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL"),
            "cluster": CLUSTER_MAP[str(row["instrument"])],
        }
        for row in enriched_rows
        if state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL") == "TRADE_FAVORABLE"
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
                end_ts=datetime.fromisoformat("2026-04-22T16:00:00-04:00"),
            )
    finally:
        if connection is not None:
            connection.close()

    horizon_observations: list[dict[str, Any]] = []
    scenario_observations: list[dict[str, Any]] = []
    for row in favorable_rows:
        raw_series = raw_series_by_symbol[str(row["instrument"])]
        horizon_observations.extend(_build_horizon_observations(row, raw_series=raw_series))
        scenario_observations.extend(_build_scenario_observations(row, raw_series=raw_series))

    horizon_rows = _aggregate_horizon_rows(horizon_observations)
    stop_target_rows = _aggregate_stop_target_rows(scenario_observations)
    instrument_rows = _aggregate_instrument_rows(horizon_rows, stop_target_rows)

    horizon_csv = layout["reports"] / "asia_drift_probabilistic_pass10_horizon_comparison.csv"
    horizon_parquet = layout["reports"] / "asia_drift_probabilistic_pass10_horizon_comparison.parquet"
    stop_target_csv = layout["reports"] / "asia_drift_probabilistic_pass10_stop_target_analysis.csv"
    stop_target_parquet = layout["reports"] / "asia_drift_probabilistic_pass10_stop_target_analysis.parquet"
    instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass10_instrument_notes.csv"
    instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass10_instrument_notes.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass10_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass10_summary.md"

    for path, rows in [
        (horizon_csv, horizon_rows),
        (stop_target_csv, stop_target_rows),
        (instrument_csv, instrument_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (horizon_parquet, horizon_rows),
        (stop_target_parquet, stop_target_rows),
        (instrument_parquet, instrument_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass10",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "source_pass6_classification_csv": str(pass6_classification_csv),
        "assumptions": {
            "scope": "TRADE_FAVORABLE + LATE_ASIA only",
            "index_symbols": list(INDEX_SYMBOLS),
            "horizons": [label for label, _ in HORIZONS],
            "stop_target_scenarios": {
                "ES/MES": [{"stop_points": 4.0, "target_points": 8.0}, {"stop_points": 6.0, "target_points": 12.0}],
                "NQ/MNQ": [{"stop_points": 15.0, "target_points": 30.0}, {"stop_points": 20.0, "target_points": 40.0}],
            },
            "time_windows": [label for label, _ in MAX_HOLD_WINDOWS],
            "tie_break": "same-minute stop/target touch counts as stop first",
            "not_yet_live": True,
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "filtered_rows": len(filtered_rows),
            "favorable_rows": len(favorable_rows),
            "horizon_observations": len(horizon_observations),
            "scenario_observations": len(scenario_observations),
            "horizon_rows": len(horizon_rows),
            "stop_target_rows": len(stop_target_rows),
            "instrument_rows": len(instrument_rows),
        },
        "artifact_paths": {
            "horizon_csv": str(horizon_csv),
            "horizon_parquet": str(horizon_parquet),
            "stop_target_csv": str(stop_target_csv),
            "stop_target_parquet": str(stop_target_parquet),
            "instrument_csv": str(instrument_csv),
            "instrument_parquet": str(instrument_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(horizon_rows=horizon_rows, stop_target_rows=stop_target_rows, instrument_rows=instrument_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass10",
            "source_pass1_root": str(pass1_root),
            "warehouse_root": str(warehouse_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "favorable_rows": len(favorable_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _build_horizon_observations(row: dict[str, Any], *, raw_series: dict[str, Any]) -> list[dict[str, Any]]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(row["direction"])
    decision_ts = _coerce_ts(row["decision_ts"])
    entry_price = float(row["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    payload: list[dict[str, Any]] = []
    for horizon, resolver in HORIZONS:
        horizon_ts = resolver(decision_ts)
        if horizon_ts <= decision_ts:
            continue
        idx_end = bisect_right(timestamps, horizon_ts)
        close_value = _last_value(closes, idx_start, idx_end)
        window_ts = timestamps[idx_start:idx_end]
        window_highs = highs[idx_start:idx_end]
        window_lows = lows[idx_start:idx_end]
        favorable_values = [
            (float(high) - entry_price) if direction == "LONG" else (entry_price - float(low))
            for high, low in zip(window_highs, window_lows, strict=False)
        ]
        adverse_values = [
            (entry_price - float(low)) if direction == "LONG" else (float(high) - entry_price)
            for high, low in zip(window_highs, window_lows, strict=False)
        ]
        payload.append(
            {
                "sample_split": row["sample_split"],
                "instrument": row["instrument"],
                "cluster": row["cluster"],
                "horizon": horizon,
                "forward_return": round(_signed_return(direction, entry_price, close_value), 6) if close_value is not None else None,
                "positive_return": bool(close_value is not None and (_signed_return(direction, entry_price, close_value) or 0.0) > 0.0),
                "mfe_points": round(max(favorable_values), 6) if favorable_values else None,
                "mae_points": round(max(adverse_values), 6) if adverse_values else None,
                "time_to_peak_favorable_minute": _time_to_peak(window_ts, decision_ts, favorable_values),
                "time_to_peak_adverse_minute": _time_to_peak(window_ts, decision_ts, adverse_values),
            }
        )
    return payload


def _build_scenario_observations(row: dict[str, Any], *, raw_series: dict[str, Any]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for window_label, max_minutes in MAX_HOLD_WINDOWS:
        for stop_points, target_points in STOP_TARGET_SCENARIOS[str(row["instrument"])]:
            simulation = _simulate_stop_target(
                row=row,
                raw_series=raw_series,
                stop_points=stop_points,
                target_points=target_points,
                max_minutes=max_minutes,
            )
            payload.append(
                {
                    "sample_split": row["sample_split"],
                    "instrument": row["instrument"],
                    "cluster": row["cluster"],
                    "holding_window": window_label,
                    "stop_points": stop_points,
                    "target_points": target_points,
                    **simulation,
                }
            )
    return payload


def _simulate_stop_target(
    *,
    row: dict[str, Any],
    raw_series: dict[str, Any],
    stop_points: float,
    target_points: float,
    max_minutes: int | None,
) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(row["direction"])
    decision_ts = _coerce_ts(row["decision_ts"])
    entry_price = float(row["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    if max_minutes is None:
        cutoff_ts = _checkpoint_on_trade_date(decision_ts, time(16, 0))
    else:
        cutoff_ts = decision_ts + timedelta(minutes=max_minutes)
    idx_end = bisect_right(timestamps, cutoff_ts)

    for ts, high, low in zip(timestamps[idx_start:idx_end], highs[idx_start:idx_end], lows[idx_start:idx_end], strict=False):
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

    close_value = _last_value(closes, idx_start, idx_end)
    return {
        "scenario_return": round(_signed_return(direction, entry_price, close_value) or 0.0, 6),
        "exit_type": "TIME",
        "time_to_exit_minutes": int((cutoff_ts - decision_ts).total_seconds() // 60),
    }


def _aggregate_horizon_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["sample_split"]), str(row["instrument"]), str(row["horizon"]))].append(row)
    payload: list[dict[str, Any]] = []
    for key, bucket in sorted(grouped.items()):
        returns = [float(row["forward_return"]) for row in bucket if row["forward_return"] is not None]
        mfe_values = [float(row["mfe_points"]) for row in bucket if row["mfe_points"] is not None]
        mae_values = [float(row["mae_points"]) for row in bucket if row["mae_points"] is not None]
        peak_f = [int(row["time_to_peak_favorable_minute"]) for row in bucket if row["time_to_peak_favorable_minute"] is not None]
        peak_a = [int(row["time_to_peak_adverse_minute"]) for row in bucket if row["time_to_peak_adverse_minute"] is not None]
        wins = [value for value in returns if value > 0.0]
        losses = [value for value in returns if value <= 0.0]
        avg_win = _mean(wins)
        avg_loss = _mean(losses)
        payload.append(
            {
                "sample_split": key[0],
                "instrument": key[1],
                "cluster": CLUSTER_MAP[key[1]],
                "horizon": key[2],
                "row_count": len(returns),
                "win_rate": round(len(wins) / len(returns), 6) if returns else 0.0,
                "avg_return": round(_mean(returns), 6),
                "avg_win": round(avg_win, 6),
                "avg_loss": round(avg_loss, 6),
                "payoff_ratio": round(avg_win / abs(avg_loss), 6) if avg_loss < 0.0 else None,
                "avg_mfe": round(_mean(mfe_values), 6),
                "avg_mae": round(_mean(mae_values), 6),
                "mae_p75": round(_quantile(mae_values, 0.75), 6) if mae_values else 0.0,
                "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
                "time_to_peak_favorable_p50": round(_quantile(peak_f, 0.50), 6) if peak_f else None,
                "time_to_peak_adverse_p50": round(_quantile(peak_a, 0.50), 6) if peak_a else None,
            }
        )
    return payload


def _aggregate_stop_target_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, float, float], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row["sample_split"]),
                str(row["instrument"]),
                str(row["holding_window"]),
                float(row["stop_points"]),
                float(row["target_points"]),
            )
        ].append(row)
    payload: list[dict[str, Any]] = []
    for key, bucket in sorted(grouped.items()):
        returns = [float(row["scenario_return"]) for row in bucket]
        exit_minutes = [int(row["time_to_exit_minutes"]) for row in bucket if row["time_to_exit_minutes"] is not None]
        payload.append(
            {
                "sample_split": key[0],
                "instrument": key[1],
                "cluster": CLUSTER_MAP[key[1]],
                "holding_window": key[2],
                "stop_points": key[3],
                "target_points": key[4],
                "row_count": len(returns),
                "avg_scenario_return": round(_mean(returns), 6),
                "median_scenario_return": round(_quantile(returns, 0.50), 6) if returns else 0.0,
                "target_hit_rate": round(sum(1 for row in bucket if row["exit_type"] == "TARGET") / len(bucket), 6),
                "stop_hit_rate": round(sum(1 for row in bucket if row["exit_type"] == "STOP") / len(bucket), 6),
                "time_exit_rate": round(sum(1 for row in bucket if row["exit_type"] == "TIME") / len(bucket), 6),
                "time_to_exit_p50": round(_quantile(exit_minutes, 0.50), 6) if exit_minutes else None,
            }
        )
    return payload


def _aggregate_instrument_rows(
    horizon_rows: Sequence[dict[str, Any]],
    scenario_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for cluster in ("SPX_CLUSTER", "NDX_CLUSTER"):
            h60 = _find_horizon(horizon_rows, sample_split=sample_split, cluster=cluster, horizon="60m")
            h120 = _find_horizon(horizon_rows, sample_split=sample_split, cluster=cluster, horizon="120m")
            h240 = _find_horizon(horizon_rows, sample_split=sample_split, cluster=cluster, horizon="240m")
            hend = _find_horizon(horizon_rows, sample_split=sample_split, cluster=cluster, horizon="session_end")
            payload.append(
                {
                    "sample_split": sample_split,
                    "cluster": cluster,
                    "avg_return_60m": h60["avg_return"] if h60 else None,
                    "avg_return_120m": h120["avg_return"] if h120 else None,
                    "avg_return_240m": h240["avg_return"] if h240 else None,
                    "avg_return_session_end": hend["avg_return"] if hend else None,
                    "mae_p90_60m": h60["mae_p90"] if h60 else None,
                    "mae_p90_120m": h120["mae_p90"] if h120 else None,
                    "mae_p90_240m": h240["mae_p90"] if h240 else None,
                    "best_stop_target_120m": _best_scenario_label(scenario_rows, sample_split=sample_split, cluster=cluster, holding_window="120m"),
                    "best_stop_target_240m": _best_scenario_label(scenario_rows, sample_split=sample_split, cluster=cluster, holding_window="240m"),
                    "best_stop_target_session_end": _best_scenario_label(scenario_rows, sample_split=sample_split, cluster=cluster, holding_window="session_end"),
                }
            )
    return payload


def _find_horizon(rows: Sequence[dict[str, Any]], *, sample_split: str, cluster: str, horizon: str) -> dict[str, Any] | None:
    cluster_rows = [row for row in rows if row["sample_split"] == sample_split and row["cluster"] == cluster and row["horizon"] == horizon]
    if not cluster_rows:
        return None
    n = sum(int(row["row_count"]) for row in cluster_rows)
    if n == 0:
        return None
    def wavg(field: str) -> float:
        return sum(float(row[field]) * int(row["row_count"]) for row in cluster_rows) / n
    return {
        "avg_return": round(wavg("avg_return"), 6),
        "mae_p90": round(wavg("mae_p90"), 6),
    }


def _best_scenario_label(
    rows: Sequence[dict[str, Any]],
    *,
    sample_split: str,
    cluster: str,
    holding_window: str,
) -> str | None:
    bucket = [row for row in rows if row["sample_split"] == sample_split and row["cluster"] == cluster and row["holding_window"] == holding_window]
    if not bucket:
        return None
    best = max(bucket, key=lambda row: float(row["avg_scenario_return"]))
    return f"{int(float(best['stop_points']))}/{int(float(best['target_points']))}:{best['avg_scenario_return']}"


def _checkpoint_on_trade_date(decision_ts: datetime, checkpoint_time: time) -> datetime:
    trade_date = _stock_index_trade_date(decision_ts)
    return datetime.combine(trade_date, checkpoint_time, tzinfo=NY)


def _stock_index_trade_date(timestamp: datetime) -> datetime.date:
    local_ts = timestamp.astimezone(NY)
    if local_ts.timetz().replace(tzinfo=None) >= time(18, 0):
        return local_ts.date() + timedelta(days=1)
    return local_ts.date()


def _time_to_peak(timestamps: Sequence[datetime], decision_ts: datetime, values: Sequence[float]) -> int | None:
    if not values:
        return None
    target = max(values)
    for ts, value in zip(timestamps, values, strict=False):
        if value == target:
            return int((ts - decision_ts).total_seconds() // 60)
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
            writer.writerow(row)


def _render_markdown(
    *,
    horizon_rows: Sequence[dict[str, Any]],
    stop_target_rows: Sequence[dict[str, Any]],
    instrument_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 10",
        "",
        "Research-only execution-framework design for the favorable late-Asia state.",
        "",
        "## Holdout Horizon Read",
    ]
    for cluster in ("SPX_CLUSTER", "NDX_CLUSTER"):
        rows = [row for row in instrument_rows if row["sample_split"] == "holdout" and row["cluster"] == cluster]
        if not rows:
            continue
        row = rows[0]
        lines.append(
            f"- {cluster}: 60m={row['avg_return_60m']} 120m={row['avg_return_120m']} "
            f"240m={row['avg_return_240m']} end={row['avg_return_session_end']}"
        )
    lines.extend(["", "## Holdout Stop/Target Read"])
    for cluster in ("SPX_CLUSTER", "NDX_CLUSTER"):
        rows = [row for row in instrument_rows if row["sample_split"] == "holdout" and row["cluster"] == cluster]
        if not rows:
            continue
        row = rows[0]
        lines.append(
            f"- {cluster}: best120={row['best_stop_target_120m']} best240={row['best_stop_target_240m']} "
            f"best_end={row['best_stop_target_session_end']}"
        )
    lines.extend([
        "",
        "## Caveat",
        "",
        "This pass is research-only and not suitable for live deployment without separate validation.",
    ])
    return "\n".join(lines) + "\n"
