"""Extended-horizon execution behavior for Asia Drift decision states."""

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
STATE_ORDER = ("TRADE_FAVORABLE", "TRADE_NEUTRAL", "DO_NOT_TRADE")
TIMING_ORDER = ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON")
CLUSTER_MAP = {
    "ES": "SPX_CLUSTER",
    "MES": "SPX_CLUSTER",
    "NQ": "NDX_CLUSTER",
    "MNQ": "NDX_CLUSTER",
}
HORIZON_SEQUENCE = (
    ("15m", lambda decision_ts: decision_ts + timedelta(minutes=15)),
    ("30m", lambda decision_ts: decision_ts + timedelta(minutes=30)),
    ("60m", lambda decision_ts: decision_ts + timedelta(minutes=60)),
    ("120m", lambda decision_ts: decision_ts + timedelta(minutes=120)),
    ("240m", lambda decision_ts: decision_ts + timedelta(minutes=240)),
    ("london_open", lambda decision_ts: _checkpoint_on_trade_date(decision_ts, time(3, 0))),
    ("early_london", lambda decision_ts: _checkpoint_on_trade_date(decision_ts, time(5, 30))),
    ("rth_open", lambda decision_ts: _checkpoint_on_trade_date(decision_ts, time(9, 30))),
    ("session_end", lambda decision_ts: _checkpoint_on_trade_date(decision_ts, time(16, 0))),
)


def run_probabilistic_pass9(
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
        and row["timing_within_asia"] in set(TIMING_ORDER)
    ]

    rows_with_vix = attach_vix_asof(filtered_rows, vix_rows=load_vol_regime_rows(warehouse_root))
    enriched_rows = [_enrich_environment_row(row) for row in rows_with_vix]
    state_lookup = _load_state_lookup(pass6_classification_csv)

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
    for row in enriched_rows:
        decision_row = {
            **row,
            "decision_state": state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL"),
            "cluster": CLUSTER_MAP[str(row["instrument"])],
        }
        horizon_observations.extend(
            _build_horizon_observations(decision_row, raw_series=raw_series_by_symbol[str(row["instrument"])])
        )

    forward_return_rows = _aggregate_forward_return_rows(horizon_observations)
    win_rate_rows = _aggregate_win_rate_rows(horizon_observations)
    excursion_rows = _aggregate_excursion_rows(horizon_observations)
    peak_time_rows = _aggregate_peak_time_rows(horizon_observations)
    cluster_rows = _aggregate_cluster_rows(horizon_observations)

    paths_and_rows = [
        (
            layout["reports"] / "asia_drift_probabilistic_pass9_forward_returns.csv",
            layout["reports"] / "asia_drift_probabilistic_pass9_forward_returns.parquet",
            forward_return_rows,
        ),
        (
            layout["reports"] / "asia_drift_probabilistic_pass9_win_rates.csv",
            layout["reports"] / "asia_drift_probabilistic_pass9_win_rates.parquet",
            win_rate_rows,
        ),
        (
            layout["reports"] / "asia_drift_probabilistic_pass9_excursions.csv",
            layout["reports"] / "asia_drift_probabilistic_pass9_excursions.parquet",
            excursion_rows,
        ),
        (
            layout["reports"] / "asia_drift_probabilistic_pass9_peak_times.csv",
            layout["reports"] / "asia_drift_probabilistic_pass9_peak_times.parquet",
            peak_time_rows,
        ),
        (
            layout["reports"] / "asia_drift_probabilistic_pass9_cluster_comparison.csv",
            layout["reports"] / "asia_drift_probabilistic_pass9_cluster_comparison.parquet",
            cluster_rows,
        ),
    ]
    for csv_path, parquet_path, rows in paths_and_rows:
        _write_csv(csv_path, rows)
        materialize_parquet_dataset(parquet_path, rows)

    summary_json = layout["reports"] / "asia_drift_probabilistic_pass9_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass9_summary.md"
    summary_payload = {
        "module": "asia_drift_probabilistic_pass9",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "source_pass6_classification_csv": str(pass6_classification_csv),
        "assumptions": {
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "decision_states": list(STATE_ORDER),
            "timing_buckets": list(TIMING_ORDER),
            "horizons": [label for label, _ in HORIZON_SEQUENCE],
            "rth_open_checkpoint": "09:30 America/New_York on stock-index trade date",
            "session_end_checkpoint": "16:00 America/New_York on stock-index trade date",
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "filtered_rows": len(filtered_rows),
            "horizon_observations": len(horizon_observations),
            "forward_return_rows": len(forward_return_rows),
            "win_rate_rows": len(win_rate_rows),
            "excursion_rows": len(excursion_rows),
            "peak_time_rows": len(peak_time_rows),
            "cluster_rows": len(cluster_rows),
        },
        "artifact_paths": {
            "forward_returns_csv": str(paths_and_rows[0][0]),
            "forward_returns_parquet": str(paths_and_rows[0][1]),
            "win_rates_csv": str(paths_and_rows[1][0]),
            "win_rates_parquet": str(paths_and_rows[1][1]),
            "excursions_csv": str(paths_and_rows[2][0]),
            "excursions_parquet": str(paths_and_rows[2][1]),
            "peak_times_csv": str(paths_and_rows[3][0]),
            "peak_times_parquet": str(paths_and_rows[3][1]),
            "cluster_comparison_csv": str(paths_and_rows[4][0]),
            "cluster_comparison_parquet": str(paths_and_rows[4][1]),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(
            forward_return_rows=forward_return_rows,
            excursion_rows=excursion_rows,
            cluster_rows=cluster_rows,
        ),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass9",
            "source_pass1_root": str(pass1_root),
            "warehouse_root": str(warehouse_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "horizon_observations": len(horizon_observations),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _build_horizon_observations(
    row: dict[str, Any],
    *,
    raw_series: dict[str, Any],
) -> list[dict[str, Any]]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(row["direction"])
    decision_ts = _coerce_ts(row["decision_ts"])
    entry_price = float(row["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    observations: list[dict[str, Any]] = []

    for horizon, resolve_ts in HORIZON_SEQUENCE:
        horizon_ts = resolve_ts(decision_ts)
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
        metrics = _window_metrics(
            decision_ts=decision_ts,
            close_value=close_value,
            direction=direction,
            entry_price=entry_price,
            timestamps=window_ts,
            favorable_values=favorable_values,
            adverse_values=adverse_values,
        )
        observations.append(
            {
                "sample_split": row["sample_split"],
                "decision_state": row["decision_state"],
                "instrument": row["instrument"],
                "cluster": row["cluster"],
                "timing_within_asia": row["timing_within_asia"],
                "direction": direction,
                "horizon": horizon,
                "decision_ts": row["decision_ts"],
                "horizon_ts": horizon_ts.isoformat(),
                **metrics,
            }
        )
    return observations


def _window_metrics(
    *,
    decision_ts: datetime,
    close_value: float | None,
    direction: str,
    entry_price: float,
    timestamps: Sequence[datetime],
    favorable_values: Sequence[float],
    adverse_values: Sequence[float],
) -> dict[str, Any]:
    if close_value is None or not timestamps:
        return {
            "forward_return": None,
            "positive_return": None,
            "mfe_points": None,
            "mae_points": None,
            "time_to_peak_favorable_minute": None,
            "time_to_peak_adverse_minute": None,
            "time_to_first_profit_minute": None,
            "time_to_first_adverse_minute": None,
        }
    forward_return = _signed_return(direction, entry_price, close_value)
    time_to_first_profit = _time_to_first_positive(timestamps, decision_ts, favorable_values)
    time_to_first_adverse = _time_to_first_positive(timestamps, decision_ts, adverse_values)
    mfe_points = max(favorable_values)
    mae_points = max(adverse_values)
    time_to_peak_favorable = _time_to_peak(timestamps, decision_ts, favorable_values, prefer_max=True)
    time_to_peak_adverse = _time_to_peak(timestamps, decision_ts, adverse_values, prefer_max=True)
    return {
        "forward_return": round(forward_return, 6) if forward_return is not None else None,
        "positive_return": bool(forward_return is not None and forward_return > 0.0),
        "mfe_points": round(mfe_points, 6),
        "mae_points": round(mae_points, 6),
        "time_to_peak_favorable_minute": time_to_peak_favorable,
        "time_to_peak_adverse_minute": time_to_peak_adverse,
        "time_to_first_profit_minute": time_to_first_profit,
        "time_to_first_adverse_minute": time_to_first_adverse,
    }


def _aggregate_forward_return_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in _group(rows, ("sample_split", "decision_state", "instrument", "timing_within_asia", "horizon")).items():
        returns = [float(row["forward_return"]) for row in bucket if row["forward_return"] is not None]
        payload.append(
            {
                "sample_split": key[0],
                "decision_state": key[1],
                "instrument": key[2],
                "timing_within_asia": key[3],
                "horizon": key[4],
                "row_count": len(returns),
                "avg_return": round(_mean(returns), 6),
                "median_return": round(_quantile(returns, 0.50), 6) if returns else 0.0,
                "return_p10": round(_quantile(returns, 0.10), 6) if returns else 0.0,
                "return_p25": round(_quantile(returns, 0.25), 6) if returns else 0.0,
                "return_p75": round(_quantile(returns, 0.75), 6) if returns else 0.0,
                "return_p90": round(_quantile(returns, 0.90), 6) if returns else 0.0,
            }
        )
    return payload


def _aggregate_win_rate_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in _group(rows, ("sample_split", "decision_state", "instrument", "timing_within_asia", "horizon")).items():
        available = [row for row in bucket if row["positive_return"] is not None]
        wins = [row for row in available if row["positive_return"]]
        payload.append(
            {
                "sample_split": key[0],
                "decision_state": key[1],
                "instrument": key[2],
                "timing_within_asia": key[3],
                "horizon": key[4],
                "row_count": len(available),
                "win_rate": round(len(wins) / len(available), 6) if available else 0.0,
            }
        )
    return payload


def _aggregate_excursion_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in _group(rows, ("sample_split", "decision_state", "instrument", "timing_within_asia", "horizon")).items():
        mfe_values = [float(row["mfe_points"]) for row in bucket if row["mfe_points"] is not None]
        mae_values = [float(row["mae_points"]) for row in bucket if row["mae_points"] is not None]
        payload.append(
            {
                "sample_split": key[0],
                "decision_state": key[1],
                "instrument": key[2],
                "timing_within_asia": key[3],
                "horizon": key[4],
                "row_count": len(mfe_values),
                "avg_mfe": round(_mean(mfe_values), 6),
                "mfe_p50": round(_quantile(mfe_values, 0.50), 6) if mfe_values else 0.0,
                "mfe_p75": round(_quantile(mfe_values, 0.75), 6) if mfe_values else 0.0,
                "mfe_p90": round(_quantile(mfe_values, 0.90), 6) if mfe_values else 0.0,
                "avg_mae": round(_mean(mae_values), 6),
                "mae_p50": round(_quantile(mae_values, 0.50), 6) if mae_values else 0.0,
                "mae_p75": round(_quantile(mae_values, 0.75), 6) if mae_values else 0.0,
                "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
            }
        )
    return payload


def _aggregate_peak_time_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in _group(rows, ("sample_split", "decision_state", "instrument", "timing_within_asia", "horizon")).items():
        peak_favorable = [int(row["time_to_peak_favorable_minute"]) for row in bucket if row["time_to_peak_favorable_minute"] is not None]
        peak_adverse = [int(row["time_to_peak_adverse_minute"]) for row in bucket if row["time_to_peak_adverse_minute"] is not None]
        first_profit = [int(row["time_to_first_profit_minute"]) for row in bucket if row["time_to_first_profit_minute"] is not None]
        first_adverse = [int(row["time_to_first_adverse_minute"]) for row in bucket if row["time_to_first_adverse_minute"] is not None]
        payload.append(
            {
                "sample_split": key[0],
                "decision_state": key[1],
                "instrument": key[2],
                "timing_within_asia": key[3],
                "horizon": key[4],
                "row_count": len(bucket),
                "time_to_peak_favorable_p25": round(_quantile(peak_favorable, 0.25), 6) if peak_favorable else None,
                "time_to_peak_favorable_p50": round(_quantile(peak_favorable, 0.50), 6) if peak_favorable else None,
                "time_to_peak_favorable_p75": round(_quantile(peak_favorable, 0.75), 6) if peak_favorable else None,
                "time_to_peak_adverse_p25": round(_quantile(peak_adverse, 0.25), 6) if peak_adverse else None,
                "time_to_peak_adverse_p50": round(_quantile(peak_adverse, 0.50), 6) if peak_adverse else None,
                "time_to_peak_adverse_p75": round(_quantile(peak_adverse, 0.75), 6) if peak_adverse else None,
                "time_to_first_profit_p50": round(_quantile(first_profit, 0.50), 6) if first_profit else None,
                "time_to_first_adverse_p50": round(_quantile(first_adverse, 0.50), 6) if first_adverse else None,
            }
        )
    return payload


def _aggregate_cluster_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for key, bucket in _group(rows, ("sample_split", "decision_state", "cluster", "timing_within_asia", "horizon")).items():
        returns = [float(row["forward_return"]) for row in bucket if row["forward_return"] is not None]
        mfe_values = [float(row["mfe_points"]) for row in bucket if row["mfe_points"] is not None]
        mae_values = [float(row["mae_points"]) for row in bucket if row["mae_points"] is not None]
        wins = [value for value in returns if value > 0.0]
        payload.append(
            {
                "sample_split": key[0],
                "decision_state": key[1],
                "cluster": key[2],
                "timing_within_asia": key[3],
                "horizon": key[4],
                "row_count": len(returns),
                "win_rate": round(len(wins) / len(returns), 6) if returns else 0.0,
                "avg_return": round(_mean(returns), 6),
                "avg_mfe": round(_mean(mfe_values), 6),
                "avg_mae": round(_mean(mae_values), 6),
                "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
            }
        )
    return payload


def _group(rows: Sequence[dict[str, Any]], keys: Sequence[str]) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row[key] for key in keys)].append(row)
    return dict(sorted(grouped.items()))


def _checkpoint_on_trade_date(decision_ts: datetime, checkpoint_time: time) -> datetime:
    trade_date = _stock_index_trade_date(decision_ts)
    return datetime.combine(trade_date, checkpoint_time, tzinfo=NY)


def _stock_index_trade_date(timestamp: datetime) -> datetime.date:
    local_ts = timestamp.astimezone(NY)
    if local_ts.timetz().replace(tzinfo=None) >= time(18, 0):
        return local_ts.date() + timedelta(days=1)
    return local_ts.date()


def _time_to_peak(
    timestamps: Sequence[datetime],
    decision_ts: datetime,
    values: Sequence[float],
    *,
    prefer_max: bool,
) -> int | None:
    if not values:
        return None
    target = max(values) if prefer_max else min(values)
    for ts, value in zip(timestamps, values, strict=False):
        if value == target:
            return int((ts - decision_ts).total_seconds() // 60)
    return None


def _time_to_first_positive(
    timestamps: Sequence[datetime],
    decision_ts: datetime,
    values: Sequence[float],
) -> int | None:
    for ts, value in zip(timestamps, values, strict=False):
        if value > 0.0:
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
    forward_return_rows: Sequence[dict[str, Any]],
    excursion_rows: Sequence[dict[str, Any]],
    cluster_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 9",
        "",
        "Extended-horizon behavior for Pass 7 decision states.",
        "",
        "## Holdout State Progression",
    ]
    for decision_state in STATE_ORDER:
        rows = [
            row for row in forward_return_rows
            if row["sample_split"] == "holdout"
            and row["decision_state"] == decision_state
            and row["instrument"] == "NQ"
            and row["timing_within_asia"] == "LATE_ASIA"
            and row["horizon"] in {"60m", "120m", "240m", "session_end"}
        ]
        rows = sorted(rows, key=lambda row: _horizon_sort_key(str(row["horizon"])))
        if not rows:
            continue
        snippets = ", ".join(f"{row['horizon']}={row['avg_return']}" for row in rows)
        lines.append(f"- {decision_state} NQ late Asia: {snippets}")
    lines.extend(["", "## Holdout Cluster Comparison"])
    for cluster in ("SPX_CLUSTER", "NDX_CLUSTER"):
        rows = [
            row for row in cluster_rows
            if row["sample_split"] == "holdout"
            and row["decision_state"] == "TRADE_FAVORABLE"
            and row["cluster"] == cluster
            and row["timing_within_asia"] == "LATE_ASIA"
            and row["horizon"] in {"60m", "120m", "240m", "session_end"}
        ]
        rows = sorted(rows, key=lambda row: _horizon_sort_key(str(row["horizon"])))
        if not rows:
            continue
        snippets = ", ".join(f"{row['horizon']}={row['avg_return']}" for row in rows)
        lines.append(f"- {cluster}: {snippets}")
    lines.extend(["", "## Holdout Risk Notes"])
    for decision_state in STATE_ORDER:
        rows = [
            row for row in excursion_rows
            if row["sample_split"] == "holdout"
            and row["decision_state"] == decision_state
            and row["instrument"] == "ES"
            and row["timing_within_asia"] == "LATE_ASIA"
            and row["horizon"] == "240m"
        ]
        if not rows:
            continue
        row = rows[0]
        lines.append(
            f"- {decision_state} ES late Asia 240m: avg_mfe={row['avg_mfe']} "
            f"avg_mae={row['avg_mae']} mae_p90={row['mae_p90']}"
        )
    return "\n".join(lines) + "\n"


def _horizon_sort_key(horizon: str) -> int:
    order = {
        "15m": 0,
        "30m": 1,
        "60m": 2,
        "120m": 3,
        "240m": 4,
        "london_open": 5,
        "early_london": 6,
        "rth_open": 7,
        "session_end": 8,
    }
    return order.get(horizon, 99)
