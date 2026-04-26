"""Shadow-trading validation harness for Asia Drift continuation."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .features import RECOVERY_CONFIRMED, build_feature_rows
from .probabilistic_pass1 import (
    _build_bar_slope_lookup,
    _build_candidate_row,
    _build_cross_asset_confirmation_map,
    _build_daily_regime_lookup,
    _coerce_ts,
    _flatten_candidate_rows,
    _last_value,
    _load_raw_outcome_series,
    _load_warehouse_bars,
    _quantile,
    _signed_return,
    _try_connect_duckdb,
)
from .probabilistic_pass2 import _timing_bucket
from .probabilistic_pass10 import _checkpoint_on_trade_date
from .probabilistic_pass6 import _enrich_environment_row, _mean
from .probabilistic_pass7 import _load_state_lookup


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
STOP_TARGET = {
    "ES": (6.0, 12.0),
    "MES": (6.0, 12.0),
    "NQ": (20.0, 40.0),
    "MNQ": (20.0, 40.0),
}
TIME_STOP_MINUTES = 120
WINDOW_START = datetime.fromisoformat("2020-01-01T18:00:00-05:00")
WINDOW_END = datetime.fromisoformat("2026-04-21T23:59:00-04:00")
ROLLING_TRADE_WINDOW = 20


def run_asia_drift_shadow_trading(
    *,
    warehouse_root: Path,
    pass6_classification_csv: Path,
    output_dir: Path,
    start_ts: datetime = WINDOW_START,
    end_ts: datetime = WINDOW_END,
) -> dict[str, Any]:
    warehouse_root = warehouse_root.resolve()
    pass6_classification_csv = pass6_classification_csv.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    connection = _try_connect_duckdb()
    raw_series_by_symbol: dict[str, dict[str, Any]] = {}
    candidate_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    cross_asset_seed: list[dict[str, Any]] = []
    try:
        for symbol in INDEX_SYMBOLS:
            bars_5m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_5m",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                timeframe="5m",
            )
            feature_rows = build_feature_rows(bars_5m=bars_5m, calibration_profile_name=RECOVERY_CONFIRMED)
            slope_15m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_15m",
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    timeframe="15m",
                ),
                lookback_bars=3,
            )
            direction_60m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_60m",
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    timeframe="60m",
                ),
                lookback_bars=3,
            )
            direction_240m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_240m",
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    timeframe="240m",
                ),
                lookback_bars=3,
            )
            daily_rows = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_daily",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                timeframe="daily",
            )
            daily_regime_lookup = _build_daily_regime_lookup(daily_rows)

            symbol_candidates: list[dict[str, Any]] = []
            for feature in feature_rows:
                candidate = _build_candidate_row(
                    feature=feature,
                    symbol=symbol,
                    slope_15m_lookup=slope_15m_lookup,
                    direction_60m_lookup=direction_60m_lookup,
                    direction_240m_lookup=direction_240m_lookup,
                    daily_regime_lookup=daily_regime_lookup,
                )
                if candidate is None:
                    continue
                symbol_candidates.append(candidate)
                cross_asset_seed.append(
                    {
                        "candidate_id": candidate["candidate_id"],
                        "decision_ts": candidate["decision_ts"],
                        "direction": candidate["direction"],
                        "root_group": candidate["root_group"],
                    }
                )
            candidate_rows_by_symbol[symbol] = symbol_candidates
            raw_series_by_symbol[symbol] = _load_raw_outcome_series(
                connection=connection,
                warehouse_root=warehouse_root,
                symbol=symbol,
                start_ts=start_ts,
                end_ts=_checkpoint_on_trade_date(end_ts, datetime.min.time().replace(hour=16, minute=0)),
            )
    finally:
        if connection is not None:
            connection.close()

    confirmation_map = _build_cross_asset_confirmation_map(cross_asset_seed)
    candidate_rows = _flatten_candidate_rows(candidate_rows_by_symbol, confirmation_map=confirmation_map)
    candidate_rows = [
        {
            **row,
            "timing_within_asia": _timing_bucket(str(row["subphase"])),
        }
        for row in candidate_rows
    ]
    candidate_rows.sort(key=lambda row: (_coerce_ts(row["decision_ts"]), str(row["instrument"])))

    candidate_rows = attach_vix_asof(candidate_rows, vix_rows=load_vol_regime_rows(warehouse_root))
    candidate_rows = [_enrich_environment_row(row) for row in candidate_rows]
    state_lookup = _load_state_lookup(pass6_classification_csv)

    candidate_log_rows: list[dict[str, Any]] = []
    trade_log_rows: list[dict[str, Any]] = []
    execution_policy = _build_execution_policy(candidate_rows, state_lookup=state_lookup)
    for row in candidate_rows:
        state = state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL")
        candidate_policy = execution_policy[str(row["candidate_id"])]
        execution_eligible = bool(candidate_policy["execution_eligible"])
        simulated = _simulate_candidate(
            row=row,
            state=state,
            raw_series=raw_series_by_symbol[str(row["instrument"])],
            execution_eligible=execution_eligible,
            skip_reason=None if execution_eligible else str(candidate_policy["skip_reason"]),
        )
        candidate_log_rows.append(simulated)
        if bool(simulated["simulated_trade"]):
            trade_log_rows.append(simulated)

    daily_trade_rows = _build_daily_trade_rows(trade_log_rows)
    rolling_rows = _build_rolling_summary_rows(trade_log_rows)
    instrument_rows = _build_instrument_rows(trade_log_rows)

    candidate_csv = layout["reports"] / "asia_drift_shadow_candidate_log.csv"
    candidate_parquet = layout["reports"] / "asia_drift_shadow_candidate_log.parquet"
    trade_csv = layout["reports"] / "asia_drift_shadow_daily_trade_log.csv"
    trade_parquet = layout["reports"] / "asia_drift_shadow_daily_trade_log.parquet"
    daily_summary_csv = layout["reports"] / "asia_drift_shadow_daily_performance.csv"
    daily_summary_parquet = layout["reports"] / "asia_drift_shadow_daily_performance.parquet"
    rolling_csv = layout["reports"] / "asia_drift_shadow_rolling_performance.csv"
    rolling_parquet = layout["reports"] / "asia_drift_shadow_rolling_performance.parquet"
    instrument_csv = layout["reports"] / "asia_drift_shadow_per_instrument_breakdown.csv"
    instrument_parquet = layout["reports"] / "asia_drift_shadow_per_instrument_breakdown.parquet"
    summary_json = layout["reports"] / "asia_drift_shadow_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_shadow_summary.md"

    for path, rows in [
        (candidate_csv, candidate_log_rows),
        (trade_csv, trade_log_rows),
        (daily_summary_csv, daily_trade_rows),
        (rolling_csv, rolling_rows),
        (instrument_csv, instrument_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (candidate_parquet, candidate_log_rows),
        (trade_parquet, trade_log_rows),
        (daily_summary_parquet, daily_trade_rows),
        (rolling_parquet, rolling_rows),
        (instrument_parquet, instrument_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_shadow_trading",
        "warehouse_root": str(warehouse_root),
        "source_pass6_classification_csv": str(pass6_classification_csv),
        "scope": {
            "symbols": list(INDEX_SYMBOLS),
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "time_stop_minutes": TIME_STOP_MINUTES,
            "execution_scope": "TRADE_FAVORABLE + LATE_ASIA only",
            "stop_target": {symbol: {"stop_points": spec[0], "target_points": spec[1]} for symbol, spec in STOP_TARGET.items()},
        },
        "row_counts": {
            "candidate_rows": len(candidate_log_rows),
            "simulated_trade_rows": len(trade_log_rows),
            "daily_summary_rows": len(daily_trade_rows),
            "rolling_rows": len(rolling_rows),
            "instrument_rows": len(instrument_rows),
        },
        "artifact_paths": {
            "candidate_csv": str(candidate_csv),
            "candidate_parquet": str(candidate_parquet),
            "trade_csv": str(trade_csv),
            "trade_parquet": str(trade_parquet),
            "daily_summary_csv": str(daily_summary_csv),
            "daily_summary_parquet": str(daily_summary_parquet),
            "rolling_csv": str(rolling_csv),
            "rolling_parquet": str(rolling_parquet),
            "instrument_csv": str(instrument_csv),
            "instrument_parquet": str(instrument_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "notes": [
            "Validation harness only.",
            "No broker integration or order routing.",
            "No parameter optimization or live execution automation.",
        ],
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(trade_log_rows=trade_log_rows, rolling_rows=rolling_rows, instrument_rows=instrument_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_shadow_trading",
            "artifact_paths": summary_payload["artifact_paths"],
            "candidate_rows": len(candidate_log_rows),
            "simulated_trade_rows": len(trade_log_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _execution_eligible(*, state: str, timing_within_asia: str) -> bool:
    return state == "TRADE_FAVORABLE" and timing_within_asia == "LATE_ASIA"


def _build_execution_policy(
    candidate_rows: Sequence[dict[str, Any]],
    *,
    state_lookup: dict[str, str],
) -> dict[str, dict[str, Any]]:
    policy: dict[str, dict[str, Any]] = {}
    active_until_by_instrument: dict[str, datetime] = {}
    session_seen_by_instrument: dict[str, set[str]] = {}
    for row in candidate_rows:
        candidate_id = str(row["candidate_id"])
        instrument = str(row["instrument"])
        decision_ts = _coerce_ts(row["decision_ts"])
        state = state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL")
        timing_within_asia = str(row["timing_within_asia"])
        session_key = _checkpoint_on_trade_date(decision_ts, datetime.min.time()).date().isoformat()
        if not _execution_eligible(state=state, timing_within_asia=timing_within_asia):
            policy[candidate_id] = {
                "execution_eligible": False,
                "skip_reason": "state_or_timing_not_eligible",
            }
            continue
        instrument_sessions = session_seen_by_instrument.setdefault(instrument, set())
        if session_key in instrument_sessions:
            policy[candidate_id] = {
                "execution_eligible": False,
                "skip_reason": "first_signal_only_per_instrument_session",
            }
            continue
        if decision_ts < active_until_by_instrument.get(instrument, datetime.min.replace(tzinfo=UTC)):
            policy[candidate_id] = {
                "execution_eligible": False,
                "skip_reason": "overlapping_trade_not_allowed",
            }
            continue
        instrument_sessions.add(session_key)
        active_until_by_instrument[instrument] = decision_ts + timedelta(minutes=TIME_STOP_MINUTES)
        policy[candidate_id] = {
            "execution_eligible": True,
            "skip_reason": None,
        }
    return policy


def _simulate_candidate(
    *,
    row: dict[str, Any],
    state: str,
    raw_series: dict[str, Any],
    execution_eligible: bool,
    skip_reason: str | None,
) -> dict[str, Any]:
    decision_ts = _coerce_ts(row["decision_ts"])
    instrument = str(row["instrument"])
    entry_price = float(row["decision_close"])
    base_payload = {
        "candidate_id": row["candidate_id"],
        "trade_date": _checkpoint_on_trade_date(decision_ts, datetime.min.time()).date().isoformat(),
        "timestamp": decision_ts.isoformat(),
        "instrument": instrument,
        "direction": row["direction"],
        "timing_within_asia": row["timing_within_asia"],
        "state_classification": state,
        "execution_eligible": execution_eligible,
        "entry_price": round(entry_price, 6),
        "simulated_trade": execution_eligible,
    }
    if not execution_eligible:
        return {
            **base_payload,
            "skip_reason": skip_reason,
            "exit_price": None,
            "exit_reason": "NOT_SIMULATED",
            "return_points": None,
            "max_favorable_excursion": None,
            "max_adverse_excursion": None,
            "stop_points": None,
            "target_points": None,
            "exit_ts": None,
        }

    stop_points, target_points = STOP_TARGET[instrument]
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]
    direction = str(row["direction"])
    idx_start = bisect_right(timestamps, decision_ts)
    cutoff_ts = decision_ts + timedelta(minutes=TIME_STOP_MINUTES)
    idx_end = bisect_right(timestamps, cutoff_ts)
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

    exit_reason = "TIME"
    exit_ts = cutoff_ts
    exit_price: float | None = _last_value(closes, idx_start, idx_end)
    for ts, high, low in zip(window_ts, window_highs, window_lows, strict=False):
        favorable = (float(high) - entry_price) if direction == "LONG" else (entry_price - float(low))
        adverse = (entry_price - float(low)) if direction == "LONG" else (float(high) - entry_price)
        if adverse >= stop_points:
            exit_reason = "STOP"
            exit_ts = ts
            exit_price = entry_price - stop_points if direction == "LONG" else entry_price + stop_points
            break
        if favorable >= target_points:
            exit_reason = "TARGET"
            exit_ts = ts
            exit_price = entry_price + target_points if direction == "LONG" else entry_price - target_points
            break

    return_points = _signed_return(direction, entry_price, exit_price) if exit_price is not None else None
    return {
        **base_payload,
        "skip_reason": None,
        "stop_points": stop_points,
        "target_points": target_points,
        "exit_price": round(exit_price, 6) if exit_price is not None else None,
        "exit_reason": exit_reason,
        "exit_ts": exit_ts.isoformat(),
        "return_points": round(return_points, 6) if return_points is not None else None,
        "max_favorable_excursion": round(max(favorable_values), 6) if favorable_values else None,
        "max_adverse_excursion": round(max(adverse_values), 6) if adverse_values else None,
    }


def _build_daily_trade_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["trade_date"]), []).append(row)
    payload: list[dict[str, Any]] = []
    cumulative_return = 0.0
    cumulative_trades = 0
    cumulative_wins = 0
    for trade_date in sorted(grouped):
        bucket = grouped[trade_date]
        returns = [float(row["return_points"]) for row in bucket if row["return_points"] is not None]
        wins = [value for value in returns if value > 0.0]
        cumulative_return += sum(returns)
        cumulative_trades += len(returns)
        cumulative_wins += len(wins)
        payload.append(
            {
                "trade_date": trade_date,
                "trade_count": len(returns),
                "win_count": len(wins),
                "win_rate": round(len(wins) / len(returns), 6) if returns else 0.0,
                "total_return_points": round(sum(returns), 6),
                "avg_return_points": round(_mean(returns), 6),
                "cumulative_trade_count": cumulative_trades,
                "cumulative_return_points": round(cumulative_return, 6),
                "cumulative_win_rate": round(cumulative_wins / cumulative_trades, 6) if cumulative_trades else 0.0,
            }
        )
    return payload


def _build_rolling_summary_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (_coerce_ts(row["timestamp"]), str(row["instrument"])))
    payload: list[dict[str, Any]] = []
    cumulative_return = 0.0
    cumulative_wins = 0
    for index, row in enumerate(ordered, start=1):
        current_return = float(row["return_points"]) if row["return_points"] is not None else 0.0
        cumulative_return += current_return
        if current_return > 0.0:
            cumulative_wins += 1
        rolling_window = ordered[max(0, index - ROLLING_TRADE_WINDOW) : index]
        rolling_returns = [float(item["return_points"]) for item in rolling_window if item["return_points"] is not None]
        rolling_wins = [value for value in rolling_returns if value > 0.0]
        payload.append(
            {
                "trade_index": index,
                "trade_date": row["trade_date"],
                "timestamp": row["timestamp"],
                "instrument": row["instrument"],
                "return_points": current_return,
                "cumulative_return_points": round(cumulative_return, 6),
                "cumulative_win_rate": round(cumulative_wins / index, 6),
                "rolling_trade_window": min(index, ROLLING_TRADE_WINDOW),
                "rolling_avg_return_points": round(_mean(rolling_returns), 6),
                "rolling_win_rate": round(len(rolling_wins) / len(rolling_returns), 6) if rolling_returns else 0.0,
            }
        )
    return payload


def _build_instrument_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["instrument"]), []).append(row)
    payload: list[dict[str, Any]] = []
    for instrument in INDEX_SYMBOLS:
        bucket = grouped.get(instrument, [])
        returns = [float(row["return_points"]) for row in bucket if row["return_points"] is not None]
        wins = [value for value in returns if value > 0.0]
        mfe_values = [float(row["max_favorable_excursion"]) for row in bucket if row["max_favorable_excursion"] is not None]
        mae_values = [float(row["max_adverse_excursion"]) for row in bucket if row["max_adverse_excursion"] is not None]
        payload.append(
            {
                "instrument": instrument,
                "trade_count": len(returns),
                "win_rate": round(len(wins) / len(returns), 6) if returns else 0.0,
                "total_return_points": round(sum(returns), 6),
                "avg_return_points": round(_mean(returns), 6),
                "avg_mfe_points": round(_mean(mfe_values), 6),
                "avg_mae_points": round(_mean(mae_values), 6),
                "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
                "target_hit_rate": round(sum(1 for row in bucket if row["exit_reason"] == "TARGET") / len(bucket), 6) if bucket else 0.0,
                "stop_hit_rate": round(sum(1 for row in bucket if row["exit_reason"] == "STOP") / len(bucket), 6) if bucket else 0.0,
                "time_exit_rate": round(sum(1 for row in bucket if row["exit_reason"] == "TIME") / len(bucket), 6) if bucket else 0.0,
            }
        )
    return payload


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
    trade_log_rows: Sequence[dict[str, Any]],
    rolling_rows: Sequence[dict[str, Any]],
    instrument_rows: Sequence[dict[str, Any]],
) -> str:
    total_trades = len(trade_log_rows)
    total_return = round(sum(float(row["return_points"]) for row in trade_log_rows if row["return_points"] is not None), 6)
    lines = [
        "# Asia Drift Shadow Trading",
        "",
        "Validation harness only. No broker integration or order routing.",
        "",
        f"- Simulated trades: {total_trades}",
        f"- Total return points: {total_return}",
        "",
        "## Per Instrument",
    ]
    for row in instrument_rows:
        lines.append(
            f"- {row['instrument']}: trades={row['trade_count']} avg={row['avg_return_points']} "
            f"win_rate={row['win_rate']} target={row['target_hit_rate']} stop={row['stop_hit_rate']}"
        )
    if rolling_rows:
        latest = rolling_rows[-1]
        lines.extend(
            [
                "",
                "## Latest Rolling Snapshot",
                f"- trade_index={latest['trade_index']} cumulative_return={latest['cumulative_return_points']} "
                f"rolling_avg={latest['rolling_avg_return_points']} rolling_win_rate={latest['rolling_win_rate']}",
            ]
        )
    return "\n".join(lines) + "\n"
