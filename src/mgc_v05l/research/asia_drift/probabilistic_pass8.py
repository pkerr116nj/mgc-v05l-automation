"""Execution-layer behavior summary for Asia Drift decision states."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass1 import _coerce_ts, _last_value, _load_raw_outcome_series, _quantile, _signed_return, _try_connect_duckdb
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags
from .probabilistic_pass6 import _enrich_environment_row, _mean
from .probabilistic_pass7 import _load_state_lookup


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
FOCUS_INSTRUMENTS = ("ES", "NQ")
SCENARIOS = {
    "ALL_TRADES": lambda row: True,
    "TRADE_FAVORABLE_ONLY": lambda row: str(row["decision_state"]) == "TRADE_FAVORABLE",
    "EXCLUDE_DO_NOT_TRADE": lambda row: str(row["decision_state"]) != "DO_NOT_TRADE",
}
HORIZONS = (
    ("15m", "forward_return_15m"),
    ("30m", "forward_return_30m"),
    ("60m", "forward_return_60m"),
)


def run_probabilistic_pass8(
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
        and row["timing_within_asia"] in {"EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"}
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
                end_ts=datetime.fromisoformat("2026-04-21T23:59:00-04:00"),
            )
    finally:
        if connection is not None:
            connection.close()

    decision_rows = [
        _enrich_execution_row(
            {
                **row,
                "decision_state": state_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL"),
            },
            raw_series=raw_series_by_symbol[str(row["instrument"])],
        )
        for row in enriched_rows
    ]

    state_horizon_rows = _build_state_horizon_rows(decision_rows)
    state_risk_rows = _build_state_risk_rows(decision_rows)
    scenario_rows = _build_scenario_rows(decision_rows)
    instrument_rows = _build_instrument_rows(decision_rows)

    state_horizon_csv = layout["reports"] / "asia_drift_probabilistic_pass8_state_horizons.csv"
    state_horizon_parquet = layout["reports"] / "asia_drift_probabilistic_pass8_state_horizons.parquet"
    state_risk_csv = layout["reports"] / "asia_drift_probabilistic_pass8_state_risk.csv"
    state_risk_parquet = layout["reports"] / "asia_drift_probabilistic_pass8_state_risk.parquet"
    scenario_csv = layout["reports"] / "asia_drift_probabilistic_pass8_scenario_comparison.csv"
    scenario_parquet = layout["reports"] / "asia_drift_probabilistic_pass8_scenario_comparison.parquet"
    instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass8_instrument_breakdown.csv"
    instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass8_instrument_breakdown.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass8_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass8_summary.md"

    for path, rows in [
        (state_horizon_csv, state_horizon_rows),
        (state_risk_csv, state_risk_rows),
        (scenario_csv, scenario_rows),
        (instrument_csv, instrument_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (state_horizon_parquet, state_horizon_rows),
        (state_risk_parquet, state_risk_rows),
        (scenario_parquet, scenario_rows),
        (instrument_parquet, instrument_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass8",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "source_pass6_classification_csv": str(pass6_classification_csv),
        "assumptions": {
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "decision_states": ["TRADE_FAVORABLE", "TRADE_NEUTRAL", "DO_NOT_TRADE"],
            "horizons": [label for label, _ in HORIZONS],
            "scenario_sets": list(SCENARIOS),
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "decision_rows": len(decision_rows),
            "state_horizon_rows": len(state_horizon_rows),
            "state_risk_rows": len(state_risk_rows),
            "scenario_rows": len(scenario_rows),
            "instrument_rows": len(instrument_rows),
        },
        "artifact_paths": {
            "state_horizon_csv": str(state_horizon_csv),
            "state_horizon_parquet": str(state_horizon_parquet),
            "state_risk_csv": str(state_risk_csv),
            "state_risk_parquet": str(state_risk_parquet),
            "scenario_csv": str(scenario_csv),
            "scenario_parquet": str(scenario_parquet),
            "instrument_csv": str(instrument_csv),
            "instrument_parquet": str(instrument_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(state_horizon_rows=state_horizon_rows, state_risk_rows=state_risk_rows, scenario_rows=scenario_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass8",
            "source_pass1_root": str(pass1_root),
            "warehouse_root": str(warehouse_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "decision_rows": len(decision_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _enrich_execution_row(row: dict[str, Any], *, raw_series: dict[str, Any]) -> dict[str, Any]:
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
    time_to_first_profit_minute = _time_to_first_positive(window_ts, decision_ts, favorable_values)
    time_to_first_adverse_minute = _time_to_first_positive(window_ts, decision_ts, adverse_values)

    return {
        **row,
        "forward_return_30m": round(forward_return_30m, 6) if forward_return_30m is not None else None,
        "time_to_first_profit_minute": time_to_first_profit_minute,
        "time_to_first_adverse_minute": time_to_first_adverse_minute,
    }


def _build_state_horizon_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for decision_state in ("TRADE_FAVORABLE", "TRADE_NEUTRAL", "DO_NOT_TRADE"):
            bucket = [row for row in rows if str(row["sample_split"]) == sample_split and str(row["decision_state"]) == decision_state]
            if not bucket:
                continue
            for horizon, field in HORIZONS:
                values = [float(row[field]) for row in bucket if row[field] is not None]
                payload.append(
                    {
                        "sample_split": sample_split,
                        "decision_state": decision_state,
                        "horizon": horizon,
                        "row_count": len(values),
                        "positive_return_probability": round(sum(1 for value in values if value > 0.0) / len(values), 6) if values else 0.0,
                        "avg_return": round(_mean(values), 6),
                        "median_return": round(_quantile(values, 0.50), 6) if values else 0.0,
                        "return_p10": round(_quantile(values, 0.10), 6) if values else 0.0,
                        "return_p25": round(_quantile(values, 0.25), 6) if values else 0.0,
                        "return_p75": round(_quantile(values, 0.75), 6) if values else 0.0,
                        "return_p90": round(_quantile(values, 0.90), 6) if values else 0.0,
                    }
                )
    return payload


def _build_state_risk_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for decision_state in ("TRADE_FAVORABLE", "TRADE_NEUTRAL", "DO_NOT_TRADE"):
            bucket = [row for row in rows if str(row["sample_split"]) == sample_split and str(row["decision_state"]) == decision_state]
            if not bucket:
                continue
            mfe_values = [float(row["mfe_60m_points"]) for row in bucket if row["mfe_60m_points"] is not None]
            mae_values = [float(row["mae_60m_points"]) for row in bucket if row["mae_60m_points"] is not None]
            time_to_profit = [int(row["time_to_first_profit_minute"]) for row in bucket if row["time_to_first_profit_minute"] is not None]
            time_to_loss = [int(row["time_to_first_adverse_minute"]) for row in bucket if row["time_to_first_adverse_minute"] is not None]
            resolution_counts = {
                "favorable_first_rate": sum(1 for row in bucket if row["resolution_proxy"] == "FAVORABLE_FIRST") / len(bucket),
                "adverse_first_rate": sum(1 for row in bucket if row["resolution_proxy"] == "ADVERSE_FIRST") / len(bucket),
                "tie_rate": sum(1 for row in bucket if row["resolution_proxy"] == "TIE") / len(bucket),
            }
            payload.append(
                {
                    "sample_split": sample_split,
                    "decision_state": decision_state,
                    "row_count": len(bucket),
                    "avg_mfe_60m": round(_mean(mfe_values), 6),
                    "mfe_p50": round(_quantile(mfe_values, 0.50), 6) if mfe_values else 0.0,
                    "mfe_p75": round(_quantile(mfe_values, 0.75), 6) if mfe_values else 0.0,
                    "mfe_p90": round(_quantile(mfe_values, 0.90), 6) if mfe_values else 0.0,
                    "avg_mae_60m": round(_mean(mae_values), 6),
                    "mae_p50": round(_quantile(mae_values, 0.50), 6) if mae_values else 0.0,
                    "mae_p75": round(_quantile(mae_values, 0.75), 6) if mae_values else 0.0,
                    "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
                    "time_to_profit_p25": round(_quantile(time_to_profit, 0.25), 6) if time_to_profit else None,
                    "time_to_profit_p50": round(_quantile(time_to_profit, 0.50), 6) if time_to_profit else None,
                    "time_to_profit_p75": round(_quantile(time_to_profit, 0.75), 6) if time_to_profit else None,
                    "time_to_loss_p25": round(_quantile(time_to_loss, 0.25), 6) if time_to_loss else None,
                    "time_to_loss_p50": round(_quantile(time_to_loss, 0.50), 6) if time_to_loss else None,
                    "time_to_loss_p75": round(_quantile(time_to_loss, 0.75), 6) if time_to_loss else None,
                    **{key: round(value, 6) for key, value in resolution_counts.items()},
                }
            )
    return payload


def _build_scenario_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        base_bucket = [row for row in rows if str(row["sample_split"]) == sample_split]
        for scenario_name, include in SCENARIOS.items():
            scenario_bucket = [row for row in base_bucket if include(row)]
            for horizon, field in HORIZONS:
                values = [float(row[field]) for row in scenario_bucket if row[field] is not None]
                mae_values = [float(row["mae_60m_points"]) for row in scenario_bucket if row["mae_60m_points"] is not None]
                payload.append(
                    {
                        "sample_split": sample_split,
                        "scenario": scenario_name,
                        "horizon": horizon,
                        "row_count": len(values),
                        "positive_return_probability": round(sum(1 for value in values if value > 0.0) / len(values), 6) if values else 0.0,
                        "avg_return": round(_mean(values), 6),
                        "payoff_ratio": _payoff_ratio(values),
                        "mae_p90_60m": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
                    }
                )
    return payload


def _build_instrument_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for sample_split in ("development", "holdout"):
        for instrument in FOCUS_INSTRUMENTS:
            base_bucket = [
                row
                for row in rows
                if str(row["sample_split"]) == sample_split and str(row["instrument"]) == instrument
            ]
            for scenario_name, include in SCENARIOS.items():
                scenario_bucket = [row for row in base_bucket if include(row)]
                payload.append(
                    {
                        "sample_split": sample_split,
                        "instrument": instrument,
                        "scenario": scenario_name,
                        **_risk_metric_summary(scenario_bucket),
                    }
                )
    return payload


def _risk_metric_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["forward_return_60m"]) for row in rows if row["forward_return_60m"] is not None]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value <= 0.0]
    mae_values = [float(row["mae_60m_points"]) for row in rows if row["mae_60m_points"] is not None]
    time_to_profit = [int(row["time_to_first_profit_minute"]) for row in rows if row["time_to_first_profit_minute"] is not None]
    time_to_loss = [int(row["time_to_first_adverse_minute"]) for row in rows if row["time_to_first_adverse_minute"] is not None]
    return {
        "row_count": len(returns),
        "win_rate_60m": round(sum(1 for value in returns if value > 0.0) / len(returns), 6) if returns else 0.0,
        "avg_return_60m": round(_mean(returns), 6),
        "payoff_ratio": _payoff_ratio(returns),
        "avg_mae_60m": round(_mean(mae_values), 6),
        "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
        "time_to_profit_p50": round(_quantile(time_to_profit, 0.50), 6) if time_to_profit else None,
        "time_to_loss_p50": round(_quantile(time_to_loss, 0.50), 6) if time_to_loss else None,
    }


def _payoff_ratio(values: Sequence[float]) -> float | None:
    wins = [value for value in values if value > 0.0]
    losses = [value for value in values if value <= 0.0]
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    if avg_loss >= 0.0:
        return None
    return round(avg_win / abs(avg_loss), 6)


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
    state_horizon_rows: Sequence[dict[str, Any]],
    state_risk_rows: Sequence[dict[str, Any]],
    scenario_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 8",
        "",
        "Execution behavior by decision state.",
        "",
        "## Holdout State Horizons",
    ]
    for row in state_horizon_rows:
        if row["sample_split"] != "holdout" or row["horizon"] != "60m":
            continue
        lines.append(
            f"- {row['decision_state']}: rows={row['row_count']} avg60={row['avg_return']} "
            f"wr={row['positive_return_probability']}"
        )
    lines.extend(["", "## Holdout Scenario Comparison"])
    for row in scenario_rows:
        if row["sample_split"] != "holdout" or row["horizon"] != "60m":
            continue
        lines.append(
            f"- {row['scenario']}: rows={row['row_count']} avg60={row['avg_return']} "
            f"wr={row['positive_return_probability']} mae_p90={row['mae_p90_60m']}"
        )
    lines.extend(["", "## Holdout Risk Summary"])
    for row in state_risk_rows:
        if row["sample_split"] != "holdout":
            continue
        lines.append(
            f"- {row['decision_state']}: t_profit_p50={row['time_to_profit_p50']} "
            f"t_loss_p50={row['time_to_loss_p50']} mae_p90={row['mae_p90']}"
        )
    return "\n".join(lines) + "\n"
