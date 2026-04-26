"""Monte Carlo sizing study for Asia Drift continuation shadow trades."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest


DEFAULT_STARTING_CAPITAL = 100_000.0
DEFAULT_SIMULATIONS = 5_000
DEFAULT_SEED = 42
RISK_LEVELS = (0.0025, 0.0050, 0.0075, 0.0100, 0.0125, 0.0150, 0.0200)
MODE_ORDER = (
    "es_mes_only",
    "nq_mnq_only",
    "combined_index_basket",
    "combined_conservative_half_nq",
)
MODE_INSTRUMENTS = {
    "es_mes_only": ("ES", "MES"),
    "nq_mnq_only": ("NQ", "MNQ"),
    "combined_index_basket": ("ES", "MES", "NQ", "MNQ"),
    "combined_conservative_half_nq": ("ES", "MES", "NQ", "MNQ"),
}
STOP_POINTS = {
    "ES": 6.0,
    "MES": 6.0,
    "NQ": 20.0,
    "MNQ": 20.0,
}
HALF_RISK_INSTRUMENTS = {"NQ", "MNQ"}
DRAW_THRESHOLDS = (0.05, 0.10, 0.15, 0.20, 0.25)


@dataclass(frozen=True)
class TradeObservation:
    instrument: str
    timestamp: str
    return_points: float
    r_multiple: float


def run_position_sizing_monte_carlo(
    *,
    trade_log_csv: Path,
    output_dir: Path,
    starting_capital: float = DEFAULT_STARTING_CAPITAL,
    simulations: int = DEFAULT_SIMULATIONS,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    trade_log_csv = trade_log_csv.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)
    observations = _load_trade_observations(trade_log_csv)

    summary_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    instrument_rows: list[dict[str, Any]] = []

    rng = random.Random(seed)
    for mode in MODE_ORDER:
        mode_observations = [row for row in observations if row.instrument in MODE_INSTRUMENTS[mode]]
        sampled_slots = [(row.instrument, row.timestamp) for row in mode_observations]
        instrument_pools = {
            instrument: [row for row in mode_observations if row.instrument == instrument]
            for instrument in MODE_INSTRUMENTS[mode]
        }
        instrument_rows.extend(_build_instrument_rows(mode=mode, observations=mode_observations))
        for risk_fraction in RISK_LEVELS:
            simulation_results = [
                _run_single_simulation(
                    mode=mode,
                    sampled_slots=sampled_slots,
                    instrument_pools=instrument_pools,
                    risk_fraction=risk_fraction,
                    starting_capital=starting_capital,
                    rng=rng,
                )
                for _ in range(simulations)
            ]
            summary_rows.append(
                _summarize_simulations(
                    mode=mode,
                    risk_fraction=risk_fraction,
                    simulations=simulations,
                    starting_capital=starting_capital,
                    results=simulation_results,
                )
            )
            drawdown_rows.extend(
                _build_drawdown_probability_rows(
                    mode=mode,
                    risk_fraction=risk_fraction,
                    simulations=simulations,
                    results=simulation_results,
                )
            )

    summary_csv = layout["reports"] / "asia_drift_monte_carlo_summary.csv"
    summary_parquet = layout["reports"] / "asia_drift_monte_carlo_summary.parquet"
    drawdown_csv = layout["reports"] / "asia_drift_monte_carlo_drawdown_probabilities.csv"
    drawdown_parquet = layout["reports"] / "asia_drift_monte_carlo_drawdown_probabilities.parquet"
    instrument_csv = layout["reports"] / "asia_drift_monte_carlo_instrument_baseline.csv"
    instrument_parquet = layout["reports"] / "asia_drift_monte_carlo_instrument_baseline.parquet"
    summary_json = layout["reports"] / "asia_drift_monte_carlo_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_monte_carlo_summary.md"

    for path, rows in [
        (summary_csv, summary_rows),
        (drawdown_csv, drawdown_rows),
        (instrument_csv, instrument_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (summary_parquet, summary_rows),
        (drawdown_parquet, drawdown_rows),
        (instrument_parquet, instrument_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    recommendation = _recommend_shadow_risk(summary_rows)
    payload = {
        "module": "asia_drift_position_sizing_monte_carlo",
        "trade_log_csv": str(trade_log_csv),
        "starting_capital": starting_capital,
        "simulations": simulations,
        "seed": seed,
        "risk_levels": list(RISK_LEVELS),
        "recommended_shadow_risk_fraction": recommendation["risk_fraction"],
        "recommended_shadow_risk_label": recommendation["risk_label"],
        "artifacts": {
            "summary_csv": str(summary_csv),
            "summary_parquet": str(summary_parquet),
            "drawdown_csv": str(drawdown_csv),
            "drawdown_parquet": str(drawdown_parquet),
            "instrument_csv": str(instrument_csv),
            "instrument_parquet": str(instrument_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "notes": [
            "Sizing/risk analysis only.",
            "Uses corrected non-overlapping shadow trade log.",
            "No strategy optimization or live execution changes.",
        ],
    }
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_rows=summary_rows, recommendation=recommendation),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_position_sizing_monte_carlo",
            "artifact_paths": payload["artifacts"],
            "summary_rows": len(summary_rows),
            "drawdown_rows": len(drawdown_rows),
        },
    )
    return payload


def _load_trade_observations(path: Path) -> list[TradeObservation]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    payload: list[TradeObservation] = []
    for row in rows:
        instrument = str(row["instrument"])
        return_points = float(row["return_points"])
        payload.append(
            TradeObservation(
                instrument=instrument,
                timestamp=str(row["timestamp"]),
                return_points=return_points,
                r_multiple=return_points / STOP_POINTS[instrument],
            )
        )
    payload.sort(key=lambda row: row.timestamp)
    return payload


def _risk_multiplier(mode: str, instrument: str) -> float:
    if mode == "combined_conservative_half_nq" and instrument in HALF_RISK_INSTRUMENTS:
        return 0.5
    return 1.0


def _run_single_simulation(
    *,
    mode: str,
    sampled_slots: list[tuple[str, str]],
    instrument_pools: dict[str, list[TradeObservation]],
    risk_fraction: float,
    starting_capital: float,
    rng: random.Random,
) -> dict[str, Any]:
    equity = starting_capital
    peak_equity = starting_capital
    max_drawdown = 0.0
    longest_losing_streak = 0
    current_losing_streak = 0
    worst_20_trade_r = float("inf")
    realized_r_path: list[float] = []
    for instrument, _timestamp in sampled_slots:
        observation = rng.choice(instrument_pools[instrument])
        weighted_r = observation.r_multiple * _risk_multiplier(mode, instrument)
        realized_r_path.append(weighted_r)
        pnl = equity * risk_fraction * weighted_r
        equity += pnl
        if equity > peak_equity:
            peak_equity = equity
        if peak_equity > 0.0:
            drawdown = (peak_equity - equity) / peak_equity
            max_drawdown = max(max_drawdown, drawdown)
        if weighted_r < 0.0:
            current_losing_streak += 1
            longest_losing_streak = max(longest_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
        if len(realized_r_path) >= 20:
            window_total = sum(realized_r_path[-20:])
            if window_total < worst_20_trade_r:
                worst_20_trade_r = window_total
    return {
        "ending_equity": equity,
        "ending_total_r": sum(realized_r_path),
        "max_drawdown": max_drawdown,
        "longest_losing_streak": longest_losing_streak,
        "worst_20_trade_r": worst_20_trade_r if worst_20_trade_r != float("inf") else 0.0,
    }


def _summarize_simulations(
    *,
    mode: str,
    risk_fraction: float,
    simulations: int,
    starting_capital: float,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    ending_equities = sorted(float(row["ending_equity"]) for row in results)
    ending_total_r = sorted(float(row["ending_total_r"]) for row in results)
    max_drawdowns = sorted(float(row["max_drawdown"]) for row in results)
    longest_losing_streaks = sorted(int(row["longest_losing_streak"]) for row in results)
    worst_20_trade_r = sorted(float(row["worst_20_trade_r"]) for row in results)
    return {
        "mode": mode,
        "risk_fraction": risk_fraction,
        "risk_label": f"{risk_fraction * 100:.2f}%",
        "simulations": simulations,
        "starting_capital": starting_capital,
        "median_ending_equity": round(_quantile(ending_equities, 0.50), 2),
        "p05_ending_equity": round(_quantile(ending_equities, 0.05), 2),
        "p25_ending_equity": round(_quantile(ending_equities, 0.25), 2),
        "p75_ending_equity": round(_quantile(ending_equities, 0.75), 2),
        "p95_ending_equity": round(_quantile(ending_equities, 0.95), 2),
        "median_ending_total_r": round(_quantile(ending_total_r, 0.50), 4),
        "p05_ending_total_r": round(_quantile(ending_total_r, 0.05), 4),
        "p95_ending_total_r": round(_quantile(ending_total_r, 0.95), 4),
        "median_max_drawdown": round(_quantile(max_drawdowns, 0.50), 6),
        "p95_max_drawdown": round(_quantile(max_drawdowns, 0.95), 6),
        "worst_drawdown_observed": round(max(max_drawdowns), 6),
        "median_longest_losing_streak": round(_quantile(longest_losing_streaks, 0.50), 2),
        "p95_longest_losing_streak": round(_quantile(longest_losing_streaks, 0.95), 2),
        "worst_longest_losing_streak": max(longest_losing_streaks),
        "median_worst_20_trade_r": round(_quantile(worst_20_trade_r, 0.50), 4),
        "p95_worst_20_trade_r": round(_quantile(worst_20_trade_r, 0.95), 4),
        "worst_20_trade_r_observed": round(min(worst_20_trade_r), 4),
    }


def _build_drawdown_probability_rows(
    *,
    mode: str,
    risk_fraction: float,
    simulations: int,
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    max_drawdowns = [float(row["max_drawdown"]) for row in results]
    payload: list[dict[str, Any]] = []
    for threshold in DRAW_THRESHOLDS:
        exceeded = sum(1 for value in max_drawdowns if value >= threshold)
        payload.append(
            {
                "mode": mode,
                "risk_fraction": risk_fraction,
                "risk_label": f"{risk_fraction * 100:.2f}%",
                "simulations": simulations,
                "drawdown_threshold": threshold,
                "probability": round(exceeded / simulations, 6),
            }
        )
    return payload


def _build_instrument_rows(*, mode: str, observations: list[TradeObservation]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for instrument in MODE_INSTRUMENTS[mode]:
        bucket = [row for row in observations if row.instrument == instrument]
        r_values = [row.r_multiple for row in bucket]
        payload.append(
            {
                "mode": mode,
                "instrument": instrument,
                "trade_count": len(bucket),
                "avg_return_points": round(sum(row.return_points for row in bucket) / len(bucket), 6) if bucket else 0.0,
                "avg_r_multiple": round(sum(r_values) / len(r_values), 6) if r_values else 0.0,
                "win_rate": round(sum(1 for value in r_values if value > 0.0) / len(r_values), 6) if r_values else 0.0,
            }
        )
    return payload


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = (len(values) - 1) * q
    lower = int(idx)
    upper = min(lower + 1, len(values) - 1)
    weight = idx - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _recommend_shadow_risk(summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    combined_rows = [row for row in summary_rows if row["mode"] == "combined_conservative_half_nq"]
    candidates = [
        row
        for row in combined_rows
        if float(row["p95_max_drawdown"]) <= 0.15 and float(row["worst_drawdown_observed"]) <= 0.25
    ]
    if candidates:
        best = max(candidates, key=lambda row: float(row["risk_fraction"]))
        return {"risk_fraction": best["risk_fraction"], "risk_label": best["risk_label"]}
    safest = min(combined_rows, key=lambda row: float(row["risk_fraction"]))
    return {"risk_fraction": safest["risk_fraction"], "risk_label": safest["risk_label"]}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
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


def _render_markdown(*, summary_rows: list[dict[str, Any]], recommendation: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Monte Carlo Sizing Study",
        "",
        f"- Recommended shadow risk: {recommendation['risk_label']}",
        "",
        "## Combined Conservative Basket",
    ]
    for row in summary_rows:
        if row["mode"] != "combined_conservative_half_nq":
            continue
        lines.append(
            f"- {row['risk_label']}: median_end=${row['median_ending_equity']}, "
            f"median_dd={row['median_max_drawdown']:.2%}, p95_dd={row['p95_max_drawdown']:.2%}, "
            f"worst_dd={row['worst_drawdown_observed']:.2%}"
        )
    return "\n".join(lines) + "\n"
