"""Signal extraction analysis for NDX U.S. open bias after 10:00 ET."""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..asia_drift.probabilistic_pass1 import _mean, _quantile, _write_csv
from ..asia_drift.probabilistic_pass2 import _coerce_row
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass2 import _annotate_rows, _merge_rows


PASS_VERSION = "us_open_probabilistic_pass4_v1"
RANDOM_BASELINE_SEED = 42
RANDOM_BASELINE_ITERATIONS = 2000
SLICE_ALL_NDX = "ALL_NDX_1000"
SLICE_UP_OPEN = "UP_OPEN"
SLICE_DOWN_OPEN = "DOWN_OPEN"
SLICE_Q5_UP_OPEN = "Q5_UP_OPEN"
SLICE_UP_CROSS_INDEX_CONFIRMED = "UP_CROSS_INDEX_CONFIRMED"
SLICE_UP_OVERNIGHT_ALIGNED = "UP_OVERNIGHT_ALIGNED"
SLICE_UP_FAVORABLE_VIX = "UP_FAVORABLE_VIX"
SLICE_BASELINE_OVERNIGHT_UP = "BASELINE_OVERNIGHT_UP"


def run_probabilistic_pass4(*, pass1_root: Path, output_dir: Path) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv")
    merged_rows = _prepare_rows(_annotate_rows(_merge_rows(candidate_rows, outcome_rows)))
    ndx_rows = [row for row in merged_rows if str(row["cluster"]) == "NDX"]

    slice_specs = _slice_specs()
    signal_rows = _build_signal_rows(ndx_rows, slice_specs)
    baseline_rows = _build_baseline_rows(ndx_rows, signal_rows)
    distribution_rows = _build_distribution_rows(ndx_rows, slice_specs)
    dev_holdout_rows = _build_dev_holdout_rows(signal_rows)

    signal_extraction_csv = layout["reports"] / "us_open_probabilistic_pass4_signal_extraction.csv"
    baseline_comparison_csv = layout["reports"] / "us_open_probabilistic_pass4_baseline_comparison.csv"
    distribution_csv = layout["reports"] / "us_open_probabilistic_pass4_distribution_table.csv"
    dev_holdout_csv = layout["reports"] / "us_open_probabilistic_pass4_dev_holdout_comparison.csv"
    summary_json = layout["reports"] / "us_open_probabilistic_pass4_summary.json"
    summary_markdown = layout["reports"] / "us_open_probabilistic_pass4_summary.md"

    _write_csv(signal_extraction_csv, signal_rows)
    _write_csv(baseline_comparison_csv, baseline_rows)
    _write_csv(distribution_csv, distribution_rows)
    _write_csv(dev_holdout_csv, dev_holdout_rows)

    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass4_signal_extraction.parquet", signal_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass4_baseline_comparison.parquet", baseline_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass4_distribution_table.parquet", distribution_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass4_dev_holdout_comparison.parquet", dev_holdout_rows)

    blunt_conclusion = _classify_signal_presence(signal_rows, baseline_rows)
    summary_payload = {
        "module": "us_open_probabilistic_pass4",
        "version": PASS_VERSION,
        "source_pass1_root": str(pass1_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "ndx_rows": len(ndx_rows),
        },
        "assumptions": {
            "focus": "long-only NDX same-day return from 10:00 ET",
            "always_long_baseline": "all NDX 10:00 days",
            "random_same_time_baseline": f"{RANDOM_BASELINE_ITERATIONS} sample-matched draws without replacement from all NDX 10:00 days per split",
            "overnight_direction_baseline": "long-only on NDX days where overnight direction is UP",
            "favorable_vix": "broad non-optimized VIX level filter: LOW or MID",
        },
        "artifact_paths": {
            "signal_extraction_csv": str(signal_extraction_csv),
            "baseline_comparison_csv": str(baseline_comparison_csv),
            "distribution_csv": str(distribution_csv),
            "dev_holdout_csv": str(dev_holdout_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "blunt_conclusion": blunt_conclusion,
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_payload, signal_rows, baseline_rows, dev_holdout_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "us_open_probabilistic_pass4",
            "version": PASS_VERSION,
            "source_pass1_root": str(pass1_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "ndx_row_count": len(ndx_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _prepare_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        direction = str(row["direction"])
        long_return_1530 = float(row["forward_return_1530_raw"]) if row.get("forward_return_1530_raw") is not None else None
        long_return_close = float(row["forward_return_close_raw"]) if row.get("forward_return_close_raw") is not None else None
        long_mfe_close = float(row["mfe_close_points"]) if direction == "UP" else float(row["mae_close_points"])
        long_mae_close = float(row["mae_close_points"]) if direction == "UP" else float(row["mfe_close_points"])
        payload.append(
            {
                **row,
                "long_return_1530": long_return_1530,
                "long_return_close": long_return_close,
                "long_mfe_close_points": long_mfe_close,
                "long_mae_close_points": long_mae_close,
                "is_favorable_vix": str(row.get("vix_level_bucket")) in {"LOW", "MID"},
            }
        )
    return payload


def _slice_specs() -> list[tuple[str, str, Any]]:
    return [
        (SLICE_ALL_NDX, "All NDX 10:00 days (always-long baseline)", lambda row: True),
        (SLICE_UP_OPEN, "NDX UP opening-drive days", lambda row: str(row["direction"]) == "UP"),
        (SLICE_DOWN_OPEN, "NDX DOWN opening-drive days", lambda row: str(row["direction"]) == "DOWN"),
        (
            SLICE_Q5_UP_OPEN,
            "NDX Q5-largest UP opening-drive days",
            lambda row: str(row["direction"]) == "UP" and bool(row["opening_drive_extreme_flag"]),
        ),
        (
            SLICE_UP_CROSS_INDEX_CONFIRMED,
            "NDX UP opening-drive days with cross-index confirmation",
            lambda row: str(row["direction"]) == "UP" and bool(row["cross_index_confirmation"]),
        ),
        (
            SLICE_UP_OVERNIGHT_ALIGNED,
            "NDX UP opening-drive days with overnight alignment",
            lambda row: str(row["direction"]) == "UP" and str(row["overnight_alignment"]) == "ALIGNED",
        ),
        (
            SLICE_UP_FAVORABLE_VIX,
            "NDX UP opening-drive days with favorable VIX (LOW/MID)",
            lambda row: str(row["direction"]) == "UP" and bool(row["is_favorable_vix"]),
        ),
        (
            SLICE_BASELINE_OVERNIGHT_UP,
            "Baseline: long-only on NDX days with overnight direction UP",
            lambda row: str(row.get("overnight_direction")) == "UP",
        ),
    ]


def _build_signal_rows(rows: Sequence[dict[str, Any]], slice_specs: Sequence[tuple[str, str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name, description, predicate in slice_specs:
        for sample_split in ("development", "holdout"):
            bucket = [row for row in rows if str(row["sample_split"]) == sample_split and predicate(row)]
            payload.append(_signal_record(slice_name, description, sample_split, bucket))
    return payload


def _signal_record(slice_name: str, description: str, sample_split: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    close_returns = [float(row["long_return_close"]) for row in rows if row.get("long_return_close") is not None]
    returns_1530 = [float(row["long_return_1530"]) for row in rows if row.get("long_return_1530") is not None]
    positive_close = [value for value in close_returns if value > 0.0]
    negative_close = [value for value in close_returns if value < 0.0]
    worst_decile_count = max(1, int(len(close_returns) * 0.10)) if close_returns else 0
    worst_decile_values = sorted(close_returns)[:worst_decile_count] if worst_decile_count else []
    return {
        "slice_name": slice_name,
        "slice_description": description,
        "sample_split": sample_split,
        "sample_count": len(rows),
        "avg_return_1530": round(_mean(returns_1530), 6),
        "median_return_1530": round(_median(returns_1530), 6),
        "win_rate_1530": round(_win_rate(returns_1530), 6),
        "p10_return_1530": round(_quantile(returns_1530, 0.10), 6),
        "p25_return_1530": round(_quantile(returns_1530, 0.25), 6),
        "p75_return_1530": round(_quantile(returns_1530, 0.75), 6),
        "p90_return_1530": round(_quantile(returns_1530, 0.90), 6),
        "avg_return_close": round(_mean(close_returns), 6),
        "median_return_close": round(_median(close_returns), 6),
        "win_rate_close": round(_win_rate(close_returns), 6),
        "p10_return_close": round(_quantile(close_returns, 0.10), 6),
        "p25_return_close": round(_quantile(close_returns, 0.25), 6),
        "p75_return_close": round(_quantile(close_returns, 0.75), 6),
        "p90_return_close": round(_quantile(close_returns, 0.90), 6),
        "worst_decile_mean_close": round(_mean(worst_decile_values), 6),
        "avg_mfe_close_points": round(_mean(float(row["long_mfe_close_points"]) for row in rows), 6),
        "avg_mae_close_points": round(_mean(float(row["long_mae_close_points"]) for row in rows), 6),
        "payoff_skew_close": round(_payoff_skew(positive_close, negative_close), 6),
    }


def _build_baseline_rows(rows: Sequence[dict[str, Any]], signal_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rng = random.Random(RANDOM_BASELINE_SEED)
    payload: list[dict[str, Any]] = []
    rows_by_split = {
        split: [row for row in rows if str(row["sample_split"]) == split]
        for split in ("development", "holdout")
    }
    overnight_up_by_split = {
        split: [row for row in rows_by_split[split] if str(row.get("overnight_direction")) == "UP"]
        for split in ("development", "holdout")
    }
    for signal_row in signal_rows:
        sample_split = str(signal_row["sample_split"])
        slice_name = str(signal_row["slice_name"])
        sample_count = int(signal_row["sample_count"])
        universe = rows_by_split[sample_split]
        overnight_up = overnight_up_by_split[sample_split]
        random_stats = _random_baseline(universe, sample_count, rng)
        baseline_all = _signal_record("BASELINE_ALWAYS_LONG", "All NDX 10:00 days", sample_split, universe)
        baseline_overnight = _signal_record("BASELINE_OVERNIGHT_UP", "Long-only overnight UP days", sample_split, overnight_up)
        payload.append(
            {
                "slice_name": slice_name,
                "sample_split": sample_split,
                "sample_count": sample_count,
                "observed_avg_return_1530": signal_row["avg_return_1530"],
                "observed_avg_return_close": signal_row["avg_return_close"],
                "always_long_avg_return_close": baseline_all["avg_return_close"],
                "delta_vs_always_long_close": round(float(signal_row["avg_return_close"]) - float(baseline_all["avg_return_close"]), 6),
                "overnight_up_avg_return_close": baseline_overnight["avg_return_close"],
                "delta_vs_overnight_up_close": round(float(signal_row["avg_return_close"]) - float(baseline_overnight["avg_return_close"]), 6),
                "random_baseline_expected_return_1530": random_stats["expected_return_1530"],
                "random_baseline_expected_return_close": random_stats["expected_return_close"],
                "random_baseline_p10_return_close": random_stats["p10_return_close"],
                "random_baseline_p90_return_close": random_stats["p90_return_close"],
                "random_baseline_percentile_close": random_stats["observed_percentile_close_by_slice"].get(slice_name, None),
            }
        )
    # fill observed percentile after all rows created, using per-split lookup from signal rows
    percentiles = _random_baseline_percentiles(rows_by_split, signal_rows)
    for row in payload:
        key = (row["sample_split"], row["slice_name"])
        row["random_baseline_percentile_close"] = percentiles.get(key, 0.0)
    return payload


def _random_baseline(universe: Sequence[dict[str, Any]], sample_count: int, rng: random.Random) -> dict[str, float]:
    if not universe or sample_count <= 0:
        return {
            "expected_return_1530": 0.0,
            "expected_return_close": 0.0,
            "p10_return_close": 0.0,
            "p90_return_close": 0.0,
            "observed_percentile_close_by_slice": {},
        }
    sample_count = min(sample_count, len(universe))
    draws_1530: list[float] = []
    draws_close: list[float] = []
    for _ in range(RANDOM_BASELINE_ITERATIONS):
        draw = rng.sample(list(universe), sample_count)
        draws_1530.append(_mean(float(row["long_return_1530"]) for row in draw))
        draws_close.append(_mean(float(row["long_return_close"]) for row in draw))
    return {
        "expected_return_1530": round(_mean(draws_1530), 6),
        "expected_return_close": round(_mean(draws_close), 6),
        "p10_return_close": round(_quantile(draws_close, 0.10), 6),
        "p90_return_close": round(_quantile(draws_close, 0.90), 6),
        "observed_percentile_close_by_slice": {},
    }


def _random_baseline_percentiles(
    rows_by_split: dict[str, list[dict[str, Any]]],
    signal_rows: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], float]:
    payload: dict[tuple[str, str], float] = {}
    for sample_split in ("development", "holdout"):
        universe = rows_by_split[sample_split]
        if not universe:
            continue
        rng = random.Random(RANDOM_BASELINE_SEED + (0 if sample_split == "development" else 1))
        rows_for_split = [row for row in signal_rows if str(row["sample_split"]) == sample_split]
        for signal_row in rows_for_split:
            sample_count = min(int(signal_row["sample_count"]), len(universe))
            observed = float(signal_row["avg_return_close"])
            draws: list[float] = []
            for _ in range(RANDOM_BASELINE_ITERATIONS):
                draw = rng.sample(list(universe), sample_count)
                draws.append(_mean(float(row["long_return_close"]) for row in draw))
            percentile = sum(1 for value in draws if value <= observed) / max(len(draws), 1)
            payload[(sample_split, str(signal_row["slice_name"]))] = round(percentile, 6)
    return payload


def _build_distribution_rows(rows: Sequence[dict[str, Any]], slice_specs: Sequence[tuple[str, str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name, description, predicate in slice_specs:
        for sample_split in ("development", "holdout"):
            bucket = [row for row in rows if str(row["sample_split"]) == sample_split and predicate(row)]
            for horizon_name, field_name in (("1530", "long_return_1530"), ("close", "long_return_close")):
                returns = [float(row[field_name]) for row in bucket if row.get(field_name) is not None]
                for decile in range(10):
                    lower_q = decile / 10
                    upper_q = (decile + 1) / 10
                    lower = _quantile(returns, lower_q)
                    upper = _quantile(returns, upper_q)
                    payload.append(
                        {
                            "slice_name": slice_name,
                            "slice_description": description,
                            "sample_split": sample_split,
                            "horizon": horizon_name,
                            "decile_bucket": f"D{decile + 1}",
                            "lower_bound": round(lower, 6),
                            "upper_bound": round(upper, 6),
                            "sample_count": len(bucket),
                        }
                    )
    return payload


def _build_dev_holdout_rows(signal_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    slice_names = sorted({str(row["slice_name"]) for row in signal_rows})
    for slice_name in slice_names:
        dev = next(row for row in signal_rows if row["slice_name"] == slice_name and row["sample_split"] == "development")
        holdout = next(row for row in signal_rows if row["slice_name"] == slice_name and row["sample_split"] == "holdout")
        payload.append(
            {
                "slice_name": slice_name,
                "slice_description": dev["slice_description"],
                "development_sample_count": dev["sample_count"],
                "holdout_sample_count": holdout["sample_count"],
                "development_avg_return_close": dev["avg_return_close"],
                "holdout_avg_return_close": holdout["avg_return_close"],
                "development_win_rate_close": dev["win_rate_close"],
                "holdout_win_rate_close": holdout["win_rate_close"],
                "development_payoff_skew_close": dev["payoff_skew_close"],
                "holdout_payoff_skew_close": holdout["payoff_skew_close"],
                "avg_return_close_delta": round(float(holdout["avg_return_close"]) - float(dev["avg_return_close"]), 6),
                "win_rate_close_delta": round(float(holdout["win_rate_close"]) - float(dev["win_rate_close"]), 6),
            }
        )
    return payload


def _classify_signal_presence(signal_rows: Sequence[dict[str, Any]], baseline_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    holdout_up = next(row for row in signal_rows if row["slice_name"] == SLICE_UP_OPEN and row["sample_split"] == "holdout")
    holdout_all = next(row for row in signal_rows if row["slice_name"] == SLICE_ALL_NDX and row["sample_split"] == "holdout")
    holdout_baseline = next(row for row in baseline_rows if row["slice_name"] == SLICE_UP_OPEN and row["sample_split"] == "holdout")
    signal_present = (
        float(holdout_up["avg_return_close"]) > float(holdout_all["avg_return_close"])
        and float(holdout_up["win_rate_close"]) >= 0.55
        and float(holdout_baseline["random_baseline_percentile_close"]) >= 0.70
        and int(holdout_up["sample_count"]) >= 100
    )
    signal_weak = (
        float(holdout_up["avg_return_close"]) > 0.0
        and float(holdout_up["avg_return_close"]) >= float(holdout_all["avg_return_close"])
    )
    signal_status = "present" if signal_present else "weak" if signal_weak else "absent"
    return {
        "signal_status": signal_status,
        "focus_slice": SLICE_UP_OPEN,
        "holdout_avg_return_close": holdout_up["avg_return_close"],
        "holdout_win_rate_close": holdout_up["win_rate_close"],
        "holdout_random_baseline_percentile_close": holdout_baseline["random_baseline_percentile_close"],
        "detail": (
            "Signal present" if signal_status == "present"
            else "Signal weak" if signal_status == "weak"
            else "Signal absent"
        ),
    }


def _render_markdown(
    summary_payload: dict[str, Any],
    signal_rows: Sequence[dict[str, Any]],
    baseline_rows: Sequence[dict[str, Any]],
    dev_holdout_rows: Sequence[dict[str, Any]],
) -> str:
    blunt = summary_payload["blunt_conclusion"]
    lines = [
        "# US Open Probabilistic Pass 4",
        "",
        "## Scope",
        "",
        "- framing: signal extraction from Pass 1, not strategy classification",
        "- focus: `NQ/MNQ` only",
        "- long-only `10:00 ET` same-day return lens",
        "- horizons: `15:30 ET`, `close`",
        "",
        "## Blunt Conclusion",
        "",
        f"- signal status: `{blunt['signal_status']}`",
        f"- focus slice: `{blunt['focus_slice']}`",
        f"- holdout avg close return: `{float(blunt['holdout_avg_return_close']):.3f}`",
        f"- holdout win rate close: `{float(blunt['holdout_win_rate_close']):.2%}`",
        f"- sample-matched random baseline percentile: `{float(blunt['holdout_random_baseline_percentile_close']):.2%}`",
        "",
        "## Signal Extraction Table",
        "",
    ]
    for row in [item for item in signal_rows if item["sample_split"] == "holdout"]:
        lines.append(
            f"- `{row['slice_name']}`: n `{row['sample_count']}`, avg1530 `{float(row['avg_return_1530']):.3f}`, "
            f"avgClose `{float(row['avg_return_close']):.3f}`, medianClose `{float(row['median_return_close']):.3f}`, "
            f"winClose `{float(row['win_rate_close']):.2%}`, p10/p90 `{float(row['p10_return_close']):.3f}/{float(row['p90_return_close']):.3f}`"
        )
    lines.extend(["", "## Baseline Comparison", ""])
    for row in [item for item in baseline_rows if item["sample_split"] == "holdout"]:
        lines.append(
            f"- `{row['slice_name']}`: delta vs always-long `{float(row['delta_vs_always_long_close']):.3f}`, "
            f"delta vs overnight-UP `{float(row['delta_vs_overnight_up_close']):.3f}`, "
            f"random percentile `{float(row['random_baseline_percentile_close']):.2%}`"
        )
    lines.extend(["", "## Dev vs Holdout", ""])
    for row in dev_holdout_rows:
        lines.append(
            f"- `{row['slice_name']}`: dev avgClose `{float(row['development_avg_return_close']):.3f}`, "
            f"holdout avgClose `{float(row['holdout_avg_return_close']):.3f}`, "
            f"holdout winClose `{float(row['holdout_win_rate_close']):.2%}`"
        )
    return "\n".join(lines) + "\n"


def _median(values: Sequence[float]) -> float:
    return median(values) if values else 0.0


def _win_rate(values: Sequence[float]) -> float:
    return sum(1 for value in values if value > 0.0) / max(len(values), 1) if values else 0.0


def _payoff_skew(positive_values: Sequence[float], negative_values: Sequence[float]) -> float:
    avg_positive = _mean(positive_values)
    avg_negative = abs(_mean(negative_values))
    if avg_negative <= 0.0:
        return avg_positive if avg_positive > 0.0 else 0.0
    return avg_positive / avg_negative
