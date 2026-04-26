"""Regime-conditioned analysis for the Asia Drift continuation evidence stack."""

from __future__ import annotations

import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass1 import _coerce_ts, _load_warehouse_bars, _quantile, _try_connect_duckdb
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
DO_NOT_TRUST_MIN_ROWS = 100


def run_probabilistic_pass5(
    *,
    pass1_root: Path,
    warehouse_root: Path,
    output_dir: Path,
    regime_mode: str = "realized_only",
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
    daily_bars_by_symbol: dict[str, list[ResearchBar]] = {}
    try:
        for symbol in INDEX_SYMBOLS:
            daily_bars_by_symbol[symbol] = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_daily",
                symbol=symbol,
                start_ts=datetime.fromisoformat("2020-01-01T18:00:00-05:00"),
                end_ts=datetime.fromisoformat("2026-04-21T23:59:00-04:00"),
                timeframe="daily",
            )
    finally:
        if connection is not None:
            connection.close()

    prior_session_lookup = {
        symbol: _build_prior_session_lookup(bars)
        for symbol, bars in daily_bars_by_symbol.items()
    }
    index_alignment_lookup = _build_index_alignment_lookup(filtered_rows)
    enriched_rows = [
        _enrich_regime_row(
            row,
            prior_session_lookup=prior_session_lookup[str(row["instrument"])],
            index_alignment_lookup=index_alignment_lookup,
        )
        for row in filtered_rows
    ]
    if regime_mode in {"vix_only", "realized_plus_vix"}:
        enriched_rows = _attach_vix_regimes(enriched_rows, warehouse_root=warehouse_root, regime_mode=regime_mode)

    regime_expectancy_rows = _build_regime_metric_rows(enriched_rows, regime_mode=regime_mode)
    instrument_regime_rows = _build_instrument_regime_rows(enriched_rows, regime_mode=regime_mode)
    time_cluster_rows = _build_time_cluster_rows(enriched_rows)
    edge_class_rows = _build_edge_classification_rows(regime_expectancy_rows)

    expectancy_csv = layout["reports"] / "asia_drift_probabilistic_pass5_regime_expectancy.csv"
    expectancy_parquet = layout["reports"] / "asia_drift_probabilistic_pass5_regime_expectancy.parquet"
    instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass5_instrument_regime_breakdown.csv"
    instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass5_instrument_regime_breakdown.parquet"
    time_cluster_csv = layout["reports"] / "asia_drift_probabilistic_pass5_time_clustering.csv"
    time_cluster_parquet = layout["reports"] / "asia_drift_probabilistic_pass5_time_clustering.parquet"
    edge_class_csv = layout["reports"] / "asia_drift_probabilistic_pass5_edge_classification.csv"
    edge_class_parquet = layout["reports"] / "asia_drift_probabilistic_pass5_edge_classification.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass5_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass5_summary.md"

    for path, rows in [
        (expectancy_csv, regime_expectancy_rows),
        (instrument_csv, instrument_regime_rows),
        (time_cluster_csv, time_cluster_rows),
        (edge_class_csv, edge_class_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (expectancy_parquet, regime_expectancy_rows),
        (instrument_parquet, instrument_regime_rows),
        (time_cluster_parquet, time_cluster_rows),
        (edge_class_parquet, edge_class_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass5",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "assumptions": {
            "do_not_trust_min_rows": DO_NOT_TRUST_MIN_ROWS,
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "regime_mode": regime_mode,
            "volatility_proxy": "daily_regime_bucket suffix: EXPANDED vs NORMAL",
            "vix_join_policy": "most_recent_vix_asof_ts_lte_decision_ts",
            "trend_regime": "daily directional regime plus 240m agreement",
            "prior_session_behavior": "prior session trend fraction and range ratio vs 20-day median",
            "index_alignment": "same-direction candidate present in both SPX and NDX root groups at decision timestamp",
            "edge_classification": {
                "EDGE_ON_CANDIDATE": "development and holdout avg60 > 0, holdout win rate >= 0.52, and no trust flag",
                "EDGE_OFF_CANDIDATE": "holdout avg60 <= 0 or holdout win rate < 0.5",
                "MIXED": "everything else",
            },
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "filtered_index_stack_rows": len(enriched_rows),
        },
        "artifact_paths": {
            "regime_expectancy_csv": str(expectancy_csv),
            "regime_expectancy_parquet": str(expectancy_parquet),
            "instrument_regime_csv": str(instrument_csv),
            "instrument_regime_parquet": str(instrument_parquet),
            "time_cluster_csv": str(time_cluster_csv),
            "time_cluster_parquet": str(time_cluster_parquet),
            "edge_classification_csv": str(edge_class_csv),
            "edge_classification_parquet": str(edge_class_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_payload, regime_expectancy_rows, instrument_regime_rows, edge_class_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass5",
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


def _build_prior_session_lookup(bars: Sequence[ResearchBar]) -> dict[date, dict[str, Any]]:
    payload: dict[date, dict[str, Any]] = {}
    ordered = sorted(bars, key=lambda item: item.end_ts)
    rolling_ranges: list[float] = []
    for idx, bar in enumerate(ordered):
        session_date = bar.end_ts.astimezone(bar.end_ts.tzinfo).date()
        if idx == 0:
            rolling_ranges.append(float(bar.high - bar.low))
            continue
        prior = ordered[idx - 1]
        prior_range = float(prior.high - prior.low)
        prior_body = abs(float(prior.close - prior.open))
        prior_trend_fraction = prior_body / prior_range if prior_range > 0.0 else 0.0
        rolling_window = rolling_ranges[max(0, len(rolling_ranges) - 20) :]
        median_range = _quantile(rolling_window, 0.50) if rolling_window else prior_range
        range_ratio = prior_range / median_range if median_range > 0.0 else 1.0
        payload[session_date] = {
            "prior_session_behavior": _prior_session_behavior_bucket(prior_trend_fraction),
            "prior_session_range_regime": _prior_session_range_bucket(range_ratio),
        }
        rolling_ranges.append(float(bar.high - bar.low))
    return payload


def _prior_session_behavior_bucket(trend_fraction: float) -> str:
    if trend_fraction >= 0.60:
        return "STRONG_PRIOR_TREND"
    if trend_fraction <= 0.30:
        return "PRIOR_CHOP"
    return "PRIOR_BALANCED"


def _prior_session_range_bucket(range_ratio: float) -> str:
    if range_ratio >= 1.20:
        return "PRIOR_LARGE_RANGE"
    if range_ratio <= 0.80:
        return "PRIOR_SMALL_RANGE"
    return "PRIOR_NORMAL_RANGE"


def _build_index_alignment_lookup(rows: Sequence[dict[str, Any]]) -> dict[tuple[datetime, str], str]:
    grouped: dict[tuple[datetime, str], set[str]] = {}
    for row in rows:
        decision_ts = _coerce_ts(row["decision_ts"])
        direction = str(row["direction"])
        grouped.setdefault((decision_ts, direction), set()).add("SPX" if str(row["instrument"]) in {"ES", "MES"} else "NDX")
    payload: dict[tuple[datetime, str], str] = {}
    for key, root_groups in grouped.items():
        payload[key] = "INDEX_ALIGNED" if root_groups == {"SPX", "NDX"} else "INDEX_DIVERGENT"
    return payload


def _enrich_regime_row(
    row: dict[str, Any],
    *,
    prior_session_lookup: dict[date, dict[str, Any]],
    index_alignment_lookup: dict[tuple[datetime, str], str],
) -> dict[str, Any]:
    decision_ts = _coerce_ts(row["decision_ts"])
    session_date = date.fromisoformat(str(row["local_session_date"]))
    daily_regime_bucket = str(row["daily_regime_bucket"])
    direction_240m_state = str(row["direction_240m_state"])
    agreement_240m = str(row["agreement_240m"])

    prior_payload = prior_session_lookup.get(
        session_date,
        {
            "prior_session_behavior": "UNKNOWN",
            "prior_session_range_regime": "UNKNOWN",
        },
    )
    return {
        **row,
        "volatility_regime": _volatility_regime(daily_regime_bucket),
        "trend_regime": _trend_regime(daily_regime_bucket, direction_240m_state, agreement_240m),
        "prior_session_behavior": prior_payload["prior_session_behavior"],
        "prior_session_range_regime": prior_payload["prior_session_range_regime"],
        "index_environment": index_alignment_lookup.get((_coerce_ts(row["decision_ts"]), str(row["direction"])), "INDEX_DIVERGENT"),
        "decision_year": decision_ts.year,
        "decision_month": f"{decision_ts.month:02d}",
    }


def _volatility_regime(daily_regime_bucket: str) -> str:
    if daily_regime_bucket.endswith("EXPANDED"):
        return "HIGH_VOL"
    if daily_regime_bucket.endswith("NORMAL"):
        return "LOW_VOL"
    return "UNKNOWN_VOL"


def _trend_regime(daily_regime_bucket: str, direction_240m_state: str, agreement_240m: str) -> str:
    if daily_regime_bucket.startswith("FLAT") or daily_regime_bucket == "UNKNOWN" or direction_240m_state in {"FLAT", "UNKNOWN"}:
        return "RANGE_BOUND_OR_UNCLEAR"
    if agreement_240m == "AGREE":
        return "TREND_SUPPORTIVE"
    return "COUNTERTREND_OR_MIXED"


def _regime_dimensions(regime_mode: str) -> tuple[str, ...]:
    shared = (
        "trend_regime",
        "prior_session_behavior",
        "prior_session_range_regime",
        "index_environment",
    )
    if regime_mode == "realized_only":
        return ("volatility_regime",) + shared
    if regime_mode == "vix_only":
        return (
            "vix_level_bucket",
            "vix_change_bucket",
            "vix_combined_bucket",
        ) + shared
    if regime_mode == "realized_plus_vix":
        return (
            "volatility_regime",
            "vix_level_bucket",
            "vix_change_bucket",
            "vix_combined_bucket",
            "realized_plus_vix_bucket",
        ) + shared
    raise RuntimeError(f"Unsupported regime mode: {regime_mode}")


def _attach_vix_regimes(rows: Sequence[dict[str, Any]], *, warehouse_root: Path, regime_mode: str) -> list[dict[str, Any]]:
    if regime_mode == "realized_only":
        return list(rows)
    vix_rows = load_vol_regime_rows(warehouse_root)
    enriched = attach_vix_asof(rows, vix_rows=vix_rows)
    payload: list[dict[str, Any]] = []
    for row in enriched:
        payload.append(
            {
                **row,
                "realized_plus_vix_bucket": f"{row['volatility_regime']}|{row['vix_combined_bucket']}",
            }
        )
    return payload


def _build_regime_metric_rows(rows: Sequence[dict[str, Any]], *, regime_mode: str) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    baseline_map = _baseline_map(rows)
    for dimension in _regime_dimensions(regime_mode):
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row["sample_split"]), str(row["timing_within_asia"]), str(row[dimension])), []).append(row)
        for key in sorted(grouped):
            bucket = grouped[key]
            metrics = _metric_summary(bucket)
            baseline = baseline_map[(key[0], key[1])]
            payload.append(
                {
                    "sample_split": key[0],
                    "timing_within_asia": key[1],
                    "regime_dimension": dimension,
                    "regime_value": key[2],
                    **metrics,
                    "avg_return_60m_uplift_vs_timing_baseline": round(float(metrics["avg_return_60m"]) - float(baseline["avg_return_60m"]), 6),
                    "win_rate_uplift_vs_timing_baseline": round(float(metrics["win_rate_60m"]) - float(baseline["win_rate_60m"]), 6),
                }
            )
    return payload


def _build_instrument_regime_rows(rows: Sequence[dict[str, Any]], *, regime_mode: str) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for dimension in _regime_dimensions(regime_mode):
        grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row["sample_split"]), str(row["instrument"]), str(row["timing_within_asia"]), str(row[dimension])), []).append(row)
        for key in sorted(grouped):
            if key[1] not in {"ES", "NQ", "MES", "MNQ"}:
                continue
            payload.append(
                {
                    "sample_split": key[0],
                    "instrument": key[1],
                    "timing_within_asia": key[2],
                    "regime_dimension": dimension,
                    "regime_value": key[3],
                    **_metric_summary(grouped[key]),
                }
            )
    return payload


def _build_time_cluster_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for dimension in ("decision_year", "decision_month"):
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row["sample_split"]), str(row["timing_within_asia"]), str(row[dimension])), []).append(row)
        for key in sorted(grouped):
            payload.append(
                {
                    "sample_split": key[0],
                    "timing_within_asia": key[1],
                    "time_dimension": dimension,
                    "time_value": key[2],
                    **_metric_summary(grouped[key]),
                }
            )
    return payload


def _build_edge_classification_rows(regime_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in regime_rows:
        key = (str(row["regime_dimension"]), str(row["regime_value"]))
        grouped.setdefault(key, {})[f"{row['sample_split']}::{row['timing_within_asia']}"] = row
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        for timing in ("EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"):
            development = grouped[key].get(f"development::{timing}")
            holdout = grouped[key].get(f"holdout::{timing}")
            if development is None or holdout is None:
                continue
            min_count = min(int(development["row_count"]), int(holdout["row_count"]))
            trust_flag = bool(development["do_not_trust"]) or bool(holdout["do_not_trust"]) or min_count < DO_NOT_TRUST_MIN_ROWS
            if trust_flag:
                edge_class = "DO_NOT_TRUST"
            elif float(development["avg_return_60m"]) > 0.0 and float(holdout["avg_return_60m"]) > 0.0 and float(holdout["win_rate_60m"]) >= 0.52:
                edge_class = "EDGE_ON_CANDIDATE"
            elif float(holdout["avg_return_60m"]) <= 0.0 or float(holdout["win_rate_60m"]) < 0.5:
                edge_class = "EDGE_OFF_CANDIDATE"
            else:
                edge_class = "MIXED"
            payload.append(
                {
                    "regime_dimension": key[0],
                    "regime_value": key[1],
                    "timing_within_asia": timing,
                    "development_row_count": int(development["row_count"]),
                    "holdout_row_count": int(holdout["row_count"]),
                    "min_row_count_across_splits": min_count,
                    "do_not_trust": trust_flag,
                    "development_avg_return_60m": float(development["avg_return_60m"]),
                    "holdout_avg_return_60m": float(holdout["avg_return_60m"]),
                    "development_win_rate_60m": float(development["win_rate_60m"]),
                    "holdout_win_rate_60m": float(holdout["win_rate_60m"]),
                    "dev_holdout_delta_avg_return_60m": round(float(holdout["avg_return_60m"]) - float(development["avg_return_60m"]), 6),
                    "dev_holdout_delta_win_rate_60m": round(float(holdout["win_rate_60m"]) - float(development["win_rate_60m"]), 6),
                    "edge_classification_candidate": edge_class,
                }
            )
    return payload


def _baseline_map(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    payload: dict[tuple[str, str], dict[str, Any]] = {}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row["sample_split"]), str(row["timing_within_asia"])), []).append(row)
    for key, bucket in grouped.items():
        payload[key] = _metric_summary(bucket)
    return payload


def _metric_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["forward_return_60m"]) for row in rows if row["forward_return_60m"] is not None]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value <= 0.0]
    mae_values = [float(row["mae_60m_points"]) for row in rows]
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    payload = {
        "row_count": len(returns),
        "win_rate_60m": round(sum(1 for value in returns if value > 0.0) / len(returns), 6) if returns else 0.0,
        "avg_return_60m": round(_mean(returns), 6),
        "avg_win_60m": round(avg_win, 6),
        "avg_loss_60m": round(avg_loss, 6),
        "payoff_ratio": round(avg_win / abs(avg_loss), 6) if avg_loss < 0.0 else None,
        "avg_mae_60m": round(_mean(mae_values), 6),
        "mae_p75": round(_quantile(mae_values, 0.75), 6) if mae_values else 0.0,
        "mae_p90": round(_quantile(mae_values, 0.90), 6) if mae_values else 0.0,
        "loss_tail_p10": round(_quantile(returns, 0.10), 6) if returns else 0.0,
        "do_not_trust": len(returns) < DO_NOT_TRUST_MIN_ROWS,
    }
    return payload


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


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
    summary_payload: dict[str, Any],
    regime_rows: Sequence[dict[str, Any]],
    instrument_rows: Sequence[dict[str, Any]],
    edge_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 5",
        "",
        "Index-only regime-conditioned analysis on the fixed evidence stack.",
        "",
        "Example holdout edge-on candidates:",
    ]
    for row in edge_rows:
        if row["edge_classification_candidate"] == "EDGE_ON_CANDIDATE":
            lines.append(
                f"- {row['timing_within_asia']} {row['regime_dimension']}={row['regime_value']} "
                f"holdout_avg60={row['holdout_avg_return_60m']} holdout_wr={row['holdout_win_rate_60m']}"
            )
    lines.extend(["", "Summary JSON:", f"`{summary_payload['artifact_paths']['summary_json']}`"])
    return "\n".join(lines) + "\n"
