"""Simplified regime consolidation for the Asia Drift continuation evidence stack."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass1 import _coerce_ts, _quantile
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
FOCUS_INSTRUMENTS = ("ES", "NQ")
DO_NOT_TRUST_MIN_ROWS = 100
INSTRUMENT_MIN_ROWS = 50


def run_probabilistic_pass6(
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
        if row["instrument"] in INDEX_SYMBOLS
        and row["primary_stack_only"]
        and row["timing_within_asia"] in {"EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"}
    ]

    rows_with_vix = attach_vix_asof(filtered_rows, vix_rows=load_vol_regime_rows(warehouse_root))
    enriched_rows = [_enrich_environment_row(row) for row in rows_with_vix]

    environment_expectancy_rows = _build_environment_expectancy_rows(enriched_rows)
    instrument_comparison_rows = _build_instrument_comparison_rows(enriched_rows)
    classification_rows = _build_classification_rows(environment_expectancy_rows, instrument_comparison_rows)
    edge_on_rows = [row for row in classification_rows if row["classification"] == "EDGE_ON" and not row["do_not_trust"]][:5]
    edge_off_rows = [row for row in classification_rows if row["classification"] == "EDGE_OFF" and not row["do_not_trust"]][:5]

    expectancy_csv = layout["reports"] / "asia_drift_probabilistic_pass6_environment_expectancy.csv"
    expectancy_parquet = layout["reports"] / "asia_drift_probabilistic_pass6_environment_expectancy.parquet"
    instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass6_instrument_comparison.csv"
    instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass6_instrument_comparison.parquet"
    classification_csv = layout["reports"] / "asia_drift_probabilistic_pass6_classification.csv"
    classification_parquet = layout["reports"] / "asia_drift_probabilistic_pass6_classification.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass6_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass6_summary.md"

    for path, rows in [
        (expectancy_csv, environment_expectancy_rows),
        (instrument_csv, instrument_comparison_rows),
        (classification_csv, classification_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (expectancy_parquet, environment_expectancy_rows),
        (instrument_parquet, instrument_comparison_rows),
        (classification_parquet, classification_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass6",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "assumptions": {
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "collapsed_vix_level": "LOW vs NOT_LOW",
            "collapsed_vix_change": "UP vs NOT_UP",
            "realized_regime": "LOW_VOL vs HIGH_VOL from daily realized proxy",
            "environment_key": "timing_within_asia|volatility_regime|vix_level_simple|vix_change_simple",
            "do_not_trust_min_rows": DO_NOT_TRUST_MIN_ROWS,
            "instrument_min_rows": INSTRUMENT_MIN_ROWS,
            "classification_rules": {
                "EDGE_ON": "not trust-flagged, dev/holdout avg60 > 0, holdout win_rate >= 0.52, and ES/NQ consistency not negative",
                "EDGE_OFF": "not trust-flagged, holdout avg60 <= 0 or holdout win_rate < 0.48, with dev also weak or negative",
                "MIXED": "everything else",
            },
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "filtered_index_stack_rows": len(enriched_rows),
            "environment_rows": len(environment_expectancy_rows),
            "classification_rows": len(classification_rows),
        },
        "headline": {
            "edge_on_environment_count": len(edge_on_rows),
            "edge_off_environment_count": len(edge_off_rows),
        },
        "artifact_paths": {
            "environment_expectancy_csv": str(expectancy_csv),
            "environment_expectancy_parquet": str(expectancy_parquet),
            "instrument_comparison_csv": str(instrument_csv),
            "instrument_comparison_parquet": str(instrument_parquet),
            "classification_csv": str(classification_csv),
            "classification_parquet": str(classification_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_payload, edge_on_rows=edge_on_rows, edge_off_rows=edge_off_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass6",
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
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _enrich_environment_row(row: dict[str, Any]) -> dict[str, Any]:
    daily_regime_bucket = str(row.get("daily_regime_bucket") or "UNKNOWN")
    volatility_regime = str(row.get("volatility_regime") or _volatility_regime(daily_regime_bucket))
    vix_level_bucket = str(row.get("vix_level_bucket") or "UNKNOWN")
    vix_change_bucket = str(row.get("vix_change_bucket") or "UNKNOWN")
    vix_level_simple = "LOW" if vix_level_bucket == "LOW" else "NOT_LOW"
    vix_change_simple = "UP" if vix_change_bucket == "UP" else "NOT_UP"
    environment_key = "|".join(
        (
            str(row["timing_within_asia"]),
            volatility_regime,
            vix_level_simple,
            vix_change_simple,
        )
    )
    return {
        **row,
        "volatility_regime": volatility_regime,
        "vix_level_simple": vix_level_simple,
        "vix_change_simple": vix_change_simple,
        "environment_key": environment_key,
    }


def _volatility_regime(daily_regime_bucket: str) -> str:
    if daily_regime_bucket.endswith("EXPANDED"):
        return "HIGH_VOL"
    if daily_regime_bucket.endswith("NORMAL"):
        return "LOW_VOL"
    return "UNKNOWN_VOL"


def _build_environment_expectancy_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["sample_split"]), str(row["environment_key"]))].append(row)

    payload: list[dict[str, Any]] = []
    for (sample_split, environment_key), bucket in sorted(grouped.items()):
        timing, realized_regime, vix_level_simple, vix_change_simple = environment_key.split("|")
        metrics = _metric_summary(bucket)
        payload.append(
            {
                "sample_split": sample_split,
                "environment_key": environment_key,
                "timing_within_asia": timing,
                "realized_regime": realized_regime,
                "vix_level_simple": vix_level_simple,
                "vix_change_simple": vix_change_simple,
                **metrics,
            }
        )
    return payload


def _build_instrument_comparison_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row["instrument"]) not in FOCUS_INSTRUMENTS:
            continue
        grouped[(str(row["sample_split"]), str(row["environment_key"]), str(row["instrument"]))].append(row)

    payload: list[dict[str, Any]] = []
    for (sample_split, environment_key, instrument), bucket in sorted(grouped.items()):
        timing, realized_regime, vix_level_simple, vix_change_simple = environment_key.split("|")
        metrics = _metric_summary(bucket)
        payload.append(
            {
                "sample_split": sample_split,
                "environment_key": environment_key,
                "timing_within_asia": timing,
                "realized_regime": realized_regime,
                "vix_level_simple": vix_level_simple,
                "vix_change_simple": vix_change_simple,
                "instrument": instrument,
                **metrics,
            }
        )
    return payload


def _build_classification_rows(
    environment_rows: Sequence[dict[str, Any]],
    instrument_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    env_map: dict[tuple[str, str], dict[str, Any]] = {
        (str(row["sample_split"]), str(row["environment_key"])): row
        for row in environment_rows
    }
    inst_map: dict[tuple[str, str, str], dict[str, Any]] = {
        (str(row["sample_split"]), str(row["environment_key"]), str(row["instrument"])): row
        for row in instrument_rows
    }
    keys = sorted({str(row["environment_key"]) for row in environment_rows})

    payload: list[dict[str, Any]] = []
    for environment_key in keys:
        development = env_map.get(("development", environment_key))
        holdout = env_map.get(("holdout", environment_key))
        if development is None or holdout is None:
            continue
        min_count = min(int(development["row_count"]), int(holdout["row_count"]))
        do_not_trust = bool(development["do_not_trust"]) or bool(holdout["do_not_trust"]) or min_count < DO_NOT_TRUST_MIN_ROWS

        es_holdout = inst_map.get(("holdout", environment_key, "ES"))
        nq_holdout = inst_map.get(("holdout", environment_key, "NQ"))
        consistency = _instrument_consistency(es_holdout, nq_holdout)
        classification = _classify_environment(
            development=development,
            holdout=holdout,
            do_not_trust=do_not_trust,
            instrument_consistency=consistency,
        )
        timing, realized_regime, vix_level_simple, vix_change_simple = environment_key.split("|")
        payload.append(
            {
                "environment_key": environment_key,
                "timing_within_asia": timing,
                "realized_regime": realized_regime,
                "vix_level_simple": vix_level_simple,
                "vix_change_simple": vix_change_simple,
                "development_row_count": int(development["row_count"]),
                "holdout_row_count": int(holdout["row_count"]),
                "min_row_count_across_splits": min_count,
                "development_avg_return_60m": float(development["avg_return_60m"]),
                "holdout_avg_return_60m": float(holdout["avg_return_60m"]),
                "development_win_rate_60m": float(development["win_rate_60m"]),
                "holdout_win_rate_60m": float(holdout["win_rate_60m"]),
                "holdout_avg_mae_60m": float(holdout["avg_mae_60m"]),
                "holdout_mae_p90": float(holdout["mae_p90"]),
                "holdout_avg_win_60m": float(holdout["avg_win_60m"]),
                "holdout_avg_loss_60m": float(holdout["avg_loss_60m"]),
                "holdout_payoff_ratio": holdout["payoff_ratio"],
                "dev_holdout_delta_avg_return_60m": round(float(holdout["avg_return_60m"]) - float(development["avg_return_60m"]), 6),
                "dev_holdout_delta_win_rate_60m": round(float(holdout["win_rate_60m"]) - float(development["win_rate_60m"]), 6),
                "es_holdout_avg_return_60m": None if es_holdout is None else float(es_holdout["avg_return_60m"]),
                "es_holdout_win_rate_60m": None if es_holdout is None else float(es_holdout["win_rate_60m"]),
                "es_holdout_row_count": None if es_holdout is None else int(es_holdout["row_count"]),
                "nq_holdout_avg_return_60m": None if nq_holdout is None else float(nq_holdout["avg_return_60m"]),
                "nq_holdout_win_rate_60m": None if nq_holdout is None else float(nq_holdout["win_rate_60m"]),
                "nq_holdout_row_count": None if nq_holdout is None else int(nq_holdout["row_count"]),
                "instrument_consistency": consistency,
                "do_not_trust": do_not_trust,
                "classification": classification,
            }
        )

    payload.sort(
        key=lambda row: (
            0 if row["classification"] == "EDGE_ON" else 1 if row["classification"] == "EDGE_OFF" else 2,
            -float(row["holdout_avg_return_60m"]),
        )
    )
    return payload


def _instrument_consistency(es_holdout: dict[str, Any] | None, nq_holdout: dict[str, Any] | None) -> str:
    if es_holdout is None or nq_holdout is None:
        return "INSUFFICIENT"
    if int(es_holdout["row_count"]) < INSTRUMENT_MIN_ROWS or int(nq_holdout["row_count"]) < INSTRUMENT_MIN_ROWS:
        return "INSUFFICIENT"
    es_positive = float(es_holdout["avg_return_60m"]) > 0.0
    nq_positive = float(nq_holdout["avg_return_60m"]) > 0.0
    if es_positive and nq_positive:
        return "BOTH_POSITIVE"
    if (not es_positive) and (not nq_positive):
        return "BOTH_NEGATIVE"
    return "MIXED"


def _classify_environment(
    *,
    development: dict[str, Any],
    holdout: dict[str, Any],
    do_not_trust: bool,
    instrument_consistency: str,
) -> str:
    if do_not_trust:
        return "DO_NOT_TRUST"
    dev_avg = float(development["avg_return_60m"])
    holdout_avg = float(holdout["avg_return_60m"])
    dev_wr = float(development["win_rate_60m"])
    holdout_wr = float(holdout["win_rate_60m"])
    if (
        dev_avg > 0.0
        and holdout_avg > 0.0
        and holdout_wr >= 0.52
        and instrument_consistency != "BOTH_NEGATIVE"
    ):
        return "EDGE_ON"
    if (
        (holdout_avg <= 0.0 or holdout_wr < 0.48)
        and (dev_avg <= 0.0 or dev_wr < 0.50)
    ):
        return "EDGE_OFF"
    return "MIXED"


def _metric_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["forward_return_60m"]) for row in rows if row["forward_return_60m"] is not None]
    mae_values = [float(row["mae_60m_points"]) for row in rows if row["mae_60m_points"] is not None]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value <= 0.0]
    avg_win = _mean(wins)
    avg_loss = _mean(losses)
    return {
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


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


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
    *,
    edge_on_rows: Sequence[dict[str, Any]],
    edge_off_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 6",
        "",
        "Simplified regime consolidation using realized regime plus collapsed VIX state.",
        "",
        "## Edge On",
    ]
    for row in edge_on_rows:
        lines.append(
            f"- `{row['environment_key']}` holdout_avg60={row['holdout_avg_return_60m']} "
            f"holdout_wr={row['holdout_win_rate_60m']} mae_p90={row['holdout_mae_p90']} "
            f"consistency={row['instrument_consistency']}"
        )
    lines.extend(["", "## Edge Off"])
    for row in edge_off_rows:
        lines.append(
            f"- `{row['environment_key']}` holdout_avg60={row['holdout_avg_return_60m']} "
            f"holdout_wr={row['holdout_win_rate_60m']} mae_p90={row['holdout_mae_p90']} "
            f"consistency={row['instrument_consistency']}"
        )
    lines.extend(["", "Summary JSON:", f"`{summary_payload['artifact_paths']['summary_json']}`"])
    return "\n".join(lines) + "\n"
