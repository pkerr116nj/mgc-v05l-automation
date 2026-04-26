"""Decision-layer structure for the simplified Asia Drift regime map."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass2 import _coerce_row, _merge_rows
from .probabilistic_pass3 import _with_stack_flags
from .probabilistic_pass6 import _enrich_environment_row, _mean


INDEX_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
FOCUS_INSTRUMENTS = ("ES", "NQ")
STATE_MAP = {
    "EDGE_ON": "TRADE_FAVORABLE",
    "EDGE_OFF": "DO_NOT_TRADE",
    "MIXED": "TRADE_NEUTRAL",
    "DO_NOT_TRUST": "TRADE_NEUTRAL",
}


def run_probabilistic_pass7(
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
    classification_lookup = _load_state_lookup(pass6_classification_csv)
    decision_rows = [
        {
            **row,
            "decision_state": classification_lookup.get(str(row["environment_key"]), "TRADE_NEUTRAL"),
        }
        for row in enriched_rows
    ]

    state_rows = _build_state_rows(decision_rows)
    comparison_rows = _build_comparison_rows(decision_rows)
    instrument_rows = _build_instrument_rows(decision_rows)

    state_csv = layout["reports"] / "asia_drift_probabilistic_pass7_state_classification.csv"
    state_parquet = layout["reports"] / "asia_drift_probabilistic_pass7_state_classification.parquet"
    comparison_csv = layout["reports"] / "asia_drift_probabilistic_pass7_performance_comparison.csv"
    comparison_parquet = layout["reports"] / "asia_drift_probabilistic_pass7_performance_comparison.parquet"
    instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass7_instrument_breakdown.csv"
    instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass7_instrument_breakdown.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass7_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass7_summary.md"

    for path, rows in [
        (state_csv, state_rows),
        (comparison_csv, comparison_rows),
        (instrument_csv, instrument_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (state_parquet, state_rows),
        (comparison_parquet, comparison_rows),
        (instrument_parquet, instrument_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass7",
        "source_pass1_root": str(pass1_root),
        "warehouse_root": str(warehouse_root),
        "source_pass6_classification_csv": str(pass6_classification_csv),
        "assumptions": {
            "stack_filter": "INDEX + 60m AGREE + confirmation + NOT_COMPRESSED",
            "state_mapping": STATE_MAP,
            "comparison_sets": [
                "ALL_TRADES",
                "TRADE_FAVORABLE_ONLY",
                "EXCLUDE_DO_NOT_TRADE",
            ],
        },
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "decision_rows": len(decision_rows),
            "state_rows": len(state_rows),
            "comparison_rows": len(comparison_rows),
            "instrument_rows": len(instrument_rows),
        },
        "artifact_paths": {
            "state_csv": str(state_csv),
            "state_parquet": str(state_parquet),
            "comparison_csv": str(comparison_csv),
            "comparison_parquet": str(comparison_parquet),
            "instrument_csv": str(instrument_csv),
            "instrument_parquet": str(instrument_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(_render_markdown(state_rows=state_rows, comparison_rows=comparison_rows), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass7",
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


def _load_state_lookup(path: Path) -> dict[str, str]:
    rows = list(csv.DictReader(path.open("r", newline="", encoding="utf-8")))
    return {
        str(row["environment_key"]): str(STATE_MAP.get(str(row["classification"]), "TRADE_NEUTRAL"))
        for row in rows
    }


def _build_state_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["sample_split"]), str(row["decision_state"]))].append(row)

    payload: list[dict[str, Any]] = []
    for (sample_split, decision_state), bucket in sorted(grouped.items()):
        payload.append(
            {
                "sample_split": sample_split,
                "decision_state": decision_state,
                **_metric_summary(bucket),
            }
        )
    return payload


def _build_comparison_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    scenarios = {
        "ALL_TRADES": lambda row: True,
        "TRADE_FAVORABLE_ONLY": lambda row: str(row["decision_state"]) == "TRADE_FAVORABLE",
        "EXCLUDE_DO_NOT_TRADE": lambda row: str(row["decision_state"]) != "DO_NOT_TRADE",
    }
    for sample_split in ("development", "holdout"):
        bucket = [row for row in rows if str(row["sample_split"]) == sample_split]
        for scenario_name, include in scenarios.items():
            scenario_bucket = [row for row in bucket if include(row)]
            payload.append(
                {
                    "sample_split": sample_split,
                    "scenario": scenario_name,
                    **_metric_summary(scenario_bucket),
                }
            )
    return payload


def _build_instrument_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    scenarios = {
        "ALL_TRADES": lambda row: True,
        "TRADE_FAVORABLE_ONLY": lambda row: str(row["decision_state"]) == "TRADE_FAVORABLE",
        "EXCLUDE_DO_NOT_TRADE": lambda row: str(row["decision_state"]) != "DO_NOT_TRADE",
    }
    for sample_split in ("development", "holdout"):
        for instrument in FOCUS_INSTRUMENTS:
            base_bucket = [
                row for row in rows
                if str(row["sample_split"]) == sample_split and str(row["instrument"]) == instrument
            ]
            for scenario_name, include in scenarios.items():
                scenario_bucket = [row for row in base_bucket if include(row)]
                payload.append(
                    {
                        "sample_split": sample_split,
                        "instrument": instrument,
                        "scenario": scenario_name,
                        **_metric_summary(scenario_bucket),
                    }
                )
    return payload


def _metric_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    returns = [float(row["forward_return_60m"]) for row in rows if row["forward_return_60m"] is not None]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value <= 0.0]
    mae_values = [float(row["mae_60m_points"]) for row in rows if row["mae_60m_points"] is not None]
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
    }


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


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


def _render_markdown(*, state_rows: Sequence[dict[str, Any]], comparison_rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 7",
        "",
        "Decision-layer structure from the simplified regime map.",
        "",
        "## State Summary",
    ]
    for row in state_rows:
        if row["sample_split"] != "holdout":
            continue
        lines.append(
            f"- {row['decision_state']}: rows={row['row_count']} avg60={row['avg_return_60m']} "
            f"wr={row['win_rate_60m']} payoff={row['payoff_ratio']} mae_p90={row['mae_p90']}"
        )
    lines.extend(["", "## Scenario Comparison"])
    for row in comparison_rows:
        if row["sample_split"] != "holdout":
            continue
        lines.append(
            f"- {row['scenario']}: rows={row['row_count']} avg60={row['avg_return_60m']} "
            f"wr={row['win_rate_60m']} payoff={row['payoff_ratio']} mae_p90={row['mae_p90']}"
        )
    return "\n".join(lines) + "\n"
