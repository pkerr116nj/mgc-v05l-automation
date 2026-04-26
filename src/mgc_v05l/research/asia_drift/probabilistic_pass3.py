"""Combined evidence-stack analysis for Asia Drift probabilistic research."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Sequence

from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass2 import _coerce_row, _merge_rows


DO_NOT_TRUST_MIN_ROWS = 100


def run_probabilistic_pass3(
    *,
    pass1_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "asia_drift_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "asia_drift_probabilistic_pass1_outcomes.csv")
    merged_rows = _merge_rows(candidate_rows, outcome_rows)

    combined_rows = _with_stack_flags(merged_rows)

    combined_condition_rows = _build_combined_condition_rows(combined_rows)
    by_cluster_rows = _build_stack_group_rows(combined_rows, group_keys=("sample_split", "instrument_cluster", "timing_within_asia"))
    by_instrument_rows = _build_stack_group_rows(combined_rows, group_keys=("sample_split", "instrument", "timing_within_asia"))
    sample_count_rows = _build_sample_count_rows(combined_condition_rows)
    tail_rows = _build_tail_rows(combined_rows)
    regime_rows = _build_regime_rows(combined_rows)

    combined_condition_csv = layout["reports"] / "asia_drift_probabilistic_pass3_combined_conditions.csv"
    combined_condition_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_combined_conditions.parquet"
    by_cluster_csv = layout["reports"] / "asia_drift_probabilistic_pass3_cluster_summary.csv"
    by_cluster_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_cluster_summary.parquet"
    by_instrument_csv = layout["reports"] / "asia_drift_probabilistic_pass3_instrument_summary.csv"
    by_instrument_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_instrument_summary.parquet"
    sample_count_csv = layout["reports"] / "asia_drift_probabilistic_pass3_sample_counts.csv"
    sample_count_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_sample_counts.parquet"
    tail_csv = layout["reports"] / "asia_drift_probabilistic_pass3_tail_tables.csv"
    tail_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_tail_tables.parquet"
    regime_csv = layout["reports"] / "asia_drift_probabilistic_pass3_regime_summary.csv"
    regime_parquet = layout["reports"] / "asia_drift_probabilistic_pass3_regime_summary.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass3_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass3_summary.md"

    for path, rows in [
        (combined_condition_csv, combined_condition_rows),
        (by_cluster_csv, by_cluster_rows),
        (by_instrument_csv, by_instrument_rows),
        (sample_count_csv, sample_count_rows),
        (tail_csv, tail_rows),
        (regime_csv, regime_rows),
    ]:
        _write_csv(path, rows)
    for path, rows in [
        (combined_condition_parquet, combined_condition_rows),
        (by_cluster_parquet, by_cluster_rows),
        (by_instrument_parquet, by_instrument_rows),
        (sample_count_parquet, sample_count_rows),
        (tail_parquet, tail_rows),
        (regime_parquet, regime_rows),
    ]:
        materialize_parquet_dataset(path, rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass3",
        "source_pass1_root": str(pass1_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(combined_rows),
        },
        "assumptions": {
            "do_not_trust_min_rows": DO_NOT_TRUST_MIN_ROWS,
        },
        "artifact_paths": {
            "combined_condition_csv": str(combined_condition_csv),
            "combined_condition_parquet": str(combined_condition_parquet),
            "by_cluster_csv": str(by_cluster_csv),
            "by_cluster_parquet": str(by_cluster_parquet),
            "by_instrument_csv": str(by_instrument_csv),
            "by_instrument_parquet": str(by_instrument_parquet),
            "sample_count_csv": str(sample_count_csv),
            "sample_count_parquet": str(sample_count_parquet),
            "tail_csv": str(tail_csv),
            "tail_parquet": str(tail_parquet),
            "regime_csv": str(regime_csv),
            "regime_parquet": str(regime_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": {
            "holdout_primary_stack_early_asia_probability": _headline_probability(
                combined_condition_rows,
                sample_split="holdout",
                timing_within_asia="EARLY_ASIA",
                primary_stack_only=True,
            ),
            "holdout_primary_stack_late_asia_probability": _headline_probability(
                combined_condition_rows,
                sample_split="holdout",
                timing_within_asia="LATE_ASIA",
                primary_stack_only=True,
            ),
            "holdout_primary_stack_pre_london_probability": _headline_probability(
                combined_condition_rows,
                sample_split="holdout",
                timing_within_asia="PRE_LONDON",
                primary_stack_only=True,
            ),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_payload, combined_condition_rows, by_cluster_rows, sample_count_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass3",
            "source_pass1_root": str(pass1_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "merged_row_count": len(combined_rows),
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


def _with_stack_flags(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        primary_stack = (
            row["agreement_60m"] == "AGREE"
            and bool(row["cross_asset_confirmation"])
            and row["compression_bucket"] == "NOT_COMPRESSED"
        )
        payload.append(
            {
                **row,
                "primary_stack_only": primary_stack,
                "primary_stack_label": (
                    f"60m={row['agreement_60m']}|confirm={bool(row['cross_asset_confirmation'])}"
                    f"|compression={row['compression_bucket']}|timing={row['timing_within_asia']}"
                ),
            }
        )
    return payload


def _build_combined_condition_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    keys = (
        "sample_split",
        "timing_within_asia",
        "agreement_60m",
        "cross_asset_confirmation",
        "compression_bucket",
    )
    for row in rows:
        grouped.setdefault(tuple(row[key] for key in keys), []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = {
            "sample_split": key[0],
            "timing_within_asia": key[1],
            "agreement_60m": key[2],
            "cross_asset_confirmation": key[3],
            "compression_bucket": key[4],
            "primary_stack_only": (
                key[1] in {"EARLY_ASIA", "LATE_ASIA", "PRE_LONDON"}
                and key[2] == "AGREE"
                and bool(key[3])
                and key[4] == "NOT_COMPRESSED"
            ),
        }
        record.update(_metric_summary(bucket))
        payload.append(record)
    return payload


def _build_stack_group_rows(rows: Sequence[dict[str, Any]], *, group_keys: Sequence[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        if not row["primary_stack_only"]:
            continue
        grouped.setdefault(tuple(row[key] for key in group_keys), []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = {group_keys[idx]: value for idx, value in enumerate(key)}
        record["primary_stack_only"] = True
        record.update(_metric_summary(bucket))
        payload.append(record)
    return payload


def _build_sample_count_rows(combined_condition_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in combined_condition_rows:
        key = (
            row["timing_within_asia"],
            row["agreement_60m"],
            row["cross_asset_confirmation"],
            row["compression_bucket"],
        )
        grouped.setdefault(key, []).append(row)
    for key in sorted(grouped):
        bucket = grouped[key]
        development_count = next((int(row["row_count"]) for row in bucket if row["sample_split"] == "development"), 0)
        holdout_count = next((int(row["row_count"]) for row in bucket if row["sample_split"] == "holdout"), 0)
        minimum_count = min(development_count, holdout_count)
        payload.append(
            {
                "timing_within_asia": key[0],
                "agreement_60m": key[1],
                "cross_asset_confirmation": key[2],
                "compression_bucket": key[3],
                "development_count": development_count,
                "holdout_count": holdout_count,
                "minimum_count_across_splits": minimum_count,
                "do_not_trust": minimum_count < DO_NOT_TRUST_MIN_ROWS,
            }
        )
    return payload


def _build_tail_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for focus in ("all_conditions", "primary_stack_only"):
        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            if focus == "primary_stack_only" and not row["primary_stack_only"]:
                continue
            key = (str(row["sample_split"]), str(row["timing_within_asia"]))
            grouped.setdefault(key, []).append(row)
        for key in sorted(grouped):
            bucket = grouped[key]
            forward_60m = [float(row["forward_return_60m"]) for row in bucket if row["forward_return_60m"] is not None]
            mae = [float(row["mae_60m_points"]) for row in bucket]
            payload.append(
                {
                    "focus": focus,
                    "sample_split": key[0],
                    "timing_within_asia": key[1],
                    "row_count": len(bucket),
                    "forward_return_60m_p10": round(_quantile(forward_60m, 0.10), 6),
                    "forward_return_60m_p25": round(_quantile(forward_60m, 0.25), 6),
                    "mae_p75": round(_quantile(mae, 0.75), 6),
                    "mae_p90": round(_quantile(mae, 0.90), 6),
                    "do_not_trust": len(bucket) < DO_NOT_TRUST_MIN_ROWS,
                }
            )
    return payload


def _build_regime_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        if not row["primary_stack_only"]:
            continue
        key = (
            str(row["sample_split"]),
            str(row["instrument_cluster"]),
            str(row["timing_within_asia"]),
            str(row["daily_regime_bucket"]),
        )
        grouped.setdefault(key, []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = {
            "sample_split": key[0],
            "instrument_cluster": key[1],
            "timing_within_asia": key[2],
            "daily_regime_bucket": key[3],
            "primary_stack_only": True,
        }
        record.update(_metric_summary(bucket))
        payload.append(record)
    return payload


def _metric_summary(bucket: Sequence[dict[str, Any]]) -> dict[str, Any]:
    row_count = len(bucket)
    forward_60m = [float(row["forward_return_60m"]) for row in bucket if row["forward_return_60m"] is not None]
    mae = [float(row["mae_60m_points"]) for row in bucket]
    mfe = [float(row["mfe_60m_points"]) for row in bucket]
    return {
        "row_count": row_count,
        "continuation_probability_60m": round(
            sum(1 for item in bucket if item["continuation_60m"]) / max(row_count, 1),
            6,
        ),
        "avg_forward_return_15m": round(_mean(item["forward_return_15m"] for item in bucket), 6),
        "avg_forward_return_60m": round(_mean(item["forward_return_60m"] for item in bucket), 6),
        "avg_session_end_return": round(_mean(item["session_end_return"] for item in bucket), 6),
        "avg_mfe_60m_points": round(_mean(item["mfe_60m_points"] for item in bucket), 6),
        "avg_mae_60m_points": round(_mean(item["mae_60m_points"] for item in bucket), 6),
        "forward_return_60m_p10": round(_quantile(forward_60m, 0.10), 6),
        "forward_return_60m_p25": round(_quantile(forward_60m, 0.25), 6),
        "mfe_p50": round(_quantile(mfe, 0.50), 6),
        "mfe_p90": round(_quantile(mfe, 0.90), 6),
        "mae_p50": round(_quantile(mae, 0.50), 6),
        "mae_p90": round(_quantile(mae, 0.90), 6),
        "do_not_trust": row_count < DO_NOT_TRUST_MIN_ROWS,
    }


def _headline_probability(
    rows: Sequence[dict[str, Any]],
    *,
    sample_split: str,
    timing_within_asia: str,
    primary_stack_only: bool,
) -> float:
    for row in rows:
        if (
            row["sample_split"] == sample_split
            and row["timing_within_asia"] == timing_within_asia
            and bool(row["primary_stack_only"]) == primary_stack_only
        ):
            return float(row["continuation_probability_60m"])
    return 0.0


def _mean(values: Sequence[Any] | Any) -> float:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return 0.0
    return sum(filtered) / len(filtered)


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


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
            writer.writerow(_json_ready(row))


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _render_markdown(
    summary_payload: dict[str, Any],
    combined_condition_rows: Sequence[dict[str, Any]],
    by_cluster_rows: Sequence[dict[str, Any]],
    sample_count_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 3",
        "",
        f"- Source pass1 root: `{summary_payload['source_pass1_root']}`",
        f"- Merged rows: `{summary_payload['row_counts']['merged_rows']}`",
        f"- Do-not-trust threshold: `< {summary_payload['assumptions']['do_not_trust_min_rows']} rows`",
        "",
        "## Headline",
        f"- Holdout primary stack early Asia probability: `{summary_payload['headline']['holdout_primary_stack_early_asia_probability']}`",
        f"- Holdout primary stack late Asia probability: `{summary_payload['headline']['holdout_primary_stack_late_asia_probability']}`",
        f"- Holdout primary stack pre-London probability: `{summary_payload['headline']['holdout_primary_stack_pre_london_probability']}`",
        "",
        "## Holdout Primary Stack By Cluster",
    ]
    for row in by_cluster_rows:
        if row["sample_split"] != "holdout":
            continue
        lines.append(
            f"- `{row['instrument_cluster']} {row['timing_within_asia']}` count={row['row_count']} "
            f"p60={row['continuation_probability_60m']} avg60={row['avg_forward_return_60m']} "
            f"do_not_trust={row['do_not_trust']}"
        )
    lines.extend(["", "## Tiny-Sample Flags"])
    for row in sample_count_rows:
        if not row["do_not_trust"]:
            continue
        lines.append(
            f"- `{row['timing_within_asia']} / {row['agreement_60m']} / confirmation={row['cross_asset_confirmation']} / "
            f"{row['compression_bucket']}` min_count={row['minimum_count_across_splits']}"
        )
    lines.extend(["", "## Holdout Combined Conditions"])
    for row in combined_condition_rows:
        if row["sample_split"] != "holdout":
            continue
        if not row["primary_stack_only"]:
            continue
        lines.append(
            f"- `{row['timing_within_asia']}` count={row['row_count']} p60={row['continuation_probability_60m']} "
            f"avg60={row['avg_forward_return_60m']} p10={row['forward_return_60m_p10']} "
            f"mae_p90={row['mae_p90']} do_not_trust={row['do_not_trust']}"
        )
    return "\n".join(lines) + "\n"
