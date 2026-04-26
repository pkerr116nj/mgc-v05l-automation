"""Focused slice analysis on top of probabilistic Asia Drift pass 1 outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest


def run_probabilistic_pass2(
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

    slice_probability_rows = _build_slice_probability_rows(merged_rows)
    slice_distribution_rows = _build_slice_distribution_rows(merged_rows)
    slice_confirmation_rows = _build_slice_confirmation_rows(merged_rows)
    slice_regime_rows = _build_slice_regime_rows(merged_rows)

    slice_probability_csv = layout["reports"] / "asia_drift_probabilistic_pass2_slice_probability.csv"
    slice_probability_parquet = layout["reports"] / "asia_drift_probabilistic_pass2_slice_probability.parquet"
    slice_distribution_csv = layout["reports"] / "asia_drift_probabilistic_pass2_slice_mfe_mae_distribution.csv"
    slice_distribution_parquet = layout["reports"] / "asia_drift_probabilistic_pass2_slice_mfe_mae_distribution.parquet"
    slice_confirmation_csv = layout["reports"] / "asia_drift_probabilistic_pass2_slice_confirmation_lift.csv"
    slice_confirmation_parquet = layout["reports"] / "asia_drift_probabilistic_pass2_slice_confirmation_lift.parquet"
    slice_regime_csv = layout["reports"] / "asia_drift_probabilistic_pass2_slice_regime_summary.csv"
    slice_regime_parquet = layout["reports"] / "asia_drift_probabilistic_pass2_slice_regime_summary.parquet"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass2_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass2_summary.md"

    _write_csv(slice_probability_csv, slice_probability_rows)
    _write_csv(slice_distribution_csv, slice_distribution_rows)
    _write_csv(slice_confirmation_csv, slice_confirmation_rows)
    _write_csv(slice_regime_csv, slice_regime_rows)

    materialize_parquet_dataset(slice_probability_parquet, slice_probability_rows)
    materialize_parquet_dataset(slice_distribution_parquet, slice_distribution_rows)
    materialize_parquet_dataset(slice_confirmation_parquet, slice_confirmation_rows)
    materialize_parquet_dataset(slice_regime_parquet, slice_regime_rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass2",
        "source_pass1_root": str(pass1_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
        },
        "artifact_paths": {
            "slice_probability_csv": str(slice_probability_csv),
            "slice_probability_parquet": str(slice_probability_parquet),
            "slice_distribution_csv": str(slice_distribution_csv),
            "slice_distribution_parquet": str(slice_distribution_parquet),
            "slice_confirmation_csv": str(slice_confirmation_csv),
            "slice_confirmation_parquet": str(slice_confirmation_parquet),
            "slice_regime_csv": str(slice_regime_csv),
            "slice_regime_parquet": str(slice_regime_parquet),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": {
            "holdout_index_probability_60m": _headline_probability(
                slice_probability_rows,
                sample_split="holdout",
                slice_name="instrument_cluster",
                slice_value="INDEX",
            ),
            "holdout_metals_probability_60m": _headline_probability(
                slice_probability_rows,
                sample_split="holdout",
                slice_name="instrument_cluster",
                slice_value="METALS",
            ),
            "holdout_pre_london_probability_60m": _headline_probability(
                slice_probability_rows,
                sample_split="holdout",
                slice_name="timing_within_asia",
                slice_value="PRE_LONDON",
            ),
            "holdout_60m_agree_probability_60m": _headline_probability(
                slice_probability_rows,
                sample_split="holdout",
                slice_name="agreement_60m",
                slice_value="AGREE",
            ),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(_render_markdown(summary_payload, slice_probability_rows, slice_confirmation_rows), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass2",
            "source_pass1_root": str(pass1_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "merged_row_count": len(merged_rows),
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


def _coerce_row(row: dict[str, str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if value is None or value == "":
            payload[key] = None
            continue
        lowered = value.lower()
        if lowered in {"true", "false"}:
            payload[key] = lowered == "true"
            continue
        try:
            if "." not in value and value.lstrip("-").isdigit():
                payload[key] = int(value)
                continue
            payload[key] = float(value)
            continue
        except ValueError:
            payload[key] = value
    return payload


def _merge_rows(candidate_rows: Sequence[dict[str, Any]], outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    outcomes_by_id = {str(row["candidate_id"]): row for row in outcome_rows}
    merged: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        outcome = outcomes_by_id.get(str(candidate["candidate_id"]))
        if outcome is None:
            continue
        row = {
            **candidate,
            **outcome,
            "instrument_cluster": _instrument_cluster(str(candidate["instrument"])),
            "agreement_60m": _agreement_bucket(str(candidate["direction_60m_state"]), str(candidate["direction"])),
            "agreement_240m": _agreement_bucket(str(candidate["direction_240m_state"]), str(candidate["direction"])),
            "compression_bucket": _compression_bucket(str(candidate["compression_state"])),
            "disorder_bucket": _disorder_bucket(str(candidate["disorder_state"])),
            "timing_within_asia": _timing_bucket(str(candidate["subphase"])),
        }
        merged.append(row)
    return merged


def _instrument_cluster(instrument: str) -> str:
    if instrument in {"GC", "MGC"}:
        return "METALS"
    return "INDEX"


def _agreement_bucket(state: str, direction: str) -> str:
    if direction == "LONG":
        return "AGREE" if state == "UP" else "NOT_AGREE"
    return "AGREE" if state == "DOWN" else "NOT_AGREE"


def _compression_bucket(state: str) -> str:
    return "COMPRESSED" if state in {"COMPRESSED", "COMPRESSED_DRIFT_EXPANSION"} else "NOT_COMPRESSED"


def _disorder_bucket(state: str) -> str:
    if state in {"POST_SPIKE", "POST_SPIKE_AND_CHOP"}:
        return "POST_SPIKE_DISORDER"
    if state == "ORDERLY":
        return "STABLE"
    return "OTHER_DISORDER"


def _timing_bucket(subphase: str) -> str:
    if subphase == "ASIA_DRIFT_BUILD":
        return "EARLY_ASIA"
    if subphase == "ASIA_DRIFT_MATURE":
        return "LATE_ASIA"
    if subphase == "ASIA_DRIFT_PRE_HANDOFF":
        return "PRE_LONDON"
    return "OTHER"


def _build_slice_probability_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name in _slice_dimensions():
        payload.extend(
            _group_metric_rows(
                rows,
                group_keys=("sample_split", slice_name),
                rename_keys={slice_name: "slice_value"},
                static_values={"slice_name": slice_name},
            )
        )
    return payload


def _build_slice_distribution_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name in _slice_dimensions():
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row["sample_split"]), str(row[slice_name])), []).append(row)
        for key in sorted(grouped):
            bucket = grouped[key]
            mfe_values = [float(row["mfe_60m_points"]) for row in bucket]
            mae_values = [float(row["mae_60m_points"]) for row in bucket]
            payload.append(
                {
                    "sample_split": key[0],
                    "slice_name": slice_name,
                    "slice_value": key[1],
                    "row_count": len(bucket),
                    "mfe_p25": round(_quantile(mfe_values, 0.25), 6),
                    "mfe_p50": round(_quantile(mfe_values, 0.50), 6),
                    "mfe_p75": round(_quantile(mfe_values, 0.75), 6),
                    "mfe_p90": round(_quantile(mfe_values, 0.90), 6),
                    "mae_p25": round(_quantile(mae_values, 0.25), 6),
                    "mae_p50": round(_quantile(mae_values, 0.50), 6),
                    "mae_p75": round(_quantile(mae_values, 0.75), 6),
                    "mae_p90": round(_quantile(mae_values, 0.90), 6),
                }
            )
    return payload


def _build_slice_confirmation_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name in _slice_dimensions():
        payload.extend(
            _group_metric_rows(
                rows,
                group_keys=("sample_split", slice_name, "cross_asset_confirmation"),
                rename_keys={slice_name: "slice_value"},
                static_values={"slice_name": slice_name},
            )
        )
    return payload


def _build_slice_regime_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for slice_name in _slice_dimensions():
        payload.extend(
            _group_metric_rows(
                rows,
                group_keys=("sample_split", slice_name, "daily_regime_bucket"),
                rename_keys={slice_name: "slice_value"},
                static_values={"slice_name": slice_name},
            )
        )
    return payload


def _slice_dimensions() -> tuple[str, ...]:
    return (
        "instrument_cluster",
        "agreement_60m",
        "agreement_240m",
        "compression_bucket",
        "disorder_bucket",
        "timing_within_asia",
    )


def _group_metric_rows(
    rows: Sequence[dict[str, Any]],
    *,
    group_keys: Sequence[str],
    rename_keys: dict[str, str] | None = None,
    static_values: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rename_keys = rename_keys or {}
    static_values = static_values or {}
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(tuple(row[key] for key in group_keys), []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = dict(static_values)
        for index, group_key in enumerate(group_keys):
            record[rename_keys.get(group_key, group_key)] = key[index]
        record.update(
            {
                "row_count": len(bucket),
                "continuation_probability_60m": round(
                    sum(1 for item in bucket if item["continuation_60m"]) / max(len(bucket), 1),
                    6,
                ),
                "avg_forward_return_15m": round(_mean(item["forward_return_15m"] for item in bucket), 6),
                "avg_forward_return_60m": round(_mean(item["forward_return_60m"] for item in bucket), 6),
                "avg_session_end_return": round(_mean(item["session_end_return"] for item in bucket), 6),
                "avg_mfe_60m_points": round(_mean(item["mfe_60m_points"] for item in bucket), 6),
                "avg_mae_60m_points": round(_mean(item["mae_60m_points"] for item in bucket), 6),
            }
        )
        payload.append(record)
    return payload


def _headline_probability(
    rows: Sequence[dict[str, Any]],
    *,
    sample_split: str,
    slice_name: str,
    slice_value: str,
) -> float:
    for row in rows:
        if (
            row["sample_split"] == sample_split
            and row["slice_name"] == slice_name
            and row["slice_value"] == slice_value
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
    slice_probability_rows: Sequence[dict[str, Any]],
    slice_confirmation_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 2",
        "",
        f"- Source pass1 root: `{summary_payload['source_pass1_root']}`",
        f"- Merged rows: `{summary_payload['row_counts']['merged_rows']}`",
        "",
        "## Headline",
        f"- Holdout index continuation probability: `{summary_payload['headline']['holdout_index_probability_60m']}`",
        f"- Holdout metals continuation probability: `{summary_payload['headline']['holdout_metals_probability_60m']}`",
        f"- Holdout pre-London continuation probability: `{summary_payload['headline']['holdout_pre_london_probability_60m']}`",
        f"- Holdout 60m-agree continuation probability: `{summary_payload['headline']['holdout_60m_agree_probability_60m']}`",
        "",
        "## Selected Slices",
    ]
    for row in slice_probability_rows:
        if row["sample_split"] != "holdout":
            continue
        if row["slice_name"] not in {"instrument_cluster", "timing_within_asia", "agreement_60m"}:
            continue
        lines.append(
            f"- `{row['slice_name']}={row['slice_value']}` count={row['row_count']} "
            f"p60={row['continuation_probability_60m']} avg60={row['avg_forward_return_60m']}"
        )
    lines.extend(["", "## Confirmation Within Slices"])
    for row in slice_confirmation_rows:
        if row["sample_split"] != "holdout":
            continue
        if row["slice_name"] not in {"instrument_cluster", "timing_within_asia"}:
            continue
        lines.append(
            f"- `{row['slice_name']}={row['slice_value']}` confirmation={row['cross_asset_confirmation']} "
            f"count={row['row_count']} p60={row['continuation_probability_60m']}"
        )
    return "\n".join(lines) + "\n"
