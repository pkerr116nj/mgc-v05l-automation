"""Descriptive slice analysis for U.S. open same-day directional bias."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..asia_drift.probabilistic_pass1 import _mean, _quantile, _write_csv
from ..asia_drift.probabilistic_pass2 import _coerce_row
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest


PASS_VERSION = "us_open_probabilistic_pass2_v1"
MIN_DEVELOPMENT_ROWS = 80
MIN_HOLDOUT_ROWS = 25
HORIZON_LABELS: tuple[str, ...] = ("60m", "120m", "1530", "close")
DAY_TYPE_TREND_UP = "TREND_UP_DAY"
DAY_TYPE_TREND_DOWN = "TREND_DOWN_DAY"
DAY_TYPE_NON_TREND = "NON_TREND_DAY"
OPENING_DRIVE_SIZE_BUCKETS: tuple[str, ...] = (
    "Q1_SMALLEST",
    "Q2_SMALL",
    "Q3_MEDIUM",
    "Q4_LARGE",
    "Q5_LARGEST",
)


def run_probabilistic_pass2(
    *,
    pass1_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv")
    merged_rows = _annotate_rows(_merge_rows(candidate_rows, outcome_rows))

    pooled_rows = _build_comparison_rows(merged_rows, feature_name="ALL", feature_getter=lambda row: "ALL")
    opening_drive_size_rows = _build_comparison_rows(
        merged_rows,
        feature_name="opening_drive_size_bucket",
        feature_getter=lambda row: str(row["opening_drive_size_bucket"]),
    )
    vix_rows = _build_comparison_rows(
        merged_rows,
        feature_name="vix_regime",
        feature_getter=lambda row: f"{row.get('vix_level_bucket', 'UNKNOWN')}|{row.get('vix_change_bucket', 'UNKNOWN')}",
    )
    overnight_alignment_rows = _build_comparison_rows(
        merged_rows,
        feature_name="overnight_alignment",
        feature_getter=lambda row: str(row["overnight_alignment"]),
    )
    cross_index_rows = _build_comparison_rows(
        merged_rows,
        feature_name="cross_index_confirmation",
        feature_getter=lambda row: str(row["cross_index_confirmation_label"]),
    )
    trend_agreement_rows = _build_comparison_rows(
        merged_rows,
        feature_name="trend_60m_agreement",
        feature_getter=lambda row: str(row["trend_60m_agreement_label"]),
    )
    extreme_exclusion_rows = _build_extreme_exclusion_rows(merged_rows)
    dev_holdout_rows = _build_dev_holdout_stability_rows(
        pooled_rows,
        opening_drive_size_rows,
        vix_rows,
        overnight_alignment_rows,
        cross_index_rows,
        trend_agreement_rows,
        extreme_exclusion_rows,
    )
    day_type_rows = _build_day_type_rows(merged_rows)

    pooled_summary_csv = layout["reports"] / "us_open_probabilistic_pass2_pooled_direction_summary.csv"
    opening_drive_size_csv = layout["reports"] / "us_open_probabilistic_pass2_opening_drive_size_summary.csv"
    vix_conditioned_csv = layout["reports"] / "us_open_probabilistic_pass2_vix_conditioned_summary.csv"
    overnight_alignment_csv = layout["reports"] / "us_open_probabilistic_pass2_overnight_alignment_summary.csv"
    cross_index_csv = layout["reports"] / "us_open_probabilistic_pass2_cross_index_summary.csv"
    trend_agreement_csv = layout["reports"] / "us_open_probabilistic_pass2_trend_agreement_summary.csv"
    extreme_exclusion_csv = layout["reports"] / "us_open_probabilistic_pass2_extreme_exclusion_summary.csv"
    dev_holdout_stability_csv = layout["reports"] / "us_open_probabilistic_pass2_dev_holdout_stability.csv"
    day_type_summary_csv = layout["reports"] / "us_open_probabilistic_pass2_day_type_summary.csv"
    summary_json = layout["reports"] / "us_open_probabilistic_pass2_summary.json"
    summary_markdown = layout["reports"] / "us_open_probabilistic_pass2_summary.md"

    _write_csv(pooled_summary_csv, pooled_rows)
    _write_csv(opening_drive_size_csv, opening_drive_size_rows)
    _write_csv(vix_conditioned_csv, vix_rows)
    _write_csv(overnight_alignment_csv, overnight_alignment_rows)
    _write_csv(cross_index_csv, cross_index_rows)
    _write_csv(trend_agreement_csv, trend_agreement_rows)
    _write_csv(extreme_exclusion_csv, extreme_exclusion_rows)
    _write_csv(dev_holdout_stability_csv, dev_holdout_rows)
    _write_csv(day_type_summary_csv, day_type_rows)

    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_pooled_direction_summary.parquet", pooled_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_opening_drive_size_summary.parquet", opening_drive_size_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_vix_conditioned_summary.parquet", vix_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_overnight_alignment_summary.parquet", overnight_alignment_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_cross_index_summary.parquet", cross_index_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_trend_agreement_summary.parquet", trend_agreement_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_extreme_exclusion_summary.parquet", extreme_exclusion_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_dev_holdout_stability.parquet", dev_holdout_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass2_day_type_summary.parquet", day_type_rows)

    summary_payload = {
        "module": "us_open_probabilistic_pass2",
        "version": PASS_VERSION,
        "source_pass1_root": str(pass1_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
        },
        "sample_rules": {
            "development_min_rows": MIN_DEVELOPMENT_ROWS,
            "holdout_min_rows": MIN_HOLDOUT_ROWS,
        },
        "opening_drive_size_bucket_method": "development absolute opening-drive quintiles applied unchanged to holdout",
        "day_type_definition": {
            DAY_TYPE_TREND_UP: "forward_return_1530_raw > 0 and forward_return_close_raw > 0",
            DAY_TYPE_TREND_DOWN: "forward_return_1530_raw < 0 and forward_return_close_raw < 0",
            DAY_TYPE_NON_TREND: "otherwise",
        },
        "artifact_paths": {
            "pooled_summary_csv": str(pooled_summary_csv),
            "opening_drive_size_csv": str(opening_drive_size_csv),
            "vix_conditioned_csv": str(vix_conditioned_csv),
            "overnight_alignment_csv": str(overnight_alignment_csv),
            "cross_index_csv": str(cross_index_csv),
            "trend_agreement_csv": str(trend_agreement_csv),
            "extreme_exclusion_csv": str(extreme_exclusion_csv),
            "dev_holdout_stability_csv": str(dev_holdout_stability_csv),
            "day_type_summary_csv": str(day_type_summary_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": _build_headline(pooled_rows, day_type_rows, extreme_exclusion_rows),
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(
            summary_payload,
            pooled_rows,
            opening_drive_size_rows,
            vix_rows,
            day_type_rows,
            extreme_exclusion_rows,
        ),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "us_open_probabilistic_pass2",
            "version": PASS_VERSION,
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


def _merge_rows(candidate_rows: Sequence[dict[str, Any]], outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    outcomes_by_id = {str(row["candidate_id"]): row for row in outcome_rows}
    merged: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        outcome = outcomes_by_id.get(str(candidate["candidate_id"]))
        if outcome is None:
            continue
        merged.append({**candidate, **outcome})
    return merged


def _annotate_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    development_sizes = sorted(
        abs(float(row["opening_drive_signed_return_points"]))
        for row in rows
        if str(row.get("sample_split")) == "development" and row.get("opening_drive_signed_return_points") is not None
    )
    quantiles = (
        _quantile(development_sizes, 0.20),
        _quantile(development_sizes, 0.40),
        _quantile(development_sizes, 0.60),
        _quantile(development_sizes, 0.80),
    )
    payload: list[dict[str, Any]] = []
    for row in rows:
        direction = str(row["direction"])
        raw_60m = _raw_return(direction, row.get("forward_return_60m"))
        raw_120m = _raw_return(direction, row.get("forward_return_120m"))
        raw_1530 = _raw_return(direction, row.get("forward_return_1530"))
        raw_close = _raw_return(direction, row.get("forward_return_close"))
        payload.append(
            {
                **row,
                "cluster": _cluster_for_instrument(str(row["instrument"])),
                "opening_drive_size_bucket": _size_bucket(abs(float(row["opening_drive_signed_return_points"])), quantiles),
                "opening_drive_extreme_flag": _size_bucket(abs(float(row["opening_drive_signed_return_points"])), quantiles) == "Q5_LARGEST",
                "overnight_alignment": _overnight_alignment(str(row.get("direction")), str(row.get("overnight_direction"))),
                "trend_60m_agreement_label": _trend_agreement_label(
                    str(row.get("direction")),
                    str(row.get("direction_60m_state")),
                ),
                "cross_index_confirmation_label": "CONFIRMED" if bool(row.get("cross_index_confirmation")) else "UNCONFIRMED",
                "forward_return_60m_raw": raw_60m,
                "forward_return_120m_raw": raw_120m,
                "forward_return_1530_raw": raw_1530,
                "forward_return_close_raw": raw_close,
                "day_type": _classify_day_type(raw_1530, raw_close),
            }
        )
    return payload


def _cluster_for_instrument(instrument: str) -> str:
    if instrument in {"ES", "MES"}:
        return "SPX"
    if instrument in {"NQ", "MNQ"}:
        return "NDX"
    return "OTHER"


def _size_bucket(value: float, quantiles: Sequence[float]) -> str:
    q20, q40, q60, q80 = quantiles
    if value <= q20:
        return "Q1_SMALLEST"
    if value <= q40:
        return "Q2_SMALL"
    if value <= q60:
        return "Q3_MEDIUM"
    if value <= q80:
        return "Q4_LARGE"
    return "Q5_LARGEST"


def _overnight_alignment(direction: str, overnight_direction: str) -> str:
    if overnight_direction not in {"UP", "DOWN"}:
        return "OVERNIGHT_FLAT_OR_UNKNOWN"
    return "ALIGNED" if overnight_direction == direction else "DISAGREE"


def _trend_agreement_label(direction: str, trend_state: str) -> str:
    if trend_state not in {"UP", "DOWN"}:
        return "UNKNOWN"
    return "AGREE" if direction == trend_state else "DISAGREE"


def _raw_return(direction: str, signed_return: Any) -> float | None:
    if signed_return is None:
        return None
    value = float(signed_return)
    return value if direction == "UP" else -value


def _classify_day_type(forward_1530_raw: float | None, forward_close_raw: float | None) -> str:
    if forward_1530_raw is not None and forward_close_raw is not None:
        if forward_1530_raw > 0.0 and forward_close_raw > 0.0:
            return DAY_TYPE_TREND_UP
        if forward_1530_raw < 0.0 and forward_close_raw < 0.0:
            return DAY_TYPE_TREND_DOWN
    return DAY_TYPE_NON_TREND


def _build_comparison_rows(
    rows: Sequence[dict[str, Any]],
    *,
    feature_name: str,
    feature_getter,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for cluster in ("ALL", "SPX", "NDX"):
        cluster_rows = [row for row in rows if cluster == "ALL" or str(row["cluster"]) == cluster]
        feature_values = sorted({str(feature_getter(row)) for row in cluster_rows})
        for feature_value in feature_values:
            bucket = [row for row in cluster_rows if str(feature_getter(row)) == feature_value]
            payload.append(
                _comparison_record(
                    bucket=bucket,
                    cluster=cluster,
                    feature_name=feature_name,
                    feature_value=feature_value,
                )
            )
    return payload


def _build_extreme_exclusion_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for cluster in ("ALL", "SPX", "NDX"):
        cluster_rows = [row for row in rows if cluster == "ALL" or str(row["cluster"]) == cluster]
        payload.append(_comparison_record(bucket=cluster_rows, cluster=cluster, feature_name="extreme_exclusion", feature_value="ALL_ROWS"))
        payload.append(
            _comparison_record(
                bucket=[row for row in cluster_rows if not bool(row["opening_drive_extreme_flag"])],
                cluster=cluster,
                feature_name="extreme_exclusion",
                feature_value="EXCLUDE_Q5_LARGEST",
            )
        )
    return payload


def _comparison_record(
    *,
    bucket: Sequence[dict[str, Any]],
    cluster: str,
    feature_name: str,
    feature_value: str,
) -> dict[str, Any]:
    development = [row for row in bucket if str(row.get("sample_split")) == "development"]
    holdout = [row for row in bucket if str(row.get("sample_split")) == "holdout"]
    record: dict[str, Any] = {
        "cluster": cluster,
        "feature_name": feature_name,
        "feature_value": feature_value,
        "feature_slice": f"{feature_name}={feature_value}",
        "development_row_count": len(development),
        "holdout_row_count": len(holdout),
        "development_sample_status": _sample_status("development", len(development)),
        "holdout_sample_status": _sample_status("holdout", len(holdout)),
        "comparison_status": _comparison_status(len(development), len(holdout)),
    }
    for horizon in HORIZON_LABELS:
        record[f"development_continuation_probability_{horizon}"] = _probability(development, f"continuation_{horizon}")
        record[f"holdout_continuation_probability_{horizon}"] = _probability(holdout, f"continuation_{horizon}")
        record[f"development_reversal_probability_{horizon}"] = _probability(development, f"reversal_{horizon}")
        record[f"holdout_reversal_probability_{horizon}"] = _probability(holdout, f"reversal_{horizon}")
        record[f"development_avg_forward_return_{horizon}"] = round(_mean(row.get(f"forward_return_{horizon}") for row in development), 6)
        record[f"holdout_avg_forward_return_{horizon}"] = round(_mean(row.get(f"forward_return_{horizon}") for row in holdout), 6)
    record["development_avg_day_return_to_close"] = round(_mean(row.get("forward_return_close_raw") for row in development), 6)
    record["holdout_avg_day_return_to_close"] = round(_mean(row.get("forward_return_close_raw") for row in holdout), 6)
    return record


def _build_dev_holdout_stability_rows(*tables: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for table in tables:
        for row in table:
            payload.append(
                {
                    "cluster": row["cluster"],
                    "feature_name": row["feature_name"],
                    "feature_value": row["feature_value"],
                    "feature_slice": row["feature_slice"],
                    "development_row_count": row["development_row_count"],
                    "holdout_row_count": row["holdout_row_count"],
                    "comparison_status": row["comparison_status"],
                    "continuation_probability_60m_delta": round(
                        float(row["holdout_continuation_probability_60m"]) - float(row["development_continuation_probability_60m"]),
                        6,
                    ),
                    "continuation_probability_close_delta": round(
                        float(row["holdout_continuation_probability_close"]) - float(row["development_continuation_probability_close"]),
                        6,
                    ),
                    "avg_forward_return_60m_delta": round(
                        float(row["holdout_avg_forward_return_60m"]) - float(row["development_avg_forward_return_60m"]),
                        6,
                    ),
                    "avg_forward_return_close_delta": round(
                        float(row["holdout_avg_forward_return_close"]) - float(row["development_avg_forward_return_close"]),
                        6,
                    ),
                }
            )
    return payload


def _build_day_type_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    feature_specs = (
        ("opening_drive_direction", lambda row: str(row["direction"])),
        ("opening_drive_size_bucket", lambda row: str(row["opening_drive_size_bucket"])),
        ("vix_regime", lambda row: f"{row.get('vix_level_bucket', 'UNKNOWN')}|{row.get('vix_change_bucket', 'UNKNOWN')}"),
        ("overnight_alignment", lambda row: str(row["overnight_alignment"])),
        ("trend_60m_agreement", lambda row: str(row["trend_60m_agreement_label"])),
        ("cross_index_confirmation", lambda row: str(row["cross_index_confirmation_label"])),
    )
    payload: list[dict[str, Any]] = []
    for cluster in ("ALL", "SPX", "NDX"):
        cluster_rows = [row for row in rows if cluster == "ALL" or str(row["cluster"]) == cluster]
        for feature_name, feature_getter in feature_specs:
            for feature_value in sorted({str(feature_getter(row)) for row in cluster_rows}):
                bucket = [row for row in cluster_rows if str(feature_getter(row)) == feature_value]
                development = [row for row in bucket if str(row.get("sample_split")) == "development"]
                holdout = [row for row in bucket if str(row.get("sample_split")) == "holdout"]
                payload.append(
                    {
                        "cluster": cluster,
                        "feature_name": feature_name,
                        "feature_value": feature_value,
                        "feature_slice": f"{feature_name}={feature_value}",
                        "development_row_count": len(development),
                        "holdout_row_count": len(holdout),
                        "development_sample_status": _sample_status("development", len(development)),
                        "holdout_sample_status": _sample_status("holdout", len(holdout)),
                        "comparison_status": _comparison_status(len(development), len(holdout)),
                        "development_trend_up_probability": _day_type_probability(development, DAY_TYPE_TREND_UP),
                        "holdout_trend_up_probability": _day_type_probability(holdout, DAY_TYPE_TREND_UP),
                        "development_trend_down_probability": _day_type_probability(development, DAY_TYPE_TREND_DOWN),
                        "holdout_trend_down_probability": _day_type_probability(holdout, DAY_TYPE_TREND_DOWN),
                        "development_non_trend_probability": _day_type_probability(development, DAY_TYPE_NON_TREND),
                        "holdout_non_trend_probability": _day_type_probability(holdout, DAY_TYPE_NON_TREND),
                        "development_avg_return_to_close": round(_mean(row.get("forward_return_close_raw") for row in development), 6),
                        "holdout_avg_return_to_close": round(_mean(row.get("forward_return_close_raw") for row in holdout), 6),
                    }
                )
    return payload


def _probability(rows: Sequence[dict[str, Any]], key: str) -> float:
    return round(sum(1 for row in rows if bool(row.get(key))) / max(len(rows), 1), 6) if rows else 0.0


def _day_type_probability(rows: Sequence[dict[str, Any]], day_type: str) -> float:
    return round(sum(1 for row in rows if str(row.get("day_type")) == day_type) / max(len(rows), 1), 6) if rows else 0.0


def _sample_status(sample_split: str, row_count: int) -> str:
    threshold = MIN_DEVELOPMENT_ROWS if sample_split == "development" else MIN_HOLDOUT_ROWS
    return "TRUSTED" if row_count >= threshold else "DO_NOT_TRUST"


def _comparison_status(development_count: int, holdout_count: int) -> str:
    if development_count >= MIN_DEVELOPMENT_ROWS and holdout_count >= MIN_HOLDOUT_ROWS:
        return "TRUSTED"
    return "DO_NOT_TRUST"


def _build_headline(
    pooled_rows: Sequence[dict[str, Any]],
    day_type_rows: Sequence[dict[str, Any]],
    extreme_exclusion_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    pooled_all = next(row for row in pooled_rows if row["cluster"] == "ALL" and row["feature_name"] == "ALL")
    day_type_all = next(
        row
        for row in day_type_rows
        if row["cluster"] == "ALL" and row["feature_name"] == "opening_drive_direction" and row["feature_value"] == "UP"
    )
    extreme_all = next(
        row
        for row in extreme_exclusion_rows
        if row["cluster"] == "ALL" and row["feature_value"] == "EXCLUDE_Q5_LARGEST"
    )
    return {
        "holdout_pooled_continuation_probability_60m": pooled_all["holdout_continuation_probability_60m"],
        "holdout_pooled_avg_forward_return_close": pooled_all["holdout_avg_forward_return_close"],
        "holdout_trend_up_probability_given_up_open": day_type_all["holdout_trend_up_probability"],
        "holdout_excluding_extremes_continuation_probability_60m": extreme_all["holdout_continuation_probability_60m"],
    }


def _render_markdown(
    summary_payload: dict[str, Any],
    pooled_rows: Sequence[dict[str, Any]],
    opening_drive_size_rows: Sequence[dict[str, Any]],
    vix_rows: Sequence[dict[str, Any]],
    day_type_rows: Sequence[dict[str, Any]],
    extreme_exclusion_rows: Sequence[dict[str, Any]],
) -> str:
    headline = summary_payload["headline"]
    lines = [
        "# US Open Probabilistic Pass 2",
        "",
        "## Scope",
        "",
        "- framing: same-day directional bias after `10:00 ET`",
        "- no trading rules",
        "- no threshold optimization",
        "- continuation/reversal retained as descriptive outcomes",
        "",
        "## Headline",
        "",
        f"- holdout pooled continuation 60m: `{headline['holdout_pooled_continuation_probability_60m']:.2%}`",
        f"- holdout pooled avg close return: `{headline['holdout_pooled_avg_forward_return_close']:.3f}`",
        f"- holdout trend-up probability given `UP` opening drive: `{headline['holdout_trend_up_probability_given_up_open']:.2%}`",
        f"- holdout continuation 60m excluding largest opening-drive quintile: `{headline['holdout_excluding_extremes_continuation_probability_60m']:.2%}`",
        "",
        "## Pooled Direction",
        "",
    ]
    for row in pooled_rows:
        lines.append(
            f"- `{row['cluster']}`: dev rows `{row['development_row_count']}`, holdout rows `{row['holdout_row_count']}`, "
            f"holdout cont60 `{row['holdout_continuation_probability_60m']:.2%}`, holdout avgClose `{row['holdout_avg_forward_return_close']:.3f}`"
        )
    lines.extend(["", "## Opening Drive Size", ""])
    for row in [item for item in opening_drive_size_rows if item["cluster"] == "ALL"]:
        lines.append(
            f"- `{row['feature_value']}`: holdout rows `{row['holdout_row_count']}`, holdout cont60 `{row['holdout_continuation_probability_60m']:.2%}`, "
            f"holdout avgClose `{row['holdout_avg_forward_return_close']:.3f}`, status `{row['comparison_status']}`"
        )
    lines.extend(["", "## VIX Conditioning", ""])
    for row in sorted(vix_rows, key=lambda item: (item["cluster"], -item["holdout_row_count"]))[:8]:
        lines.append(
            f"- `{row['cluster']} {row['feature_value']}`: holdout rows `{row['holdout_row_count']}`, "
            f"holdout cont60 `{row['holdout_continuation_probability_60m']:.2%}`, holdout avgClose `{row['holdout_avg_forward_return_close']:.3f}`"
        )
    lines.extend(["", "## Is the Open Predictive of Day Type?", ""])
    for row in [item for item in day_type_rows if item["cluster"] == "ALL" and item["feature_name"] == "opening_drive_direction"]:
        lines.append(
            f"- `{row['feature_value']}` opening drive: holdout trend-up `{row['holdout_trend_up_probability']:.2%}`, "
            f"trend-down `{row['holdout_trend_down_probability']:.2%}`, non-trend `{row['holdout_non_trend_probability']:.2%}`, "
            f"avg close return `{row['holdout_avg_return_to_close']:.3f}`"
        )
    lines.extend(["", "## Extreme Opening Drives", ""])
    for row in [item for item in extreme_exclusion_rows if item["cluster"] == "ALL"]:
        lines.append(
            f"- `{row['feature_value']}`: holdout cont60 `{row['holdout_continuation_probability_60m']:.2%}`, "
            f"holdout avg120 `{row['holdout_avg_forward_return_120m']:.3f}`, holdout avgClose `{row['holdout_avg_forward_return_close']:.3f}`"
        )
    return "\n".join(lines) + "\n"
