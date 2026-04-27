"""Decision-focused U.S. open follow-through analysis for NDX day-bias viability."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Sequence

from ..asia_drift.probabilistic_pass1 import _mean, _write_csv
from ..asia_drift.probabilistic_pass2 import _coerce_row
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from .probabilistic_pass2 import (
    DAY_TYPE_NON_TREND,
    DAY_TYPE_TREND_DOWN,
    DAY_TYPE_TREND_UP,
    _annotate_rows,
    _merge_rows,
)


PASS_VERSION = "us_open_probabilistic_pass3_v1"
MIN_DEVELOPMENT_ROWS = 100
MIN_HOLDOUT_ROWS = 35
PROMOTE_NARROW_LANE = "PROMOTE_NARROW_LANE"
RETAIN_RESEARCH_ONLY = "RETAIN_RESEARCH_ONLY"
KILL = "KILL"


def run_probabilistic_pass3(*, pass1_root: Path, output_dir: Path) -> dict[str, Any]:
    pass1_root = pass1_root.resolve()
    output_dir = output_dir.resolve()
    layout = build_layout(output_dir)

    candidate_rows = _read_csv_rows(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv")
    outcome_rows = _read_csv_rows(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv")
    merged_rows = _annotate_rows(_merge_rows(candidate_rows, outcome_rows))

    ndx_rows = [row for row in merged_rows if str(row["cluster"]) == "NDX"]
    spx_rows = [row for row in merged_rows if str(row["cluster"]) == "SPX"]

    ndx_direction_rows = _build_surface_rows(
        ndx_rows,
        feature_name="opening_drive_direction",
        feature_getter=lambda row: str(row["direction"]),
    )
    ndx_q5_rows = _build_surface_rows(
        ndx_rows,
        feature_name="opening_drive_q5_group",
        feature_getter=lambda row: "Q5_LARGEST" if bool(row["opening_drive_extreme_flag"]) else "Q1_Q4",
    )
    ndx_vix_direction_rows = _build_surface_rows(
        ndx_rows,
        feature_name="vix_x_direction",
        feature_getter=lambda row: f"{row['direction']}|{row.get('vix_level_bucket', 'UNKNOWN')}|{row.get('vix_change_bucket', 'UNKNOWN')}",
    )
    ndx_overnight_direction_rows = _build_surface_rows(
        ndx_rows,
        feature_name="overnight_alignment_x_direction",
        feature_getter=lambda row: f"{row['direction']}|{row['overnight_alignment']}",
    )
    ndx_cross_index_direction_rows = _build_surface_rows(
        ndx_rows,
        feature_name="cross_index_confirmation_x_direction",
        feature_getter=lambda row: f"{row['direction']}|{row['cross_index_confirmation_label']}",
    )
    ndx_trend_direction_rows = _build_surface_rows(
        ndx_rows,
        feature_name="trend_60m_agreement_x_direction",
        feature_getter=lambda row: f"{row['direction']}|{row['trend_60m_agreement_label']}",
    )
    reference_rows = _build_reference_rows(spx_rows, ndx_rows)
    decision_summary_rows, decision_payload = _build_decision_summary(
        ndx_direction_rows=ndx_direction_rows,
        ndx_q5_rows=ndx_q5_rows,
        ndx_vix_direction_rows=ndx_vix_direction_rows,
        ndx_overnight_direction_rows=ndx_overnight_direction_rows,
        ndx_cross_index_direction_rows=ndx_cross_index_direction_rows,
        ndx_trend_direction_rows=ndx_trend_direction_rows,
        reference_rows=reference_rows,
    )

    ndx_direction_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_direction_surface.csv"
    ndx_q5_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_q5_surface.csv"
    ndx_vix_direction_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_vix_direction_surface.csv"
    ndx_overnight_direction_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_overnight_alignment_surface.csv"
    ndx_cross_index_direction_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_cross_index_surface.csv"
    ndx_trend_direction_csv = layout["reports"] / "us_open_probabilistic_pass3_ndx_trend_agreement_surface.csv"
    spx_vs_ndx_reference_csv = layout["reports"] / "us_open_probabilistic_pass3_spx_vs_ndx_reference_surface.csv"
    decision_summary_csv = layout["reports"] / "us_open_probabilistic_pass3_decision_summary.csv"
    summary_json = layout["reports"] / "us_open_probabilistic_pass3_summary.json"
    summary_markdown = layout["reports"] / "us_open_probabilistic_pass3_summary.md"

    _write_csv(ndx_direction_csv, ndx_direction_rows)
    _write_csv(ndx_q5_csv, ndx_q5_rows)
    _write_csv(ndx_vix_direction_csv, ndx_vix_direction_rows)
    _write_csv(ndx_overnight_direction_csv, ndx_overnight_direction_rows)
    _write_csv(ndx_cross_index_direction_csv, ndx_cross_index_direction_rows)
    _write_csv(ndx_trend_direction_csv, ndx_trend_direction_rows)
    _write_csv(spx_vs_ndx_reference_csv, reference_rows)
    _write_csv(decision_summary_csv, decision_summary_rows)

    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_direction_surface.parquet", ndx_direction_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_q5_surface.parquet", ndx_q5_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_vix_direction_surface.parquet", ndx_vix_direction_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_overnight_alignment_surface.parquet", ndx_overnight_direction_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_cross_index_surface.parquet", ndx_cross_index_direction_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_ndx_trend_agreement_surface.parquet", ndx_trend_direction_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_spx_vs_ndx_reference_surface.parquet", reference_rows)
    materialize_parquet_dataset(layout["reports"] / "us_open_probabilistic_pass3_decision_summary.parquet", decision_summary_rows)

    summary_payload = {
        "module": "us_open_probabilistic_pass3",
        "version": PASS_VERSION,
        "source_pass1_root": str(pass1_root),
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
            "merged_rows": len(merged_rows),
            "ndx_rows": len(ndx_rows),
            "spx_rows": len(spx_rows),
        },
        "sample_rules": {
            "development_min_rows": MIN_DEVELOPMENT_ROWS,
            "holdout_min_rows": MIN_HOLDOUT_ROWS,
        },
        "decision": decision_payload,
        "artifact_paths": {
            "ndx_direction_csv": str(ndx_direction_csv),
            "ndx_q5_csv": str(ndx_q5_csv),
            "ndx_vix_direction_csv": str(ndx_vix_direction_csv),
            "ndx_overnight_direction_csv": str(ndx_overnight_direction_csv),
            "ndx_cross_index_direction_csv": str(ndx_cross_index_direction_csv),
            "ndx_trend_direction_csv": str(ndx_trend_direction_csv),
            "spx_vs_ndx_reference_csv": str(spx_vs_ndx_reference_csv),
            "decision_summary_csv": str(decision_summary_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(
            decision_payload=decision_payload,
            ndx_direction_rows=ndx_direction_rows,
            ndx_q5_rows=ndx_q5_rows,
            reference_rows=reference_rows,
            ndx_vix_direction_rows=ndx_vix_direction_rows,
            ndx_overnight_direction_rows=ndx_overnight_direction_rows,
            ndx_trend_direction_rows=ndx_trend_direction_rows,
        ),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "us_open_probabilistic_pass3",
            "version": PASS_VERSION,
            "source_pass1_root": str(pass1_root),
            "artifact_paths": summary_payload["artifact_paths"],
            "ndx_row_count": len(ndx_rows),
            "spx_row_count": len(spx_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [_coerce_row(row) for row in csv.DictReader(handle)]


def _build_surface_rows(
    rows: Sequence[dict[str, Any]],
    *,
    feature_name: str,
    feature_getter,
) -> list[dict[str, Any]]:
    feature_values = sorted({str(feature_getter(row)) for row in rows})
    payload: list[dict[str, Any]] = []
    for feature_value in feature_values:
        bucket = [row for row in rows if str(feature_getter(row)) == feature_value]
        development = [row for row in bucket if str(row["sample_split"]) == "development"]
        holdout = [row for row in bucket if str(row["sample_split"]) == "holdout"]
        payload.append(
            {
                "cluster": "NDX",
                "feature_name": feature_name,
                "feature_value": feature_value,
                "feature_slice": f"{feature_name}={feature_value}",
                "development_row_count": len(development),
                "holdout_row_count": len(holdout),
                "development_sample_status": _pass3_sample_status("development", len(development)),
                "holdout_sample_status": _pass3_sample_status("holdout", len(holdout)),
                "comparison_status": _pass3_comparison_status(len(development), len(holdout)),
                "development_trend_up_probability": _day_type_probability(development, DAY_TYPE_TREND_UP),
                "holdout_trend_up_probability": _day_type_probability(holdout, DAY_TYPE_TREND_UP),
                "development_trend_down_probability": _day_type_probability(development, DAY_TYPE_TREND_DOWN),
                "holdout_trend_down_probability": _day_type_probability(holdout, DAY_TYPE_TREND_DOWN),
                "development_non_trend_probability": _day_type_probability(development, DAY_TYPE_NON_TREND),
                "holdout_non_trend_probability": _day_type_probability(holdout, DAY_TYPE_NON_TREND),
                "development_avg_return_1530": round(_mean(row.get("forward_return_1530_raw") for row in development), 6),
                "holdout_avg_return_1530": round(_mean(row.get("forward_return_1530_raw") for row in holdout), 6),
                "development_avg_return_close": round(_mean(row.get("forward_return_close_raw") for row in development), 6),
                "holdout_avg_return_close": round(_mean(row.get("forward_return_close_raw") for row in holdout), 6),
            }
        )
    return payload


def _build_reference_rows(spx_rows: Sequence[dict[str, Any]], ndx_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for cluster, bucket in (("SPX", spx_rows), ("NDX", ndx_rows)):
        development = [row for row in bucket if str(row["sample_split"]) == "development"]
        holdout = [row for row in bucket if str(row["sample_split"]) == "holdout"]
        payload.append(
            {
                "cluster": cluster,
                "development_row_count": len(development),
                "holdout_row_count": len(holdout),
                "comparison_status": _pass3_comparison_status(len(development), len(holdout)),
                "development_trend_up_probability": _day_type_probability(development, DAY_TYPE_TREND_UP),
                "holdout_trend_up_probability": _day_type_probability(holdout, DAY_TYPE_TREND_UP),
                "development_trend_down_probability": _day_type_probability(development, DAY_TYPE_TREND_DOWN),
                "holdout_trend_down_probability": _day_type_probability(holdout, DAY_TYPE_TREND_DOWN),
                "development_non_trend_probability": _day_type_probability(development, DAY_TYPE_NON_TREND),
                "holdout_non_trend_probability": _day_type_probability(holdout, DAY_TYPE_NON_TREND),
                "development_avg_return_1530": round(_mean(row.get("forward_return_1530_raw") for row in development), 6),
                "holdout_avg_return_1530": round(_mean(row.get("forward_return_1530_raw") for row in holdout), 6),
                "development_avg_return_close": round(_mean(row.get("forward_return_close_raw") for row in development), 6),
                "holdout_avg_return_close": round(_mean(row.get("forward_return_close_raw") for row in holdout), 6),
            }
        )
    return payload


def _build_decision_summary(
    *,
    ndx_direction_rows: Sequence[dict[str, Any]],
    ndx_q5_rows: Sequence[dict[str, Any]],
    ndx_vix_direction_rows: Sequence[dict[str, Any]],
    ndx_overnight_direction_rows: Sequence[dict[str, Any]],
    ndx_cross_index_direction_rows: Sequence[dict[str, Any]],
    ndx_trend_direction_rows: Sequence[dict[str, Any]],
    reference_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    spx_reference = next(row for row in reference_rows if row["cluster"] == "SPX")
    ndx_reference = next(row for row in reference_rows if row["cluster"] == "NDX")
    up_row = next(row for row in ndx_direction_rows if row["feature_value"] == "UP")
    down_row = next(row for row in ndx_direction_rows if row["feature_value"] == "DOWN")
    q5_row = next(row for row in ndx_q5_rows if row["feature_value"] == "Q5_LARGEST")
    q1_q4_row = next(row for row in ndx_q5_rows if row["feature_value"] == "Q1_Q4")

    ndx_beats_spx = float(ndx_reference["holdout_avg_return_close"]) > float(spx_reference["holdout_avg_return_close"]) + 4.0
    up_drive_dominates = (
        float(up_row["holdout_trend_up_probability"]) > 0.55
        and float(up_row["holdout_avg_return_close"]) > 5.0
        and float(up_row["holdout_avg_return_close"]) > float(down_row["holdout_avg_return_close"]) + 8.0
    )
    down_is_weak = (
        float(down_row["holdout_trend_down_probability"]) < 0.50
        and float(down_row["holdout_avg_return_close"]) < 0.0
        and float(down_row["holdout_trend_up_probability"]) >= float(down_row["holdout_trend_down_probability"])
    )
    q5_helpful = (
        str(q5_row["comparison_status"]) == "TRUSTED"
        and float(q5_row["holdout_avg_return_close"]) > float(q1_q4_row["holdout_avg_return_close"]) + 6.0
        and float(q5_row["holdout_trend_up_probability"]) >= float(q1_q4_row["holdout_trend_up_probability"])
    )
    interaction_clarity = _interaction_clarity(
        ndx_vix_direction_rows=ndx_vix_direction_rows,
        ndx_overnight_direction_rows=ndx_overnight_direction_rows,
        ndx_cross_index_direction_rows=ndx_cross_index_direction_rows,
        ndx_trend_direction_rows=ndx_trend_direction_rows,
    )
    stable_core = (
        str(up_row["comparison_status"]) == "TRUSTED"
        and str(down_row["comparison_status"]) == "TRUSTED"
        and str(ndx_reference["comparison_status"]) == "TRUSTED"
    )

    coherent_sub_lane_exists = up_drive_dominates and down_is_weak and stable_core

    if ndx_beats_spx and up_drive_dominates and stable_core and interaction_clarity["clarifies"] and q5_helpful:
        family_decision = PROMOTE_NARROW_LANE
    elif coherent_sub_lane_exists:
        family_decision = RETAIN_RESEARCH_ONLY
    else:
        family_decision = KILL

    lane_conclusion = (
        "Broad U.S. open family is not strategy-ready, but NDX UP-drive same-day bias remains a retained research candidate"
        if coherent_sub_lane_exists
        else "No meaningful directional substructure survived holdout"
    )
    up_drive_only = up_drive_dominates and down_is_weak
    strategy_candidate = family_decision == PROMOTE_NARROW_LANE and up_drive_only and q5_helpful

    decision_payload = {
        "family_decision": family_decision,
        "ndx_beats_spx_close": ndx_beats_spx,
        "up_drive_dominates": up_drive_dominates,
        "down_is_weak": down_is_weak,
        "q5_helpful": q5_helpful,
        "coherent_sub_lane_exists": coherent_sub_lane_exists,
        "interaction_clarity": interaction_clarity,
        "lane_conclusion": lane_conclusion,
        "up_drive_only": up_drive_only,
        "strategy_candidate": strategy_candidate,
        "blunt_conclusion": {
            "is_ndx_same_day_directional_bias_lane": "YES" if coherent_sub_lane_exists else "NO",
            "is_up_drive_only": "YES" if up_drive_only else "NO",
            "is_q5_helpful_or_unstable": "HELPFUL" if q5_helpful else "UNSTABLE_OR_INCONCLUSIVE",
            "does_vix_overnight_trend_clarify_enough": "YES" if interaction_clarity["clarifies"] else "NO",
            "is_it_tight_enough_to_become_strategy_candidate": "YES" if strategy_candidate else "NO",
        },
    }
    decision_rows = [
        {
            "decision_key": "family_decision",
            "decision_value": family_decision,
            "detail": lane_conclusion,
        },
        {
            "decision_key": "ndx_beats_spx_close",
            "decision_value": str(ndx_beats_spx),
            "detail": f"NDX holdout avg close {ndx_reference['holdout_avg_return_close']} vs SPX {spx_reference['holdout_avg_return_close']}",
        },
        {
            "decision_key": "coherent_sub_lane_exists",
            "decision_value": str(coherent_sub_lane_exists),
            "detail": "NDX UP-drive holdout behavior is meaningfully stronger while DOWN drives remain weak/negative",
        },
        {
            "decision_key": "up_drive_dominates",
            "decision_value": str(up_drive_dominates),
            "detail": f"UP holdout avg close {up_row['holdout_avg_return_close']} vs DOWN {down_row['holdout_avg_return_close']}",
        },
        {
            "decision_key": "down_is_weak",
            "decision_value": str(down_is_weak),
            "detail": f"DOWN trend-up {down_row['holdout_trend_up_probability']} vs trend-down {down_row['holdout_trend_down_probability']}",
        },
        {
            "decision_key": "q5_helpful",
            "decision_value": str(q5_helpful),
            "detail": f"Q5 holdout avg close {q5_row['holdout_avg_return_close']} vs Q1_Q4 {q1_q4_row['holdout_avg_return_close']}",
        },
        {
            "decision_key": "interaction_clarity",
            "decision_value": str(interaction_clarity['clarifies']),
            "detail": interaction_clarity["detail"],
        },
    ]
    return decision_rows, decision_payload


def _interaction_clarity(
    *,
    ndx_vix_direction_rows: Sequence[dict[str, Any]],
    ndx_overnight_direction_rows: Sequence[dict[str, Any]],
    ndx_cross_index_direction_rows: Sequence[dict[str, Any]],
    ndx_trend_direction_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    trusted_vix = [
        row for row in ndx_vix_direction_rows
        if row["comparison_status"] == "TRUSTED" and row["feature_value"].startswith("UP|")
    ]
    best_vix = max((float(row["holdout_avg_return_close"]) for row in trusted_vix), default=0.0)
    worst_vix = min((float(row["holdout_avg_return_close"]) for row in trusted_vix), default=0.0)

    overnight_up_disagree = _find_surface_value(ndx_overnight_direction_rows, "UP|DISAGREE")
    overnight_up_aligned = _find_surface_value(ndx_overnight_direction_rows, "UP|ALIGNED")
    trend_up_agree = _find_surface_value(ndx_trend_direction_rows, "UP|AGREE")
    trend_up_disagree = _find_surface_value(ndx_trend_direction_rows, "UP|DISAGREE")
    cross_up_confirmed = _find_surface_value(ndx_cross_index_direction_rows, "UP|CONFIRMED")
    cross_up_unconfirmed = _find_surface_value(ndx_cross_index_direction_rows, "UP|UNCONFIRMED")

    clarifies = (
        abs(best_vix - worst_vix) >= 8.0
        or abs(overnight_up_disagree - overnight_up_aligned) >= 8.0
        or abs(trend_up_agree - trend_up_disagree) >= 8.0
        or abs(cross_up_confirmed - cross_up_unconfirmed) >= 8.0
    )
    detail = (
        f"VIX spread {best_vix - worst_vix:.3f}; "
        f"overnight UP disagree-aligned diff {overnight_up_disagree - overnight_up_aligned:.3f}; "
        f"trend UP agree-disagree diff {trend_up_agree - trend_up_disagree:.3f}; "
        f"cross-index UP confirmed-unconfirmed diff {cross_up_confirmed - cross_up_unconfirmed:.3f}"
    )
    return {"clarifies": clarifies, "detail": detail}


def _find_surface_value(rows: Sequence[dict[str, Any]], feature_value: str) -> float:
    row = next((item for item in rows if item["feature_value"] == feature_value and item["comparison_status"] == "TRUSTED"), None)
    if row is None:
        return 0.0
    return float(row["holdout_avg_return_close"])


def _day_type_probability(rows: Sequence[dict[str, Any]], day_type: str) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if str(row.get("day_type")) == day_type) / len(rows), 6)


def _pass3_sample_status(sample_split: str, row_count: int) -> str:
    threshold = MIN_DEVELOPMENT_ROWS if sample_split == "development" else MIN_HOLDOUT_ROWS
    return "TRUSTED" if row_count >= threshold else "DO_NOT_TRUST"


def _pass3_comparison_status(development_count: int, holdout_count: int) -> str:
    if development_count >= MIN_DEVELOPMENT_ROWS and holdout_count >= MIN_HOLDOUT_ROWS:
        return "TRUSTED"
    return "DO_NOT_TRUST"


def _render_markdown(
    *,
    decision_payload: dict[str, Any],
    ndx_direction_rows: Sequence[dict[str, Any]],
    ndx_q5_rows: Sequence[dict[str, Any]],
    reference_rows: Sequence[dict[str, Any]],
    ndx_vix_direction_rows: Sequence[dict[str, Any]],
    ndx_overnight_direction_rows: Sequence[dict[str, Any]],
    ndx_trend_direction_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# US Open Probabilistic Pass 3",
        "",
        "## Family Decision",
        "",
        f"- classification: `{decision_payload['family_decision']}`",
        f"- lane conclusion: `{decision_payload['lane_conclusion']}`",
        "- Do not promote this as a main strategy lane now.",
        "- Do not kill the finding entirely.",
        "- Preserve `NDX UP-drive same-day bias after 10:00` as a retained research candidate.",
        "- It may later be tested as an overlay/context filter, not a standalone strategy yet.",
        "",
        "## Blunt Conclusion",
        "",
        f"- Is this an NDX same-day directional-bias lane? `{decision_payload['blunt_conclusion']['is_ndx_same_day_directional_bias_lane']}`",
        f"- Is it UP-drive-only? `{decision_payload['blunt_conclusion']['is_up_drive_only']}`",
        f"- Is Q5 helpful or unstable? `{decision_payload['blunt_conclusion']['is_q5_helpful_or_unstable']}`",
        f"- Does VIX/overnight/trend alignment clarify enough? `{decision_payload['blunt_conclusion']['does_vix_overnight_trend_clarify_enough']}`",
        f"- Is it tight enough to become a strategy candidate? `{decision_payload['blunt_conclusion']['is_it_tight_enough_to_become_strategy_candidate']}`",
        "",
        "## NDX UP vs DOWN",
        "",
    ]
    for row in ndx_direction_rows:
        lines.append(
            f"- `{row['feature_value']}`: holdout trend-up `{row['holdout_trend_up_probability']:.2%}`, "
            f"trend-down `{row['holdout_trend_down_probability']:.2%}`, avg1530 `{row['holdout_avg_return_1530']:.3f}`, "
            f"avgClose `{row['holdout_avg_return_close']:.3f}`, status `{row['comparison_status']}`"
        )
    lines.extend(["", "## NDX Q5", ""])
    for row in ndx_q5_rows:
        lines.append(
            f"- `{row['feature_value']}`: holdout trend-up `{row['holdout_trend_up_probability']:.2%}`, "
            f"avg1530 `{row['holdout_avg_return_1530']:.3f}`, avgClose `{row['holdout_avg_return_close']:.3f}`, status `{row['comparison_status']}`"
        )
    lines.extend(["", "## SPX vs NDX Reference", ""])
    for row in reference_rows:
        lines.append(
            f"- `{row['cluster']}`: holdout trend-up `{row['holdout_trend_up_probability']:.2%}`, "
            f"trend-down `{row['holdout_trend_down_probability']:.2%}`, avg1530 `{row['holdout_avg_return_1530']:.3f}`, "
            f"avgClose `{row['holdout_avg_return_close']:.3f}`"
        )
    lines.extend(["", "## Interaction Notes", ""])
    for label, rows in (
        ("VIX", ndx_vix_direction_rows),
        ("Overnight", ndx_overnight_direction_rows),
        ("Trend 60m", ndx_trend_direction_rows),
    ):
        lines.append(f"- `{label}`:")
        for row in rows[:6]:
            lines.append(
                f"  - `{row['feature_value']}` holdout avgClose `{row['holdout_avg_return_close']:.3f}`, "
                f"trend-up `{row['holdout_trend_up_probability']:.2%}`, status `{row['comparison_status']}`"
            )
    return "\n".join(lines) + "\n"
