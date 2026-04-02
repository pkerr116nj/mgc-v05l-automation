"""Separator analysis for the ASIA_LATE pause_pullback_resume_long lane."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

TARGET_FAMILY = "pause_pullback_resume_long"
TARGET_DIRECTION = "LONG"
TARGET_SESSION = "ASIA_LATE"
PHASE_NAMES = ("setup", "pullback", "resumption", "confirmation")
PRIMITIVE_FIELDS = (
    "slope_state",
    "curvature_state",
    "expansion_state",
    "ema_location_state",
    "breakout_state",
)
MIN_REPEATABLE_MATCH_COUNT = 6


@dataclass(frozen=True)
class SeparatorLaneRow:
    anchor_timestamp: str
    phase_sequence: tuple[str, ...]
    primitive_steps: tuple[dict[str, str], ...]
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal


def build_and_write_asia_late_pause_long_separator(*, detail_csv_path: Path) -> dict[str, str]:
    rows = _load_lane_rows(detail_csv_path)
    lane_summary = _build_lane_summary(rows)
    candidate_rows = _build_candidate_rows(rows)
    summary_payload = _build_summary_payload(lane_summary=lane_summary, candidate_rows=candidate_rows)

    prefix = Path(str(detail_csv_path).removesuffix(".pattern_engine_v1_detail.csv"))
    lane_summary_path = prefix.with_suffix(".pattern_engine_v1_asia_late_pause_long_lane_summary.json")
    candidate_csv_path = prefix.with_suffix(".pattern_engine_v1_asia_late_pause_long_separator_candidates.csv")
    summary_json_path = prefix.with_suffix(".pattern_engine_v1_asia_late_pause_long_separator_summary.json")

    lane_summary_path.write_text(json.dumps(lane_summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(candidate_csv_path, candidate_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "lane_summary_json_path": str(lane_summary_path),
        "separator_candidates_csv_path": str(candidate_csv_path),
        "separator_summary_json_path": str(summary_json_path),
    }


def _load_lane_rows(detail_csv_path: Path) -> list[SeparatorLaneRow]:
    rows: list[SeparatorLaneRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["family_name"] != TARGET_FAMILY:
                continue
            if row["direction"] != TARGET_DIRECTION:
                continue
            if row["session_phase"] != TARGET_SESSION:
                continue
            phase_sequence = tuple(part.strip() for part in row["phase_sequence"].split(">"))
            primitive_steps = tuple(_parse_step_signature(step) for step in row["primitive_signature"].split(" > "))
            rows.append(
                SeparatorLaneRow(
                    anchor_timestamp=row["anchor_timestamp"],
                    phase_sequence=phase_sequence,
                    primitive_steps=primitive_steps,
                    move_10bar=Decimal(row["move_10bar"]),
                    mfe_20bar=Decimal(row["mfe_20bar"]),
                    mae_20bar=Decimal(row["mae_20bar"]),
                )
            )
    return rows


def _parse_step_signature(step_signature: str) -> dict[str, str]:
    values = step_signature.split("|")
    return dict(zip(PRIMITIVE_FIELDS, values, strict=True))


def _build_lane_summary(rows: list[SeparatorLaneRow]) -> dict[str, Any]:
    avg_move_10bar = _avg(row.move_10bar for row in rows)
    avg_mfe_20bar = _avg(row.mfe_20bar for row in rows)
    avg_mae_20bar = _avg(row.mae_20bar for row in rows)
    return {
        "family_name": TARGET_FAMILY,
        "direction": TARGET_DIRECTION,
        "session_phase": TARGET_SESSION,
        "match_count": len(rows),
        "estimated_value_mfe20": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
        "avg_move_10bar": str(avg_move_10bar),
        "avg_mfe_20bar": str(avg_mfe_20bar),
        "avg_mae_20bar": str(avg_mae_20bar),
        "favorable_move10_rate": str(_ratio(sum(1 for row in rows if row.move_10bar > 0), len(rows))),
        "mfe_mae_ratio": str(_safe_ratio(avg_mfe_20bar, avg_mae_20bar)),
    }


def _build_candidate_rows(rows: list[SeparatorLaneRow]) -> list[dict[str, Any]]:
    baseline = _baseline_metrics(rows)
    candidates: list[dict[str, Any]] = []

    for phase_index, phase_name in enumerate(PHASE_NAMES):
        for primitive_field in PRIMITIVE_FIELDS:
            values = sorted({row.primitive_steps[phase_index][primitive_field] for row in rows})
            for value in values:
                bucket = [row for row in rows if row.primitive_steps[phase_index][primitive_field] == value]
                candidates.append(
                    _candidate_row(
                        rows=bucket,
                        baseline=baseline,
                        candidate_type="single_phase",
                        phase_path=phase_name,
                        primitive_field=primitive_field,
                        primitive_value=value,
                    )
                )

    phase_pairs = ((0, 1), (1, 2), (0, 2))
    for left_index, right_index in phase_pairs:
        left_name = PHASE_NAMES[left_index]
        right_name = PHASE_NAMES[right_index]
        for primitive_field in PRIMITIVE_FIELDS:
            values = sorted(
                {
                    (
                        row.primitive_steps[left_index][primitive_field],
                        row.primitive_steps[right_index][primitive_field],
                    )
                    for row in rows
                }
            )
            for left_value, right_value in values:
                bucket = [
                    row
                    for row in rows
                    if row.primitive_steps[left_index][primitive_field] == left_value
                    and row.primitive_steps[right_index][primitive_field] == right_value
                ]
                candidates.append(
                    _candidate_row(
                        rows=bucket,
                        baseline=baseline,
                        candidate_type="phase_transition",
                        phase_path=f"{left_name}->{right_name}",
                        primitive_field=primitive_field,
                        primitive_value=f"{left_value}->{right_value}",
                    )
                )

    candidates.sort(
        key=lambda row: (
            Decimal(row["usefulness_score"]),
            row["match_count"],
            Decimal(row["avg_mfe_20bar"]),
        ),
        reverse=True,
    )
    return candidates


def _baseline_metrics(rows: list[SeparatorLaneRow]) -> dict[str, Decimal]:
    avg_mfe = _avg(row.mfe_20bar for row in rows)
    avg_mae = _avg(row.mae_20bar for row in rows)
    return {
        "match_count": Decimal(len(rows)),
        "avg_move_10bar": _avg(row.move_10bar for row in rows),
        "avg_mfe_20bar": avg_mfe,
        "avg_mae_20bar": avg_mae,
        "favorable_move10_rate": _ratio(sum(1 for row in rows if row.move_10bar > 0), len(rows)),
        "mfe_mae_ratio": _safe_ratio(avg_mfe, avg_mae),
    }


def _candidate_row(
    *,
    rows: list[SeparatorLaneRow],
    baseline: dict[str, Decimal],
    candidate_type: str,
    phase_path: str,
    primitive_field: str,
    primitive_value: str,
) -> dict[str, Any]:
    match_count = len(rows)
    avg_move_10bar = _avg(row.move_10bar for row in rows)
    avg_mfe_20bar = _avg(row.mfe_20bar for row in rows)
    avg_mae_20bar = _avg(row.mae_20bar for row in rows)
    favorable_rate = _ratio(sum(1 for row in rows if row.move_10bar > 0), match_count)
    mfe_mae_ratio = _safe_ratio(avg_mfe_20bar, avg_mae_20bar)
    coverage_share = _safe_ratio(Decimal(match_count), baseline["match_count"])
    avg_mfe_lift = _safe_ratio(avg_mfe_20bar, baseline["avg_mfe_20bar"])
    favorable_rate_lift = _safe_ratio(favorable_rate, baseline["favorable_move10_rate"])
    mfe_mae_lift = _safe_ratio(mfe_mae_ratio, baseline["mfe_mae_ratio"])
    usefulness_score = coverage_share * avg_mfe_lift * favorable_rate_lift * mfe_mae_lift
    repeatability = "repeatable" if match_count >= MIN_REPEATABLE_MATCH_COUNT else "thin"
    separator_quality = _separator_quality(
        match_count=match_count,
        favorable_rate=favorable_rate,
        favorable_rate_lift=favorable_rate_lift,
        mfe_mae_lift=mfe_mae_lift,
        avg_move_10bar=avg_move_10bar,
    )

    return {
        "candidate_type": candidate_type,
        "phase_path": phase_path,
        "primitive_field": primitive_field,
        "primitive_value": primitive_value,
        "match_count": match_count,
        "coverage_share": str(coverage_share),
        "estimated_value_mfe20": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
        "avg_move_10bar": str(avg_move_10bar),
        "avg_mfe_20bar": str(avg_mfe_20bar),
        "avg_mae_20bar": str(avg_mae_20bar),
        "favorable_move10_rate": str(favorable_rate),
        "mfe_mae_ratio": str(mfe_mae_ratio),
        "avg_mfe_lift": str(avg_mfe_lift),
        "favorable_rate_lift": str(favorable_rate_lift),
        "mfe_mae_lift": str(mfe_mae_lift),
        "usefulness_score": str(usefulness_score),
        "repeatability": repeatability,
        "separator_quality": separator_quality,
    }


def _separator_quality(
    *,
    match_count: int,
    favorable_rate: Decimal,
    favorable_rate_lift: Decimal,
    mfe_mae_lift: Decimal,
    avg_move_10bar: Decimal,
) -> str:
    if (
        match_count >= MIN_REPEATABLE_MATCH_COUNT
        and favorable_rate >= Decimal("0.60")
        and favorable_rate_lift > Decimal("1.05")
        and mfe_mae_lift > Decimal("1.20")
        and avg_move_10bar > 0
    ):
        return "candidate"
    return "broad_or_noisy"


def _build_summary_payload(*, lane_summary: dict[str, Any], candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
    repeatable_candidates = [row for row in candidate_rows if row["repeatability"] == "repeatable"]
    promotable = [row for row in repeatable_candidates if row["separator_quality"] == "candidate"]
    best_repeatable = repeatable_candidates[0] if repeatable_candidates else None
    best_promotable = promotable[0] if promotable else None
    highest_lift_thin = next((row for row in candidate_rows if row["repeatability"] == "thin"), None)
    return {
        "lane_summary": lane_summary,
        "best_repeatable_discriminator": best_repeatable,
        "best_promotable_discriminator": best_promotable,
        "highest_lift_thin_discriminator": highest_lift_thin,
        "candidate_count": len(promotable),
        "top_repeatable_rankings": repeatable_candidates[:12],
        "top_promotable_rankings": promotable[:12],
    }


def _avg(values: Any) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("detail_csv", type=Path)
    args = parser.parse_args(argv)

    outputs = build_and_write_asia_late_pause_long_separator(detail_csv_path=args.detail_csv)
    print(json.dumps(outputs, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
