"""Refinement separator analysis inside the US_MIDDAY SHORT failed_move_reversal COMPRESSED reversal subset."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


TARGET_FAMILY = "failed_move_reversal"
TARGET_DIRECTION = "SHORT"
TARGET_SESSION = "US_MIDDAY"
PHASE_NAMES = ("failed_move", "reversal", "confirmation")
PRIMITIVE_FIELDS = (
    "slope_state",
    "curvature_state",
    "expansion_state",
    "ema_location_state",
    "breakout_state",
)
MIN_REPEATABLE_MATCH_COUNT = 12


@dataclass(frozen=True)
class RefinementLaneRow:
    anchor_timestamp: str
    primitive_steps: tuple[dict[str, str], ...]
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal

    @property
    def directional_move_10bar(self) -> Decimal:
        return -self.move_10bar


def build_and_write_failed_move_reversal_us_midday_short_refinement_separator(
    *, detail_csv_path: Path
) -> dict[str, str]:
    rows = _load_lane_rows(detail_csv_path)
    lane_summary = _build_lane_summary(rows)
    candidate_rows = _build_candidate_rows(rows)
    summary_payload = _build_summary_payload(lane_summary=lane_summary, candidate_rows=candidate_rows)

    prefix = Path(str(detail_csv_path).removesuffix(".pattern_engine_v1_detail.csv"))
    lane_summary_path = prefix.with_suffix(
        ".pattern_engine_v1_failed_move_reversal_us_midday_short_refinement_lane_summary.json"
    )
    candidate_csv_path = prefix.with_suffix(
        ".pattern_engine_v1_failed_move_reversal_us_midday_short_refinement_candidates.csv"
    )
    summary_json_path = prefix.with_suffix(
        ".pattern_engine_v1_failed_move_reversal_us_midday_short_refinement_summary.json"
    )

    lane_summary_path.write_text(json.dumps(lane_summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(candidate_csv_path, candidate_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "lane_summary_json_path": str(lane_summary_path),
        "separator_candidates_csv_path": str(candidate_csv_path),
        "separator_summary_json_path": str(summary_json_path),
    }


def _load_lane_rows(detail_csv_path: Path) -> list[RefinementLaneRow]:
    rows: list[RefinementLaneRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["family_name"] != TARGET_FAMILY:
                continue
            if row["direction"] != TARGET_DIRECTION:
                continue
            if row["session_phase"] != TARGET_SESSION:
                continue
            primitive_steps = tuple(_parse_step_signature(step) for step in row["primitive_signature"].split(" > "))
            if primitive_steps[1]["expansion_state"] != "COMPRESSED":
                continue
            rows.append(
                RefinementLaneRow(
                    anchor_timestamp=row["anchor_timestamp"],
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


def _build_lane_summary(rows: list[RefinementLaneRow]) -> dict[str, Any]:
    avg_move_10bar = _avg(row.move_10bar for row in rows)
    avg_directional_move_10bar = _avg(row.directional_move_10bar for row in rows)
    avg_mfe_20bar = _avg(row.mfe_20bar for row in rows)
    avg_mae_20bar = _avg(row.mae_20bar for row in rows)
    return {
        "family_name": TARGET_FAMILY,
        "direction": TARGET_DIRECTION,
        "session_phase": TARGET_SESSION,
        "base_separator": {
            "phase_path": "reversal",
            "primitive_field": "expansion_state",
            "primitive_value": "COMPRESSED",
        },
        "match_count": len(rows),
        "estimated_value_mfe20": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
        "avg_move_10bar": str(avg_move_10bar),
        "directional_avg_move_10bar": str(avg_directional_move_10bar),
        "avg_mfe_20bar": str(avg_mfe_20bar),
        "avg_mae_20bar": str(avg_mae_20bar),
        "aligned_move10_rate": str(_ratio(sum(1 for row in rows if row.directional_move_10bar > 0), len(rows))),
        "mfe_mae_ratio": str(_safe_ratio(avg_mfe_20bar, avg_mae_20bar)),
    }


def _build_candidate_rows(rows: list[RefinementLaneRow]) -> list[dict[str, Any]]:
    baseline = _baseline_metrics(rows)
    candidates: list[dict[str, Any]] = []

    for phase_index, phase_name in enumerate(PHASE_NAMES):
        for primitive_field in PRIMITIVE_FIELDS:
            values = sorted({row.primitive_steps[phase_index][primitive_field] for row in rows})
            for value in values:
                bucket = [row for row in rows if row.primitive_steps[phase_index][primitive_field] == value]
                if len(bucket) == len(rows):
                    continue
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

    for left_index, right_index in ((0, 1), (1, 2), (0, 2)):
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
                if len(bucket) == len(rows):
                    continue
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
            Decimal(row["mfe_mae_ratio"]),
            row["match_count"],
        ),
        reverse=True,
    )
    return candidates


def _baseline_metrics(rows: list[RefinementLaneRow]) -> dict[str, Decimal]:
    avg_mfe = _avg(row.mfe_20bar for row in rows)
    avg_mae = _avg(row.mae_20bar for row in rows)
    return {
        "match_count": Decimal(len(rows)),
        "directional_avg_move_10bar": _avg(row.directional_move_10bar for row in rows),
        "avg_mfe_20bar": avg_mfe,
        "avg_mae_20bar": avg_mae,
        "aligned_move10_rate": _ratio(sum(1 for row in rows if row.directional_move_10bar > 0), len(rows)),
        "mfe_mae_ratio": _safe_ratio(avg_mfe, avg_mae),
    }


def _candidate_row(
    *,
    rows: list[RefinementLaneRow],
    baseline: dict[str, Decimal],
    candidate_type: str,
    phase_path: str,
    primitive_field: str,
    primitive_value: str,
) -> dict[str, Any]:
    match_count = len(rows)
    avg_move_10bar = _avg(row.move_10bar for row in rows)
    directional_avg_move_10bar = _avg(row.directional_move_10bar for row in rows)
    avg_mfe_20bar = _avg(row.mfe_20bar for row in rows)
    avg_mae_20bar = _avg(row.mae_20bar for row in rows)
    aligned_rate = _ratio(sum(1 for row in rows if row.directional_move_10bar > 0), match_count)
    mfe_mae_ratio = _safe_ratio(avg_mfe_20bar, avg_mae_20bar)
    coverage_share = _safe_ratio(Decimal(match_count), baseline["match_count"])
    directional_move_lift = _safe_ratio(directional_avg_move_10bar, baseline["directional_avg_move_10bar"])
    aligned_rate_lift = _safe_ratio(aligned_rate, baseline["aligned_move10_rate"])
    mfe_mae_lift = _safe_ratio(mfe_mae_ratio, baseline["mfe_mae_ratio"])
    usefulness_score = coverage_share * directional_move_lift * aligned_rate_lift * mfe_mae_lift
    repeatability = "repeatable" if match_count >= MIN_REPEATABLE_MATCH_COUNT else "thin"
    separator_quality = _separator_quality(
        match_count=match_count,
        aligned_rate=aligned_rate,
        aligned_rate_lift=aligned_rate_lift,
        mfe_mae_lift=mfe_mae_lift,
        directional_avg_move_10bar=directional_avg_move_10bar,
        mfe_mae_ratio=mfe_mae_ratio,
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
        "directional_avg_move_10bar": str(directional_avg_move_10bar),
        "avg_mfe_20bar": str(avg_mfe_20bar),
        "avg_mae_20bar": str(avg_mae_20bar),
        "aligned_move10_rate": str(aligned_rate),
        "mfe_mae_ratio": str(mfe_mae_ratio),
        "directional_move_lift": str(directional_move_lift),
        "aligned_rate_lift": str(aligned_rate_lift),
        "mfe_mae_lift": str(mfe_mae_lift),
        "usefulness_score": str(usefulness_score),
        "repeatability": repeatability,
        "separator_quality": separator_quality,
    }


def _separator_quality(
    *,
    match_count: int,
    aligned_rate: Decimal,
    aligned_rate_lift: Decimal,
    mfe_mae_lift: Decimal,
    directional_avg_move_10bar: Decimal,
    mfe_mae_ratio: Decimal,
) -> str:
    if (
        match_count >= MIN_REPEATABLE_MATCH_COUNT
        and aligned_rate >= Decimal("0.58")
        and aligned_rate_lift >= Decimal("1.02")
        and mfe_mae_lift >= Decimal("1.03")
        and mfe_mae_ratio >= Decimal("1.00")
        and directional_avg_move_10bar > 0
    ):
        return "candidate"
    return "broad_or_noisy"


def _build_summary_payload(*, lane_summary: dict[str, Any], candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
    repeatable = [row for row in candidate_rows if row["repeatability"] == "repeatable"]
    candidates = [row for row in repeatable if row["separator_quality"] == "candidate"]
    return {
        "lane_summary": lane_summary,
        "best_repeatable_discriminator": repeatable[0] if repeatable else None,
        "best_promotable_discriminator": candidates[0] if candidates else None,
        "useful_secondary_discriminators": candidates[1:6],
        "broad_or_noisy_repeatables": repeatable[:10],
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
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("detail_csv_path", type=Path)
    args = parser.parse_args(argv)

    result = build_and_write_failed_move_reversal_us_midday_short_refinement_separator(
        detail_csv_path=args.detail_csv_path
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
