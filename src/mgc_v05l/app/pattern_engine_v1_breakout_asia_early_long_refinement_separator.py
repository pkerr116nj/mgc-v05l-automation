"""Separator analysis inside the ASIA_EARLY LONG breakout_retest_hold SLOPE_FLAT subset."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any


TARGET_FAMILY = "breakout_retest_hold"
TARGET_DIRECTION = "LONG"
TARGET_SESSION = "ASIA_EARLY"
PHASE_NAMES = ("breakout", "retest", "hold")
PRIMITIVE_FIELDS = (
    "slope_state",
    "curvature_state",
    "expansion_state",
    "ema_location_state",
    "breakout_state",
)
MIN_REPEATABLE_MATCH_COUNT = 12
MIDDLE_SLICE_START = datetime.fromisoformat("2025-10-27T00:00:00-04:00")
RECENT_SLICE_START = datetime.fromisoformat("2026-01-06T03:10:00-05:00")


@dataclass(frozen=True)
class BreakoutLaneRow:
    anchor_timestamp: str
    anchor_dt: datetime
    primitive_steps: tuple[dict[str, str], ...]
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal


def build_and_write_breakout_asia_early_long_refinement_separator(*, detail_csv_path: Path) -> dict[str, str]:
    rows = _load_lane_rows(detail_csv_path)
    lane_summary = _build_lane_summary(rows)
    candidate_rows = _build_candidate_rows(rows)
    summary_payload = _build_summary_payload(lane_summary=lane_summary, candidate_rows=candidate_rows)

    prefix = Path(str(detail_csv_path).removesuffix(".pattern_engine_v1_detail.csv"))
    lane_summary_path = prefix.with_suffix(".pattern_engine_v1_breakout_asia_early_long_refinement_lane_summary.json")
    candidate_csv_path = prefix.with_suffix(".pattern_engine_v1_breakout_asia_early_long_refinement_candidates.csv")
    summary_json_path = prefix.with_suffix(".pattern_engine_v1_breakout_asia_early_long_refinement_summary.json")

    lane_summary_path.write_text(json.dumps(lane_summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(candidate_csv_path, candidate_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "lane_summary_json_path": str(lane_summary_path),
        "separator_candidates_csv_path": str(candidate_csv_path),
        "separator_summary_json_path": str(summary_json_path),
    }


def _load_lane_rows(detail_csv_path: Path) -> list[BreakoutLaneRow]:
    rows: list[BreakoutLaneRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["family_name"] != TARGET_FAMILY:
                continue
            if row["direction"] != TARGET_DIRECTION:
                continue
            if row["session_phase"] != TARGET_SESSION:
                continue
            primitive_steps = tuple(_parse_step_signature(step) for step in row["primitive_signature"].split(" > "))
            if primitive_steps[0]["slope_state"] != "SLOPE_FLAT":
                continue
            rows.append(
                BreakoutLaneRow(
                    anchor_timestamp=row["anchor_timestamp"],
                    anchor_dt=datetime.fromisoformat(row["anchor_timestamp"]),
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


def _build_lane_summary(rows: list[BreakoutLaneRow]) -> dict[str, Any]:
    avg_move_10bar = _avg(row.move_10bar for row in rows)
    avg_mfe_20bar = _avg(row.mfe_20bar for row in rows)
    avg_mae_20bar = _avg(row.mae_20bar for row in rows)
    return {
        "family_name": TARGET_FAMILY,
        "direction": TARGET_DIRECTION,
        "session_phase": TARGET_SESSION,
        "base_separator": {
            "phase_path": "breakout",
            "primitive_field": "slope_state",
            "primitive_value": "SLOPE_FLAT",
        },
        "match_count": len(rows),
        "estimated_value_mfe20": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
        "avg_move_10bar": str(avg_move_10bar),
        "avg_mfe_20bar": str(avg_mfe_20bar),
        "avg_mae_20bar": str(avg_mae_20bar),
        "favorable_move10_rate": str(_ratio(sum(1 for row in rows if row.move_10bar > 0), len(rows))),
        "mfe_mae_ratio": str(_safe_ratio(avg_mfe_20bar, avg_mae_20bar)),
        "slice_breakdown": _slice_metrics(rows),
    }


def _build_candidate_rows(rows: list[BreakoutLaneRow]) -> list[dict[str, Any]]:
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

    for bucket_name in ("ASIA_EARLY_OPEN", "ASIA_EARLY_MID", "ASIA_EARLY_LATE"):
        bucket = [row for row in rows if _asia_early_time_bucket(row.anchor_dt) == bucket_name]
        if bucket and len(bucket) != len(rows):
            candidates.append(
                _candidate_row(
                    rows=bucket,
                    baseline=baseline,
                    candidate_type="time_bucket",
                    phase_path="anchor_time",
                    primitive_field="time_bucket",
                    primitive_value=bucket_name,
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


def _baseline_metrics(rows: list[BreakoutLaneRow]) -> dict[str, Decimal]:
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
    rows: list[BreakoutLaneRow],
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
    avg_move_lift = _safe_ratio(avg_move_10bar, baseline["avg_move_10bar"])
    favorable_rate_lift = _safe_ratio(favorable_rate, baseline["favorable_move10_rate"])
    mfe_mae_lift = _safe_ratio(mfe_mae_ratio, baseline["mfe_mae_ratio"])
    usefulness_score = coverage_share * avg_move_lift * favorable_rate_lift * mfe_mae_lift
    repeatability = "repeatable" if match_count >= MIN_REPEATABLE_MATCH_COUNT else "thin"
    recent_rows = [row for row in rows if _slice_name(row.anchor_dt) == "recent"]
    recent_avg_move_10bar = _avg(row.move_10bar for row in recent_rows)
    recent_favorable_rate = _ratio(sum(1 for row in recent_rows if row.move_10bar > 0), len(recent_rows))
    separator_quality = _separator_quality(
        match_count=match_count,
        favorable_rate=favorable_rate,
        favorable_rate_lift=favorable_rate_lift,
        mfe_mae_lift=mfe_mae_lift,
        avg_move_10bar=avg_move_10bar,
        recent_avg_move_10bar=recent_avg_move_10bar,
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
        "avg_move_lift": str(avg_move_lift),
        "favorable_rate_lift": str(favorable_rate_lift),
        "mfe_mae_lift": str(mfe_mae_lift),
        "usefulness_score": str(usefulness_score),
        "repeatability": repeatability,
        "separator_quality": separator_quality,
        "recent_match_count": len(recent_rows),
        "recent_avg_move_10bar": str(recent_avg_move_10bar),
        "recent_favorable_move10_rate": str(recent_favorable_rate),
    }


def _separator_quality(
    *,
    match_count: int,
    favorable_rate: Decimal,
    favorable_rate_lift: Decimal,
    mfe_mae_lift: Decimal,
    avg_move_10bar: Decimal,
    recent_avg_move_10bar: Decimal,
) -> str:
    if (
        match_count >= MIN_REPEATABLE_MATCH_COUNT
        and favorable_rate >= Decimal("0.58")
        and favorable_rate_lift > Decimal("1.02")
        and mfe_mae_lift > Decimal("1.08")
        and avg_move_10bar > 0
        and recent_avg_move_10bar >= 0
    ):
        return "candidate"
    return "broad_or_noisy"


def _build_summary_payload(*, lane_summary: dict[str, Any], candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
    repeatable_candidates = [row for row in candidate_rows if row["repeatability"] == "repeatable"]
    promotable = [row for row in repeatable_candidates if row["separator_quality"] == "candidate"]
    best_repeatable = repeatable_candidates[0] if repeatable_candidates else None
    best_promotable = promotable[0] if promotable else None
    useful_secondaries = promotable[1:6]
    recent_drag = sorted(repeatable_candidates, key=lambda row: (Decimal(row["recent_avg_move_10bar"]), -row["match_count"]))[:8]
    return {
        "lane_summary": lane_summary,
        "repeatable_candidate_count": len(repeatable_candidates),
        "promotable_candidate_count": len(promotable),
        "best_repeatable_separator": best_repeatable,
        "best_promotable_separator": best_promotable,
        "useful_secondary_separators": useful_secondaries,
        "recent_drag_candidates": recent_drag,
    }


def _slice_metrics(rows: list[BreakoutLaneRow]) -> dict[str, dict[str, str | int]]:
    payload: dict[str, dict[str, str | int]] = {}
    for slice_name in ("early", "middle", "recent"):
        bucket = [row for row in rows if _slice_name(row.anchor_dt) == slice_name]
        payload[slice_name] = {
            "match_count": len(bucket),
            "avg_move_10bar": str(_avg(row.move_10bar for row in bucket)),
            "favorable_move10_rate": str(_ratio(sum(1 for row in bucket if row.move_10bar > 0), len(bucket))),
        }
    return payload


def _slice_name(anchor_dt: datetime) -> str:
    if anchor_dt >= RECENT_SLICE_START:
        return "recent"
    if anchor_dt >= MIDDLE_SLICE_START:
        return "middle"
    return "early"


def _asia_early_time_bucket(anchor_dt: datetime) -> str:
    anchor_time = anchor_dt.timetz().replace(tzinfo=None)
    if anchor_time < time(19, 0):
        return "ASIA_EARLY_OPEN"
    if anchor_time < time(19, 45):
        return "ASIA_EARLY_MID"
    return "ASIA_EARLY_LATE"


def _avg(values: Any) -> Decimal:
    collected = list(values)
    if not collected:
        return Decimal("0")
    return sum(collected, Decimal("0")) / Decimal(len(collected))


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


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("detail_csv_path", type=Path)
    args = parser.parse_args()
    result = build_and_write_breakout_asia_early_long_refinement_separator(detail_csv_path=args.detail_csv_path)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
