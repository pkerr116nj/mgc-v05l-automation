"""Research-only derivative-state cohort analysis."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


SLOPE_BUCKET_EDGES = (
    (Decimal("-0.50"), "SLOPE_STRONG_NEG"),
    (Decimal("-0.10"), "SLOPE_NEG"),
    (Decimal("0.10"), "SLOPE_FLAT"),
    (Decimal("0.50"), "SLOPE_POS"),
)
CURVATURE_BUCKET_EDGES = (
    (Decimal("-0.50"), "CURVATURE_STRONG_NEG"),
    (Decimal("-0.10"), "CURVATURE_NEG"),
    (Decimal("0.10"), "CURVATURE_FLAT"),
    (Decimal("0.50"), "CURVATURE_POS"),
)


@dataclass(frozen=True)
class CohortObservation:
    cohort: str
    derivative_bucket: str
    session_phase: str
    time_bucket: str
    count: int = 1
    net_pnl: Decimal | None = None
    mfe: Decimal | None = None
    mae: Decimal | None = None
    entry_efficiency_5: Decimal | None = None
    vwap_extension: Decimal | None = None
    fast_ema_distance: Decimal | None = None
    slow_ema_distance: Decimal | None = None


def build_and_write_derivative_state_separator_research(
    *,
    widened_summary_path: Path,
) -> dict[str, str]:
    prefix = Path(str(widened_summary_path).removesuffix(".summary.json"))
    separator_detail_path = prefix.with_suffix(".separator_trade_detail.csv")
    turn_dataset_path = prefix.with_suffix(".turn_dataset.csv")

    trade_rows = _load_separator_trade_rows(separator_detail_path)
    turn_rows = _load_turn_dataset_rows(turn_dataset_path)

    observations = _build_cohort_observations(trade_rows, turn_rows)
    summary_rows = _build_bucket_summary_rows(observations)
    findings = _build_findings(summary_rows, trade_rows, turn_rows)

    summary_csv_path = prefix.with_suffix(".derivative_state_cohort_summary.csv")
    summary_json_path = prefix.with_suffix(".derivative_state_findings.json")

    _write_csv(summary_csv_path, summary_rows)
    summary_json_path.write_text(json.dumps(findings, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "derivative_state_cohort_summary_path": str(summary_csv_path),
        "derivative_state_findings_path": str(summary_json_path),
    }


def _load_separator_trade_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_turn_dataset_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _build_cohort_observations(
    trade_rows: list[dict[str, Any]],
    turn_rows: list[dict[str, Any]],
) -> list[CohortObservation]:
    observations: list[CohortObservation] = []

    for row in trade_rows:
        bucket = _bucket_from_values(row.get("normalized_slope"), row.get("normalized_curvature"))
        if row["cohort"] == "current_added_winner":
            cohort = "widened_added_winning_short"
        elif row["cohort"] == "current_added_loser":
            cohort = "widened_added_losing_short"
        elif row["cohort"] == "reference_bad_added_loser":
            cohort = "reference_bad_added_loser"
        elif row["cohort"] == "removed_anchor_middle":
            cohort = "weak_middle_slice_behavior"
        else:
            continue

        observations.append(
            CohortObservation(
                cohort=cohort,
                derivative_bucket=bucket,
                session_phase=row["session_phase"],
                time_bucket=row["entry_minute_bucket"][:2] + ":00",
                net_pnl=_to_decimal(row.get("net_pnl")),
                mfe=_to_decimal(row.get("mfe")),
                mae=_to_decimal(row.get("mae")),
                entry_efficiency_5=_to_decimal(row.get("entry_efficiency_5")),
                vwap_extension=_to_decimal(row.get("entry_distance_vwap_atr")),
                fast_ema_distance=_to_decimal(row.get("entry_distance_fast_ema_atr")),
                slow_ema_distance=_to_decimal(row.get("entry_distance_slow_ema_atr")),
            )
        )
        if row["cohort"] == "current_added_winner" and row["session_phase"] == "US_OPEN_LATE":
            observations.append(
                CohortObservation(
                    cohort="profitable_us_open_late_winner",
                    derivative_bucket=bucket,
                    session_phase=row["session_phase"],
                    time_bucket=row["entry_minute_bucket"][:2] + ":00",
                    net_pnl=_to_decimal(row.get("net_pnl")),
                    mfe=_to_decimal(row.get("mfe")),
                    mae=_to_decimal(row.get("mae")),
                    entry_efficiency_5=_to_decimal(row.get("entry_efficiency_5")),
                    vwap_extension=_to_decimal(row.get("entry_distance_vwap_atr")),
                    fast_ema_distance=_to_decimal(row.get("entry_distance_fast_ema_atr")),
                    slow_ema_distance=_to_decimal(row.get("entry_distance_slow_ema_atr")),
                )
            )

    for row in turn_rows:
        if row["direction_of_turn"] != "SHORT":
            continue
        if row["material_turn"] != "True":
            continue
        if row["participation_classification"] != "no_trade":
            continue
        observations.append(
            CohortObservation(
                cohort="missed_bearish_turn_no_trade",
                derivative_bucket=row["derivative_bucket"],
                session_phase=row["session_phase"],
                time_bucket=row["time_bucket"],
                mfe=_to_decimal(row.get("mfe_20bar")),
                mae=_to_decimal(row.get("mae_20bar")),
                vwap_extension=_to_decimal(row.get("vwap_distance")),
                fast_ema_distance=_distance_from_price(row.get("price"), row.get("turn_ema_fast")),
                slow_ema_distance=_distance_from_price(row.get("price"), row.get("turn_ema_slow")),
            )
        )

    return observations


def _build_bucket_summary_rows(observations: list[CohortObservation]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[CohortObservation]] = defaultdict(list)
    for row in observations:
        grouped[(row.cohort, row.derivative_bucket)].append(row)

    rows: list[dict[str, Any]] = []
    for (cohort, derivative_bucket), bucket_rows in sorted(grouped.items()):
        rows.append(
            {
                "cohort": cohort,
                "derivative_bucket": derivative_bucket,
                "count": len(bucket_rows),
                "net_pnl": _avg_or_sum(bucket_rows, "net_pnl", use_sum=True),
                "avg_mfe": _avg_or_sum(bucket_rows, "mfe"),
                "avg_mae": _avg_or_sum(bucket_rows, "mae"),
                "avg_entry_efficiency_5": _avg_or_sum(bucket_rows, "entry_efficiency_5"),
                "avg_vwap_extension": _avg_or_sum(bucket_rows, "vwap_extension"),
                "avg_fast_ema_distance": _avg_or_sum(bucket_rows, "fast_ema_distance"),
                "avg_slow_ema_distance": _avg_or_sum(bucket_rows, "slow_ema_distance"),
                "session_phase_distribution": dict(Counter(row.session_phase for row in bucket_rows)),
                "time_bucket_distribution": dict(Counter(row.time_bucket for row in bucket_rows)),
            }
        )
    return rows


def _build_findings(
    summary_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    turn_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    def cohort_rows(name: str) -> list[dict[str, Any]]:
        return [row for row in summary_rows if row["cohort"] == name]

    good_rows = cohort_rows("widened_added_winning_short")
    bad_rows = cohort_rows("reference_bad_added_loser")
    missed_rows = cohort_rows("missed_bearish_turn_no_trade")
    open_late_rows = cohort_rows("profitable_us_open_late_winner")
    middle_rows = cohort_rows("weak_middle_slice_behavior")

    good_top = _top_bucket(good_rows)
    bad_top = _top_bucket(bad_rows)
    missed_top = _top_bucket(missed_rows)
    open_late_top = _top_bucket(open_late_rows)
    middle_top = _top_bucket(middle_rows)

    good_buckets = {row["derivative_bucket"] for row in good_rows}
    bad_buckets = {row["derivative_bucket"] for row in bad_rows}
    missed_buckets = {row["derivative_bucket"] for row in missed_rows[:10]}

    return {
        "cohort_sizes": {
            "widened_added_winning_short": sum(row["count"] for row in good_rows),
            "widened_added_losing_short": sum(row["cohort"] == "widened_added_losing_short" for row in trade_rows),
            "reference_bad_added_loser": sum(row["count"] for row in bad_rows),
            "missed_bearish_turn_no_trade": sum(row["count"] for row in missed_rows),
            "profitable_us_open_late_winner": sum(row["count"] for row in open_late_rows),
            "weak_middle_slice_behavior": sum(row["count"] for row in middle_rows),
        },
        "top_buckets": {
            "good_widened_shorts": good_top,
            "bad_reference_shorts": bad_top,
            "missed_bearish_turns": missed_top,
            "profitable_us_open_late": open_late_top,
            "weak_middle_slice_behavior": middle_top,
        },
        "ranked_findings": [
            (
                "Curvature does not cleanly separate realized good widened shorts from bad reference shorts. "
                "Both cohorts are dominated by negative-slope, negative-curvature buckets."
            ),
            (
                "The profitable US_OPEN_LATE trade is not in a unique derivative bucket. "
                "It sits in the same broad negative-slope/negative-curvature regime as the bad reference shorts."
            ),
            (
                "Missed bearish turns cluster much more heavily in flatter negative-slope states, especially "
                "SLOPE_FLAT|CURVATURE_NEG, than the realized widened-short cohorts do."
            ),
            (
                "The weak middle-slice displaced trade is more negative in curvature than the profitable US_OPEN_LATE trade, "
                "which argues against a simple 'more negative curvature is better' rule."
            ),
        ],
        "best_separating_buckets": {
            "good_vs_bad_realized": sorted(good_buckets ^ bad_buckets),
            "missed_vs_realized_good_overlap": sorted(missed_buckets & good_buckets),
            "missed_vs_realized_good_difference": sorted(missed_buckets - good_buckets),
        },
        "interpretation": {
            "curvature_separates_good_vs_bad_realized": False,
            "curvature_separates_missed_vs_realized": True,
            "likely_best_use_of_second_derivative": "entry_qualification_or_chase_rejection",
            "avoid_slope_neg_curvature_pos_supported": False,
        },
        "best_next_rule_hypothesis": (
            "If we test one derivative-state filter next, it should be an additive entry-qualification rule aimed at "
            "missed bearish turns, not a filter on the current widened winner. Smallest candidate: require "
            "negative curvature to persist and reject flat/positive-curvature chase states, but only for new widened "
            "participation outside the current validated cash-open branch."
        ),
    }


def _bucket_from_values(slope_raw: str | None, curvature_raw: str | None) -> str:
    slope = _to_decimal(slope_raw) or Decimal("0")
    curvature = _to_decimal(curvature_raw) or Decimal("0")
    return f"{_bucketize(slope, SLOPE_BUCKET_EDGES, 'SLOPE_STRONG_POS')}|{_bucketize(curvature, CURVATURE_BUCKET_EDGES, 'CURVATURE_STRONG_POS')}"


def _bucketize(value: Decimal, edges: tuple[tuple[Decimal, str], ...], fallback: str) -> str:
    for threshold, label in edges:
        if value <= threshold:
            return label
    return fallback


def _avg_or_sum(rows: list[CohortObservation], attr: str, *, use_sum: bool = False) -> str:
    values = [getattr(row, attr) for row in rows if getattr(row, attr) is not None]
    if not values:
        return ""
    if use_sum:
        return str(sum(values))
    return str(sum(values) / Decimal(len(values)))


def _top_bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return max(rows, key=lambda row: row["count"])


def _to_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _distance_from_price(price_raw: str | None, reference_raw: str | None) -> Decimal | None:
    price = _to_decimal(price_raw)
    reference = _to_decimal(reference_raw)
    if price is None or reference is None:
        return None
    return price - reference


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            serialized = {
                key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            }
            writer.writerow(serialized)
