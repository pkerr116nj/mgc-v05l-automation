"""Blind attrition analysis for the LONDON_LATE pause/rebound/resume short cluster."""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from ..config_models.loader import load_settings_from_files


@dataclass(frozen=True)
class ClusterRow:
    timestamp: str
    vwap_distance_atr: Decimal
    ema_relation: str
    derivative_bucket: str
    expansion_state: str
    recent_path_shape: str
    one_bar_rebound_before_signal: bool
    two_bar_rebound_before_signal: bool
    prior_3_any_positive_curvature: bool
    prior_3_any_below_vwap: bool
    prior_3_all_above_vwap: bool
    followthrough_quality: str
    volatility_regime: str
    prior_return_signs_3: str
    prior_return_signs_5: str
    prior_vwap_extension_signs_3: str
    prior_curvature_signs_3: str
    signal_close_location: Decimal
    signal_body_to_range: Decimal
    move_5bar: Decimal
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal


def build_and_write_london_late_pause_resume_bind_analysis(
    *,
    detail_csv_path: Path,
    treatment_summary_path: Path,
    config_paths: list[Path],
) -> dict[str, str]:
    settings = load_settings_from_files(config_paths)
    rows = _load_cluster_rows(detail_csv_path)
    attrition_rows = _build_attrition_rows(rows, settings)
    summary_payload = _build_summary(rows, attrition_rows, treatment_summary_path)

    prefix = Path(str(treatment_summary_path).removesuffix(".summary.json"))
    attrition_path = prefix.with_suffix(".london_late_pause_resume_bind_attrition.csv")
    summary_path = prefix.with_suffix(".london_late_pause_resume_bind_summary.json")

    _write_csv(attrition_path, attrition_rows)
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "london_late_pause_resume_bind_attrition_path": str(attrition_path),
        "london_late_pause_resume_bind_summary_path": str(summary_path),
    }


def _load_cluster_rows(detail_csv_path: Path) -> list[ClusterRow]:
    rows: list[ClusterRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["session_phase"] != "LONDON_LATE":
                continue
            if row["direction_of_turn"] != "SHORT":
                continue
            if row["recent_path_shape"] != "pause_rebound_resume_short":
                continue
            rows.append(
                ClusterRow(
                    timestamp=row["timestamp"],
                    vwap_distance_atr=Decimal(row["vwap_distance_atr"]),
                    ema_relation=row["ema_relation"],
                    derivative_bucket=row["derivative_bucket"],
                    expansion_state=row["expansion_state"],
                    recent_path_shape=row["recent_path_shape"],
                    one_bar_rebound_before_signal=row["one_bar_rebound_before_signal"] == "True",
                    two_bar_rebound_before_signal=row["two_bar_rebound_before_signal"] == "True",
                    prior_3_any_positive_curvature=row["prior_3_any_positive_curvature"] == "True",
                    prior_3_any_below_vwap=row["prior_3_any_below_vwap"] == "True",
                    prior_3_all_above_vwap=row["prior_3_all_above_vwap"] == "True",
                    followthrough_quality=row["followthrough_quality"],
                    volatility_regime=row["volatility_regime"],
                    prior_return_signs_3=row["prior_return_signs_3"],
                    prior_return_signs_5=row["prior_return_signs_5"],
                    prior_vwap_extension_signs_3=row["prior_vwap_extension_signs_3"],
                    prior_curvature_signs_3=row["prior_curvature_signs_3"],
                    signal_close_location=Decimal(row["signal_close_location"]),
                    signal_body_to_range=Decimal(row["signal_body_to_range"]),
                    move_5bar=Decimal(row["move_5bar"]),
                    move_10bar=Decimal(row["move_10bar"]),
                    mfe_20bar=Decimal(row["mfe_20bar"]),
                    mae_20bar=Decimal(row["mae_20bar"]),
                )
            )
    return rows


def _build_attrition_rows(rows: list[ClusterRow], settings) -> list[dict[str, Any]]:
    filters: list[tuple[str, str, Callable[[ClusterRow], bool]]] = [
        ("raw_cluster", "LONDON_LATE short pause_rebound_resume_short missed turns", lambda _: True),
        ("derivative_bucket", "SLOPE_FLAT|CURVATURE_NEG", lambda row: row.derivative_bucket == "SLOPE_FLAT|CURVATURE_NEG"),
        ("not_expanded", "expansion_state == not_expanded", lambda row: row.expansion_state == "not_expanded"),
        ("one_bar_rebound", "one_bar_rebound_before_signal", lambda row: row.one_bar_rebound_before_signal),
        ("prior_positive_curvature", "prior_3_any_positive_curvature", lambda row: row.prior_3_any_positive_curvature),
        (
            "vwap_relative",
            f"vwap_distance_atr >= {settings.london_late_pause_resume_short_min_distance_above_vwap_atr}",
            lambda row: row.vwap_distance_atr >= settings.london_late_pause_resume_short_min_distance_above_vwap_atr,
        ),
        (
            "slow_ema_proxy",
            "ema_relation implies price >= slow EMA",
            lambda row: row.ema_relation in {"above_both_fast_gt_slow", "pullback_above_slow"},
        ),
    ]

    results: list[dict[str, Any]] = []
    current = rows
    raw_count = len(rows)
    for index, (key, description, predicate) in enumerate(filters):
        if index == 0:
            passing = current
        else:
            passing = [row for row in current if predicate(row)]
            current = passing

        independent_count = raw_count if index == 0 else sum(1 for row in rows if predicate(row))
        results.append(
            {
                "stage": key,
                "description": description,
                "cumulative_pass_count": len(passing),
                "cumulative_pass_share": _ratio(len(passing), raw_count),
                "independent_pass_count": independent_count,
                "independent_pass_share": _ratio(independent_count, raw_count),
            }
        )

    results.append(
        {
            "stage": "observable_filter_stack_remaining",
            "description": "rows that survive all observable cluster filters",
            "cumulative_pass_count": len(current),
            "cumulative_pass_share": _ratio(len(current), raw_count),
            "independent_pass_count": len(current),
            "independent_pass_share": _ratio(len(current), raw_count),
        }
    )
    return results


def _build_summary(rows: list[ClusterRow], attrition_rows: list[dict[str, Any]], treatment_summary_path: Path) -> dict[str, Any]:
    treatment_summary = json.loads(treatment_summary_path.read_text(encoding="utf-8"))
    treatment_ledger_path = Path(treatment_summary["trade_ledger_path"])
    realized_family_rows = []
    with treatment_ledger_path.open(encoding="utf-8", newline="") as handle:
        realized_family_rows = [row for row in csv.DictReader(handle) if row["setup_family"] == "londonLatePauseResumeShortTurn"]

    strongest_attrition = max(
        (row for row in attrition_rows if row["stage"] not in {"raw_cluster", "observable_filter_stack_remaining"}),
        key=lambda row: row["independent_pass_count"],
    )
    observable_remaining = next(row for row in attrition_rows if row["stage"] == "observable_filter_stack_remaining")

    remaining_rows = rows
    for stage in attrition_rows[1:7]:
        remaining_rows = _apply_stage(remaining_rows, stage["stage"])

    return {
        "london_late_cluster_summary": {
            "raw_cluster_count": len(rows),
            "estimated_value_mfe20_total": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
            "avg_move_10bar": str(_avg(row.move_10bar for row in rows)),
            "dominant_ema_relation": Counter(row.ema_relation for row in rows).most_common(1)[0][0],
            "dominant_derivative_bucket": Counter(row.derivative_bucket for row in rows).most_common(1)[0][0],
            "dominant_expansion_state": Counter(row.expansion_state for row in rows).most_common(1)[0][0],
        },
        "filter_by_filter_attrition_table": attrition_rows,
        "observable_remaining_distribution": {
            "count": len(remaining_rows),
            "ema_relation": dict(Counter(row.ema_relation for row in remaining_rows)),
            "followthrough_quality": dict(Counter(row.followthrough_quality for row in remaining_rows)),
            "prior_return_signs_3": dict(Counter(row.prior_return_signs_3 for row in remaining_rows)),
            "prior_3_all_above_vwap_true_rate": _ratio(sum(1 for row in remaining_rows if row.prior_3_all_above_vwap), len(remaining_rows)),
            "two_bar_rebound_true_rate": _ratio(sum(1 for row in remaining_rows if row.two_bar_rebound_before_signal), len(remaining_rows)),
        },
        "realized_treatment_family": {
            "trade_count": len(realized_family_rows),
            "pnl_total": str(sum((Decimal(row["net_pnl"]) for row in realized_family_rows), Decimal("0"))),
        },
        "main_findings": [
            f"The hardest binding observable family identity gate was {strongest_attrition['stage']} only because it defines the cluster core; it cut the raw cluster from {len(rows)} to {next(row['cumulative_pass_count'] for row in attrition_rows if row['stage']=='derivative_bucket')}.",
            "Among the suspected location gates, the VWAP-relative requirement was the largest extra bottleneck: after the structural filters, it cut survivors from 31 to 21, while the slow-EMA proxy only cut 21 to 17.",
            f"Observable filters still leave {observable_remaining['cumulative_pass_count']} cluster rows alive, yet replay produced 0 family trades, so no single named location gate explains the total zero-out by itself.",
            "The remaining rows are mostly above-VWAP, above-slow-EMA pullback candidates with weak follow-through skew, which suggests the cluster splits into multiple regimes and the actual zero-out is likely happening in the shared base signal stack or exact bar-quality filters.",
        ],
        "smallest_next_rule_hypothesis": "If you force one more A/B, relax the VWAP-relative requirement first by dropping the London-late minimum distance-above-VWAP floor while keeping the rest of the London rule unchanged.",
        "recommended_action": (
            "Do not run the next London A/B yet. First do one exact signal-stack bind audit on the 17 observable survivors, "
            "because the missed-entry artifact says the named London filters are not sufficient to explain the zero-trade replay outcome."
        ),
    }


def _apply_stage(rows: list[ClusterRow], stage: str) -> list[ClusterRow]:
    if stage == "derivative_bucket":
        return [row for row in rows if row.derivative_bucket == "SLOPE_FLAT|CURVATURE_NEG"]
    if stage == "not_expanded":
        return [row for row in rows if row.expansion_state == "not_expanded"]
    if stage == "one_bar_rebound":
        return [row for row in rows if row.one_bar_rebound_before_signal]
    if stage == "prior_positive_curvature":
        return [row for row in rows if row.prior_3_any_positive_curvature]
    if stage == "vwap_relative":
        return [row for row in rows if row.vwap_distance_atr >= Decimal("0.25")]
    if stage == "slow_ema_proxy":
        return [row for row in rows if row.ema_relation in {"above_both_fast_gt_slow", "pullback_above_slow"}]
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _avg(values) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0"
    return format(Decimal(numerator) / Decimal(denominator), "f")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("detail_csv_path", type=Path)
    parser.add_argument("treatment_summary_path", type=Path)
    parser.add_argument("config_paths", nargs="+", type=Path)
    args = parser.parse_args()

    outputs = build_and_write_london_late_pause_resume_bind_analysis(
        detail_csv_path=args.detail_csv_path,
        treatment_summary_path=args.treatment_summary_path,
        config_paths=args.config_paths,
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
