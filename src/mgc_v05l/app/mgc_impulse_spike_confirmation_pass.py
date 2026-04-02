"""Spike-quality confirmation pass for the MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from typing import Any

from .mgc_impulse_burst_asymmetry_report import _build_trade_outcome, _collect_candidate_events
from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    _build_latest_context_lookup,
    _max_drawdown,
    _profit_factor,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
    _survives_without_top,
    _top_trade_share,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_third_pass_narrowing import (
    NarrowingVariant,
    _event_snapshot,
    _passes_variant,
)
from .mgc_impulse_spike_subtypes import (
    SPIKE_BASELINE_VARIANT,
    SpikeFeatureRow,
    _spike_feature_row,
)


BASE_VARIANT = "breadth_plus_agreement_combo"


@dataclass(frozen=True)
class ConfirmationVariant:
    variant_name: str
    description: str
    require_new_extension: bool = False
    min_confirmation_bar_count: int | None = None
    min_first_2_bar_continuation: float | None = None
    max_first_2_bar_retrace: float | None = None
    require_continuation_over_retrace: bool = False
    max_largest_bar_share: float | None = None


CURRENT_PATH_VARIANTS: tuple[ConfirmationVariant, ...] = (
    ConfirmationVariant(
        variant_name="base_stronger_anti_late_chase_control",
        description="Current best structural path with no extra spike confirmation gate.",
    ),
    ConfirmationVariant(
        variant_name="minimal_post_trigger_confirmation_rule",
        description="Require new extension and at least 2 confirming bars inside the first 3 bars for spike-classified launches.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
    ),
    ConfirmationVariant(
        variant_name="minimal_retrace_quality_rule",
        description="Require manageable 2-bar retrace and continuation not weaker than retrace for spike-classified launches.",
        max_first_2_bar_retrace=45.0,
        require_continuation_over_retrace=True,
    ),
    ConfirmationVariant(
        variant_name="compact_confirmation_plus_retrace_combo",
        description="Require new extension plus acceptable 2-bar continuation versus retrace.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
        min_first_2_bar_continuation=40.0,
        max_first_2_bar_retrace=45.0,
        require_continuation_over_retrace=True,
    ),
    ConfirmationVariant(
        variant_name="best_judgment_spike_quality",
        description="Compact best-judgment spike-quality gate with one modest shape guard.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
        min_first_2_bar_continuation=45.0,
        max_first_2_bar_retrace=40.0,
        require_continuation_over_retrace=True,
        max_largest_bar_share=0.345,
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_spike_confirmation_pass(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-spike-confirmation-pass")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_spike_confirmation_pass(*, symbol: str) -> dict[str, Any]:
    spec = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == BASE_VARIANT)
    one_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_DETECTION_TIMEFRAME)
    five_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_CONTEXT_TIMEFRAME)
    overlap_start = max(one_minute_bars[0].timestamp, five_minute_bars[0].timestamp)
    overlap_end = min(one_minute_bars[-1].timestamp, five_minute_bars[-1].timestamp)
    one_minute = [bar for bar in one_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    five_minute = [bar for bar in five_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    atr_1m = _rolling_atr(one_minute, length=14)
    rv_1m = _rolling_realized_vol(one_minute, length=20)
    vol_baseline_1m = _rolling_mean([bar.volume for bar in one_minute], length=20)
    atr_5m = _rolling_atr(five_minute, length=14)
    context_lookup = _build_latest_context_lookup(one_minute=one_minute, five_minute=five_minute)

    base_events = _collect_candidate_events(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=spec,
    )
    raw_snapshots = [_event_snapshot(bars_1m=one_minute, atr_1m=atr_1m, rv_1m=rv_1m, event=event) for event in base_events]
    current_snapshots = [snapshot for snapshot in raw_snapshots if _passes_variant(snapshot, SPIKE_BASELINE_VARIANT)]

    current_rows = [_evaluate_confirmation_variant(bars=one_minute, snapshots=current_snapshots, variant=variant) for variant in CURRENT_PATH_VARIANTS]
    best_current = _pick_best_current_variant(current_rows)
    raw_reference = _evaluate_confirmation_variant(
        bars=one_minute,
        snapshots=raw_snapshots,
        variant=next(variant for variant in CURRENT_PATH_VARIANTS if variant.variant_name == best_current["variant_name"]),
    )

    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_variant": "stronger_anti_late_chase",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "exact_metrics_used": _metric_notes(),
        "confirmation_variants_tested": [row["variant_name"] for row in current_rows],
        "variant_results": current_rows,
        "best_current_path_variant": {
            "variant_name": best_current["variant_name"],
            "decision_bucket": best_current["decision_bucket"],
        },
        "raw_population_reconsideration": _raw_population_reconsideration(best_current=best_current, raw_reference=raw_reference),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_spike_confirmation_pass.json"
    md_path = OUTPUT_DIR / "mgc_impulse_spike_confirmation_pass.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_spike_confirmation_pass",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_current_path_variant": payload["best_current_path_variant"],
        "raw_population_reconsideration": payload["raw_population_reconsideration"],
    }


def _evaluate_confirmation_variant(*, bars: list[Any], snapshots: list[Any], variant: ConfirmationVariant) -> dict[str, Any]:
    spike_rows = [_spike_feature_row(bars, snapshot) for snapshot in snapshots if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    non_spike_snapshots = [snapshot for snapshot in snapshots if snapshot.subclass_bucket != "SPIKE_DOMINATED_OTHER"]

    good_total = sum(1 for row in spike_rows if row.subtype == "GOOD_IGNITION_SPIKE")
    bad_total = sum(1 for row in spike_rows if row.subtype == "BAD_SPIKE_TRAP")
    retained_spike_rows = [row for row in spike_rows if _passes_confirmation_variant(row, variant)]
    retained_spike_trade_keys = {(row.time_of_day_bucket, row.pnl, row.first_2_bars_continuation_amount, row.first_2_bars_max_retrace) for row in retained_spike_rows}

    all_trades = []
    for snapshot in non_spike_snapshots:
        all_trades.append(_build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None))
    for snapshot in snapshots:
        if snapshot.subclass_bucket != "SPIKE_DOMINATED_OTHER":
            continue
        row = _spike_feature_row(bars, snapshot)
        key = (row.time_of_day_bucket, row.pnl, row.first_2_bars_continuation_amount, row.first_2_bars_max_retrace)
        if key in retained_spike_trade_keys:
            all_trades.append(_build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None))

    metrics = _metrics(all_trades)
    good_retained = sum(1 for row in retained_spike_rows if row.subtype == "GOOD_IGNITION_SPIKE")
    bad_retained = sum(1 for row in retained_spike_rows if row.subtype == "BAD_SPIKE_TRAP")
    decision_bucket = _decision_bucket(
        variant=variant,
        metrics=metrics,
        control_count=len(snapshots),
        good_preserved=_share(good_retained, good_total),
        bad_removed=1.0 - _share(bad_retained, bad_total),
    )
    return {
        "variant_name": variant.variant_name,
        "description": variant.description,
        "rules": _variant_rule_summary(variant),
        "metrics": metrics,
        "subtype_preservation_vs_removal": {
            "GOOD_IGNITION_SPIKE_retained_count": good_retained,
            "BAD_SPIKE_TRAP_retained_count": bad_retained,
            "percent_GOOD_IGNITION_SPIKE_preserved": _share(good_retained, good_total),
            "percent_BAD_SPIKE_TRAP_removed": 1.0 - _share(bad_retained, bad_total),
        },
        "decision_bucket": decision_bucket,
    }


def _passes_confirmation_variant(row: SpikeFeatureRow, variant: ConfirmationVariant) -> bool:
    if row.subtype != "GOOD_IGNITION_SPIKE" and row.subtype != "BAD_SPIKE_TRAP" and variant.variant_name == "base_stronger_anti_late_chase_control":
        return True
    if variant.variant_name == "base_stronger_anti_late_chase_control":
        return True
    if row.new_extension_within_2_bars < 1.0 and variant.require_new_extension:
        return False
    if variant.min_confirmation_bar_count is not None and row.confirmation_bar_count_first_3 < variant.min_confirmation_bar_count:
        return False
    if variant.min_first_2_bar_continuation is not None and row.first_2_bars_continuation_amount < variant.min_first_2_bar_continuation:
        return False
    if variant.max_first_2_bar_retrace is not None and row.first_2_bars_max_retrace > variant.max_first_2_bar_retrace:
        return False
    if variant.require_continuation_over_retrace and row.first_2_bars_continuation_amount < row.first_2_bars_max_retrace:
        return False
    if variant.max_largest_bar_share is not None and row.largest_bar_concentration_metric > variant.max_largest_bar_share:
        return False
    return True


def _metrics(trades: list[Any]) -> dict[str, Any]:
    pnls = [trade.pnl for trade in trades]
    losers = [-trade.pnl for trade in trades if trade.pnl < 0]
    winners = [trade.pnl for trade in trades if trade.pnl > 0]
    return {
        "trades": len(trades),
        "realized_pnl": round(sum(pnls), 4),
        "avg_trade": _mean_or_none(pnls),
        "median_trade": _median_or_none(pnls),
        "profit_factor": _profit_factor(pnls),
        "max_drawdown": _max_drawdown(pnls),
        "win_rate": round(len(winners) / len(trades), 4) if trades else None,
        "average_loser": _mean_or_none(losers),
        "median_loser": _median_or_none(losers),
        "p95_loser": _percentile_or_none(losers, 0.95),
        "worst_loser": round(max(losers), 4) if losers else None,
        "average_winner": _mean_or_none(winners),
        "avg_winner_over_avg_loser": _safe_ratio(_mean_or_none(winners), _mean_or_none(losers)),
        "top_1_contribution": _top_trade_share(pnls, top_n=1),
        "top_3_contribution": _top_trade_share(pnls, top_n=3),
        "survives_without_top_1": _survives_without_top(pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        "large_winner_count": _count_above_threshold(winners, 84.0),
        "very_large_winner_count": _count_above_threshold(winners, 140.0),
    }


def _decision_bucket(*, variant: ConfirmationVariant, metrics: dict[str, Any], control_count: int, good_preserved: float, bad_removed: float) -> str:
    trade_ratio = (metrics["trades"] or 0) / max(control_count, 1)
    if metrics["profit_factor"] and metrics["profit_factor"] >= 1.15 and good_preserved >= 0.6 and bad_removed >= 0.5 and trade_ratio >= 0.65:
        return "CLEANER_AND_STILL_REAL"
    if metrics["profit_factor"] and metrics["profit_factor"] >= 1.0 and good_preserved >= 0.4 and bad_removed >= 0.35 and trade_ratio >= 0.45:
        return "IMPROVED_BUT_STILL_MIXED"
    return "TOO_CONFIRMATION_DESTRUCTIVE"


def _pick_best_current_variant(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            {
                "CLEANER_AND_STILL_REAL": 2,
                "IMPROVED_BUT_STILL_MIXED": 1,
                "TOO_CONFIRMATION_DESTRUCTIVE": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _raw_population_reconsideration(*, best_current: dict[str, Any], raw_reference: dict[str, Any]) -> dict[str, Any]:
    verdict = "CURRENT_PATH_REMAINS_OPTIMAL"
    if (
        (raw_reference["metrics"]["profit_factor"] or 0.0) > (best_current["metrics"]["profit_factor"] or 0.0) + 0.03
        and (raw_reference["metrics"]["realized_pnl"] or 0.0) >= (best_current["metrics"]["realized_pnl"] or 0.0) * 0.95
    ):
        verdict = "RAW_POPULATION_CONTAINS_BETTER_SUBTYPE"
    elif abs((raw_reference["metrics"]["realized_pnl"] or 0.0) - (best_current["metrics"]["realized_pnl"] or 0.0)) <= 75.0:
        verdict = "MIXED_RESULT_NEEDS_ONE_MORE_AUDIT"
    return {
        "title": "RAW_POPULATION_RECONSIDERATION",
        "verdict": verdict,
        "current_path_best_variant": best_current["variant_name"],
        "current_path_metrics": best_current["metrics"],
        "raw_population_same_rule_metrics": raw_reference["metrics"],
        "current_path_subtype_preservation_vs_removal": best_current["subtype_preservation_vs_removal"],
        "raw_population_subtype_preservation_vs_removal": raw_reference["subtype_preservation_vs_removal"],
    }


def _variant_rule_summary(variant: ConfirmationVariant) -> dict[str, Any]:
    return {
        "require_new_extension": variant.require_new_extension,
        "min_confirmation_bar_count": variant.min_confirmation_bar_count,
        "min_first_2_bar_continuation": variant.min_first_2_bar_continuation,
        "max_first_2_bar_retrace": variant.max_first_2_bar_retrace,
        "require_continuation_over_retrace": variant.require_continuation_over_retrace,
        "max_largest_bar_share": variant.max_largest_bar_share,
    }


def _metric_notes() -> dict[str, Any]:
    return {
        "confirmation_metrics": [
            "first_1_bar_continuation_amount",
            "first_2_bars_continuation_amount",
            "new_extension_within_2_bars",
            "confirmation_bar_count_first_3",
        ],
        "retrace_metrics": [
            "first_1_bar_retrace",
            "first_2_bars_max_retrace",
        ],
        "shape_metrics": [
            "largest_bar_concentration_metric",
            "materially_contributing_bar_count",
            "contributing_bar_breadth_metric",
            "same_direction_share",
            "body_dominance",
            "path_efficiency",
            "normalized_move",
            "acceleration_ratio",
            "late_extension_share",
            "body_to_range_quality",
            "wickiness_metric",
        ],
        "methodology_note": "Confirmation rules are evaluated as research-only admission gates on the current base entry path, not as delayed-entry execution redesigns.",
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Spike Confirmation Pass",
        "",
        f"Base variant: {payload['base_variant']}",
        "",
        "## Variant Results",
        "",
        "| Variant | Bucket | Trades | PnL | PF | DD | Median | Avg Loser | Avg Winner | Top3 | Good Kept % | Bad Removed % |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        subtype = row["subtype_preservation_vs_removal"]
        lines.append(
            f"| {row['variant_name']} | {row['decision_bucket']} | {metrics['trades']} | {metrics['realized_pnl']} | {metrics['profit_factor']} | "
            f"{metrics['max_drawdown']} | {metrics['median_trade']} | {metrics['average_loser']} | {metrics['average_winner']} | {metrics['top_3_contribution']} | "
            f"{subtype['percent_GOOD_IGNITION_SPIKE_preserved']} | {subtype['percent_BAD_SPIKE_TRAP_removed']} |"
        )
    lines.extend(
        [
            "",
            "## RAW_POPULATION_RECONSIDERATION",
            "",
            f"- verdict: {payload['raw_population_reconsideration']['verdict']}",
        ]
    )
    return "\n".join(lines)


def _share(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return round(part / whole, 4)


def _mean_or_none(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 4) if values else None


def _median_or_none(values: list[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def _percentile_or_none(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    raw_index = (len(ordered) - 1) * pct
    lower = int(math.floor(raw_index))
    upper = int(math.ceil(raw_index))
    if lower == upper:
        return round(ordered[lower], 4)
    weight = raw_index - lower
    return round((ordered[lower] * (1.0 - weight)) + (ordered[upper] * weight), 4)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, 4)


def _count_above_threshold(values: list[float], threshold: float) -> int:
    return sum(1 for value in values if value >= threshold)


if __name__ == "__main__":
    raise SystemExit(main())
