"""Local-neighborhood validation for the MGC impulse confirmation candidate."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from typing import Any

from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    _profit_factor,
    _build_latest_context_lookup,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_third_pass_narrowing import _event_snapshot
from .mgc_impulse_burst_asymmetry_report import _build_trade_outcome
from .mgc_impulse_spike_confirmation_pass import (
    ConfirmationVariant,
    CURRENT_PATH_VARIANTS,
    _decision_bucket,
    _evaluate_confirmation_variant,
    _metrics,
    _variant_rule_summary,
)
from .mgc_impulse_spike_subtypes import SPIKE_BASELINE_VARIANT
from .mgc_impulse_burst_asymmetry_report import _collect_candidate_events
from .mgc_impulse_burst_third_pass_narrowing import _passes_variant


BASE_VARIANT = "breadth_plus_agreement_combo"


VALIDATION_VARIANTS: tuple[ConfirmationVariant, ...] = (
    ConfirmationVariant(
        variant_name="base_raw_minimal_confirmation_control",
        description="Raw breadth_plus_agreement_combo population gated by new_extension_within_2_bars and confirmation_bar_count_first_3 >= 2.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
    ),
    ConfirmationVariant(
        variant_name="slightly_looser_confirmation",
        description="Loosen confirmation by allowing one confirming bar in the first 3 bars while still requiring new extension.",
        require_new_extension=True,
        min_confirmation_bar_count=1,
    ),
    ConfirmationVariant(
        variant_name="slightly_tighter_confirmation",
        description="Tighten confirmation by requiring all first 3 bars to show at least 3 confirming bars.",
        require_new_extension=True,
        min_confirmation_bar_count=3,
    ),
    ConfirmationVariant(
        variant_name="mild_retrace_sanity_check",
        description="Base confirmation plus a mild 2-bar retrace cap.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
        max_first_2_bar_retrace=45.0,
    ),
    ConfirmationVariant(
        variant_name="best_judgment_compact_combo",
        description="Base confirmation plus mild retrace sanity and continuation over retrace.",
        require_new_extension=True,
        min_confirmation_bar_count=2,
        max_first_2_bar_retrace=45.0,
        require_continuation_over_retrace=True,
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_confirmation_validation(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-confirmation-validation")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_confirmation_validation(*, symbol: str) -> dict[str, Any]:
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

    current_reference_variant = next(v for v in CURRENT_PATH_VARIANTS if v.variant_name == "minimal_post_trigger_confirmation_rule")
    current_reference = _evaluate_confirmation_variant(
        bars=one_minute,
        snapshots=current_snapshots,
        variant=current_reference_variant,
    )

    variant_rows = []
    for variant in VALIDATION_VARIANTS:
        row = _evaluate_confirmation_variant(
            bars=one_minute,
            snapshots=raw_snapshots,
            variant=variant,
        )
        row["pocket_descriptives"] = _pocket_descriptives(
            bars=one_minute,
            snapshots=raw_snapshots,
            variant=variant,
        )
        row["decision_bucket"] = _validation_bucket(
            metrics=row["metrics"],
            subtype=row["subtype_preservation_vs_removal"],
            trade_count=row["metrics"]["trades"],
            control_trade_count=variant_rows[0]["metrics"]["trades"] if variant_rows else row["metrics"]["trades"],
        )
        variant_rows.append(row)

    best_variant = _pick_best_variant(variant_rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_candidate_definition": {
            "population": "raw breadth_plus_agreement_combo",
            "confirmation_rule": "require new_extension_within_2_bars and confirmation_bar_count_first_3 >= 2",
        },
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "validation_variants_tested": [row["variant_name"] for row in variant_rows],
        "variant_results": variant_rows,
        "current_path_reference": current_reference,
        "raw_vs_current_confirmation": {
            "raw_population_best_variant": best_variant["variant_name"],
            "raw_population_best_bucket": best_variant["decision_bucket"],
            "raw_population_best_metrics": best_variant["metrics"],
            "current_path_reference_metrics": current_reference["metrics"],
            "verdict": (
                "RAW_POPULATION_MINIMAL_CONFIRMATION_REMAINS_BEST"
                if (best_variant["metrics"]["realized_pnl"] or 0.0) >= (current_reference["metrics"]["realized_pnl"] or 0.0)
                and (best_variant["metrics"]["profit_factor"] or 0.0) >= (current_reference["metrics"]["profit_factor"] or 0.0)
                else "CURRENT_PATH_REFERENCE_STILL_COMPETITIVE"
            ),
        },
        "validation_conclusion": {
            "best_variant": best_variant["variant_name"],
            "best_bucket": best_variant["decision_bucket"],
            "mgc_paper_readiness": best_variant["decision_bucket"] == "VALIDATED_MGC_PAPER_CANDIDATE",
            "mnq_transfer_readiness": False,
            "remaining_transfer_blocker": (
                None
                if best_variant["decision_bucket"] == "VALIDATED_MGC_PAPER_CANDIDATE"
                else "Needs one more MGC pass to confirm near-neighbor stability and trade-count resilience before transfer."
            ),
        },
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_confirmation_validation.json"
    md_path = OUTPUT_DIR / "mgc_impulse_confirmation_validation.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_confirmation_validation",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "validation_conclusion": payload["validation_conclusion"],
    }


def _validation_bucket(*, metrics: dict[str, Any], subtype: dict[str, Any], trade_count: int, control_trade_count: int) -> str:
    trade_ratio = trade_count / max(control_trade_count, 1)
    if (
        (metrics["profit_factor"] or 0.0) >= 2.5
        and (metrics["median_trade"] or -999999.0) > 0
        and (metrics["top_3_contribution"] or 999999.0) <= 60.0
        and metrics["survives_without_top_3"]
        and subtype["percent_GOOD_IGNITION_SPIKE_preserved"] >= 0.7
        and subtype["percent_BAD_SPIKE_TRAP_removed"] >= 0.9
        and trade_ratio >= 0.6
    ):
        return "VALIDATED_MGC_PAPER_CANDIDATE"
    if (
        (metrics["profit_factor"] or 0.0) >= 1.8
        and (metrics["median_trade"] or -999999.0) > 0
        and metrics["survives_without_top_3"]
        and subtype["percent_BAD_SPIKE_TRAP_removed"] >= 0.75
        and trade_ratio >= 0.4
    ):
        return "PROMISING_BUT_ONE_MORE_MGC_PASS_NEEDED"
    if (
        (metrics["profit_factor"] or 0.0) >= 1.3
        and subtype["percent_BAD_SPIKE_TRAP_removed"] >= 0.75
    ):
        return "CURRENT_WINNER_REMAINS_BEST_BUT_NOT_READY"
    return "TOO_BRITTLE_FOR_PAPER"


def _pick_best_variant(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            {
                "VALIDATED_MGC_PAPER_CANDIDATE": 3,
                "PROMISING_BUT_ONE_MORE_MGC_PASS_NEEDED": 2,
                "CURRENT_WINNER_REMAINS_BEST_BUT_NOT_READY": 1,
                "TOO_BRITTLE_FOR_PAPER": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _pocket_descriptives(*, bars: list[Any], snapshots: list[Any], variant: ConfirmationVariant) -> list[dict[str, Any]]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    from .mgc_impulse_spike_subtypes import _spike_feature_row

    spike_rows = [_spike_feature_row(bars, snapshot) for snapshot in snapshots if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    retained_spike_keys = {
        (row.time_of_day_bucket, row.pnl, row.first_2_bars_continuation_amount, row.first_2_bars_max_retrace)
        for row in spike_rows
        if _passes_variant_spike(row, variant)
    }

    for snapshot in snapshots:
        trade = _build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None)
        if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER":
            row = _spike_feature_row(bars, snapshot)
            key = (row.time_of_day_bucket, row.pnl, row.first_2_bars_continuation_amount, row.first_2_bars_max_retrace)
            if key not in retained_spike_keys:
                continue
        grouped[trade.signal_phase].append(trade)

    ordered_pockets = ["ASIA_EARLY", "ASIA_LATE", "LONDON_OPEN", "US_MIDDAY", "US_LATE"]
    rows = []
    for pocket in ordered_pockets:
        trades = grouped.get(pocket, [])
        pnls = [trade.pnl for trade in trades]
        winners = [p for p in pnls if p > 0]
        rows.append(
            {
                "session_pocket": pocket,
                "trades": len(trades),
                "realized_pnl": round(sum(pnls), 4),
                "profit_factor": _profit_factor(pnls),
                "win_rate": round(len(winners) / len(trades), 4) if trades else None,
            }
        )
    return rows


def _passes_variant_spike(row: Any, variant: ConfirmationVariant) -> bool:
    from .mgc_impulse_spike_confirmation_pass import _passes_confirmation_variant

    return _passes_confirmation_variant(row, variant)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Confirmation Validation",
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
    lines.extend(["", "## Pocket Descriptives (Best Variant)", "", "| Pocket | Trades | PnL | PF | Win Rate |", "| --- | ---: | ---: | ---: | ---: |"])
    best = next(row for row in payload["variant_results"] if row["variant_name"] == payload["validation_conclusion"]["best_variant"])
    for pocket in best["pocket_descriptives"]:
        lines.append(
            f"| {pocket['session_pocket']} | {pocket['trades']} | {pocket['realized_pnl']} | {pocket['profit_factor']} | {pocket['win_rate']} |"
        )
    return "\n".join(lines)


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
