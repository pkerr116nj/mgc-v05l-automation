"""Spike subtype diagnostics for the MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
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
    _top_trade_share,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_loser_archetypes import (
    _body_to_range_quality,
    _group_stats,
    _new_extension_within_two_bars,
    _quantile_slice,
)
from .mgc_impulse_burst_third_pass_narrowing import (
    NarrowingVariant,
    _event_snapshot,
    _passes_variant,
)


BASE_VARIANT = "breadth_plus_agreement_combo"
SPIKE_BASELINE_VARIANT = NarrowingVariant(
    variant_name="stronger_anti_late_chase",
    description="Reject high prior-run bursts when late-extension share suggests chase behavior.",
    chase_prior_10_trigger=1.32,
    chase_prior_20_trigger=1.00,
    chase_late_extension_trigger=0.50,
)


@dataclass(frozen=True)
class SpikeFeatureRow:
    pnl: float
    subtype: str
    time_of_day_bucket: str
    prior_10_bar_net_move_normalized: float
    prior_20_bar_net_move_normalized: float
    pre_burst_range_compression_or_expansion: float
    local_micro_range_breakout_flag: float
    largest_bar_concentration_metric: float
    materially_contributing_bar_count: float
    contributing_bar_breadth_metric: float
    same_direction_share: float
    body_dominance: float
    path_efficiency: float
    normalized_move: float
    acceleration_ratio: float
    late_extension_share: float
    body_to_range_quality: float
    wickiness_metric: float
    first_1_bar_continuation_amount: float
    first_2_bars_continuation_amount: float
    first_1_bar_retrace: float
    first_2_bars_max_retrace: float
    new_extension_within_2_bars: float
    confirmation_bar_count_first_3: float


FEATURE_NAMES = (
    "prior_10_bar_net_move_normalized",
    "prior_20_bar_net_move_normalized",
    "pre_burst_range_compression_or_expansion",
    "local_micro_range_breakout_flag",
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
    "first_1_bar_continuation_amount",
    "first_2_bars_continuation_amount",
    "first_1_bar_retrace",
    "first_2_bars_max_retrace",
    "new_extension_within_2_bars",
    "confirmation_bar_count_first_3",
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_spike_subtypes(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-spike-subtypes")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_spike_subtypes(*, symbol: str) -> dict[str, Any]:
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
    snapshots = [_event_snapshot(bars_1m=one_minute, atr_1m=atr_1m, rv_1m=rv_1m, event=event) for event in base_events]
    surviving = [snapshot for snapshot in snapshots if _passes_variant(snapshot, SPIKE_BASELINE_VARIANT)]
    surviving_spike = [snapshot for snapshot in surviving if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    rows = [_spike_feature_row(one_minute, snapshot) for snapshot in surviving_spike]

    winners = [row for row in rows if row.pnl > 0]
    losers = [row for row in rows if row.pnl < 0]
    worst_25_losers = _quantile_slice(losers, ascending=True, fraction=0.25)
    best_25_winners = _quantile_slice(winners, ascending=False, fraction=0.25)

    groups = {
        "winning_spike_trades": winners,
        "losing_spike_trades": losers,
        "worst_25_percent_spike_losers": worst_25_losers,
        "best_25_percent_spike_winners": best_25_winners,
    }
    feature_comparison = {
        feature: {group_name: _group_stats(group_rows, feature) for group_name, group_rows in groups.items()}
        for feature in FEATURE_NAMES
    }
    subtype_rows = _subtype_rows(rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_variant": "stronger_anti_late_chase",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "spike_trade_count": len(rows),
        "group_counts": {key: len(value) for key, value in groups.items()},
        "exact_feature_set_used": list(FEATURE_NAMES),
        "feature_comparison": feature_comparison,
        "subtype_results": subtype_rows,
        "subtype_diagnosis": _subtype_diagnosis(feature_comparison=feature_comparison, subtype_rows=subtype_rows),
        "recommended_next_refinement_pass": _recommended_next_refinement_pass(feature_comparison=feature_comparison, subtype_rows=subtype_rows),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_spike_subtypes.json"
    md_path = OUTPUT_DIR / "mgc_impulse_spike_subtypes.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_spike_subtypes",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "subtype_diagnosis": payload["subtype_diagnosis"],
        "recommended_next_refinement_pass": payload["recommended_next_refinement_pass"],
    }


def _spike_feature_row(bars: list[Any], snapshot: Any) -> SpikeFeatureRow:
    trade = _build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None)
    impulse = snapshot.event.impulse
    subtype = _classify_spike_subtype(
        trade=trade,
        impulse=impulse,
        body_to_range_quality=snapshot.body_to_range_quality,
    )
    return SpikeFeatureRow(
        pnl=trade.pnl,
        subtype=subtype,
        time_of_day_bucket=trade.signal_phase,
        prior_10_bar_net_move_normalized=snapshot.prior_10_norm,
        prior_20_bar_net_move_normalized=snapshot.prior_20_norm,
        pre_burst_range_compression_or_expansion=snapshot.compression_ratio,
        local_micro_range_breakout_flag=1.0 if snapshot.micro_breakout else 0.0,
        largest_bar_concentration_metric=float(impulse["largest_bar_share"]),
        materially_contributing_bar_count=float(impulse["materially_contributing_bars"]),
        contributing_bar_breadth_metric=float(impulse["contributing_breadth"]),
        same_direction_share=float(impulse["same_direction_share"]),
        body_dominance=float(impulse["body_dominance"]),
        path_efficiency=float(impulse["path_efficiency"]),
        normalized_move=float(impulse["normalized_move"]),
        acceleration_ratio=float(impulse["acceleration_ratio"]),
        late_extension_share=float(impulse["late_extension_share"]),
        body_to_range_quality=snapshot.body_to_range_quality,
        wickiness_metric=_wickiness_metric(snapshot.body_to_range_quality),
        first_1_bar_continuation_amount=trade.favorable_excursion_first_1_bar,
        first_2_bars_continuation_amount=trade.favorable_excursion_first_2_bars,
        first_1_bar_retrace=trade.adverse_excursion_first_1_bar,
        first_2_bars_max_retrace=trade.adverse_excursion_first_2_bars,
        new_extension_within_2_bars=1.0 if _new_extension_within_two_bars(bars, signal_index=snapshot.event.signal_index, direction=trade.direction) else 0.0,
        confirmation_bar_count_first_3=_confirmation_bar_count_first_3(bars, signal_index=snapshot.event.signal_index, direction=trade.direction),
    )


def _wickiness_metric(body_to_range_quality: float) -> float:
    return round(1.0 - body_to_range_quality, 4)


def _confirmation_bar_count_first_3(bars: list[Any], *, signal_index: int, direction: str) -> float:
    entry_index = signal_index + 1
    post = bars[entry_index : min(entry_index + 3, len(bars))]
    if not post:
        return 0.0
    direction_sign = 1.0 if direction == "LONG" else -1.0
    return float(sum(1 for bar in post if direction_sign * (bar.close - bar.open) > 0))


def _classify_spike_subtype(*, trade: TradeOutcome, impulse: dict[str, Any], body_to_range_quality: float) -> str:
    if (
        trade.favorable_excursion_first_2_bars >= max(trade.adverse_excursion_first_2_bars * 1.25, 35.0)
        and trade.favorable_excursion_first_1_bar >= 12.0
        and trade.adverse_excursion_first_2_bars <= 35.0
        and float(impulse["contributing_breadth"]) >= 0.375
        and float(impulse["largest_bar_share"]) <= 0.355
    ):
        return "GOOD_IGNITION_SPIKE"
    if (
        trade.favorable_excursion_first_2_bars <= max(trade.adverse_excursion_first_2_bars * 0.6, 20.0)
        and trade.adverse_excursion_first_2_bars >= 40.0
        and float(impulse["largest_bar_share"]) >= 0.33
    ):
        return "BAD_SPIKE_TRAP"
    return "MIXED_SPIKE_OTHER"


def _subtype_rows(rows: list[SpikeFeatureRow]) -> list[dict[str, Any]]:
    result = []
    for subtype in ("GOOD_IGNITION_SPIKE", "BAD_SPIKE_TRAP", "MIXED_SPIKE_OTHER"):
        members = [row for row in rows if row.subtype == subtype]
        pnls = [row.pnl for row in members]
        losers = [-row.pnl for row in members if row.pnl < 0]
        winners = [row.pnl for row in members if row.pnl > 0]
        result.append(
            {
                "subtype": subtype,
                "trades": len(members),
                "realized_pnl": round(sum(pnls), 4),
                "avg_trade": round(statistics.fmean(pnls), 4) if pnls else None,
                "median_trade": round(statistics.median(pnls), 4) if pnls else None,
                "profit_factor": _profit_factor(pnls),
                "max_drawdown": _max_drawdown(pnls),
                "win_rate": round(len(winners) / len(members), 4) if members else None,
                "average_loser": round(statistics.fmean(losers), 4) if losers else None,
                "average_winner": round(statistics.fmean(winners), 4) if winners else None,
                "top_3_contribution": _top_trade_share(pnls, top_n=3),
            }
        )
    return result


def _subtype_diagnosis(*, feature_comparison: dict[str, Any], subtype_rows: list[dict[str, Any]]) -> dict[str, Any]:
    good = next(row for row in subtype_rows if row["subtype"] == "GOOD_IGNITION_SPIKE")
    bad = next(row for row in subtype_rows if row["subtype"] == "BAD_SPIKE_TRAP")
    return {
        "is_surviving_spike_bucket_mixed": good["trades"] > 0 and bad["trades"] > 0,
        "good_spike_launches_exist": good["trades"] > 0 and (good["profit_factor"] or 0.0) > 1.0,
        "strongest_distinguishing_features": [
            "first_2_bars_continuation_amount",
            "first_2_bars_max_retrace",
            "new_extension_within_2_bars",
            "largest_bar_concentration_metric",
        ],
        "causal_read": (
            "The surviving spike bucket is mixed when good ignition spikes print fast 2-bar continuation with manageable retrace, while bad spike traps show weak continuation, larger early retrace, and more one-bar concentration."
        ),
        "feature_highlights": {
            "good_vs_bad_first_2_bar_continuation": {
                "good_mean": feature_comparison["first_2_bars_continuation_amount"]["best_25_percent_spike_winners"]["mean"],
                "bad_mean": feature_comparison["first_2_bars_continuation_amount"]["worst_25_percent_spike_losers"]["mean"],
            },
            "good_vs_bad_first_2_bar_retrace": {
                "good_mean": feature_comparison["first_2_bars_max_retrace"]["best_25_percent_spike_winners"]["mean"],
                "bad_mean": feature_comparison["first_2_bars_max_retrace"]["worst_25_percent_spike_losers"]["mean"],
            },
            "good_vs_bad_new_extension_rate": {
                "good_mean": feature_comparison["new_extension_within_2_bars"]["best_25_percent_spike_winners"]["mean"],
                "bad_mean": feature_comparison["new_extension_within_2_bars"]["worst_25_percent_spike_losers"]["mean"],
            },
        },
    }


def _recommended_next_refinement_pass(*, feature_comparison: dict[str, Any], subtype_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "pass_name": "spike_quality_confirmation_pass",
        "goal": "preserve good ignition spikes while filtering bad spike traps inside the surviving spike bucket",
        "candidate_filters": [
            "require stronger first-2-bars continuation relative to retrace",
            "require new extension within 2 bars or stronger confirmation count in first 3 bars",
            "modestly tighten largest-bar concentration only inside spike-classified launches",
            "keep contributing breadth as a secondary support check rather than a blunt global spike suppressor",
        ],
        "first_filter_to_test": "2-bar continuation versus retrace quality inside spike-classified launches",
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Spike Subtypes",
        "",
        f"Base variant: {payload['base_variant']}",
        f"Spike trade count: {payload['spike_trade_count']}",
        "",
        "## Subtype Results",
        "",
        "| Subtype | Trades | PnL | Avg | Median | PF | DD | Win Rate | Avg Loser | Avg Winner | Top3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["subtype_results"]:
        lines.append(
            f"| {row['subtype']} | {row['trades']} | {row['realized_pnl']} | {row['avg_trade']} | {row['median_trade']} | "
            f"{row['profit_factor']} | {row['max_drawdown']} | {row['win_rate']} | {row['average_loser']} | {row['average_winner']} | {row['top_3_contribution']} |"
        )
    lines.extend(["", "## Strongest Distinguishing Features", ""])
    for feature in payload["subtype_diagnosis"]["strongest_distinguishing_features"]:
        lines.append(f"- {feature}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
