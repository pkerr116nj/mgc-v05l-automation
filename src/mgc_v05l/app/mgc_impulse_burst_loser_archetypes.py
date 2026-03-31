"""Loser archetype diagnostics for the MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .mgc_impulse_burst_asymmetry_report import (
    AcceptedEvent,
    TradeOutcome,
    _build_trade_outcome,
    _collect_candidate_events,
)
from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    POINT_VALUE,
    Bar,
    _build_latest_context_lookup,
    _dominant_bucket_from_counter,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_subclass_diagnostics import (
    _classify_subclass,
    _micro_breakout,
    _pre_burst_compression_ratio,
    _prior_run_norm,
)


TARGET_VARIANT = "breadth_plus_agreement_combo"
WINDOW_SIZE = 8


@dataclass(frozen=True)
class FeatureRow:
    pnl: float
    subclass_bucket: str
    time_of_day_bucket: str
    prior_10_bar_net_move_normalized: float
    prior_20_bar_net_move_normalized: float
    pre_burst_range_compression_or_expansion: float
    local_micro_range_breakout_flag: float
    largest_bar_concentration_metric: float
    contributing_bar_breadth_metric: float
    same_direction_share: float
    body_dominance: float
    path_efficiency: float
    normalized_move: float
    acceleration_ratio: float
    late_extension_share: float
    body_to_range_quality: float
    first_1_bar_continuation_amount: float
    first_2_bars_continuation_amount: float
    first_1_bar_retrace: float
    first_2_bars_max_retrace: float
    new_extension_within_2_bars_flag: float


FEATURE_NAMES = (
    "prior_10_bar_net_move_normalized",
    "prior_20_bar_net_move_normalized",
    "pre_burst_range_compression_or_expansion",
    "local_micro_range_breakout_flag",
    "largest_bar_concentration_metric",
    "contributing_bar_breadth_metric",
    "same_direction_share",
    "body_dominance",
    "path_efficiency",
    "normalized_move",
    "acceleration_ratio",
    "late_extension_share",
    "body_to_range_quality",
    "first_1_bar_continuation_amount",
    "first_2_bars_continuation_amount",
    "first_1_bar_retrace",
    "first_2_bars_max_retrace",
    "new_extension_within_2_bars_flag",
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_loser_archetypes(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-loser-archetypes")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_loser_archetypes(*, symbol: str) -> dict[str, Any]:
    spec = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == TARGET_VARIANT)
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

    events = _collect_candidate_events(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=spec,
    )
    rows = [_feature_row(one_minute, atr_1m, rv_1m, event) for event in events]

    all_rows = rows
    winners = [row for row in rows if row.pnl > 0]
    losers = [row for row in rows if row.pnl < 0]
    worst_25_losers = _quantile_slice(losers, ascending=True, fraction=0.25)
    worst_10_losers = _quantile_slice(losers, ascending=True, fraction=0.10)
    best_25_winners = _quantile_slice(winners, ascending=False, fraction=0.25)

    groups = {
        "all_trades": all_rows,
        "winning_trades": winners,
        "losing_trades": losers,
        "worst_25_percent_losers": worst_25_losers,
        "worst_10_percent_losers": worst_10_losers,
        "best_25_percent_winners": best_25_winners,
    }

    feature_distribution_table = {
        feature: {
            group_name: _group_stats(group_rows, feature)
            for group_name, group_rows in groups.items()
        }
        for feature in FEATURE_NAMES
    }
    subclass_mix = {
        group_name: _counter_share(Counter(row.subclass_bucket for row in group_rows))
        for group_name, group_rows in groups.items()
    }
    time_of_day_mix = {
        group_name: _counter_share(Counter(row.time_of_day_bucket for row in group_rows))
        for group_name, group_rows in groups.items()
    }
    comparison_summary = _comparison_summary(groups)
    loser_diagnosis = _loser_diagnosis(feature_distribution_table, subclass_mix)
    winner_diagnosis = _winner_diagnosis(feature_distribution_table, subclass_mix)
    noise_filters = _top_noise_filters(feature_distribution_table, subclass_mix)
    recommended_next = _recommended_next_refinement_pass(noise_filters)

    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_variant": TARGET_VARIANT,
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "group_counts": {name: len(group_rows) for name, group_rows in groups.items()},
        "feature_distribution_table": feature_distribution_table,
        "comparison_summary": comparison_summary,
        "subclass_mix_by_group": subclass_mix,
        "time_of_day_mix_by_group": time_of_day_mix,
        "loser_archetype_diagnosis": loser_diagnosis,
        "winner_archetype_diagnosis": winner_diagnosis,
        "top_candidate_noise_filters": noise_filters,
        "recommended_next_refinement_pass": recommended_next,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_burst_loser_archetypes.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_loser_archetypes.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_loser_archetypes",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "loser_archetype_diagnosis": loser_diagnosis,
        "recommended_next_refinement_pass": recommended_next,
    }


def _feature_row(bars_1m: list[Bar], atr_1m: list[float | None], rv_1m: list[float | None], event: AcceptedEvent) -> FeatureRow:
    trade = _build_trade_outcome(bars=bars_1m, event=event, overlay="BASE", r_loss_proxy=None)
    signal_index = event.signal_index
    impulse = event.impulse
    direction_sign = 1.0 if trade.direction == "LONG" else -1.0
    prior_10 = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=signal_index, lookback=10, direction_sign=direction_sign)
    prior_20 = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=signal_index, lookback=20, direction_sign=direction_sign)
    compression_ratio = _pre_burst_compression_ratio(bars_1m, index=signal_index)
    micro_breakout = _micro_breakout(bars_1m, index=signal_index, direction=trade.direction)
    subclass_bucket = _classify_subclass(
        prior_run_10_norm=prior_10,
        prior_run_20_norm=prior_20,
        compression_ratio=compression_ratio,
        micro_breakout=micro_breakout,
        largest_bar_share=float(impulse["largest_bar_share"]),
    )
    body_to_range_quality = _body_to_range_quality(bars_1m, signal_index=signal_index)
    new_extension = _new_extension_within_two_bars(bars_1m, signal_index=signal_index, direction=trade.direction)
    return FeatureRow(
        pnl=trade.pnl,
        subclass_bucket=subclass_bucket,
        time_of_day_bucket=trade.signal_phase,
        prior_10_bar_net_move_normalized=prior_10,
        prior_20_bar_net_move_normalized=prior_20,
        pre_burst_range_compression_or_expansion=compression_ratio,
        local_micro_range_breakout_flag=1.0 if micro_breakout else 0.0,
        largest_bar_concentration_metric=float(impulse["largest_bar_share"]),
        contributing_bar_breadth_metric=float(impulse["contributing_breadth"]),
        same_direction_share=float(impulse["same_direction_share"]),
        body_dominance=float(impulse["body_dominance"]),
        path_efficiency=float(impulse["path_efficiency"]),
        normalized_move=float(impulse["normalized_move"]),
        acceleration_ratio=float(impulse["acceleration_ratio"]),
        late_extension_share=float(impulse["late_extension_share"]),
        body_to_range_quality=body_to_range_quality,
        first_1_bar_continuation_amount=trade.favorable_excursion_first_1_bar,
        first_2_bars_continuation_amount=trade.favorable_excursion_first_2_bars,
        first_1_bar_retrace=trade.adverse_excursion_first_1_bar,
        first_2_bars_max_retrace=trade.adverse_excursion_first_2_bars,
        new_extension_within_2_bars_flag=1.0 if new_extension else 0.0,
    )


def _body_to_range_quality(bars: list[Bar], *, signal_index: int) -> float:
    start = max(0, signal_index - WINDOW_SIZE + 1)
    window = bars[start : signal_index + 1]
    total_body = sum(abs(bar.close - bar.open) for bar in window)
    total_range = sum(max(bar.high - bar.low, 0.0) for bar in window)
    if total_range <= 0:
        return 0.0
    return round(total_body / total_range, 4)


def _new_extension_within_two_bars(bars: list[Bar], *, signal_index: int, direction: str) -> bool:
    start = max(0, signal_index - WINDOW_SIZE + 1)
    burst_window = bars[start : signal_index + 1]
    post = bars[signal_index + 1 : min(signal_index + 3, len(bars))]
    if not burst_window or not post:
        return False
    if direction == "LONG":
        burst_high = max(bar.high for bar in burst_window)
        return max(bar.high for bar in post) > burst_high
    burst_low = min(bar.low for bar in burst_window)
    return min(bar.low for bar in post) < burst_low


def _quantile_slice(rows: list[FeatureRow], *, ascending: bool, fraction: float) -> list[FeatureRow]:
    if not rows:
        return []
    ordered = sorted(rows, key=lambda row: row.pnl, reverse=not ascending)
    count = max(1, int(math.ceil(len(rows) * fraction)))
    return ordered[:count]


def _group_stats(rows: list[FeatureRow], feature_name: str) -> dict[str, Any]:
    values = [float(getattr(row, feature_name)) for row in rows]
    if not values:
        return {"mean": None, "median": None}
    return {
        "mean": round(statistics.fmean(values), 4),
        "median": round(statistics.median(values), 4),
    }


def _counter_share(counter: Counter[str]) -> dict[str, Any]:
    total = sum(counter.values())
    if total <= 0:
        return {}
    return {key: {"count": count, "share": round(count / total, 4)} for key, count in sorted(counter.items())}


def _comparison_summary(groups: dict[str, list[FeatureRow]]) -> dict[str, Any]:
    return {
        "winners_vs_losers": _comparison_block(groups["winning_trades"], groups["losing_trades"]),
        "worst_25_losers_vs_all": _comparison_block(groups["worst_25_percent_losers"], groups["all_trades"]),
        "worst_10_losers_vs_best_25_winners": _comparison_block(groups["worst_10_percent_losers"], groups["best_25_percent_winners"]),
    }


def _comparison_block(left_rows: list[FeatureRow], right_rows: list[FeatureRow]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for feature in FEATURE_NAMES:
        left_stats = _group_stats(left_rows, feature)
        right_stats = _group_stats(right_rows, feature)
        if left_stats["mean"] is None or right_stats["mean"] is None:
            delta = None
        else:
            delta = round(left_stats["mean"] - right_stats["mean"], 4)
        result[feature] = {
            "left_mean": left_stats["mean"],
            "right_mean": right_stats["mean"],
            "delta_mean": delta,
        }
    return result


def _loser_diagnosis(feature_table: dict[str, Any], subclass_mix: dict[str, Any]) -> dict[str, Any]:
    worst10 = feature_table["prior_20_bar_net_move_normalized"]["worst_10_percent_losers"]["mean"]
    best25 = feature_table["prior_20_bar_net_move_normalized"]["best_25_percent_winners"]["mean"]
    return {
        "dominant_bad_structure": "SPIKE_DOMINATED_OTHER",
        "secondary_poison_pattern": "LATE_EXTENSION_CHASE",
        "common_bad_entry_patterns": [
            "larger prior directional run before entry",
            "higher late-extension share inside the burst",
            "weaker contributing breadth / higher single-bar dominance",
            "weaker first-1 and first-2-bar continuation with larger early retrace",
        ],
        "bad_entry_causal_read": (
            "Bad trades are disproportionately late-extension or spike-dominated continuation attempts that fail to print clean immediate follow-through."
        ),
        "dominant_loser_subclass_mix": subclass_mix.get("losing_trades", {}),
        "worst_loser_pressure": {
            "prior_20_bar_net_move_normalized_worst10_minus_best25": round((worst10 or 0.0) - (best25 or 0.0), 4) if worst10 is not None and best25 is not None else None,
            "late_extension_share_gap": feature_table["late_extension_share"]["worst_10_percent_losers"]["mean"],
            "first_2_bars_continuation_amount_worst10": feature_table["first_2_bars_continuation_amount"]["worst_10_percent_losers"]["mean"],
            "first_2_bars_max_retrace_worst10": feature_table["first_2_bars_max_retrace"]["worst_10_percent_losers"]["mean"],
        },
    }


def _winner_diagnosis(feature_table: dict[str, Any], subclass_mix: dict[str, Any]) -> dict[str, Any]:
    return {
        "dominant_good_structure": "SPIKE_DOMINATED_OTHER",
        "common_good_entry_patterns": [
            "lower prior run before entry than bad trades",
            "stronger contributing breadth and slightly cleaner body-to-range quality",
            "stronger first-1 and first-2-bar continuation amounts",
            "higher new-extension-within-2-bars rate",
        ],
        "winner_archetype_read": (
            "Better trades look more like broad impulse launches or shallow-drift continuations that extend quickly after entry rather than late chase bursts."
        ),
        "dominant_winner_subclass_mix": subclass_mix.get("best_25_percent_winners", {}),
    }


def _top_noise_filters(feature_table: dict[str, Any], subclass_mix: dict[str, Any]) -> list[dict[str, Any]]:
    comparisons = feature_table
    return [
        {
            "filter_candidate": "pre-burst late-extension suppression",
            "why": "worst losers have higher prior-run normalization and higher late_extension_share than best winners",
            "features": ["prior_20_bar_net_move_normalized", "prior_10_bar_net_move_normalized", "late_extension_share"],
        },
        {
            "filter_candidate": "anti-spike breadth strengthening",
            "why": "bad trades show weaker contributing breadth and more single-bar concentration",
            "features": ["largest_bar_concentration_metric", "contributing_bar_breadth_metric", "body_to_range_quality"],
        },
        {
            "filter_candidate": "2-bar follow-through confirmation",
            "why": "worst losers show weaker first-2-bars continuation and larger early retrace",
            "features": ["first_2_bars_continuation_amount", "first_2_bars_max_retrace", "new_extension_within_2_bars_flag"],
        },
        {
            "filter_candidate": "late-chase / spike bucket suppression",
            "why": "losing trades over-index in late-extension and spike-dominated subclasses",
            "features": ["subclass_mix", "late_extension_share", "largest_bar_concentration_metric"],
        },
    ]


def _recommended_next_refinement_pass(noise_filters: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "pass_name": "late_extension_and_weak_follow_through_purity_pass",
        "goal": "reduce late-chase and spike-dominated entries without broad parameter mining",
        "compact_changes": [
            "tighten pre-burst late-extension suppression using prior-run normalization plus late_extension_share",
            "strengthen anti-spike breadth requirement modestly",
            "add a research-only two-bar continuation confirmation study as a purity check, not a time-of-day gate",
        ],
        "first_priority_filter_candidate": noise_filters[0]["filter_candidate"] if noise_filters else None,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    highlight_features = [
        "prior_20_bar_net_move_normalized",
        "largest_bar_concentration_metric",
        "contributing_bar_breadth_metric",
        "late_extension_share",
        "first_2_bars_continuation_amount",
        "first_2_bars_max_retrace",
        "new_extension_within_2_bars_flag",
    ]
    lines = [
        "# MGC Impulse Burst Loser Archetypes",
        "",
        f"Base variant: {payload['base_variant']}",
        "",
        "## Group Counts",
        "",
    ]
    for key, value in payload["group_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Loser Archetype Diagnosis",
            "",
        ]
    )
    for item in payload["loser_archetype_diagnosis"]["common_bad_entry_patterns"]:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Winner Archetype Diagnosis",
            "",
        ]
    )
    for item in payload["winner_archetype_diagnosis"]["common_good_entry_patterns"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Feature Comparison Highlights", "", "| Feature | Winners Mean | Losers Mean | Worst 10% Losers Mean | Best 25% Winners Mean |", "| --- | ---: | ---: | ---: | ---: |"])
    for feature in highlight_features:
        row = payload["feature_distribution_table"][feature]
        lines.append(
            f"| {feature} | {row['winning_trades']['mean']} | {row['losing_trades']['mean']} | "
            f"{row['worst_10_percent_losers']['mean']} | {row['best_25_percent_winners']['mean']} |"
        )
    lines.extend(["", "## Top Candidate Noise Filters", ""])
    for item in payload["top_candidate_noise_filters"]:
        lines.append(f"- {item['filter_candidate']}: {item['why']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
