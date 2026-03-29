"""Research-only second-pass refinement for MGC impulse burst continuation."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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
    _load_bars,
    _load_promoted_mgc_family_comparison,
    _max_drawdown,
    _passes_context_filter,
    _profit_factor,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
    _survives_without_top,
    _top_trade_share,
)
from .session_phase_labels import label_session_phase


MAX_HOLD_BARS = 8
WINDOW_SIZE = 8


@dataclass(frozen=True)
class RefinementSpec:
    variant_name: str
    require_volume_expansion: bool = False
    normalized_move_threshold: float = 1.35
    same_direction_share_min: float = 0.70
    body_dominance_min: float = 0.65
    path_efficiency_min: float = 0.45
    largest_bar_share_max: float | None = None
    min_material_bars: int | None = None
    material_bar_share_min: float = 0.12
    acceleration_ratio_min: float | None = None
    late_extension_share_min: float | None = None
    fast_failure_exit: bool = False


@dataclass(frozen=True)
class EntryTrade:
    entry_ts: str
    exit_ts: str
    direction: str
    entry_px: float
    exit_px: float
    pnl: float
    hold_bars: int
    signal_phase: str
    signal_bar_ts: str
    captured_move: float
    false_start: bool
    burst_size_points: float
    largest_bar_share: float
    contributing_bar_breadth: float
    adverse_excursion_first_1_bar: float
    adverse_excursion_first_2_bars: float
    favorable_excursion_first_1_bar: float
    favorable_excursion_first_2_bars: float
    pnl_if_exit_after_2_bars: float
    first_1_bar_failure: bool
    first_2_bars_failure: bool
    time_of_day_phase: str


BASE_SPEC = RefinementSpec(variant_name="base_direct_impulse_continuation_w8_control")
REFINEMENT_SPECS: tuple[RefinementSpec, ...] = (
    BASE_SPEC,
    RefinementSpec(
        variant_name="anti_spike_breadth_filter",
        largest_bar_share_max=0.52,
        min_material_bars=3,
        material_bar_share_min=0.12,
    ),
    RefinementSpec(
        variant_name="stronger_agreement_quality",
        same_direction_share_min=0.75,
        body_dominance_min=0.72,
        path_efficiency_min=0.52,
    ),
    RefinementSpec(
        variant_name="force_quality_variant",
        normalized_move_threshold=1.55,
        acceleration_ratio_min=1.05,
        late_extension_share_min=0.30,
    ),
    RefinementSpec(
        variant_name="fast_failure_exit_variant",
        fast_failure_exit=True,
    ),
    RefinementSpec(
        variant_name="breadth_plus_agreement_combo",
        largest_bar_share_max=0.55,
        min_material_bars=3,
        material_bar_share_min=0.12,
        same_direction_share_min=0.75,
        body_dominance_min=0.70,
        path_efficiency_min=0.50,
    ),
    RefinementSpec(
        variant_name="breadth_force_fast_failure_combo",
        largest_bar_share_max=0.55,
        min_material_bars=3,
        material_bar_share_min=0.12,
        normalized_move_threshold=1.50,
        acceleration_ratio_min=1.05,
        late_extension_share_min=0.28,
        fast_failure_exit=True,
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_continuation_second_pass(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-continuation-second-pass")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_continuation_second_pass(*, symbol: str) -> dict[str, Any]:
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
    promoted_comparison = _load_promoted_mgc_family_comparison(overlap_start=overlap_start, overlap_end=overlap_end)

    results = [
        _evaluate_refinement_candidate(
            bars_1m=one_minute,
            bars_5m=five_minute,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            vol_baseline_1m=vol_baseline_1m,
            atr_5m=atr_5m,
            context_lookup=context_lookup,
            spec=spec,
        )
        for spec in REFINEMENT_SPECS
    ]
    base_result = next(row for row in results if row["variant_name"] == BASE_SPEC.variant_name)
    enriched_results = [_attach_interpretation(row=row, base_result=base_result) for row in results]
    best_result = _pick_best_result(enriched_results)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "pass_name": "second_pass_refinement",
        "base_variant": BASE_SPEC.variant_name,
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "refinement_metrics_used": _refinement_metric_notes(),
        "refinement_variants_tested": [asdict(spec) for spec in REFINEMENT_SPECS],
        "results": enriched_results,
        "best_result": best_result,
        "comparison_vs_promoted_mgc_families": promoted_comparison,
        "family_second_pass_conclusion": _second_pass_conclusion(best_result=best_result, base_result=base_result),
    }
    json_path = OUTPUT_DIR / "mgc_impulse_burst_continuation_second_pass.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_continuation_second_pass.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_continuation_second_pass",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_result": {
            "variant_name": best_result["variant_name"] if best_result else None,
            "decision_bucket": best_result["decision_bucket"] if best_result else None,
        },
    }


def _evaluate_refinement_candidate(
    *,
    bars_1m: list[Bar],
    bars_5m: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    atr_5m: list[float | None],
    context_lookup: list[int | None],
    spec: RefinementSpec,
) -> dict[str, Any]:
    raw_events = 0
    post_filter_events = 0
    trades: list[EntryTrade] = []
    index = max(WINDOW_SIZE, 20)
    next_available_index = index
    while index < len(bars_1m) - MAX_HOLD_BARS - 1:
        if index < next_available_index:
            index += 1
            continue
        impulse = _detect_refined_impulse_event(
            bars=bars_1m,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            vol_baseline_1m=vol_baseline_1m,
            index=index,
            spec=spec,
        )
        if impulse is None:
            index += 1
            continue
        raw_events += 1
        if not _passes_context_filter(
            bars_5m=bars_5m,
            atr_5m=atr_5m,
            context_lookup=context_lookup,
            one_minute_index=index,
            direction=impulse["direction"],
            threshold=0.35,
            body_share_min=2.0 / 3.0,
        ):
            index += WINDOW_SIZE
            continue
        trade = _build_direct_trade_with_refined_exit(bars=bars_1m, signal_index=index, impulse=impulse, fast_failure_exit=spec.fast_failure_exit)
        if trade is None:
            index += WINDOW_SIZE
            continue
        post_filter_events += 1
        trades.append(trade)
        exit_index = _find_bar_index_by_timestamp(bars_1m, trade.exit_ts)
        next_available_index = max(index + WINDOW_SIZE, exit_index + 1)
        index += WINDOW_SIZE

    metrics = _trade_metrics_second_pass(trades)
    overlay = _fast_rejection_overlay(trades=trades)
    time_distribution = Counter(trade.time_of_day_phase for trade in trades)
    return {
        "variant_name": spec.variant_name,
        "bars_scanned": len(bars_1m),
        "raw_impulse_events": raw_events,
        "post_filter_events": post_filter_events,
        "trades": len(trades),
        "realized_pnl": metrics["realized_pnl"],
        "avg_trade": metrics["avg_trade"],
        "median_trade": metrics["median_trade"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown": metrics["max_drawdown"],
        "false_start_rate": metrics["false_start_rate"],
        "top_1_trade_contribution": metrics["top_1_trade_contribution"],
        "top_3_trade_contribution": metrics["top_3_trade_contribution"],
        "survives_without_top_1": metrics["survives_without_top_1"],
        "survives_without_top_3": metrics["survives_without_top_3"],
        "largest_bar_concentration_metric": metrics["largest_bar_concentration_metric"],
        "contributing_bar_breadth_metric": metrics["contributing_bar_breadth_metric"],
        "avg_adverse_excursion_first_1_bar": metrics["avg_adverse_excursion_first_1_bar"],
        "avg_adverse_excursion_first_2_bars": metrics["avg_adverse_excursion_first_2_bars"],
        "first_1_bar_failure_rate": metrics["first_1_bar_failure_rate"],
        "first_2_bars_failure_rate": metrics["first_2_bars_failure_rate"],
        "average_move_captured_after_signal": metrics["average_move_captured_after_signal"],
        "time_of_day_summary": {
            "distribution": dict(sorted(time_distribution.items())),
            "dominant_phase": _dominant_bucket_from_counter(time_distribution)[0],
            "dominant_phase_share": _dominant_bucket_from_counter(time_distribution)[1],
        },
        "fast_rejection_overlay": overlay,
        "fast_rejection_practical_rule": _fast_rejection_practical(overlay=overlay, baseline_metrics=metrics),
        "decision_bucket": _decision_bucket_second_pass(raw_events=raw_events, post_filter_events=post_filter_events, trade_count=len(trades), metrics=metrics),
    }


def _detect_refined_impulse_event(
    *,
    bars: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    index: int,
    spec: RefinementSpec,
) -> dict[str, Any] | None:
    start = index - WINDOW_SIZE + 1
    if start < 0:
        return None
    window = bars[start : index + 1]
    if len(window) != WINDOW_SIZE:
        return None
    atr_value = atr_1m[index]
    rv_value = rv_1m[index]
    vol_baseline = vol_baseline_1m[index]
    if atr_value is None or rv_value is None or vol_baseline is None or vol_baseline <= 0:
        return None
    signed_net = window[-1].close - window[0].open
    if signed_net == 0:
        return None
    direction = "LONG" if signed_net > 0 else "SHORT"
    direction_sign = 1.0 if direction == "LONG" else -1.0
    bodies = [bar.close - bar.open for bar in window]
    aligned_bodies = [max(direction_sign * body, 0.0) for body in bodies]
    aligned_sum = sum(aligned_bodies)
    total_body = sum(abs(body) for body in bodies)
    if aligned_sum <= 0 or total_body <= 0:
        return None
    same_direction_share = sum(1 for body in bodies if direction_sign * body > 0) / WINDOW_SIZE
    body_dominance = aligned_sum / total_body
    path_efficiency = abs(signed_net) / total_body
    scale = max(atr_value * math.sqrt(WINDOW_SIZE), rv_value * math.sqrt(WINDOW_SIZE), 0.1)
    normalized_move = abs(signed_net) / scale
    window_volume = statistics.fmean(bar.volume for bar in window)
    volume_ratio = window_volume / vol_baseline if vol_baseline > 0 else 0.0
    largest_bar_share = max(aligned_bodies) / aligned_sum
    materially_contributing_bars = sum(1 for value in aligned_bodies if value / aligned_sum >= spec.material_bar_share_min)
    contributing_breadth = materially_contributing_bars / WINDOW_SIZE
    first_half = sum(aligned_bodies[: WINDOW_SIZE // 2])
    second_half = sum(aligned_bodies[WINDOW_SIZE // 2 :])
    acceleration_ratio = second_half / max(first_half, 1e-6)
    late_extension = direction_sign * (window[-1].close - window[WINDOW_SIZE // 2].close)
    late_extension_share = late_extension / abs(signed_net) if signed_net else 0.0
    if same_direction_share < spec.same_direction_share_min:
        return None
    if body_dominance < spec.body_dominance_min:
        return None
    if path_efficiency < spec.path_efficiency_min:
        return None
    if normalized_move < spec.normalized_move_threshold:
        return None
    if spec.require_volume_expansion and volume_ratio < 1.15:
        return None
    if spec.largest_bar_share_max is not None and largest_bar_share > spec.largest_bar_share_max:
        return None
    if spec.min_material_bars is not None and materially_contributing_bars < spec.min_material_bars:
        return None
    if spec.acceleration_ratio_min is not None and acceleration_ratio < spec.acceleration_ratio_min:
        return None
    if spec.late_extension_share_min is not None and late_extension_share < spec.late_extension_share_min:
        return None
    return {
        "signal_ts": window[-1].timestamp.isoformat(),
        "signal_phase": label_session_phase(window[-1].timestamp),
        "direction": direction,
        "burst_size_points": abs(signed_net),
        "normalized_move": normalized_move,
        "same_direction_share": same_direction_share,
        "body_dominance": body_dominance,
        "path_efficiency": path_efficiency,
        "volume_ratio": volume_ratio,
        "largest_bar_share": round(largest_bar_share, 4),
        "materially_contributing_bars": materially_contributing_bars,
        "contributing_breadth": round(contributing_breadth, 4),
        "acceleration_ratio": round(acceleration_ratio, 4),
        "late_extension_share": round(late_extension_share, 4),
    }


def _build_direct_trade_with_refined_exit(
    *,
    bars: list[Bar],
    signal_index: int,
    impulse: dict[str, Any],
    fast_failure_exit: bool,
) -> EntryTrade | None:
    entry_index = signal_index + 1
    if entry_index >= len(bars):
        return None
    direction = str(impulse["direction"])
    direction_sign = 1.0 if direction == "LONG" else -1.0
    entry_bar = bars[entry_index]
    default_exit = min(entry_index + MAX_HOLD_BARS - 1, len(bars) - 1)
    chosen_exit = default_exit
    adverse_streak = 0
    for index in range(entry_index, default_exit + 1):
        body = direction_sign * (bars[index].close - bars[index].open)
        if body < 0:
            adverse_streak += 1
        else:
            adverse_streak = 0
        if adverse_streak >= 2:
            chosen_exit = index
            break

    first_window_end = min(default_exit, entry_index)
    second_window_end = min(default_exit, entry_index + 1)
    if direction == "LONG":
        adverse_1 = max(entry_bar.open - bar.low for bar in bars[entry_index : first_window_end + 1])
        adverse_2 = max(entry_bar.open - bar.low for bar in bars[entry_index : second_window_end + 1])
        favorable_1 = max(bar.high - entry_bar.open for bar in bars[entry_index : first_window_end + 1])
        favorable_2 = max(bar.high - entry_bar.open for bar in bars[entry_index : second_window_end + 1])
        close_1 = bars[first_window_end].close
        close_2 = bars[second_window_end].close
    else:
        adverse_1 = max(bar.high - entry_bar.open for bar in bars[entry_index : first_window_end + 1])
        adverse_2 = max(bar.high - entry_bar.open for bar in bars[entry_index : second_window_end + 1])
        favorable_1 = max(entry_bar.open - bar.low for bar in bars[entry_index : first_window_end + 1])
        favorable_2 = max(entry_bar.open - bar.low for bar in bars[entry_index : second_window_end + 1])
        close_1 = bars[first_window_end].close
        close_2 = bars[second_window_end].close
    burst_size = float(impulse["burst_size_points"])
    first_1_bar_failure = favorable_1 < (0.10 * burst_size) and direction_sign * (close_1 - entry_bar.open) <= 0
    first_2_bars_failure = favorable_2 < (0.20 * burst_size) and direction_sign * (close_2 - entry_bar.open) <= 0
    if fast_failure_exit and second_window_end >= entry_index + 1 and first_2_bars_failure:
        chosen_exit = min(chosen_exit, second_window_end)

    exit_bar = bars[chosen_exit]
    captured_move = direction_sign * (exit_bar.close - entry_bar.open)
    pnl = captured_move * POINT_VALUE
    return EntryTrade(
        entry_ts=entry_bar.timestamp.isoformat(),
        exit_ts=exit_bar.timestamp.isoformat(),
        direction=direction,
        entry_px=entry_bar.open,
        exit_px=exit_bar.close,
        pnl=round(pnl, 4),
        hold_bars=chosen_exit - entry_index + 1,
        signal_phase=str(impulse["signal_phase"]),
        signal_bar_ts=str(impulse["signal_ts"]),
        captured_move=round(captured_move, 4),
        false_start=first_2_bars_failure and pnl <= 0,
        burst_size_points=round(burst_size, 4),
        largest_bar_share=float(impulse["largest_bar_share"]),
        contributing_bar_breadth=float(impulse["contributing_breadth"]),
        adverse_excursion_first_1_bar=round(adverse_1, 4),
        adverse_excursion_first_2_bars=round(adverse_2, 4),
        favorable_excursion_first_1_bar=round(favorable_1, 4),
        favorable_excursion_first_2_bars=round(favorable_2, 4),
        pnl_if_exit_after_2_bars=round(direction_sign * (close_2 - entry_bar.open) * POINT_VALUE, 4),
        first_1_bar_failure=first_1_bar_failure,
        first_2_bars_failure=first_2_bars_failure,
        time_of_day_phase=str(impulse["signal_phase"]),
    )


def _find_bar_index_by_timestamp(bars: list[Bar], timestamp_text: str) -> int:
    target = datetime.fromisoformat(timestamp_text)
    for index, bar in enumerate(bars):
        if bar.timestamp == target:
            return index
    return len(bars) - 1


def _trade_metrics_second_pass(trades: list[EntryTrade]) -> dict[str, Any]:
    pnls = [trade.pnl for trade in trades]
    return {
        "realized_pnl": round(sum(pnls), 4),
        "avg_trade": round(statistics.fmean(pnls), 4) if pnls else None,
        "median_trade": round(statistics.median(pnls), 4) if pnls else None,
        "profit_factor": _profit_factor(pnls),
        "max_drawdown": _max_drawdown(pnls),
        "false_start_rate": round(sum(1 for trade in trades if trade.false_start) / len(trades), 4) if trades else None,
        "top_1_trade_contribution": _top_trade_share(pnls, top_n=1),
        "top_3_trade_contribution": _top_trade_share(pnls, top_n=3),
        "survives_without_top_1": _survives_without_top(pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        "largest_bar_concentration_metric": round(statistics.fmean([trade.largest_bar_share for trade in trades]), 4) if trades else None,
        "contributing_bar_breadth_metric": round(statistics.fmean([trade.contributing_bar_breadth for trade in trades]), 4) if trades else None,
        "avg_adverse_excursion_first_1_bar": round(statistics.fmean([trade.adverse_excursion_first_1_bar for trade in trades]), 4) if trades else None,
        "avg_adverse_excursion_first_2_bars": round(statistics.fmean([trade.adverse_excursion_first_2_bars for trade in trades]), 4) if trades else None,
        "first_1_bar_failure_rate": round(sum(1 for trade in trades if trade.first_1_bar_failure) / len(trades), 4) if trades else None,
        "first_2_bars_failure_rate": round(sum(1 for trade in trades if trade.first_2_bars_failure) / len(trades), 4) if trades else None,
        "average_move_captured_after_signal": round(statistics.fmean([trade.captured_move for trade in trades]), 4) if trades else None,
    }


def _fast_rejection_overlay(*, trades: list[EntryTrade]) -> dict[str, Any]:
    overlay_pnls = []
    for trade in trades:
        if trade.first_2_bars_failure:
            overlay_pnls.append(trade.pnl_if_exit_after_2_bars)
        else:
            overlay_pnls.append(trade.pnl)
    return {
        "realized_pnl": round(sum(overlay_pnls), 4),
        "avg_trade": round(statistics.fmean(overlay_pnls), 4) if overlay_pnls else None,
        "median_trade": round(statistics.median(overlay_pnls), 4) if overlay_pnls else None,
        "profit_factor": _profit_factor(overlay_pnls),
        "max_drawdown": _max_drawdown(overlay_pnls),
        "top_1_trade_contribution": _top_trade_share(overlay_pnls, top_n=1),
        "top_3_trade_contribution": _top_trade_share(overlay_pnls, top_n=3),
        "survives_without_top_1": _survives_without_top(overlay_pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(overlay_pnls, top_n=3),
    }


def _fast_rejection_practical(*, overlay: dict[str, Any], baseline_metrics: dict[str, Any]) -> bool:
    improved_pnl = (overlay["realized_pnl"] or 0.0) > (baseline_metrics["realized_pnl"] or 0.0)
    improved_median = (overlay["median_trade"] or -999999.0) > (baseline_metrics["median_trade"] or -999999.0)
    improved_pf = (overlay["profit_factor"] or 0.0) > (baseline_metrics["profit_factor"] or 0.0)
    lower_dd = (overlay["max_drawdown"] or 999999.0) < (baseline_metrics["max_drawdown"] or 999999.0)
    return sum(int(flag) for flag in (improved_pnl, improved_median, improved_pf, lower_dd)) >= 3


def _decision_bucket_second_pass(*, raw_events: int, post_filter_events: int, trade_count: int, metrics: dict[str, Any]) -> str:
    if raw_events == 0 or trade_count == 0:
        return "NOT_WORTH_CONTINUING"
    if trade_count < 25:
        return "TOO_THIN"
    if (
        (metrics["realized_pnl"] or 0.0) > 0
        and (metrics["profit_factor"] or 0.0) >= 1.15
        and (metrics["median_trade"] or -999999.0) > -5.0
        and (metrics["top_3_trade_contribution"] is None or metrics["top_3_trade_contribution"] < 160.0)
    ):
        return "PROMISING_NEW_FAMILY"
    if (metrics["profit_factor"] or 0.0) >= 1.0 and (metrics["realized_pnl"] or 0.0) > 0:
        return "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT"
    if (metrics["false_start_rate"] or 0.0) >= 0.5 or (metrics["profit_factor"] or 0.0) < 1.0:
        return "TOO_NOISY"
    return "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT"


def _attach_interpretation(*, row: dict[str, Any], base_result: dict[str, Any]) -> dict[str, Any]:
    trade_density_preserved = (row["trades"] or 0) >= int((base_result["trades"] or 0) * 0.5)
    improved_median_trade = (row["median_trade"] or -999999.0) > (base_result["median_trade"] or -999999.0)
    reduced_fragility = (
        (row["top_3_trade_contribution"] or 999999.0) < (base_result["top_3_trade_contribution"] or 999999.0)
        and (row["max_drawdown"] or 999999.0) <= (base_result["max_drawdown"] or 999999.0)
    )
    reduced_spike_contamination = (
        (row["largest_bar_concentration_metric"] or 999999.0) < (base_result["largest_bar_concentration_metric"] or 999999.0)
        and (row["contributing_bar_breadth_metric"] or -999999.0) >= (base_result["contributing_bar_breadth_metric"] or -999999.0)
    )
    fast_rejection_helped = row["fast_rejection_practical_rule"]
    cleaner_or_sparser = (
        "cleaner"
        if reduced_fragility or improved_median_trade
        else "just_sparser"
        if (row["trades"] or 0) < (base_result["trades"] or 0)
        else "not_cleaner"
    )
    enriched = dict(row)
    enriched["interpretation"] = {
        "did_it_preserve_meaningful_trade_density": trade_density_preserved,
        "did_it_improve_median_trade": improved_median_trade,
        "did_it_reduce_fragility_concentration": reduced_fragility,
        "did_it_reduce_one_bar_spike_contamination": reduced_spike_contamination,
        "did_fast_rejection_materially_improve_outcomes": fast_rejection_helped,
        "is_the_family_getting_cleaner_or_just_sparser": cleaner_or_sparser,
    }
    return enriched


def _pick_best_result(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not results:
        return None
    ranked = sorted(
        results,
        key=lambda row: (
            {"PROMISING_NEW_FAMILY": 4, "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT": 3, "TOO_NOISY": 2, "TOO_THIN": 1, "NOT_WORTH_CONTINUING": 0}[row["decision_bucket"]],
            float(row["realized_pnl"] or 0.0),
            float(row["profit_factor"] or 0.0),
            -float(row["max_drawdown"] or 0.0),
        ),
        reverse=True,
    )
    return ranked[0]


def _refinement_metric_notes() -> dict[str, Any]:
    return {
        "base_branch": "direct_impulse_continuation_w8",
        "breadth_metrics": {
            "largest_bar_concentration_metric": "average largest aligned-body share of total aligned burst body per accepted event",
            "contributing_bar_breadth_metric": "average share of burst bars contributing at least 12% of aligned burst body",
        },
        "agreement_metrics": {
            "same_direction_share": "share of burst bars whose bodies align with burst direction",
            "body_dominance": "aligned body sum divided by total absolute body sum",
            "path_efficiency": "net burst move divided by total absolute body path",
        },
        "force_metrics": {
            "normalized_move": "abs(net move) / max(ATR14*sqrt(8), realized-vol20*sqrt(8))",
            "acceleration_ratio": "aligned body sum in back half / aligned body sum in front half",
            "late_extension_share": "last-half directional extension as a share of total burst move",
        },
        "fast_rejection_rule": {
            "first_1_bar_failure": "first post-entry bar fails to produce at least 10% of burst-size continuation and closes flat/against entry direction",
            "first_2_bars_failure": "first two post-entry bars fail to produce at least 20% of burst-size continuation and remain flat/against entry direction",
            "overlay_exit": "if first_2_bars_failure, exit at the second bar close in the overlay analysis",
        },
    }


def _second_pass_conclusion(*, best_result: dict[str, Any] | None, base_result: dict[str, Any]) -> str:
    if best_result is None:
        return "No second-pass refinement result was produced."
    if best_result["variant_name"] == base_result["variant_name"]:
        return "The family remains structurally real, but none of the compact refinements materially cleaned it up versus the base direct w8 control."
    if best_result["decision_bucket"] == "PROMISING_NEW_FAMILY":
        return "The second pass materially cleaned the family enough to justify a later robustness lane."
    return "The second pass improved parts of the family, but it is still better viewed as structurally real and refinement-needing rather than robustness-ready."


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Burst Continuation Second Pass",
        "",
        f"Base branch: {payload['base_variant']}",
        f"Conclusion: {payload['family_second_pass_conclusion']}",
        "",
    ]
    for row in payload["results"]:
        lines.append(f"## {row['variant_name']}")
        lines.append(
            f"- bucket={row['decision_bucket']}, trades={row['trades']}, pnl={row['realized_pnl']}, avg={row['avg_trade']}, "
            f"median={row['median_trade']}, pf={row['profit_factor']}, dd={row['max_drawdown']}"
        )
        lines.append(
            f"- breadth: largest_bar={row['largest_bar_concentration_metric']}, contributing_breadth={row['contributing_bar_breadth_metric']}"
        )
        lines.append(
            f"- fast rejection: first1_fail={row['first_1_bar_failure_rate']}, first2_fail={row['first_2_bars_failure_rate']}, "
            f"overlay_pnl={row['fast_rejection_overlay']['realized_pnl']}, practical={row['fast_rejection_practical_rule']}"
        )
        lines.append(
            f"- interpretation: density={row['interpretation']['did_it_preserve_meaningful_trade_density']}, "
            f"median_up={row['interpretation']['did_it_improve_median_trade']}, "
            f"fragility_down={row['interpretation']['did_it_reduce_fragility_concentration']}, "
            f"spike_down={row['interpretation']['did_it_reduce_one_bar_spike_contamination']}, "
            f"cleaner_or_sparser={row['interpretation']['is_the_family_getting_cleaner_or_just_sparser']}"
        )
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
