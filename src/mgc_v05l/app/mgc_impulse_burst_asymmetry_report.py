"""Asymmetry-focused research report for the MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime
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
    _max_drawdown,
    _profit_factor,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
    _survives_without_top,
    _top_trade_share,
)
from .mgc_impulse_burst_continuation_second_pass import (
    REFINEMENT_SPECS,
    RefinementSpec,
    _detect_refined_impulse_event,
    _find_bar_index_by_timestamp,
    _load_bars,
    _passes_context_filter,
)


BASE_CANDIDATE_NAMES = (
    "breadth_plus_agreement_combo",
    "anti_spike_breadth_filter",
    "base_direct_impulse_continuation_w8_control",
)
WINDOW_SIZE = 8
MAX_HOLD_BARS = 8
EXTENDED_HOLD_BARS = 10


@dataclass(frozen=True)
class AcceptedEvent:
    signal_index: int
    impulse: dict[str, Any]
    base_exit_ts: str


@dataclass(frozen=True)
class TradeOutcome:
    entry_ts: str
    exit_ts: str
    direction: str
    pnl: float
    hold_bars: int
    burst_size_points: float
    signal_phase: str
    adverse_excursion_before_exit: float
    favorable_excursion_before_exit: float
    adverse_excursion_first_1_bar: float
    adverse_excursion_first_2_bars: float
    favorable_excursion_first_1_bar: float
    favorable_excursion_first_2_bars: float
    first_1_bar_failure: bool
    first_2_bars_failure: bool
    exit_reason: str


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_asymmetry_report(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-asymmetry-report")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_asymmetry_report(*, symbol: str) -> dict[str, Any]:
    specs = [spec for spec in REFINEMENT_SPECS if spec.variant_name in BASE_CANDIDATE_NAMES]
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

    candidate_rows = []
    for spec in specs:
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
        base_trades = [_build_trade_outcome(bars=one_minute, event=event, overlay="BASE", r_loss_proxy=None) for event in events]
        r_loss_proxy = _median_abs_loser([trade.pnl for trade in base_trades])
        overlays = {
            "base_exit_path": _overlay_summary(base_trades, r_loss_proxy=r_loss_proxy, overlay_name="base_exit_path"),
            "slightly_tighter_early_failure_rejection": _overlay_summary(
                [_build_trade_outcome(bars=one_minute, event=event, overlay="TIGHT_EARLY", r_loss_proxy=r_loss_proxy) for event in events],
                r_loss_proxy=r_loss_proxy,
                overlay_name="slightly_tighter_early_failure_rejection",
            ),
            "simple_hard_loss_cap_proxy": _overlay_summary(
                [_build_trade_outcome(bars=one_minute, event=event, overlay="HARD_CAP", r_loss_proxy=r_loss_proxy) for event in events],
                r_loss_proxy=r_loss_proxy,
                overlay_name="simple_hard_loss_cap_proxy",
            ),
            "slightly_looser_winner_retention": _overlay_summary(
                [_build_trade_outcome(bars=one_minute, event=event, overlay="LOOSER_WINNER", r_loss_proxy=r_loss_proxy) for event in events],
                r_loss_proxy=r_loss_proxy,
                overlay_name="slightly_looser_winner_retention",
            ),
        }
        base_summary = overlays["base_exit_path"]
        candidate_rows.append(
            {
                "variant_name": spec.variant_name,
                "bars_scanned": len(one_minute),
                "raw_impulse_events": len(events),
                "r_equivalent_proxy": {
                    "definition": "candidate base-exit median absolute loser",
                    "value": r_loss_proxy,
                },
                "base_metrics": base_summary,
                "overlay_scenarios": {
                    name: _overlay_delta(summary=summary, base_summary=base_summary)
                    for name, summary in overlays.items()
                },
                "candidate_verdict": _candidate_verdict(base_summary),
                "candidate_interpretation": _candidate_interpretation(base_summary=base_summary, overlays=overlays),
            }
        )

    best_candidate = _pick_best_candidate(candidate_rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "evaluation_mode": "positive_skew_asymmetric_payoff",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "candidates_tested": list(BASE_CANDIDATE_NAMES),
        "exact_asymmetry_metrics_used": _metric_notes(),
        "candidate_results": candidate_rows,
        "best_asymmetry_candidate": {
            "variant_name": best_candidate["variant_name"] if best_candidate else None,
            "candidate_verdict": best_candidate["candidate_verdict"] if best_candidate else None,
        },
        "family_asymmetry_conclusion": _family_conclusion(best_candidate),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_burst_asymmetry_report.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_asymmetry_report.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_asymmetry_report",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_asymmetry_candidate": payload["best_asymmetry_candidate"],
        "family_asymmetry_conclusion": payload["family_asymmetry_conclusion"],
    }


def _collect_candidate_events(
    *,
    bars_1m: list[Bar],
    bars_5m: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    atr_5m: list[float | None],
    context_lookup: list[int | None],
    spec: RefinementSpec,
) -> list[AcceptedEvent]:
    events: list[AcceptedEvent] = []
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
        base_trade = _build_trade_outcome(bars=bars_1m, event=AcceptedEvent(signal_index=index, impulse=impulse, base_exit_ts=""), overlay="BASE", r_loss_proxy=None)
        events.append(
            AcceptedEvent(
                signal_index=index,
                impulse=impulse,
                base_exit_ts=base_trade.exit_ts,
            )
        )
        exit_index = _find_bar_index_by_timestamp(bars_1m, base_trade.exit_ts)
        next_available_index = max(index + WINDOW_SIZE, exit_index + 1)
        index += WINDOW_SIZE
    return events


def _build_trade_outcome(*, bars: list[Bar], event: AcceptedEvent, overlay: str, r_loss_proxy: float | None) -> TradeOutcome:
    signal_index = event.signal_index
    impulse = event.impulse
    entry_index = signal_index + 1
    direction = str(impulse["direction"])
    direction_sign = 1.0 if direction == "LONG" else -1.0
    entry_bar = bars[entry_index]

    max_hold_bars = EXTENDED_HOLD_BARS if overlay == "LOOSER_WINNER" else MAX_HOLD_BARS
    adverse_limit = 3 if overlay == "LOOSER_WINNER" else 2
    default_exit = min(entry_index + max_hold_bars - 1, len(bars) - 1)
    chosen_exit = default_exit
    exit_reason = "max_hold"
    adverse_streak = 0
    hard_cap_price = None
    if overlay == "HARD_CAP" and r_loss_proxy and r_loss_proxy > 0:
        hard_cap_points = (1.25 * r_loss_proxy) / POINT_VALUE
        hard_cap_price = entry_bar.open - hard_cap_points if direction == "LONG" else entry_bar.open + hard_cap_points

    for index in range(entry_index, default_exit + 1):
        bar = bars[index]
        if hard_cap_price is not None:
            if direction == "LONG" and bar.low <= hard_cap_price:
                chosen_exit = index
                exit_reason = "hard_loss_cap_proxy"
                break
            if direction == "SHORT" and bar.high >= hard_cap_price:
                chosen_exit = index
                exit_reason = "hard_loss_cap_proxy"
                break
        body = direction_sign * (bar.close - bar.open)
        if body < 0:
            adverse_streak += 1
        else:
            adverse_streak = 0
        if adverse_streak >= adverse_limit:
            chosen_exit = index
            exit_reason = "adverse_body_streak"
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

    if overlay == "TIGHT_EARLY":
        if first_1_bar_failure:
            chosen_exit = min(chosen_exit, first_window_end)
            exit_reason = "tight_first_bar_rejection"
        elif second_window_end >= entry_index + 1 and first_2_bars_failure:
            chosen_exit = min(chosen_exit, second_window_end)
            exit_reason = "tight_second_bar_rejection"

    exit_bar = bars[chosen_exit]
    if hard_cap_price is not None and exit_reason == "hard_loss_cap_proxy":
        exit_price = hard_cap_price
    else:
        exit_price = exit_bar.close

    window = bars[entry_index : chosen_exit + 1]
    if direction == "LONG":
        adverse_before_exit = max(entry_bar.open - bar.low for bar in window)
        favorable_before_exit = max(bar.high - entry_bar.open for bar in window)
    else:
        adverse_before_exit = max(bar.high - entry_bar.open for bar in window)
        favorable_before_exit = max(entry_bar.open - bar.low for bar in window)

    pnl = direction_sign * (exit_price - entry_bar.open) * POINT_VALUE
    return TradeOutcome(
        entry_ts=entry_bar.timestamp.isoformat(),
        exit_ts=exit_bar.timestamp.isoformat(),
        direction=direction,
        pnl=round(pnl, 4),
        hold_bars=chosen_exit - entry_index + 1,
        burst_size_points=round(burst_size, 4),
        signal_phase=str(impulse["signal_phase"]),
        adverse_excursion_before_exit=round(adverse_before_exit * POINT_VALUE, 4),
        favorable_excursion_before_exit=round(favorable_before_exit * POINT_VALUE, 4),
        adverse_excursion_first_1_bar=round(adverse_1 * POINT_VALUE, 4),
        adverse_excursion_first_2_bars=round(adverse_2 * POINT_VALUE, 4),
        favorable_excursion_first_1_bar=round(favorable_1 * POINT_VALUE, 4),
        favorable_excursion_first_2_bars=round(favorable_2 * POINT_VALUE, 4),
        first_1_bar_failure=first_1_bar_failure,
        first_2_bars_failure=first_2_bars_failure,
        exit_reason=exit_reason,
    )


def _overlay_summary(trades: list[TradeOutcome], *, r_loss_proxy: float | None, overlay_name: str) -> dict[str, Any]:
    pnls = [trade.pnl for trade in trades]
    losers = [-trade.pnl for trade in trades if trade.pnl < 0]
    winners = [trade.pnl for trade in trades if trade.pnl > 0]
    large_threshold = (r_loss_proxy or 0.0) * 3.0
    very_large_threshold = (r_loss_proxy or 0.0) * 5.0
    loser_trades = [trade for trade in trades if trade.pnl < 0]
    phase_counts = {}
    for trade in trades:
        phase_counts[trade.signal_phase] = phase_counts.get(trade.signal_phase, 0) + 1
    dominant_phase, dominant_share = _dominant_bucket_from_counter(phase_counts)
    return {
        "overlay_name": overlay_name,
        "core_performance": {
            "trades": len(trades),
            "realized_pnl": round(sum(pnls), 4),
            "avg_trade": _mean_or_none(pnls),
            "median_trade": _median_or_none(pnls),
            "profit_factor": _profit_factor(pnls),
            "max_drawdown": _max_drawdown(pnls),
            "win_rate": round(len(winners) / len(trades), 4) if trades else None,
        },
        "loss_containment": {
            "average_loser": _mean_or_none(losers),
            "median_loser": _median_or_none(losers),
            "p90_loser": _percentile_or_none(losers, 0.90),
            "p95_loser": _percentile_or_none(losers, 0.95),
            "worst_loser": round(max(losers), 4) if losers else None,
            "average_adverse_excursion_before_exit": _mean_or_none([trade.adverse_excursion_before_exit for trade in loser_trades]),
            "median_adverse_excursion_before_exit": _median_or_none([trade.adverse_excursion_before_exit for trade in loser_trades]),
        },
        "win_asymmetry": {
            "average_winner": _mean_or_none(winners),
            "median_winner": _median_or_none(winners),
            "p90_winner": _percentile_or_none(winners, 0.90),
            "p95_winner": _percentile_or_none(winners, 0.95),
            "best_winner": round(max(winners), 4) if winners else None,
            "average_favorable_excursion": _mean_or_none([trade.favorable_excursion_before_exit for trade in trades if trade.pnl > 0]),
            "median_favorable_excursion": _median_or_none([trade.favorable_excursion_before_exit for trade in trades if trade.pnl > 0]),
        },
        "payoff_structure": {
            "r_equivalent_proxy": r_loss_proxy,
            "r_equivalent_proxy_definition": "candidate base-exit median absolute loser",
            "avg_winner_over_avg_loser": _safe_ratio(_mean_or_none(winners), _mean_or_none(losers)),
            "median_winner_over_median_loser": _safe_ratio(_median_or_none(winners), _median_or_none(losers)),
            "percent_trades_above_1r": _share_above_threshold(pnls, r_loss_proxy, 1.0),
            "percent_trades_above_2r": _share_above_threshold(pnls, r_loss_proxy, 2.0),
            "percent_trades_above_3r": _share_above_threshold(pnls, r_loss_proxy, 3.0),
            "percent_trades_above_5r": _share_above_threshold(pnls, r_loss_proxy, 5.0),
        },
        "right_tail_repeatability": {
            "top_1_contribution": _top_trade_share(pnls, top_n=1),
            "top_3_contribution": _top_trade_share(pnls, top_n=3),
            "large_winner_threshold": round(large_threshold, 4) if r_loss_proxy else None,
            "very_large_winner_threshold": round(very_large_threshold, 4) if r_loss_proxy else None,
            "large_winner_count": _count_above_threshold(pnls, large_threshold) if r_loss_proxy else 0,
            "very_large_winner_count": _count_above_threshold(pnls, very_large_threshold) if r_loss_proxy else 0,
            "outsized_winners_recur": _right_tail_repeatability(pnls, r_loss_proxy),
            "survives_without_top_1": _survives_without_top(pnls, top_n=1),
            "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        },
        "rejection_stop_behavior": {
            "avg_hold_bars_loser": _mean_or_none([trade.hold_bars for trade in loser_trades]),
            "loser_failure_within_first_1_bar_share": round(sum(1 for trade in loser_trades if trade.first_1_bar_failure) / len(loser_trades), 4) if loser_trades else None,
            "loser_failure_within_first_2_bars_share": round(sum(1 for trade in loser_trades if (not trade.first_1_bar_failure) and trade.first_2_bars_failure) / len(loser_trades), 4) if loser_trades else None,
            "loser_failure_later_share": round(sum(1 for trade in loser_trades if (not trade.first_1_bar_failure) and (not trade.first_2_bars_failure)) / len(loser_trades), 4) if loser_trades else None,
            "losses_naturally_small_enough": _losses_naturally_small_enough(r_loss_proxy=r_loss_proxy, losers=losers),
            "dominant_time_of_day_phase": dominant_phase,
            "dominant_time_of_day_phase_share": dominant_share,
        },
    }


def _overlay_delta(*, summary: dict[str, Any], base_summary: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(summary)
    base_core = base_summary["core_performance"]
    core = summary["core_performance"]
    tail = summary["right_tail_repeatability"]
    base_tail = base_summary["right_tail_repeatability"]
    enriched["delta_vs_base"] = {
        "realized_pnl_delta": round((core["realized_pnl"] or 0.0) - (base_core["realized_pnl"] or 0.0), 4),
        "median_trade_delta": round((core["median_trade"] or 0.0) - (base_core["median_trade"] or 0.0), 4),
        "profit_factor_delta": round((core["profit_factor"] or 0.0) - (base_core["profit_factor"] or 0.0), 4),
        "max_drawdown_delta": round((core["max_drawdown"] or 0.0) - (base_core["max_drawdown"] or 0.0), 4),
        "top_1_contribution_delta": round((tail["top_1_contribution"] or 0.0) - (base_tail["top_1_contribution"] or 0.0), 4),
        "top_3_contribution_delta": round((tail["top_3_contribution"] or 0.0) - (base_tail["top_3_contribution"] or 0.0), 4),
    }
    return enriched


def _candidate_verdict(base_summary: dict[str, Any]) -> str:
    core = base_summary["core_performance"]
    payoff = base_summary["payoff_structure"]
    tail = base_summary["right_tail_repeatability"]
    loss = base_summary["loss_containment"]
    if (core["profit_factor"] or 0.0) < 1.0 or (core["realized_pnl"] or 0.0) <= 0:
        return "NOT_WORTH_CONTINUING"
    if (payoff["avg_winner_over_avg_loser"] or 0.0) >= 2.5 and (tail["large_winner_count"] or 0) >= 6 and tail["outsized_winners_recur"] == "RECURRING" and (loss["p95_loser"] or 999999.0) <= 2.5 * (payoff["r_equivalent_proxy"] or 1.0):
        return "VALID_POSITIVE_SKEW_ENGINE"
    if (tail["large_winner_count"] or 0) < 4 or tail["outsized_winners_recur"] == "ISOLATED" or (tail["top_3_contribution"] or 999999.0) > 170.0:
        return "STRUCTURALLY_REAL_BUT_RIGHT_TAIL_TOO_ISOLATED"
    return "STRUCTURALLY_REAL_BUT_LOSS_CONTROL_TOO_WEAK"


def _candidate_interpretation(*, base_summary: dict[str, Any], overlays: dict[str, dict[str, Any]]) -> dict[str, Any]:
    tight = overlays["slightly_tighter_early_failure_rejection"]
    cap = overlays["simple_hard_loss_cap_proxy"]
    loose = overlays["slightly_looser_winner_retention"]
    tail = base_summary["right_tail_repeatability"]
    return {
        "is_viable_as_low_win_rate_high_payoff_ratio_system": base_summary["core_performance"]["profit_factor"] is not None and base_summary["core_performance"]["profit_factor"] > 1.0 and (base_summary["payoff_structure"]["avg_winner_over_avg_loser"] or 0.0) > 2.0,
        "did_tighter_early_rejection_improve_asymmetry_without_killing_tail": (
            (tight["core_performance"]["profit_factor"] or 0.0) >= (base_summary["core_performance"]["profit_factor"] or 0.0)
            and (tight["core_performance"]["max_drawdown"] or 999999.0) <= (base_summary["core_performance"]["max_drawdown"] or 999999.0)
            and (tight["right_tail_repeatability"]["large_winner_count"] or 0) >= max((tail["large_winner_count"] or 0) - 1, 0)
        ),
        "did_hard_loss_cap_proxy_help": (
            (cap["core_performance"]["max_drawdown"] or 999999.0) < (base_summary["core_performance"]["max_drawdown"] or 999999.0)
            and (cap["core_performance"]["profit_factor"] or 0.0) >= (base_summary["core_performance"]["profit_factor"] or 0.0)
        ),
        "did_looser_winner_retention_help": (
            (loose["core_performance"]["realized_pnl"] or 0.0) > (base_summary["core_performance"]["realized_pnl"] or 0.0)
            and (loose["right_tail_repeatability"]["large_winner_count"] or 0) >= (tail["large_winner_count"] or 0)
        ),
        "main_problem_event_purity_or_trade_management": (
            "event_purity"
            if (
                (tight["core_performance"]["realized_pnl"] or 0.0) <= (base_summary["core_performance"]["realized_pnl"] or 0.0)
                and (cap["core_performance"]["realized_pnl"] or 0.0) <= (base_summary["core_performance"]["realized_pnl"] or 0.0)
            )
            else "trade_management"
        ),
    }


def _pick_best_candidate(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda row: (
            {
                "VALID_POSITIVE_SKEW_ENGINE": 3,
                "STRUCTURALLY_REAL_BUT_LOSS_CONTROL_TOO_WEAK": 2,
                "STRUCTURALLY_REAL_BUT_RIGHT_TAIL_TOO_ISOLATED": 1,
                "NOT_WORTH_CONTINUING": 0,
            }[row["candidate_verdict"]],
            float(row["base_metrics"]["core_performance"]["realized_pnl"] or 0.0),
            float(row["base_metrics"]["payoff_structure"]["avg_winner_over_avg_loser"] or 0.0),
            -float(row["base_metrics"]["right_tail_repeatability"]["top_3_contribution"] or 0.0),
        ),
        reverse=True,
    )
    return ranked[0]


def _family_conclusion(best_candidate: dict[str, Any] | None) -> str:
    if best_candidate is None:
        return "NOT_WORTH_CONTINUING"
    return best_candidate["candidate_verdict"]


def _metric_notes() -> dict[str, Any]:
    return {
        "core_performance": ["trades", "realized_pnl", "avg_trade", "median_trade", "profit_factor", "max_drawdown", "win_rate"],
        "loss_containment": [
            "average_loser",
            "median_loser",
            "p90_loser",
            "p95_loser",
            "worst_loser",
            "average_adverse_excursion_before_exit",
            "median_adverse_excursion_before_exit",
        ],
        "win_asymmetry": [
            "average_winner",
            "median_winner",
            "p90_winner",
            "p95_winner",
            "best_winner",
            "average_favorable_excursion",
            "median_favorable_excursion",
        ],
        "payoff_structure": {
            "r_equivalent_proxy": "candidate base-exit median absolute loser",
            "thresholds": ["1R", "2R", "3R", "5R"],
        },
        "overlay_scenarios": {
            "base_exit_path": "8-bar max hold or two consecutive adverse bodies",
            "slightly_tighter_early_failure_rejection": "exit after first bar on first_1_bar_failure, else after second bar on first_2_bars_failure",
            "simple_hard_loss_cap_proxy": "bar-level proxy cap at 1.25 * candidate base-exit median absolute loser",
            "slightly_looser_winner_retention": "10-bar max hold and three consecutive adverse bodies",
        },
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Burst Asymmetry Report",
        "",
        f"Family conclusion: {payload['family_asymmetry_conclusion']}",
        "",
    ]
    for candidate in payload["candidate_results"]:
        core = candidate["base_metrics"]["core_performance"]
        payoff = candidate["base_metrics"]["payoff_structure"]
        tail = candidate["base_metrics"]["right_tail_repeatability"]
        lines.append(f"## {candidate['variant_name']}")
        lines.append(
            f"- verdict={candidate['candidate_verdict']}, trades={core['trades']}, pnl={core['realized_pnl']}, avg={core['avg_trade']}, "
            f"median={core['median_trade']}, pf={core['profit_factor']}, dd={core['max_drawdown']}, win_rate={core['win_rate']}"
        )
        lines.append(
            f"- asymmetry: avg_win/avg_loss={payoff['avg_winner_over_avg_loser']}, median_win/median_loss={payoff['median_winner_over_median_loser']}, "
            f"%>3R={payoff['percent_trades_above_3r']}, %>5R={payoff['percent_trades_above_5r']}"
        )
        lines.append(
            f"- tail: top1={tail['top_1_contribution']}, top3={tail['top_3_contribution']}, large={tail['large_winner_count']}, very_large={tail['very_large_winner_count']}, recur={tail['outsized_winners_recur']}"
        )
        lines.append("")
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


def _median_abs_loser(pnls: list[float]) -> float | None:
    losers = sorted(-pnl for pnl in pnls if pnl < 0)
    if not losers:
        return None
    return round(statistics.median(losers), 4)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, 4)


def _share_above_threshold(pnls: list[float], r_proxy: float | None, multiple: float) -> float | None:
    if not pnls or not r_proxy or r_proxy <= 0:
        return None
    threshold = r_proxy * multiple
    return round(sum(1 for pnl in pnls if pnl >= threshold) / len(pnls), 4)


def _count_above_threshold(pnls: list[float], threshold: float) -> int:
    return sum(1 for pnl in pnls if pnl >= threshold)


def _right_tail_repeatability(pnls: list[float], r_proxy: float | None) -> str:
    if not pnls or not r_proxy or r_proxy <= 0:
        return "INSUFFICIENT"
    count_3r = _count_above_threshold(pnls, 3.0 * r_proxy)
    count_5r = _count_above_threshold(pnls, 5.0 * r_proxy)
    if count_3r >= 5 and count_5r >= 2:
        return "RECURRING"
    if count_3r >= 2:
        return "LIMITED"
    return "ISOLATED"


def _losses_naturally_small_enough(*, r_loss_proxy: float | None, losers: list[float]) -> bool | None:
    if not losers or not r_loss_proxy or r_loss_proxy <= 0:
        return None
    p95 = _percentile_or_none(losers, 0.95)
    worst = max(losers)
    return bool((p95 or 999999.0) <= 2.5 * r_loss_proxy and worst <= 4.0 * r_loss_proxy)


if __name__ == "__main__":
    raise SystemExit(main())
