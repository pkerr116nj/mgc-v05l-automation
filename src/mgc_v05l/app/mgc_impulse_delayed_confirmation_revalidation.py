"""Causal delayed-confirmation revalidation for MGC impulse burst continuation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .mgc_impulse_burst_asymmetry_report import (
    AcceptedEvent,
    MAX_HOLD_BARS,
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
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_loser_archetypes import _new_extension_within_two_bars
from .mgc_impulse_burst_third_pass_narrowing import _event_snapshot
from .mgc_impulse_confirmation_validation import VALIDATION_VARIANTS
from .mgc_impulse_same_bar_causalization import BENCHMARK_VARIANT
from .mgc_impulse_spike_confirmation_pass import _evaluate_confirmation_variant, _metrics
from .mgc_impulse_spike_subtypes import _confirmation_bar_count_first_3


BASE_VARIANT = "breadth_plus_agreement_combo"
SAME_BAR_ARTIFACT = OUTPUT_DIR / "mgc_impulse_same_bar_causalization.json"


@dataclass(frozen=True)
class DelayedConfirmationVariant:
    variant_name: str
    description: str
    entry_mode: str


@dataclass(frozen=True)
class ConfirmationResolution:
    confirmation_index: int
    observed_confirmation_bar_count: int
    new_extension_within_2_bars: bool
    resolution_timing: str


DELAYED_CONFIRMATION_VARIANTS: tuple[DelayedConfirmationVariant, ...] = (
    DelayedConfirmationVariant(
        variant_name="enter_next_open_after_minimal_confirmation",
        description="Enter at the next 1m open immediately after the minimal confirmation rule becomes causally satisfied.",
        entry_mode="NEXT_OPEN_AFTER_CONFIRM",
    ),
    DelayedConfirmationVariant(
        variant_name="enter_confirmation_bar_close",
        description="Enter at the close of the bar that makes the minimal confirmation rule causally true.",
        entry_mode="CONFIRMATION_BAR_CLOSE",
    ),
    DelayedConfirmationVariant(
        variant_name="enter_next_open_after_full_confirmation_window",
        description="Wait for the full 3-bar confirmation window to close, then enter at the next 1m open if the rule is satisfied.",
        entry_mode="NEXT_OPEN_AFTER_FULL_WINDOW",
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_delayed_confirmation_revalidation(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-delayed-confirmation-revalidation")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_delayed_confirmation_revalidation(*, symbol: str) -> dict[str, Any]:
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
    raw_control_trades = [_build_trade_outcome(bars=one_minute, event=event, overlay="BASE", r_loss_proxy=None) for event in base_events]
    raw_control_metrics = _metrics(raw_control_trades)

    benchmark_variant = next(variant for variant in VALIDATION_VARIANTS if variant.variant_name == BENCHMARK_VARIANT)
    benchmark_row = _evaluate_confirmation_variant(bars=one_minute, snapshots=raw_snapshots, variant=benchmark_variant)
    same_bar_reference = _load_same_bar_reference(fallback_metrics=raw_control_metrics)

    variant_rows = [
        _evaluate_delayed_variant(
            bars=one_minute,
            events=base_events,
            variant=variant,
            raw_control_metrics=raw_control_metrics,
            benchmark_metrics=benchmark_row["metrics"],
            same_bar_reference=same_bar_reference,
        )
        for variant in DELAYED_CONFIRMATION_VARIANTS
    ]
    best_variant = _pick_best_variant(variant_rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "base_candidate_definition": {
            "population": "raw breadth_plus_agreement_combo",
            "delayed_confirmation_rule": {
                "new_extension_within_2_bars": True,
                "confirmation_bar_count_first_3_min": 2,
            },
        },
        "delayed_entry_variants_tested": [variant.variant_name for variant in DELAYED_CONFIRMATION_VARIANTS],
        "raw_control_metrics": raw_control_metrics,
        "non_causal_benchmark": {
            "variant_name": BENCHMARK_VARIANT,
            "metrics": benchmark_row["metrics"],
        },
        "failed_same_bar_causalization_reference": same_bar_reference,
        "variant_results": variant_rows,
        "best_delayed_confirmation_variant": {
            "variant_name": best_variant["variant_name"],
            "decision_bucket": best_variant["decision_bucket"],
        },
        "causal_revalidation_conclusion": _causal_revalidation_conclusion(
            best_variant=best_variant,
            benchmark_metrics=benchmark_row["metrics"],
        ),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_delayed_confirmation_revalidation.json"
    md_path = OUTPUT_DIR / "mgc_impulse_delayed_confirmation_revalidation.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_delayed_confirmation_revalidation",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_delayed_confirmation_variant": payload["best_delayed_confirmation_variant"],
        "causal_revalidation_conclusion": payload["causal_revalidation_conclusion"],
    }


def _evaluate_delayed_variant(
    *,
    bars: list[Bar],
    events: list[AcceptedEvent],
    variant: DelayedConfirmationVariant,
    raw_control_metrics: dict[str, Any],
    benchmark_metrics: dict[str, Any],
    same_bar_reference: dict[str, Any],
) -> dict[str, Any]:
    trades: list[TradeOutcome] = []
    active_until_index = -1
    confirmations_observed = 0
    for event in events:
        if event.signal_index <= active_until_index:
            continue
        resolution = _resolve_confirmation_resolution(
            bars=bars,
            signal_index=event.signal_index,
            direction=str(event.impulse["direction"]),
            entry_mode=variant.entry_mode,
        )
        if resolution is None:
            continue
        confirmations_observed += 1
        trade = _build_delayed_trade_outcome(
            bars=bars,
            event=event,
            resolution=resolution,
            entry_mode=variant.entry_mode,
        )
        if trade is None:
            continue
        trades.append(trade)
        active_until_index = _find_bar_index_by_iso_timestamp(bars, trade.exit_ts)

    metrics = _metrics(trades)
    return {
        "variant_name": variant.variant_name,
        "description": variant.description,
        "entry_mode": variant.entry_mode,
        "confirmation_rule": {
            "new_extension_within_2_bars": True,
            "confirmation_bar_count_first_3_min": 2,
        },
        "metrics": metrics,
        "causal_execution_stats": {
            "confirmed_setups_observed_before_entry": confirmations_observed,
        },
        "comparison_vs_raw_control": _metric_delta(metrics, raw_control_metrics),
        "comparison_vs_non_causal_benchmark": _metric_delta(metrics, benchmark_metrics),
        "comparison_vs_failed_same_bar_causalization": _metric_delta(metrics, same_bar_reference["metrics"]),
        "decision_bucket": _decision_bucket(metrics=metrics, raw_control_metrics=raw_control_metrics, benchmark_metrics=benchmark_metrics),
    }


def _resolve_confirmation_resolution(
    *,
    bars: list[Bar],
    signal_index: int,
    direction: str,
    entry_mode: str,
) -> ConfirmationResolution | None:
    if entry_mode == "NEXT_OPEN_AFTER_FULL_WINDOW":
        third_index = signal_index + 3
        if third_index >= len(bars):
            return None
        if not _new_extension_within_two_bars(bars, signal_index=signal_index, direction=direction):
            return None
        confirmation_count = int(_confirmation_bar_count_first_3(bars, signal_index=signal_index, direction=direction))
        if confirmation_count < 2:
            return None
        return ConfirmationResolution(
            confirmation_index=third_index,
            observed_confirmation_bar_count=confirmation_count,
            new_extension_within_2_bars=True,
            resolution_timing="after_bar_3_close",
        )

    second_index = signal_index + 2
    if second_index >= len(bars):
        return None
    if _new_extension_within_two_bars(bars, signal_index=signal_index, direction=direction):
        first_two_count = _confirmation_count(bars=bars, signal_index=signal_index, direction=direction, bars_after_signal=2)
        if first_two_count >= 2:
            return ConfirmationResolution(
                confirmation_index=second_index,
                observed_confirmation_bar_count=first_two_count,
                new_extension_within_2_bars=True,
                resolution_timing="after_bar_2_close",
            )

    third_index = signal_index + 3
    if third_index >= len(bars):
        return None
    if not _new_extension_within_two_bars(bars, signal_index=signal_index, direction=direction):
        return None
    confirmation_count = int(_confirmation_bar_count_first_3(bars, signal_index=signal_index, direction=direction))
    if confirmation_count < 2:
        return None
    return ConfirmationResolution(
        confirmation_index=third_index,
        observed_confirmation_bar_count=confirmation_count,
        new_extension_within_2_bars=True,
        resolution_timing="after_bar_3_close",
    )


def _confirmation_count(*, bars: list[Bar], signal_index: int, direction: str, bars_after_signal: int) -> int:
    start = signal_index + 1
    stop = min(signal_index + 1 + bars_after_signal, len(bars))
    post = bars[start:stop]
    if not post:
        return 0
    direction_sign = 1.0 if direction == "LONG" else -1.0
    return sum(1 for bar in post if direction_sign * (bar.close - bar.open) > 0)


def _build_delayed_trade_outcome(
    *,
    bars: list[Bar],
    event: AcceptedEvent,
    resolution: ConfirmationResolution,
    entry_mode: str,
) -> TradeOutcome | None:
    direction = str(event.impulse["direction"])
    direction_sign = 1.0 if direction == "LONG" else -1.0

    if entry_mode == "CONFIRMATION_BAR_CLOSE":
        entry_index = resolution.confirmation_index
        monitor_start_index = entry_index + 1
        if monitor_start_index >= len(bars):
            return None
        entry_price = bars[entry_index].close
        entry_ts = bars[entry_index].timestamp.isoformat()
    else:
        entry_index = resolution.confirmation_index + 1
        monitor_start_index = entry_index
        if entry_index >= len(bars):
            return None
        entry_price = bars[entry_index].open
        entry_ts = bars[entry_index].timestamp.isoformat()

    default_exit = min(monitor_start_index + MAX_HOLD_BARS - 1, len(bars) - 1)
    chosen_exit = default_exit
    exit_reason = "max_hold"
    adverse_streak = 0
    for index in range(monitor_start_index, default_exit + 1):
        bar = bars[index]
        body = direction_sign * (bar.close - bar.open)
        if body < 0:
            adverse_streak += 1
        else:
            adverse_streak = 0
        if adverse_streak >= 2:
            chosen_exit = index
            exit_reason = "adverse_body_streak"
            break

    first_window_end = min(default_exit, monitor_start_index)
    second_window_end = min(default_exit, monitor_start_index + 1)
    if direction == "LONG":
        adverse_1 = max(entry_price - bar.low for bar in bars[monitor_start_index : first_window_end + 1])
        adverse_2 = max(entry_price - bar.low for bar in bars[monitor_start_index : second_window_end + 1])
        favorable_1 = max(bar.high - entry_price for bar in bars[monitor_start_index : first_window_end + 1])
        favorable_2 = max(bar.high - entry_price for bar in bars[monitor_start_index : second_window_end + 1])
        close_1 = bars[first_window_end].close
        close_2 = bars[second_window_end].close
        adverse_before_exit = max(entry_price - bar.low for bar in bars[monitor_start_index : chosen_exit + 1])
        favorable_before_exit = max(bar.high - entry_price for bar in bars[monitor_start_index : chosen_exit + 1])
    else:
        adverse_1 = max(bar.high - entry_price for bar in bars[monitor_start_index : first_window_end + 1])
        adverse_2 = max(bar.high - entry_price for bar in bars[monitor_start_index : second_window_end + 1])
        favorable_1 = max(entry_price - bar.low for bar in bars[monitor_start_index : first_window_end + 1])
        favorable_2 = max(entry_price - bar.low for bar in bars[monitor_start_index : second_window_end + 1])
        close_1 = bars[first_window_end].close
        close_2 = bars[second_window_end].close
        adverse_before_exit = max(bar.high - entry_price for bar in bars[monitor_start_index : chosen_exit + 1])
        favorable_before_exit = max(entry_price - bar.low for bar in bars[monitor_start_index : chosen_exit + 1])

    burst_size = float(event.impulse["burst_size_points"])
    first_1_bar_failure = favorable_1 < (0.10 * burst_size) and direction_sign * (close_1 - entry_price) <= 0
    first_2_bars_failure = favorable_2 < (0.20 * burst_size) and direction_sign * (close_2 - entry_price) <= 0

    exit_bar = bars[chosen_exit]
    exit_price = exit_bar.close
    pnl = direction_sign * (exit_price - entry_price) * POINT_VALUE
    return TradeOutcome(
        entry_ts=entry_ts,
        exit_ts=exit_bar.timestamp.isoformat(),
        direction=direction,
        pnl=round(pnl, 4),
        hold_bars=chosen_exit - monitor_start_index + 1,
        burst_size_points=round(burst_size, 4),
        signal_phase=str(event.impulse["signal_phase"]),
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


def _load_same_bar_reference(*, fallback_metrics: dict[str, Any]) -> dict[str, Any]:
    if not SAME_BAR_ARTIFACT.exists():
        return {
            "variant_name": "raw_breadth_plus_agreement_control",
            "decision_bucket": "CAUSAL_PROXY_TOO_WEAK_USE_DELAYED_ENTRY",
            "metrics": fallback_metrics,
            "artifact_source": None,
        }
    payload = json.loads(SAME_BAR_ARTIFACT.read_text(encoding="utf-8"))
    best_variant_name = payload.get("best_causal_proxy_variant", {}).get("variant_name")
    rows = {row["variant_name"]: row for row in payload.get("variant_results", [])}
    best_row = rows.get(best_variant_name)
    if best_row is None:
        return {
            "variant_name": "raw_breadth_plus_agreement_control",
            "decision_bucket": payload.get("best_causal_proxy_variant", {}).get("decision_bucket"),
            "metrics": fallback_metrics,
            "artifact_source": str(SAME_BAR_ARTIFACT),
        }
    return {
        "variant_name": best_row["variant_name"],
        "decision_bucket": best_row["decision_bucket"],
        "metrics": best_row["metrics"],
        "artifact_source": str(SAME_BAR_ARTIFACT),
    }


def _metric_delta(metrics: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_delta": _delta(metrics.get("trades"), baseline.get("trades")),
        "realized_pnl_delta": _delta(metrics.get("realized_pnl"), baseline.get("realized_pnl")),
        "avg_trade_delta": _delta(metrics.get("avg_trade"), baseline.get("avg_trade")),
        "median_trade_delta": _delta(metrics.get("median_trade"), baseline.get("median_trade")),
        "profit_factor_delta": _delta(metrics.get("profit_factor"), baseline.get("profit_factor")),
        "max_drawdown_delta": _delta(metrics.get("max_drawdown"), baseline.get("max_drawdown")),
        "win_rate_delta": _delta(metrics.get("win_rate"), baseline.get("win_rate")),
        "average_loser_delta": _delta(metrics.get("average_loser"), baseline.get("average_loser")),
        "top_1_contribution_delta": _delta(metrics.get("top_1_contribution"), baseline.get("top_1_contribution")),
        "top_3_contribution_delta": _delta(metrics.get("top_3_contribution"), baseline.get("top_3_contribution")),
    }


def _delta(left: float | int | None, right: float | int | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 4)


def _decision_bucket(*, metrics: dict[str, Any], raw_control_metrics: dict[str, Any], benchmark_metrics: dict[str, Any]) -> str:
    benchmark_realized = float(benchmark_metrics.get("realized_pnl") or 0.0)
    benchmark_trades = float(benchmark_metrics.get("trades") or 0.0)
    realized_share = float(metrics.get("realized_pnl") or 0.0) / benchmark_realized if benchmark_realized > 0 else 0.0
    trade_share = float(metrics.get("trades") or 0.0) / benchmark_trades if benchmark_trades > 0 else 0.0

    if (
        (metrics.get("profit_factor") or 0.0) >= 2.0
        and (metrics.get("median_trade") or -999999.0) > 0
        and (metrics.get("top_3_contribution") or 999999.0) <= 90.0
        and bool(metrics.get("survives_without_top_3"))
        and realized_share >= 0.5
        and trade_share >= 0.5
    ):
        return "DELAYED_CONFIRMATION_RECOVERS_ENOUGH"
    if (
        (metrics.get("profit_factor") or 0.0) >= 1.3
        and (metrics.get("median_trade") or -999999.0) > 0
        and (metrics.get("realized_pnl") or 0.0) > (raw_control_metrics.get("realized_pnl") or 0.0)
        and bool(metrics.get("survives_without_top_1"))
    ):
        return "DELAYED_CONFIRMATION_PROMISING_BUT_WEAKER"
    if (metrics.get("profit_factor") or 0.0) >= 1.0 and (metrics.get("realized_pnl") or 0.0) > 0:
        return "DELAYED_CONFIRMATION_TOO_WEAK"
    return "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION"


def _pick_best_variant(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            {
                "DELAYED_CONFIRMATION_RECOVERS_ENOUGH": 3,
                "DELAYED_CONFIRMATION_PROMISING_BUT_WEAKER": 2,
                "DELAYED_CONFIRMATION_TOO_WEAK": 1,
                "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _causal_revalidation_conclusion(*, best_variant: dict[str, Any], benchmark_metrics: dict[str, Any]) -> dict[str, Any]:
    benchmark_realized = float(benchmark_metrics.get("realized_pnl") or 0.0)
    best_realized = float(best_variant["metrics"].get("realized_pnl") or 0.0)
    realized_share = round(best_realized / benchmark_realized, 4) if benchmark_realized > 0 else None
    return {
        "best_variant": best_variant["variant_name"],
        "decision_bucket": best_variant["decision_bucket"],
        "delayed_confirmation_preserves_enough_to_remain_credible": best_variant["decision_bucket"] in {
            "DELAYED_CONFIRMATION_RECOVERS_ENOUGH",
            "DELAYED_CONFIRMATION_PROMISING_BUT_WEAKER",
        },
        "benchmark_realized_pnl_recovery_share": realized_share,
        "good_enough_for_narrow_executable_paper_design_pass": best_variant["decision_bucket"] == "DELAYED_CONFIRMATION_RECOVERS_ENOUGH",
        "remaining_runtime_blocker_besides_multi_timeframe_plumbing": (
            None
            if best_variant["decision_bucket"] == "DELAYED_CONFIRMATION_RECOVERS_ENOUGH"
            else "Delayed confirmation preserved too little of the non-causal winner to justify executable paper design yet."
        ),
    }


def _find_bar_index_by_iso_timestamp(bars: list[Bar], timestamp_iso: str) -> int:
    for index, bar in enumerate(bars):
        if bar.timestamp.isoformat() == timestamp_iso:
            return index
    raise ValueError(f"timestamp not found in bars: {timestamp_iso}")


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Delayed Confirmation Revalidation",
        "",
        f"- Symbol: {payload['symbol']}",
        f"- Family: {payload['family_name']}",
        f"- Window: {payload['sample_start_date']} to {payload['sample_end_date']}",
        f"- Best delayed variant: {payload['best_delayed_confirmation_variant']['variant_name']}",
        f"- Decision bucket: {payload['best_delayed_confirmation_variant']['decision_bucket']}",
        "",
        "## Variant Results",
        "",
    ]
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        lines.extend(
            [
                f"### {row['variant_name']}",
                "",
                f"- Description: {row['description']}",
                f"- Trades: {metrics['trades']}",
                f"- Realized P/L: {metrics['realized_pnl']}",
                f"- Avg trade: {metrics['avg_trade']}",
                f"- Median trade: {metrics['median_trade']}",
                f"- PF: {metrics['profit_factor']}",
                f"- Max DD: {metrics['max_drawdown']}",
                f"- Win rate: {metrics['win_rate']}",
                f"- Avg loser: {metrics['average_loser']}",
                f"- Avg winner / avg loser: {metrics['avg_winner_over_avg_loser']}",
                f"- Top-1 contribution: {metrics['top_1_contribution']}",
                f"- Top-3 contribution: {metrics['top_3_contribution']}",
                f"- Decision bucket: {row['decision_bucket']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Conclusion",
            "",
            f"- Delayed confirmation preserves enough: {payload['causal_revalidation_conclusion']['delayed_confirmation_preserves_enough_to_remain_credible']}",
            f"- Benchmark realized-P/L recovery share: {payload['causal_revalidation_conclusion']['benchmark_realized_pnl_recovery_share']}",
            f"- Good enough for executable paper-design pass: {payload['causal_revalidation_conclusion']['good_enough_for_narrow_executable_paper_design_pass']}",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
