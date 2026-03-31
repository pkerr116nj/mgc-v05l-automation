"""Third-pass narrowing study for MGC impulse burst continuation."""

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
    _build_latest_context_lookup,
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    _max_drawdown,
    _profit_factor,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
    _survives_without_top,
    _top_trade_share,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_loser_archetypes import _body_to_range_quality
from .mgc_impulse_burst_subclass_diagnostics import (
    _classify_subclass,
    _micro_breakout,
    _pre_burst_compression_ratio,
    _prior_run_norm,
)


BASE_VARIANT = "breadth_plus_agreement_combo"


@dataclass(frozen=True)
class NarrowingVariant:
    variant_name: str
    description: str
    largest_bar_share_max: float | None = None
    min_material_bars: int | None = None
    chase_prior_10_trigger: float | None = None
    chase_prior_20_trigger: float | None = None
    chase_late_extension_trigger: float | None = None
    excluded_phases: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventSnapshot:
    event: AcceptedEvent
    prior_10_norm: float
    prior_20_norm: float
    compression_ratio: float
    micro_breakout: bool
    body_to_range_quality: float
    subclass_bucket: str


VARIANTS: tuple[NarrowingVariant, ...] = (
    NarrowingVariant(
        variant_name="base_breadth_plus_agreement_combo_control",
        description="Base accepted event stream from breadth_plus_agreement_combo with no extra narrowing.",
    ),
    NarrowingVariant(
        variant_name="stronger_anti_spike",
        description="Tighter largest-bar concentration cap plus minimum contributing bars.",
        largest_bar_share_max=0.50,
        min_material_bars=4,
    ),
    NarrowingVariant(
        variant_name="stronger_anti_late_chase",
        description="Reject high prior-run bursts when late-extension share suggests chase behavior.",
        chase_prior_10_trigger=1.32,
        chase_prior_20_trigger=1.00,
        chase_late_extension_trigger=0.50,
    ),
    NarrowingVariant(
        variant_name="anti_spike_plus_anti_late_chase",
        description="Combine tighter spike suppression with late-chase suppression.",
        largest_bar_share_max=0.50,
        min_material_bars=4,
        chase_prior_10_trigger=1.32,
        chase_prior_20_trigger=1.00,
        chase_late_extension_trigger=0.50,
    ),
    NarrowingVariant(
        variant_name="best_judgment_compact_combo",
        description="Slightly tighter combined variant justified by loser-archetype evidence.",
        largest_bar_share_max=0.49,
        min_material_bars=4,
        chase_prior_10_trigger=1.28,
        chase_prior_20_trigger=0.96,
        chase_late_extension_trigger=0.49,
    ),
)

SESSION_VARIANTS: tuple[NarrowingVariant, ...] = (
    NarrowingVariant(
        variant_name="session_control_none",
        description="No session restriction.",
    ),
    NarrowingVariant(
        variant_name="exclude_asia_only",
        description="Exclude ASIA_EARLY and ASIA_LATE only.",
        excluded_phases=("ASIA_EARLY", "ASIA_LATE"),
    ),
    NarrowingVariant(
        variant_name="exclude_asia_and_london_open",
        description="Exclude ASIA_EARLY, ASIA_LATE, and LONDON_OPEN.",
        excluded_phases=("ASIA_EARLY", "ASIA_LATE", "LONDON_OPEN"),
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_third_pass_narrowing(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-third-pass-narrowing")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_third_pass_narrowing(*, symbol: str) -> dict[str, Any]:
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
    snapshots = [
        _event_snapshot(
            bars_1m=one_minute,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            event=event,
        )
        for event in base_events
    ]

    control_snapshot = _evaluate_variant(
        bars=one_minute,
        snapshots=snapshots,
        variant=VARIANTS[0],
        control_snapshots=snapshots,
    )
    variant_rows = [control_snapshot]
    for variant in VARIANTS[1:]:
        variant_rows.append(
            _evaluate_variant(
                bars=one_minute,
                snapshots=snapshots,
                variant=variant,
                control_snapshots=snapshots,
            )
        )

    best_narrowed = _pick_best_narrowed_variant(variant_rows)
    best_variant = next(variant for variant in VARIANTS if variant.variant_name == best_narrowed["variant_name"])
    session_rows = [
        _evaluate_variant(
            bars=one_minute,
            snapshots=snapshots,
            variant=NarrowingVariant(
                variant_name=session_variant.variant_name,
                description=session_variant.description,
                largest_bar_share_max=best_variant.largest_bar_share_max,
                min_material_bars=best_variant.min_material_bars,
                chase_prior_10_trigger=best_variant.chase_prior_10_trigger,
                chase_prior_20_trigger=best_variant.chase_prior_20_trigger,
                chase_late_extension_trigger=best_variant.chase_late_extension_trigger,
                excluded_phases=session_variant.excluded_phases,
            ),
            control_snapshots=snapshots,
        )
        for session_variant in SESSION_VARIANTS
    ]

    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "base_variant": BASE_VARIANT,
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "narrowing_rules_used": _rules_used(),
        "variants_tested": [row["variant_name"] for row in variant_rows],
        "variant_results": variant_rows,
        "best_narrowed_variant": {
            "variant_name": best_narrowed["variant_name"],
            "decision_bucket": best_narrowed["decision_bucket"],
        },
        "session_restriction_comparison": session_rows,
        "narrowing_conclusion": _narrowing_conclusion(best_narrowed=best_narrowed, session_rows=session_rows),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_burst_third_pass_narrowing.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_third_pass_narrowing.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_third_pass_narrowing",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_narrowed_variant": payload["best_narrowed_variant"],
        "narrowing_conclusion": payload["narrowing_conclusion"],
    }


def _event_snapshot(*, bars_1m: list[Any], atr_1m: list[float | None], rv_1m: list[float | None], event: AcceptedEvent) -> EventSnapshot:
    trade = _build_trade_outcome(bars=bars_1m, event=event, overlay="BASE", r_loss_proxy=None)
    signal_index = event.signal_index
    direction_sign = 1.0 if event.impulse["direction"] == "LONG" else -1.0
    prior_10 = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=signal_index, lookback=10, direction_sign=direction_sign)
    prior_20 = _prior_run_norm(bars_1m, atr_1m, rv_1m, index=signal_index, lookback=20, direction_sign=direction_sign)
    compression_ratio = _pre_burst_compression_ratio(bars_1m, index=signal_index)
    micro_breakout = _micro_breakout(bars_1m, index=signal_index, direction=str(event.impulse["direction"]))
    subclass = _classify_subclass(
        prior_run_10_norm=prior_10,
        prior_run_20_norm=prior_20,
        compression_ratio=compression_ratio,
        micro_breakout=micro_breakout,
        largest_bar_share=float(event.impulse["largest_bar_share"]),
    )
    return EventSnapshot(
        event=AcceptedEvent(signal_index=signal_index, impulse=event.impulse, base_exit_ts=trade.exit_ts),
        prior_10_norm=prior_10,
        prior_20_norm=prior_20,
        compression_ratio=compression_ratio,
        micro_breakout=micro_breakout,
        body_to_range_quality=_body_to_range_quality(bars_1m, signal_index=signal_index),
        subclass_bucket=subclass,
    )


def _passes_variant(snapshot: EventSnapshot, variant: NarrowingVariant) -> bool:
    impulse = snapshot.event.impulse
    if variant.excluded_phases and str(impulse["signal_phase"]) in variant.excluded_phases:
        return False
    if variant.largest_bar_share_max is not None and float(impulse["largest_bar_share"]) > variant.largest_bar_share_max:
        return False
    if variant.min_material_bars is not None and int(impulse["materially_contributing_bars"]) < variant.min_material_bars:
        return False
    if (
        variant.chase_late_extension_trigger is not None
        and variant.chase_prior_20_trigger is not None
        and variant.chase_prior_10_trigger is not None
    ):
        late_extension = float(impulse["late_extension_share"])
        if (
            (snapshot.prior_20_norm >= variant.chase_prior_20_trigger and late_extension >= variant.chase_late_extension_trigger)
            or (snapshot.prior_10_norm >= variant.chase_prior_10_trigger and late_extension >= max(variant.chase_late_extension_trigger - 0.02, 0.0))
        ):
            return False
    return True


def _evaluate_variant(
    *,
    bars: list[Any],
    snapshots: list[EventSnapshot],
    variant: NarrowingVariant,
    control_snapshots: list[EventSnapshot],
) -> dict[str, Any]:
    kept = [snapshot for snapshot in snapshots if _passes_variant(snapshot, variant)]
    trades = [_build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None) for snapshot in kept]
    metrics = _metrics(trades)
    control_counts = Counter(snapshot.subclass_bucket for snapshot in control_snapshots)
    kept_counts = Counter(snapshot.subclass_bucket for snapshot in kept)
    loser_counts = Counter(snapshot.subclass_bucket for snapshot, trade in zip(kept, trades) if trade.pnl < 0)
    composition = {
        "all_trade_subclass_counts": dict(kept_counts),
        "losing_trade_subclass_counts": dict(loser_counts),
        "removed_share_vs_control": {
            bucket: _removed_share(control_counts.get(bucket, 0), kept_counts.get(bucket, 0))
            for bucket in ("SPIKE_DOMINATED_OTHER", "LATE_EXTENSION_CHASE")
        },
        "surviving_dominant_loser_archetype": max(loser_counts, key=loser_counts.get) if loser_counts else None,
    }
    decision_bucket = _decision_bucket(variant=variant, metrics=metrics, composition=composition, control_metrics=_metrics([_build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None) for snapshot in control_snapshots]))
    return {
        "variant_name": variant.variant_name,
        "description": variant.description,
        "session_exclusions": list(variant.excluded_phases),
        "rules": _variant_rule_summary(variant),
        "metrics": metrics,
        "loser_archetype_composition": composition,
        "decision_bucket": decision_bucket,
    }


def _metrics(trades: list[TradeOutcome]) -> dict[str, Any]:
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
        "avg_winner_over_avg_loser": _safe_ratio(_mean_or_none(winners), _mean_or_none(losers)),
        "top_1_contribution": _top_trade_share(pnls, top_n=1),
        "top_3_contribution": _top_trade_share(pnls, top_n=3),
        "survives_without_top_1": _survives_without_top(pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        "large_winner_count": _count_above_threshold(winners, 84.0),
        "very_large_winner_count": _count_above_threshold(winners, 140.0),
    }


def _removed_share(control_count: int, kept_count: int) -> float | None:
    if control_count <= 0:
        return None
    return round(1.0 - (kept_count / control_count), 4)


def _decision_bucket(*, variant: NarrowingVariant, metrics: dict[str, Any], composition: dict[str, Any], control_metrics: dict[str, Any]) -> str:
    trades = metrics["trades"] or 0
    if trades <= 0:
        return "FAMILY_COLLAPSES_WHEN_POISON_REMOVED"
    trade_ratio = trades / max(control_metrics["trades"] or 1, 1)
    cleaner = (
        (metrics["profit_factor"] or 0.0) >= (control_metrics["profit_factor"] or 0.0)
        and (metrics["average_loser"] or 999999.0) <= (control_metrics["average_loser"] or 999999.0)
        and (metrics["top_3_contribution"] or 999999.0) <= (control_metrics["top_3_contribution"] or 999999.0)
    )
    if cleaner and trade_ratio >= 0.65 and (metrics["large_winner_count"] or 0) >= int(max((control_metrics["large_winner_count"] or 0) * 0.75, 1)):
        return "CLEANER_AND_STILL_REAL"
    if (metrics["profit_factor"] or 0.0) >= 1.0 and trade_ratio >= 0.45:
        return "IMPROVED_BUT_STILL_MIXED"
    if trade_ratio < 0.35 and (metrics["profit_factor"] or 0.0) >= 1.0:
        return "TOO_DENSITY_DESTRUCTIVE"
    return "FAMILY_COLLAPSES_WHEN_POISON_REMOVED"


def _pick_best_narrowed_variant(rows: list[dict[str, Any]]) -> dict[str, Any]:
    narrowed = [row for row in rows if row["variant_name"] != VARIANTS[0].variant_name]
    return sorted(
        narrowed,
        key=lambda row: (
            {
                "CLEANER_AND_STILL_REAL": 3,
                "IMPROVED_BUT_STILL_MIXED": 2,
                "TOO_DENSITY_DESTRUCTIVE": 1,
                "FAMILY_COLLAPSES_WHEN_POISON_REMOVED": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _narrowing_conclusion(*, best_narrowed: dict[str, Any], session_rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_session = max(session_rows, key=lambda row: (row["metrics"]["realized_pnl"] or 0.0, row["metrics"]["profit_factor"] or 0.0))
    return {
        "family_survives_after_poison_pattern_suppression": best_narrowed["decision_bucket"] in {"CLEANER_AND_STILL_REAL", "IMPROVED_BUT_STILL_MIXED"},
        "best_narrowed_variant": best_narrowed["variant_name"],
        "best_narrowed_bucket": best_narrowed["decision_bucket"],
        "best_session_variant": best_session["variant_name"],
        "session_restriction_materially_justified": best_session["variant_name"] != "session_control_none" and (best_session["metrics"]["realized_pnl"] or 0.0) > (session_rows[0]["metrics"]["realized_pnl"] or 0.0) * 1.10 and (best_session["metrics"]["trades"] or 0) >= int((session_rows[0]["metrics"]["trades"] or 0) * 0.70),
        "mnq_transfer_readiness": best_narrowed["decision_bucket"] == "CLEANER_AND_STILL_REAL" and best_session["variant_name"] == "session_control_none",
    }


def _rules_used() -> dict[str, Any]:
    return {
        "methodology": "All narrowed variants are filtered subsets of the same accepted breadth_plus_agreement_combo event stream.",
        "anti_spike_tools": {
            "largest_bar_concentration_control": "tighter largest_bar_share cap",
            "minimum_materially_contributing_bars": "require at least 4 materially contributing burst bars in narrowed variants",
            "body_to_range_quality": "measured descriptively but not promoted to a hard gate because loser-archetype evidence did not support it cleanly",
        },
        "anti_late_chase_tools": {
            "prior_run_norms": "use prior_10 and prior_20 normalized directional run",
            "late_extension_share": "suppress bursts that already look materially extended before entry",
            "philosophy": "suppress late-extension chase, not all strong prior directional movement",
        },
        "session_branch": {
            "control": "no session restriction",
            "asia_only": ["ASIA_EARLY", "ASIA_LATE"],
            "asia_plus_early_europe": ["ASIA_EARLY", "ASIA_LATE", "LONDON_OPEN"],
        },
    }


def _variant_rule_summary(variant: NarrowingVariant) -> dict[str, Any]:
    return {
        "largest_bar_share_max": variant.largest_bar_share_max,
        "min_material_bars": variant.min_material_bars,
        "chase_prior_10_trigger": variant.chase_prior_10_trigger,
        "chase_prior_20_trigger": variant.chase_prior_20_trigger,
        "chase_late_extension_trigger": variant.chase_late_extension_trigger,
        "excluded_phases": list(variant.excluded_phases),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Burst Third Pass Narrowing",
        "",
        f"Base variant: {payload['base_variant']}",
        "",
        "## Variant Results",
        "",
        "| Variant | Bucket | Trades | PnL | Avg | Median | PF | DD | Win Rate | Avg Loser | Top3 | Large | Very Large |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        lines.append(
            f"| {row['variant_name']} | {row['decision_bucket']} | {metrics['trades']} | {metrics['realized_pnl']} | "
            f"{metrics['avg_trade']} | {metrics['median_trade']} | {metrics['profit_factor']} | {metrics['max_drawdown']} | "
            f"{metrics['win_rate']} | {metrics['average_loser']} | {metrics['top_3_contribution']} | "
            f"{metrics['large_winner_count']} | {metrics['very_large_winner_count']} |"
        )
    lines.extend(
        [
            "",
            "## Session Restriction Comparison",
            "",
            "| Variant | Trades | PnL | PF | DD | Win Rate | Top3 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in payload["session_restriction_comparison"]:
        metrics = row["metrics"]
        lines.append(
            f"| {row['variant_name']} | {metrics['trades']} | {metrics['realized_pnl']} | {metrics['profit_factor']} | {metrics['max_drawdown']} | {metrics['win_rate']} | {metrics['top_3_contribution']} |"
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
