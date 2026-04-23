"""GC ATP promoted-exit evolution study for better trend participation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass, replace
from bisect import bisect_right
from datetime import timedelta
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..research.trend_participation.atp_promotion_add_review import STRONGEST_PROMOTION_ADD_CANDIDATE_ID
from ..research.trend_participation.backtest import summarize_performance
from ..research.trend_participation.models import ResearchBar, TradeRecord
from ..research.trend_participation.phase3_timing import (
    ATP_REPLAY_CHECKPOINT_LOCK_R,
    ATP_REPLAY_NO_TRACTION_ABORT_BARS,
    ATP_REPLAY_NO_TRACTION_MIN_FAVORABLE_R,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_15M_2OF3,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_20M_LOOSE,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
    ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_NO_TRACTION,
    _build_completed_maintenance_context,
    _maintenance_exit_triggered,
    _promoted_maintenance_stop_price,
    _replay_exit_policy_profile,
)
from .atp_scope_replay_probe import (
    _apply_confirmation_sizing_profile,
    _load_bars_for_intervals,
    _merged_replay_intervals,
    load_scope_replay_probe_bundle,
)

DEFAULT_SCOPE_BUNDLE_MANIFEST = Path(
    "outputs/research_platform/atp_substrate/scope_bundles/7c367b0af017569b/manifest.json"
)
DEFAULT_OUTPUT_DIR = Path("outputs/reports/atp_gc_exit_evolution")
BASELINE_REFERENCE_ID = "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only"
BASELINE_REFERENCE_LABEL = "ATP Companion Candidate / GC / Asia Only / promotion_1_075r_favorable_only"
DEFAULT_POINT_VALUE = 100.0


@dataclass(frozen=True)
class ExitVariantSpec:
    variant_id: str
    label: str
    exit_policy: str
    promotion_checkpoint_r: float | None
    maintenance_timeframe: str
    notes: str
    is_baseline: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-gc-exit-evolution")
    parser.add_argument(
        "--scope-bundle-manifest",
        default=str(DEFAULT_SCOPE_BUNDLE_MANIFEST),
        help="ATP scope-bundle manifest for the strongest current GC add-size path.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for the GC exit evolution report artifacts.",
    )
    parser.add_argument(
        "--point-value",
        type=float,
        default=DEFAULT_POINT_VALUE,
        help="Optional point-value override. Defaults to GC $100/point.",
    )
    return parser


def run_gc_exit_evolution(
    *,
    scope_bundle_manifest: Path,
    output_dir: Path,
    point_value: float = DEFAULT_POINT_VALUE,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    specs = _variant_specs()
    results: list[dict[str, Any]] = []
    bundle = load_scope_replay_probe_bundle(scope_bundle_manifest)
    long_bars = _load_bars_for_intervals(
        sqlite_path=Path(bundle.manifest["source_db"]),
        symbol=str(bundle.manifest["symbol"]).upper(),
        intervals=_merged_replay_intervals(
            timing_states=bundle.timing_states,
            window_minutes=480,
        ),
    )
    baseline_trades, baseline_sizing_summary = _apply_confirmation_sizing_profile(
        trades=bundle.trade_records,
        bars_1m=long_bars,
        point_value=point_value,
        profile=_baseline_confirmation_profile(),
    )

    for spec in specs:
        if spec.is_baseline:
            trades = baseline_trades
            probe_summary = {
                "trade_source": "bundle_trade_records_with_confirmation_add_overlay",
                "bars_loaded": len(long_bars),
                "merged_intervals": len(
                    _merged_replay_intervals(
                        timing_states=bundle.timing_states,
                        window_minutes=480,
                    )
                ),
                "confirmation_add_trade_count": baseline_sizing_summary["confirmation_add_trade_count"],
                "confirmation_add_net_pnl": baseline_sizing_summary["confirmation_add_net_pnl"],
            }
        else:
            replayed_trades = _replay_trade_records_under_exit_policy(
                trades=bundle.trade_records,
                bars_1m=long_bars,
                point_value=point_value,
                exit_policy=spec.exit_policy,
            )
            trades, sizing_summary = _apply_confirmation_sizing_profile(
                trades=replayed_trades,
                bars_1m=long_bars,
                point_value=point_value,
                profile=_baseline_confirmation_profile(),
            )
            probe_summary = {
                "trade_source": "bundle_trade_records_replayed_exit_policy",
                "bars_loaded": len(long_bars),
                "merged_intervals": len(
                    _merged_replay_intervals(
                        timing_states=bundle.timing_states,
                        window_minutes=480,
                    )
                ),
                "confirmation_add_trade_count": sizing_summary["confirmation_add_trade_count"],
                "confirmation_add_net_pnl": sizing_summary["confirmation_add_net_pnl"],
            }
        results.append(
            {
                "variant_id": spec.variant_id,
                "label": spec.label,
                "exit_policy": spec.exit_policy,
                "promotion_checkpoint_r": spec.promotion_checkpoint_r,
                "maintenance_timeframe": spec.maintenance_timeframe,
                "notes": spec.notes,
                "is_baseline": spec.is_baseline,
                "probe_summary": probe_summary,
                "metrics": _variant_metrics(trades=trades, point_value=point_value),
                "bundle_manifest": str(bundle.manifest.get("source_db") or scope_bundle_manifest),
            }
        )

    baseline = next(result for result in results if result["is_baseline"])
    for result in results:
        result["comparison_vs_baseline"] = _comparison_vs_baseline(
            baseline=baseline["metrics"],
            candidate=result["metrics"],
        )

    best_candidate = _best_candidate(results)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study": "atp_gc_exit_evolution",
        "baseline_reference": {
            "strategy_id": BASELINE_REFERENCE_ID,
            "label": BASELINE_REFERENCE_LABEL,
            "scope_bundle_manifest": str(scope_bundle_manifest.resolve()),
            "confirmation_add_candidate_id": STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
            "probe_size_fraction": 1.0,
            "confirmation_add_size_fraction": 1.0,
        },
        "implementation_plan": {
            "files_touched": [
                "src/mgc_v05l/research/trend_participation/models.py",
                "src/mgc_v05l/research/trend_participation/phase3_timing.py",
                "src/mgc_v05l/app/atp_scope_replay_probe.py",
                "src/mgc_v05l/app/atp_gc_exit_evolution.py",
                "src/mgc_v05l/app/main.py",
                "tests/unit/test_trend_participation_engine.py",
            ],
            "extension_type": "checkpoint_exit_extension_on_existing_gc_promotion_add_path",
            "assumptions": [
                "Higher-timeframe maintenance uses completed bars only.",
                "Maintenance fast EMA span is 5 bars on the chosen maintenance timeframe.",
                "The 15m 2-of-3 lower-half condition only activates once promoted MFE exceeds checkpoint + 0.25R.",
                "After promotion, the short max-hold no longer governs the trade; the replay holds through the current session until maintenance exit or structural stop.",
                "The current strongest GC reference path is the Asia-only promotion_1_075r_favorable_only add-size lane.",
            ],
        },
        "tested_variants": [
            {
                "variant_id": result["variant_id"],
                "label": result["label"],
                "exit_policy": result["exit_policy"],
                "promotion_checkpoint_r": result["promotion_checkpoint_r"],
            }
            for result in results
        ],
        "results": results,
        "best_candidate": {
            "variant_id": best_candidate["variant_id"],
            "label": best_candidate["label"],
            "metrics": best_candidate["metrics"],
            "comparison_vs_baseline": best_candidate["comparison_vs_baseline"],
        },
        "judgment": _judgment(results=results, best_candidate=best_candidate),
    }
    json_path = output_dir / "atp_gc_exit_evolution.json"
    markdown_path = output_dir / "atp_gc_exit_evolution.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_gc_exit_evolution_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _variant_specs() -> list[ExitVariantSpec]:
    return [
        ExitVariantSpec(
            variant_id="baseline_target_checkpoint_no_traction",
            label="Current strongest GC reference",
            exit_policy=ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_NO_TRACTION,
            promotion_checkpoint_r=None,
            maintenance_timeframe="legacy",
            notes="Existing checkpoint + no-traction exit stack with the current strongest GC favorable-only add path.",
            is_baseline=True,
        ),
        ExitVariantSpec(
            variant_id="checkpoint075_no_traction_abort_15m_2of3",
            label="Checkpoint 0.75R + 15m 2-of-3 maintenance",
            exit_policy=ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_15M_2OF3,
            promotion_checkpoint_r=0.75,
            maintenance_timeframe="15m",
            notes="Primary candidate: graduate proven trades into 15m trend maintenance after 0.75R.",
        ),
        ExitVariantSpec(
            variant_id="checkpoint100_no_traction_abort_15m_2of3",
            label="Checkpoint 1.00R + 15m 2-of-3 maintenance",
            exit_policy=ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
            promotion_checkpoint_r=1.0,
            maintenance_timeframe="15m",
            notes="Stricter promotion trigger to see whether waiting for 1.0R materially improves quality.",
        ),
        ExitVariantSpec(
            variant_id="checkpoint075_no_traction_abort_10m_double_weak_close",
            label="Checkpoint 0.75R + 10m double weak close",
            exit_policy=ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK,
            promotion_checkpoint_r=0.75,
            maintenance_timeframe="10m",
            notes="Tighter maintenance for responsiveness after promotion.",
        ),
        ExitVariantSpec(
            variant_id="checkpoint075_no_traction_abort_20m_loose_trend_hold",
            label="Checkpoint 0.75R + 20m loose trend hold",
            exit_policy=ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_20M_LOOSE,
            promotion_checkpoint_r=0.75,
            maintenance_timeframe="20m",
            notes="Looser maintenance to test whether extra breathing room creates real value or just more giveback.",
        ),
    ]


def _baseline_confirmation_profile():
    from .atp_scope_replay_probe import ConfirmationSizingProfile, _candidate_by_id

    return ConfirmationSizingProfile(
        probe_size_fraction=1.0,
        confirmation_add_size_fraction=1.0,
        confirmation_add_candidate=_candidate_by_id(STRONGEST_PROMOTION_ADD_CANDIDATE_ID),
    )


def _replay_trade_records_under_exit_policy(
    *,
    trades: Sequence[TradeRecord],
    bars_1m: Sequence[ResearchBar],
    point_value: float,
    exit_policy: str,
) -> list[TradeRecord]:
    policy_profile = _replay_exit_policy_profile(exit_policy)
    bars_by_instrument: dict[str, list[ResearchBar]] = {}
    minute_end_ts_by_instrument: dict[str, list[datetime]] = {}
    maintenance_cache: dict[tuple[str, int], dict[str, Any]] = {}
    for bar in sorted(bars_1m, key=lambda item: (item.instrument, item.end_ts)):
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)
    for instrument, rows in bars_by_instrument.items():
        minute_end_ts_by_instrument[instrument] = [bar.end_ts for bar in rows]
        if policy_profile.get("use_promoted_maintenance_exit"):
            maintenance_cache[(instrument, int(policy_profile["maintenance_timeframe_minutes"]))] = (
                _build_completed_maintenance_context(
                    minute_bars=rows,
                    timeframe_minutes=int(policy_profile["maintenance_timeframe_minutes"]),
                )
            )

    replayed: list[TradeRecord] = []
    for trade in trades:
        if trade.side != "LONG":
            replayed.append(trade)
            continue
        candidate_bars = bars_by_instrument.get(trade.instrument, [])
        minute_end_timestamps = minute_end_ts_by_instrument.get(trade.instrument, [])
        entry_index = bisect_right(minute_end_timestamps, trade.entry_ts) - 1
        if entry_index < 0 or entry_index >= len(candidate_bars):
            replayed.append(trade)
            continue
        execution_window = _trade_execution_window(
            candidate_bars=candidate_bars,
            entry_index=entry_index,
            session_segment=trade.session_segment,
        )
        if not execution_window:
            replayed.append(trade)
            continue
        risk_points = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
        exit_slippage_points = max(float(trade.slippage_cost or 0.0) / max(point_value, 1e-9) / 2.0, 0.0)
        checkpoint_reached = False
        dynamic_stop_price = float(trade.stop_price)
        exit_bar = execution_window[-1]
        raw_exit_price = float(exit_bar.close)
        exit_price = raw_exit_price - exit_slippage_points
        exit_reason = "time_stop"
        mfe_points = 0.0
        mae_points = 0.0
        bars_held = 0
        promotion_ts = None
        promotion_price = None
        pnl_points_at_promotion = None
        post_promotion_peak_open_profit_points = None
        last_completed_maintenance_bar_end_ts = None
        maintenance_state = maintenance_cache.get(
            (trade.instrument, int(policy_profile.get("maintenance_timeframe_minutes") or 0))
        )

        for relative_index, bar in enumerate(execution_window, start=1):
            bars_held = relative_index
            mfe_points = max(mfe_points, float(bar.high) - float(trade.entry_price))
            mae_points = max(mae_points, float(trade.entry_price) - float(bar.low))
            stop_hit = float(bar.low) <= dynamic_stop_price
            promotion_hit = (
                not checkpoint_reached
                and policy_profile.get("promotion_r_multiple") is not None
                and mfe_points >= risk_points * float(policy_profile["promotion_r_multiple"])
            )
            if promotion_hit:
                checkpoint_reached = True
                promotion_ts = bar.end_ts
                promotion_price = float(bar.close)
                pnl_points_at_promotion = promotion_price - float(trade.entry_price)
                post_promotion_peak_open_profit_points = max(
                    mfe_points,
                    float(bar.high) - float(trade.entry_price),
                )
                dynamic_stop_price = _promoted_maintenance_stop_price(
                    current_stop=dynamic_stop_price,
                    entry_fill_price=float(trade.entry_price),
                    risk_points=risk_points,
                    side=trade.side,
                )
            if checkpoint_reached:
                post_promotion_peak_open_profit_points = max(
                    float(post_promotion_peak_open_profit_points or 0.0),
                    float(bar.high) - float(trade.entry_price),
                )
            if stop_hit:
                exit_bar = bar
                raw_exit_price = dynamic_stop_price
                exit_price = raw_exit_price - exit_slippage_points
                exit_reason = "checkpoint_stop" if checkpoint_reached else "stop"
                break
            if (
                not checkpoint_reached
                and policy_profile.get("use_no_traction_abort")
                and relative_index >= ATP_REPLAY_NO_TRACTION_ABORT_BARS
                and mfe_points < (risk_points * ATP_REPLAY_NO_TRACTION_MIN_FAVORABLE_R)
            ):
                exit_bar = bar
                raw_exit_price = float(bar.close)
                exit_price = raw_exit_price - exit_slippage_points
                exit_reason = "no_traction_abort"
                break
            if checkpoint_reached and policy_profile.get("use_promoted_maintenance_exit") and maintenance_state is not None:
                maintenance_bar = maintenance_state["bars_by_end_ts"].get(bar.end_ts)
                if (
                    maintenance_bar is not None
                    and bar.end_ts != last_completed_maintenance_bar_end_ts
                    and promotion_ts is not None
                    and maintenance_bar.end_ts >= promotion_ts
                ):
                    if _maintenance_exit_triggered(
                        maintenance_bar=maintenance_bar,
                        maintenance_bars=maintenance_state["bars"],
                        maintenance_ema_by_end_ts=maintenance_state["ema_by_end_ts"],
                        side=trade.side,
                        policy_profile=policy_profile,
                        mfe_points=mfe_points,
                        risk_points=risk_points,
                    ):
                        last_completed_maintenance_bar_end_ts = bar.end_ts
                        exit_bar = bar
                        raw_exit_price = float(bar.close)
                        exit_price = raw_exit_price - exit_slippage_points
                        exit_reason = str(policy_profile["maintenance_exit_reason"])
                        break
                    last_completed_maintenance_bar_end_ts = bar.end_ts

        gross_pnl_points = raw_exit_price - float(trade.entry_price)
        pnl_points = exit_price - float(trade.entry_price)
        gross_pnl_cash = gross_pnl_points * point_value
        pnl_cash = pnl_points * point_value - float(trade.fees_paid)
        replayed.append(
            replace(
                trade,
                exit_ts=exit_bar.end_ts,
                exit_price=exit_price,
                stop_price=dynamic_stop_price,
                target_price=None,
                pnl_points=pnl_points,
                gross_pnl_cash=gross_pnl_cash,
                pnl_cash=pnl_cash,
                mfe_points=mfe_points,
                mae_points=mae_points,
                bars_held_1m=bars_held,
                hold_minutes=float(bars_held),
                exit_reason=exit_reason,
                stopout=exit_reason in {"stop", "checkpoint_stop"},
                participation_promoted=checkpoint_reached and promotion_ts is not None,
                promotion_trigger_r_multiple=(
                    float(policy_profile["promotion_r_multiple"])
                    if checkpoint_reached and policy_profile.get("promotion_r_multiple") is not None
                    else None
                ),
                promotion_ts=promotion_ts,
                promotion_price=promotion_price,
                pnl_points_at_promotion=pnl_points_at_promotion,
                post_promotion_peak_open_profit_points=post_promotion_peak_open_profit_points,
            )
        )
    return replayed


def _trade_execution_window(
    *,
    candidate_bars: Sequence[ResearchBar],
    entry_index: int,
    session_segment: str,
) -> list[ResearchBar]:
    bounded: list[ResearchBar] = []
    for bar in candidate_bars[entry_index:]:
        if bounded and str(bar.session_segment or "") != str(session_segment or ""):
            break
        bounded.append(bar)
    return bounded


def _variant_metrics(*, trades: Sequence[TradeRecord], point_value: float) -> dict[str, Any]:
    performance = summarize_performance(trades)
    promoted = [trade for trade in trades if trade.participation_promoted]
    non_promoted = [trade for trade in trades if not trade.participation_promoted]
    promoted_pnl = sum(float(trade.pnl_cash) for trade in promoted)
    total_pnl = sum(float(trade.pnl_cash) for trade in trades)

    return {
        "trade_count": performance.trade_count,
        "net_pnl_cash": round(float(performance.net_pnl_cash), 6),
        "max_drawdown": round(float(performance.max_drawdown), 6),
        "profit_factor": round(float(performance.profit_factor), 6),
        "win_rate": round(float(performance.win_rate), 6),
        "expectancy": round(float(performance.expectancy), 6),
        "average_hold_minutes": round(float(performance.avg_hold_minutes), 6),
        "median_hold_minutes": round(_median([float(trade.hold_minutes) for trade in trades]), 6),
        "mfe_summary": _point_summary([float(trade.mfe_points) for trade in trades]),
        "mae_summary": _point_summary([float(trade.mae_points) for trade in trades]),
        "giveback_from_peak_open_profit_cash": round(
            _mean(
                max(float(trade.mfe_points) - float(trade.pnl_points), 0.0) * point_value
                for trade in trades
            ),
            6,
        ),
        "share_promoted": round(len(promoted) / max(len(trades), 1), 6),
        "promoted_trade_count": len(promoted),
        "non_promoted_trade_count": len(non_promoted),
        "promoted_trade_metrics": _subset_metrics(promoted),
        "non_promoted_trade_metrics": _subset_metrics(non_promoted),
        "promoted_trade_pnl_contribution_cash": round(promoted_pnl, 6),
        "promoted_trade_pnl_contribution_share": round(promoted_pnl / total_pnl, 6) if total_pnl else 0.0,
        "exit_reason_counts": dict(sorted(Counter(str(trade.exit_reason) for trade in trades).items())),
        "no_traction_abort_count": sum(1 for trade in trades if trade.exit_reason == "no_traction_abort"),
        "htf_maintenance_exit_count": sum(1 for trade in trades if str(trade.exit_reason).startswith("htf_")),
        "average_additional_pnl_after_checkpoint_cash": round(
            _mean(
                (float(trade.pnl_points) - float(trade.pnl_points_at_promotion or 0.0)) * point_value
                for trade in promoted
                if trade.pnl_points_at_promotion is not None
            ),
            6,
        ),
        "average_giveback_after_checkpoint_cash": round(
            _mean(
                max(
                    float(trade.post_promotion_peak_open_profit_points or 0.0) - float(trade.pnl_points),
                    0.0,
                )
                * point_value
                for trade in promoted
            ),
            6,
        ),
        "top_winners": _top_winners(trades),
    }


def _subset_metrics(trades: Sequence[TradeRecord]) -> dict[str, Any]:
    if not trades:
        return {
            "trade_count": 0,
            "net_pnl_cash": 0.0,
            "profit_factor": 0.0,
            "win_rate": 0.0,
            "expectancy": 0.0,
            "average_hold_minutes": 0.0,
        }
    performance = summarize_performance(trades)
    return {
        "trade_count": performance.trade_count,
        "net_pnl_cash": round(float(performance.net_pnl_cash), 6),
        "profit_factor": round(float(performance.profit_factor), 6),
        "win_rate": round(float(performance.win_rate), 6),
        "expectancy": round(float(performance.expectancy), 6),
        "average_hold_minutes": round(float(performance.avg_hold_minutes), 6),
    }


def _comparison_vs_baseline(*, baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline_top = baseline.get("top_winners") or {}
    candidate_top = candidate.get("top_winners") or {}
    return {
        "net_pnl_cash_delta": round(float(candidate["net_pnl_cash"]) - float(baseline["net_pnl_cash"]), 6),
        "max_drawdown_delta": round(float(candidate["max_drawdown"]) - float(baseline["max_drawdown"]), 6),
        "profit_factor_delta": round(float(candidate["profit_factor"]) - float(baseline["profit_factor"]), 6),
        "expectancy_delta": round(float(candidate["expectancy"]) - float(baseline["expectancy"]), 6),
        "average_hold_minutes_delta": round(
            float(candidate["average_hold_minutes"]) - float(baseline["average_hold_minutes"]), 6
        ),
        "top_winner_cash_delta": round(
            float(candidate_top.get("largest_winner_cash", 0.0)) - float(baseline_top.get("largest_winner_cash", 0.0)),
            6,
        ),
        "top5_winner_sum_cash_delta": round(
            float(candidate_top.get("top5_winner_sum_cash", 0.0))
            - float(baseline_top.get("top5_winner_sum_cash", 0.0)),
            6,
        ),
    }


def _best_candidate(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    candidates = [result for result in results if not result["is_baseline"]]
    return max(
        candidates,
        key=lambda result: (
            float(result["metrics"]["net_pnl_cash"]),
            float(result["metrics"]["profit_factor"]),
            -float(result["metrics"]["max_drawdown"]),
        ),
    )


def _judgment(*, results: Sequence[dict[str, Any]], best_candidate: dict[str, Any]) -> dict[str, Any]:
    by_id = {result["variant_id"]: result for result in results}
    baseline = next(result for result in results if result["is_baseline"])
    checkpoint15 = by_id["checkpoint075_no_traction_abort_15m_2of3"]
    checkpoint10 = by_id["checkpoint075_no_traction_abort_10m_double_weak_close"]
    checkpoint20 = by_id["checkpoint075_no_traction_abort_20m_loose_trend_hold"]
    checkpoint100 = by_id["checkpoint100_no_traction_abort_15m_2of3"]

    fifteen_better_than_ten = (
        float(checkpoint15["metrics"]["net_pnl_cash"]) > float(checkpoint10["metrics"]["net_pnl_cash"])
        and float(checkpoint15["metrics"]["profit_factor"]) >= float(checkpoint10["metrics"]["profit_factor"])
    )
    twenty_too_loose = (
        float(checkpoint20["metrics"]["average_giveback_after_checkpoint_cash"])
        > float(checkpoint15["metrics"]["average_giveback_after_checkpoint_cash"])
        and float(checkpoint20["metrics"]["net_pnl_cash"]) <= float(checkpoint15["metrics"]["net_pnl_cash"])
    )
    post_checkpoint_improves_ev = (
        float(best_candidate["metrics"]["net_pnl_cash"]) > float(baseline["metrics"]["net_pnl_cash"])
        and float(best_candidate["metrics"]["expectancy"]) > float(baseline["metrics"]["expectancy"])
    )
    trend_participation_real = (
        float(best_candidate["comparison_vs_baseline"]["top5_winner_sum_cash_delta"]) > 0.0
        and float(best_candidate["comparison_vs_baseline"]["net_pnl_cash_delta"]) > 0.0
    )

    return {
        "best_candidate_id": best_candidate["variant_id"],
        "is_15m_better_than_10m": {
            "answer": "yes" if fifteen_better_than_ten else "no",
            "reason": (
                "The 15m maintenance candidate beats or matches the 10m candidate on both net P&L and profit factor."
                if fifteen_better_than_ten
                else "The 10m candidate is at least as strong economically, so 15m is not clearly superior on this pass."
            ),
        },
        "is_20m_too_loose": {
            "answer": "yes" if twenty_too_loose else "no",
            "reason": (
                "The 20m hold gives back more after checkpoint without paying for it in better net P&L."
                if twenty_too_loose
                else "The 20m hold does not look obviously too loose on this first pass."
            ),
        },
        "does_removing_short_max_hold_after_checkpoint_improve_expected_value": {
            "answer": "yes" if post_checkpoint_improves_ev else "no",
            "reason": (
                "At least one promoted-maintenance variant improved both net P&L and expectancy versus the current reference."
                if post_checkpoint_improves_ev
                else "Longer promoted holding did not yet beat the current reference on both net P&L and expectancy."
            ),
        },
        "does_the_new_exit_architecture_improve_trend_participation": {
            "answer": "yes" if trend_participation_real else "no",
            "reason": (
                "The best candidate grew the top winners and total P&L instead of merely stretching hold time."
                if trend_participation_real
                else "The new exits mostly delayed exits without enough additional winner capture."
            ),
        },
        "checkpoint100_vs_checkpoint075_on_15m": {
            "winner": (
                checkpoint100["variant_id"]
                if float(checkpoint100["metrics"]["net_pnl_cash"]) > float(checkpoint15["metrics"]["net_pnl_cash"])
                else checkpoint15["variant_id"]
            ),
        },
    }


def render_gc_exit_evolution_markdown(payload: dict[str, Any]) -> str:
    baseline = payload["baseline_reference"]
    best = payload["best_candidate"]
    judgment = payload["judgment"]
    results = payload["results"]
    lines = [
        "# ATP GC Exit Evolution",
        "",
        f"1. baseline reference used: `{baseline['strategy_id']}`",
        "2. tested variants: "
        + ", ".join(f"`{row['variant_id']}`" for row in payload["tested_variants"]),
        f"3. best-performing candidate: `{best['variant_id']}`",
        f"4. whether 15m maintenance exit appears structurally superior: `{judgment['is_15m_better_than_10m']['answer']}`",
        f"5. whether the GC add-size path now behaves more like true trend participation: `{judgment['does_the_new_exit_architecture_improve_trend_participation']['answer']}`",
        "",
        "## Comparative Summary",
        "",
        "| Variant | Net P&L | Max DD | PF | Win rate | Expectancy | Avg hold | Median hold | Promoted share | HTF exits | Avg addl after checkpoint | Avg giveback after checkpoint |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        metrics = result["metrics"]
        lines.append(
            "| "
            + " | ".join(
                [
                    result["variant_id"],
                    f"{metrics['net_pnl_cash']:.2f}",
                    f"{metrics['max_drawdown']:.2f}",
                    f"{metrics['profit_factor']:.3f}",
                    f"{metrics['win_rate']:.3f}",
                    f"{metrics['expectancy']:.2f}",
                    f"{metrics['average_hold_minutes']:.2f}",
                    f"{metrics['median_hold_minutes']:.2f}",
                    f"{metrics['share_promoted']:.3f}",
                    str(metrics["htf_maintenance_exit_count"]),
                    f"{metrics['average_additional_pnl_after_checkpoint_cash']:.2f}",
                    f"{metrics['average_giveback_after_checkpoint_cash']:.2f}",
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Recommendation",
            f"- Best candidate: `{best['variant_id']}`",
            f"- 15m vs 10m: {judgment['is_15m_better_than_10m']['reason']}",
            f"- 20m looseness: {judgment['is_20m_too_loose']['reason']}",
            f"- Max-hold removal after checkpoint: {judgment['does_removing_short_max_hold_after_checkpoint_improve_expected_value']['reason']}",
            f"- Trend participation verdict: {judgment['does_the_new_exit_architecture_improve_trend_participation']['reason']}",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _point_summary(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"mean": 0.0, "median": 0.0, "max": 0.0}
    return {
        "mean": round(sum(values) / len(values), 6),
        "median": round(float(median(values)), 6),
        "max": round(float(max(values)), 6),
    }


def _top_winners(trades: Sequence[TradeRecord]) -> dict[str, float]:
    pnls = sorted((float(trade.pnl_cash) for trade in trades), reverse=True)
    return {
        "largest_winner_cash": round(float(pnls[0]), 6) if pnls else 0.0,
        "top5_winner_sum_cash": round(sum(pnls[:5]), 6),
    }


def _mean(values: Sequence[float] | Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(sum(items) / len(items))


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(median(values))
