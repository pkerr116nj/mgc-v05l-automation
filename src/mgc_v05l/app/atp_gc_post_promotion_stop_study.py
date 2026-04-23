"""GC-only post-promotion stop-loosening study anchored to the best 15m maintenance architecture."""

from __future__ import annotations

import argparse
import json
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..research.trend_participation.atp_promotion_add_review import STRONGEST_PROMOTION_ADD_CANDIDATE_ID
from ..research.trend_participation.backtest import summarize_performance
from ..research.trend_participation.models import ResearchBar, TradeRecord
from ..research.trend_participation.phase3_timing import (
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
    ATP_REPLAY_NO_TRACTION_ABORT_BARS,
    ATP_REPLAY_NO_TRACTION_MIN_FAVORABLE_R,
    _build_completed_maintenance_context,
    _maintenance_exit_triggered,
    _replay_exit_policy_profile,
)
from .atp_gc_exit_evolution import (
    BASELINE_REFERENCE_ID,
    BASELINE_REFERENCE_LABEL,
    DEFAULT_POINT_VALUE,
    DEFAULT_SCOPE_BUNDLE_MANIFEST,
)
from .atp_scope_replay_probe import (
    _apply_confirmation_sizing_profile,
    _load_bars_for_intervals,
    _merged_replay_intervals,
    load_scope_replay_probe_bundle,
)

DEFAULT_OUTPUT_DIR = Path("outputs/reports/atp_gc_post_promotion_stop_study")
CONTROL_VARIANT_ID = "checkpoint100_no_traction_abort_15m_2of3_control"
MATERIAL_RESCUE_DELTA_CASH = 250.0


@dataclass(frozen=True)
class PromotedStopVariantSpec:
    variant_id: str
    label: str
    notes: str
    stop_mode: str
    initial_lock_r: float
    update_basis: str
    is_control: bool = False
    structure_timeframe_minutes: int | None = None
    structure_lookback_bars: int = 1
    structure_buffer_r: float | None = None
    second_stage_trigger_r: float | None = None
    second_stage_lock_r: float | None = None
    second_stage_structure_buffer_r: float | None = None
    second_stage_lookback_bars: int = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-gc-post-promotion-stop-study")
    parser.add_argument(
        "--scope-bundle-manifest",
        default=str(DEFAULT_SCOPE_BUNDLE_MANIFEST),
        help="Path to the GC ATP scope-bundle manifest to replay.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for the GC post-promotion stop study.",
    )
    parser.add_argument(
        "--point-value",
        type=float,
        default=DEFAULT_POINT_VALUE,
        help="Optional point-value override. Defaults to GC $100/point.",
    )
    return parser


def run_gc_post_promotion_stop_study(
    *,
    scope_bundle_manifest: Path,
    output_dir: Path,
    point_value: float = DEFAULT_POINT_VALUE,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_scope_replay_probe_bundle(scope_bundle_manifest)
    bars_1m = _load_bars_for_intervals(
        sqlite_path=Path(bundle.manifest["source_db"]),
        symbol=str(bundle.manifest["symbol"]).upper(),
        intervals=_merged_replay_intervals(
            timing_states=bundle.timing_states,
            window_minutes=480,
        ),
    )
    baseline_trades, baseline_sizing_summary = _apply_confirmation_sizing_profile(
        trades=bundle.trade_records,
        bars_1m=bars_1m,
        point_value=point_value,
        profile=_baseline_confirmation_profile(),
    )
    baseline_metrics = _variant_metrics(baseline_trades, point_value=point_value)

    variants = _variant_specs()
    results: list[dict[str, Any]] = []
    variant_trade_map: dict[str, list[TradeRecord]] = {}

    for variant in variants:
        replayed = _replay_trade_records_under_promoted_stop_variant(
            trades=bundle.trade_records,
            bars_1m=bars_1m,
            point_value=point_value,
            variant=variant,
        )
        sized_trades, sizing_summary = _apply_confirmation_sizing_profile(
            trades=replayed,
            bars_1m=bars_1m,
            point_value=point_value,
            profile=_baseline_confirmation_profile(),
        )
        variant_trade_map[variant.variant_id] = sized_trades
        metrics = _variant_metrics(sized_trades, point_value=point_value)
        results.append(
            {
                "variant_id": variant.variant_id,
                "label": variant.label,
                "notes": variant.notes,
                "stop_definition": {
                    "stop_mode": variant.stop_mode,
                    "initial_lock_r": variant.initial_lock_r,
                    "update_basis": variant.update_basis,
                    "structure_timeframe_minutes": variant.structure_timeframe_minutes,
                    "structure_lookback_bars": variant.structure_lookback_bars,
                    "structure_buffer_r": variant.structure_buffer_r,
                    "second_stage_trigger_r": variant.second_stage_trigger_r,
                    "second_stage_lock_r": variant.second_stage_lock_r,
                    "second_stage_structure_buffer_r": variant.second_stage_structure_buffer_r,
                    "second_stage_lookback_bars": variant.second_stage_lookback_bars,
                },
                "is_control": variant.is_control,
                "probe_summary": {
                    "trade_source": "bundle_trade_records_replayed_checkpoint100_15m_with_stop_variant",
                    "bars_loaded": len(bars_1m),
                    "merged_intervals": len(
                        _merged_replay_intervals(
                            timing_states=bundle.timing_states,
                            window_minutes=480,
                        )
                    ),
                    "confirmation_add_trade_count": sizing_summary["confirmation_add_trade_count"],
                    "confirmation_add_net_pnl": sizing_summary["confirmation_add_net_pnl"],
                },
                "metrics": metrics,
                "comparison_vs_baseline": _comparison_metrics(
                    baseline=baseline_metrics,
                    candidate=metrics,
                ),
            }
        )

    control_result = next(result for result in results if result["is_control"])
    control_trades = variant_trade_map[control_result["variant_id"]]
    for result in results:
        result["comparison_vs_control"] = _comparison_metrics(
            baseline=control_result["metrics"],
            candidate=result["metrics"],
        )
        if not result["is_control"]:
            result["event_level_insight"] = _event_level_insight(
                control_trades=control_trades,
                candidate_trades=variant_trade_map[result["variant_id"]],
            )

    best_variant = _best_variant(results)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study": "atp_gc_post_promotion_stop_study",
        "baseline_reference": {
            "strategy_id": BASELINE_REFERENCE_ID,
            "label": BASELINE_REFERENCE_LABEL,
            "scope_bundle_manifest": str(scope_bundle_manifest.resolve()),
            "confirmation_add_candidate_id": STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
            "probe_size_fraction": 1.0,
            "confirmation_add_size_fraction": 1.0,
        },
        "fixed_base_architecture": {
            "control_variant_id": CONTROL_VARIANT_ID,
            "fixed_exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
            "fixed_exit_policy_label": "checkpoint100_no_traction_abort_15m_2of3",
            "checkpoint_r_multiple": 1.0,
            "no_traction_abort_retained": True,
            "maintenance_timeframe": "15m",
            "maintenance_rule": "2-of-3 deterioration on completed bars",
            "gc_only": True,
        },
        "implementation_summary": {
            "files_touched": [
                "src/mgc_v05l/app/atp_gc_post_promotion_stop_study.py",
                "src/mgc_v05l/app/main.py",
                "tests/unit/test_mgc_v05l_atp_gc_post_promotion_stop_study.py",
            ],
            "promoted_stop_definition_basis": [
                "Control keeps the current post-promotion locked-profit floor.",
                "Looser v1 reduces the locked-profit floor after promotion.",
                "Two-stage begins with a controlled post-promotion floor and then explicitly widens into a looser structure-plus-buffer runner stop after further extension.",
                "Structure-buffer variants trail against completed 5m structure plus an explicit R-based buffer.",
            ],
            "add_size_state_affected_stop_logic": False,
            "assumptions": [
                "Only long-side promoted GC trades are replayed under the new post-promotion stop geometry; short trades remain unchanged from the current reference path.",
                "The add-size overlay is applied after exit replay so the stop study isolates post-promotion geometry rather than changing add eligibility.",
                "Completed 5m structure bars are used for structure-based promoted stops.",
                f"Material rescue / harm threshold is ${MATERIAL_RESCUE_DELTA_CASH:.0f}.",
            ],
        },
        "baseline_metrics": baseline_metrics,
        "baseline_probe_summary": {
            "trade_source": "bundle_trade_records_with_confirmation_add_overlay",
            "bars_loaded": len(bars_1m),
            "confirmation_add_trade_count": baseline_sizing_summary["confirmation_add_trade_count"],
            "confirmation_add_net_pnl": baseline_sizing_summary["confirmation_add_net_pnl"],
        },
        "tested_variants": [
            {
                "variant_id": row["variant_id"],
                "label": row["label"],
            }
            for row in results
        ],
        "results": results,
        "best_variant": {
            "variant_id": best_variant["variant_id"],
            "label": best_variant["label"],
            "metrics": best_variant["metrics"],
            "comparison_vs_control": best_variant["comparison_vs_control"],
            "comparison_vs_baseline": best_variant["comparison_vs_baseline"],
        },
        "judgment": _judgment(
            baseline_metrics=baseline_metrics,
            control_result=control_result,
            results=results,
            best_variant=best_variant,
        ),
    }
    json_path = output_dir / "atp_gc_post_promotion_stop_study.json"
    markdown_path = output_dir / "atp_gc_post_promotion_stop_study.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_gc_post_promotion_stop_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _baseline_confirmation_profile():
    from .atp_scope_replay_probe import ConfirmationSizingProfile, _candidate_by_id

    return ConfirmationSizingProfile(
        probe_size_fraction=1.0,
        confirmation_add_size_fraction=1.0,
        confirmation_add_candidate=_candidate_by_id(STRONGEST_PROMOTION_ADD_CANDIDATE_ID),
    )


def _variant_specs() -> list[PromotedStopVariantSpec]:
    return [
        PromotedStopVariantSpec(
            variant_id=CONTROL_VARIANT_ID,
            label="Control: checkpoint100 + 15m 2-of-3",
            notes="Unchanged best current candidate from the prior pass.",
            stop_mode="locked_floor",
            initial_lock_r=0.35,
            update_basis="continuous",
            is_control=True,
        ),
        PromotedStopVariantSpec(
            variant_id="checkpoint100_15m_2of3_looser_post_promo_stop_v1",
            label="Looser post-promo stop v1",
            notes="Moderately looser promoted floor after checkpoint.",
            stop_mode="locked_floor",
            initial_lock_r=0.20,
            update_basis="continuous",
        ),
        PromotedStopVariantSpec(
            variant_id="checkpoint100_15m_2of3_two_stage_post_promo_stop_v1",
            label="Two-stage post-promo stop v1",
            notes="Start controlled after promotion, then widen into a looser runner stop after further extension.",
            stop_mode="two_stage",
            initial_lock_r=0.25,
            update_basis="continuous then completed_5m",
            structure_timeframe_minutes=5,
            second_stage_trigger_r=1.75,
            second_stage_lock_r=0.10,
            second_stage_structure_buffer_r=0.20,
            second_stage_lookback_bars=1,
        ),
        PromotedStopVariantSpec(
            variant_id="checkpoint100_15m_2of3_structure_buffer_post_promo_v1",
            label="Structure-plus-buffer post-promo v1",
            notes="Trail against completed 5m structure plus an explicit R-based buffer.",
            stop_mode="structure_buffer",
            initial_lock_r=0.15,
            update_basis="completed_5m",
            structure_timeframe_minutes=5,
            structure_lookback_bars=1,
            structure_buffer_r=0.20,
        ),
        PromotedStopVariantSpec(
            variant_id="checkpoint100_15m_2of3_looser_post_promo_stop_v2",
            label="Looser post-promo stop v2",
            notes="Widest defensible promoted runner stop in the test family.",
            stop_mode="structure_buffer",
            initial_lock_r=0.05,
            update_basis="completed_5m",
            structure_timeframe_minutes=5,
            structure_lookback_bars=2,
            structure_buffer_r=0.35,
        ),
    ]


def _replay_trade_records_under_promoted_stop_variant(
    *,
    trades: Sequence[TradeRecord],
    bars_1m: Sequence[ResearchBar],
    point_value: float,
    variant: PromotedStopVariantSpec,
) -> list[TradeRecord]:
    policy_profile = _replay_exit_policy_profile(ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3)
    bars_by_instrument: dict[str, list[ResearchBar]] = {}
    minute_end_ts_by_instrument: dict[str, list[datetime]] = {}
    maintenance_cache: dict[tuple[str, int], dict[str, Any]] = {}
    structure_cache: dict[tuple[str, int], dict[str, Any]] = {}
    for bar in sorted(bars_1m, key=lambda item: (item.instrument, item.end_ts)):
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)
    for instrument, rows in bars_by_instrument.items():
        minute_end_ts_by_instrument[instrument] = [bar.end_ts for bar in rows]
        maintenance_cache[(instrument, int(policy_profile["maintenance_timeframe_minutes"]))] = (
            _completed_structure_context(
                minute_bars=rows,
                timeframe_minutes=int(policy_profile["maintenance_timeframe_minutes"]),
            )
        )
        if variant.structure_timeframe_minutes is not None:
            structure_cache[(instrument, int(variant.structure_timeframe_minutes))] = _completed_structure_context(
                minute_bars=rows,
                timeframe_minutes=int(variant.structure_timeframe_minutes),
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
        maintenance_state = maintenance_cache[(trade.instrument, int(policy_profile["maintenance_timeframe_minutes"]))]
        structure_state = (
            structure_cache.get((trade.instrument, int(variant.structure_timeframe_minutes)))
            if variant.structure_timeframe_minutes is not None
            else None
        )
        stage2_activated = False
        promoted_stop_stage = "pre_promotion"

        for relative_index, bar in enumerate(execution_window, start=1):
            bars_held = relative_index
            mfe_points = max(mfe_points, float(bar.high) - float(trade.entry_price))
            mae_points = max(mae_points, float(trade.entry_price) - float(bar.low))
            stop_hit = float(bar.low) <= dynamic_stop_price
            promotion_hit = (
                not checkpoint_reached
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
                dynamic_stop_price = float(trade.entry_price) + risk_points * float(variant.initial_lock_r)
                promoted_stop_stage = "promoted_initial"
            if checkpoint_reached:
                dynamic_stop_price, stage2_activated, promoted_stop_stage = _advance_promoted_stop(
                    variant=variant,
                    current_stop_price=dynamic_stop_price,
                    entry_price=float(trade.entry_price),
                    risk_points=risk_points,
                    mfe_points=mfe_points,
                    current_bar_end_ts=bar.end_ts,
                    promotion_ts=promotion_ts,
                    structure_state=structure_state,
                    stage2_activated=stage2_activated,
                    current_stage=promoted_stop_stage,
                )
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
            if checkpoint_reached:
                if (
                    maintenance_bar := maintenance_state["bars_by_end_ts"].get(bar.end_ts)
                ) is not None and bar.end_ts != last_completed_maintenance_bar_end_ts and promotion_ts is not None and maintenance_bar.end_ts >= promotion_ts:
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
                promotion_trigger_r_multiple=float(policy_profile["promotion_r_multiple"]) if checkpoint_reached else None,
                promotion_ts=promotion_ts,
                promotion_price=promotion_price,
                pnl_points_at_promotion=pnl_points_at_promotion,
                post_promotion_peak_open_profit_points=post_promotion_peak_open_profit_points,
            )
        )
    return replayed


def _completed_structure_context(*, minute_bars: Sequence[ResearchBar], timeframe_minutes: int) -> dict[str, Any]:
    context = _build_completed_maintenance_context(
        minute_bars=minute_bars,
        timeframe_minutes=timeframe_minutes,
    )
    context["index_by_end_ts"] = {
        bar.end_ts: index
        for index, bar in enumerate(context["bars"])
    }
    return context


def _advance_promoted_stop(
    *,
    variant: PromotedStopVariantSpec,
    current_stop_price: float,
    entry_price: float,
    risk_points: float,
    mfe_points: float,
    current_bar_end_ts: datetime,
    promotion_ts: datetime | None,
    structure_state: dict[str, Any] | None,
    stage2_activated: bool,
    current_stage: str,
) -> tuple[float, bool, str]:
    if promotion_ts is None:
        return current_stop_price, stage2_activated, current_stage
    if variant.stop_mode == "locked_floor":
        candidate = entry_price + risk_points * float(variant.initial_lock_r)
        return max(current_stop_price, candidate), stage2_activated, "locked_floor"
    if variant.stop_mode == "structure_buffer":
        candidate = _structure_buffer_candidate(
            entry_price=entry_price,
            risk_points=risk_points,
            min_lock_r=float(variant.initial_lock_r),
            current_bar_end_ts=current_bar_end_ts,
            promotion_ts=promotion_ts,
            structure_state=structure_state,
            lookback_bars=int(variant.structure_lookback_bars),
            buffer_r=float(variant.structure_buffer_r or 0.0),
        )
        return max(current_stop_price, candidate), stage2_activated, "structure_buffer"
    if variant.stop_mode == "two_stage":
        if not stage2_activated and mfe_points >= risk_points * float(variant.second_stage_trigger_r or 0.0):
            candidate = _structure_buffer_candidate(
                entry_price=entry_price,
                risk_points=risk_points,
                min_lock_r=float(variant.second_stage_lock_r or 0.0),
                current_bar_end_ts=current_bar_end_ts,
                promotion_ts=promotion_ts,
                structure_state=structure_state,
                lookback_bars=int(variant.second_stage_lookback_bars),
                buffer_r=float(variant.second_stage_structure_buffer_r or 0.0),
            )
            return min(current_stop_price, candidate), True, "two_stage_runner"
        if stage2_activated:
            candidate = _structure_buffer_candidate(
                entry_price=entry_price,
                risk_points=risk_points,
                min_lock_r=float(variant.second_stage_lock_r or 0.0),
                current_bar_end_ts=current_bar_end_ts,
                promotion_ts=promotion_ts,
                structure_state=structure_state,
                lookback_bars=int(variant.second_stage_lookback_bars),
                buffer_r=float(variant.second_stage_structure_buffer_r or 0.0),
            )
            return max(current_stop_price, candidate), True, "two_stage_runner"
        candidate = entry_price + risk_points * float(variant.initial_lock_r)
        return max(current_stop_price, candidate), False, "two_stage_controlled"
    return current_stop_price, stage2_activated, current_stage


def _structure_buffer_candidate(
    *,
    entry_price: float,
    risk_points: float,
    min_lock_r: float,
    current_bar_end_ts: datetime,
    promotion_ts: datetime,
    structure_state: dict[str, Any] | None,
    lookback_bars: int,
    buffer_r: float,
) -> float:
    floor_price = entry_price + risk_points * min_lock_r
    if structure_state is None:
        return floor_price
    index = structure_state["index_by_end_ts"].get(current_bar_end_ts)
    if index is None:
        return floor_price
    completed_bars = [
        bar
        for bar in structure_state["bars"][: index + 1]
        if bar.end_ts >= promotion_ts
    ]
    if not completed_bars:
        return floor_price
    selected = completed_bars[-max(int(lookback_bars), 1) :]
    anchor_low = min(float(bar.low) for bar in selected)
    return max(floor_price, anchor_low - risk_points * buffer_r)


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


def _variant_metrics(trades: Sequence[TradeRecord], *, point_value: float) -> dict[str, Any]:
    performance = summarize_performance(trades)
    promoted = [trade for trade in trades if trade.participation_promoted]
    non_promoted = [trade for trade in trades if not trade.participation_promoted]
    promoted_pnl = sum(float(trade.pnl_cash) for trade in promoted)
    total_pnl = sum(float(trade.pnl_cash) for trade in trades)
    promoted_winners = [trade for trade in promoted if float(trade.pnl_cash) > 0.0]
    promoted_structural_stop_exits = [trade for trade in promoted if str(trade.exit_reason) == "checkpoint_stop"]
    promoted_maintenance_exits = [trade for trade in promoted if str(trade.exit_reason).startswith("htf_")]
    return {
        "trade_count": performance.trade_count,
        "net_pnl_cash": round(float(performance.net_pnl_cash), 6),
        "max_drawdown": round(float(performance.max_drawdown), 6),
        "profit_factor": round(float(performance.profit_factor), 6),
        "win_rate": round(float(performance.win_rate), 6),
        "expectancy": round(float(performance.expectancy), 6),
        "average_hold_minutes": round(float(performance.avg_hold_minutes), 6),
        "median_hold_minutes": round(_median(float(trade.hold_minutes) for trade in trades), 6),
        "trade_count_promoted": len(promoted),
        "promoted_trade_count": len(promoted),
        "promoted_trade_net_pnl_cash": round(promoted_pnl, 6),
        "promoted_trade_expectancy": round(_mean(float(trade.pnl_cash) for trade in promoted), 6),
        "promoted_trade_profit_factor": round(_profit_factor(promoted), 6),
        "promoted_trade_average_hold_minutes": round(_mean(float(trade.hold_minutes) for trade in promoted), 6),
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
                max(float(trade.post_promotion_peak_open_profit_points or 0.0) - float(trade.pnl_points), 0.0) * point_value
                for trade in promoted
            ),
            6,
        ),
        "share_promoted": round(len(promoted) / max(len(trades), 1), 6),
        "promoted_trade_metrics": _subset_metrics(promoted),
        "non_promoted_trade_metrics": _subset_metrics(non_promoted),
        "promoted_trade_pnl_contribution_cash": round(promoted_pnl, 6),
        "promoted_trade_pnl_contribution_share": round(promoted_pnl / total_pnl, 6) if total_pnl else 0.0,
        "promoted_structural_stop_exit_count": len(promoted_structural_stop_exits),
        "promoted_maintenance_exit_count": len(promoted_maintenance_exits),
        "would_be_larger_winner_candidate_count": len(promoted_winners),
        "exit_reason_counts": dict(sorted(Counter(str(trade.exit_reason) for trade in trades).items())),
        "top_winners": _winner_distribution(trades),
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


def _winner_distribution(trades: Sequence[TradeRecord]) -> dict[str, float]:
    winner_pnls = sorted((float(trade.pnl_cash) for trade in trades if float(trade.pnl_cash) > 0.0), reverse=True)
    top_decile_count = max(int(len(winner_pnls) * 0.10), 1) if winner_pnls else 0
    return {
        "largest_winner_cash": round(winner_pnls[0], 6) if winner_pnls else 0.0,
        "top5_winner_sum_cash": round(sum(winner_pnls[:5]), 6),
        "top10_winner_sum_cash": round(sum(winner_pnls[:10]), 6),
        "top_decile_winner_sum_cash": round(sum(winner_pnls[:top_decile_count]), 6),
        "top_decile_winner_mean_cash": round(_mean(winner_pnls[:top_decile_count]), 6),
    }


def _comparison_metrics(*, baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline_top = baseline.get("top_winners") or {}
    candidate_top = candidate.get("top_winners") or {}
    return {
        "net_pnl_cash_delta": round(float(candidate["net_pnl_cash"]) - float(baseline["net_pnl_cash"]), 6),
        "max_drawdown_delta": round(float(candidate["max_drawdown"]) - float(baseline["max_drawdown"]), 6),
        "profit_factor_delta": round(float(candidate["profit_factor"]) - float(baseline["profit_factor"]), 6),
        "expectancy_delta": round(float(candidate["expectancy"]) - float(baseline["expectancy"]), 6),
        "average_hold_minutes_delta": round(
            float(candidate["average_hold_minutes"]) - float(baseline["average_hold_minutes"]),
            6,
        ),
        "promoted_trade_net_pnl_delta": round(
            float(candidate["promoted_trade_net_pnl_cash"]) - float(baseline["promoted_trade_net_pnl_cash"]),
            6,
        ),
        "average_additional_pnl_after_checkpoint_delta": round(
            float(candidate["average_additional_pnl_after_checkpoint_cash"])
            - float(baseline["average_additional_pnl_after_checkpoint_cash"]),
            6,
        ),
        "average_giveback_after_checkpoint_delta": round(
            float(candidate["average_giveback_after_checkpoint_cash"])
            - float(baseline["average_giveback_after_checkpoint_cash"]),
            6,
        ),
        "promoted_structural_stop_exit_count_delta": (
            int(candidate["promoted_structural_stop_exit_count"])
            - int(baseline["promoted_structural_stop_exit_count"])
        ),
        "promoted_maintenance_exit_count_delta": (
            int(candidate["promoted_maintenance_exit_count"])
            - int(baseline["promoted_maintenance_exit_count"])
        ),
        "top5_winner_sum_cash_delta": round(
            float(candidate_top.get("top5_winner_sum_cash", 0.0)) - float(baseline_top.get("top5_winner_sum_cash", 0.0)),
            6,
        ),
        "top10_winner_sum_cash_delta": round(
            float(candidate_top.get("top10_winner_sum_cash", 0.0)) - float(baseline_top.get("top10_winner_sum_cash", 0.0)),
            6,
        ),
        "top_decile_winner_mean_cash_delta": round(
            float(candidate_top.get("top_decile_winner_mean_cash", 0.0))
            - float(baseline_top.get("top_decile_winner_mean_cash", 0.0)),
            6,
        ),
    }


def _event_level_insight(
    *,
    control_trades: Sequence[TradeRecord],
    candidate_trades: Sequence[TradeRecord],
) -> dict[str, Any]:
    control_by_id = {trade.decision_id: trade for trade in control_trades}
    candidate_by_id = {trade.decision_id: trade for trade in candidate_trades}
    materially_rescued: list[dict[str, Any]] = []
    materially_harmed: list[dict[str, Any]] = []
    for decision_id, control in control_by_id.items():
        candidate = candidate_by_id.get(decision_id)
        if candidate is None:
            continue
        pnl_delta = float(candidate.pnl_cash) - float(control.pnl_cash)
        row = {
            "decision_id": decision_id,
            "entry_ts": control.entry_ts.isoformat(),
            "control_exit_reason": control.exit_reason,
            "candidate_exit_reason": candidate.exit_reason,
            "control_pnl_cash": round(float(control.pnl_cash), 6),
            "candidate_pnl_cash": round(float(candidate.pnl_cash), 6),
            "pnl_delta_cash": round(pnl_delta, 6),
            "control_hold_minutes": round(float(control.hold_minutes), 6),
            "candidate_hold_minutes": round(float(candidate.hold_minutes), 6),
        }
        if (
            control.participation_promoted
            and candidate.participation_promoted
            and pnl_delta >= MATERIAL_RESCUE_DELTA_CASH
            and float(candidate.pnl_cash) > 0.0
        ):
            materially_rescued.append(row)
        if (
            control.participation_promoted
            and candidate.participation_promoted
            and pnl_delta <= -MATERIAL_RESCUE_DELTA_CASH
        ):
            materially_harmed.append(row)
    materially_rescued.sort(key=lambda item: item["pnl_delta_cash"], reverse=True)
    materially_harmed.sort(key=lambda item: item["pnl_delta_cash"])
    rescue_patterns = Counter(
        f"{row['control_exit_reason']}->{row['candidate_exit_reason']}"
        for row in materially_rescued
    )
    return {
        "material_rescued_winner_count": len(materially_rescued),
        "material_harm_count": len(materially_harmed),
        "rescued_exit_reason_patterns": dict(rescue_patterns.most_common(6)),
        "representative_rescued_trades": materially_rescued[:5],
        "representative_harmed_trades": materially_harmed[:5],
    }


def _best_variant(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    candidates = [row for row in results if not row["is_control"]]
    return max(
        candidates,
        key=lambda row: (
            float(row["metrics"]["net_pnl_cash"]),
            float(row["metrics"]["profit_factor"]),
            -float(row["metrics"]["max_drawdown"]),
        ),
    )


def _judgment(
    *,
    baseline_metrics: dict[str, Any],
    control_result: dict[str, Any],
    results: Sequence[dict[str, Any]],
    best_variant: dict[str, Any],
) -> dict[str, Any]:
    best_vs_control = best_variant["comparison_vs_control"]
    best_vs_baseline = best_variant["comparison_vs_baseline"]
    rescued_count = int((best_variant.get("event_level_insight") or {}).get("material_rescued_winner_count", 0))
    harmed_count = int((best_variant.get("event_level_insight") or {}).get("material_harm_count", 0))
    looseness_materially_helped = (
        float(best_vs_control["net_pnl_cash_delta"]) > 0.0
        and float(best_vs_control["top10_winner_sum_cash_delta"]) > 0.0
    )
    current_stop_too_tight = (
        int(best_vs_control["promoted_structural_stop_exit_count_delta"]) < 0
        and rescued_count > harmed_count
    )
    branch_alive = (
        looseness_materially_helped
        and float(best_vs_baseline["net_pnl_cash_delta"]) > -50000.0
    )
    return {
        "best_variant_id": best_variant["variant_id"],
        "did_any_looser_stop_materially_close_gap_vs_baseline": {
            "answer": "yes" if float(best_vs_baseline["net_pnl_cash_delta"]) > -75000.0 else "no",
            "net_pnl_gap_vs_baseline_cash": float(best_vs_baseline["net_pnl_cash_delta"]),
            "expectancy_gap_vs_baseline": float(best_vs_baseline["expectancy_delta"]),
        },
        "is_current_promoted_stop_hierarchy_too_tight": {
            "answer": "yes" if current_stop_too_tight else "no",
            "reason": (
                "The best looser-stop variant reduced promoted structural stop exits and rescued more promoted winners than it harmed."
                if current_stop_too_tight
                else "The looser-stop family did not show enough repeatable rescue versus harm to call the current promoted stop clearly too tight."
            ),
        },
        "are_rescued_winners_real_and_repeatable": {
            "answer": "yes" if rescued_count >= 10 and rescued_count > harmed_count else "no",
            "rescued_count": rescued_count,
            "harmed_count": harmed_count,
        },
        "does_the_branch_still_look_alive": {
            "answer": "yes" if branch_alive else "no",
            "reason": (
                "Looser promoted-stop geometry improved the control and clawed back a meaningful chunk of the gap versus baseline."
                if branch_alive
                else "Stop looseness may help at the margin, but the gap to baseline remains large enough that this branch still looks constrained."
            ),
        },
        "remaining_bottleneck": _remaining_bottleneck(
            baseline_metrics=baseline_metrics,
            control_result=control_result,
            best_variant=best_variant,
        ),
    }


def _remaining_bottleneck(
    *,
    baseline_metrics: dict[str, Any],
    control_result: dict[str, Any],
    best_variant: dict[str, Any],
) -> str:
    best_vs_control = best_variant["comparison_vs_control"]
    best_vs_baseline = best_variant["comparison_vs_baseline"]
    if float(best_vs_control["net_pnl_cash_delta"]) <= 0.0:
        return "post-promotion stop geometry is likely not the dominant remaining bottleneck"
    if float(best_vs_baseline["net_pnl_cash_delta"]) < -75000.0:
        return "baseline still monetizes the edge better overall; remaining gap likely extends beyond stop geometry alone"
    return "post-promotion stop geometry still looks like the next GC-only refinement lane"


def render_gc_post_promotion_stop_markdown(payload: dict[str, Any]) -> str:
    control = next(row for row in payload["results"] if row["is_control"])
    best = payload["best_variant"]
    judgment = payload["judgment"]
    lines = [
        "# ATP GC Post-Promotion Stop Study",
        "",
        f"1. control used: `{control['variant_id']}`",
        "2. tested post-promotion stop variants: " + ", ".join(f"`{row['variant_id']}`" for row in payload["results"]),
        f"3. best-performing variant: `{best['variant_id']}`",
        f"4. whether promoted-stop looseness materially helped: `{judgment['is_current_promoted_stop_hierarchy_too_tight']['answer']}`",
        f"5. whether the branch still looks alive: `{judgment['does_the_branch_still_look_alive']['answer']}`",
        "",
        "## Variant Comparison",
        "",
        "| Variant | Net P&L | Max DD | PF | Win rate | Expectancy | Avg hold | Median hold | Promoted count | Promoted stop exits | HTF exits | Avg addl after promo | Avg giveback after promo |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["results"]:
        metrics = row["metrics"]
        lines.append(
            "| "
            + " | ".join(
                [
                    row["variant_id"],
                    f"{metrics['net_pnl_cash']:.2f}",
                    f"{metrics['max_drawdown']:.2f}",
                    f"{metrics['profit_factor']:.3f}",
                    f"{metrics['win_rate']:.3f}",
                    f"{metrics['expectancy']:.2f}",
                    f"{metrics['average_hold_minutes']:.2f}",
                    f"{metrics['median_hold_minutes']:.2f}",
                    str(metrics["promoted_trade_count"]),
                    str(metrics["promoted_structural_stop_exit_count"]),
                    str(metrics["promoted_maintenance_exit_count"]),
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
            f"- Best variant: `{best['variant_id']}`",
            f"- Gap vs baseline: `{best['comparison_vs_baseline']['net_pnl_cash_delta']:.2f}` net P&L, `{best['comparison_vs_baseline']['expectancy_delta']:.2f}` expectancy",
            f"- Gap vs control: `{best['comparison_vs_control']['net_pnl_cash_delta']:.2f}` net P&L, `{best['comparison_vs_control']['top10_winner_sum_cash_delta']:.2f}` top-10 winner sum",
            f"- Remaining bottleneck: {judgment['remaining_bottleneck']}",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _profit_factor(trades: Sequence[TradeRecord]) -> float:
    gross_wins = sum(max(float(trade.pnl_cash), 0.0) for trade in trades)
    gross_losses = abs(sum(min(float(trade.pnl_cash), 0.0) for trade in trades))
    if gross_losses <= 0.0:
        return gross_wins if gross_wins > 0.0 else 0.0
    return gross_wins / gross_losses


def _mean(values: Sequence[float] | Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(sum(items) / len(items))


def _median(values: Sequence[float] | Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(median(items))
