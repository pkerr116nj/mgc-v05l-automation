"""ATP v1 Phase 3 replay/paper timing bridge using 1m bars."""

from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Sequence

from .models import AtpEntryState, AtpTimingState, ConflictOutcome, PatternVariant, TradeRecord
from .phase2_continuation import (
    ATP_V1_LONG_CONTINUATION_FAMILY,
    ATP_V1_LONG_CONTINUATION_VARIANT_ID,
    ENTRY_ELIGIBLE,
    atp_phase2_variant,
)
from .state_layers import LONG_BIAS, rolling_ema

ATP_TIMING_WAITING = "ATP_TIMING_WAITING"
ATP_TIMING_CONFIRMED = "ATP_TIMING_CONFIRMED"
ATP_TIMING_CHASE_RISK = "ATP_TIMING_CHASE_RISK"
ATP_TIMING_INVALIDATED = "ATP_TIMING_INVALIDATED"
ATP_TIMING_UNAVAILABLE = "ATP_TIMING_UNAVAILABLE"

ATP_TIMING_ACTIVATION_COMPLETED_5M = "completed_5m_close"
ATP_TIMING_ACTIVATION_ROLLING_5M = "rolling_5m_on_1m"

VWAP_FAVORABLE = "VWAP_FAVORABLE"
VWAP_NEUTRAL = "VWAP_NEUTRAL"
VWAP_CHASE_RISK = "VWAP_CHASE_RISK"

ATP_TIMING_5M_CONTEXT_NOT_READY = "ATP_TIMING_5M_CONTEXT_NOT_READY"
ATP_TIMING_NO_1M_WINDOW = "ATP_TIMING_NO_1M_WINDOW"
ATP_TIMING_CONFIRMATION_NOT_REACHED = "ATP_TIMING_CONFIRMATION_NOT_REACHED"
ATP_TIMING_VWAP_CHASE_RISK = "ATP_TIMING_VWAP_CHASE_RISK"
ATP_TIMING_INVALIDATED_BEFORE_ENTRY = "ATP_TIMING_INVALIDATED_BEFORE_ENTRY"
ATP_TIMING_LONDON_DISABLED = "ATP_TIMING_LONDON_DISABLED"

MINUTE_FAST_EMA_SPAN = 5
TIMING_NEUTRAL_VWAP_BAND = 0.10


def build_phase3_replay_package(
    *,
    entry_states: Sequence[AtpEntryState],
    bars_1m: Sequence[Any],
    point_value: float,
    old_proxy_trade_count: int,
    entry_activation_basis: str = ATP_TIMING_ACTIVATION_COMPLETED_5M,
) -> dict[str, Any]:
    initial_timing_states = classify_timing_states(
        entry_states=entry_states,
        bars_1m=bars_1m,
        entry_activation_basis=entry_activation_basis,
    )
    initial_trades = simulate_timed_entries(
        timing_states=initial_timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        variant=atp_phase2_variant(),
    )
    timing_states = overlay_position_blocks(timing_states=initial_timing_states, trades=initial_trades)
    trades = simulate_timed_entries(
        timing_states=timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        variant=atp_phase2_variant(),
    )
    diagnostics = summarize_phase3_timing_diagnostics(
        timing_states=timing_states,
        trades=trades,
        old_proxy_trade_count=old_proxy_trade_count,
    )
    return {
        "timing_states": timing_states,
        "shadow_trades": trades,
        "diagnostics": diagnostics,
    }


def classify_timing_states(
    *,
    entry_states: Sequence[AtpEntryState],
    bars_1m: Sequence[Any],
    entry_activation_basis: str = ATP_TIMING_ACTIVATION_COMPLETED_5M,
) -> list[AtpTimingState]:
    minute_bars = sorted(bars_1m, key=lambda bar: (bar.instrument, bar.end_ts))
    bars_by_instrument: dict[str, list[Any]] = {}
    for bar in minute_bars:
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)
    timing_states: list[AtpTimingState] = []
    for state in entry_states:
        instrument_bars = bars_by_instrument.get(state.instrument, [])
        if state.entry_state != ENTRY_ELIGIBLE:
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_5M_CONTEXT_NOT_READY,),
                    primary_blocker=ATP_TIMING_5M_CONTEXT_NOT_READY,
                    feature_snapshot={"timing_activation": "blocked_by_5m_context"},
                )
            )
            continue
        if state.session_segment == "LONDON":
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_LONDON_DISABLED,),
                    primary_blocker=ATP_TIMING_LONDON_DISABLED,
                    feature_snapshot={"timing_activation": "london_disabled"},
                )
            )
            continue
        candidate_window = _timing_window_bars(
            state=state,
            minute_bars=instrument_bars,
            entry_activation_basis=entry_activation_basis,
        )
        if not candidate_window:
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_NO_1M_WINDOW,),
                    primary_blocker=ATP_TIMING_NO_1M_WINDOW,
                    feature_snapshot={"timing_activation": "no_1m_window"},
                )
            )
            continue
        minute_ema_by_ts = _minute_fast_ema_map(candidate_window)
        pivot_price = _pivot_price(state)
        invalidation_price = _invalidation_price(state)
        saw_confirmation = False
        saw_chase_risk = False
        last_price_quality = VWAP_NEUTRAL
        last_snapshot: dict[str, Any] = {}

        for index, bar in enumerate(candidate_window):
            previous_bar = candidate_window[index - 1] if index > 0 else None
            bar_vwap = _bar_vwap(bar)
            entry_price = max(float(bar.open), pivot_price)
            price_quality = classify_vwap_price_quality(
                side="LONG",
                entry_price=entry_price,
                bar_vwap=bar_vwap,
                band_reference=max(bar.range_points, float(state.feature_snapshot.get("average_range") or 0.0) / 5.0, 1e-9),
            )
            last_price_quality = price_quality
            minute_fast_ema = minute_ema_by_ts[bar.end_ts]
            confirmation_checks = {
                "pivot_price": round(pivot_price, 6),
                "invalidation_price": round(invalidation_price, 6),
                "bar_vwap": round(bar_vwap, 6),
                "entry_price": round(entry_price, 6),
                "price_quality": price_quality,
                "close_above_pivot": float(bar.close) >= pivot_price,
                "high_breaks_pivot": float(bar.high) >= pivot_price,
                "close_above_minute_fast_ema": float(bar.close) >= minute_fast_ema,
                "positive_reacceleration": (
                    previous_bar is not None
                    and float(bar.close) > float(previous_bar.close)
                    and float(bar.high) >= float(previous_bar.high)
                ),
                "non_violent_against_trend": float(bar.low) > invalidation_price,
            }
            last_snapshot = {
                "timing_bar_ts": bar.end_ts.isoformat(),
                "timing_checks": confirmation_checks,
            }
            if float(bar.low) <= invalidation_price:
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_INVALIDATED,
                        vwap_price_quality_state=price_quality,
                        blocker_codes=(ATP_TIMING_INVALIDATED_BEFORE_ENTRY,),
                        primary_blocker=ATP_TIMING_INVALIDATED_BEFORE_ENTRY,
                        invalidated_before_entry=True,
                        feature_snapshot=last_snapshot,
                    )
                )
                break
            confirmed = all(
                (
                    confirmation_checks["close_above_pivot"],
                    confirmation_checks["high_breaks_pivot"],
                    confirmation_checks["close_above_minute_fast_ema"],
                    confirmation_checks["positive_reacceleration"],
                    confirmation_checks["non_violent_against_trend"],
                )
            )
            if not confirmed:
                continue
            saw_confirmation = True
            if price_quality == VWAP_CHASE_RISK:
                saw_chase_risk = True
                last_snapshot["timing_checks"]["blocked_for_chase_risk"] = True
                continue
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_CONFIRMED,
                    vwap_price_quality_state=price_quality,
                    blocker_codes=(),
                    primary_blocker=None,
                    timing_confirmed=True,
                    executable_entry=True,
                    timing_bar_ts=bar.end_ts,
                    entry_ts=bar.end_ts,
                    entry_price=entry_price,
                    feature_snapshot=last_snapshot,
                )
            )
            break
        else:
            if saw_chase_risk and saw_confirmation:
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_CHASE_RISK,
                        vwap_price_quality_state=VWAP_CHASE_RISK,
                        blocker_codes=(ATP_TIMING_VWAP_CHASE_RISK,),
                        primary_blocker=ATP_TIMING_VWAP_CHASE_RISK,
                        timing_confirmed=True,
                        feature_snapshot=last_snapshot,
                    )
                )
            else:
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_WAITING,
                        vwap_price_quality_state=last_price_quality,
                        blocker_codes=(ATP_TIMING_CONFIRMATION_NOT_REACHED,),
                        primary_blocker=ATP_TIMING_CONFIRMATION_NOT_REACHED,
                        feature_snapshot=last_snapshot,
                    )
                )
    return timing_states


def simulate_timed_entries(
    *,
    timing_states: Sequence[AtpTimingState],
    bars_1m: Sequence[Any],
    point_value: float,
    variant: PatternVariant,
    slippage_points: float = 0.25,
    fee_per_trade: float = 1.50,
) -> list[TradeRecord]:
    minute_bars = sorted(bars_1m, key=lambda bar: (bar.instrument, bar.end_ts))
    bars_by_instrument: dict[str, list[Any]] = {}
    for bar in minute_bars:
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)

    trades: list[TradeRecord] = []
    for state in timing_states:
        if not state.executable_entry or state.entry_ts is None or state.entry_price is None:
            continue
        candidate_bars = bars_by_instrument.get(state.instrument, [])
        minute_end_timestamps = [bar.end_ts for bar in candidate_bars]
        entry_index = bisect_right(minute_end_timestamps, state.entry_ts) - 1
        if entry_index < 0 or entry_index >= len(candidate_bars):
            continue
        entry_bar = candidate_bars[entry_index]
        execution_window = candidate_bars[entry_index : entry_index + variant.max_hold_bars_1m + 1]
        if not execution_window:
            continue
        raw_entry_price = float(state.entry_price)
        entry_price = raw_entry_price + slippage_points
        average_range = max(float(state.feature_snapshot.get("average_range") or 0.25), 0.25)
        decision_bar_low = float(state.feature_snapshot.get("decision_bar_low") or raw_entry_price - average_range)
        risk = max(average_range * variant.stop_atr_multiple, 0.25)
        stop_price = decision_bar_low - risk
        target_price = entry_price + risk * variant.target_r_multiple if variant.target_r_multiple is not None else None

        exit_bar = execution_window[-1]
        raw_exit_price = float(exit_bar.close)
        exit_price = raw_exit_price - slippage_points
        exit_reason = "time_stop"
        mfe_points = 0.0
        mae_points = 0.0
        bars_held = 0

        for relative_index, bar in enumerate(execution_window, start=1):
            bars_held = relative_index
            mfe_points = max(mfe_points, float(bar.high) - raw_entry_price)
            mae_points = max(mae_points, raw_entry_price - float(bar.low))
            stop_hit = float(bar.low) <= stop_price
            target_hit = target_price is not None and float(bar.high) >= target_price
            if stop_hit and target_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price - slippage_points
                exit_reason = "stop_first_conflict"
                break
            if stop_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price - slippage_points
                exit_reason = "stop"
                break
            if target_hit and target_price is not None:
                exit_bar = bar
                raw_exit_price = target_price
                exit_price = target_price - slippage_points
                exit_reason = "target"
                break

        gross_pnl_points = raw_exit_price - raw_entry_price
        pnl_points = exit_price - entry_price
        gross_pnl_cash = gross_pnl_points * point_value
        pnl_cash = pnl_points * point_value - fee_per_trade
        slippage_cost = max(gross_pnl_cash - pnl_points * point_value, 0.0)
        trades.append(
            TradeRecord(
                instrument=state.instrument,
                variant_id=ATP_V1_LONG_CONTINUATION_VARIANT_ID,
                family=ATP_V1_LONG_CONTINUATION_FAMILY,
                side="LONG",
                live_eligible=False,
                shadow_only=True,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                decision_id=f"{state.instrument}|{ATP_V1_LONG_CONTINUATION_VARIANT_ID}|{state.decision_ts.isoformat()}",
                decision_ts=state.decision_ts,
                entry_ts=entry_bar.end_ts,
                exit_ts=exit_bar.end_ts,
                entry_price=entry_price,
                exit_price=exit_price,
                stop_price=stop_price,
                target_price=target_price,
                pnl_points=pnl_points,
                gross_pnl_cash=gross_pnl_cash,
                pnl_cash=pnl_cash,
                fees_paid=fee_per_trade,
                slippage_cost=slippage_cost,
                mfe_points=mfe_points,
                mae_points=mae_points,
                bars_held_1m=bars_held,
                hold_minutes=float(bars_held),
                exit_reason=exit_reason,
                is_reentry=False,
                reentry_type="NONE",
                stopout=exit_reason in {"stop", "stop_first_conflict"},
                setup_signature=str(state.feature_snapshot.get("setup_signature") or ATP_V1_LONG_CONTINUATION_FAMILY),
                setup_quality_bucket=str(state.feature_snapshot.get("setup_quality_bucket") or "MEDIUM"),
                session_segment=state.session_segment,
                regime_bucket=str(state.feature_snapshot.get("regime_bucket") or "UNKNOWN"),
                volatility_bucket=str(state.feature_snapshot.get("volatility_bucket") or "UNKNOWN"),
            )
        )
    return trades


def overlay_position_blocks(
    *,
    timing_states: Sequence[AtpTimingState],
    trades: Sequence[TradeRecord],
) -> list[AtpTimingState]:
    active_windows = sorted((trade.entry_ts, trade.exit_ts) for trade in trades)
    updated: list[AtpTimingState] = []
    for state in timing_states:
        if not state.executable_entry or state.entry_ts is None:
            updated.append(state)
            continue
        overlap = next(
            (
                window
                for window in active_windows
                if window[0] < state.entry_ts < window[1]
            ),
            None,
        )
        if overlap is None:
            updated.append(replace(state, entry_executed=True))
            continue
        blockers = list(state.blocker_codes)
        blockers.extend(code for code in ("ATP_POSITION_NOT_FLAT", "ATP_ONE_POSITION_BASELINE_BLOCK") if code not in blockers)
        updated.append(
            replace(
                state,
                timing_state=ATP_TIMING_UNAVAILABLE,
                blocker_codes=tuple(blockers),
                primary_blocker="ATP_POSITION_NOT_FLAT",
                executable_entry=False,
                entry_executed=False,
                setup_armed_but_not_executable=True,
            )
        )
    return updated


def classify_vwap_price_quality(
    *,
    side: str,
    entry_price: float,
    bar_vwap: float,
    band_reference: float,
) -> str:
    neutral_band = max(float(band_reference), 1e-9) * TIMING_NEUTRAL_VWAP_BAND
    if side == "LONG":
        if entry_price <= bar_vwap:
            return VWAP_FAVORABLE
        if entry_price <= bar_vwap + neutral_band:
            return VWAP_NEUTRAL
        return VWAP_CHASE_RISK
    if entry_price >= bar_vwap:
        return VWAP_FAVORABLE
    if entry_price >= bar_vwap - neutral_band:
        return VWAP_NEUTRAL
    return VWAP_CHASE_RISK


def latest_atp_timing_state_summary(timing_state: AtpTimingState | None) -> dict[str, Any]:
    if timing_state is None:
        return {
            "family_name": ATP_V1_LONG_CONTINUATION_FAMILY,
            "timing_state": ATP_TIMING_UNAVAILABLE,
            "vwap_price_quality_state": VWAP_NEUTRAL,
            "primary_blocker": ATP_TIMING_5M_CONTEXT_NOT_READY,
            "blocker_codes": [ATP_TIMING_5M_CONTEXT_NOT_READY],
            "entry_executed": False,
        }
    return {
        "family_name": timing_state.family_name,
        "context_entry_state": timing_state.context_entry_state,
        "timing_state": timing_state.timing_state,
        "vwap_price_quality_state": timing_state.vwap_price_quality_state,
        "primary_blocker": timing_state.primary_blocker,
        "blocker_codes": list(timing_state.blocker_codes),
        "setup_armed": timing_state.setup_armed,
        "timing_confirmed": timing_state.timing_confirmed,
        "executable_entry": timing_state.executable_entry,
        "setup_armed_but_not_executable": timing_state.setup_armed_but_not_executable,
        "entry_executed": timing_state.entry_executed,
        "timing_bar_ts": timing_state.timing_bar_ts.isoformat() if timing_state.timing_bar_ts else None,
        "entry_ts": timing_state.entry_ts.isoformat() if timing_state.entry_ts else None,
        "entry_price": timing_state.entry_price,
    }


def summarize_phase3_timing_diagnostics(
    *,
    timing_states: Sequence[AtpTimingState],
    trades: Sequence[TradeRecord],
    old_proxy_trade_count: int,
) -> dict[str, Any]:
    total = len(timing_states)
    ready_rows = [state for state in timing_states if state.context_entry_state == ENTRY_ELIGIBLE]
    timing_confirmed_rows = [state for state in ready_rows if state.timing_confirmed]
    executed_rows = [state for state in ready_rows if state.entry_executed]
    vwap_counter = Counter(state.vwap_price_quality_state for state in ready_rows)
    primary_blockers = Counter(state.primary_blocker for state in timing_states if state.primary_blocker)
    session_breakdown: dict[str, Any] = {}
    for session in sorted({state.session_segment for state in timing_states}):
        session_rows = [state for state in timing_states if state.session_segment == session]
        ready_session = [state for state in session_rows if state.context_entry_state == ENTRY_ELIGIBLE]
        confirmed_session = [state for state in ready_session if state.timing_confirmed]
        executed_session = [state for state in ready_session if state.entry_executed]
        session_breakdown[session] = {
            "ready_5m_bars": len(ready_session),
            "timing_confirmed": len(confirmed_session),
            "executed_entries": len(executed_session),
            "ready_to_executed_percent": _percent(len(executed_session), len(ready_session)),
        }
    return {
        "family_name": ATP_V1_LONG_CONTINUATION_FAMILY,
        "timing_bar_count": total,
        "ready_5m_bars_count": len(ready_rows),
        "timing_confirmed_count": len(timing_confirmed_rows),
        "executed_entry_count": len(executed_rows),
        "conversion_rates": {
            "ready_to_timing_confirmed": _percent(len(timing_confirmed_rows), len(ready_rows)),
            "timing_confirmed_to_executed": _percent(len(executed_rows), len(timing_confirmed_rows)),
            "ready_to_executed": _percent(len(executed_rows), len(ready_rows)),
        },
        "timing_state_percent": _percentages(Counter(state.timing_state for state in timing_states), total),
        "vwap_price_quality_percent_on_ready": _percentages(vwap_counter, len(ready_rows)),
        "top_timing_blockers": [
            {"code": code, "count": count, "percent": _percent(count, total)}
            for code, count in primary_blockers.most_common(8)
        ],
        "top_vwap_price_quality_blockers": [
            {"code": code, "count": count}
            for code, count in Counter(
                state.primary_blocker
                for state in timing_states
                if state.primary_blocker == ATP_TIMING_VWAP_CHASE_RISK
            ).most_common(4)
        ],
        "session_breakdown": session_breakdown,
        "old_proxy_comparison": {
            "old_proxy_executed_trade_count": old_proxy_trade_count,
            "phase3_executed_trade_count": len(trades),
            "executed_trade_delta": len(trades) - old_proxy_trade_count,
        },
    }


def render_phase3_timing_diagnostics_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Phase 3 Timing Diagnostics",
        "",
        f"- Ready 5m bars: `{payload.get('ready_5m_bars_count')}`",
        f"- Timing confirmed: `{payload.get('timing_confirmed_count')}`",
        f"- Executed entries: `{payload.get('executed_entry_count')}`",
        f"- Ready -> executed: `{payload.get('conversion_rates', {}).get('ready_to_executed')}`%",
        "",
        "## Top Timing Blockers",
    ]
    blockers = payload.get("top_timing_blockers") or []
    if not blockers:
        lines.append("- None")
    else:
        for row in blockers:
            lines.append(f"- `{row['code']}` count=`{row['count']}` percent=`{row['percent']}`")
    return "\n".join(lines) + "\n"


def write_phase3_artifacts(
    *,
    reports_dir: Path,
    diagnostics: dict[str, Any],
) -> tuple[Path, Path]:
    json_path = reports_dir / "atp_phase3_timing_diagnostics.json"
    markdown_path = reports_dir / "atp_phase3_timing_diagnostics.md"
    json_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_phase3_timing_diagnostics_markdown(diagnostics), encoding="utf-8")
    return json_path, markdown_path


def _timing_window_bars(
    *,
    state: AtpEntryState,
    minute_bars: Sequence[Any],
    entry_activation_basis: str,
) -> list[Any]:
    minute_end_timestamps = [bar.end_ts for bar in minute_bars]
    variant = atp_phase2_variant()
    if entry_activation_basis == ATP_TIMING_ACTIVATION_ROLLING_5M:
        start_index = bisect_left(minute_end_timestamps, state.decision_ts)
        return list(minute_bars[start_index : start_index + variant.entry_window_bars_1m])
    start_index = bisect_right(minute_end_timestamps, state.decision_ts)
    return list(minute_bars[start_index : start_index + variant.entry_window_bars_1m])


def _minute_fast_ema_map(bars: Sequence[Any]) -> dict[Any, float]:
    if not bars:
        return {}
    ema_values = rolling_ema([float(bar.close) for bar in bars], span=MINUTE_FAST_EMA_SPAN)
    return {bar.end_ts: ema_values[index] for index, bar in enumerate(bars)}


def _pivot_price(state: AtpEntryState) -> float:
    setup_high = float(state.feature_snapshot.get("setup_bar_high") or state.feature_snapshot.get("decision_bar_high") or 0.0)
    decision_close = float(state.feature_snapshot.get("decision_bar_close") or setup_high)
    return max(setup_high, decision_close)


def _invalidation_price(state: AtpEntryState) -> float:
    setup_low = float(state.feature_snapshot.get("setup_bar_low") or state.feature_snapshot.get("decision_bar_low") or 0.0)
    decision_low = float(state.feature_snapshot.get("decision_bar_low") or setup_low)
    return min(setup_low, decision_low)


def _bar_vwap(bar: Any) -> float:
    return (float(bar.high) + float(bar.low) + float(bar.close)) / 3.0


def _timing_state(
    *,
    state: AtpEntryState,
    timing_state: str,
    vwap_price_quality_state: str,
    blocker_codes: tuple[str, ...],
    primary_blocker: str | None,
    timing_confirmed: bool = False,
    executable_entry: bool = False,
    invalidated_before_entry: bool = False,
    timing_bar_ts=None,
    entry_ts=None,
    entry_price=None,
    feature_snapshot: dict[str, Any],
) -> AtpTimingState:
    return AtpTimingState(
        instrument=state.instrument,
        decision_ts=state.decision_ts,
        session_date=state.session_date,
        session_segment=state.session_segment,
        family_name=state.family_name,
        context_entry_state=state.entry_state,
        timing_state=timing_state,
        vwap_price_quality_state=vwap_price_quality_state,
        blocker_codes=blocker_codes,
        primary_blocker=primary_blocker,
        setup_armed=state.entry_state == ENTRY_ELIGIBLE,
        timing_confirmed=timing_confirmed,
        executable_entry=executable_entry,
        invalidated_before_entry=invalidated_before_entry,
        setup_armed_but_not_executable=state.entry_state == ENTRY_ELIGIBLE and not executable_entry,
        entry_executed=False,
        timing_bar_ts=timing_bar_ts,
        entry_ts=entry_ts,
        entry_price=entry_price,
        feature_snapshot={
            **feature_snapshot,
            "setup_signature": state.setup_signature,
            "setup_quality_bucket": state.setup_quality_bucket,
            "decision_bar_low": state.feature_snapshot.get("decision_bar_low"),
            "decision_bar_high": state.feature_snapshot.get("decision_bar_high"),
            "average_range": state.feature_snapshot.get("average_range"),
            "regime_bucket": state.feature_snapshot.get("regime_bucket"),
            "volatility_bucket": state.feature_snapshot.get("volatility_bucket"),
        },
    )


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _percentages(counter: Counter, total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        str(key): round((value / total) * 100.0, 4)
        for key, value in sorted(counter.items(), key=lambda item: str(item[0]))
    }
