"""ATP v1 Phase 2 entry-readiness and continuation-family helpers."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .backtest import backtest_decisions_with_audit, summarize_performance
from .conflict import resolve_conflict
from .models import AtpEntryState, FeatureState, HigherPrioritySignal, PatternVariant, SignalDecision
from .state_layers import LONG_BIAS, NEUTRAL, NO_PULLBACK, NORMAL_PULLBACK, SHORT_BIAS, STRETCHED_PULLBACK, VIOLENT_PULLBACK_DISQUALIFY

ENTRY_ELIGIBLE = "ENTRY_ELIGIBLE"
ENTRY_BLOCKED = "ENTRY_BLOCKED"

CONTINUATION_TRIGGER_CONFIRMED = "CONTINUATION_TRIGGER_CONFIRMED"
CONTINUATION_TRIGGER_NOT_CONFIRMED = "CONTINUATION_TRIGGER_NOT_CONFIRMED"
CONTINUATION_TRIGGER_UNAVAILABLE = "CONTINUATION_TRIGGER_UNAVAILABLE"

ATP_V1_LONG_CONTINUATION_FAMILY = "atp_v1_long_pullback_continuation"
ATP_V1_LONG_CONTINUATION_VARIANT_ID = "trend_participation.atp_v1_long_pullback_continuation.long.base"
ATP_V1_SHORT_CONTINUATION_FAMILY = "atp_v1_short_pullback_continuation"
ATP_V1_SHORT_CONTINUATION_VARIANT_ID = "trend_participation.atp_v1_short_pullback_continuation.short.base"

ATP_WARMUP_INCOMPLETE = "ATP_WARMUP_INCOMPLETE"
ATP_SESSION_BLOCKED = "ATP_SESSION_BLOCKED"
ATP_RUNTIME_HEALTH_BLOCKED = "ATP_RUNTIME_HEALTH_BLOCKED"
ATP_POSITION_NOT_FLAT = "ATP_POSITION_NOT_FLAT"
ATP_ONE_POSITION_BASELINE_BLOCK = "ATP_ONE_POSITION_BASELINE_BLOCK"
ATP_NEUTRAL_BIAS = "ATP_NEUTRAL_BIAS"
ATP_SHORT_BIAS_UNSUPPORTED = "ATP_SHORT_BIAS_UNSUPPORTED"
ATP_NO_PULLBACK = "ATP_NO_PULLBACK"
ATP_VIOLENT_PULLBACK_DISQUALIFY = "ATP_VIOLENT_PULLBACK_DISQUALIFY"
ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED = "ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED"

PHASE2_ALLOWED_SESSIONS = frozenset({"ASIA", "LONDON", "US"})
PHASE2_WARMUP_BARS = 9
REASSERTION_ANATOMY = {"BULL_IMPULSE", "LOWER_REJECTION", "BALANCED"}
SHORT_REASSERTION_ANATOMY = {"BEAR_IMPULSE", "UPPER_REJECTION", "BALANCED"}


def atp_phase2_variant(side: str = "LONG", variant_overrides: Mapping[str, Any] | None = None) -> PatternVariant:
    normalized_side = str(side or "LONG").strip().upper()
    normalized_overrides = dict(variant_overrides or {})
    if normalized_side == "SHORT":
        base_variant = PatternVariant(
            variant_id=ATP_V1_SHORT_CONTINUATION_VARIANT_ID,
            family=ATP_V1_SHORT_CONTINUATION_FAMILY,
            side="SHORT",
            strictness="base",
            description=(
                "ATP v1 short continuation after an acceptable pullback when the next completed 5m bar reasserts downside trend."
            ),
            entry_window_bars_1m=6,
            max_hold_bars_1m=24,
            stop_atr_multiple=0.85,
            target_r_multiple=1.6,
            local_cooldown_bars_1m=0,
            reset_window_bars_5m=0,
            allow_reentry=False,
            reentry_policy="structural_only",
            trigger_reclaim_band_multiple=0.08,
            notes=(
                "phase2",
                "replay_paper_only",
                "short_continuation_family",
                "continuation_trigger_requires_prior_pullback_and_completed_bar_reassertion",
            ),
        )
        return replace(base_variant, **normalized_overrides) if normalized_overrides else base_variant
    base_variant = PatternVariant(
        variant_id=ATP_V1_LONG_CONTINUATION_VARIANT_ID,
        family=ATP_V1_LONG_CONTINUATION_FAMILY,
        side="LONG",
        strictness="base",
        description=(
            "ATP v1 long continuation after an acceptable pullback when the next completed 5m bar reasserts the trend."
        ),
        entry_window_bars_1m=6,
        max_hold_bars_1m=24,
        stop_atr_multiple=0.85,
        target_r_multiple=1.6,
        local_cooldown_bars_1m=0,
        reset_window_bars_5m=0,
        allow_reentry=False,
        reentry_policy="structural_only",
        trigger_reclaim_band_multiple=0.08,
        notes=(
            "phase2",
            "replay_paper_only",
            "long_only_initial_family",
            "continuation_trigger_requires_prior_pullback_and_completed_bar_reassertion",
        ),
    )
    return replace(base_variant, **normalized_overrides) if normalized_overrides else base_variant


def build_phase2_replay_package(
    *,
    feature_rows: Sequence[FeatureState],
    bars_1m: Sequence[Any],
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    point_value: float,
    allowed_sessions: frozenset[str] = PHASE2_ALLOWED_SESSIONS,
    variant_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    base_states = classify_entry_states(
        feature_rows=feature_rows,
        allowed_sessions=allowed_sessions,
        variant_overrides=variant_overrides,
    )
    states = list(base_states)
    previous_decision_ids: tuple[str, ...] | None = None
    trades: list[Any] = []
    decisions: list[SignalDecision] = []
    audit: list[Any] = []
    variant = atp_phase2_variant(variant_overrides=variant_overrides)
    variants_by_id = {variant.variant_id: variant}

    for _ in range(4):
        decisions = build_signal_decisions_from_entry_states(
            entry_states=states,
            higher_priority_signals=higher_priority_signals,
            variant_overrides=variant_overrides,
        )
        decision_ids = tuple(decision.decision_id for decision in decisions)
        trades, audit = backtest_decisions_with_audit(
            decisions=decisions,
            bars_1m=bars_1m,
            variants_by_id=variants_by_id,
            point_values={feature_rows[0].instrument: point_value} if feature_rows else {},
            include_shadow_only=True,
        )
        if decision_ids == previous_decision_ids:
            break
        previous_decision_ids = decision_ids
        states = overlay_position_blocks(entry_states=base_states, trades=trades)

    diagnostics = summarize_phase2_entry_diagnostics(
        feature_rows=feature_rows,
        entry_states=states,
        decisions=decisions,
        trades=trades,
    )
    return {
        "variant": variant,
        "entry_states": states,
        "decisions": decisions,
        "shadow_trades": trades,
        "shadow_audit": audit,
        "diagnostics": diagnostics,
    }


def classify_entry_states(
    *,
    feature_rows: Sequence[FeatureState],
    allowed_sessions: frozenset[str] = PHASE2_ALLOWED_SESSIONS,
    runtime_ready: bool = True,
    position_flat: bool = True,
    one_position_rule_clear: bool = True,
    side: str = "LONG",
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[AtpEntryState]:
    normalized_side = str(side or "LONG").strip().upper()
    variant = atp_phase2_variant(normalized_side, variant_overrides=variant_overrides)
    states: list[AtpEntryState] = []
    for index, feature in enumerate(feature_rows):
        setup_feature = feature_rows[index - 1] if index > 0 else None
        warmup_complete = index >= PHASE2_WARMUP_BARS
        session_allowed = feature.session_segment in allowed_sessions
        bias_state = feature.atp_bias_state
        setup_pullback_state = setup_feature.atp_pullback_state if setup_feature is not None else NO_PULLBACK
        trigger_confirmed, trigger_snapshot = _continuation_trigger(
            side=normalized_side,
            feature=feature,
            setup_feature=setup_feature,
        )
        raw_candidate = bool(
            bias_state == (LONG_BIAS if normalized_side == "LONG" else SHORT_BIAS)
            and setup_pullback_state in {NORMAL_PULLBACK, STRETCHED_PULLBACK}
            and setup_feature is not None
        )
        continuation_trigger_state = (
            CONTINUATION_TRIGGER_CONFIRMED
            if trigger_confirmed
            else CONTINUATION_TRIGGER_NOT_CONFIRMED
            if raw_candidate
            else CONTINUATION_TRIGGER_UNAVAILABLE
        )
        blockers = _entry_blockers(
            side=normalized_side,
            bias_state=bias_state,
            setup_pullback_state=setup_pullback_state,
            warmup_complete=warmup_complete,
            session_allowed=session_allowed,
            runtime_ready=runtime_ready,
            position_flat=position_flat,
            one_position_rule_clear=one_position_rule_clear,
            raw_candidate=raw_candidate,
            trigger_confirmed=trigger_confirmed,
        )
        setup_quality_score = _setup_quality_score(
            side=normalized_side,
            feature=feature,
            setup_feature=setup_feature,
            trigger_confirmed=trigger_confirmed,
        )
        states.append(
            AtpEntryState(
                instrument=feature.instrument,
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment=feature.session_segment,
                family_name=variant.family,
                bias_state=bias_state,
                pullback_state=setup_pullback_state,
                continuation_trigger_state=continuation_trigger_state,
                entry_state=ENTRY_ELIGIBLE if not blockers else ENTRY_BLOCKED,
                blocker_codes=tuple(blockers),
                primary_blocker=blockers[0] if blockers else None,
                raw_candidate=raw_candidate,
                trigger_confirmed=trigger_confirmed,
                entry_eligible=not blockers,
                session_allowed=session_allowed,
                warmup_complete=warmup_complete,
                runtime_ready=runtime_ready,
                position_flat=position_flat,
                one_position_rule_clear=one_position_rule_clear,
                setup_signature=_setup_signature(side=normalized_side, feature=feature, setup_feature=setup_feature),
                setup_state_signature=_setup_state_signature(side=normalized_side, feature=feature, setup_feature=setup_feature),
                setup_quality_score=setup_quality_score,
                setup_quality_bucket=_setup_quality_bucket(setup_quality_score),
                feature_snapshot={
                    "decision_bar_open": feature.open,
                    "decision_bar_high": feature.high,
                    "decision_bar_low": feature.low,
                    "decision_bar_close": feature.close,
                    "setup_bar_open": setup_feature.open if setup_feature is not None else None,
                    "setup_bar_high": setup_feature.high if setup_feature is not None else None,
                    "setup_bar_low": setup_feature.low if setup_feature is not None else None,
                    "setup_bar_close": setup_feature.close if setup_feature is not None else None,
                    "average_range": feature.average_range,
                    "regime_bucket": feature.regime_bucket,
                    "volatility_bucket": feature.volatility_bucket,
                    "current_pullback_state": feature.atp_pullback_state,
                    "current_pullback_reason": feature.atp_pullback_reason,
                    "current_bar_anatomy": feature.bar_anatomy,
                    "setup_pullback_state": setup_pullback_state,
                    "setup_pullback_reason": setup_feature.atp_pullback_reason if setup_feature is not None else None,
                    "setup_pullback_depth_score": setup_feature.atp_pullback_depth_score if setup_feature is not None else None,
                    "setup_pullback_violence_score": setup_feature.atp_pullback_violence_score if setup_feature is not None else None,
                    "bias_reasons": list(feature.atp_bias_reasons),
                    "bias_blockers": list(feature.atp_long_bias_blockers if normalized_side == "LONG" else feature.atp_short_bias_blockers),
                    "trigger_checks": trigger_snapshot,
                },
                side=normalized_side,
            )
        )
    return states


def overlay_position_blocks(
    *,
    entry_states: Sequence[AtpEntryState],
    trades: Sequence[Any],
) -> list[AtpEntryState]:
    windows = sorted(
        (
            trade.decision_ts,
            trade.exit_ts,
            trade.decision_id,
        )
        for trade in trades
    )
    blocked_states: list[AtpEntryState] = []
    for state in entry_states:
        active_window = next(
            (
                window
                for window in windows
                if window[0] < state.decision_ts <= window[1]
            ),
            None,
        )
        if active_window is None:
            blocked_states.append(state)
            continue
        blockers = list(state.blocker_codes)
        if ATP_POSITION_NOT_FLAT not in blockers:
            blockers.append(ATP_POSITION_NOT_FLAT)
        if ATP_ONE_POSITION_BASELINE_BLOCK not in blockers:
            blockers.append(ATP_ONE_POSITION_BASELINE_BLOCK)
        blocked_states.append(
            replace(
                state,
                entry_state=ENTRY_BLOCKED,
                blocker_codes=tuple(blockers),
                primary_blocker=state.primary_blocker or ATP_POSITION_NOT_FLAT,
                entry_eligible=False,
                position_flat=False,
                one_position_rule_clear=False,
            )
        )
    return blocked_states


def build_signal_decisions_from_entry_states(
    *,
    entry_states: Sequence[AtpEntryState],
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[SignalDecision]:
    higher_priority = tuple(higher_priority_signals)
    decisions: list[SignalDecision] = []
    for state in entry_states:
        if not state.entry_eligible:
            continue
        variant = atp_phase2_variant(state.side, variant_overrides=variant_overrides)
        conflict_outcome, block_reason = resolve_conflict(
            instrument=state.instrument,
            side=variant.side,
            decision_ts=state.decision_ts,
            entry_window_minutes=variant.entry_window_bars_1m,
            higher_priority_signals=higher_priority,
        )
        live_eligible = conflict_outcome.value == "no_conflict"
        decisions.append(
            SignalDecision(
                decision_id=_decision_id(state, variant_overrides=variant_overrides),
                instrument=state.instrument,
                variant_id=variant.variant_id,
                family=variant.family,
                side=variant.side,
                strictness=variant.strictness,
                decision_ts=state.decision_ts,
                session_date=state.session_date,
                session_segment=state.session_segment,
                regime_bucket=str(state.feature_snapshot.get("regime_bucket") or "UNKNOWN"),
                volatility_bucket=str(state.feature_snapshot.get("volatility_bucket") or "UNKNOWN"),
                conflict_outcome=conflict_outcome,
                live_eligible=live_eligible,
                shadow_only=not live_eligible,
                block_reason=block_reason,
                decision_bar_high=float(state.feature_snapshot.get("decision_bar_high") or 0.0),
                decision_bar_low=float(state.feature_snapshot.get("decision_bar_low") or 0.0),
                decision_bar_close=float(state.feature_snapshot.get("decision_bar_close") or 0.0),
                decision_bar_open=float(state.feature_snapshot.get("decision_bar_open") or 0.0),
                average_range=float(state.feature_snapshot.get("average_range") or 0.0),
                setup_signature=state.setup_signature,
                setup_state_signature=state.setup_state_signature,
                setup_quality_score=state.setup_quality_score,
                setup_quality_bucket=state.setup_quality_bucket,
                feature_snapshot={
                    "atp_bias_state": state.bias_state,
                    "atp_pullback_state": state.pullback_state,
                    "atp_continuation_trigger_state": state.continuation_trigger_state,
                    "atp_entry_state": state.entry_state,
                    "atp_primary_blocker": state.primary_blocker or "",
                    "atp_blocker_codes": ",".join(state.blocker_codes),
                },
            )
        )
    return decisions


def latest_atp_entry_state_summary(entry_state: AtpEntryState | None) -> dict[str, Any]:
    if entry_state is None:
        return {
            "family_name": ATP_V1_LONG_CONTINUATION_FAMILY,
            "side": "LONG",
            "entry_state": ENTRY_BLOCKED,
            "continuation_trigger_state": CONTINUATION_TRIGGER_UNAVAILABLE,
            "blocker_codes": [],
            "primary_blocker": None,
        }
    return {
        "family_name": entry_state.family_name,
        "side": entry_state.side,
        "bias_state": entry_state.bias_state,
        "pullback_state": entry_state.pullback_state,
        "continuation_trigger_state": entry_state.continuation_trigger_state,
        "entry_state": entry_state.entry_state,
        "blocker_codes": list(entry_state.blocker_codes),
        "primary_blocker": entry_state.primary_blocker,
        "raw_candidate": entry_state.raw_candidate,
        "trigger_confirmed": entry_state.trigger_confirmed,
        "entry_eligible": entry_state.entry_eligible,
        "session_allowed": entry_state.session_allowed,
        "warmup_complete": entry_state.warmup_complete,
        "runtime_ready": entry_state.runtime_ready,
        "position_flat": entry_state.position_flat,
        "one_position_rule_clear": entry_state.one_position_rule_clear,
        "setup_quality_score": round(entry_state.setup_quality_score, 4),
        "setup_quality_bucket": entry_state.setup_quality_bucket,
    }


def summarize_phase2_entry_diagnostics(
    *,
    feature_rows: Sequence[FeatureState],
    entry_states: Sequence[AtpEntryState],
    decisions: Sequence[SignalDecision],
    trades: Sequence[Any],
    variant_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    total = len(entry_states)
    bias_rows = [feature for feature in feature_rows if feature.atp_bias_state != NEUTRAL]
    state_rows = list(entry_states)
    eligible_rows = [state for state in state_rows if state.entry_state == ENTRY_ELIGIBLE]
    raw_candidates = [state for state in state_rows if state.raw_candidate]
    blocker_counter = Counter(
        blocker
        for state in state_rows
        for blocker in (state.blocker_codes[:1] if state.blocker_codes else ())
    )
    blocker_totals = Counter(
        blocker
        for state in state_rows
        for blocker in state.blocker_codes
    )
    session_breakdown: dict[str, Any] = {}
    for session in sorted({state.session_segment for state in state_rows}):
        session_states = [state for state in state_rows if state.session_segment == session]
        session_eligible = [state for state in session_states if state.entry_eligible]
        session_raw = [state for state in session_states if state.raw_candidate]
        session_breakdown[session] = {
            "bar_count": len(session_states),
            "raw_continuation_candidates": len(session_raw),
            "entry_eligible_bars": len(session_eligible),
            "entries_per_100_bars": _rate(len(session_eligible), len(session_states)),
        }

    pullback_counter = Counter(feature.atp_pullback_state for feature in bias_rows)
    decision_count = len(decisions)
    trade_metrics = summarize_performance(trades)
    reference_state = state_rows[0] if state_rows else None
    reference_variant = atp_phase2_variant(
        reference_state.side if reference_state is not None else "LONG",
        variant_overrides=variant_overrides,
    )
    return {
        "family_name": reference_variant.family,
        "variant_id": reference_variant.variant_id,
        "bar_count": total,
        "raw_continuation_candidates": len(raw_candidates),
        "entry_eligible_bars": len(eligible_rows),
        "entries_per_100_bars": _rate(len(eligible_rows), total),
        "executed_trade_count": len(trades),
        "signal_count": decision_count,
        "signal_count_live_eligible": sum(1 for decision in decisions if decision.live_eligible),
        "percent_of_bias_bars_becoming_entry_eligible": _percent(len(eligible_rows), len(bias_rows)),
        "pullback_state_percent_on_bias_bars": _percentages(pullback_counter, len(bias_rows)),
        "entry_state_percent": _percentages(Counter(state.entry_state for state in state_rows), total),
        "continuation_trigger_percent": _percentages(
            Counter(state.continuation_trigger_state for state in state_rows),
            total,
        ),
        "session_breakdown": session_breakdown,
        "top_blockers": [
            {"code": code, "count": count, "percent": _percent(count, total)}
            for code, count in blocker_counter.most_common(8)
        ],
        "blocker_totals": [
            {"code": code, "count": count, "percent": _percent(count, total)}
            for code, count in blocker_totals.most_common(12)
        ],
        "top_violent_pullback_reasons": [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                state.feature_snapshot.get("setup_pullback_reason")
                for state in state_rows
                if state.pullback_state == VIOLENT_PULLBACK_DISQUALIFY and state.feature_snapshot.get("setup_pullback_reason")
            ).most_common(6)
        ],
        "top_neutral_reasons": [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                reason
                for feature in feature_rows
                if feature.atp_bias_state == NEUTRAL
                for reason in feature.atp_bias_reasons
            ).most_common(6)
        ],
        "directional_tape_breakdown": _percentages(
            Counter(feature.momentum_persistence for feature in feature_rows),
            total,
        ),
        "performance": {
            "trade_count": trade_metrics.trade_count,
            "trades_per_day": round(trade_metrics.trades_per_day, 4),
            "expectancy": round(trade_metrics.expectancy, 4),
            "profit_factor": round(trade_metrics.profit_factor, 4),
            "net_pnl_cash": round(trade_metrics.net_pnl_cash, 4),
            "max_drawdown": round(trade_metrics.max_drawdown, 4),
        },
    }


def render_phase2_entry_diagnostics_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Phase 2 Entry Diagnostics",
        "",
        f"- Family: `{payload.get('family_name')}`",
        f"- Variant: `{payload.get('variant_id')}`",
        f"- Bars: `{payload.get('bar_count')}`",
        f"- Raw continuation candidates: `{payload.get('raw_continuation_candidates')}`",
        f"- Entry-eligible bars: `{payload.get('entry_eligible_bars')}`",
        f"- Entries per 100 bars: `{payload.get('entries_per_100_bars')}`",
        f"- Executed trades: `{payload.get('executed_trade_count')}`",
        "",
        "## Top Blockers",
    ]
    blockers = payload.get("top_blockers") or []
    if not blockers:
        lines.append("- None")
    else:
        for row in blockers:
            lines.append(f"- `{row['code']}` count=`{row['count']}` percent=`{row['percent']}`")
    lines.extend(["", "## Sessions"])
    for session, row in sorted((payload.get("session_breakdown") or {}).items()):
        lines.append(
            f"- `{session}` bars=`{row['bar_count']}` raw=`{row['raw_continuation_candidates']}` "
            f"eligible=`{row['entry_eligible_bars']}` per100=`{row['entries_per_100_bars']}`"
        )
    return "\n".join(lines) + "\n"


def write_phase2_artifacts(
    *,
    reports_dir: Path,
    diagnostics: dict[str, Any],
) -> tuple[Path, Path]:
    json_path = reports_dir / "atp_phase2_entry_diagnostics.json"
    markdown_path = reports_dir / "atp_phase2_entry_diagnostics.md"
    json_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_phase2_entry_diagnostics_markdown(diagnostics), encoding="utf-8")
    return json_path, markdown_path


def _entry_blockers(
    *,
    side: str,
    bias_state: str,
    setup_pullback_state: str,
    warmup_complete: bool,
    session_allowed: bool,
    runtime_ready: bool,
    position_flat: bool,
    one_position_rule_clear: bool,
    raw_candidate: bool,
    trigger_confirmed: bool,
) -> list[str]:
    blockers: list[str] = []
    if not warmup_complete:
        blockers.append(ATP_WARMUP_INCOMPLETE)
    if not session_allowed:
        blockers.append(ATP_SESSION_BLOCKED)
    if not runtime_ready:
        blockers.append(ATP_RUNTIME_HEALTH_BLOCKED)
    if not position_flat:
        blockers.append(ATP_POSITION_NOT_FLAT)
    if not one_position_rule_clear:
        blockers.append(ATP_ONE_POSITION_BASELINE_BLOCK)
    normalized_side = str(side or "LONG").strip().upper()
    supported_bias = LONG_BIAS if normalized_side == "LONG" else SHORT_BIAS
    if bias_state == NEUTRAL:
        blockers.append(ATP_NEUTRAL_BIAS)
    elif bias_state != supported_bias:
        blockers.append(ATP_SHORT_BIAS_UNSUPPORTED)
    if setup_pullback_state == NO_PULLBACK:
        blockers.append(ATP_NO_PULLBACK)
    elif setup_pullback_state == VIOLENT_PULLBACK_DISQUALIFY:
        blockers.append(ATP_VIOLENT_PULLBACK_DISQUALIFY)
    if raw_candidate and not trigger_confirmed:
        blockers.append(ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED)
    return blockers


def _continuation_trigger(*, side: str, feature: FeatureState, setup_feature: FeatureState | None) -> tuple[bool, dict[str, Any]]:
    if str(side or "LONG").strip().upper() == "SHORT":
        return _short_continuation_trigger(feature=feature, setup_feature=setup_feature)
    return _long_continuation_trigger(feature=feature, setup_feature=setup_feature)


def _long_continuation_trigger(*, feature: FeatureState, setup_feature: FeatureState | None) -> tuple[bool, dict[str, Any]]:
    if setup_feature is None:
        return False, {"setup_bar_available": False}
    close_above_fast_ema = feature.close >= feature.atp_fast_ema
    close_above_previous_close = feature.close > setup_feature.close
    breaks_prior_high = feature.high >= setup_feature.high
    bullish_reassertion_body = feature.close >= ((feature.open + feature.high + feature.low) / 3.0)
    anatomy_ok = feature.bar_anatomy in REASSERTION_ANATOMY
    snapshot = {
        "setup_bar_available": True,
        "close_above_fast_ema": close_above_fast_ema,
        "close_above_previous_close": close_above_previous_close,
        "breaks_prior_high": breaks_prior_high,
        "bullish_reassertion_body": bullish_reassertion_body,
        "anatomy_ok": anatomy_ok,
    }
    confirmed = all(snapshot.values())
    return confirmed, snapshot


def _short_continuation_trigger(*, feature: FeatureState, setup_feature: FeatureState | None) -> tuple[bool, dict[str, Any]]:
    if setup_feature is None:
        return False, {"setup_bar_available": False}
    close_below_fast_ema = feature.close <= feature.atp_fast_ema
    close_below_previous_close = feature.close < setup_feature.close
    breaks_prior_low = feature.low <= setup_feature.low
    bearish_reassertion_body = feature.close <= ((feature.open + feature.high + feature.low) / 3.0)
    anatomy_ok = feature.bar_anatomy in SHORT_REASSERTION_ANATOMY
    snapshot = {
        "setup_bar_available": True,
        "close_below_fast_ema": close_below_fast_ema,
        "close_below_previous_close": close_below_previous_close,
        "breaks_prior_low": breaks_prior_low,
        "bearish_reassertion_body": bearish_reassertion_body,
        "anatomy_ok": anatomy_ok,
    }
    confirmed = all(snapshot.values())
    return confirmed, snapshot


def _setup_quality_score(
    *,
    side: str,
    feature: FeatureState,
    setup_feature: FeatureState | None,
    trigger_confirmed: bool,
) -> float:
    score = max(float(feature.atp_bias_score), 0.0)
    if setup_feature is not None:
        if setup_feature.atp_pullback_state == NORMAL_PULLBACK:
            score += 1.0
        elif setup_feature.atp_pullback_state == STRETCHED_PULLBACK:
            score += 0.65
        score += max(0.0, 1.0 - min(setup_feature.atp_pullback_violence_score, 2.0) * 0.25)
    if trigger_confirmed:
        score += 1.1
    normalized_side = str(side or "LONG").strip().upper()
    impulse_anatomy = "BULL_IMPULSE" if normalized_side == "LONG" else "BEAR_IMPULSE"
    rejection_anatomy = "LOWER_REJECTION" if normalized_side == "LONG" else "UPPER_REJECTION"
    if feature.bar_anatomy == impulse_anatomy:
        score += 0.6
    elif feature.bar_anatomy == rejection_anatomy:
        score += 0.4
    else:
        score += 0.2
    return round(score, 4)


def _setup_quality_bucket(score: float) -> str:
    if score >= 4.6:
        return "HIGH"
    if score >= 3.0:
        return "MEDIUM"
    return "LOW"


def _setup_signature(*, side: str, feature: FeatureState, setup_feature: FeatureState | None) -> str:
    normalized_side = str(side or "LONG").strip().upper()
    setup_origin_ts = setup_feature.decision_ts if setup_feature is not None else feature.decision_ts
    return "|".join(
        [
            ATP_V1_LONG_CONTINUATION_FAMILY if normalized_side == "LONG" else ATP_V1_SHORT_CONTINUATION_FAMILY,
            normalized_side,
            feature.session_segment,
            feature.atp_bias_state,
            setup_feature.atp_pullback_state if setup_feature is not None else NO_PULLBACK,
            feature.bar_anatomy,
            setup_origin_ts.isoformat(),
            feature.decision_ts.isoformat(),
        ]
    )


def _setup_state_signature(*, side: str, feature: FeatureState, setup_feature: FeatureState | None) -> str:
    return "|".join(
        [
            str(side or "LONG").strip().upper(),
            feature.atp_bias_state,
            setup_feature.atp_pullback_state if setup_feature is not None else NO_PULLBACK,
            feature.atp_pullback_state,
            feature.bar_anatomy,
            feature.momentum_persistence,
        ]
    )


def _decision_id(state: AtpEntryState, variant_overrides: Mapping[str, Any] | None = None) -> str:
    return f"{state.instrument}|{atp_phase2_variant(state.side, variant_overrides=variant_overrides).variant_id}|{state.decision_ts.isoformat()}"


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 4)


def _percentages(counter: Counter, total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        str(key): round((value / total) * 100.0, 4)
        for key, value in sorted(counter.items(), key=lambda item: str(item[0]))
    }
