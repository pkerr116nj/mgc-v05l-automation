from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.research.trend_participation import (
    ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED,
    ATP_TIMING_5M_CONTEXT_NOT_READY,
    ATP_TIMING_CHASE_RISK,
    ATP_TIMING_CONFIRMED,
    ATP_TIMING_INVALIDATED,
    ATP_TIMING_INVALIDATED_BEFORE_ENTRY,
    ATP_TIMING_LONDON_DISABLED,
    ATP_TIMING_UNAVAILABLE,
    ATP_TIMING_VWAP_CHASE_RISK,
    ATP_TIMING_WAITING,
    ATP_NO_PULLBACK,
    ATP_POSITION_NOT_FLAT,
    ATP_V1_LONG_CONTINUATION_FAMILY,
    CONTINUATION_TRIGGER_CONFIRMED,
    CONTINUATION_TRIGGER_NOT_CONFIRMED,
    ENTRY_BLOCKED,
    ENTRY_ELIGIBLE,
    VWAP_CHASE_RISK,
    VWAP_FAVORABLE,
    VWAP_NEUTRAL,
    build_phase3_replay_package,
    ConflictOutcome,
    HigherPrioritySignal,
    PatternVariant,
    ResearchBar,
    SignalDecision,
    backtest_decisions,
    build_atp_performance_validation_report,
    build_feature_states,
    build_signal_decisions_from_entry_states,
    classify_entry_states,
    classify_timing_states,
    classify_vwap_price_quality,
    default_pattern_variants,
    generate_signal_decisions,
    latest_atp_timing_state_summary,
    run_trend_participation_engine,
    summarize_phase2_entry_diagnostics,
    summarize_phase3_timing_diagnostics,
    summarize_atp_state_diagnostics,
)
from mgc_v05l.research.trend_participation.canary import CanaryLaneSpec, _decision_policy_row, _signal_row
from mgc_v05l.research.trend_participation.phase2_continuation import overlay_position_blocks
from mgc_v05l.research.trend_participation.phase4 import build_rolling_windows
from mgc_v05l.research.trend_participation.phase5 import _fragility_diagnosis
from mgc_v05l.research.trend_participation.phase3_timing import ATP_TIMING_ACTIVATION_ROLLING_5M
from mgc_v05l.research.trend_participation.storage import rolling_window_bars_from_1m


def _bar(
    *,
    instrument: str,
    timeframe: str,
    minute_offset: int,
    minutes: int,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> ResearchBar:
    start = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    end = start + timedelta(minutes=minutes)
    return ResearchBar(
        instrument=instrument,
        timeframe=timeframe,
        start_ts=start,
        end_ts=end,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="US_MIDDAY",
        session_segment="US",
        source="synthetic",
        provenance="unit_test",
    )


def _decision(
    *,
    decision_id: str,
    decision_minute: int,
    setup_signature: str,
    setup_state_signature: str,
) -> SignalDecision:
    decision_ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=decision_minute)
    return SignalDecision(
        decision_id=decision_id,
        instrument="MES",
        variant_id="trend_participation.breakout_continuation.long.active",
        family="breakout_continuation",
        side="LONG",
        strictness="active",
        decision_ts=decision_ts,
        session_date=decision_ts.date(),
        session_segment="US",
        regime_bucket="TREND_UP",
        volatility_bucket="NORMAL",
        conflict_outcome=ConflictOutcome.NO_CONFLICT,
        live_eligible=True,
        shadow_only=False,
        block_reason=None,
        decision_bar_high=101.0,
        decision_bar_low=100.2,
        decision_bar_close=100.95,
        decision_bar_open=100.4,
        average_range=0.5,
        setup_signature=setup_signature,
        setup_state_signature=setup_state_signature,
        setup_quality_score=1.6,
        setup_quality_bucket="MEDIUM",
        feature_snapshot={"trend_state": "UP"},
    )


def _eligible_phase2_setup_and_1m_followthrough() -> tuple[list[ResearchBar], list[ResearchBar]]:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=103.48, low=102.5, close=103.36),
    ]
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=51, minutes=1, open_=103.12, high=103.22, low=103.05, close=103.16),
        _bar(instrument="MES", timeframe="1m", minute_offset=52, minutes=1, open_=103.18, high=103.82, low=103.32, close=103.70),
        _bar(instrument="MES", timeframe="1m", minute_offset=53, minutes=1, open_=103.72, high=103.9, low=103.55, close=103.86),
        _bar(instrument="MES", timeframe="1m", minute_offset=54, minutes=1, open_=103.84, high=104.02, low=103.74, close=103.98),
    ]
    return bars_5m, bars_1m


def test_build_feature_states_detects_uptrend_alignment() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=0, minutes=5, open_=100.0, high=101.0, low=99.9, close=100.8),
        _bar(instrument="MES", timeframe="5m", minute_offset=5, minutes=5, open_=100.8, high=101.5, low=100.6, close=101.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=10, minutes=5, open_=101.3, high=102.0, low=101.1, close=101.9),
        _bar(instrument="MES", timeframe="5m", minute_offset=15, minutes=5, open_=101.9, high=102.4, low=101.7, close=102.2),
    ]
    bars_1m = [
        _bar(
            instrument="MES",
            timeframe="1m",
            minute_offset=15 + idx,
            minutes=1,
            open_=101.7 + idx * 0.05,
            high=101.8 + idx * 0.05,
            low=101.65 + idx * 0.05,
            close=101.78 + idx * 0.05,
        )
        for idx in range(5)
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=bars_1m)

    assert features[-1].trend_state in {"UP", "STRONG_UP"}
    assert features[-1].mtf_agreement_state == "ALIGNED_UP"
    assert features[-1].direction_bias == "LONG_BIAS"
    assert features[-1].atp_bias_state == "LONG_BIAS"


def test_build_feature_states_marks_short_bias_when_completed_bar_stack_is_down() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=0, minutes=5, open_=103.0, high=103.2, low=102.6, close=102.7),
        _bar(instrument="MES", timeframe="5m", minute_offset=5, minutes=5, open_=102.7, high=102.8, low=102.1, close=102.2),
        _bar(instrument="MES", timeframe="5m", minute_offset=10, minutes=5, open_=102.2, high=102.3, low=101.7, close=101.8),
        _bar(instrument="MES", timeframe="5m", minute_offset=15, minutes=5, open_=101.8, high=101.9, low=101.2, close=101.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=20, minutes=5, open_=101.3, high=101.4, low=100.7, close=100.8),
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=100.8, high=100.9, low=100.1, close=100.2),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])

    assert features[-1].atp_bias_state == "SHORT_BIAS"
    assert features[-1].atp_bias_score < 0
    assert not features[-1].atp_short_bias_blockers


def test_build_feature_states_marks_neutral_with_explicit_bias_blockers() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=0, minutes=5, open_=100.0, high=100.3, low=99.8, close=100.1),
        _bar(instrument="MES", timeframe="5m", minute_offset=5, minutes=5, open_=100.1, high=100.2, low=99.9, close=100.0),
        _bar(instrument="MES", timeframe="5m", minute_offset=10, minutes=5, open_=100.0, high=100.15, low=99.85, close=100.02),
        _bar(instrument="MES", timeframe="5m", minute_offset=15, minutes=5, open_=100.02, high=100.18, low=99.88, close=99.98),
        _bar(instrument="MES", timeframe="5m", minute_offset=20, minutes=5, open_=99.98, high=100.14, low=99.86, close=100.01),
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=100.01, high=100.16, low=99.87, close=100.0),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])

    assert features[-1].atp_bias_state == "NEUTRAL"
    assert features[-1].atp_bias_reasons
    assert features[-1].atp_long_bias_blockers
    assert features[-1].atp_short_bias_blockers


def test_atp_pullback_classifier_marks_shallow_standard_stretched_and_violent() -> None:
    shallow_bars = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(6)
    ]
    standard_bars = shallow_bars[:-1] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=102.0, high=102.05, low=101.2, close=101.45),
    ]
    stretched_bars = shallow_bars[:-1] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=101.15, high=101.28, low=100.8, close=100.98),
    ]
    violent_bars = shallow_bars[:-1] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=101.55, high=101.7, low=101.0, close=101.18),
    ]

    shallow_feature = build_feature_states(bars_5m=shallow_bars, bars_1m=[])[-1]
    standard_feature = build_feature_states(bars_5m=standard_bars, bars_1m=[])[-1]
    stretched_feature = build_feature_states(bars_5m=stretched_bars, bars_1m=[])[-1]
    violent_feature = build_feature_states(bars_5m=violent_bars, bars_1m=[])[-1]

    assert shallow_feature.atp_pullback_state == "NO_PULLBACK"
    assert shallow_feature.atp_pullback_envelope_state == "SHALLOW"
    assert standard_feature.atp_pullback_state == "NORMAL_PULLBACK"
    assert standard_feature.atp_pullback_envelope_state == "STANDARD"
    assert stretched_feature.atp_pullback_state == "STRETCHED_PULLBACK"
    assert stretched_feature.atp_pullback_envelope_state == "DEEP_ACCEPTABLE"
    assert violent_feature.atp_pullback_state == "VIOLENT_PULLBACK_DISQUALIFY"
    assert violent_feature.atp_pullback_envelope_state == "ABNORMAL"
    assert violent_feature.atp_pullback_reason in {
        "structure_damage",
        "retracement_exceeds_stretched_envelope",
        "countertrend_velocity_range_expansion",
        "countertrend_range_expansion_extreme",
    }


def test_atp_state_diagnostics_summary_uses_feature_states() -> None:
    long_bias_bars = [
        _bar(instrument="MES", timeframe="5m", minute_offset=0, minutes=5, open_=100.0, high=100.4, low=99.9, close=100.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=5, minutes=5, open_=100.3, high=100.9, low=100.2, close=100.8),
        _bar(instrument="MES", timeframe="5m", minute_offset=10, minutes=5, open_=100.8, high=101.4, low=100.7, close=101.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=15, minutes=5, open_=101.3, high=101.9, low=101.2, close=101.8),
        _bar(instrument="MES", timeframe="5m", minute_offset=20, minutes=5, open_=101.8, high=102.4, low=101.7, close=102.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=25, minutes=5, open_=102.3, high=102.35, low=101.7, close=101.9),
    ]
    neutral_bars = [
        _bar(instrument="MES", timeframe="5m", minute_offset=30, minutes=5, open_=101.9, high=102.0, low=101.7, close=101.85),
        _bar(instrument="MES", timeframe="5m", minute_offset=35, minutes=5, open_=101.85, high=101.98, low=101.72, close=101.82),
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=101.82, high=101.94, low=101.7, close=101.81),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=101.81, high=101.92, low=101.68, close=101.8),
    ]
    features = build_feature_states(bars_5m=long_bias_bars + neutral_bars, bars_1m=[])

    summary = summarize_atp_state_diagnostics(features)

    assert summary["bar_count"] == len(features)
    assert "LONG_BIAS" in summary["bias_state_percent"]
    assert "NEUTRAL" in summary["bias_state_percent"]
    assert "US" in summary["session_breakdown"]
    assert summary["top_neutral_reasons"]


def test_phase2_entry_states_mark_continuation_bar_as_entry_eligible() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=103.48, low=102.5, close=103.36),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    latest = entry_states[-1]

    assert latest.family_name == ATP_V1_LONG_CONTINUATION_FAMILY
    assert latest.entry_state == ENTRY_ELIGIBLE
    assert latest.continuation_trigger_state == CONTINUATION_TRIGGER_CONFIRMED
    assert latest.blocker_codes == ()
    assert latest.raw_candidate is True
    assert latest.pullback_state in {"NORMAL_PULLBACK", "STRETCHED_PULLBACK"}


def test_phase2_entry_states_emit_exact_trigger_blocker_when_reassertion_fails() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=102.95, low=102.5, close=102.82),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    latest = entry_states[-1]

    assert latest.entry_state == ENTRY_BLOCKED
    assert latest.continuation_trigger_state == CONTINUATION_TRIGGER_NOT_CONFIRMED
    assert latest.primary_blocker == ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED
    assert latest.blocker_codes == (ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED,)


def test_phase2_entry_states_emit_no_pullback_blocker_without_prior_setup() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.35, high=100.28 + idx * 0.35, low=99.92 + idx * 0.35, close=100.22 + idx * 0.35)
        for idx in range(10)
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    latest = entry_states[-1]

    assert latest.entry_state == ENTRY_BLOCKED
    assert latest.primary_blocker == ATP_NO_PULLBACK
    assert ATP_NO_PULLBACK in latest.blocker_codes


def test_phase2_position_overlay_blocks_when_prior_trade_is_still_open() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=103.48, low=102.5, close=103.36),
        _bar(instrument="MES", timeframe="5m", minute_offset=50, minutes=5, open_=103.35, high=103.4, low=102.85, close=102.95),
        _bar(instrument="MES", timeframe="5m", minute_offset=55, minutes=5, open_=102.95, high=103.75, low=102.9, close=103.6),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    eligible_rows = [state for state in entry_states if state.entry_state == ENTRY_ELIGIBLE]
    blocked_rows = overlay_position_blocks(
        entry_states=entry_states,
        trades=[
            SimpleNamespace(
                decision_ts=eligible_rows[0].decision_ts,
                exit_ts=eligible_rows[0].decision_ts + timedelta(minutes=12),
                decision_id="trade-1",
            )
        ],
    )

    blocked_rows = [state for state in blocked_rows if ATP_POSITION_NOT_FLAT in state.blocker_codes]

    assert eligible_rows
    assert blocked_rows
    assert all(state.entry_state == ENTRY_BLOCKED for state in blocked_rows)


def test_phase2_signal_decisions_and_summary_use_persisted_entry_truth() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.4, high=100.35 + idx * 0.4, low=99.9 + idx * 0.4, close=100.25 + idx * 0.4)
        for idx in range(8)
    ] + [
        _bar(instrument="MES", timeframe="5m", minute_offset=40, minutes=5, open_=103.0, high=103.05, low=102.4, close=102.55),
        _bar(instrument="MES", timeframe="5m", minute_offset=45, minutes=5, open_=102.55, high=103.48, low=102.5, close=103.36),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    decisions = build_signal_decisions_from_entry_states(entry_states=entry_states)
    summary = summarize_phase2_entry_diagnostics(
        feature_rows=features,
        entry_states=entry_states,
        decisions=decisions,
        trades=[],
    )

    assert decisions
    assert all(decision.family == ATP_V1_LONG_CONTINUATION_FAMILY for decision in decisions)
    assert summary["entry_eligible_bars"] == sum(1 for state in entry_states if state.entry_state == ENTRY_ELIGIBLE)
    assert summary["top_blockers"]


def test_phase3_timing_is_unavailable_when_5m_context_is_not_ready() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=idx * 5, minutes=5, open_=100.0 + idx * 0.35, high=100.28 + idx * 0.35, low=99.92 + idx * 0.35, close=100.22 + idx * 0.35)
        for idx in range(10)
    ]
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=51, minutes=1, open_=103.0, high=103.4, low=102.98, close=103.32),
        _bar(instrument="MES", timeframe="1m", minute_offset=52, minutes=1, open_=103.3, high=103.7, low=103.22, close=103.62),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=bars_1m)
    latest = timing_states[-1]

    assert latest.context_entry_state == ENTRY_BLOCKED
    assert latest.timing_state == ATP_TIMING_UNAVAILABLE
    assert latest.primary_blocker == ATP_TIMING_5M_CONTEXT_NOT_READY


def test_phase3_timing_confirms_with_favorable_vwap_quality() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=bars_1m)
    latest = timing_states[-1]
    summary = latest_atp_timing_state_summary(latest)

    assert entry_states[-1].entry_state == ENTRY_ELIGIBLE
    assert latest.timing_state == ATP_TIMING_CONFIRMED
    assert latest.vwap_price_quality_state == VWAP_FAVORABLE
    assert latest.executable_entry is True
    assert summary["timing_state"] == ATP_TIMING_CONFIRMED
    assert summary["entry_executed"] is False


def test_phase3_timing_marks_chase_risk_when_vwap_quality_is_poor() -> None:
    bars_5m, _ = _eligible_phase2_setup_and_1m_followthrough()
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=51, minutes=1, open_=103.18, high=103.3, low=103.12, close=103.22),
        _bar(instrument="MES", timeframe="1m", minute_offset=52, minutes=1, open_=103.92, high=103.96, low=103.5, close=103.72),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=bars_1m)
    latest = timing_states[-1]

    assert latest.timing_state == ATP_TIMING_CHASE_RISK
    assert latest.vwap_price_quality_state == VWAP_CHASE_RISK
    assert latest.primary_blocker == ATP_TIMING_VWAP_CHASE_RISK


def test_phase3_timing_tightens_borderline_vwap_neutral_entries() -> None:
    bars_5m, _ = _eligible_phase2_setup_and_1m_followthrough()
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=51, minutes=1, open_=103.12, high=103.22, low=103.05, close=103.16),
        _bar(instrument="MES", timeframe="1m", minute_offset=52, minutes=1, open_=103.48, high=103.52, low=103.30, close=103.49),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=bars_1m)
    latest = timing_states[-1]

    assert latest.timing_state == ATP_TIMING_CHASE_RISK
    assert latest.vwap_price_quality_state == VWAP_CHASE_RISK
    assert latest.primary_blocker == ATP_TIMING_VWAP_CHASE_RISK


def test_phase3_timing_disables_london_execution_even_when_setup_is_ready() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    london_state = replace(entry_states[-1], session_segment="LONDON")
    timing_states = classify_timing_states(entry_states=[london_state], bars_1m=bars_1m)
    latest = timing_states[-1]

    assert latest.timing_state == ATP_TIMING_UNAVAILABLE
    assert latest.vwap_price_quality_state == VWAP_NEUTRAL
    assert latest.primary_blocker == ATP_TIMING_LONDON_DISABLED
    assert latest.setup_armed is True
    assert latest.executable_entry is False


def test_phase3_timing_marks_invalidation_before_entry() -> None:
    bars_5m, _ = _eligible_phase2_setup_and_1m_followthrough()
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=51, minutes=1, open_=102.6, high=102.72, low=102.32, close=102.4),
        _bar(instrument="MES", timeframe="1m", minute_offset=52, minutes=1, open_=102.42, high=102.55, low=102.28, close=102.34),
    ]

    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=bars_1m)
    latest = timing_states[-1]

    assert latest.timing_state == ATP_TIMING_INVALIDATED
    assert latest.primary_blocker == ATP_TIMING_INVALIDATED_BEFORE_ENTRY
    assert latest.invalidated_before_entry is True


def test_rolling_window_bars_from_1m_emits_minute_stepped_5m_context() -> None:
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=index, minutes=1, open_=100.0 + index * 0.1, high=100.2 + index * 0.1, low=99.9 + index * 0.1, close=100.1 + index * 0.1)
        for index in range(6)
    ]

    rolling = rolling_window_bars_from_1m(bars_1m=bars_1m)

    assert len(rolling) == 2
    assert rolling[0].timeframe == "5m"
    assert rolling[0].open == bars_1m[0].open
    assert rolling[0].close == bars_1m[4].close
    assert rolling[1].open == bars_1m[1].open
    assert rolling[1].close == bars_1m[5].close


def test_phase3_timing_rolling_activation_can_execute_on_current_minute_window() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()
    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    rolling_entry_state = replace(entry_states[-1], decision_ts=bars_1m[0].end_ts)

    timing_states = classify_timing_states(
        entry_states=[rolling_entry_state],
        bars_1m=bars_1m,
        entry_activation_basis=ATP_TIMING_ACTIVATION_ROLLING_5M,
    )

    assert rolling_entry_state.entry_state == ENTRY_ELIGIBLE
    assert any(state.executable_entry for state in timing_states)
    assert timing_states[-1].entry_ts is not None
    assert timing_states[-1].entry_ts >= bars_1m[0].end_ts


def test_phase3_vwap_price_quality_classifier_is_explicit() -> None:
    assert classify_vwap_price_quality(side="LONG", entry_price=100.0, bar_vwap=100.1, band_reference=0.5) == VWAP_FAVORABLE
    assert classify_vwap_price_quality(side="LONG", entry_price=100.149, bar_vwap=100.1, band_reference=0.5) == VWAP_NEUTRAL
    assert classify_vwap_price_quality(side="LONG", entry_price=100.16, bar_vwap=100.1, band_reference=0.5) == VWAP_CHASE_RISK
    assert classify_vwap_price_quality(side="LONG", entry_price=100.3, bar_vwap=100.1, band_reference=0.5) == VWAP_CHASE_RISK


def test_phase3_replay_package_reports_conversion_and_old_proxy_comparison() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()
    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)

    package = build_phase3_replay_package(
        entry_states=entry_states,
        bars_1m=bars_1m,
        point_value=5.0,
        old_proxy_trade_count=0,
    )
    summary = summarize_phase3_timing_diagnostics(
        timing_states=package["timing_states"],
        trades=package["shadow_trades"],
        old_proxy_trade_count=0,
    )

    assert package["shadow_trades"]
    assert summary["ready_5m_bars_count"] >= 1
    assert summary["timing_confirmed_count"] >= 1
    assert summary["executed_entry_count"] >= 1
    assert summary["old_proxy_comparison"]["phase3_executed_trade_count"] >= 1
    assert summary["old_proxy_comparison"]["executed_trade_delta"] >= 1


def test_phase3_replay_package_resimulates_after_position_overlay() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()
    bars_1m = bars_1m + [
        _bar(instrument="MES", timeframe="1m", minute_offset=55, minutes=1, open_=103.98, high=104.05, low=103.9, close=104.0),
        _bar(instrument="MES", timeframe="1m", minute_offset=56, minutes=1, open_=104.0, high=104.08, low=103.94, close=104.02),
        _bar(instrument="MES", timeframe="1m", minute_offset=57, minutes=1, open_=104.02, high=104.1, low=103.96, close=104.03),
        _bar(instrument="MES", timeframe="1m", minute_offset=58, minutes=1, open_=104.03, high=104.11, low=103.98, close=104.04),
    ]
    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    eligible = [state for state in entry_states if state.entry_state == ENTRY_ELIGIBLE]

    package = build_phase3_replay_package(
        entry_states=[
            eligible[-1],
            replace(eligible[-1], decision_ts=eligible[-1].decision_ts + timedelta(minutes=2)),
        ],
        bars_1m=bars_1m,
        point_value=5.0,
        old_proxy_trade_count=0,
    )

    assert len(package["shadow_trades"]) == 1
    assert package["diagnostics"]["executed_entry_count"] == 1
    assert package["diagnostics"]["old_proxy_comparison"]["phase3_executed_trade_count"] == 1
    assert any(state.primary_blocker == ATP_POSITION_NOT_FLAT for state in package["timing_states"])


def test_atp_performance_validation_segments_trades_from_persisted_state_truth() -> None:
    bars_5m, bars_1m = _eligible_phase2_setup_and_1m_followthrough()
    features = build_feature_states(bars_5m=bars_5m, bars_1m=[])
    entry_states = classify_entry_states(feature_rows=features)
    package = build_phase3_replay_package(
        entry_states=entry_states,
        bars_1m=bars_1m,
        point_value=5.0,
        old_proxy_trade_count=0,
    )
    base_trade = replace(
        package["shadow_trades"][0],
        pnl_cash=24.0,
        gross_pnl_cash=27.0,
        mfe_points=0.92,
        mae_points=0.18,
    )
    base_timing_state = next(state for state in package["timing_states"] if state.decision_ts == base_trade.decision_ts)
    matched_entry_state = next(state for state in entry_states if state.decision_ts == base_trade.decision_ts)
    second_trade = replace(
        base_trade,
        decision_id="trade-2",
        decision_ts=base_trade.decision_ts + timedelta(minutes=30),
        entry_ts=base_trade.entry_ts + timedelta(minutes=30),
        exit_ts=base_trade.exit_ts + timedelta(minutes=30),
        pnl_cash=-18.0,
        gross_pnl_cash=-15.0,
        mfe_points=0.22,
        mae_points=0.61,
        session_segment="LONDON",
    )
    second_entry_state = replace(
        matched_entry_state,
        decision_ts=second_trade.decision_ts,
        session_segment="LONDON",
        pullback_state="STRETCHED_PULLBACK",
    )
    second_timing_state = replace(
        base_timing_state,
        decision_ts=second_trade.decision_ts,
        session_segment="LONDON",
        entry_ts=second_trade.entry_ts,
        timing_bar_ts=second_trade.entry_ts,
        vwap_price_quality_state="VWAP_NEUTRAL",
        entry_executed=True,
    )
    near_miss_timing_state = replace(
        base_timing_state,
        decision_ts=second_trade.decision_ts + timedelta(minutes=30),
        session_segment="US",
        timing_state=ATP_TIMING_CHASE_RISK,
        vwap_price_quality_state=VWAP_CHASE_RISK,
        primary_blocker=ATP_TIMING_VWAP_CHASE_RISK,
        blocker_codes=(ATP_TIMING_VWAP_CHASE_RISK,),
        executable_entry=False,
        entry_executed=False,
        setup_armed_but_not_executable=True,
    )
    near_miss_entry_state = replace(
        matched_entry_state,
        decision_ts=near_miss_timing_state.decision_ts,
        session_segment="US",
    )

    payload = build_atp_performance_validation_report(
        bar_count=len(features),
        entry_states=[matched_entry_state, second_entry_state, near_miss_entry_state],
        timing_states=[base_timing_state, second_timing_state, near_miss_timing_state],
        atp_trades=[base_trade, second_trade],
        legacy_proxy_trades=[second_trade],
    )

    assert payload["atp_phase3_performance"]["total_trades"] == 2
    assert payload["atp_phase3_performance"]["winners"] == 1
    assert payload["atp_phase3_performance"]["losers"] == 1
    assert payload["same_window_comparison"]["delta"]["trade_count_delta"] == 1
    assert payload["same_window_comparison"]["delta"]["entries_per_100_bars_delta"] > 0.0
    vwap_rows = {row["segment"]: row for row in payload["segment_breakdowns"]["by_vwap_price_quality_state"]}
    assert vwap_rows["VWAP_FAVORABLE"]["total_trades"] == 1
    assert vwap_rows["VWAP_NEUTRAL"]["total_trades"] == 1
    assert payload["near_miss_breakdown"]["top_blockers"][0]["code"] == ATP_TIMING_VWAP_CHASE_RISK


def test_phase3_is_separate_from_legacy_variant_library() -> None:
    variant_ids = {variant.variant_id for variant in default_pattern_variants()}
    assert ATP_V1_LONG_CONTINUATION_FAMILY not in {variant.family for variant in default_pattern_variants()}
    assert "trend_participation.atp_v1_long_pullback_continuation.long.base" not in variant_ids


def test_generate_signal_decisions_marks_agreement_as_shadow_only() -> None:
    bars_5m = [
        _bar(instrument="MES", timeframe="5m", minute_offset=0, minutes=5, open_=100.0, high=100.8, low=99.9, close=100.7),
        _bar(instrument="MES", timeframe="5m", minute_offset=5, minutes=5, open_=100.7, high=101.1, low=100.5, close=101.0),
        _bar(instrument="MES", timeframe="5m", minute_offset=10, minutes=5, open_=101.0, high=101.4, low=100.9, close=101.3),
        _bar(instrument="MES", timeframe="5m", minute_offset=15, minutes=5, open_=101.3, high=101.8, low=101.2, close=101.7),
    ]
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=15 + idx, minutes=1, open_=101.2 + idx * 0.03, high=101.3 + idx * 0.03, low=101.15 + idx * 0.03, close=101.28 + idx * 0.03)
        for idx in range(5)
    ]
    features = build_feature_states(bars_5m=bars_5m, bars_1m=bars_1m)
    priority = HigherPrioritySignal(
        instrument="MES",
        side="LONG",
        start_ts=features[-1].decision_ts,
        end_ts=features[-1].decision_ts + timedelta(minutes=3),
        reason="approved_quant_lane",
    )

    decisions = generate_signal_decisions(
        feature_rows=features[-1:],
        variants=default_pattern_variants(),
        higher_priority_signals=(priority,),
    )

    assert decisions
    assert all(decision.conflict_outcome == ConflictOutcome.AGREEMENT for decision in decisions)
    assert all(decision.shadow_only is True for decision in decisions)
    assert all(decision.live_eligible is False for decision in decisions)
    assert all(decision.setup_signature for decision in decisions)
    assert all(decision.setup_quality_bucket in {"HIGH", "MEDIUM", "LOW"} for decision in decisions)


def test_backtest_decisions_waits_until_next_1m_bar_for_entry() -> None:
    decision = _decision(
        decision_id="d1",
        decision_minute=5,
        setup_signature="breakout|LONG|US|UP|MODERATE|EXPANDED|NEAR_RECENT_HIGH",
        setup_state_signature="UP|MODERATE|EXPANDED|BULL_IMPULSE|NEAR_RECENT_HIGH|ALIGNED_UP",
    )
    variants = {
        decision.variant_id: PatternVariant(
            variant_id=decision.variant_id,
            family=decision.family,
            side=decision.side,
            strictness=decision.strictness,
            description="test",
            entry_window_bars_1m=3,
            max_hold_bars_1m=4,
            stop_atr_multiple=0.8,
            target_r_multiple=1.5,
            local_cooldown_bars_1m=1,
            reset_window_bars_5m=1,
        )
    }
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=0, minutes=1, open_=100.4, high=100.9, low=100.3, close=100.8),
        _bar(instrument="MES", timeframe="1m", minute_offset=5, minutes=1, open_=101.02, high=101.2, low=100.95, close=101.15),
        _bar(instrument="MES", timeframe="1m", minute_offset=6, minutes=1, open_=101.15, high=101.25, low=100.9, close=100.95),
        _bar(instrument="MES", timeframe="1m", minute_offset=7, minutes=1, open_=100.95, high=101.8, low=100.9, close=101.7),
    ]

    trades = backtest_decisions(
        decisions=[decision],
        bars_1m=bars_1m,
        variants_by_id=variants,
        point_values={"MES": 5.0},
        include_shadow_only=False,
        slippage_points=0.0,
        fee_per_trade=0.0,
    )

    assert len(trades) == 1
    assert trades[0].entry_ts > decision.decision_ts
    assert trades[0].hold_minutes >= 1.0
    assert trades[0].decision_id == "d1"


def test_backtest_allows_reentry_only_after_reset_and_one_position_per_variant() -> None:
    variant = PatternVariant(
        variant_id="trend_participation.breakout_continuation.long.active",
        family="breakout_continuation",
        side="LONG",
        strictness="active",
        description="active test",
        entry_window_bars_1m=2,
        max_hold_bars_1m=2,
        stop_atr_multiple=0.8,
        target_r_multiple=1.0,
        local_cooldown_bars_1m=1,
        reset_window_bars_5m=2,
    )
    decisions = [
        _decision(
            decision_id="d1",
            decision_minute=5,
            setup_signature="same_setup",
            setup_state_signature="same_state",
        ),
        _decision(
            decision_id="d2",
            decision_minute=6,
            setup_signature="same_setup",
            setup_state_signature="same_state",
        ),
        _decision(
            decision_id="d3",
            decision_minute=12,
            setup_signature="same_setup",
            setup_state_signature="fresh_state",
        ),
    ]
    bars_1m = [
        _bar(instrument="MES", timeframe="1m", minute_offset=6, minutes=1, open_=101.0, high=101.2, low=100.9, close=101.1),
        _bar(instrument="MES", timeframe="1m", minute_offset=7, minutes=1, open_=101.1, high=101.7, low=101.0, close=101.6),
        _bar(instrument="MES", timeframe="1m", minute_offset=8, minutes=1, open_=101.6, high=101.65, low=101.2, close=101.3),
        _bar(instrument="MES", timeframe="1m", minute_offset=12, minutes=1, open_=100.95, high=101.05, low=100.9, close=101.0),
        _bar(instrument="MES", timeframe="1m", minute_offset=13, minutes=1, open_=101.0, high=101.3, low=100.95, close=101.2),
        _bar(instrument="MES", timeframe="1m", minute_offset=14, minutes=1, open_=101.2, high=101.9, low=101.1, close=101.8),
        _bar(instrument="MES", timeframe="1m", minute_offset=15, minutes=1, open_=101.8, high=101.85, low=101.4, close=101.5),
    ]

    trades = backtest_decisions(
        decisions=decisions,
        bars_1m=bars_1m,
        variants_by_id={variant.variant_id: variant},
        point_values={"MES": 5.0},
        include_shadow_only=False,
        slippage_points=0.0,
        fee_per_trade=0.0,
    )

    assert len(trades) == 2
    assert trades[0].decision_id in {"d1", "d2"}
    assert trades[1].decision_id == "d3"
    assert trades[0].is_reentry is False
    assert trades[1].is_reentry is True
    assert trades[1].reentry_type == "STRUCTURAL_RESET"
    assert trades[0].exit_ts <= trades[1].entry_ts


def test_run_trend_participation_engine_returns_active_report_without_materialization(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "bars.sqlite3"
    import sqlite3

    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute(
            """
            create table bars (
              symbol text,
              timeframe text,
              start_ts text,
              end_ts text,
              open real,
              high real,
              low real,
              close real,
              volume integer
            )
            """
        )
        rows = []
        for idx in range(90):
            start_1m = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=idx)
            end_1m = start_1m + timedelta(minutes=1)
            base = 100.0 + idx * 0.05
            rows.append(("MES", "1m", start_1m.isoformat(), end_1m.isoformat(), base, base + 0.25, base - 0.12, base + 0.15, 100))
        for idx in range(18):
            start_5m = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=idx * 5)
            end_5m = start_5m + timedelta(minutes=5)
            base = 100.0 + idx * 0.25
            rows.append(("MES", "5m", start_5m.isoformat(), end_5m.isoformat(), base, base + 0.7, base - 0.2, base + 0.45, 500))
        connection.executemany("insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        connection.commit()
    finally:
        connection.close()

    artifacts = run_trend_participation_engine(
        source_sqlite_path=sqlite_path,
        output_dir=tmp_path / "tpe",
        instruments=("MES",),
        materialize_storage=False,
    )

    assert artifacts.report_json_path.exists()
    assert artifacts.report_markdown_path.exists()
    assert artifacts.storage_manifest_path.exists()
    assert artifacts.phase1_diagnostics_json_path is not None and artifacts.phase1_diagnostics_json_path.exists()
    assert artifacts.phase1_diagnostics_markdown_path is not None and artifacts.phase1_diagnostics_markdown_path.exists()
    assert artifacts.phase2_diagnostics_json_path is not None and artifacts.phase2_diagnostics_json_path.exists()
    assert artifacts.phase2_diagnostics_markdown_path is not None and artifacts.phase2_diagnostics_markdown_path.exists()
    assert artifacts.phase3_diagnostics_json_path is not None and artifacts.phase3_diagnostics_json_path.exists()
    assert artifacts.phase3_diagnostics_markdown_path is not None and artifacts.phase3_diagnostics_markdown_path.exists()
    assert artifacts.performance_validation_json_path is not None and artifacts.performance_validation_json_path.exists()
    assert artifacts.performance_validation_markdown_path is not None and artifacts.performance_validation_markdown_path.exists()
    assert artifacts.report["module"] == "Active Trend Participation Engine"
    required = artifacts.report["promotion_report_format"]["required_metrics"]
    assert "trades_per_day" in required
    assert "reentry_performance" in required
    assert "atp_phase1_state_diagnostics" in artifacts.report["data_summary"]
    assert "atp_phase2_entry_diagnostics" in artifacts.report["data_summary"]
    assert "atp_phase3_timing_diagnostics" in artifacts.report["data_summary"]
    assert "atp_performance_validation" in artifacts.report["data_summary"]


def test_build_rolling_windows_uses_full_week_blocks_only() -> None:
    shared_start = datetime(2026, 2, 4, 0, 8, tzinfo=UTC)
    shared_end = datetime(2026, 3, 17, 4, 32, tzinfo=UTC)

    windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=7)

    assert windows
    assert windows[0].start_ts >= shared_start
    assert all(window.end_ts - window.start_ts == timedelta(days=7) for window in windows)
    assert windows[-1].end_ts == shared_end


def test_fragility_diagnosis_distinguishes_execution_fragility() -> None:
    assert _fragility_diagnosis({"trade_count": 5, "pre_cost_expectancy": 1.2, "post_cost_expectancy": -0.3}) == "execution_fragile"
    assert _fragility_diagnosis({"trade_count": 5, "pre_cost_expectancy": -0.2, "post_cost_expectancy": -0.5}) == "raw_signal_weakness"


def test_canary_signal_row_marks_experimental_paper_only_allowance() -> None:
    lane = CanaryLaneSpec(
        lane_id="lane1",
        lane_name="Lane 1",
        side="LONG",
        variant_id="trend_participation.pullback_continuation.long.conservative",
        quality_buckets=frozenset({"MEDIUM", "HIGH"}),
        quality_bucket_policy="MEDIUM_HIGH_ONLY",
    )
    row = _signal_row(
        decision=_decision(decision_id="d3", decision_minute=10, setup_signature="s", setup_state_signature="ss"),
        lane=lane,
        lane_id=lane.lane_id,
        lane_name=lane.lane_name,
        kill_switch_active=False,
    )

    assert row["experimental_status"] == "experimental_canary"
    assert row["paper_only"] is True
    assert row["signal_passed_flag"] is True
    assert row["live_eligible"] is False
    assert row["allow_block_reason"] == "allowed_no_conflict"
    assert row["override_reason"] == "paper_only_experimental_canary"


def test_canary_policy_blocks_when_kill_switch_is_active() -> None:
    decision = _decision(decision_id="d4", decision_minute=11, setup_signature="s", setup_state_signature="ss")

    allow_block_reason, override_reason, signal_passed = _decision_policy_row(decision=decision, kill_switch_active=True)

    assert allow_block_reason == "blocked_kill_switch"
    assert override_reason == "canary_kill_switch_active"
    assert signal_passed is False
