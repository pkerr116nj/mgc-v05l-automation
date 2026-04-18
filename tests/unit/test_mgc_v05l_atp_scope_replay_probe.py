from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.research.trend_participation.models import AtpEntryState, AtpTimingState, ConflictOutcome, ResearchBar, TradeRecord
from mgc_v05l.app.atp_scope_replay_probe import (
    ConfirmationSizingProfile,
    PreConfirmationRiskProfile,
    _apply_confirmation_sizing_profile,
    _apply_pre_confirmation_risk_profile,
    _candidate_by_id,
    _enrich_timing_states_with_entry_signatures,
    _merged_trade_intervals,
    _merged_replay_intervals,
)


def _entry_state(*, minute: int, signature: str, state_signature: str) -> AtpEntryState:
    ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=minute)
    return AtpEntryState(
        instrument="GC",
        decision_ts=ts,
        session_date=ts.date(),
        session_segment="ASIA",
        family_name="atp_v1_long_pullback_continuation",
        bias_state="LONG_BIAS",
        pullback_state="NORMAL_PULLBACK",
        continuation_trigger_state="CONTINUATION_TRIGGER_CONFIRMED",
        entry_state="ENTRY_ELIGIBLE",
        blocker_codes=(),
        primary_blocker=None,
        raw_candidate=True,
        trigger_confirmed=True,
        entry_eligible=True,
        session_allowed=True,
        warmup_complete=True,
        runtime_ready=True,
        position_flat=True,
        one_position_rule_clear=True,
        setup_signature=signature,
        setup_state_signature=state_signature,
        setup_quality_score=1.0,
        setup_quality_bucket="HIGH",
        feature_snapshot={},
        side="LONG",
    )


def _timing_state(*, minute: int, entry_minute: int, signature: str | None = None) -> AtpTimingState:
    decision_ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=minute)
    entry_ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=entry_minute)
    snapshot = {"setup_signature": signature} if signature is not None else {}
    return AtpTimingState(
        instrument="GC",
        decision_ts=decision_ts,
        session_date=decision_ts.date(),
        session_segment="ASIA",
        family_name="atp_v1_long_pullback_continuation",
        context_entry_state="ENTRY_ELIGIBLE",
        timing_state="ATP_TIMING_CONFIRMED",
        vwap_price_quality_state="VWAP_FAVORABLE",
        blocker_codes=(),
        primary_blocker=None,
        setup_armed=True,
        timing_confirmed=True,
        executable_entry=True,
        invalidated_before_entry=False,
        setup_armed_but_not_executable=False,
        entry_executed=False,
        timing_bar_ts=entry_ts,
        entry_ts=entry_ts,
        entry_price=100.0,
        feature_snapshot=snapshot,
        side="LONG",
    )


def test_enrich_timing_states_restores_setup_state_signature() -> None:
    entry = _entry_state(minute=0, signature="gc-reset", state_signature="state-a")
    timing = _timing_state(minute=0, entry_minute=2)

    [enriched] = _enrich_timing_states_with_entry_signatures(
        timing_states=[timing],
        entry_states=[entry],
    )

    assert enriched.feature_snapshot["setup_signature"] == "gc-reset"
    assert enriched.feature_snapshot["setup_state_signature"] == "state-a"


def test_merged_replay_intervals_collapses_overlapping_windows() -> None:
    timing_states = [
        _timing_state(minute=0, entry_minute=2, signature="a"),
        _timing_state(minute=1, entry_minute=3, signature="b"),
        _timing_state(minute=20, entry_minute=22, signature="c"),
    ]

    intervals = _merged_replay_intervals(
        timing_states=timing_states,
        window_minutes=5,
    )

    assert len(intervals) == 2
    assert intervals[0][0] < intervals[0][1]
    assert intervals[1][0] < intervals[1][1]


def test_merged_trade_intervals_collapses_overlapping_trade_windows() -> None:
    trade_a = _trade_record()
    trade_b = TradeRecord(**{**trade_a.__dict__, "decision_id": "gc-2", "decision_ts": trade_a.decision_ts + timedelta(minutes=2), "entry_ts": trade_a.entry_ts + timedelta(minutes=2), "exit_ts": trade_a.exit_ts + timedelta(minutes=2)})
    trade_c = TradeRecord(**{**trade_a.__dict__, "decision_id": "gc-3", "decision_ts": trade_a.decision_ts + timedelta(minutes=20), "entry_ts": trade_a.entry_ts + timedelta(minutes=20), "exit_ts": trade_a.exit_ts + timedelta(minutes=20)})

    intervals = _merged_trade_intervals(trades=[trade_a, trade_b, trade_c])

    assert len(intervals) == 2
    assert intervals[0][0] < intervals[0][1]
    assert intervals[1][0] < intervals[1][1]


def _trade_record() -> TradeRecord:
    ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
    return TradeRecord(
        instrument="GC",
        variant_id="atp_v1_long_pullback_continuation.long.base",
        family="atp_v1_long_pullback_continuation",
        side="LONG",
        live_eligible=True,
        shadow_only=False,
        conflict_outcome=ConflictOutcome.NO_CONFLICT,
        decision_id="gc-1",
        decision_ts=ts,
        entry_ts=ts,
        exit_ts=ts + timedelta(minutes=3),
        entry_price=100.0,
        exit_price=102.0,
        stop_price=99.0,
        target_price=101.6,
        pnl_points=2.0,
        gross_pnl_cash=20.0,
        pnl_cash=18.5,
        fees_paid=1.5,
        slippage_cost=0.0,
        mfe_points=2.2,
        mae_points=0.3,
        bars_held_1m=3,
        hold_minutes=3.0,
        exit_reason="target_momentum_fade",
        is_reentry=False,
        reentry_type="NONE",
        stopout=False,
        setup_signature="gc-setup",
        setup_quality_bucket="HIGH",
        session_segment="ASIA",
        regime_bucket="TREND",
        volatility_bucket="NORMAL",
    )


def _bars() -> list[ResearchBar]:
    start = datetime(2026, 1, 5, 13, 59, tzinfo=UTC)
    prices = [
        (100.0, 100.2, 99.9, 100.0),
        (100.1, 100.6, 100.02, 100.3),
        (100.4, 100.85, 100.25, 100.7),
        (100.8, 102.2, 100.75, 102.0),
    ]
    bars: list[ResearchBar] = []
    for minute, (open_, high, low, close) in enumerate(prices):
        end_ts = start + timedelta(minutes=minute + 1)
        bars.append(
            ResearchBar(
                instrument="GC",
                timeframe="1m",
                start_ts=end_ts - timedelta(minutes=1),
                end_ts=end_ts,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=100,
                session_label="ASIA_LATE",
                session_segment="ASIA",
            )
        )
    return bars


def test_apply_confirmation_sizing_profile_adds_only_after_confirmation() -> None:
    trade = _trade_record()
    bars = _bars()
    profile = ConfirmationSizingProfile(
        probe_size_fraction=0.5,
        confirmation_add_size_fraction=0.5,
        confirmation_add_candidate=_candidate_by_id("promotion_1_050r_neutral_plus"),
    )

    adjusted_trades, summary = _apply_confirmation_sizing_profile(
        trades=[trade],
        bars_1m=bars,
        point_value=10.0,
        profile=profile,
    )

    assert len(adjusted_trades) == 1
    adjusted_trade = adjusted_trades[0]
    assert round(adjusted_trade.pnl_cash, 4) == 16.0
    assert summary["confirmation_add_trade_count"] == 1
    assert round(summary["confirmation_add_net_pnl"], 4) == 6.75


def test_apply_confirmation_sizing_profile_can_reduce_probe_without_add() -> None:
    trade = _trade_record()
    profile = ConfirmationSizingProfile(
        probe_size_fraction=0.5,
        confirmation_add_size_fraction=0.0,
        confirmation_add_candidate=None,
    )

    adjusted_trades, summary = _apply_confirmation_sizing_profile(
        trades=[trade],
        bars_1m=_bars(),
        point_value=10.0,
        profile=profile,
    )

    assert round(adjusted_trades[0].pnl_cash, 4) == 9.25
    assert summary["confirmation_add_trade_count"] == 0
    assert summary["confirmation_add_candidate_id"] is None


def test_apply_confirmation_sizing_profile_does_not_add_on_short_trade() -> None:
    trade = TradeRecord(**{**_trade_record().__dict__, "side": "SHORT"})
    profile = ConfirmationSizingProfile(
        probe_size_fraction=0.5,
        confirmation_add_size_fraction=0.5,
        confirmation_add_candidate=_candidate_by_id("promotion_1_050r_neutral_plus"),
    )

    adjusted_trades, summary = _apply_confirmation_sizing_profile(
        trades=[trade],
        bars_1m=_bars(),
        point_value=10.0,
        profile=profile,
    )

    assert round(adjusted_trades[0].pnl_cash, 4) == 9.25
    assert summary["confirmation_add_trade_count"] == 0


def test_apply_pre_confirmation_risk_profile_tightens_stop_before_confirmation() -> None:
    trade = _trade_record()
    bars = _bars()
    bars[1] = ResearchBar(
        instrument=bars[1].instrument,
        timeframe=bars[1].timeframe,
        start_ts=bars[1].start_ts,
        end_ts=bars[1].end_ts,
        open=bars[1].open,
        high=bars[1].high,
        low=99.4,
        close=bars[1].close,
        volume=bars[1].volume,
        session_label=bars[1].session_label,
        session_segment=bars[1].session_segment,
    )
    profile = PreConfirmationRiskProfile(
        pre_confirmation_stop_r_multiple=0.5,
        confirmation_release_candidate=_candidate_by_id("promotion_1_075r_favorable_only"),
    )

    adjusted_trades, summary = _apply_pre_confirmation_risk_profile(
        trades=[trade],
        bars_1m=bars,
        point_value=10.0,
        profile=profile,
    )

    adjusted = adjusted_trades[0]
    assert adjusted.exit_reason == "pre_confirmation_tight_stop"
    assert adjusted.exit_ts == datetime(2026, 1, 5, 14, 1, tzinfo=UTC)
    assert round(adjusted.pnl_cash, 4) == -6.5
    assert summary["pre_confirmation_stopout_trade_count"] == 1
