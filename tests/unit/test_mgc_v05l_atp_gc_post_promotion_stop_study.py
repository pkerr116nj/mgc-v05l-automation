from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.app.atp_gc_post_promotion_stop_study import (
    PromotedStopVariantSpec,
    _advance_promoted_stop,
    _completed_structure_context,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


def _minute_bars(prices: list[tuple[float, float, float, float]]) -> list[ResearchBar]:
    start = datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
    bars: list[ResearchBar] = []
    for index, (open_, high, low, close) in enumerate(prices):
        end_ts = start + timedelta(minutes=index + 1)
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
                source="test",
            )
        )
    return bars


def test_locked_floor_promoted_stop_respects_configured_lock_r() -> None:
    variant = PromotedStopVariantSpec(
        variant_id="control",
        label="control",
        notes="",
        stop_mode="locked_floor",
        initial_lock_r=0.35,
        update_basis="continuous",
    )

    stop_price, stage2, stage = _advance_promoted_stop(
        variant=variant,
        current_stop_price=99.0,
        entry_price=100.0,
        risk_points=2.0,
        mfe_points=2.5,
        current_bar_end_ts=datetime(2026, 1, 5, 14, 10, tzinfo=UTC),
        promotion_ts=datetime(2026, 1, 5, 14, 5, tzinfo=UTC),
        structure_state=None,
        stage2_activated=False,
        current_stage="promoted_initial",
    )

    assert round(stop_price, 6) == 100.7
    assert stage2 is False
    assert stage == "locked_floor"


def test_structure_buffer_promoted_stop_updates_from_completed_5m_structure() -> None:
    structure_state = _completed_structure_context(
        minute_bars=_minute_bars(
            [
                (100.0, 101.0, 99.9, 100.8),
                (100.8, 101.8, 100.7, 101.6),
                (101.6, 103.0, 101.4, 102.8),
                (102.8, 104.0, 102.4, 103.7),
                (103.7, 105.2, 103.5, 104.8),
            ]
        ),
        timeframe_minutes=5,
    )
    variant = PromotedStopVariantSpec(
        variant_id="structure_buffer",
        label="structure buffer",
        notes="",
        stop_mode="structure_buffer",
        initial_lock_r=0.15,
        update_basis="completed_5m",
        structure_timeframe_minutes=5,
        structure_lookback_bars=1,
        structure_buffer_r=0.20,
    )

    stop_price, _, stage = _advance_promoted_stop(
        variant=variant,
        current_stop_price=100.0,
        entry_price=100.0,
        risk_points=10.0,
        mfe_points=15.0,
        current_bar_end_ts=datetime(2026, 1, 5, 14, 5, tzinfo=UTC),
        promotion_ts=datetime(2026, 1, 5, 14, 1, tzinfo=UTC),
        structure_state=structure_state,
        stage2_activated=False,
        current_stage="promoted_initial",
    )

    assert round(stop_price, 6) == 101.5
    assert stage == "structure_buffer"


def test_two_stage_promoted_stop_can_widen_after_further_extension() -> None:
    structure_state = _completed_structure_context(
        minute_bars=_minute_bars(
            [
                (100.0, 101.0, 99.9, 100.8),
                (100.8, 101.8, 100.7, 101.6),
                (101.6, 103.0, 101.4, 102.8),
                (102.8, 104.0, 102.4, 103.7),
                (103.7, 105.2, 103.5, 104.8),
            ]
        ),
        timeframe_minutes=5,
    )
    variant = PromotedStopVariantSpec(
        variant_id="two_stage",
        label="two stage",
        notes="",
        stop_mode="two_stage",
        initial_lock_r=0.25,
        update_basis="continuous then completed_5m",
        structure_timeframe_minutes=5,
        second_stage_trigger_r=1.75,
        second_stage_lock_r=0.10,
        second_stage_structure_buffer_r=0.20,
        second_stage_lookback_bars=1,
    )

    stop_price, stage2, stage = _advance_promoted_stop(
        variant=variant,
        current_stop_price=102.5,
        entry_price=100.0,
        risk_points=10.0,
        mfe_points=18.0,
        current_bar_end_ts=datetime(2026, 1, 5, 14, 5, tzinfo=UTC),
        promotion_ts=datetime(2026, 1, 5, 14, 1, tzinfo=UTC),
        structure_state=structure_state,
        stage2_activated=False,
        current_stage="two_stage_controlled",
    )

    assert round(stop_price, 6) == 101.0
    assert stage2 is True
    assert stage == "two_stage_runner"
