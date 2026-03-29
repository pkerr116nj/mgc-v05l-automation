from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from mgc_v05l.app.missed_entry_discovery import (
    MissedTurnObservation,
    OrderedBarContext,
    _build_candidate_family_rows,
    _ema_relation,
    _recent_path_shape,
)


def _bar(
    *,
    open_price: str,
    close_price: str,
    vwap: str,
    curvature: str,
) -> OrderedBarContext:
    return OrderedBarContext(
        timestamp=datetime(2026, 1, 1, 10, 0),
        open=Decimal(open_price),
        high=max(Decimal(open_price), Decimal(close_price)) + Decimal("1"),
        low=min(Decimal(open_price), Decimal(close_price)) - Decimal("1"),
        close=Decimal(close_price),
        atr=Decimal("4"),
        vwap=Decimal(vwap),
        turn_ema_fast=Decimal("100"),
        turn_ema_slow=Decimal("99"),
        normalized_slope=Decimal("0.2"),
        normalized_curvature=Decimal(curvature),
        range_expansion_ratio=Decimal("1.0"),
        volatility_regime="NORMAL",
    )


def test_recent_path_shape_classifies_pause_resume_short() -> None:
    prior = [
        _bar(open_price="100", close_price="101", vwap="100.5", curvature="-0.1"),
        _bar(open_price="101", close_price="100.5", vwap="100.4", curvature="0.2"),
        _bar(open_price="100.5", close_price="101.5", vwap="100.6", curvature="0.1"),
    ]
    assert _recent_path_shape(direction="SHORT", prior=prior) == "pause_rebound_resume_short"


def test_ema_relation_tracks_price_structure() -> None:
    assert _ema_relation(Decimal("102"), Decimal("101"), Decimal("100")) == "above_both_fast_gt_slow"
    assert _ema_relation(Decimal("98"), Decimal("99"), Decimal("100")) == "below_both_fast_lt_slow"


def test_candidate_rows_rank_non_noise_family_first() -> None:
    observations = [
        MissedTurnObservation(
            timestamp=datetime(2026, 1, 1, 10, 0),
            session_phase="US_MIDDAY",
            direction_of_turn="SHORT",
            participation_classification="no_trade",
            local_turn_type="pivot_high_reversal",
            signal_family_if_any=None,
            move_5bar=Decimal("8"),
            move_10bar=Decimal("12"),
            move_20bar=Decimal("15"),
            mfe_20bar=Decimal("20"),
            mae_20bar=Decimal("4"),
            atr=Decimal("4"),
            vwap_distance_atr=Decimal("1.2"),
            vwap_bucket=">=1atr",
            ema_relation="pullback_above_slow",
            slope_bucket="SLOPE_FLAT",
            curvature_bucket="CURVATURE_NEG",
            derivative_bucket="SLOPE_FLAT|CURVATURE_NEG",
            expansion_state="not_expanded",
            recent_path_shape="pause_rebound_resume_short",
            followthrough_quality="strong",
            volatility_regime="NORMAL",
            prior_return_signs_3="UDU",
            prior_return_signs_5="UDUDU",
            prior_vwap_extension_signs_3="+-+",
            prior_curvature_signs_3="+-+",
            one_bar_rebound_before_signal=True,
            two_bar_rebound_before_signal=False,
            prior_3_any_below_vwap=True,
            prior_3_all_above_vwap=False,
            prior_3_any_positive_curvature=True,
            signal_close_location=Decimal("0.2"),
            signal_body_to_range=Decimal("0.8"),
        )
        for _ in range(25)
    ]
    rows = _build_candidate_family_rows(observations)
    assert rows[0]["session_phase"] == "US_MIDDAY"
    assert rows[0]["noise_trap_flag"] == "false"
