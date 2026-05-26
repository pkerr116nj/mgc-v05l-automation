from __future__ import annotations

from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    close_action_for_position_side,
    close_limit_from_profile,
    resolve_track_b_exit_profile,
    resolve_track_b_exit_profile_for_position,
)


def test_mnq_snap_turn_timebox_profile_resolves_from_exit_roster() -> None:
    profile = resolve_track_b_exit_profile(MNQ_SNAP_TURN_TIMEBOX_3X5M_V1)

    assert profile.managed_exit_policy_id == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1
    assert profile.required_completed_5m_bars == 3
    assert profile.instrument_family == "MNQ"
    assert profile.paper_only is True
    assert profile.live_money_eligible is False
    assert profile.paper_proof_allowed is False
    assert profile.broad_cancel_allowed is False
    assert profile.global_flatten_allowed is False


def test_profile_derives_exact_close_side_and_limit() -> None:
    profile = resolve_track_b_exit_profile(MNQ_SNAP_TURN_TIMEBOX_3X5M_V1)

    assert close_action_for_position_side("LONG") == "SELL"
    assert close_limit_from_profile(latest_price="29954.75", side="LONG", profile=profile) == "29954.25"


def test_mgc_diagnostic_timebox_profile_resolves_by_policy_and_instrument() -> None:
    profile = resolve_track_b_exit_profile_for_position(
        instrument_family="MGC",
        managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    )

    assert profile.exit_profile_id == MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1
    assert profile.instrument_family == "MGC"
    assert profile.managed_exit_policy_id == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1
    assert profile.required_completed_5m_bars == 3
    assert close_limit_from_profile(latest_price="4534.9", side="LONG", profile=profile) == "4534.7"
