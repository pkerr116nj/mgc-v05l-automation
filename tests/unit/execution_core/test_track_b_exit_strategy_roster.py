from __future__ import annotations

from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    BTC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    BTC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    BTC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1,
    MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1,
    MES_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    ES_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    ES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    ES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    GC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    GC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    GC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MBT_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MBT_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MBT_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MGC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MGC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
    MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1,
    MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1,
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
    MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    NQ_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    NQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    NQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    ZB_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    ZB_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    ZB_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    ZF_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    ZF_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    ZF_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    ZN_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    ZN_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    ZN_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    ZT_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    ZT_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    ZT_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    close_action_for_position_side,
    close_limit_from_profile,
    managed_close_limit_from_reference,
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


def test_validated_futures_diagnostic_timebox_profiles_resolve_by_policy_and_instrument() -> None:
    for symbol, profile_id, tick_size in (
        ("MES", MES_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.25"),
        ("GC", GC_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.1"),
        ("NQ", NQ_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.25"),
        ("ES", ES_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.25"),
        ("ZT", ZT_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.00390625"),
        ("ZF", ZF_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.0078125"),
        ("ZN", ZN_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.015625"),
        ("ZB", ZB_DIAGNOSTIC_TIMEBOX_3X5M_V1, "0.03125"),
        ("BTC", BTC_DIAGNOSTIC_TIMEBOX_3X5M_V1, "5"),
        ("MBT", MBT_DIAGNOSTIC_TIMEBOX_3X5M_V1, "5"),
    ):
        profile = resolve_track_b_exit_profile_for_position(
            instrument_family=symbol,
            managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        )

        assert profile.exit_profile_id == profile_id
        assert profile.instrument_family == symbol
        assert profile.tick_size == tick_size
        assert profile.required_completed_5m_bars == 3
        assert profile.paper_only is True
        assert profile.live_money_eligible is False
        assert profile.paper_proof_allowed is False
        assert profile.broad_cancel_allowed is False
        assert profile.global_flatten_allowed is False


def test_mnq_changeover_timebox_profile_resolves_by_policy_and_instrument() -> None:
    profile = resolve_track_b_exit_profile_for_position(
        instrument_family="MNQ",
        managed_exit_policy_id="CHANGEOVER_0300_LONG_TIMEBOX_6H_EXIT_V1",
    )

    assert profile.exit_profile_id == MNQ_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1
    assert profile.instrument_family == "MNQ"
    assert profile.required_completed_5m_bars == 72
    assert profile.paper_only is True
    assert profile.live_money_eligible is False


def test_changeover_continuation_profiles_cover_mnq_and_mes_timeboxes() -> None:
    mnq_0700 = resolve_track_b_exit_profile(MNQ_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1)
    mes_0300 = resolve_track_b_exit_profile(MES_CHANGEOVER_0300_LONG_TIMEBOX_6H_V1)
    mes_0700 = resolve_track_b_exit_profile(MES_CHANGEOVER_0700_LONG_TIMEBOX_4H_V1)

    assert mnq_0700.instrument_family == "MNQ"
    assert mnq_0700.managed_exit_policy_id == "CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1"
    assert mnq_0700.required_completed_5m_bars == 48
    assert mes_0300.instrument_family == "MES"
    assert mes_0300.required_completed_5m_bars == 72
    assert mes_0700.instrument_family == "MES"
    assert mes_0700.managed_exit_policy_id == "CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1"


def test_paper_active_evidence_profiles_cover_mnq_and_mes_60m_timeboxes() -> None:
    mnq = resolve_track_b_exit_profile(MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1)
    mes = resolve_track_b_exit_profile(MES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1)

    assert mnq.instrument_family == "MNQ"
    assert mes.instrument_family == "MES"
    assert mnq.managed_exit_policy_id == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert mes.managed_exit_policy_id == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert mnq.required_completed_5m_bars == 12
    assert mes.required_completed_5m_bars == 12
    assert mnq.price_offset_ticks == ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS
    assert mes.max_slippage_ticks == ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS
    assert mnq.live_money_eligible is False
    assert mes.paper_proof_allowed is False


def test_active_evidence_managed_close_uses_more_aggressive_marketable_offset_than_entry() -> None:
    profile = resolve_track_b_exit_profile(MNQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1)

    assert profile.price_offset_ticks > 4
    assert close_limit_from_profile(latest_price="30525.00", side="LONG", profile=profile) == "30523"


def test_globex_active_evidence_profiles_cover_mnq_and_mes_15m_test_timeboxes() -> None:
    mnq = resolve_track_b_exit_profile(MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1)
    mes = resolve_track_b_exit_profile(MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1)

    assert mnq.instrument_family == "MNQ"
    assert mes.instrument_family == "MES"
    assert mnq.managed_exit_policy_id == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert mes.managed_exit_policy_id == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert mnq.required_completed_5m_bars == 3
    assert mes.required_completed_5m_bars == 3
    assert mnq.live_money_eligible is False
    assert mes.paper_proof_allowed is False


def test_batch1_active_evidence_profiles_cover_validated_symbols() -> None:
    for symbol, us_profile_id, globex_profile_id, tick_size in (
        ("MGC", MGC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, MGC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.1"),
        ("GC", GC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, GC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.1"),
        ("NQ", NQ_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, NQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.25"),
        ("ES", ES_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, ES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.25"),
        ("ZT", ZT_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, ZT_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.00390625"),
        ("ZF", ZF_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, ZF_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.0078125"),
        ("ZN", ZN_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, ZN_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.015625"),
        ("ZB", ZB_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, ZB_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "0.03125"),
        ("BTC", BTC_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, BTC_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "5"),
        ("MBT", MBT_US_ACTIVE_EVIDENCE_TIMEBOX_60M_V1, MBT_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1, "5"),
    ):
        us_profile = resolve_track_b_exit_profile_for_position(
            instrument_family=symbol,
            managed_exit_policy_id="US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        )
        globex_profile = resolve_track_b_exit_profile_for_position(
            instrument_family=symbol,
            managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        )

        assert us_profile.exit_profile_id == us_profile_id
        assert globex_profile.exit_profile_id == globex_profile_id
        assert us_profile.tick_size == tick_size
        assert globex_profile.tick_size == tick_size
        assert us_profile.price_offset_ticks == ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS
        assert globex_profile.max_slippage_ticks == ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS
        assert us_profile.live_money_eligible is False
        assert globex_profile.paper_proof_allowed is False
        assert globex_profile.broad_cancel_allowed is False
        assert globex_profile.global_flatten_allowed is False


def test_legacy_globex_active_evidence_60m_profiles_remain_resolvable() -> None:
    mnq = resolve_track_b_exit_profile(MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1)
    mes = resolve_track_b_exit_profile(MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1)

    assert mnq.managed_exit_policy_id == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert mes.managed_exit_policy_id == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"


def test_managed_close_reprice_escalates_within_cap_and_does_not_loop_unbounded() -> None:
    first = managed_close_limit_from_reference(
        reference_price="30525",
        close_action="SELL",
        tick_size="0.25",
        base_offset_ticks=8,
        max_slippage_ticks=16,
        reprice_attempts=0,
        reprice_escalation_ticks=4,
    )
    later = managed_close_limit_from_reference(
        reference_price="30525",
        close_action="SELL",
        tick_size="0.25",
        base_offset_ticks=8,
        max_slippage_ticks=16,
        reprice_attempts=9,
        reprice_escalation_ticks=4,
    )

    assert first["classification"] == "MANAGED_CLOSE_PRICED"
    assert first["limit_price"] == "30523"
    assert later["limit_price"] == "30521"
    assert later["marketable_limit_offset_ticks"] == 16.0


def test_managed_close_blocks_stale_reference() -> None:
    priced = managed_close_limit_from_reference(
        reference_price="30525",
        close_action="SELL",
        tick_size="0.25",
        base_offset_ticks=8,
        max_slippage_ticks=16,
        reference_age_seconds=121.0,
        stale_reference_seconds=120,
    )

    assert priced["classification"] == "MANAGED_CLOSE_PRICING_BLOCKED"
    assert priced["stale_reference_blocker"] == "MANAGED_CLOSE_REFERENCE_STALE"
