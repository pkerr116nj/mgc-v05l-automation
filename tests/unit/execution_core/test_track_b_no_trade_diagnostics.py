from __future__ import annotations

import json
from datetime import datetime, timezone

from mgc_v05l.domain.models import SignalPacket
from mgc_v05l.execution_core.track_b_no_trade_diagnostics import (
    NoTradeFinalDecision,
    build_no_trade_diagnostic,
    write_no_trade_diagnostic,
)


def signal_packet(**overrides: object) -> SignalPacket:
    values: dict[str, object] = {
        "bar_id": "bar-1",
        "bull_snap_downside_stretch_ok": False,
        "bull_snap_range_ok": False,
        "bull_snap_body_ok": False,
        "bull_snap_close_strong": False,
        "bull_snap_velocity_ok": False,
        "bull_snap_reversal_bar": False,
        "bull_snap_location_ok": False,
        "bull_snap_raw": False,
        "bull_snap_turn_candidate": False,
        "first_bull_snap_turn": False,
        "below_vwap_recently": False,
        "reclaim_range_ok": False,
        "reclaim_vol_ok": False,
        "reclaim_color_ok": False,
        "reclaim_close_ok": False,
        "asia_reclaim_bar_raw": False,
        "asia_hold_bar": False,
        "asia_hold_close_vwap_ok": False,
        "asia_hold_low_ok": False,
        "asia_hold_bar_ok": False,
        "asia_acceptance_bar": False,
        "asia_acceptance_close_high_ok": False,
        "asia_acceptance_close_vwap_ok": False,
        "asia_acceptance_bar_ok": False,
        "asia_vwap_long_signal": False,
        "midday_pause_resume_long_turn_candidate": False,
        "us_late_pause_resume_long_turn_candidate": False,
        "us_late_failed_move_reversal_long_turn_candidate": False,
        "us_late_breakout_retest_hold_long_turn_candidate": False,
        "asia_early_breakout_retest_hold_long_turn_candidate": False,
        "asia_early_normal_breakout_retest_hold_long_turn_candidate": False,
        "asia_late_pause_resume_long_turn_candidate": False,
        "asia_late_flat_pullback_pause_resume_long_turn_candidate": False,
        "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate": False,
        "bear_snap_up_stretch_ok": False,
        "bear_snap_range_ok": False,
        "bear_snap_body_ok": False,
        "bear_snap_close_weak": False,
        "bear_snap_velocity_ok": False,
        "bear_snap_reversal_bar": False,
        "bear_snap_location_ok": False,
        "bear_snap_raw": False,
        "bear_snap_turn_candidate": False,
        "first_bear_snap_turn": False,
        "derivative_bear_slope_ok": False,
        "derivative_bear_curvature_ok": False,
        "derivative_bear_turn_candidate": False,
        "derivative_bear_additive_turn_candidate": False,
        "midday_compressed_failed_move_reversal_short_turn_candidate": False,
        "midday_compressed_rebound_failed_move_reversal_short_turn_candidate": False,
        "midday_expanded_pause_resume_short_turn_candidate": False,
        "midday_compressed_pause_resume_short_turn_candidate": False,
        "midday_pause_resume_short_turn_candidate": False,
        "london_late_pause_resume_short_turn_candidate": False,
        "asia_early_expanded_breakout_retest_hold_short_turn_candidate": False,
        "asia_early_compressed_pause_resume_short_turn_candidate": False,
        "asia_early_pause_resume_short_turn_candidate": False,
        "long_entry_raw": False,
        "short_entry_raw": False,
        "recent_long_setup": False,
        "recent_short_setup": False,
        "long_entry": False,
        "short_entry": False,
        "long_entry_source": None,
        "short_entry_source": None,
    }
    values.update(overrides)
    return SignalPacket(**values)


def build_record(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "lane_id": "mgc_1x_all_lanes__asia_early_long",
        "symbol": "MGC",
        "session": "ASIA_EARLY",
        "bar_timestamp": datetime(2026, 5, 19, 1, 5, tzinfo=timezone.utc),
        "bar_id": "MGC-20260519-0105",
        "session_allowed": True,
        "market_data_fresh": True,
        "strategy_evaluated": True,
        "signal_packet": signal_packet(),
    }
    values.update(overrides)
    return build_no_trade_diagnostic(**values)


def test_no_setup_record_explains_no_order_intent() -> None:
    record = build_record(setup_detected=False, blocker_reason="no_setup_detected")

    assert record["final_decision"] == "NO_SETUP"
    assert record["setup_detected"] is False
    assert record["predicates_passed"] == []
    assert record["blocker_reason"] == "no_setup_detected"
    assert record["live_money_eligible"] is False


def test_setup_filtered_record_lists_predicates_and_near_miss_score() -> None:
    packet = signal_packet(
        asia_early_pause_resume_short_turn_candidate=True,
        short_entry_raw=True,
        short_entry=False,
    )

    record = build_record(
        signal_packet=packet,
        setup_detected=True,
        blocker_reason="entry_signal_filtered_or_controls_not_satisfied",
    )

    assert record["final_decision"] == "FILTER_REJECTED"
    assert "asia_early_pause_resume_short_turn_candidate" in record["predicates_passed"]
    assert "short_entry" in record["predicates_failed"]
    assert isinstance(record["near_miss_score"], float)
    assert record["would_trade_if_relaxed"]["safe_to_infer"] is True


def test_submit_blocker_classifies_governance_block() -> None:
    packet = signal_packet(long_entry_raw=True, long_entry=True, long_entry_source="asiaEarlyLong")

    record = build_record(
        signal_packet=packet,
        setup_detected=True,
        submit_blocker="paper strategy monitor bridge_allowed=false",
    )

    assert record["final_decision"] == "GOVERNANCE_BLOCKED"
    assert record["blocker_reason"] == "paper strategy monitor bridge_allowed=false"


def test_exposure_blocker_classifies_exposure_block() -> None:
    packet = signal_packet(short_entry_raw=True, short_entry=True, short_entry_source="asiaEarlyShort")

    record = build_record(
        signal_packet=packet,
        setup_detected=True,
        blocker_reason="opposite-side position conflict",
    )

    assert record["final_decision"] == "EXPOSURE_BLOCKED"


def test_order_intent_created_record_is_explicit() -> None:
    packet = signal_packet(long_entry_raw=True, long_entry=True, long_entry_source="asiaEarlyLong")

    record = build_record(
        signal_packet=packet,
        setup_detected=True,
        order_intent_created=True,
        order_intent_id="bar-1|BUY_TO_OPEN",
    )

    assert record["final_decision"] == "ORDER_INTENT_CREATED"
    assert record["order_intent_id"] == "bar-1|BUY_TO_OPEN"


def test_explicit_would_route_record_is_supported_without_broker_action() -> None:
    packet = signal_packet(long_entry_raw=True, long_entry=True, long_entry_source="asiaEarlyLong")

    record = build_record(
        signal_packet=packet,
        setup_detected=True,
        final_decision=NoTradeFinalDecision.WOULD_ROUTE,
    )

    assert record["final_decision"] == "WOULD_ROUTE"


def test_writer_updates_latest_and_jsonl(tmp_path) -> None:
    record = build_record(setup_detected=False, blocker_reason="no_setup_detected")

    paths = write_no_trade_diagnostic(record, diagnostics_root=tmp_path / "diag")

    latest = json.loads(paths["latest"].read_text(encoding="utf-8"))
    jsonl_rows = [json.loads(line) for line in paths["jsonl"].read_text(encoding="utf-8").splitlines()]
    assert latest["lane_id"] == "mgc_1x_all_lanes__asia_early_long"
    assert jsonl_rows[-1]["final_decision"] == "NO_SETUP"
