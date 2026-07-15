from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from mgc_v05l.domain.models import SignalPacket
import mgc_v05l.execution_core.track_b_no_trade_diagnostics as no_trade_diag
from mgc_v05l.execution_core.track_b_no_trade_diagnostics import (
    NoCandidateReasonCode,
    NoCandidateWindowAggregator,
    NoTradeFinalDecision,
    classify_no_candidate_reason,
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


def test_no_candidate_reason_taxonomy_covers_required_codes() -> None:
    base_time = datetime(2026, 5, 19, 1, 5, tzinfo=timezone.utc)
    examples = [
        build_record(blocker_reason="no_setup_detected"),
        build_record(session_allowed=False, blocker_reason="session_not_allowed"),
        build_record(blocker_reason="warmup_incomplete", extra={"warmup_complete": False}),
        build_record(blocker_reason="entry_signal_filtered_or_controls_not_satisfied", setup_detected=True),
        build_record(blocker_reason="long_entry_side_not_allowed"),
        build_record(blocker_reason="operator_halt", extra={"operator_halt": True}),
        build_record(blocker_reason="same_underlying_entry_hold", extra={"same_underlying_entry_hold": True}),
        build_record(
            bar_timestamp=base_time + timedelta(minutes=1),
            strategy_evaluated=False,
            blocker_reason="context_feature_history_not_ready",
        ),
    ]

    assert {classify_no_candidate_reason(record) for record in examples} >= {
        NoCandidateReasonCode.NO_SETUP.value,
        NoCandidateReasonCode.SESSION_DISALLOWED.value,
        NoCandidateReasonCode.WARMUP_INCOMPLETE.value,
        NoCandidateReasonCode.MIN_EVIDENCE_NOT_MET.value,
        NoCandidateReasonCode.SIDE_DISABLED.value,
        NoCandidateReasonCode.COOLDOWN_ACTIVE.value,
        NoCandidateReasonCode.SAME_UNDERLYING_HOLD.value,
        NoCandidateReasonCode.MISSING_CONTEXT.value,
    }


def test_no_candidate_aggregator_emits_one_summary_per_window(tmp_path) -> None:
    root = tmp_path / "diag"
    aggregator = NoCandidateWindowAggregator(diagnostics_root=root, window_seconds=300)
    start = datetime(2026, 5, 19, 1, 0, tzinfo=timezone.utc)

    assert aggregator.observe(build_record(bar_timestamp=start, blocker_reason="no_setup_detected")) is None
    assert aggregator.observe(
        build_record(
            bar_timestamp=start + timedelta(minutes=1),
            session_allowed=False,
            blocker_reason="session_not_allowed",
        )
    ) is None

    emitted = aggregator.observe(
        build_record(
            bar_timestamp=start + timedelta(minutes=5),
            blocker_reason="warmup_incomplete",
            extra={"warmup_complete": False, "warmup_bars_observed": 2, "warmup_bars_required": 20},
        )
    )

    assert emitted is not None
    assert emitted["bars_evaluated"] == 2
    assert emitted["primary_no_candidate_reason_counts"] == {
        "NO_SETUP": 1,
        "SESSION_DISALLOWED": 1,
    }
    latest = json.loads((root / "latest_no_candidate_summary.json").read_text(encoding="utf-8"))
    rows = (root / "no_candidate_summary.jsonl").read_text(encoding="utf-8").splitlines()
    assert latest["schema_version"] == "track_b_no_candidate_observability_v1"
    assert len(rows) == 1


def test_no_candidate_aggregator_does_not_emit_per_bar(monkeypatch, tmp_path) -> None:
    writes: list[dict[str, object]] = []

    def fake_write(payload: dict[str, object], **_: object) -> dict[str, object]:
        writes.append(payload)
        return {}

    monkeypatch.setattr(no_trade_diag, "write_no_candidate_summary", fake_write)
    aggregator = NoCandidateWindowAggregator(diagnostics_root=tmp_path, window_seconds=300)
    start = datetime(2026, 5, 19, 1, 0, tzinfo=timezone.utc)

    for minute in range(4):
        aggregator.observe(build_record(bar_timestamp=start + timedelta(minutes=minute), blocker_reason="no_setup_detected"))

    assert writes == []
    aggregator.observe(build_record(bar_timestamp=start + timedelta(minutes=5), blocker_reason="no_setup_detected"))
    assert len(writes) == 1


def test_no_candidate_aggregator_ignores_candidate_payload_without_mutation(tmp_path) -> None:
    aggregator = NoCandidateWindowAggregator(diagnostics_root=tmp_path, window_seconds=300)
    record = build_record(
        setup_detected=True,
        order_intent_created=True,
        order_intent_id="bar-1|BUY_TO_OPEN",
    )

    assert aggregator.observe(record) is None
    assert aggregator.flush() is None
    assert record["final_decision"] == "ORDER_INTENT_CREATED"
    assert not (tmp_path / "latest_no_candidate_summary.json").exists()
    assert not (tmp_path / "no_candidate_summary.jsonl").exists()
