from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from mgc_v05l.app.operator_dashboard import _decode_signal_audit_row
from mgc_v05l.app.paper_engine_silent_failure_audit import _summarize_signal_payload_rows
from mgc_v05l.app.probationary_runtime import _runtime_atp_companion_pre_timing_signal_row
from mgc_v05l.research.trend_participation.models import AtpEntryState


def test_runtime_atp_pre_timing_signal_row_captures_entry_blocker() -> None:
    spec = SimpleNamespace(
        lane_id="atp_lane_gc",
        display_name="ATP GC",
        experimental_status="paper_candidate",
        paper_only=True,
        non_approved=True,
        quality_bucket_policy="VWAP_FAVORABLE_ONLY",
    )
    entry_state = AtpEntryState(
        instrument="GC",
        decision_ts=datetime(2026, 4, 17, 9, 30, tzinfo=timezone.utc),
        session_date=date(2026, 4, 17),
        session_segment="ASIA",
        family_name="atp_v1_long_pullback_continuation",
        bias_state="LONG_BIAS",
        pullback_state="NORMAL_PULLBACK",
        continuation_trigger_state="CONTINUATION_TRIGGER_NOT_CONFIRMED",
        entry_state="ENTRY_BLOCKED",
        blocker_codes=("ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED",),
        primary_blocker="ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED",
        raw_candidate=True,
        trigger_confirmed=False,
        entry_eligible=False,
        session_allowed=True,
        warmup_complete=True,
        runtime_ready=True,
        position_flat=True,
        one_position_rule_clear=True,
        setup_signature="sig",
        setup_state_signature="state_sig",
        setup_quality_score=1.25,
        setup_quality_bucket="MEDIUM",
        feature_snapshot={"regime_bucket": "TREND"},
        side="LONG",
    )

    row = _runtime_atp_companion_pre_timing_signal_row(
        spec=spec,
        entry_state=entry_state,
        observed_instruments=("GC",),
    )

    assert row["decision"] == "blocked"
    assert row["signal_passed_flag"] is False
    assert row["rejection_reason_code"] == "ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED"
    assert row["atp_decision_stage"] == "entry_state_blocked_before_timing"
    assert row["raw_setup_candidate"] is True
    assert row["feature_snapshot"]["atp_decision_stage"] == "entry_state_blocked_before_timing"
    assert row["feature_snapshot"]["blocker_codes"] == ["ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED"]


def test_decode_signal_audit_row_understands_atp_rows_without_payload_json() -> None:
    timestamp = "2026-04-17T09:30:00+00:00"
    decoded = _decode_signal_audit_row(
        {
            "family": "atp_v1_long_pullback_continuation",
            "side": "LONG",
            "signal_passed_flag": False,
            "signal_timestamp": timestamp,
            "raw_setup_candidate": True,
            "rejection_reason_code": "ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED",
            "feature_snapshot": {"raw_setup_candidate": True},
            "atp_decision_stage": "entry_state_blocked_before_timing",
        },
        bars_by_id={},
    )

    assert decoded["timestamp"] == timestamp
    assert decoded["signal_family"] == "atp_v1_long_pullback_continuation"
    assert decoded["raw_setup_candidate"] is True
    assert decoded["long_entry_raw"] is True
    assert decoded["actionable_entry"] is False
    assert decoded["payload"]["atp_decision_stage"] == "entry_state_blocked_before_timing"


def test_summarize_signal_payload_rows_counts_blockers_and_decision_stages() -> None:
    summary = _summarize_signal_payload_rows(
        [
            '{"signal_passed_flag": false, "raw_setup_candidate": true, "rejection_reason_code": "ATP_TIMING_5M_CONTEXT_NOT_READY", "atp_decision_stage": "entry_state_blocked_before_timing"}',
            '{"signal_passed_flag": false, "raw_setup_candidate": true, "block_reason": "ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED", "timing_state": "ATP_TIMING_WAITING"}',
            '{"signal_passed_flag": true, "raw_setup_candidate": true}',
        ]
    )

    assert summary["actionable_signals_today"] == 1
    assert summary["raw_setup_candidate_signals_today"] == 3
    assert summary["blocked_reason_counts_today"][0]["reason"] == "ATP_TIMING_5M_CONTEXT_NOT_READY"
    assert {row["stage"] for row in summary["decision_stage_counts_today"]} == {
        "entry_state_blocked_before_timing",
        "ATP_TIMING_WAITING",
    }
