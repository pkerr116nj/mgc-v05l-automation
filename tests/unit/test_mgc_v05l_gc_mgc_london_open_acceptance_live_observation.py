from __future__ import annotations

import json

from mgc_v05l.app.gc_mgc_london_open_acceptance_live_observation import (
    build_gc_mgc_london_open_acceptance_live_observation_report,
)


def test_live_observation_report_handles_zero_actionable_live_sample(tmp_path):
    audit_snapshot = {
        "rows": [
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                "instrument": "GC",
                "runtime_state_loaded": True,
                "runtime_instance_present": True,
                "processed_bar_count": 276,
                "actionable_entry_signal_count": 0,
                "raw_setup_candidate_count": 0,
                "audit_verdict": "NO_SETUP_OBSERVED",
                "audit_reason": "Processed completed bars exist in the inspected window, but no actionable entry signal or order intent was persisted.",
                "current_session": "LONDON_LATE",
                "inspection_start_ts": "2026-03-24T00:00:00-04:00",
                "inspection_end_ts": "2026-03-24T06:40:00-04:00",
            },
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__MGC",
                "instrument": "MGC",
                "runtime_state_loaded": True,
                "runtime_instance_present": True,
                "processed_bar_count": 276,
                "actionable_entry_signal_count": 0,
                "raw_setup_candidate_count": 0,
                "audit_verdict": "NO_SETUP_OBSERVED",
                "audit_reason": "Processed completed bars exist in the inspected window, but no actionable entry signal or order intent was persisted.",
                "current_session": "LONDON_LATE",
                "inspection_start_ts": "2026-03-24T00:00:00-04:00",
                "inspection_end_ts": "2026-03-24T06:40:00-04:00",
            },
        ]
    }
    non_approved_snapshot = {
        "rows": [
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                "display_name": "GC/MGC London-Open Acceptance Continuation Long / GC",
                "temporary_paper_strategy": True,
                "experimental_status": "experimental_temp_paper",
                "paper_only": True,
                "non_approved": True,
                "processed_bars": 276,
                "signal_count": 81,
                "intent_count": 0,
                "fill_count": 0,
                "fired": True,
                "fired_at": "2026-03-24T06:40:00-04:00",
                "latest_signal_label": "2026-03-24T06:40:00-04:00 • seen",
                "latest_activity_timestamp": "2026-03-24T10:44:13.946088+00:00",
            },
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__MGC",
                "display_name": "GC/MGC London-Open Acceptance Continuation Long / MGC",
                "temporary_paper_strategy": True,
                "experimental_status": "experimental_temp_paper",
                "paper_only": True,
                "non_approved": True,
                "processed_bars": 276,
                "signal_count": 81,
                "intent_count": 0,
                "fill_count": 0,
                "fired": True,
                "fired_at": "2026-03-24T06:40:00-04:00",
                "latest_signal_label": "2026-03-24T06:40:00-04:00 • seen",
                "latest_activity_timestamp": "2026-03-24T10:44:15.259824+00:00",
            },
        ]
    }
    lane_activity_snapshot = {
        "rows": [
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                "verdict": "NO_ACTIVITY_YET",
                "latest_event_type": "NO_ACTIVITY",
            },
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__MGC",
                "verdict": "NO_ACTIVITY_YET",
                "latest_event_type": "NO_ACTIVITY",
            },
        ]
    }
    close_review = {
        "rows": [
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__GC",
                "instrument": "GC",
                "history_sessions_found": 1,
                "history_sufficiency_status": "HISTORY_SPARSE",
                "fill_count": 0,
            },
            {
                "lane_id": "gc_mgc_london_open_acceptance_continuation_long__MGC",
                "instrument": "MGC",
                "history_sessions_found": 1,
                "history_sufficiency_status": "HISTORY_SPARSE",
                "fill_count": 0,
            },
        ]
    }
    conflicts_path = tmp_path / "conflicts.jsonl"
    conflicts_path.write_text(
        json.dumps(
            {
                "occurred_at": "2026-03-24T10:40:09.598543+00:00",
                "instrument": "GC",
                "event_type": "conflict_auto_reopened",
                "severity": "WARNING",
                "hold_new_entries": False,
                "entry_hold_effective": False,
                "standalone_strategy_ids": [
                    "breakout_metals_us_unknown_continuation__GC",
                    "gc_mgc_london_open_acceptance_continuation_long__GC",
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    silent_failure_audit = {
        "generated_at": "2026-03-24T11:00:00+00:00",
        "ranked_blockers": [{"rank": 1, "blocker": "no_usable_runtime_state_or_zero_materialization", "count": 0}],
    }

    paths = {
        "audit": tmp_path / "audit.json",
        "non_approved": tmp_path / "non_approved.json",
        "lane_activity": tmp_path / "lane_activity.json",
        "close_review": tmp_path / "close_review.json",
        "silent_failure": tmp_path / "silent_failure.json",
    }
    paths["audit"].write_text(json.dumps(audit_snapshot), encoding="utf-8")
    paths["non_approved"].write_text(json.dumps(non_approved_snapshot), encoding="utf-8")
    paths["lane_activity"].write_text(json.dumps(lane_activity_snapshot), encoding="utf-8")
    paths["close_review"].write_text(json.dumps(close_review), encoding="utf-8")
    paths["silent_failure"].write_text(json.dumps(silent_failure_audit), encoding="utf-8")

    report = build_gc_mgc_london_open_acceptance_live_observation_report(
        audit_snapshot_path=paths["audit"],
        non_approved_snapshot_path=paths["non_approved"],
        lane_activity_snapshot_path=paths["lane_activity"],
        close_review_path=paths["close_review"],
        conflict_events_path=conflicts_path,
        silent_failure_audit_path=paths["silent_failure"],
    )

    assert report["branch_definition_changed"] is False
    assert report["sample_status"]["label"] == "runtime_valid_but_no_live_entries_yet"
    assert report["live_runtime_summary"]["total_processed_bars"] == 552
    assert report["live_runtime_summary"]["total_runtime_signal_count"] == 162
    assert report["live_runtime_summary"]["total_actionable_entry_signal_count"] == 0
    assert report["gc_vs_mgc"]["GC"]["temporary_paper_strategy"] is True
    assert report["gc_vs_mgc"]["GC"]["latest_audit_verdict"] == "NO_SETUP_OBSERVED"
    assert report["session_timing_distribution"]["qualifying_entry_signals"] == {}
    assert report["late_entry_review"]["assessment"] == "insufficient_live_entry_sample"
    assert report["overlap_conflict_with_other_metals_lanes"]["same_underlying_conflict_event_count"] == 1
