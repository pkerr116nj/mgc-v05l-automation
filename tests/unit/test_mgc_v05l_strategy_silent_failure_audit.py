from __future__ import annotations

from mgc_v05l.app.strategy_silent_failure_audit import _group_wrong_session_rows, _rank_blockers


def test_group_wrong_session_rows_groups_by_family_and_session_pair() -> None:
    grouped = _group_wrong_session_rows(
        [
            {
                "lane_id": "gc_lane",
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "GC",
                "session_restriction": "ASIA_EARLY",
                "current_detected_session": "LONDON_LATE",
            },
            {
                "lane_id": "mgc_lane",
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "symbol": "MGC",
                "session_restriction": "ASIA_EARLY",
                "current_detected_session": "LONDON_LATE",
            },
        ]
    )

    assert grouped == [
        {
            "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "session_restriction": "ASIA_EARLY",
            "current_detected_session": "LONDON_LATE",
            "count": 2,
            "lane_ids": ["gc_lane", "mgc_lane"],
            "symbols": ["GC", "MGC"],
        }
    ]


def test_rank_blockers_prioritizes_runtime_and_materialization_failures() -> None:
    ranked = _rank_blockers(
        no_usable_runtime_state=[{"lane_id": "breakout_gc"}, {"lane_id": "breakout_mgc"}],
        materialization_failures=[{"lane_guess": "breakout_gc"}, {"lane_guess": "breakout_mgc"}],
        wrong_session_rows=[{"lane_id": "asia_gc"}],
        eligible_no_raw_setup=[{"lane_id": "asia_gc"}, {"lane_id": "asia_mgc"}],
    )

    assert ranked[0]["blocker"] == "no_usable_runtime_state_or_zero_materialization"
    assert ranked[0]["count"] == 2
    assert ranked[1]["blocker"] == "repeated_wrong_session_gating"
    assert ranked[2]["blocker"] == "eligible_but_no_raw_setup_candidates"
