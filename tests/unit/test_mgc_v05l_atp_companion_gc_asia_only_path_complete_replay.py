from __future__ import annotations

from types import SimpleNamespace

from mgc_v05l.app import atp_companion_gc_asia_only_path_complete_replay as subject


def test_enrich_minute_path_adds_running_excursion_and_trigger_flags() -> None:
    trade = SimpleNamespace(
        side="LONG",
        entry_price=100.0,
        stop_price=98.0,
        target_price=103.0,
    )
    minute_path = [
        {
            "start_ts": "2024-01-01T19:00:00-05:00",
            "end_ts": "2024-01-01T19:01:00-05:00",
            "open": 100.0,
            "high": 101.5,
            "low": 99.5,
            "close": 101.0,
            "high_progress_r_multiple": 0.75,
            "low_progress_r_multiple": -0.25,
        },
        {
            "start_ts": "2024-01-01T19:01:00-05:00",
            "end_ts": "2024-01-01T19:02:00-05:00",
            "open": 101.0,
            "high": 103.5,
            "low": 100.0,
            "close": 102.0,
            "high_progress_r_multiple": 1.75,
            "low_progress_r_multiple": 0.0,
        },
    ]

    enriched = subject._enrich_minute_path(trade=trade, minute_path=minute_path)

    assert enriched[0]["distance_from_entry_close_points"] == 1.0
    assert enriched[0]["running_mfe_points"] == 1.5
    assert enriched[1]["reached_plus_1_0r"] is True
    assert enriched[1]["baseline_target_touched"] is True
    assert "reference_trailing_stop_1r_touched" in enriched[1]


def test_classification_marks_built_when_completeness_is_high() -> None:
    overall = {
        "pct_non_empty_minute_path": 100.0,
        "pct_entry_boundary_ok": 100.0,
        "pct_exit_boundary_ok": 100.0,
        "pct_exit_reproduced_from_path": 90.0,
    }

    assert subject._classification(overall) == "PATH_COMPLETE_REPLAY_BUILT"


def test_classification_marks_partial_or_blocked_when_coverage_is_low() -> None:
    partial = {
        "pct_non_empty_minute_path": 80.0,
        "pct_entry_boundary_ok": 75.0,
        "pct_exit_boundary_ok": 75.0,
        "pct_exit_reproduced_from_path": 60.0,
    }
    blocked = {
        "pct_non_empty_minute_path": 10.0,
        "pct_entry_boundary_ok": 0.0,
        "pct_exit_boundary_ok": 0.0,
        "pct_exit_reproduced_from_path": 0.0,
    }

    assert subject._classification(partial) == "PATH_COMPLETE_REPLAY_PARTIAL"
    assert subject._classification(blocked) == "PATH_COMPLETE_REPLAY_BLOCKED"


def test_same_bar_support_keeps_single_bar_trades_non_empty() -> None:
    bar = SimpleNamespace(end_ts="2024-01-01T20:06:00-05:00")
    trade = SimpleNamespace(entry_ts="2024-01-01T20:06:00-05:00", exit_ts="2024-01-01T20:06:00-05:00")

    rows = subject._trade_window_with_same_bar_support(
        trade=trade,
        bars_1m=[bar],
        bars_by_end_ts={bar.end_ts: 0},
    )

    assert rows == [bar]
