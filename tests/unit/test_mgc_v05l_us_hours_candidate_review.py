from pathlib import Path

from mgc_v05l.app.us_hours_candidate_review import (
    _cleaner_than_failed_impulse,
    _load_admitted_sources,
    _session_windows_et,
    _verdict_bucket,
    CandidateMetrics,
)


def test_load_admitted_sources_finds_existing_us_late_lane() -> None:
    admitted = _load_admitted_sources(
        Path("/Users/patrick/Documents/MGC-v05l-automation/config/probationary_pattern_engine_paper.yaml")
    )
    assert "usLatePauseResumeLongTurn" in admitted
    assert "usDerivativeBearTurn" not in admitted


def test_verdict_bucket_prioritizes_admitted_and_next_derivative_candidate() -> None:
    metrics = CandidateMetrics(
        sample_start="a",
        sample_end="b",
        trades=10,
        realized_pnl=100.0,
        avg_trade=10.0,
        median_trade=2.0,
        profit_factor=2.0,
        max_drawdown=20.0,
        win_rate=0.5,
        top_1_contribution=20.0,
        top_3_contribution=50.0,
        survives_without_top_1=True,
        survives_without_top_3=True,
    )
    assert _verdict_bucket("usLatePauseResumeLongTurn", metrics, already_admitted=True) == "PRIORITIZE_NOW"
    assert _verdict_bucket("usDerivativeBearTurn", metrics, already_admitted=False) == "SERIOUS_NEXT_CANDIDATE"


def test_session_windows_cover_us_and_asia_labels() -> None:
    windows = _session_windows_et({"US_OPEN_LATE": 3, "US_LATE": 2, "ASIA_EARLY": 1})  # type: ignore[arg-type]
    assert "US_OPEN_LATE: 10:00-10:30 ET" in windows
    assert "US_LATE: 14:00-17:00 ET" in windows
    assert "ASIA_EARLY: 18:00-20:30 ET" in windows


def test_cleaner_than_failed_impulse_requires_stronger_profile_for_snap_reference() -> None:
    impulse_reference = {
        "failed_same_bar_metrics": {
            "profit_factor": 1.1918,
            "median_trade": -8.0,
            "top_3_contribution": 156.33,
            "max_drawdown": 891.0,
        }
    }
    snap_metrics = CandidateMetrics(
        sample_start="a",
        sample_end="b",
        trades=20,
        realized_pnl=100.0,
        avg_trade=5.0,
        median_trade=-8.0,
        profit_factor=1.26,
        max_drawdown=542.0,
        win_rate=0.33,
        top_1_contribution=80.0,
        top_3_contribution=187.0,
        survives_without_top_1=True,
        survives_without_top_3=False,
    )
    assert _cleaner_than_failed_impulse(snap_metrics, "firstBullSnapTurn", impulse_reference) is False
