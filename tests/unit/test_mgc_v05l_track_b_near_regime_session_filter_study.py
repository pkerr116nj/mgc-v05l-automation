from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_near_regime_session_filter_study import (
    RegimeCandidate,
    _adaptive_trade,
    _time_bucket,
    build_near_regime_session_filter_study,
)


def test_time_bucket_uses_new_york_session_windows() -> None:
    assert _time_bucket(_candidate_at(datetime(2024, 1, 2, 23, 30, tzinfo=UTC))) == "early_asia_first_segment"
    assert _time_bucket(_candidate_at(datetime(2024, 1, 3, 2, 30, tzinfo=UTC))) == "later_asia"
    assert _time_bucket(_candidate_at(datetime(2024, 1, 3, 7, 30, tzinfo=UTC))) == "london_overlap_open_edge"


def test_adaptive_trade_exits_at_24_when_extension_conditions_fail() -> None:
    rows = [_bar(index, close=100.0 - 0.1 * index, high=100.2, low=98.0) for index in range(50)]
    candidate = _candidate_at(datetime(2024, 1, 2, tzinfo=UTC), bar_index=1)

    trade = _adaptive_trade(candidate, rows_by_instrument={"GC": rows})

    assert trade is not None
    assert trade["exit_branch"] == "exit_24b"
    assert trade["net_return"] == pytest.approx(-3.0)


def test_adaptive_trade_extends_to_36_when_extension_conditions_pass() -> None:
    rows = [_bar(index, close=101.0, high=101.5, low=99.8) for index in range(50)]
    rows[37] = _bar(37, close=104.0, high=104.2, low=103.5)
    candidate = _candidate_at(datetime(2024, 1, 2, tzinfo=UTC), bar_index=1)

    trade = _adaptive_trade(candidate, rows_by_instrument={"GC": rows})

    assert trade is not None
    assert trade["exit_branch"] == "extend_to_36b"
    assert trade["net_return"] == pytest.approx(3.5)


def test_build_near_regime_session_filter_study_rejects_runtime_like_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="refusing non-research/live/runtime-like source path"):
        build_near_regime_session_filter_study(
            input_root=tmp_path / "outputs" / "runtime" / "entry_acceptance_research",
            output_root=tmp_path / "reports",
        )


def test_build_near_regime_session_filter_study_writes_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "outputs" / "reports" / "entry_acceptance_research" / "quarterly_reports"
    output_root = tmp_path / "reports"
    row = _archive_row(datetime(2024, 1, 2, 23, 30, tzinfo=UTC))
    archive = input_root / "GC" / "2024Q1" / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive.jsonl"
    archive.parent.mkdir(parents=True)
    archive.write_text(__import__("json").dumps(row) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "mgc_v05l.app.track_b_near_regime_session_filter_study._score_rows",
        lambda rows: [
            {
                "acceptance_class": "NEAR_STRUCTURAL_MATCH",
                "acceptance_score": 0.82,
                "failure_reasons": [],
            }
        ],
    )

    report = build_near_regime_session_filter_study(input_root=input_root, output_root=output_root)

    assert report["candidate_counts"] == {"near_candidate": 1}
    assert Path(report["report_json"]).exists()
    assert Path(report["report_markdown"]).exists()
    assert report["authority_flags"]["broker_state_mutated"] is False


def _candidate_at(timestamp: datetime, *, bar_index: int = 0) -> RegimeCandidate:
    return RegimeCandidate(
        row={},
        scored={},
        instrument="GC",
        timestamp=timestamp,
        bar_index=bar_index,
        acceptance_score=0.8,
        gap_flags=(),
        primary_gap="test",
        bucket="near_candidate",
    )


def _bar(index: int, *, close: float, high: float, low: float) -> dict[str, object]:
    return {
        "instrument": "GC",
        "timestamp": datetime(2024, 1, 2, tzinfo=UTC) + timedelta(minutes=5 * index),
        "signal_candle": {
            "open": 100.0,
            "high": high,
            "low": low,
            "close": close,
            "timestamp": datetime(2024, 1, 2, tzinfo=UTC) + timedelta(minutes=5 * index),
        },
    }


def _archive_row(timestamp: datetime) -> dict[str, object]:
    return {
        "instrument": "GC",
        "timestamp": timestamp.isoformat(),
        "session": "ASIA_EARLY",
        "current_exact_rule_flag": False,
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": False,
        "retest_depth_normalized": -0.02,
        "hold_margin_normalized": -0.01,
        "range_expansion_ratio": 1.0,
        "close_location": 0.8,
        "churn_score": 0.0,
        "snap_turn_conflict_strength": 0.0,
        "source_feature_values": {"atr": 1.0},
        "data_quality": {"has_min_breakout_history": True},
        "signal_candle": {
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "timestamp": timestamp.isoformat(),
        },
    }
