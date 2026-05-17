from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_near_candidate_narrowing_study import (
    _dedupe_candidates,
    _gap_flags,
    _structural_soft_fail,
    build_near_narrowing_study,
)


def test_gap_flags_classify_soft_retest_hold_miss() -> None:
    row = {
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": False,
        "retest_depth_normalized": -0.02,
        "hold_margin_normalized": -0.01,
        "range_expansion_ratio": 1.0,
        "close_location": 0.8,
        "churn_score": 0.0,
        "snap_turn_conflict_strength": 0.0,
        "session": "ASIA_EARLY",
        "data_quality": {"has_min_breakout_history": True},
    }

    assert _gap_flags(row=row, scored={"failure_reasons": []}) == ["soft_retest_hold_miss"]


def test_gap_flags_classify_churn_and_direction_conflict() -> None:
    row = {
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": True,
        "retest_depth_normalized": 0.1,
        "hold_margin_normalized": 0.1,
        "range_expansion_ratio": 1.0,
        "close_location": 0.2,
        "churn_score": 0.6,
        "snap_turn_conflict_strength": 0.0,
        "session": "ASIA_EARLY",
        "data_quality": {"has_min_breakout_history": True},
    }

    flags = _gap_flags(row=row, scored={"failure_reasons": []})

    assert "wrong_side_directional_contradiction" in flags
    assert "anti_churn_snap_turn_conflict" in flags


def test_structural_soft_fail_requires_no_conflict_and_acceptable_range() -> None:
    row = {
        "breakout_breaks_prior_1_high": True,
        "retest_depth_normalized": -0.05,
        "hold_margin_normalized": -0.05,
        "range_expansion_ratio": 1.2,
    }

    assert _structural_soft_fail(row, ("soft_retest_hold_miss",)) is True
    assert _structural_soft_fail(row, ("anti_churn_snap_turn_conflict",)) is False


def test_dedupe_candidates_uses_12_bar_cooldown() -> None:
    candidates = [
        _candidate("GC", 1),
        _candidate("GC", 5),
        _candidate("GC", 14),
        _candidate("MGC", 5),
    ]

    selected = _dedupe_candidates(candidates, cooldown=12)

    assert [(item.instrument, item.bar_index) for item in selected] == [("GC", 1), ("GC", 14), ("MGC", 5)]


def test_build_near_narrowing_study_rejects_runtime_like_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="refusing non-research/live/runtime-like source path"):
        build_near_narrowing_study(
            input_root=tmp_path / "outputs" / "runtime" / "entry_acceptance_research",
            output_root=tmp_path / "reports",
        )


def test_build_near_narrowing_study_writes_report_with_no_near_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "outputs" / "reports" / "entry_acceptance_research" / "quarterly_reports"
    archive = input_root / "GC" / "2024Q1" / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive.jsonl"
    archive.parent.mkdir(parents=True)
    archive.write_text(json.dumps({"instrument": "GC", "timestamp": "2024-01-01T00:00:00+00:00"}) + "\n")

    monkeypatch.setattr(
        "mgc_v05l.app.track_b_near_candidate_narrowing_study._score_rows",
        lambda rows: [
            {
                "acceptance_class": "STRUCTURALLY_INVALID",
                "acceptance_score": 0.1,
                "failure_reasons": [],
            }
        ],
    )

    report = build_near_narrowing_study(input_root=input_root, output_root=tmp_path / "reports")

    assert report["near_candidate_count"] == 0
    assert Path(report["report_json"]).exists()
    assert Path(report["report_markdown"]).exists()
    assert report["authority_flags"]["broker_state_mutated"] is False


def _candidate(instrument: str, bar_index: int):
    from datetime import datetime, timezone
    from mgc_v05l.app.track_b_near_candidate_narrowing_study import StudyCandidate

    return StudyCandidate(
        row={},
        scored={},
        instrument=instrument,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        bar_index=bar_index,
        acceptance_score=0.8,
        gap_flags=(),
        primary_gap="test",
    )
