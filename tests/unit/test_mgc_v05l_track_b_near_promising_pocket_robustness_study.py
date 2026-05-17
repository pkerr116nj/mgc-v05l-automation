from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_near_promising_pocket_robustness_study import (
    _concentration,
    _dedupe_candidates,
    _verdict,
    build_near_promising_pocket_robustness_study,
)
from mgc_v05l.app.track_b_near_regime_session_filter_study import RegimeCandidate


def test_dedupe_candidates_uses_instrument_specific_cooldown() -> None:
    candidates = [
        _candidate("GC", 1),
        _candidate("GC", 5),
        _candidate("GC", 14),
        _candidate("MGC", 5),
    ]

    selected = _dedupe_candidates(candidates, cooldown=12)

    assert [(item.instrument, item.bar_index) for item in selected] == [
        ("GC", 1),
        ("GC", 14),
        ("MGC", 5),
    ]


def test_concentration_flags_top_positive_contribution() -> None:
    trades = [{"net_return": value} for value in (10.0, 3.0, -2.0, -1.0)]

    concentration = _concentration(trades)

    assert concentration["net_total_return"] == pytest.approx(10.0)
    assert concentration["top_positive_contribution_share_of_gross_profit"]["top_1"] == pytest.approx(10.0 / 13.0)


def test_verdict_keeps_concentrated_positive_subset_as_research_branch() -> None:
    verdict = _verdict(
        {
            "trade_count": 248,
            "average_return": 2.4,
            "profit_factor_proxy": 1.36,
        },
        {"quarter": {"positive_period_share": 0.55}},
        {"few_outlier_trades_driven": False},
    )

    assert verdict == "RESEARCH_BRANCH_CONTINUE"


def test_verdict_rejects_negative_or_tiny_samples() -> None:
    assert _verdict(
        {"trade_count": 20, "average_return": 4.0, "profit_factor_proxy": 2.0},
        {"quarter": {"positive_period_share": 1.0}},
        {"few_outlier_trades_driven": False},
    ) == "REJECTED"
    assert _verdict(
        {"trade_count": 200, "average_return": -0.1, "profit_factor_proxy": 0.9},
        {"quarter": {"positive_period_share": 0.1}},
        {"few_outlier_trades_driven": False},
    ) == "REJECTED"


def test_build_study_writes_report_and_preserves_research_verdicts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "outputs" / "reports" / "entry_acceptance_research" / "quarterly_reports"
    output_root = tmp_path / "reports"
    archive = input_root / "GC" / "2024Q1" / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive.jsonl"
    archive.parent.mkdir(parents=True)
    rows = [_archive_row(index) for index in range(50)]
    archive.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(
        "mgc_v05l.app.track_b_near_promising_pocket_robustness_study._score_rows",
        lambda rows: [
            {
                "acceptance_class": "NEAR_STRUCTURAL_MATCH",
                "acceptance_score": 0.82,
                "failure_reasons": [],
            }
            for _ in rows
        ],
    )

    report = build_near_promising_pocket_robustness_study(
        input_root=input_root,
        output_root=output_root,
    )

    assert Path(report["report_json"]).exists()
    assert Path(report["report_markdown"]).exists()
    assert report["conclusion"]["BROAD_NEAR_PARKED"] is True
    assert report["conclusion"]["NO_NEAR_POCKET_PROMOTED"] is True
    assert report["authority_flags"]["broker_state_mutated"] is False
    assert report["authority_flags"]["runtime_trade_eligible"] is False


def _candidate(instrument: str, bar_index: int) -> RegimeCandidate:
    return RegimeCandidate(
        row={},
        scored={},
        instrument=instrument,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        bar_index=bar_index,
        acceptance_score=0.8,
        gap_flags=("soft_retest_hold_miss",),
        primary_gap="soft_retest_hold_miss",
        bucket="near_candidate",
    )


def _archive_row(index: int) -> dict[str, object]:
    timestamp = datetime(2024, 1, 2, 23, 30, tzinfo=UTC) + timedelta(minutes=5 * index)
    return {
        "instrument": "GC",
        "timestamp": timestamp.isoformat(),
        "session": "ASIA_EARLY",
        "current_exact_rule_flag": False,
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": False,
        "retest_depth_normalized": -0.02,
        "hold_margin_normalized": -0.01,
        "range_expansion_ratio": 0.8,
        "close_location": 0.8,
        "churn_score": 0.0,
        "snap_turn_conflict_strength": 0.0,
        "source_feature_values": {"atr": 1.0},
        "data_quality": {"has_min_breakout_history": True},
        "signal_candle": {
            "open": 100.0,
            "high": 101.0 + index * 0.1,
            "low": 99.5,
            "close": 100.5 + index * 0.1,
            "timestamp": timestamp.isoformat(),
        },
    }
