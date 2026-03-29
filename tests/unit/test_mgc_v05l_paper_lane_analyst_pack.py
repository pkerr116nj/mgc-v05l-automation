from __future__ import annotations

from pathlib import Path

from mgc_v05l.app.paper_lane_analyst_pack import (
    _admitted_verdict,
    _build_retention_history_substrate,
    _candidate_verdict,
    _candidate_primary_blocker,
    _candidate_readiness_confidence,
    _compute_candidate_metrics,
    _evidence_sufficiency,
    _next_tier_ranking_score,
    _retention_history_readiness_verdict,
    _retention_verdict,
    _retention_warning_flags,
    CandidateArtifact,
)


def _candidate(tmp_path: Path, rows: list[dict[str, str]]) -> CandidateArtifact:
    summary_path = tmp_path / "candidate.summary.json"
    trade_ledger_path = tmp_path / "candidate.trade_ledger.csv"
    metrics_path = tmp_path / "candidate.summary_metrics.json"
    summary_path.write_text("{}", encoding="utf-8")
    metrics_path.write_text("{}", encoding="utf-8")
    return CandidateArtifact(
        instrument="CL",
        branch="usLatePauseResumeLongTurn",
        path=summary_path,
        summary={
            "approved_branch_signal_bars": {
                "usLatePauseResumeLongTurn": [
                    "CL|5m|2026-03-01T19:00:00Z",
                    "CL|5m|2026-03-02T19:00:00Z",
                    "CL|5m|2026-03-03T19:00:00Z",
                ]
            },
            "processed_bars": 999,
            "source_first_bar_ts": "2026-03-01T00:00:00Z",
            "source_last_bar_ts": "2026-03-04T00:00:00Z",
            "timeframe": "5m",
            "trade_ledger_path": str(trade_ledger_path),
            "summary_metrics_path": str(metrics_path),
        },
        summary_metrics={},
        trade_rows=rows,
        variant="direct",
        cohort="NEXT_TIER",
    )


def test_compute_candidate_metrics_tracks_concentration_and_survival(tmp_path: Path) -> None:
    candidate = _candidate(
        tmp_path,
        [
            {"entry_ts": "2026-03-01T19:00:00Z", "exit_ts": "2026-03-01T19:30:00Z", "net_pnl": "10.0"},
            {"entry_ts": "2026-03-02T19:00:00Z", "exit_ts": "2026-03-02T19:30:00Z", "net_pnl": "-3.0"},
            {"entry_ts": "2026-03-03T19:00:00Z", "exit_ts": "2026-03-03T19:30:00Z", "net_pnl": "2.0"},
        ],
    )
    metrics = _compute_candidate_metrics(candidate)
    assert metrics["realized_pnl"] == 9.0
    assert metrics["trade_count"] == 3
    assert metrics["sessions_used"] == 3
    assert metrics["survives_without_top_1"] is False
    assert metrics["survives_without_top_3"] is False
    assert metrics["top_1_trade_concentration"] is not None
    assert metrics["max_drawdown"] == 3.0


def test_candidate_verdicts_distinguish_next_vs_later_vs_drop() -> None:
    next_metrics = {
        "realized_pnl": 25.0,
        "trade_count": 35,
        "profit_factor": 1.5,
        "top_1_trade_concentration": 0.20,
        "top_3_trade_concentration": 0.45,
        "session_pocket_concentration": 0.30,
        "survives_without_top_1": True,
        "survives_without_top_3": True,
    }
    later_metrics = {
        "realized_pnl": 5.0,
        "trade_count": 18,
        "profit_factor": 2.0,
        "top_1_trade_concentration": 0.50,
        "top_3_trade_concentration": 0.80,
        "session_pocket_concentration": 0.55,
        "survives_without_top_1": True,
        "survives_without_top_3": False,
    }
    drop_metrics = {
        "realized_pnl": -1.0,
        "trade_count": 40,
        "profit_factor": 0.9,
        "top_1_trade_concentration": 0.10,
        "top_3_trade_concentration": 0.20,
        "session_pocket_concentration": 0.10,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    assert _candidate_verdict(next_metrics) == "NEXT_ADMISSION_CANDIDATE"
    assert _candidate_verdict(later_metrics) == "LATER_REVIEW"
    assert _candidate_verdict(drop_metrics) == "DROP_FOR_NOW"
    assert _candidate_readiness_confidence(next_metrics, [], "NEXT_ADMISSION_CANDIDATE") == "LOW"
    assert _candidate_primary_blocker(later_metrics, ["THIN_SAMPLE"], "LATER_REVIEW") == "THIN_SAMPLE"


def test_admitted_verdict_degrades_to_under_watch_when_sample_is_sparse() -> None:
    row = {}
    detail = {
        "risk_state": "OK",
        "reconciliation_state": "CLEAN",
        "unresolved_intent_count": 0,
        "blocked_count": 0,
        "fill_count": 0,
        "signal_count": 0,
    }
    assert _admitted_verdict(row, detail, 0.0) == "STAY_ADMITTED_UNDER_WATCH"


def test_admitted_verdict_flags_review_on_operational_problems() -> None:
    row = {}
    detail = {
        "risk_state": "HALTED",
        "reconciliation_state": "DIRTY",
        "unresolved_intent_count": 2,
        "blocked_count": 1,
        "fill_count": 1,
        "signal_count": 2,
    }
    assert _admitted_verdict(row, detail, 0.5) == "REVIEW_BEFORE_KEEPING"


def test_retention_layer_marks_sparse_clean_lane_too_early() -> None:
    warnings = _retention_warning_flags(
        paper_sessions_in_window=1,
        active_sessions_in_window=1,
        filled_sessions_in_window=0,
        blocked_sessions_in_window=0,
        open_risk_close_sessions_in_window=0,
        dirty_close_sessions_in_window=0,
        halted_sessions_in_window=0,
        attribution_coverage_rate=0.0,
        session_pocket_concentration=1.0,
    )
    sufficiency = _evidence_sufficiency(1, 1, 0, 0.0)
    verdict = _retention_verdict(sufficiency, warnings, None)
    assert sufficiency == "INSUFFICIENT_PAPER_HISTORY"
    assert verdict == "TOO_EARLY_TO_JUDGE"
    assert "TOO_FEW_ACTIVE_SESSIONS" in warnings
    assert "TOO_FEW_FILLED_SESSIONS" in warnings


def test_retention_layer_pushes_repeated_operational_problems_to_review() -> None:
    warnings = _retention_warning_flags(
        paper_sessions_in_window=6,
        active_sessions_in_window=5,
        filled_sessions_in_window=4,
        blocked_sessions_in_window=3,
        open_risk_close_sessions_in_window=2,
        dirty_close_sessions_in_window=2,
        halted_sessions_in_window=2,
        attribution_coverage_rate=0.4,
        session_pocket_concentration=0.7,
    )
    sufficiency = _evidence_sufficiency(6, 5, 4, 0.4)
    verdict = _retention_verdict(sufficiency, warnings, -5.0)
    assert sufficiency == "LIGHT_BUT_USABLE"
    assert verdict == "REVIEW_FOR_REMOVAL"
    assert "REPEATED_DIRTY_CLOSES" in warnings
    assert "REPEATED_HALTS" in warnings
    assert "OPEN_RISK_CLOSE_PATTERN" in warnings


def test_retention_history_readiness_requires_archived_sessions() -> None:
    assert _retention_history_readiness_verdict(0, []) == "RETENTION_HISTORY_NOT_READY"
    assert _retention_history_readiness_verdict(1, []) == "RETENTION_HISTORY_PARTIAL"
    assert _retention_history_readiness_verdict(3, ["primary_gap_reason"]) == "RETENTION_HISTORY_PARTIAL"
    assert _retention_history_readiness_verdict(3, []) == "RETENTION_HISTORY_READY"


def test_next_tier_ranking_prefers_broader_sample() -> None:
    broad = {
        "instrument": "6B",
        "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "verdict": "LATER_REVIEW",
        "warnings": [],
        "metrics": {
            "trade_count": 48,
            "survives_without_top_3": False,
            "top_3_trade_concentration": 0.73,
            "profit_factor": 1.4,
            "realized_pnl": 0.02,
        },
    }
    thin = {
        "instrument": "CL",
        "branch": "asiaEarlyPauseResumeShortTurn",
        "verdict": "LATER_REVIEW",
        "warnings": ["THIN_SAMPLE", "CONCENTRATED_TOP_3"],
        "metrics": {
            "trade_count": 6,
            "survives_without_top_3": False,
            "top_3_trade_concentration": 1.0,
            "profit_factor": 3.9,
            "realized_pnl": 2.9,
        },
    }
    assert _next_tier_ranking_score(broad) > _next_tier_ranking_score(thin)


def test_retention_history_substrate_detects_archived_lane_history_and_keeps_same_family_lanes_separate(tmp_path: Path) -> None:
    archive_dir = tmp_path / "outputs" / "operator_dashboard" / "paper_session_lane_history"
    archive_dir.mkdir(parents=True)
    first_payload = """{
  "session_date": "2026-03-19",
  "lanes": [
    {
      "session_date": "2026-03-19",
      "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
      "instrument": "MGC",
      "source_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
      "session_pocket": "ASIA_EARLY",
      "active": true,
      "blocked": false,
      "signal": true,
      "intent": true,
      "fill": true,
      "open_risk_at_close": false,
      "clean_vs_dirty_close": "CLEAN",
      "halted_by_risk": false,
      "attributable_realized_pnl": "5.0",
      "attribution_coverage_confidence": "HIGH",
      "primary_gap_reason": null
    },
    {
      "session_date": "2026-03-19",
      "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
      "instrument": "GC",
      "source_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
      "session_pocket": "ASIA_EARLY",
      "active": true,
      "blocked": false,
      "signal": true,
      "intent": false,
      "fill": false,
      "open_risk_at_close": false,
      "clean_vs_dirty_close": "CLEAN",
      "halted_by_risk": false,
      "attributable_realized_pnl": null,
      "attribution_coverage_confidence": "MEDIUM",
      "primary_gap_reason": "INSUFFICIENT_PERSISTED_EVIDENCE"
    }
  ]
}
"""
    second_payload = """{
  "session_date": "2026-03-19",
  "lanes": [
    {
      "session_date": "2026-03-19",
      "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
      "instrument": "MGC",
      "source_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
      "session_pocket": "ASIA_EARLY",
      "active": true,
      "blocked": true,
      "signal": true,
      "intent": true,
      "fill": false,
      "open_risk_at_close": false,
      "clean_vs_dirty_close": "DIRTY",
      "halted_by_risk": false,
      "attributable_realized_pnl": null,
      "attribution_coverage_confidence": "MEDIUM",
      "primary_gap_reason": "INSUFFICIENT_PERSISTED_EVIDENCE"
    }
  ]
}
"""
    (archive_dir / "2026-03-19_2026-03-19T20-10-00p00-00.json").write_text(
        first_payload,
        encoding="utf-8",
    )
    (archive_dir / "2026-03-19_2026-03-19T20-15-00p00-00.json").write_text(
        second_payload,
        encoding="utf-8",
    )
    lane_rows = [
        {
            "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "instrument": "MGC",
            "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
        },
        {
            "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
            "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
            "instrument": "GC",
            "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
        },
    ]
    substrate = _build_retention_history_substrate(lane_rows, repo_root=tmp_path)
    assert substrate["archived_history_present"] is True
    assert substrate["archived_file_count"] == 2
    assert substrate["archived_session_count"] == 1
    assert substrate["retention_history_readiness_verdict"] == "RETENTION_HISTORY_PARTIAL"
    lane_history = {row["lane_id"]: row for row in substrate["lane_history"]}
    assert lane_history["mgc_asia_early_normal_breakout_retest_hold_long"]["lane_history_usable"] is True
    assert lane_history["gc_asia_early_normal_breakout_retest_hold_long"]["lane_history_usable"] is True
    assert lane_history["mgc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "MGC"
    assert lane_history["gc_asia_early_normal_breakout_retest_hold_long"]["instrument"] == "GC"
