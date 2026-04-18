from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.approved_exit_transplant_similarity import _branch_to_probable_study_id
from mgc_v05l.app.approved_exit_transplant_similarity import run_approved_exit_transplant_similarity


def test_branch_to_probable_study_id_handles_canonical_and_family_first_labels() -> None:
    assert _branch_to_probable_study_id("GC / asiaEarlyNormalBreakoutRetestHoldTurn") == "gc_asia_early_normal_breakout_retest_hold_turn__GC"
    assert _branch_to_probable_study_id("breakout_metals_us_unknown_continuation / GC") == "breakout_metals_us_unknown_continuation__GC"


def test_run_approved_exit_transplant_similarity_writes_ranked_report(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "paper_approved_models_snapshot.json"
    historical_dir = tmp_path / "historical_playback"
    historical_dir.mkdir()
    snapshot_payload = {
        "rows": [
            {
                "branch": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "lane_id": "gc_asia_early_normal_breakout_retest_hold_long",
                "instrument": "GC",
                "source_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "side": "LONG",
                "session_restriction": "ASIA_EARLY",
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "execution_timeframe": "1m",
                "context_timeframes": ["5m"],
                "paper_strategy_class": "approved_or_admitted_paper_strategy",
            },
            {
                "branch": "MGC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
                "instrument": "MGC",
                "source_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "side": "LONG",
                "session_restriction": "ASIA_EARLY",
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "execution_timeframe": "1m",
                "context_timeframes": ["5m"],
                "paper_strategy_class": "approved_or_admitted_paper_strategy",
            },
            {
                "branch": "breakout_metals_us_unknown_continuation / GC",
                "lane_id": "breakout_metals_us_unknown_continuation__GC",
                "instrument": "GC",
                "source_family": "breakout_continuation",
                "side": "LONG",
                "session_restriction": "US/UNKNOWN",
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "execution_timeframe": "1m",
                "context_timeframes": ["5m"],
                "paper_strategy_class": "approved_or_admitted_paper_strategy",
            },
        ]
    }
    snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")
    (historical_dir / "historical_playback_mgc_asia_early_normal_breakout_retest_hold_turn__MGC.strategy_study.json").write_text(
        json.dumps(
            {
                "standalone_strategy_id": "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
                "summary": {"closed_trade_breakdown": [{"trade_id": "1"}]},
            }
        ),
        encoding="utf-8",
    )
    (historical_dir / "historical_playback_breakout_metals_us_unknown_continuation__GC.strategy_study.json").write_text(
        json.dumps(
            {
                "standalone_strategy_id": "breakout_metals_us_unknown_continuation__GC",
                "summary": {"closed_trade_breakdown": []},
            }
        ),
        encoding="utf-8",
    )

    result = run_approved_exit_transplant_similarity(
        target_branch="GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        approved_snapshot_path=snapshot_path,
        historical_playback_dir=historical_dir,
        report_dir=tmp_path / "report",
    )
    payload = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    assert payload["ranked_candidates"][0]["branch"] == "MGC / asiaEarlyNormalBreakoutRetestHoldTurn"
    assert payload["ranked_candidates"][0]["review_bucket"] == "near_clone"
    breakout_row = next(row for row in payload["ranked_candidates"] if row["branch"] == "breakout_metals_us_unknown_continuation / GC")
    assert breakout_row["review_bucket"] == "wait_for_history"
