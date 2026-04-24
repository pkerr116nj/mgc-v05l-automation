from __future__ import annotations

import json
from pathlib import Path

from validation_layer.orchestration.evidence_building import (
    _lane_gap_categories,
    _lane_judgeability_level,
    run_targeted_evidence_building_assessment,
)


def test_lane_gap_categories_identify_zero_trade_runtime_surface() -> None:
    row = {
        "sample_evidence_status": "no_realized_trade_sample",
        "robustness_evidence_status": "insufficient",
        "optimization_history_status": "unavailable",
        "blocked_reason": None,
        "evaluation_status": "completed",
    }

    assert _lane_gap_categories(row) == [
        "no_realized_trade_sample",
        "robustness_prerequisites_not_met",
        "too_thin_sample_for_phase4",
        "optimization_history_unavailable",
    ]


def test_lane_judgeability_level_recognizes_substantive_optimization_backed_candidate() -> None:
    row = {
        "evaluation_status": "completed",
        "layer1_status_resolved": "adapted_in_this_pass",
        "sample_evidence_status": "substantive_trade_sample",
        "optimization_history_status": "available",
    }

    assert _lane_judgeability_level(row) == 5


def test_targeted_evidence_building_assessment_ranks_cleanest_family_first(tmp_path: Path, monkeypatch) -> None:
    rows = [
        {
            "item_id": "lane::breakout_gc",
            "source_family": "breakout_continuation",
            "taxonomy": "insufficient_evidence",
            "sample_evidence_status": "no_realized_trade_sample",
            "robustness_evidence_status": "insufficient",
            "optimization_history_status": "unavailable",
            "blocked_reason": None,
            "evaluation_status": "completed",
            "layer1_status_resolved": "adapted_in_this_pass",
            "trade_count": 0,
        },
        {
            "item_id": "lane::failed_cl",
            "source_family": "failed_move_reversal",
            "taxonomy": "insufficient_evidence",
            "sample_evidence_status": "no_realized_trade_sample",
            "robustness_evidence_status": "insufficient",
            "optimization_history_status": "unavailable",
            "blocked_reason": None,
            "evaluation_status": "completed",
            "layer1_status_resolved": "adapted_in_this_pass",
            "trade_count": 0,
        },
        {
            "item_id": "lane::asia_gc",
            "source_family": "asia_london_participation_core_v1",
            "taxonomy": "insufficient_evidence",
            "sample_evidence_status": "no_realized_trade_sample",
            "robustness_evidence_status": "insufficient",
            "optimization_history_status": "unavailable",
            "blocked_reason": None,
            "evaluation_status": "completed",
            "layer1_status_resolved": "adapted_in_this_pass",
            "trade_count": 0,
        },
        {
            "item_id": "lane::index_nq",
            "source_family": "index_futures_ny_intraday_forced_core_v2",
            "taxonomy": "insufficient_evidence",
            "sample_evidence_status": "no_realized_trade_sample",
            "robustness_evidence_status": "insufficient",
            "optimization_history_status": "unavailable",
            "blocked_reason": None,
            "evaluation_status": "completed",
            "layer1_status_resolved": "adapted_in_this_pass",
            "trade_count": 0,
        },
        {
            "item_id": "lane::gold_gc",
            "source_family": "gold_forced_session_baseline_v2",
            "taxonomy": "insufficient_evidence",
            "sample_evidence_status": "no_realized_trade_sample",
            "robustness_evidence_status": "insufficient",
            "optimization_history_status": "unavailable",
            "blocked_reason": None,
            "evaluation_status": "completed",
            "layer1_status_resolved": "adapted_in_this_pass",
            "trade_count": 0,
        },
    ]
    monkeypatch.setattr(
        "validation_layer.orchestration.evidence_building._load_live_rows",
        lambda audit_root: rows,
    )
    monkeypatch.setattr(
        "validation_layer.orchestration.evidence_building._approved_quant_family_reports",
        lambda root_dir: {
            "breakout_continuation": {
                "family_level_status": "partially_judgeable",
                "trade_count": 4,
                "bar_count": 32,
                "overall_status": "reject",
                "report_paths": {"validation_report_json": str(root_dir / "breakout.json")},
                "module_statuses": {"market_permutation": "fail"},
            },
            "failed_move_reversal": {
                "family_level_status": "partially_judgeable",
                "trade_count": 5,
                "bar_count": 40,
                "overall_status": "reject",
                "report_paths": {"validation_report_json": str(root_dir / "failed.json")},
                "module_statuses": {"trade_normalization": "fail"},
            },
        },
    )

    result = run_targeted_evidence_building_assessment(output_dir=tmp_path / "evidence")
    payload = json.loads(Path(result["inventory_path"]).read_text(encoding="utf-8"))

    assert payload["priority_ranking"][0]["family_name"] == "breakout_continuation"
    assert payload["priority_ranking"][1]["family_name"] == "failed_move_reversal"
    assert payload["priority_ranking"][0]["family_level_uplift"]["family_level_status"] == "partially_judgeable"
