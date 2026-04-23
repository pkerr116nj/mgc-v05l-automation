from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.asia_london_participation_candidate_admission_plan import (
    run_asia_london_participation_candidate_admission_plan,
)
from mgc_v05l.app.asia_london_participation_candidate_system_research import (
    run_asia_london_participation_candidate_system_research,
)


def test_candidate_system_builds_from_optimization_payloads(tmp_path: Path) -> None:
    gc_json = tmp_path / "gc.json"
    nq_json = tmp_path / "nq.json"
    gc_json.write_text(json.dumps(_synthetic_gc_payload()), encoding="utf-8")
    nq_json.write_text(json.dumps(_synthetic_nq_payload()), encoding="utf-8")

    result = run_asia_london_participation_candidate_system_research(
        gc_source_json=gc_json,
        nq_source_json=nq_json,
        output_dir=tmp_path / "out",
    )

    assert result["candidate_id"] == "asia_london_participation_core_v1"
    summaries = {row["scenario_id"]: row["summary"] for row in result["scenario_reports"]}
    assert summaries["gc_1x_asia_london_participation"]["aggregate_average_net_pnl_dollars"] == 54.23
    assert summaries["nq_1x_asia_london_participation"]["minimum_lane_profit_factor"] == 1.49


def test_candidate_admission_plan_builds_combined_package(tmp_path: Path) -> None:
    candidate_json = tmp_path / "candidate_system.json"
    candidate_json.write_text(json.dumps(_synthetic_candidate_system()), encoding="utf-8")
    result = run_asia_london_participation_candidate_admission_plan(
        candidate_system_json=candidate_json,
        output_dir=tmp_path / "admission",
    )
    payload = json.loads((tmp_path / "admission" / "asia_london_participation_all_variants_1x.paper_package.json").read_text(encoding="utf-8"))
    assert result["mode"] == "asia_london_participation_candidate_admission_plan"
    assert len(payload["probationary_paper_lanes"]) == 2
    assert {row["runtime_kind"] for row in payload["probationary_paper_lanes"]} == {"asia_london_participation_candidate_runtime"}


def _synthetic_gc_payload() -> dict[str, object]:
    return {
        "pair_rankings": {
            "GC_MGC": [
                {
                    "variant_key": "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base",
                    "min_average_net_pnl_points": 0.2876,
                    "min_net_profit_factor": 1.3735,
                    "min_entered_trade_count": 590,
                    "max_pair_drawdown_points": 75.91,
                },
                {
                    "variant_key": "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base",
                    "min_average_net_pnl_points": 0.2547,
                    "min_net_profit_factor": 1.3053,
                    "min_entered_trade_count": 590,
                    "max_pair_drawdown_points": 81.6,
                },
            ]
        },
        "symbol_reports": {
            "GC": {
                "variants": {
                    "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {"trade_summary": {"average_net_pnl_points": 0.2876, "net_profit_factor": 1.3735, "entered_trade_count": 590, "max_drawdown_points": 75.91}},
                    "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base": {"trade_summary": {"average_net_pnl_points": 0.2547, "net_profit_factor": 1.3053, "entered_trade_count": 590, "max_drawdown_points": 81.6}},
                }
            },
            "MGC": {
                "variants": {
                    "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {"trade_summary": {"average_net_pnl_points": 0.31, "net_profit_factor": 1.4, "entered_trade_count": 590, "max_drawdown_points": 70.0}},
                    "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base": {"trade_summary": {"average_net_pnl_points": 0.28, "net_profit_factor": 1.33, "entered_trade_count": 590, "max_drawdown_points": 80.0}},
                }
            },
        },
    }


def _synthetic_nq_payload() -> dict[str, object]:
    return {
        "pair_rankings": {
            "NQ_MNQ": [
                {
                    "variant_key": "LONG__segment_forced_long_v6_contextual_fallback__base",
                    "min_average_net_pnl_points": 3.7689,
                    "min_net_profit_factor": 2.1798,
                    "min_entered_trade_count": 595,
                    "max_pair_drawdown_points": 206.75,
                },
                {
                    "variant_key": "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base",
                    "min_average_net_pnl_points": 3.6004,
                    "min_net_profit_factor": 2.1427,
                    "min_entered_trade_count": 595,
                    "max_pair_drawdown_points": 248.75,
                },
                {
                    "variant_key": "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base",
                    "min_average_net_pnl_points": 1.7878,
                    "min_net_profit_factor": 1.49,
                    "min_entered_trade_count": 595,
                    "max_pair_drawdown_points": 153.0,
                },
            ]
        },
        "symbol_reports": {
            "NQ": {
                "variants": {
                    "LONG__segment_forced_long_v6_contextual_fallback__base": {"trade_summary": {"average_net_pnl_points": 3.7689, "net_profit_factor": 2.1798, "entered_trade_count": 595, "max_drawdown_points": 206.75}},
                    "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {"trade_summary": {"average_net_pnl_points": 3.6004, "net_profit_factor": 2.1427, "entered_trade_count": 595, "max_drawdown_points": 248.75}},
                    "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base": {"trade_summary": {"average_net_pnl_points": 1.7878, "net_profit_factor": 1.49, "entered_trade_count": 595, "max_drawdown_points": 153.0}},
                }
            },
            "MNQ": {
                "variants": {
                    "LONG__segment_forced_long_v6_contextual_fallback__base": {"trade_summary": {"average_net_pnl_points": 4.0, "net_profit_factor": 2.25, "entered_trade_count": 595, "max_drawdown_points": 180.0}},
                    "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {"trade_summary": {"average_net_pnl_points": 3.7, "net_profit_factor": 2.18, "entered_trade_count": 595, "max_drawdown_points": 230.0}},
                    "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base": {"trade_summary": {"average_net_pnl_points": 1.9, "net_profit_factor": 1.55, "entered_trade_count": 595, "max_drawdown_points": 140.0}},
                }
            },
        },
    }


def _synthetic_candidate_system() -> dict[str, object]:
    return {
        "candidate_system": {
            "candidate_id": "asia_london_participation_core_v1",
            "lane_sequence": [
                {"lane_id": "ASIA_LONDON_LONG_V5", "segment_id": "ASIA_EARLY", "side": "LONG", "symbol_group": "GC_MGC", "source_variant": "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base"},
                {"lane_id": "ASIA_LONDON_LONG_V6", "segment_id": "ASIA_EARLY", "side": "LONG", "symbol_group": "NQ_MNQ", "source_variant": "LONG__segment_forced_long_v6_contextual_fallback__base"},
            ],
        },
        "scenario_reports": [
            {
                "scenario_id": "gc_1x_asia_london_participation",
                "label": "GC 1x Asia-London Participation",
                "description": "desc",
                "symbol": "GC",
                "contracts": 1,
                "summary": {},
            },
            {
                "scenario_id": "nq_1x_asia_london_participation",
                "label": "NQ 1x Asia-London Participation",
                "description": "desc",
                "symbol": "NQ",
                "contracts": 1,
                "summary": {},
            },
        ],
    }
