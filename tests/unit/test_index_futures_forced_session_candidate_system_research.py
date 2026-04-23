from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.index_futures_forced_session_candidate_system_research import (
    run_index_futures_forced_session_candidate_system_research,
)


def test_index_futures_candidate_system_builds_artifacts(tmp_path: Path) -> None:
    source_json = tmp_path / "research.json"
    source_json.write_text(
        json.dumps(
            {
                "all_symbol_ranking": [
                    {
                        "variant_id": "NY_EARLY__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
                        "min_average_net_pnl_points": 2.0,
                        "min_net_profit_factor": 3.0,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 2.0, "net_profit_factor": 3.0, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 2.1, "net_profit_factor": 3.1, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 10.0, "net_profit_factor": 4.0, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 9.0, "net_profit_factor": 3.8, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "NY_EARLY__SHORT__segment_forced_short_v4_breakdown_or_bar7",
                        "min_average_net_pnl_points": 1.5,
                        "min_net_profit_factor": 2.5,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 1.5, "net_profit_factor": 2.5, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.6, "net_profit_factor": 2.6, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 8.0, "net_profit_factor": 3.5, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 7.0, "net_profit_factor": 3.4, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "NY_EARLY__SHORT__segment_forced_short_v2_reclaim_fail_or_bar7",
                        "min_average_net_pnl_points": 1.25,
                        "min_net_profit_factor": 2.1,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 1.25, "net_profit_factor": 2.1, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.3, "net_profit_factor": 2.2, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 6.0, "net_profit_factor": 3.2, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 5.5, "net_profit_factor": 3.0, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "US_MIDDAY__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
                        "min_average_net_pnl_points": 1.1,
                        "min_net_profit_factor": 1.8,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 1.1, "net_profit_factor": 1.8, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.2, "net_profit_factor": 1.9, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 6.5, "net_profit_factor": 2.6, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 5.9, "net_profit_factor": 2.5, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "US_MIDDAY__SHORT__segment_forced_short_v4_breakdown_or_bar7",
                        "min_average_net_pnl_points": 1.2,
                        "min_net_profit_factor": 1.9,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 1.2, "net_profit_factor": 1.9, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.25, "net_profit_factor": 2.0, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 6.8, "net_profit_factor": 2.7, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 6.1, "net_profit_factor": 2.6, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "US_LATE__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
                        "min_average_net_pnl_points": 1.0,
                        "min_net_profit_factor": 2.0,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 1.0, "net_profit_factor": 2.0, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.1, "net_profit_factor": 2.1, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 5.0, "net_profit_factor": 2.4, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 4.7, "net_profit_factor": 2.3, "entered_trade_count": 100},
                        },
                    },
                    {
                        "variant_id": "US_LATE__SHORT__segment_forced_short_v2_reclaim_fail_or_bar7",
                        "min_average_net_pnl_points": 0.9,
                        "min_net_profit_factor": 1.9,
                        "min_entered_trade_count": 100,
                        "symbols": ["ES", "MES", "NQ", "MNQ"],
                        "symbol_metrics": {
                            "ES": {"average_net_pnl_points": 0.9, "net_profit_factor": 1.9, "entered_trade_count": 100},
                            "MES": {"average_net_pnl_points": 1.0, "net_profit_factor": 2.0, "entered_trade_count": 100},
                            "NQ": {"average_net_pnl_points": 4.8, "net_profit_factor": 2.3, "entered_trade_count": 100},
                            "MNQ": {"average_net_pnl_points": 4.5, "net_profit_factor": 2.2, "entered_trade_count": 100},
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = run_index_futures_forced_session_candidate_system_research(
        source_json=source_json,
        output_dir=tmp_path / "out",
    )

    assert result["candidate_id"] == "index_futures_ny_intraday_forced_core_v2"
    summaries = {row["scenario_id"]: row["summary"] for row in result["scenario_reports"]}
    assert summaries["nq_1x_ny_early_core"]["aggregate_average_net_pnl_dollars"] == 942.0
    assert summaries["es_1x_ny_early_core"]["minimum_lane_profit_factor"] == 1.8
    assert (tmp_path / "out" / "index_futures_forced_session_candidate_system_research.json").exists()
