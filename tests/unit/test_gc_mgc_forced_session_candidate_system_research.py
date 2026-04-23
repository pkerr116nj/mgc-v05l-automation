from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_forced_session_candidate_system_research import (
    run_gc_mgc_forced_session_candidate_system_research,
)


def test_candidate_system_builds_scenario_reports(tmp_path: Path) -> None:
    source_json = tmp_path / "portfolio.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_portfolio()), encoding="utf-8")

    payload = run_gc_mgc_forced_session_candidate_system_research(
        source_json=source_json,
        output_dir=output_dir,
    )

    assert payload["mode"] == "gc_mgc_forced_session_candidate_system_research"
    scenario_reports = {row["scenario_id"]: row["summary"] for row in payload["scenario_reports"]}
    assert scenario_reports["gc_1x_all_lanes"]["total_net_pnl_dollars"] == 120.0
    assert scenario_reports["mgc_10x_all_lanes_gc_equivalent"]["max_simultaneous_contracts"] == 10
    assert output_dir.joinpath("gc_mgc_forced_session_candidate_system_research.json").exists()


def _synthetic_portfolio() -> dict[str, object]:
    return {
        "symbol_reports": {
            "GC": {
                "daily_rows": [
                    {
                        "trade_date": "2026-01-01",
                        "lane_count": 4,
                        "daily_net_pnl_points": 1.2,
                        "cumulative_net_pnl_points": 1.2,
                        "drawdown_points": 0.0,
                        "lane_pnls": {
                            "ASIA_EARLY_SHORT": 0.1,
                            "LONDON_EARLY_LONG": 0.2,
                            "US_EARLY_SHORT": 0.3,
                            "US_MIDDAY_SHORT": 0.6,
                        },
                    }
                ]
            },
            "MGC": {
                "daily_rows": [
                    {
                        "trade_date": "2026-01-01",
                        "lane_count": 4,
                        "daily_net_pnl_points": 0.8,
                        "cumulative_net_pnl_points": 0.8,
                        "drawdown_points": 0.0,
                        "lane_pnls": {
                            "ASIA_EARLY_SHORT": 0.1,
                            "LONDON_EARLY_LONG": 0.2,
                            "US_EARLY_SHORT": 0.2,
                            "US_MIDDAY_SHORT": 0.3,
                        },
                    }
                ]
            },
        }
    }
