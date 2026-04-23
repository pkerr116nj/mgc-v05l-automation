from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_forced_session_portfolio_shaping_research import (
    PortfolioShapeProfile,
    run_gc_mgc_forced_session_portfolio_shaping_research,
)


def test_portfolio_shaping_profiles_apply_caps_and_weights(tmp_path: Path) -> None:
    source_json = tmp_path / "portfolio.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_portfolio()), encoding="utf-8")

    payload = run_gc_mgc_forced_session_portfolio_shaping_research(
        source_json=source_json,
        output_dir=output_dir,
        test_window_dates=2,
        profiles=(
            PortfolioShapeProfile(profile_id="baseline", label="Baseline"),
            PortfolioShapeProfile(
                profile_id="asia_half_cap",
                label="Asia Half + Cap",
                lane_weights={"ASIA_EARLY_SHORT": 0.5},
                daily_realized_loss_cap_points=3.0,
                conditional_weight_rules=(
                    {
                        "rule_id": "asia_loss_skips_ny_late",
                        "trigger_lane_id": "ASIA_EARLY_SHORT",
                        "threshold_points": -1.5,
                        "target_lanes": ("NY_LATE_SHORT",),
                        "target_weight": 0.0,
                    },
                ),
            ),
        ),
    )

    assert payload["mode"] == "gc_mgc_forced_session_portfolio_shaping_research"
    ranking = payload["symbol_reports"]["GC"]["profile_ranking"]
    assert len(ranking) == 2
    shaped_profile = next(row for row in payload["symbol_reports"]["GC"]["profile_ranking"] if row["profile_id"] == "asia_half_cap")
    assert shaped_profile["trade_date_count"] == 2
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_shaping_research.json").exists()


def _synthetic_portfolio() -> dict[str, object]:
    rows = [
        {
            "trade_date": "2026-01-01",
            "lane_count": 4,
            "daily_net_pnl_points": 4.0,
            "cumulative_net_pnl_points": 4.0,
            "drawdown_points": 0.0,
            "lane_pnls": {"ASIA_EARLY_SHORT": -4.0, "LONDON_EARLY_LONG": 2.0, "NY_EARLY_SHORT": 3.0, "NY_LATE_SHORT": 3.0},
        },
        {
            "trade_date": "2026-01-02",
            "lane_count": 4,
            "daily_net_pnl_points": 1.0,
            "cumulative_net_pnl_points": 5.0,
            "drawdown_points": 0.0,
            "lane_pnls": {"ASIA_EARLY_SHORT": 1.0, "LONDON_EARLY_LONG": 0.0, "NY_EARLY_SHORT": 0.0, "NY_LATE_SHORT": 0.0},
        },
    ]
    return {
        "symbol_reports": {
            "GC": {"daily_rows": rows},
            "MGC": {"daily_rows": rows},
        }
    }
