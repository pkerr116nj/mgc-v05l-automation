from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_forced_session_portfolio_research import (
    PortfolioLaneSpec,
    run_gc_mgc_forced_session_portfolio_research,
)


def test_forced_session_portfolio_builds_symbol_reports(tmp_path: Path) -> None:
    source_a = tmp_path / "a.json"
    source_b = tmp_path / "b.json"
    output_dir = tmp_path / "out"
    source_a.write_text(json.dumps(_synthetic_source("lane_a", "var_a", 1.0, 0.5)), encoding="utf-8")
    source_b.write_text(json.dumps(_synthetic_source("lane_b", "var_b", -0.5, 0.25)), encoding="utf-8")

    payload = run_gc_mgc_forced_session_portfolio_research(
        lane_specs=(
            PortfolioLaneSpec(lane_id="LANE_A", source_json=source_a, source_variant="var_a"),
            PortfolioLaneSpec(lane_id="LANE_B", source_json=source_b, source_variant="var_b"),
        ),
        output_dir=output_dir,
        test_window_dates=3,
    )

    assert payload["mode"] == "gc_mgc_forced_session_portfolio_research"
    assert payload["symbol_reports"]["GC"]["portfolio_summary"]["trade_date_count"] == 6
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_research.json").exists()


def _synthetic_source(lane_id: str, variant_id: str, gc_value: float, mgc_value: float) -> dict[str, object]:
    def sessions(value: float) -> list[dict[str, object]]:
        return [
            {"trade_date": f"2026-01-{index + 1:02d}", "entered": True, "net_pnl_points": value}
            for index in range(6)
        ]

    return {
        "symbol_reports": {
            "GC": {"variants": {variant_id: {"trade_summary": {"entered_trade_count": 6}, "sessions": sessions(gc_value)}}},
            "MGC": {"variants": {variant_id: {"trade_summary": {"entered_trade_count": 6}, "sessions": sessions(mgc_value)}}},
        }
    }
