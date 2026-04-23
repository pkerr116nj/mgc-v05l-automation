from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_ny_early_short_walkforward_research import run_gc_mgc_ny_early_short_walkforward_research


def test_walkforward_research_reports_folds_and_policies(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_ny_early_short_walkforward_research(
        source_json=source_json,
        source_variant="ny_early_short_v2_failed_pop_3m",
        output_dir=output_dir,
        min_train_dates=12,
        test_window_dates=4,
    )

    assert payload["mode"] == "gc_mgc_ny_early_short_walkforward_research"
    assert payload["fold_count"] >= 2
    assert "pooled_p_060" in payload["aggregate_policy_reports"]
    assert output_dir.joinpath("gc_mgc_ny_early_short_walkforward_research.json").exists()


def _synthetic_source_report() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(24):
            date = f"2026-01-{index + 1:02d}"
            strong = index % 4 != 0
            sessions.append(
                {
                    "trade_date": date,
                    "entered": True,
                    "net_pnl_points": 2.0 if strong else -1.5,
                    "pre_context_to_setup_range_ratio": 0.8 if strong else 1.8,
                    "setup_abs_efficiency": 0.7 if strong else 0.2,
                    "setup_close_location": 0.8 if strong else 0.35,
                    "setup_vwap_displacement": 0.2 if strong else -0.05,
                    "setup_green_share": 0.7 if strong else 0.25,
                    "setup_red_share": 0.3 if strong else 0.75,
                    "setup_volume_ratio": 1.8 if strong else 0.9,
                    "setup_score": 6 if strong else 3,
                }
            )
        symbols[symbol] = {
            "variants": {
                "ny_early_short_v2_failed_pop_3m": {
                    "sessions": sessions,
                }
            }
        }
    return {"symbol_reports": symbols}
