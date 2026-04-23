from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_segment_forced_session_walkforward_research import (
    run_gc_mgc_segment_forced_session_walkforward_research,
)


def test_segment_forced_session_walkforward_reports_variant_folds(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_segment_forced_session_walkforward_research(
        source_json=source_json,
        source_variants=("segment_forced_long_v1_breakout_or_bar6",),
        output_dir=output_dir,
        test_window_dates=5,
    )

    assert payload["mode"] == "gc_mgc_segment_forced_session_walkforward_research"
    variant = payload["variant_reports"][0]
    assert variant["fold_count"] >= 2
    assert output_dir.joinpath("gc_mgc_segment_forced_session_walkforward_research.json").exists()


def _synthetic_source_report() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(15):
            sessions.append(
                {
                    "trade_date": f"2026-01-{index + 1:02d}",
                    "entered": True,
                    "net_pnl_points": 1.5 if index % 3 else -0.5,
                }
            )
        symbols[symbol] = {
            "variants": {
                "segment_forced_long_v1_breakout_or_bar6": {
                    "sessions": sessions,
                }
            }
        }
    return {"definition": {"segment_id": "ASIA_EARLY"}, "symbol_reports": symbols}
