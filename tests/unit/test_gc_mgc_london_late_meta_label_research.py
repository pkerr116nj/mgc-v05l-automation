from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_london_late_meta_label_research import run_gc_mgc_london_late_meta_label_research


def test_meta_label_research_filters_synthetic_candidate_pool(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_london_late_meta_label_research(
        source_json=source_json,
        source_variants=("london_late_long_v5_3m_soft",),
        output_dir=output_dir,
        train_fraction=0.7,
    )

    assert payload["mode"] == "gc_mgc_london_late_meta_label_research"
    variant = payload["variant_reports"][0]
    assert variant["source_variant"] == "london_late_long_v5_3m_soft"
    assert variant["test_summary"]["filtered"]["trade_count"] > 0
    assert (
        variant["test_summary"]["filtered"]["average_net_pnl_points"]
        > variant["test_summary"]["baseline"]["average_net_pnl_points"]
    )
    assert output_dir.joinpath("gc_mgc_london_late_meta_label_research.json").exists()
    assert output_dir.joinpath("gc_mgc_london_late_meta_label_dataset.csv").exists()


def test_meta_label_research_supports_gc_only_signal_with_paired_execution(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out_gc_signal"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_london_late_meta_label_research(
        source_json=source_json,
        source_variants=("london_late_long_v5_3m_soft",),
        output_dir=output_dir,
        train_fraction=0.7,
        signal_symbol="GC",
    )

    variant = payload["variant_reports"][0]
    assert variant["signal_symbol"] == "GC"
    assert variant["paired_execution_test_summary"]["filtered"]["trade_count"] > 0
    assert (
        variant["paired_execution_test_summary"]["filtered"]["average_net_pnl_points"]
        > variant["paired_execution_test_summary"]["baseline"]["average_net_pnl_points"]
    )


def _synthetic_source_report() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(60):
            date = f"2026-01-{index + 1:02d}" if index < 31 else f"2026-02-{index - 30:02d}"
            strong = index % 3 != 0
            sessions.append(
                {
                    "trade_date": date,
                    "entered": True,
                    "net_pnl_points": 2.0 if strong else -1.5,
                    "pre_context_to_setup_range_ratio": 0.9 if strong else 2.2,
                    "setup_abs_efficiency": 0.7 if strong else 0.2,
                    "setup_close_location": 0.9 if strong else 0.55,
                    "setup_vwap_displacement": 0.3 if strong else 0.02,
                    "setup_green_share": 0.75 if strong else 0.25,
                    "setup_volume_ratio": 2.5 if strong else 0.8,
                    "setup_score": 6 if strong else 3,
                }
            )
        symbols[symbol] = {
            "variants": {
                "london_late_long_v5_3m_soft": {
                    "sessions": sessions,
                }
            }
        }
    return {
        "symbol_reports": symbols,
    }
