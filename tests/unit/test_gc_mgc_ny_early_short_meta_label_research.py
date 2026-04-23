from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_ny_early_short_meta_label_research import run_gc_mgc_ny_early_short_meta_label_research


def test_ny_early_short_meta_label_research_filters_synthetic_candidate_pool(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_ny_early_short_meta_label_research(
        source_json=source_json,
        source_variants=("ny_early_short_v2_failed_pop_3m",),
        output_dir=output_dir,
        train_fraction=0.7,
    )

    assert payload["mode"] == "gc_mgc_ny_early_short_meta_label_research"
    variant = payload["variant_reports"][0]
    assert variant["source_variant"] == "ny_early_short_v2_failed_pop_3m"
    assert variant["test_summary"]["filtered"]["trade_count"] > 0
    assert (
        variant["test_summary"]["filtered"]["average_net_pnl_points"]
        > variant["test_summary"]["baseline"]["average_net_pnl_points"]
    )
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_label_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_label_dataset.csv").exists()


def test_ny_early_short_meta_label_research_supports_gc_only_signal_with_paired_execution(tmp_path: Path) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "out_gc_signal"
    source_json.write_text(json.dumps(_synthetic_source_report()), encoding="utf-8")

    payload = run_gc_mgc_ny_early_short_meta_label_research(
        source_json=source_json,
        source_variants=("ny_early_short_v2_failed_pop_3m",),
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
                    "net_pnl_points": 1.8 if strong else -1.4,
                    "pre_context_to_setup_range_ratio": 0.8 if strong else 2.0,
                    "setup_abs_efficiency": 0.75 if strong else 0.2,
                    "setup_close_location": 0.85 if strong else 0.35,
                    "setup_vwap_displacement": 0.22 if strong else -0.08,
                    "setup_green_share": 0.75 if strong else 0.25,
                    "setup_red_share": 0.25 if strong else 0.75,
                    "setup_volume_ratio": 1.9 if strong else 0.9,
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
    return {
        "symbol_reports": symbols,
    }
