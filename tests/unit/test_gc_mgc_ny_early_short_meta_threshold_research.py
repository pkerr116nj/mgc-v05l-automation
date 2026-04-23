from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_ny_early_short_meta_threshold_research import run_gc_mgc_ny_early_short_meta_threshold_research


def test_meta_threshold_research_reports_frontier(tmp_path: Path) -> None:
    dataset_csv = tmp_path / "dataset.csv"
    meta_json = tmp_path / "meta.json"
    output_dir = tmp_path / "out"
    dataset_csv.write_text(
        "\n".join(
            [
                "source_variant,split,symbol,trade_date,predicted_probability,net_pnl_points",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-10,0.45,1.0",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-11,0.65,3.0",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-10,0.70,4.0",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-11,0.55,-2.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    meta_json.write_text(json.dumps({"dataset_path": str(dataset_csv)}), encoding="utf-8")

    payload = run_gc_mgc_ny_early_short_meta_threshold_research(
        meta_json=meta_json,
        output_dir=output_dir,
    )

    assert payload["mode"] == "gc_mgc_ny_early_short_meta_threshold_research"
    overall = payload["overall"]
    row_06 = next(item for item in overall if item["threshold"] == 0.6)
    assert row_06["trade_count"] == 2
    assert row_06["average_net_pnl_points"] == 3.5
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_threshold_research.json").exists()
