from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_ny_early_short_confirmation_policy_research import (
    run_gc_mgc_ny_early_short_confirmation_policy_research,
)


def test_confirmation_policy_research_reports_threshold_summaries(tmp_path: Path) -> None:
    meta_json = tmp_path / "meta.json"
    dataset_csv = tmp_path / "dataset.csv"
    output_dir = tmp_path / "out"
    dataset_csv.write_text(
        "\n".join(
            [
                "source_variant,split,symbol,trade_date,label,net_pnl_points,predicted_probability,selected",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-10,1,5.0,0.80,True",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-10,1,4.5,0.77,True",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-11,0,-3.0,0.66,True",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-11,0,-2.5,0.68,True",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-12,1,2.0,0.86,True",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    meta_json.write_text(json.dumps({"dataset_path": str(dataset_csv)}), encoding="utf-8")

    payload = run_gc_mgc_ny_early_short_confirmation_policy_research(
        meta_json=meta_json,
        output_dir=output_dir,
    )

    assert payload["mode"] == "gc_mgc_ny_early_short_confirmation_policy_research"
    policy = next(item for item in payload["policy_reports"] if item["policy_id"] == "gc_confirm_070")
    assert policy["mgc_execution_summary"]["trade_count"] == 1
    assert policy["mgc_execution_summary"]["average_net_pnl_points"] == 4.5
    assert output_dir.joinpath("gc_mgc_ny_early_short_confirmation_policy_research.json").exists()
