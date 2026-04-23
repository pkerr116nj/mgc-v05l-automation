from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_forced_session_candidate_admission_plan import (
    run_gc_mgc_forced_session_candidate_admission_plan,
)


def test_candidate_admission_plan_writes_package_configs(tmp_path: Path) -> None:
    candidate_json = tmp_path / "candidate.json"
    output_dir = tmp_path / "out"
    candidate_json.write_text(json.dumps(_synthetic_candidate_system()), encoding="utf-8")

    payload = run_gc_mgc_forced_session_candidate_admission_plan(
        candidate_system_json=candidate_json,
        output_dir=output_dir,
    )

    assert payload["mode"] == "gc_mgc_forced_session_candidate_admission_plan"
    assert payload["admission_status"] == "PACKAGE_READY_AND_RUNTIME_WIRED"
    scenario_rows = {row["scenario_id"]: row for row in payload["package_scenarios"]}
    assert "gc_1x_all_lanes" in scenario_rows
    assert output_dir.joinpath("gc_1x_all_lanes.paper_package.yaml").exists()
    assert output_dir.joinpath("gc_mgc_forced_session_candidate_admission_plan.json").exists()
    package_json = json.loads(output_dir.joinpath("gc_1x_all_lanes.paper_package.json").read_text(encoding="utf-8"))
    lane_row = package_json["probationary_paper_lanes"][0]
    assert lane_row["runtime_kind"] == "gc_mgc_forced_session_candidate_runtime"
    assert lane_row["structural_signal_timeframe"] == "3m"
    assert lane_row["execution_timeframe"] == "1m"
    assert lane_row["artifact_timeframe"] == "3m"
    assert lane_row["context_timeframes"] == ["3m"]


def _synthetic_candidate_system() -> dict[str, object]:
    lane_sequence = [
        {
            "lane_id": "ASIA_EARLY_LONG",
            "segment_id": "ASIA_EARLY",
            "side": "LONG",
            "session_start_et": "19:00",
            "session_end_et": "20:30",
            "source_json": "/tmp/aa.json",
            "source_variant": "variant_aa",
            "entry_family": "dip reclaim",
            "execution_note": "note",
        },
        {
            "lane_id": "ASIA_EARLY_SHORT",
            "segment_id": "ASIA_EARLY",
            "side": "SHORT",
            "session_start_et": "19:00",
            "session_end_et": "20:30",
            "source_json": "/tmp/a.json",
            "source_variant": "variant_a",
            "entry_family": "failed reclaim",
            "execution_note": "note",
        },
        {
            "lane_id": "LONDON_EARLY_LONG",
            "segment_id": "LONDON_EARLY",
            "side": "LONG",
            "session_start_et": "03:00",
            "session_end_et": "05:30",
            "source_json": "/tmp/b.json",
            "source_variant": "variant_b",
            "entry_family": "dip reclaim",
            "execution_note": "note",
        },
        {
            "lane_id": "US_EARLY_SHORT",
            "segment_id": "US_EARLY",
            "side": "SHORT",
            "session_start_et": "08:20",
            "session_end_et": "11:00",
            "source_json": "/tmp/c.json",
            "source_variant": "variant_c",
            "entry_family": "reclaim fail",
            "execution_note": "note",
        },
        {
            "lane_id": "US_MIDDAY_SHORT",
            "segment_id": "US_MIDDAY",
            "side": "SHORT",
            "session_start_et": "11:00",
            "session_end_et": "13:30",
            "source_json": "/tmp/d.json",
            "source_variant": "variant_d",
            "entry_family": "reclaim fail",
            "execution_note": "note",
        },
    ]
    scenario_reports = [
        {
            "scenario_id": "gc_1x_all_lanes",
            "label": "GC 1x",
            "description": "desc",
            "allocations": [
                {"lane_id": "ASIA_EARLY_LONG", "symbol": "GC", "contracts": 1},
                {"lane_id": "ASIA_EARLY_SHORT", "symbol": "GC", "contracts": 1},
                {"lane_id": "LONDON_EARLY_LONG", "symbol": "GC", "contracts": 1},
                {"lane_id": "US_EARLY_SHORT", "symbol": "GC", "contracts": 1},
                {"lane_id": "US_MIDDAY_SHORT", "symbol": "GC", "contracts": 1},
            ],
            "summary": {"total_net_pnl_dollars": 1000.0, "net_profit_factor": 2.0},
        }
    ]
    return {
        "candidate_system": {
            "candidate_id": "gc_mgc_forced_session_baseline_v2",
            "lane_sequence": lane_sequence,
        },
        "scenario_reports": scenario_reports,
    }
