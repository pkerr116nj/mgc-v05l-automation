from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.gc_mgc_forced_session_candidate_admission_plan import (
    run_gc_mgc_forced_session_candidate_admission_plan,
)
from mgc_v05l.app.gc_mgc_forced_session_candidate_load_proof import (
    run_gc_mgc_forced_session_candidate_load_proof,
)
from mgc_v05l.app.gc_mgc_forced_session_runtime import (
    ASIA_EARLY_LONG_SOURCE,
    ASIA_EARLY_SHORT_SOURCE,
    GC_MGC_FORCED_SESSION_RUNTIME_KIND,
    LONDON_EARLY_LONG_SOURCE,
    NY_EARLY_SHORT_SOURCE,
    NY_LATE_SHORT_SOURCE,
)


def test_candidate_load_proof_loads_custom_runtime_package(tmp_path: Path) -> None:
    candidate_json = tmp_path / "candidate.json"
    admission_dir = tmp_path / "admission"
    load_dir = tmp_path / "load"
    candidate_json.write_text(json.dumps(_synthetic_candidate_system()), encoding="utf-8")

    run_gc_mgc_forced_session_candidate_admission_plan(
        candidate_system_json=candidate_json,
        output_dir=admission_dir,
    )
    package_yaml = admission_dir / "gc_1x_all_lanes.paper_package.yaml"

    payload = run_gc_mgc_forced_session_candidate_load_proof(
        config_paths=[Path("config/base.yaml"), Path("config/live.yaml"), Path("config/probationary_pattern_engine.yaml")],
        package_yaml=package_yaml,
        output_dir=load_dir,
    )

    assert payload["mode"] == "gc_mgc_forced_session_candidate_load_proof"
    assert payload["all_loaded"] is True
    assert payload["lane_count"] == 5
    proof_json = json.loads(load_dir.joinpath("gc_mgc_forced_session_candidate_load_proof.json").read_text(encoding="utf-8"))
    assert proof_json["probationary_paper_runtime_exclusive_config"] is True
    assert {lane["runtime_kind"] for lane in proof_json["lanes"]} == {GC_MGC_FORCED_SESSION_RUNTIME_KIND}
    assert {lane["engine_class"] for lane in proof_json["lanes"]} == {"GcMgcForcedSessionStrategyEngine"}
    assert {
        source
        for lane in proof_json["lanes"]
        for source in (lane["approved_long_entry_sources"] + lane["approved_short_entry_sources"])
        if source
        in {
            ASIA_EARLY_LONG_SOURCE,
            ASIA_EARLY_SHORT_SOURCE,
            LONDON_EARLY_LONG_SOURCE,
            NY_EARLY_SHORT_SOURCE,
            NY_LATE_SHORT_SOURCE,
        }
    } == {
        ASIA_EARLY_LONG_SOURCE,
        ASIA_EARLY_SHORT_SOURCE,
        LONDON_EARLY_LONG_SOURCE,
        NY_EARLY_SHORT_SOURCE,
        NY_LATE_SHORT_SOURCE,
    }


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
