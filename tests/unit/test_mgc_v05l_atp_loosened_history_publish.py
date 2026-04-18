from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app import atp_loosened_history_publish as publish


def test_build_loosened_probationary_universe_uses_parallel_strategy_ids() -> None:
    universe = publish._build_loosened_probationary_universe(
        target_configs=publish.DEFAULT_TARGET_CONFIGS,
        study_suffix="_loosened_v1",
        label_suffix=" [Loosened v1]",
    )

    strategy_ids = {str(row["strategy_id"]) for row in universe["probationary_lanes"]}

    assert len(universe["probationary_lanes"]) == 5
    assert "atp_companion_v1__paper_gc_asia_us_loosened_v1" in strategy_ids
    assert "atp_companion_v1__paper_pl_asia_us_loosened_v1" in strategy_ids
    assert "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only_loosened_v1" in strategy_ids
    assert "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_loosened_v1" in strategy_ids
    assert "atp_companion_v1__production_track_gc_asia_us_loosened_v1" in strategy_ids


def test_merge_study_rows_replaces_matching_strategy_ids(tmp_path: Path) -> None:
    study_payload = {
        "meta": {
            "standalone_strategy_id": "atp_companion_v1__paper_gc_asia_us_loosened_v1",
            "strategy_id": "atp_companion_v1__paper_gc_asia_us_loosened_v1",
        }
    }
    study_json_path = tmp_path / "study.json"
    study_json_path.write_text(json.dumps(study_payload), encoding="utf-8")

    existing = [
        {
            "strategy_id": "atp_companion_v1__paper_gc_asia_us_loosened_v1",
            "label": "old",
            "symbol": "GC",
            "study_mode": "research_execution_mode",
            "execution_model": "OLD",
            "summary_payload": {},
            "strategy_study_json_path": str(study_json_path),
            "strategy_study_markdown_path": str(tmp_path / "old.md"),
        }
    ]
    new = [
        {
            "strategy_id": "atp_companion_v1__paper_gc_asia_us_loosened_v1",
            "label": "new",
            "symbol": "GC",
            "study_mode": "research_execution_mode",
            "execution_model": "NEW",
            "summary_payload": {},
            "strategy_study_json_path": str(study_json_path),
            "strategy_study_markdown_path": str(tmp_path / "new.md"),
        }
    ]

    merged = publish._merge_study_rows(existing_studies=existing, new_studies=new)

    assert len(merged) == 1
    assert merged[0]["label"] == "new"
    assert merged[0]["execution_model"] == "NEW"


def test_resolve_research_max_workers_defaults_to_single_worker_on_macos(monkeypatch) -> None:
    monkeypatch.setattr(publish.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(publish.os, "cpu_count", lambda: 12)

    resolved = publish._resolve_research_max_workers(
        job_count=9,
        requested_max_workers=None,
        safe_mode=True,
        unsafe_cap=4,
    )

    assert resolved == 1


def test_resolve_research_max_workers_honors_requested_override(monkeypatch) -> None:
    monkeypatch.setattr(publish.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(publish.os, "cpu_count", lambda: 12)

    resolved = publish._resolve_research_max_workers(
        job_count=9,
        requested_max_workers=2,
        safe_mode=True,
        unsafe_cap=4,
    )

    assert resolved == 2
