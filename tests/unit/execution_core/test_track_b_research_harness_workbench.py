from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_harness_workbench import (
    TrackBResearchHarnessWorkbenchConfig,
    create_track_b_research_harness_workbench,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_research_harness_workbench_backlogs_single_window_location_variant(tmp_path: Path) -> None:
    research = _write_json(
        tmp_path / "location_research.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_FALSE_POSITIVE",
            "sample_count": 22,
            "sample_frame": {
                "lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC",
                "start_timestamp": "2026-05-07T00:19:00+00:00",
                "end_timestamp": "2026-05-07T18:05:00+00:00",
            },
        },
    )
    exit_sensitivity = _write_json(
        tmp_path / "exit_sensitivity.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_ALL_TESTED_POLICIES",
            "sample_count": 22,
            "sample_frame": {
                "lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC",
                "start_timestamp": "2026-05-07T00:19:00+00:00",
                "end_timestamp": "2026-05-07T18:05:00+00:00",
            },
        },
    )

    result = create_track_b_research_harness_workbench(
        config=TrackBResearchHarnessWorkbenchConfig(
            repo_root=tmp_path,
            location_variant_research_json=research,
            location_variant_exit_sensitivity_json=exit_sensitivity,
            output_json=tmp_path / "workbench.json",
            output_md=tmp_path / "workbench.md",
        ),
        now=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert result.report["workbench_status"] == "BUILDING_REUSABLE_RESEARCH_HARNESS"
    assert result.report["full_history_retest_contract"]["through_prior_friday"] == "2026-05-01"
    candidate = result.report["candidate_backlog"][0]
    assert candidate["candidate_name"] == "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"
    assert candidate["candidate_status"] == "NOT_PROMOTED"
    assert candidate["candidate_status_reasons"] == [
        "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
        "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
    ]
    assert candidate["paper_eligible"] is False
    assert candidate["delete_from_research_inventory"] is False
    assert candidate["future_retest_contract"]["data_scope"] == "maximum Track 1 1m-bar history"
    assert "Sharpe" in candidate["future_retest_contract"]["required_metrics"]
    assert result.report["broker_commands_invoked"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False
