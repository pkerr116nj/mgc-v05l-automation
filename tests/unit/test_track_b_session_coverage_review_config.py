from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.loader import load_settings_from_files
from mgc_v05l.execution_core.track_b_strategy_registry import (
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    resolve_track_b_strategy_registry_entry,
)
from mgc_v05l.session_phase_labels import label_session_phase, session_restriction_matches_timestamp


REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_session_coverage_review.yaml"
ACTIVE_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"


def _load_review_settings():
    return load_settings_from_files(
        [
            REPO_ROOT / "config" / "base.yaml",
            REPO_ROOT / "config" / "live.yaml",
            REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
            REVIEW_CONFIG,
        ]
    )


def test_session_coverage_review_lanes_load_and_remain_review_only() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert set(lanes) == {
        "mgc_asia_late_flat_pullback_pause_resume_long",
        "mgc_london_late_pause_resume_short",
    }
    assert {str(row["symbol"]) for row in lanes.values()} == {"MGC"}
    assert all(row.get("paper_only") is True for row in lanes.values())
    assert all(row.get("live_money_eligible") is False for row in lanes.values())
    assert all(row.get("submit_capable_without_explicit_approval") is False for row in lanes.values())
    assert all(row.get("review_blocker") == "CURRENT_MGC_PLUS_ONE_MUST_BE_RESOLVED_BEFORE_SUBMIT_CAPABLE_RESTART" for row in lanes.values())
    assert all(row.get("managed_exit_policy_id") == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 for row in lanes.values())
    assert "CL" not in {str(row["symbol"]).upper() for row in lanes.values()}
    assert "CL" not in json.dumps(list(lanes.values())).upper()


def test_session_coverage_review_lanes_match_registry_metadata() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    expected = {
        "mgc_asia_late_flat_pullback_pause_resume_long": (
            "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            "ASIA_LATE",
            ("asiaLateFlatPullbackPauseResumeLongTurn",),
            (),
        ),
        "mgc_london_late_pause_resume_short": (
            "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            "LONDON_LATE",
            (),
            ("londonLatePauseResumeShortTurn",),
        ),
    }
    for lane_id, (registry_id, session, long_sources, short_sources) in expected.items():
        lane = lanes[lane_id]
        entry = resolve_track_b_strategy_registry_entry(
            rule_mode=registry_id,
            rule_id=registry_id,
            strategy_id=registry_id,
        )
        assert entry is not None
        assert entry.paper_eligible is True
        assert entry.live_money_eligible is False
        assert entry.instrument_family == "MGC"
        assert entry.managed_exit_policy_id == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1
        assert lane["registry_strategy_id"] == registry_id
        assert lane["session_restriction"] == session
        assert lane["allowed_sessions"] == [session]
        assert tuple(lane.get("long_sources") or ()) == long_sources
        assert tuple(lane.get("short_sources") or ()) == short_sources


def test_session_coverage_review_lanes_have_expected_phase_windows() -> None:
    ny = ZoneInfo("America/New_York")
    asia_late = datetime(2026, 5, 17, 22, 0, tzinfo=ny)
    london_late = datetime(2026, 5, 18, 6, 0, tzinfo=ny)
    london_open = datetime(2026, 5, 18, 4, 0, tzinfo=ny)

    assert label_session_phase(asia_late) == "ASIA_LATE"
    assert session_restriction_matches_timestamp(asia_late, "ASIA_LATE") is True
    assert session_restriction_matches_timestamp(asia_late, "LONDON_LATE") is False
    assert label_session_phase(london_late) == "LONDON_LATE"
    assert session_restriction_matches_timestamp(london_late, "LONDON_LATE") is True
    assert session_restriction_matches_timestamp(london_late, "ASIA_LATE") is False
    assert label_session_phase(london_open) == "LONDON_OPEN"
    assert session_restriction_matches_timestamp(london_open, "LONDON_LATE") is False


def test_session_coverage_review_config_is_not_in_active_headless_runtime_inputs() -> None:
    active_payload = ACTIVE_CONFIG.read_text(encoding="utf-8")
    run_script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert REVIEW_CONFIG.name not in active_payload
    assert REVIEW_CONFIG.name not in run_script
    assert "mgc_asia_late_flat_pullback_pause_resume_long" not in active_payload
    assert "mgc_london_late_pause_resume_short" not in active_payload
