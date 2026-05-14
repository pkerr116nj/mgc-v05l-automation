import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORED_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_track_b_restored.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_probationary_paper_soak.sh"
SESSION_OPEN_LANES = {
    "mgc_1x_asia_london_participation__asia_london_long_v5",
    "mgc_1x_asia_london_participation__asia_london_short_v2",
    "gc_1x_asia_london_participation__asia_london_long_v5",
    "gc_1x_asia_london_participation__asia_london_short_v2",
    "mnq_1x_asia_london_participation__asia_london_long_v6",
    "mnq_1x_asia_london_participation__asia_london_short_v2",
}


def _load_restored_settings():
    return load_settings_from_files(
        [
            REPO_ROOT / "config" / "base.yaml",
            REPO_ROOT / "config" / "live.yaml",
            REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
            RESTORED_CONFIG,
        ]
    )


def test_restored_track_b_paper_package_has_expected_lane_universe() -> None:
    settings = _load_restored_settings()

    lanes = list(settings.probationary_paper_lane_specs)
    lane_ids = {str(row["lane_id"]) for row in lanes}

    assert settings.probationary_paper_runtime_exclusive_config is True
    assert len(lanes) == 47
    assert {str(row["symbol"]) for row in lanes} == {"GC", "MGC", "MNQ", "PL"}
    assert {str(row["runtime_kind"]) for row in lanes} == {
        "atp_companion_benchmark_paper",
        "asia_london_participation_candidate_runtime",
        "gc_mgc_forced_session_candidate_runtime",
        "index_futures_forced_session_candidate_runtime",
        "strategy_engine",
    }
    assert "mnq_1x_ny_early_core__us_early_long" in lane_ids
    assert "mgc_1x_all_lanes__asia_early_long" in lane_ids
    assert "mgc_1x_all_lanes__asia_early_short" in lane_ids
    assert "gc_1x_asia_london_participation__asia_london_long_v5" in lane_ids
    assert "mgc_1x_asia_london_participation__asia_london_long_v5" in lane_ids
    assert "mnq_1x_asia_london_participation__asia_london_long_v5" in lane_ids
    assert "atp_companion_v1_pl_asia_us" in lane_ids
    assert "pl_us_late_pause_resume_long" in lane_ids


def test_restored_track_b_paper_package_preserves_safety_flags() -> None:
    settings = _load_restored_settings()
    lanes = list(settings.probationary_paper_lane_specs)

    assert lanes
    assert settings.mode == "paper"
    assert settings.probationary_paper_execution_canary_enabled is False
    assert all(row.get("paper_only") is True for row in lanes)
    assert all(row.get("live_money_eligible") is False for row in lanes)
    assert not any("paper_proof" in json.dumps(row).lower() for row in lanes)
    assert not any(str(row["symbol"]).upper() in {"ES", "NQ", "MES", "ZT", "ZF", "ZN", "ZB"} for row in lanes)


def test_selected_asia_london_lanes_are_session_open_eligible_only() -> None:
    settings = _load_restored_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert SESSION_OPEN_LANES.issubset(lanes)
    for lane_id, lane in lanes.items():
        allowed_sessions = {str(value) for value in lane.get("allowed_sessions", [])}
        restriction = str(lane.get("session_restriction") or "")
        if lane_id in SESSION_OPEN_LANES:
            assert "SESSION_OPEN" in allowed_sessions
            assert "SESSION_OPEN" in restriction.split("/")
            assert lane.get("paper_only") is True
            assert lane.get("live_money_eligible") is False
            assert lane.get("runtime_kind") == "asia_london_participation_candidate_runtime"
        else:
            assert "SESSION_OPEN" not in allowed_sessions
            assert "SESSION_OPEN" not in restriction.split("/")


def test_restored_london_late_and_us_lanes_keep_explicit_session_scopes() -> None:
    settings = _load_restored_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    expected_scopes = {
        "gc_1x_asia_london_participation__asia_london_long_v5": {
            "SESSION_OPEN",
            "ASIA_EARLY",
            "ASIA_LATE",
            "LONDON_EARLY",
            "LONDON_LATE",
        },
        "mgc_1x_asia_london_participation__asia_london_short_v2": {
            "SESSION_OPEN",
            "ASIA_EARLY",
            "ASIA_LATE",
            "LONDON_EARLY",
            "LONDON_LATE",
        },
        "gc_1x_all_lanes__ny_early_short": {"NY_EARLY"},
        "mgc_1x_all_lanes__ny_early_short": {"NY_EARLY"},
        "gc_1x_all_lanes__us_midday_short": {"US_MIDDAY"},
        "mgc_1x_all_lanes__us_midday_short": {"US_MIDDAY"},
        "gc_1x_all_lanes__ny_late_short": {"NY_LATE"},
        "mnq_1x_ny_early_core__us_early_long": {"US_EARLY"},
        "mnq_1x_ny_early_core__us_midday_long": {"US_MIDDAY"},
        "mnq_1x_ny_early_core__us_late_long": {"US_LATE"},
        "mgc_us_late_pause_resume_long": {"US_LATE"},
        "pl_us_late_pause_resume_long": {"US_LATE"},
    }

    assert expected_scopes.keys() <= lanes.keys()
    for lane_id, expected in expected_scopes.items():
        row = lanes[lane_id]
        assert set(row.get("allowed_sessions") or []) == expected
        assert set(str(row.get("session_restriction") or "").split("/")) == expected


def test_default_paper_soak_bootstrap_includes_restored_package_last() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")
    restored = '${REPO_ROOT}/config/probationary_pattern_engine_paper_track_b_restored.yaml'
    shared = '${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_shared_runtime.yaml'

    assert restored in script
    assert script.index(restored) > script.index(shared)
    assert "placeOrder" not in script
    assert "cancelOrder" not in script
    assert "paper_proof" not in script
