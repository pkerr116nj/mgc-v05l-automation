import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORED_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_track_b_restored.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_probationary_paper_soak.sh"


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
    assert len(lanes) == 24
    assert {str(row["symbol"]) for row in lanes} == {"GC", "MGC", "MNQ"}
    assert {str(row["runtime_kind"]) for row in lanes} == {
        "asia_london_participation_candidate_runtime",
        "gc_mgc_forced_session_candidate_runtime",
        "index_futures_forced_session_candidate_runtime",
    }
    assert "mnq_1x_ny_early_core__us_early_long" in lane_ids
    assert "mgc_1x_all_lanes__asia_early_long" in lane_ids
    assert "mgc_1x_all_lanes__asia_early_short" in lane_ids
    assert "gc_1x_asia_london_participation__asia_london_long_v5" in lane_ids
    assert "mgc_1x_asia_london_participation__asia_london_long_v5" in lane_ids
    assert "mnq_1x_asia_london_participation__asia_london_long_v5" in lane_ids


def test_restored_track_b_paper_package_preserves_safety_flags() -> None:
    settings = _load_restored_settings()
    lanes = list(settings.probationary_paper_lane_specs)

    assert lanes
    assert settings.mode == "paper"
    assert settings.probationary_paper_execution_canary_enabled is False
    assert all(row.get("paper_only") is True for row in lanes)
    assert all(row.get("live_money_eligible") is False for row in lanes)
    assert not any("paper_proof" in json.dumps(row).lower() for row in lanes)
    assert not any(str(row["symbol"]).upper() in {"PL", "ES", "NQ", "MES", "ZT", "ZF", "ZN", "ZB"} for row in lanes)


def test_default_paper_soak_bootstrap_includes_restored_package_last() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")
    restored = '${REPO_ROOT}/config/probationary_pattern_engine_paper_track_b_restored.yaml'
    shared = '${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_shared_runtime.yaml'

    assert restored in script
    assert script.index(restored) > script.index(shared)
    assert "placeOrder" not in script
    assert "cancelOrder" not in script
    assert "paper_proof" not in script
