from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files
from mgc_v05l.config_models.settings import ProbationaryPaperMarketDataSource
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter


REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mgc_forced_session_mgc_1x_review.yaml"
ACTIVE_CONFIGS = (
    REPO_ROOT / "config" / "headless_supervised_paper_runtime.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_restored_review.yaml",
)
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"

EXPECTED_LANES = {
    "mgc_1x_all_lanes__asia_early_long": {
        "session": "ASIA_EARLY",
        "long_sources": ("asiaEarlyLongV5",),
        "short_sources": (),
        "governance": "recognized",
    },
    "mgc_1x_all_lanes__asia_early_short": {
        "session": "ASIA_EARLY",
        "long_sources": (),
        "short_sources": ("asiaEarlyShortV2",),
        "governance": "recognized",
    },
    "mgc_1x_all_lanes__london_early_long": {
        "session": "LONDON_EARLY",
        "long_sources": ("londonEarlyLongV5",),
        "short_sources": (),
        "governance": "recognized",
    },
    "mgc_1x_all_lanes__us_early_short": {
        "session": "US_EARLY",
        "long_sources": (),
        "short_sources": ("nyEarlyShortV2",),
        "governance": "alias_or_migration_required",
    },
    "mgc_1x_all_lanes__us_midday_short": {
        "session": "US_MIDDAY",
        "long_sources": (),
        "short_sources": ("nyLateShortV2",),
        "governance": "recognized",
    },
}


def _load_review_settings():
    return load_settings_from_files(
        [
            REPO_ROOT / "config" / "base.yaml",
            REPO_ROOT / "config" / "live.yaml",
            REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
            REVIEW_CONFIG,
        ]
    )


def test_mgc_forced_session_review_overlay_loads_exact_five_lanes() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert settings.mode == "paper"
    assert settings.probationary_paper_runtime_exclusive_config is True
    assert settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT
    assert set(lanes) == set(EXPECTED_LANES)
    assert {str(row["symbol"]) for row in lanes.values()} == {"MGC"}
    assert {str(row["runtime_kind"]) for row in lanes.values()} == {"gc_mgc_forced_session_candidate_runtime"}
    assert {str(row["strategy_family"]) for row in lanes.values()} == {"gold_forced_session_baseline_v2"}

    disabled_symbols = {"CL", "GC", "MNQ", "NQ", "ES", "MES", "PL"}
    assert disabled_symbols.isdisjoint({str(row["symbol"]).upper() for row in lanes.values()})
    assert disabled_symbols.isdisjoint(
        {
            str(value).upper()
            for row in lanes.values()
            for key in ("observed_instruments", "identity_components", "long_sources", "short_sources")
            for value in (row.get(key) if isinstance(row.get(key), list) else [row.get(key)])
        }
    )


def test_mgc_forced_session_review_overlay_preserves_paper_safety_flags() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    for lane_id, expected in EXPECTED_LANES.items():
        lane = lanes[lane_id]
        assert lane["paper_only"] is True
        assert lane["live_money_eligible"] is False
        assert lane["bridge_submit_capable"] is False
        assert lane["submit_capable_without_explicit_approval"] is False
        assert lane["symbol"] == "MGC"
        assert lane["trade_size"] == 1
        assert lane["max_position_quantity"] == 1
        assert lane["max_concurrent_entries"] == 1
        assert lane["max_adds_after_entry"] == 0
        assert lane["allow_stacking"] is False
        assert lane["allow_long_and_short_netting"] is False
        assert lane["allow_direct_strategy_flip"] is False
        assert lane["participation_policy"] == "SINGLE_ENTRY_ONLY"
        assert lane["add_direction_policy"] == "SAME_DIRECTION_ONLY"
        assert lane["session_restriction"] == expected["session"]
        assert lane["allowed_sessions"] == [expected["session"]]
        assert tuple(lane.get("long_sources") or ()) == expected["long_sources"]
        assert tuple(lane.get("short_sources") or ()) == expected["short_sources"]
        assert lane["lifecycle_ownership"] == "STRATEGY_MANAGED_PAPER_LIFECYCLE"
        assert lane["managed_exit_policy_id"] == "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"


def test_mgc_forced_session_review_overlay_requires_phase1_runtime_artifact_source() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT
    for lane in lanes.values():
        assert lane["probationary_paper_market_data_source"] == "phase1_runtime_artifact"
        assert lane["market_data_source"] == "phase1_runtime_artifact"
        assert lane["required_market_data_provenance"] == "DATABENTO_REALTIME_PHASE1"
        assert lane["phase1_broker_reconciliation_required"] is True


def test_mgc_forced_session_review_overlay_is_not_in_active_runtime_inputs() -> None:
    run_script = RUN_SCRIPT.read_text(encoding="utf-8")
    active_payload = "\n".join(path.read_text(encoding="utf-8") for path in ACTIVE_CONFIGS)

    assert REVIEW_CONFIG.name not in run_script
    assert REVIEW_CONFIG.name not in active_payload
    for lane_id in EXPECTED_LANES:
        assert lane_id not in run_script
        assert lane_id not in active_payload


def test_mgc_forced_session_review_governance_surfaces_us_early_alias_gap() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    governance_review = {
        lane_id: "recognized" if lane_submit_bridge_adapter(lane_id=lane_id) is not None else "alias_or_migration_required"
        for lane_id in lanes
    }

    assert governance_review == {lane_id: expected["governance"] for lane_id, expected in EXPECTED_LANES.items()}
    assert governance_review["mgc_1x_all_lanes__us_early_short"] == "alias_or_migration_required"
    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__ny_early_short") is not None
    assert lanes["mgc_1x_all_lanes__us_early_short"]["review_blocker"] == (
        "REQUIRES_US_EARLY_ROUTE_GOVERNANCE_ALIAS_OR_MIGRATION_BEFORE_ACTIVATION"
    )


def test_mgc_forced_session_review_overlay_has_no_order_api_terms() -> None:
    payload = REVIEW_CONFIG.read_text(encoding="utf-8")
    parsed = json.dumps(_load_review_settings().probationary_paper_lane_specs)

    forbidden_order_apis = (
        "place" + "Order",
        "cancel" + "Order",
        "global" + "Cancel",
        "close" + "Position",
    )
    assert not any(term in payload for term in forbidden_order_apis)
    assert not any(term in parsed for term in forbidden_order_apis)
