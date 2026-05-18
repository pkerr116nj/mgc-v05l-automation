from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files
from mgc_v05l.config_models.settings import ProbationaryPaperMarketDataSource
from mgc_v05l.execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    load_paper_strategy_governance_status,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter


REPO_ROOT = Path(__file__).resolve().parents[2]
COMBINED_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_mgc_review_combined.yaml"
MNQ_REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_restored_review.yaml"
MGC_REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mgc_forced_session_mgc_1x_review.yaml"
ACTIVE_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"

EXPECTED_MNQ_LANES = {
    "mnq_1x_asia_london_participation__asia_london_long_v5",
    "mnq_1x_asia_london_participation__asia_london_long_v6",
    "mnq_1x_asia_london_participation__asia_london_short_v2",
}
EXPECTED_MGC_LANES = {
    "mgc_1x_all_lanes__asia_early_long",
    "mgc_1x_all_lanes__asia_early_short",
    "mgc_1x_all_lanes__london_early_long",
    "mgc_1x_all_lanes__us_early_short",
    "mgc_1x_all_lanes__us_midday_short",
}
EXPECTED_LANES = EXPECTED_MNQ_LANES | EXPECTED_MGC_LANES


def _load_combined_settings():
    return load_settings_from_files(
        [
            REPO_ROOT / "config" / "base.yaml",
            REPO_ROOT / "config" / "live.yaml",
            REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
            COMBINED_CONFIG,
        ]
    )


def _combined_lanes() -> dict[str, dict[str, object]]:
    settings = _load_combined_settings()
    return {str(row["lane_id"]): dict(row) for row in settings.probationary_paper_lane_specs}


def _write_phase1_reconciliation(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "broker_reconciled": True,
                "review_required_count": 0,
                "track_b_broker_open_order_count": 0,
                "track_b_broker_position_count": 0,
                "track_b_broker_positions": [],
                "track_b_lifecycle_positions": [],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def _write_monitor(tmp_path: Path) -> None:
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "health_classification": "HEALTHY",
                "stale": False,
                "submit_allowed": True,
                "account_id": "DUM882026",
                "broker_position_quantity": 0.0,
                "ledger_position_quantity": 0.0,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "block_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def _write_backend_source_readiness(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "operator_dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_readiness_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "runtime_running": True,
                "paper_runtime_ready": True,
                "paper_trade_allowed": True,
                "market_data_stale_count": 0,
                "bar_authority_unavailable_count": 0,
                "blocking_fault_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "startup_control_plane_snapshot.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "overall_state": "READY"}),
        encoding="utf-8",
    )
    (output_dir / "supervised_paper_operability_snapshot.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "app_usable_for_supervised_paper": True}),
        encoding="utf-8",
    )
    (output_dir / "paper_temporary_paper_runtime_integrity_snapshot.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "temp_paper_blocked": False}),
        encoding="utf-8",
    )


def _write_paper_config_in_force(tmp_path: Path) -> None:
    lanes = list(_combined_lanes().values())
    path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"lanes": lanes}), encoding="utf-8")


def _governance_config(tmp_path: Path) -> IbkrPaperStrategyGovernanceConfig:
    return IbkrPaperStrategyGovernanceConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_governance",
        var_status_path=Path("var") / "per_strategy_paper_status.json",
        var_dashboard_path=Path("var") / "strategy_probation_dashboard.json",
        var_performance_path=Path("var") / "per_strategy_paper_performance.csv",
    )


def test_combined_review_overlay_loads_exact_eight_lanes() -> None:
    settings = _load_combined_settings()
    lanes = _combined_lanes()

    assert settings.mode == "paper"
    assert settings.probationary_paper_runtime_exclusive_config is True
    assert settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT
    assert set(lanes) == EXPECTED_LANES
    assert {str(row["symbol"]) for row in lanes.values()} == {"MNQ", "MGC"}
    assert {lane_id for lane_id, row in lanes.items() if row["symbol"] == "MNQ"} == EXPECTED_MNQ_LANES
    assert {lane_id for lane_id, row in lanes.items() if row["symbol"] == "MGC"} == EXPECTED_MGC_LANES


def test_combined_review_overlay_excludes_unrequested_symbols_and_lanes() -> None:
    lanes = _combined_lanes()
    disabled_symbols = {"CL", "GC", "NQ", "ES", "MES", "PL"}

    assert disabled_symbols.isdisjoint({str(row["symbol"]).upper() for row in lanes.values()})
    for lane_id, row in lanes.items():
        assert not lane_id.startswith(("cl_", "gc_", "nq_", "es_", "mes_", "pl_"))
        assert disabled_symbols.isdisjoint(
            {
                str(value).upper()
                for key in ("observed_instruments", "identity_components", "long_sources", "short_sources")
                for value in (row.get(key) if isinstance(row.get(key), list) else [row.get(key)])
                if value
            }
        )


def test_combined_review_overlay_preserves_paper_phase1_safety_flags() -> None:
    lanes = _combined_lanes()

    for lane in lanes.values():
        assert lane["paper_only"] is True
        assert lane["live_money_eligible"] is False
        assert lane["probationary_paper_market_data_source"] == "phase1_runtime_artifact"
        assert lane["market_data_source"] == "phase1_runtime_artifact"
        assert lane["required_market_data_provenance"] == "DATABENTO_REALTIME_PHASE1"
        assert lane["phase1_broker_reconciliation_required"] is True
        assert lane["submit_capable_without_explicit_approval"] is False
        assert lane["bridge_submit_capable"] is False
        assert lane["max_position_quantity"] == 1
        assert lane["max_concurrent_entries"] == 1
        assert lane["max_adds_after_entry"] == 0
        assert lane["allow_stacking"] is False
        assert lane["allow_long_and_short_netting"] is False
        assert lane["allow_direct_strategy_flip"] is False
        assert lane["participation_policy"] == "SINGLE_ENTRY_ONLY"
        assert lane["exclude_from_strategy_performance"] is True


def test_combined_review_overlay_lanes_are_governance_recognized_without_wildcards() -> None:
    for lane_id in EXPECTED_LANES:
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)
        assert adapter is not None
        expected_symbol = "MNQ" if lane_id in EXPECTED_MNQ_LANES else "MGC"
        assert adapter["source_instrument"] == expected_symbol
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["bridge_execution_target"]["symbol"] == expected_symbol

    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__us_early_long") is None
    assert lane_submit_bridge_adapter(lane_id="mgc_1x_all_lanes__london_early_short") is None
    assert lane_submit_bridge_adapter(lane_id="mnq_1x_asia_london_participation__asia_london_short_v3") is None
    assert lane_submit_bridge_adapter(lane_id="cl_1x_asia_london_participation__asia_london_long_v5") is None


def test_combined_review_overlay_generates_per_strategy_governance_rows(tmp_path: Path) -> None:
    _write_phase1_reconciliation(tmp_path)
    _write_monitor(tmp_path)
    _write_backend_source_readiness(tmp_path)
    _write_paper_config_in_force(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    rows = {str(row["strategy_id"]): row for row in artifacts.performance_rows}
    assert EXPECTED_LANES.issubset(rows)

    for lane_id in EXPECTED_LANES:
        row = rows[lane_id]
        assert row["ibkr_bridge_submit_capable"] is True
        assert row["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert row["live_money_eligible"] is False
        assert row["strategy_status"] not in {"PAUSED", "DISABLED", "KILL_CANDIDATE"}
        assert "paper_strategy_governance_strategy_missing" not in row["submit_block_reasons"]
        assert "lane_not_yet_submit_ported" not in row["submit_block_reasons"]
        assert "strategy_lane_not_yet_submit_ported" not in row["submit_block_reasons"]

    for lane_id in EXPECTED_MGC_LANES:
        status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id=lane_id)
        assert status["selected_strategy"]["strategy_id"] == lane_id
        assert "paper_strategy_governance_strategy_missing" not in status["block_reasons"]


def test_combined_review_overlay_is_inactive_until_explicit_launch_stack_approval() -> None:
    run_script = RUN_SCRIPT.read_text(encoding="utf-8")
    active_payload = ACTIVE_CONFIG.read_text(encoding="utf-8")

    assert COMBINED_CONFIG.name not in run_script
    assert COMBINED_CONFIG.name not in active_payload
    for lane_id in EXPECTED_LANES:
        assert lane_id not in active_payload

    assert MNQ_REVIEW_CONFIG.exists()
    assert MGC_REVIEW_CONFIG.exists()


def test_combined_review_overlay_has_no_broker_or_order_api_terms() -> None:
    payload = COMBINED_CONFIG.read_text(encoding="utf-8")
    parsed = json.dumps(_combined_lanes(), sort_keys=True)

    forbidden_terms = (
        "place" + "Order",
        "cancel" + "Order",
        "global" + "Cancel",
        "close" + "Position",
        "submit" + "_order",
        "broker" + "_mutation",
    )
    assert not any(term in payload for term in forbidden_terms)
    assert not any(term in parsed for term in forbidden_terms)
