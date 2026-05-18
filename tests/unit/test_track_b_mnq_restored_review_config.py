from __future__ import annotations

import ast
import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files
from mgc_v05l.execution.ibkr_paper_strategy_exposure import evaluate_paper_strategy_exposure_gate
from mgc_v05l.execution.ibkr_paper_strategy_porting import submit_capable_lane_adapters
from mgc_v05l.execution_core.track_b_strategy_registry import PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1


REPO_ROOT = Path(__file__).resolve().parents[2]
RESTORED_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_track_b_restored.yaml"
REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_restored_review.yaml"
ACTIVE_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"

EXPECTED_LANES = {
    "mnq_1x_asia_london_participation__asia_london_long_v5",
    "mnq_1x_asia_london_participation__asia_london_long_v6",
    "mnq_1x_asia_london_participation__asia_london_short_v2",
}


def _load_settings():
    return load_settings_from_files(
        [
            REPO_ROOT / "config" / "base.yaml",
            REPO_ROOT / "config" / "live.yaml",
            REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
            REVIEW_CONFIG,
        ]
    )


def _raw_lanes(path: Path) -> list[dict[str, object]]:
    raw = next(
        line.split(": ", 1)[1]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith("probationary_paper_lanes_json: ")
    )
    return list(json.loads(ast.literal_eval(raw)))


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
                "broker_position_quantity": 1.0,
                "ledger_position_quantity": 1.0,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
                "block_reasons": [],
                "orphan_positions": [],
            }
        ),
        encoding="utf-8",
    )


def _write_phase1_reconciliation_with_mgc_plus_one(tmp_path: Path) -> None:
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
                "track_b_lifecycle_positions": [
                    {
                        "account_id": "DUM882026",
                        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                        "track_b_root": "MGC",
                        "instrument_family": "MGC",
                        "contract_key": "MGC-202606",
                        "local_symbol": "MGCM6",
                        "con_id": 712565978,
                        "quantity": "1",
                        "side": "LONG",
                        "lifecycle_id": "bridge_fill_existing_mgc_plus_one",
                    }
                ],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def _write_broker_truth_with_mgc_plus_one_and_flat_mnq(tmp_path: Path) -> None:
    root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    root.mkdir(parents=True, exist_ok=True)
    (root / "ibkr_positions_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "source": "IBKR_TWS_API_REQ_POSITIONS",
                "request_method": "reqPositions",
                "positions_complete": True,
                "ok": True,
                "selected_account_id": "DUM882026",
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "symbol": "MGC",
                        "local_symbol": "MGCM6",
                        "con_id": 712565978,
                        "quantity": "1.0",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (root / "ibkr_open_orders_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "source": "IBKR_TWS_API_REQ_ALL_OPEN_ORDERS",
                "request_method": "reqAllOpenOrders",
                "open_orders_complete": True,
                "ok": True,
                "selected_account_id": "DUM882026",
                "open_order_count": 0,
                "has_open_orders": False,
                "open_orders": [],
            }
        ),
        encoding="utf-8",
    )


def test_mnq_restored_review_overlay_loads_only_requested_mnq_lanes() -> None:
    settings = _load_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert set(lanes) == EXPECTED_LANES
    assert {str(row["symbol"]) for row in lanes.values()} == {"MNQ"}
    assert all(row.get("paper_only") is True for row in lanes.values())
    assert all(row.get("live_money_eligible") is False for row in lanes.values())
    assert all(row.get("phase1_broker_reconciliation_required") is True for row in lanes.values())
    assert all(row.get("managed_exit_policy_id") == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 for row in lanes.values())
    assert all(row.get("lifecycle_ownership") == "STRATEGY_MANAGED_PAPER_LIFECYCLE" for row in lanes.values())
    assert all(
        row.get("route_governance_mode") == "PAPER_ONLY_PHASE1_DIRECT_MNQ_REQUIRES_FRESH_RECONCILIATION"
        for row in lanes.values()
    )
    assert all(row.get("submit_capable_without_explicit_approval") is False for row in lanes.values())
    assert all(row.get("allow_stacking") is False for row in lanes.values())
    assert all(row.get("allow_long_and_short_netting") is False for row in lanes.values())
    assert all(row.get("allow_direct_strategy_flip") is False for row in lanes.values())
    assert all(row.get("event_gating") == "SESSION_RESTRICTION_AND_STRATEGY_SIGNAL_ONLY" for row in lanes.values())


def test_mnq_restored_review_overlay_excludes_other_symbols() -> None:
    lanes = _raw_lanes(REVIEW_CONFIG)
    disallowed_symbols = {"CL", "MGC", "GC", "PL", "NQ", "ES", "MES"}

    assert {str(row.get("symbol")).upper() for row in lanes} == {"MNQ"}
    for row in lanes:
        assert not disallowed_symbols.intersection(
            str(symbol).upper() for symbol in row.get("observed_instruments", [])
        )
        assert not str(row.get("lane_id", "")).startswith(("cl_", "mgc_", "gc_", "pl_", "nq_", "es_", "mes_"))
        assert not str(row.get("package_id", "")).startswith(("cl_", "mgc_", "gc_", "pl_", "nq_", "es_", "mes_"))


def test_mnq_restored_review_lanes_are_subset_of_dev_restored_package() -> None:
    restored = {str(row["lane_id"]): row for row in _raw_lanes(RESTORED_CONFIG)}
    review = {str(row["lane_id"]): row for row in _raw_lanes(REVIEW_CONFIG)}

    assert set(review) == EXPECTED_LANES
    for lane_id, row in review.items():
        source = restored[lane_id]
        for key in (
            "symbol",
            "standalone_strategy_id",
            "long_sources",
            "short_sources",
            "session_restriction",
            "allowed_sessions",
            "participation_policy",
            "max_concurrent_entries",
            "max_position_quantity",
            "max_adds_after_entry",
            "add_direction_policy",
            "approval_evidence_path",
        ):
            assert row.get(key) == source.get(key)


def test_mnq_restored_review_lanes_have_paper_phase1_route_adapters() -> None:
    adapters = submit_capable_lane_adapters()

    for lane_id in EXPECTED_LANES:
        adapter = adapters[lane_id]
        assert adapter["source_instrument"] == "MNQ"
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["bridge_proxy_mode"] == "MNQ_SIGNAL_DIRECT_PHASE1"
        assert adapter["bridge_execution_target"]["symbol"] == "MNQ"
        assert adapter["bridge_execution_target"]["phase1_proxy_mode"] == "DIRECT"


def test_mnq_restored_review_config_is_not_active_runtime_input() -> None:
    active_payload = ACTIVE_CONFIG.read_text(encoding="utf-8")
    run_script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert REVIEW_CONFIG.name not in active_payload
    assert REVIEW_CONFIG.name not in run_script
    for lane_id in EXPECTED_LANES:
        assert lane_id not in active_payload
        assert lane_id not in run_script


def test_mnq_restored_review_lanes_do_not_conflict_with_current_mgc_plus_one(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_phase1_reconciliation_with_mgc_plus_one(tmp_path)
    _write_broker_truth_with_mgc_plus_one_and_flat_mnq(tmp_path)

    for lane_id in EXPECTED_LANES:
        action = "SELL" if lane_id.endswith("_short_v2") else "BUY"
        intent_type = "SELL_TO_OPEN" if action == "SELL" else "BUY_TO_OPEN"
        gate = evaluate_paper_strategy_exposure_gate(
            repo_root=tmp_path,
            strategy_id=lane_id,
            bridge_strategy_id=lane_id,
            executable_symbol="MNQ",
            action=action,
            intent_type=intent_type,
            quantity=1.0,
        )

        assert gate["classification"] == "PAPER_EXPOSURE_ATTRIBUTION_READY"
        assert gate["submit_allowed"] is True
        assert gate["aggregate_strategy_position_sum"] == 0.0
        assert gate["aggregate_broker_position"] == 0.0
