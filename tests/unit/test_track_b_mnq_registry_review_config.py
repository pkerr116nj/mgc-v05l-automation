from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.config_models.loader import load_settings_from_files
from mgc_v05l.execution.ibkr_paper_strategy_exposure import evaluate_paper_strategy_exposure_gate
from mgc_v05l.execution_core.track_b_strategy_registry import (
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    resolve_track_b_strategy_registry_entry,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
REVIEW_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_registry_review.yaml"
ACTIVE_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"


EXPECTED_LANES = {
    "mnq_first_bull_snap_turn": {
        "registry_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "long_sources": ("firstBullSnapTurn",),
        "short_sources": (),
        "session_restriction": "ASIA/LONDON/US",
        "event_gating": "mnq_first_bull_snap_turn_state.session_allowed",
    },
    "mnq_first_bear_snap_turn": {
        "registry_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "long_sources": (),
        "short_sources": ("firstBearSnapTurn",),
        "session_restriction": "ASIA/LONDON/US",
        "event_gating": "mnq_first_bear_snap_turn_state.session_allowed",
    },
    "mnq_us_derivative_bear_turn": {
        "registry_id": "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        "long_sources": (),
        "short_sources": ("usDerivativeBearTurn",),
        "session_restriction": "US",
        "event_gating": "mnq_us_derivative_bear_turn_state.session_us && mnq_us_derivative_bear_turn_state.allow_us",
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


def _write_governance(tmp_path: Path, lane_id: str, registry_id: str) -> None:
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
                "strategies": [
                    {
                        "strategy_id": lane_id,
                        "bridge_strategy_id": registry_id,
                        "strategy_status": "PROBATION_ACTIVE",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                        "open_order_ambiguity_count": 0,
                    }
                ],
                "summary": {"strategy_count": 1, "submit_capable_count": 1},
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
    positions_path = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
    orders_path = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
    positions_path.parent.mkdir(parents=True, exist_ok=True)
    positions_path.write_text(
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
    orders_path.write_text(
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


def test_mnq_registry_review_lanes_load_and_remain_review_only() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    assert set(lanes) == set(EXPECTED_LANES)
    assert {str(row["symbol"]) for row in lanes.values()} == {"MNQ"}
    assert all(row.get("paper_only") is True for row in lanes.values())
    assert all(row.get("live_money_eligible") is False for row in lanes.values())
    assert all(row.get("bridge_submit_capable") is False for row in lanes.values())
    assert all(row.get("submit_capable_without_explicit_approval") is False for row in lanes.values())
    assert all(row.get("allow_stacking") is False for row in lanes.values())
    assert all(row.get("allow_long_and_short_netting") is False for row in lanes.values())
    assert all(row.get("allow_direct_strategy_flip") is False for row in lanes.values())
    assert all(row.get("managed_exit_policy_id") == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 for row in lanes.values())
    assert all(row.get("lifecycle_ownership") == "STRATEGY_MANAGED_PAPER_LIFECYCLE" for row in lanes.values())
    assert all(row.get("route_governance_mode") == "PAPER_ONLY_REVIEW_NOT_SUBMIT_PORTED" for row in lanes.values())
    assert "CL" not in {str(row["symbol"]).upper() for row in lanes.values()}
    scoped_values = {
        str(value).upper()
        for row in lanes.values()
        for key in ("symbol", "observed_instruments", "identity_components", "long_sources", "short_sources")
        for value in (row.get(key) if isinstance(row.get(key), list) else [row.get(key)])
    }
    assert "CL" not in scoped_values


def test_mnq_registry_review_lanes_match_registry_metadata() -> None:
    settings = _load_review_settings()
    lanes = {str(row["lane_id"]): row for row in settings.probationary_paper_lane_specs}

    for lane_id, expected in EXPECTED_LANES.items():
        lane = lanes[lane_id]
        registry_id = expected["registry_id"]
        entry = resolve_track_b_strategy_registry_entry(
            rule_mode=registry_id,
            rule_id=registry_id,
            strategy_id=registry_id,
        )
        assert entry is not None
        assert entry.paper_eligible is True
        assert entry.live_money_eligible is False
        assert entry.instrument_family == "MNQ"
        assert entry.managed_exit_policy_id == PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1
        assert entry.exit_not_available is False
        assert lane["registry_strategy_id"] == registry_id
        assert lane["registry_rule_id"] == registry_id
        assert lane["registry_rule_mode"] == registry_id
        assert lane["session_restriction"] == expected["session_restriction"]
        assert lane["event_gating"] == expected["event_gating"]
        assert tuple(lane.get("long_sources") or ()) == expected["long_sources"]
        assert tuple(lane.get("short_sources") or ()) == expected["short_sources"]


def test_mnq_registry_review_config_is_not_in_active_headless_runtime_inputs() -> None:
    active_payload = ACTIVE_CONFIG.read_text(encoding="utf-8")
    run_script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert REVIEW_CONFIG.name not in active_payload
    assert REVIEW_CONFIG.name not in run_script
    for lane_id in EXPECTED_LANES:
        assert lane_id not in active_payload
        assert lane_id not in run_script


def test_mnq_registry_review_lanes_do_not_conflict_with_existing_mgc_plus_one(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_phase1_reconciliation_with_mgc_plus_one(tmp_path)
    _write_broker_truth_with_mgc_plus_one_and_flat_mnq(tmp_path)

    for lane_id, expected in EXPECTED_LANES.items():
        registry_id = expected["registry_id"]
        _write_governance(tmp_path, lane_id, registry_id)
        action = "BUY" if expected["long_sources"] else "SELL"
        intent_type = "BUY_TO_OPEN" if action == "BUY" else "SELL_TO_OPEN"

        gate = evaluate_paper_strategy_exposure_gate(
            repo_root=tmp_path,
            strategy_id=lane_id,
            bridge_strategy_id=registry_id,
            executable_symbol="MNQ",
            action=action,
            intent_type=intent_type,
            quantity=1.0,
        )

        assert gate["classification"] == "PAPER_EXPOSURE_ATTRIBUTION_READY"
        assert gate["submit_allowed"] is True
        assert gate["aggregate_strategy_position_sum"] == 0.0
        assert gate["aggregate_broker_position"] == 0.0
        assert "strategy_stacking_disabled" not in gate["block_reasons"]
        assert "opposite_direction_strategy_exposure" not in gate["block_reasons"]
