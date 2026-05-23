from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import mgc_v05l.execution.ibkr_lane_submit_port as lane_submit_module
from mgc_v05l.execution.ibkr_lane_submit_port import (
    IbkrLaneSubmitPortConfig,
    run_ibkr_lane_submit_port,
    write_ibkr_lane_submit_port_artifacts,
)

_PLAN_LANE_SUBMIT_PORT = "PLAN_LANE_SUBMIT_PORT"
_ACTION_LANE_SUBMIT_PORT = "LANE_SUBMIT_PORT"


def _config(tmp_path: Path) -> IbkrLaneSubmitPortConfig:
    return IbkrLaneSubmitPortConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_lane_submit_port",
        porting_output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        submit=False,
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
                "open_order_count": 0,
                "broker_position_quantity": 1.0,
                "ledger_position_quantity": 1.0,
                "last_successful_broker_refresh": "2026-04-28T20:39:42.036284+00:00",
            }
        ),
        encoding="utf-8",
    )


def _write_ledger(tmp_path: Path) -> None:
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "symbol": "MGC",
                        "quantity": 1.0,
                        "side": "LONG",
                        "average_entry_price": 4586.7,
                    }
                ],
                "orphan_positions": [],
            }
        ),
        encoding="utf-8",
    )


def _write_dashboard(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "dashboard_api_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"paper": {"status": {"stale": False}}}), encoding="utf-8")


def _write_signal_audit(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "id": "asia_london_participation_core_v1__GC",
                        "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "GC",
                        "family": "asia_london_participation_core_v1",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "INSUFFICIENT_HISTORY",
                        "last_actionable_signal_family": None,
                        "last_actionable_signal_timestamp": None,
                        "last_recent_long_setup": False,
                        "last_recent_short_setup": False,
                        "last_intent_type": None,
                        "last_fill_timestamp": None,
                    },
                    {
                        "id": "asia_london_participation_core_v1__MGC",
                        "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "MGC",
                        "family": "asia_london_participation_core_v1",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "INSUFFICIENT_HISTORY",
                        "last_actionable_signal_family": None,
                        "last_actionable_signal_timestamp": None,
                        "last_recent_long_setup": False,
                        "last_recent_short_setup": False,
                        "last_intent_type": None,
                        "last_fill_timestamp": None,
                    },
                    {
                        "id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                        "lane_id": "gc_1x_all_lanes__london_early_long",
                        "instrument": "GC",
                        "family": "gold_forced_session_baseline_v2",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "EXIT_RECENTLY_FILLED",
                        "last_actionable_signal_family": "londonEarlyLongV5",
                        "last_actionable_signal_timestamp": "2026-04-29T03:04:00-04:00",
                        "last_recent_long_setup": False,
                        "last_recent_short_setup": False,
                        "last_intent_type": "SELL_TO_CLOSE",
                        "last_fill_timestamp": "2026-04-29T03:08:00-04:00",
                    },
                    {
                        "id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "lane_id": "nq_1x_ny_early_core__us_late_long",
                        "instrument": "NQ",
                        "family": "index_futures_ny_intraday_forced_core_v2",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": True,
                        "audit_verdict": "READY",
                        "last_actionable_signal_family": "nyLateLong",
                        "last_actionable_signal_timestamp": "2026-04-28T14:00:00-04:00",
                        "last_recent_long_setup": True,
                        "last_recent_short_setup": False,
                        "last_intent_type": None,
                        "last_fill_timestamp": None,
                    },
                    {
                        "id": "asia_london_participation_core_v1__NQ",
                        "lane_id": "nq_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "NQ",
                        "family": "asia_london_participation_core_v1",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "INSUFFICIENT_HISTORY",
                        "last_actionable_signal_family": None,
                        "last_actionable_signal_timestamp": None,
                        "last_recent_long_setup": False,
                        "last_recent_short_setup": False,
                        "last_intent_type": None,
                        "last_fill_timestamp": None,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_strategy_performance(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "GC",
                        "strategy_family": "asia_london_participation_core_v1",
                        "standalone_strategy_id": "asia_london_participation_core_v1__GC",
                        "position_side": "FLAT",
                        "status": "READY",
                    },
                    {
                        "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "MGC",
                        "strategy_family": "asia_london_participation_core_v1",
                        "standalone_strategy_id": "asia_london_participation_core_v1__MGC",
                        "position_side": "FLAT",
                        "status": "READY",
                    },
                    {
                        "lane_id": "gc_1x_all_lanes__london_early_long",
                        "instrument": "GC",
                        "strategy_family": "gold_forced_session_baseline_v2",
                        "standalone_strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                        "position_side": "FLAT",
                        "status": "READY",
                    },
                    {
                        "lane_id": "nq_1x_ny_early_core__us_late_long",
                        "instrument": "NQ",
                        "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                        "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "position_side": "FLAT",
                        "status": "READY",
                    },
                    {
                        "lane_id": "nq_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "NQ",
                        "strategy_family": "asia_london_participation_core_v1",
                        "standalone_strategy_id": "asia_london_participation_core_v1__NQ",
                        "position_side": "FLAT",
                        "status": "READY",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_governance_status(tmp_path: Path) -> None:
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
                "strategies": [
                    {
                        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "bridge_strategy_id": "asia_london_participation_core_v1__GC",
                        "standalone_strategy_id": "asia_london_participation_core_v1__GC",
                        "strategy_status": "PROBATION_ACTIVE",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                    },
                    {
                        "strategy_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "bridge_strategy_id": "asia_london_participation_core_v1__MGC",
                        "standalone_strategy_id": "asia_london_participation_core_v1__MGC",
                        "strategy_status": "WATCHLIST",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                    },
                    {
                        "strategy_id": "gc_1x_all_lanes__london_early_long",
                        "bridge_strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                        "standalone_strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                        "strategy_status": "KILL_CANDIDATE",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                    },
                    {
                        "strategy_id": "nq_1x_ny_early_core__us_late_long",
                        "bridge_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "strategy_status": "WATCHLIST",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                    },
                    {
                        "strategy_id": "nq_1x_asia_london_participation__asia_london_long_v5",
                        "bridge_strategy_id": "asia_london_participation_core_v1__NQ",
                        "standalone_strategy_id": "asia_london_participation_core_v1__NQ",
                        "strategy_status": "WATCHLIST",
                        "submit_allowed": True,
                        "submit_block_reasons": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_lane_submit_port_reports_ready_no_action(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_governance_status(tmp_path)

    artifacts = run_ibkr_lane_submit_port(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_LANE_SUBMIT_READY_NO_ACTION"
    assert artifacts.report["selected_lane"]["strategy_id"] == "gc_1x_all_lanes__london_early_long"
    assert artifacts.report["selected_inventory_row"]["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert artifacts.report["selected_intent_row"]["action"] == "NO_ACTION"


def test_lane_submit_port_can_target_submit_capable_mgc_lane(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_governance_status(tmp_path)

    config = IbkrLaneSubmitPortConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_lane_submit_port",
        porting_output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        submit=False,
        strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
    )
    artifacts = run_ibkr_lane_submit_port(config=config)

    assert artifacts.classification == "PAPER_LANE_SUBMIT_READY_NO_ACTION"
    assert artifacts.report["selected_lane"]["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5"


def test_lane_submit_port_can_target_submit_capable_nq_lane(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_governance_status(tmp_path)

    config = IbkrLaneSubmitPortConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_lane_submit_port",
        porting_output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        submit=False,
        strategy_id="nq_1x_asia_london_participation__asia_london_long_v5",
    )

    artifacts = run_ibkr_lane_submit_port(config=config)

    assert artifacts.classification == "PAPER_LANE_SUBMIT_READY_NO_ACTION"
    assert artifacts.report["selected_lane"]["strategy_id"] == "nq_1x_asia_london_participation__asia_london_long_v5"
    assert artifacts.report["bridge_adapter"]["bridge_execution_target"]["symbol"] == "NQ"
    assert artifacts.report["selected_inventory_row"]["current_order_destination"] == "ibkr_paper_bridge_submit_capable"


def test_lane_submit_port_allows_kill_candidate_when_ported_and_requested(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_governance_status(tmp_path)

    config = IbkrLaneSubmitPortConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_lane_submit_port",
        porting_output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        submit=False,
        strategy_id="gc_1x_all_lanes__london_early_long",
    )
    artifacts = run_ibkr_lane_submit_port(config=config)

    assert artifacts.classification == "PAPER_LANE_SUBMIT_READY_NO_ACTION"
    assert artifacts.report["selected_lane"]["strategy_id"] == "gc_1x_all_lanes__london_early_long"
    assert artifacts.report["selected_lane_governance_status"]["selected_strategy"]["strategy_status"] == "KILL_CANDIDATE"
    check = next(row for row in artifacts.report["preflight_checks"] if row["name"] == "governance_status_allowed")
    assert check["passed"] is True


def test_lane_submit_port_writes_artifacts(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_governance_status(tmp_path)

    config = _config(tmp_path)
    artifacts = run_ibkr_lane_submit_port(config=config)
    write_ibkr_lane_submit_port_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_lane_submit_port"
    assert (output_dir / "ibkr_lane_submit_port_report.json").exists()
    assert (output_dir / "ibkr_lane_submit_port_report.md").exists()
    assert (output_dir / "ibkr_lane_submit_port_audit.jsonl").exists()


def test_lane_submit_default_is_non_mutating_fail_closed(monkeypatch, tmp_path: Path) -> None:
    _patch_actionable_lane(monkeypatch)

    artifacts = run_ibkr_lane_submit_port(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_LANE_PREFLIGHT_BLOCKED"
    assert "submit was disabled" in artifacts.report["detail"]
    assert artifacts.report["default_submit_enabled"] is False
    assert artifacts.report["pre_action_snapshot_validation"] == {}


def test_lane_submit_cannot_bypass_snapshot_gate(monkeypatch, tmp_path: Path) -> None:
    _patch_actionable_lane(monkeypatch)
    monkeypatch.setattr(
        lane_submit_module,
        "run_ibkr_paper_strategy_bridge",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("bridge should not run")),
    )

    artifacts = run_ibkr_lane_submit_port(
        config=IbkrLaneSubmitPortConfig(
            repo_root=tmp_path,
            submit=True,
            control_plane_authorized_submit=True,
            strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        )
    )

    assert artifacts.classification == "PAPER_LANE_PREFLIGHT_BLOCKED"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"


def test_lane_submit_requires_control_plane_authorized_submit(monkeypatch, tmp_path: Path) -> None:
    _patch_actionable_lane(monkeypatch)
    _write_pre_action_snapshot_for_lane_submit(tmp_path)

    artifacts = run_ibkr_lane_submit_port(
        config=IbkrLaneSubmitPortConfig(
            repo_root=tmp_path,
            submit=True,
            control_plane_authorized_submit=False,
            strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        )
    )

    assert artifacts.classification == "PAPER_LANE_PREFLIGHT_BLOCKED"
    assert "control-plane-authorized" in artifacts.report["detail"]
    assert artifacts.report["pre_action_snapshot_validation"] == {}


def test_lane_submit_valid_snapshot_reaches_existing_bridge_gate(monkeypatch, tmp_path: Path) -> None:
    _patch_actionable_lane(monkeypatch)
    _write_pre_action_snapshot_for_lane_submit(tmp_path)
    bridge_calls: list[object] = []

    def _fake_bridge(*, config):
        bridge_calls.append(config)
        return SimpleNamespace(
            classification="PAPER_STRATEGY_ORDER_WORKING",
            report={"detail": "fake bridge reached"},
        )

    monkeypatch.setattr(lane_submit_module, "run_ibkr_paper_strategy_bridge", _fake_bridge)

    artifacts = run_ibkr_lane_submit_port(
        config=IbkrLaneSubmitPortConfig(
            repo_root=tmp_path,
            submit=True,
            control_plane_authorized_submit=True,
            strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        )
    )

    assert artifacts.classification == "PAPER_LANE_ORDER_WORKING"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_SNAPSHOT_VALID"
    assert artifacts.report["delegated_result"]["detail"] == "fake bridge reached"
    assert len(bridge_calls) == 1


def test_lane_submit_dashboard_projection_not_consumed_as_authority() -> None:
    source = Path(lane_submit_module.__file__).read_text(encoding="utf-8")
    assert "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json" not in source


def _patch_actionable_lane(monkeypatch) -> None:
    strategy_id = "mgc_1x_asia_london_participation__asia_london_long_v5"
    monkeypatch.setattr(
        lane_submit_module,
        "run_ibkr_paper_strategy_porting",
        lambda *, config: SimpleNamespace(
            report={
                "inventory_rows": [
                    {
                        "strategy_id": strategy_id,
                        "instrument": "MGC",
                        "current_position_state": "FLAT",
                        "current_order_destination": "ibkr_paper_bridge_submit_capable",
                    }
                ],
                "intent_rows": [
                    {
                        "strategy_id": strategy_id,
                        "action": "BUY",
                        "quantity": 1.0,
                        "route_blockers": [],
                        "reason": "unit test actionable lane",
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(lane_submit_module, "write_ibkr_paper_strategy_porting_artifacts", lambda **_kwargs: None)
    monkeypatch.setattr(
        lane_submit_module,
        "load_paper_strategy_monitor_status",
        lambda *, repo_root: {
            "monitor_running": True,
            "health_classification": "HEALTHY",
            "stale": False,
            "open_order_count": 0,
        },
    )
    monkeypatch.setattr(
        lane_submit_module,
        "load_paper_strategy_governance_status",
        lambda *, repo_root, strategy_id: {
            "selected_strategy": {"strategy_status": "WATCHLIST"},
            "submit_allowed": True,
        },
    )
    monkeypatch.setattr(
        lane_submit_module,
        "_load_governance_rows",
        lambda repo_root: [{"strategy_id": strategy_id, "strategy_status": "WATCHLIST"}],
    )


def _write_pre_action_snapshot_for_lane_submit(root: Path) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    snapshot_id = "snapshot-lane-submit-port"
    generation_id = "generation-lane-submit-port"
    supervisor_decision_id = "supervisor-lane-submit-port"
    target = {
        "account_id": "DUM882026",
        "strategy_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
        "symbol": "MGC",
        "contract_month": "202606",
        "action": "BUY",
        "quantity": "1.0",
    }
    _write_json(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": generated_at,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_decision_id": supervisor_decision_id,
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "safe_to_start_runtime": True,
            "live_money_eligible": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": generated_at,
            "supervisor_decision_id": supervisor_decision_id,
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": generated_at,
            "classification": _PLAN_LANE_SUBMIT_PORT,
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "execution_enabled": False,
            "proposed_actions": [
                {
                    "action_id": "lane_submit_port",
                    "action_type": _ACTION_LANE_SUBMIT_PORT,
                    "target_identity": target,
                    "execution_enabled": False,
                }
            ],
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
