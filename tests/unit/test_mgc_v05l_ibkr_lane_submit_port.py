from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_lane_submit_port import (
    IbkrLaneSubmitPortConfig,
    run_ibkr_lane_submit_port,
    write_ibkr_lane_submit_port_artifacts,
)


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
                "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
                "strategies": [
                    {
                        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "bridge_strategy_id": "asia_london_participation_core_v1__GC",
                        "standalone_strategy_id": "asia_london_participation_core_v1__GC",
                        "strategy_status": "PROBATION_ACTIVE",
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
    assert artifacts.report["selected_lane"]["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5"
    assert artifacts.report["selected_inventory_row"]["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert artifacts.report["selected_intent_row"]["action"] == "NO_ACTION"


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
