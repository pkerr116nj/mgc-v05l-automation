from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    run_ibkr_paper_strategy_porting,
    write_ibkr_paper_strategy_porting_artifacts,
)


def _config(tmp_path: Path) -> IbkrPaperStrategyPortingConfig:
    return IbkrPaperStrategyPortingConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_porting",
        dashboard_snapshot_path=Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json",
        signal_audit_path=Path("outputs") / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json",
        strategy_performance_path=Path("outputs") / "operator_dashboard" / "paper_strategy_performance_snapshot.json",
        ledger_path=Path("var") / "paper_strategy_position_ledger.json",
        bridge_audit_path=Path("outputs") / "reports" / "ibkr_strategy_porting" / "ibkr_paper_strategy_bridge_porting_audit.jsonl",
    )


def _write_monitor(tmp_path: Path, **overrides: object) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
        "monitor_running": True,
        "health_classification": "HEALTHY",
        "stale": False,
        "submit_allowed": True,
        "broker_position_quantity": 1.0,
        "ledger_position_quantity": 1.0,
        "open_order_count": 0,
        "last_successful_broker_refresh": "2026-04-28T19:38:25.796168+00:00",
    }
    payload.update(overrides)
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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
    path.write_text(json.dumps({"paper": {"status": {"stale": False}, "readiness": {"current_detected_session": "US_LATE"}}}), encoding="utf-8")


def _write_signal_audit(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
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
                "id": "ATP_COMPANION_V1_ASIA_US",
                "lane_id": "atp_companion_v1_asia_us",
                "instrument": "MGC",
                "family": "active_trend_participation_engine",
                "current_strategy_status": "READY",
                "entries_enabled": True,
                "eligible_now": False,
                "audit_verdict": "FILLED",
                "last_actionable_signal_family": "atp_pullback_long",
                "last_actionable_signal_timestamp": "2026-04-28T14:30:00-04:00",
                "last_recent_long_setup": False,
                "last_recent_short_setup": False,
                "last_intent_type": "BUY_TO_OPEN",
                "last_fill_timestamp": "2026-04-28T14:34:38-04:00",
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
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_strategy_performance(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
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
                "lane_id": "atp_companion_v1_asia_us",
                "instrument": "MGC",
                "strategy_family": "active_trend_participation_engine",
                "standalone_strategy_id": "ATP_COMPANION_V1_ASIA_US",
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
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_builds_inventory_and_intent_rows_for_live_paper_lanes(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_porting(config=_config(tmp_path))

    assert artifacts.classification == "IBKR_PAPER_STRATEGY_PORT_PARTIAL"
    assert artifacts.adapter_classification == "STRATEGY_INTENT_ADAPTER_READY"
    assert len(artifacts.inventory_rows) == 4
    atp = next(row for row in artifacts.inventory_rows if row["strategy_id"] == "atp_companion_v1_asia_us")
    assert atp["current_position_state"] == "LONG"
    assert atp["current_order_destination"] == "ibkr_paper_bridge_adopted_position"
    gc = next(row for row in artifacts.inventory_rows if row["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5")
    assert gc["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert gc["bridge_adapter_ready"] is True
    mgc = next(row for row in artifacts.intent_rows if row["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5")
    assert mgc["action"] == "NO_ACTION"
    gc_intent = next(row for row in artifacts.intent_rows if row["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5")
    assert gc_intent["bridge_submit_capable"] is True
    assert gc_intent["bridge_execution_target"]["symbol"] == "MGC"
    nq = next(row for row in artifacts.intent_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["can_route_to_ibkr_now"] is False
    assert "unsupported_instrument_scope" in nq["route_blockers"]


def test_write_porting_artifacts(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_porting(config=config)
    write_ibkr_paper_strategy_porting_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_strategy_porting"
    assert (output_dir / "ibkr_live_paper_strategy_inventory.csv").exists()
    assert (output_dir / "ibkr_strategy_intent_adapter_report.md").exists()
    assert (output_dir / "ibkr_paper_strategy_bridge_porting_report.json").exists()
    assert (output_dir / "ibkr_paper_strategy_bridge_porting_report.md").exists()
    assert (output_dir / "per_strategy_ibkr_paper_status.csv").exists()
    assert (output_dir / "per_strategy_order_intent_examples.jsonl").exists()
