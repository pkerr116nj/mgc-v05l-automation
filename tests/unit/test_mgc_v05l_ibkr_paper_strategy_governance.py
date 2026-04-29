from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    load_paper_strategy_governance_status,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)


def _config(tmp_path: Path) -> IbkrPaperStrategyGovernanceConfig:
    return IbkrPaperStrategyGovernanceConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_governance",
        var_status_path=Path("var") / "per_strategy_paper_status.json",
        var_dashboard_path=Path("var") / "strategy_probation_dashboard.json",
        var_performance_path=Path("var") / "per_strategy_paper_performance.csv",
    )


def _write_monitor(tmp_path: Path, **overrides: object) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
        "monitor_running": True,
        "health_classification": "HEALTHY",
        "stale": False,
        "submit_allowed": True,
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "account_id": "DUM882026",
        "broker_position_quantity": 1.0,
        "ledger_position_quantity": 1.0,
        "open_order_count": 0,
        "last_successful_broker_refresh": "2026-04-28T20:14:04.229102+00:00",
        "block_reasons": [],
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
                        "account_id": "DUM882026",
                        "symbol": "MGC",
                        "expiry": "20260626",
                        "con_id": 712565978,
                        "local_symbol": "MGCM6",
                        "quantity": 1.0,
                        "side": "LONG",
                        "average_entry_price": 4586.7,
                        "unrealized_pnl": 223.03,
                        "realized_pnl": 14.18,
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
    path.write_text(
        json.dumps(
            {
                "paper": {
                    "status": {"stale": False},
                    "readiness": {"current_detected_session": "US_LATE"},
                    "tracked_strategies": {
                        "details_by_strategy_id": {
                            "atp_companion_v1_asia_us": {
                                "strategy_id": "atp_companion_v1_asia_us",
                                "status": "ACTIVE",
                                "realized_pnl": "-444.0",
                                "open_pnl": "223.03",
                                "trade_count": 27,
                                "winner_count": 12,
                                "loser_count": 15,
                                "win_rate": "44.44",
                                "profit_factor": "0.61",
                                "max_drawdown": "642.0",
                                "current_day_pnl": "0",
                                "latest_trade_timestamp": "2026-04-20T20:51:00+00:00",
                            }
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )


def _write_signal_audit(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
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
                        "last_actionable_signal_timestamp": "2026-04-28T17:40:00+00:00",
                        "last_fill_timestamp": "2026-04-28T17:44:00+00:00",
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
                        "last_fill_timestamp": None,
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
                        "last_actionable_signal_timestamp": "2026-04-28T17:35:00+00:00",
                        "last_fill_timestamp": None,
                        "last_recent_long_setup": True,
                    },
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
                        "lane_id": "atp_companion_v1_asia_us",
                        "instrument": "MGC",
                        "strategy_family": "active_trend_participation_engine",
                        "standalone_strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "-444.0",
                        "trade_count": 27,
                        "day_pnl": "0",
                        "max_drawdown": "642.0",
                    },
                    {
                        "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "MGC",
                        "strategy_family": "asia_london_participation_core_v1",
                        "standalone_strategy_id": "asia_london_participation_core_v1__MGC",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "0",
                        "trade_count": 0,
                        "day_pnl": "0",
                        "max_drawdown": "0",
                    },
                    {
                        "lane_id": "nq_1x_ny_early_core__us_late_long",
                        "instrument": "NQ",
                        "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                        "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "1130.0",
                        "trade_count": 7,
                        "day_pnl": "0",
                        "max_drawdown": "520.0",
                    },
                ],
                "trade_log": [
                    {"lane_id": "atp_companion_v1_asia_us", "realized_pnl": "25.0", "exit_ts": "2026-04-20T20:51:00+00:00"},
                    {"lane_id": "atp_companion_v1_asia_us", "realized_pnl": "-104.0", "exit_ts": "2026-04-19T22:11:00+00:00"},
                    {"lane_id": "nq_1x_ny_early_core__us_late_long", "realized_pnl": "675.0", "exit_ts": "2026-04-28T18:02:00+00:00"},
                ],
            }
        ),
        encoding="utf-8",
    )


def test_governance_builds_strategy_rows_and_status_payload(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    atp = next(row for row in artifacts.performance_rows if row["strategy_id"] == "atp_companion_v1_asia_us")
    assert atp["current_state"] == "LONG"
    assert atp["strategy_status"] == "DEGRADED"
    assert atp["submit_allowed"] is True
    mgc = next(row for row in artifacts.performance_rows if row["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5")
    assert "conflicting_owned_position_under_other_strategy" not in mgc["submit_block_reasons"]
    assert mgc["strategy_status"] == "PROBATION_ACTIVE"
    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["strategy_status"] == "WATCHLIST"


def test_write_artifacts_and_load_by_bridge_strategy_id(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_strategy_governance"
    assert (output_dir / "per_strategy_paper_performance.csv").exists()
    assert (output_dir / "per_strategy_paper_status.json").exists()
    assert (output_dir / "strategy_probation_dashboard.json").exists()
    assert (output_dir / "strategy_pause_reasons.csv").exists()
    assert (output_dir / "strategy_performance_governance_report.md").exists()

    payload = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="ATP_COMPANION_V1_ASIA_US")
    assert payload["selected_strategy"]["strategy_id"] == "atp_companion_v1_asia_us"
    assert payload["selected_strategy"]["bridge_strategy_id"] == "ATP_COMPANION_V1_ASIA_US"


def test_load_status_blocks_when_file_missing(tmp_path: Path) -> None:
    payload = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="ATP_COMPANION_V1_ASIA_US")

    assert payload["submit_allowed"] is False
    assert "paper_strategy_governance_status_missing" in payload["block_reasons"]
