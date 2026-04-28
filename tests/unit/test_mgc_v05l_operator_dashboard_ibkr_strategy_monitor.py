from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.operator_dashboard import OperatorDashboardService


def test_dashboard_reads_live_ibkr_strategy_monitor_runtime_files(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    heartbeat_path = tmp_path / "var" / "paper_strategy_monitor_heartbeat.json"
    ledger_path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    pnl_path = tmp_path / "var" / "paper_strategy_pnl_snapshot.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)

    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "health_classification": "HEALTHY",
                "submit_allowed": True,
                "stale": False,
                "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "broker_position_quantity": 1.0,
                "average_entry_price": 4586.7,
                "unrealized_pnl": 256.03,
                "realized_pnl": 14.18,
                "open_order_count": 0,
                "broker_ledger_match": "MATCH",
                "last_poll_time": "2026-04-28T18:28:25.742464+00:00",
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "ibkr_connection_state": "CONNECTED",
                "continuous_monitor_active": True,
                "block_reasons": [],
                "detail": "healthy",
            }
        ),
        encoding="utf-8",
    )
    heartbeat_path.write_text(json.dumps({"monitor_running": True, "stale": False}), encoding="utf-8")
    ledger_path.write_text(
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
                        "side": "LONG",
                        "quantity": 1.0,
                        "average_entry_price": 4586.7,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    pnl_path.write_text(
        json.dumps(
            {
                "unrealized_pnl": 256.03,
                "realized_pnl": 14.18,
                "pnl_source": "ibkr_updatePortfolio",
            }
        ),
        encoding="utf-8",
    )

    service = OperatorDashboardService(repo_root=tmp_path)
    payload = service._paper_ibkr_strategy_monitor_payload(generated_at="2026-04-28T18:29:00+00:00")

    assert payload["monitor_running"] is True
    assert payload["stale"] is False
    assert payload["strategy_id"] == "ATP_COMPANION_V1_ASIA_US"
    assert payload["position_quantity"] == 1.0
    assert payload["unrealized_pnl"] == 256.03
    assert payload["broker_ledger_match"] == "MATCH"
