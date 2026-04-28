from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_monitor import (
    IbkrPaperStrategyMonitorConfig,
    load_paper_strategy_monitor_status,
    run_ibkr_paper_strategy_monitor,
    write_ibkr_paper_strategy_monitor_artifacts,
)


def _config(tmp_path: Path, **overrides: object) -> IbkrPaperStrategyMonitorConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9245,
        "account_id": "DUM882026",
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "symbol": "MGC",
        "contract_month": "202606",
        "exact_expiry": "20260626",
        "con_id": 712565978,
        "local_symbol": "MGCM6",
        "dashboard_url": "http://127.0.0.1:8790/api/dashboard",
        "ledger_path": Path("var") / "paper_strategy_position_ledger.json",
        "output_dir": Path("outputs") / "reports" / "paper_strategy_monitor",
        "recent_fill_lookback_minutes": 240,
        "bridge_report_path": Path("outputs") / "reports" / "ibkr_paper_strategy_bridge" / "ibkr_paper_strategy_bridge_report.json",
        "prepared_bundle_path": Path("outputs") / "reports" / "ibkr_paper_strategy_bridge" / "prepared_manual_harness" / "ibkr_manual_paper_fill_test_frozen_preview.json",
        "strategy_tracking_snapshot_path": Path("outputs") / "reports" / "ibkr_paper_strategy_tracking_snapshot" / "strategy_position_snapshot.json",
    }
    payload.update(overrides)
    return IbkrPaperStrategyMonitorConfig(**payload)


def _dashboard_payload(*, stale: bool = True, attached: bool = True) -> dict[str, object]:
    return {
        "dashboard_meta": {"degraded": False},
        "supervised_paper_operability": {
            "dashboard_attached": attached,
            "launch_allowed": attached,
            "state": "USABLE" if attached else "UNAVAILABLE",
            "summary_line": "ready" if attached else "fallback",
        },
        "startup_control_plane": {"overall_state": "READY" if attached else "DOWN"},
        "paper": {
            "status": {"stale": stale, "market_data_semantics": "STALE" if stale else "LIVE_DELAYED"},
            "temporary_paper_runtime_integrity": {"temp_paper_blocked": False, "mismatch_status": "CLEAR"},
            "readiness": {"current_detected_session": "US_MIDDAY"},
        },
    }


def _reconciliation_report(*, quantity: float = 1.0, perm_id: int = 490708968, client_id: int = 10221) -> dict[str, object]:
    exec_row = {
        "broker_order_id": 1,
        "client_id": client_id,
        "con_id": 712565978,
        "executed_at": "2026-04-28T15:34:38.556912+00:00",
        "execution_id": "0000e1a7.69f1fa35.01.01",
        "expiry": "20260626",
        "local_symbol": "MGCM6",
        "perm_id": perm_id,
        "price": 4586.7,
        "quantity": 1.0,
        "side": "BOT",
        "symbol": "MGC",
    }
    return {
        "classification": "IBKR_POSITION_RECONCILED_LONG_MGC" if quantity == 1.0 else "IBKR_POSITION_RECONCILED_FLAT",
        "diagnosis": {
            "latest_exact_position_quantity": quantity,
            "latest_matching_perm_id": perm_id,
        },
        "execution_truth": {
            "recent_matching_execution_rows": [exec_row],
            "matching_execution_rows": [exec_row],
        },
        "portfolio_update_summary": {
            "rows": [
                {
                    "average_cost": 45867.97,
                    "market_price": 4587.3,
                    "market_value": 45873.0,
                    "quantity": quantity,
                    "realized_pnl": 14.18,
                    "unrealized_pnl": 5.03,
                    "updated_at": "2026-04-28T15:34:29.936252+00:00",
                }
            ]
        },
        "provider_snapshot": {"open_orders": []},
        "account_truth": {"net_liquidation": 1100299.05, "buying_power": 3747308.30, "currency": "USD"},
    }


class _Artifacts:
    def __init__(self, report: dict[str, object]) -> None:
        self.classification = str(report.get("classification"))
        self.report = report


def _write_ownership_evidence(tmp_path: Path, *, perm_id: int = 490708968, client_id: int = 10221) -> None:
    bridge_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_bridge"
    prepared_path = bridge_path / "prepared_manual_harness"
    tracking_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_tracking_snapshot"
    prepared_path.mkdir(parents=True, exist_ok=True)
    tracking_path.mkdir(parents=True, exist_ok=True)
    (bridge_path / "ibkr_paper_strategy_bridge_report.json").write_text(
        json.dumps(
            {
                "intent": {
                    "intent_id": "intent-1",
                    "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                    "action": "BUY",
                    "quantity": 1.0,
                }
            }
        ),
        encoding="utf-8",
    )
    (prepared_path / "ibkr_manual_paper_fill_test_frozen_preview.json").write_text(
        json.dumps({"preview_payload": {"environment": {"client_id": client_id}}}),
        encoding="utf-8",
    )
    (tracking_path / "strategy_position_snapshot.json").write_text(
        json.dumps(
            {
                "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "latest_matching_perm_id": perm_id,
                "current_reconciled_quantity": 1.0,
            }
        ),
        encoding="utf-8",
    )


def test_adopts_current_broker_position_and_blocks_submit_when_runtime_is_stale(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=True),
    )

    assert artifacts.classification == "PAPER_STRATEGY_POSITION_ADOPTED"
    assert artifacts.status["submit_allowed"] is False
    assert "paper_runtime_stale" in artifacts.status["block_reasons"]
    assert artifacts.ledger["positions"][0]["strategy_id"] == "ATP_COMPANION_V1_ASIA_US"
    assert artifacts.ledger["positions"][0]["perm_id"] == 490708968


def test_orphan_position_is_classified_when_ownership_cannot_be_proven(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_ORPHAN_POSITION"
    assert artifacts.status["submit_allowed"] is False
    assert "orphan_broker_position" in artifacts.status["block_reasons"]
    assert artifacts.ledger["positions"] == []
    assert len(artifacts.ledger["orphan_positions"]) == 1


def test_ledger_broker_mismatch_is_classified(tmp_path: Path) -> None:
    ledger_path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "quantity": 0.0,
                        "adopted_from_broker_truth": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_LEDGER_BROKER_MISMATCH"
    assert "ledger_broker_mismatch" in artifacts.status["block_reasons"]


def test_write_artifacts_and_load_status(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)
    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_monitor(
        config=config,
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    write_ibkr_paper_strategy_monitor_artifacts(config=config, artifacts=artifacts)

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)
    assert status["classification"] in {"PAPER_STRATEGY_POSITION_ADOPTED", "PAPER_STRATEGY_MONITOR_ACTIVE"}
    assert (tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_strategy_position_ledger.json").exists()
    assert (tmp_path / "var" / "paper_strategy_position_ledger.json").exists()
