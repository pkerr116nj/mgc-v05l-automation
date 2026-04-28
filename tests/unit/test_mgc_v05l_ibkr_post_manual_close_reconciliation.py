from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_post_manual_close_reconciliation import (
    IbkrPostManualCloseReconciliationConfig,
    render_ibkr_post_manual_close_reconciliation_markdown,
    run_ibkr_post_manual_close_reconciliation,
    write_ibkr_post_manual_close_reconciliation_artifacts,
)


def test_reconciles_flat_after_manual_close(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        )
    )

    assert artifacts.classification == "IBKR_RECONCILED_FLAT_AFTER_MANUAL_CLOSE"
    assert artifacts.report["current_truth"]["exact_position_quantity"] == 0.0
    assert artifacts.report["current_truth"]["working_mgc_open_order_count"] == 0
    assert artifacts.report["final_close_fill"]["execution_row"]["perm_id"] == 490708950
    assert artifacts.report["original_unattended_close_order"]["submitted_perm_id"] == 490708950


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        )
    )
    write_ibkr_post_manual_close_reconciliation_artifacts(output_dir=tmp_path / "out", artifacts=artifacts)

    report = json.loads((tmp_path / "out" / "ibkr_post_manual_close_reconciliation_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_post_manual_close_reconciliation_markdown(report)
    assert "IBKR_RECONCILED_FLAT_AFTER_MANUAL_CLOSE" in markdown
    assert "permId=490708950" in markdown
    assert (tmp_path / "out" / "ibkr_post_manual_close_positions.json").exists()
    assert (tmp_path / "out" / "ibkr_post_manual_close_open_orders.json").exists()
    assert (tmp_path / "out" / "ibkr_post_manual_close_executions.json").exists()
    assert (tmp_path / "out" / "ibkr_post_manual_close_completed_orders.json").exists()


def _reconciliation_report() -> dict:
    return {
        "contract_report": {
            "exact_contract": {
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "exchange": "COMEX",
                "currency": "USD",
                "multiplier": "10",
            }
        },
        "diagnosis": {
            "latest_exact_position_quantity": 0.0,
        },
        "execution_truth": {
            "matching_execution_rows": [
                {
                    "account_id": "DUM882026",
                    "broker_order_id": 1,
                    "client_id": 9191,
                    "perm_id": 490708950,
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "security_type": "FUT",
                    "exchange": "COMEX",
                    "currency": "USD",
                    "expiry": "20260626",
                    "multiplier": "10",
                    "trading_class": "MGC",
                    "con_id": 712565978,
                    "side": "SLD",
                    "quantity": 1.0,
                    "price": 4584.4,
                    "executed_at": "2026-04-28T14:26:12.268274+00:00",
                },
                {
                    "account_id": "DUM882026",
                    "broker_order_id": 1,
                    "client_id": 9157,
                    "perm_id": 490708929,
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "security_type": "FUT",
                    "exchange": "COMEX",
                    "currency": "USD",
                    "expiry": "20260626",
                    "multiplier": "10",
                    "trading_class": "MGC",
                    "con_id": 712565978,
                    "side": "BOT",
                    "quantity": 1.0,
                    "price": 4589.6,
                    "executed_at": "2026-04-28T14:26:12.267979+00:00",
                },
            ],
            "matching_completed_order_rows": [
                {
                    "account_id": "DUM882026",
                    "broker_order_id": 0,
                    "client_id": 0,
                    "perm_id": 490708950,
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "security_type": "FUT",
                    "exchange": "COMEX",
                    "currency": "USD",
                    "expiry": "20260626",
                    "multiplier": "10",
                    "trading_class": "MGC",
                    "con_id": 712565978,
                    "status": "Filled",
                    "quantity": 0.0,
                    "completed_at": "2026-04-28T14:26:12.168517+00:00",
                }
            ],
        },
    }


def _observation_report() -> dict:
    return {
        "positions_snapshot": {
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "expiry": "20260626",
                    "quantity": "0.0",
                    "security_type": "FUT",
                }
            ]
        },
        "open_order_snapshot": {
            "open_order_count": 0,
            "open_orders": [],
        },
    }


def _unattended_close_report() -> dict:
    return {
        "classification": "IBKR_UNATTENDED_CLOSE_UNKNOWN",
        "limit_price": 4588.9,
        "lifecycle": {
            "submitted_order_id": 1,
            "submitted_perm_id": 490708950,
            "latest_order_status": {
                "client_id": 9191,
                "status": "Submitted",
            },
            "open_order_after_submit": {
                "open_orders": [
                    {
                        "status": "Submitted",
                        "broker_order_id": 1,
                        "perm_id": 490708950,
                        "client_id": 9191,
                    }
                ]
            },
            "positions_after_submit": {
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "symbol": "MGC",
                        "local_symbol": "MGCM6",
                        "expiry": "20260626",
                        "quantity": "1.0",
                    }
                ]
            },
        },
    }
