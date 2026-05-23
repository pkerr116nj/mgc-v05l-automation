from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution.ibkr_post_manual_close_reconciliation import (
    IbkrPostManualCloseReconciliationConfig,
    render_ibkr_post_manual_close_reconciliation_markdown,
    run_ibkr_post_manual_close_reconciliation,
    write_ibkr_post_manual_close_reconciliation_artifacts,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)


def test_reconciles_flat_after_manual_close(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(tmp_path)

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
    )

    assert artifacts.classification == "IBKR_RECONCILED_FLAT_AFTER_MANUAL_CLOSE"
    assert artifacts.report["current_truth"]["exact_position_quantity"] == 0.0
    assert artifacts.report["current_truth"]["working_mgc_open_order_count"] == 0
    assert artifacts.report["final_close_fill"]["execution_row"]["perm_id"] == 490708950
    assert artifacts.report["original_unattended_close_order"]["submitted_perm_id"] == 490708950
    assert artifacts.report["shared_truth_evidence"]["blockers"] == []
    assert artifacts.report["shared_truth_evidence"]["classifications"]["open_order_truth"] == "NO_OPEN_ORDERS"


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(tmp_path)

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
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


def test_open_order_still_exists_blocks_flat_derivation(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report(open_order_count=1)), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(tmp_path, open_order_truth_classification="OPEN_CLOSE_ORDER_WORKING")

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
    )

    assert artifacts.classification == "IBKR_RECONCILIATION_SHARED_TRUTH_BLOCKED"
    assert artifacts.report["current_truth"]["working_mgc_open_order_count"] == 1
    assert any("Open Order Truth blocks" in item for item in artifacts.report["shared_truth_evidence"]["blockers"])


def test_broker_position_still_exists_does_not_mark_flat(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report(latest_exact_position_quantity=1.0)), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report(position_qty="1.0")), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(tmp_path, position_truth_rows=[_shared_position_row()])

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
    )

    assert artifacts.classification == "IBKR_RECONCILED_POSITION_STILL_OPEN"
    assert artifacts.report["current_truth"]["exact_position_quantity"] == 1.0


def test_ambiguous_lifecycle_target_blocks_review(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(
        tmp_path,
        managed_position_registry_rows=[
            {
                "classification": "REVIEW_REQUIRED",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "quantity": "1",
            }
        ],
    )

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
    )

    assert artifacts.classification == "IBKR_RECONCILIATION_SHARED_TRUTH_BLOCKED"
    assert any("active rows conflict" in item for item in artifacts.report["shared_truth_evidence"]["blockers"])


def test_suspicious_order_truth_blocks_post_manual_close_reconciliation(tmp_path: Path) -> None:
    recon_path = tmp_path / "recon.json"
    dry_path = tmp_path / "dry.json"
    close_path = tmp_path / "close.json"
    recon_path.write_text(json.dumps(_reconciliation_report()), encoding="utf-8")
    dry_path.write_text(json.dumps(_observation_report()), encoding="utf-8")
    close_path.write_text(json.dumps(_unattended_close_report()), encoding="utf-8")
    _write_shared_truth(tmp_path, open_order_truth_classification="SUSPICIOUS_ORDER_STATE")

    artifacts = run_ibkr_post_manual_close_reconciliation(
        config=IbkrPostManualCloseReconciliationConfig(
            repo_root=tmp_path,
            read_only=True,
            position_reconciliation_report_path=recon_path,
            observation_dry_run_report_path=dry_path,
            unattended_close_report_path=close_path,
        ),
        now=NOW,
    )

    assert artifacts.classification == "IBKR_RECONCILIATION_SHARED_TRUTH_BLOCKED"
    assert any("SUSPICIOUS_ORDER_STATE" in item for item in artifacts.report["shared_truth_evidence"]["blockers"])


def test_post_manual_close_reconciliation_does_not_consume_dashboard_projections() -> None:
    source = Path("src/mgc_v05l/execution/ibkr_post_manual_close_reconciliation.py").read_text(encoding="utf-8")

    assert "latest_track_b_position_truth.json" not in source
    assert "latest_track_b_open_order_truth.json" not in source
    assert "latest_track_b_managed_orders.json" not in source
    assert "latest_track_b_managed_positions.json" not in source
    assert "latest_track_b_runtime_supervisor_authority.json" not in source


def _reconciliation_report(*, latest_exact_position_quantity: float = 0.0) -> dict:
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
            "latest_exact_position_quantity": latest_exact_position_quantity,
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


def _observation_report(*, position_qty: str = "0.0", open_order_count: int = 0) -> dict:
    return {
        "positions_snapshot": {
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "expiry": "20260626",
                    "quantity": position_qty,
                    "security_type": "FUT",
                }
            ]
        },
        "open_order_snapshot": {
            "open_order_count": open_order_count,
            "open_orders": [{} for _ in range(open_order_count)],
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


def _write_shared_truth(
    tmp_path: Path,
    *,
    open_order_truth_classification: str = "NO_OPEN_ORDERS",
    managed_order_registry_classification: str = "NO_MANAGED_ORDERS",
    runtime_supervisor_classification: str = "SUPERVISOR_NO_ACTION_NEEDED",
    position_truth_rows: list[dict] | None = None,
    managed_position_registry_rows: list[dict] | None = None,
) -> None:
    generated_at = NOW.isoformat()
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {
            "classification": open_order_truth_classification,
            "generated_at": generated_at,
            "order_states": [],
            "source_authority": "execution_core_authority",
        },
    )
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "classification": managed_order_registry_classification,
            "generated_at": generated_at,
            "managed_orders": [],
            "source_authority": "execution_core_authority",
        },
    )
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "classification": "CLEAN_FLAT_READY" if not position_truth_rows else "ATTENTION_REQUIRED",
            "generated_at": generated_at,
            "position_states": position_truth_rows or [],
            "summary": {"overall_classification": "CLEAN_FLAT_READY" if not position_truth_rows else "ATTENTION_REQUIRED"},
            "source_authority": "execution_core_authority",
        },
    )
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {
            "classification": "NO_MANAGED_POSITIONS" if not managed_position_registry_rows else "REVIEW_REQUIRED",
            "generated_at": generated_at,
            "managed_positions": managed_position_registry_rows or [],
            "source_authority": "execution_core_authority",
        },
    )
    _write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "runtime_supervisor"
        / "latest_runtime_supervisor_authority.json",
        {
            "classification": runtime_supervisor_classification,
            "generated_at": generated_at,
            "source_authority": "execution_core_authority",
        },
    )
    _write_json(
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": generated_at,
        },
    )
    _write_json(
        tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {
            "classification": "ACTIVE",
            "lease_state": "ACTIVE",
            "generated_at": generated_at,
        },
    )


def _shared_position_row() -> dict:
    return {
        "classification": "OPEN_MANAGED_MATCHED",
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
