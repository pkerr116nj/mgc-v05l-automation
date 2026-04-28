from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.brokers.ibkr import IbkrQualifiedContract
from mgc_v05l.execution.ibkr_position_reconciliation import (
    IbkrPositionReconciliationArtifacts,
    IbkrPositionReconciliationCollector,
    _build_execution_position_match_rows,
    _diagnose_reconciliation,
    _matches_exact_contract,
    _reference_report_showed_zero_position,
    render_ibkr_position_reconciliation_markdown,
    write_ibkr_position_reconciliation_artifacts,
)


def _exact_contract() -> IbkrQualifiedContract:
    return IbkrQualifiedContract(
        internal_symbol="MGC",
        broker_symbol="MGC",
        local_symbol="MGCM6",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="20260626",
        multiplier="10",
        trading_class="MGC",
        con_id=712565978,
        metadata={"contract_month": "202606"},
    )


def test_matches_exact_contract_rejects_shorthand_expiry_conflict() -> None:
    exact_contract = _exact_contract()

    assert _matches_exact_contract(
        {
            "account_id": "DUM882026",
            "con_id": 712565978,
            "symbol": "MGC",
            "local_symbol": "MGCM6",
            "expiry": "20260626",
        },
        exact_contract,
    )
    assert not _matches_exact_contract(
        {
            "account_id": "DUM882026",
            "con_id": 712565978,
            "symbol": "MGC",
            "local_symbol": "MGCM6",
            "expiry": "202606",
        },
        exact_contract,
    )


def test_reference_report_detects_prior_zero_position() -> None:
    report = {
        "submit_cancel_lifecycle": {
            "fill_verification": {
                "positions_after_submit": {
                    "positions": [
                        {
                            "symbol": "MGC",
                            "local_symbol": "MGCM6",
                            "expiry": "20260626",
                            "quantity": "0.0",
                        }
                    ]
                }
            }
        }
    }

    assert _reference_report_showed_zero_position(report) is True


def test_diagnose_reconciliation_identifies_latency_when_current_long() -> None:
    diagnosis = _diagnose_reconciliation(
        snapshots=[
            {
                "snapshot_index": 0,
                "latest_exact_position_quantity": 1.0,
                "exact_position_rows": [{"quantity": 1.0}],
            }
        ],
        execution_truth={
            "matching_execution_rows": [
                {
                    "side": "BOT",
                    "quantity": 1.0,
                    "executed_at": "2026-04-28T13:08:03+00:00",
                    "perm_id": 490708929,
                }
            ],
            "matching_completed_order_rows": [],
        },
        exact_contract=_exact_contract(),
        reference_fill_report={
            "submit_cancel_lifecycle": {
                "fill_verification": {
                    "positions_after_submit": {
                        "positions": [
                            {
                                "symbol": "MGC",
                                "local_symbol": "MGCM6",
                                "expiry": "20260626",
                                "quantity": "0.0",
                            }
                        ]
                    }
                }
            }
        },
    )

    assert diagnosis["classification"] == "IBKR_POSITION_RECONCILED_LONG_MGC"
    assert diagnosis["likely_root_cause"] == "timing_or_request_sequencing_latency"


def test_diagnose_reconciliation_identifies_flat_when_sell_present() -> None:
    diagnosis = _diagnose_reconciliation(
        snapshots=[
            {
                "snapshot_index": 0,
                "latest_exact_position_quantity": 0.0,
                "exact_position_rows": [{"quantity": 0.0}],
            }
        ],
        execution_truth={
            "matching_execution_rows": [
                {
                    "side": "BOT",
                    "quantity": 1.0,
                    "executed_at": "2026-04-28T13:08:03+00:00",
                    "perm_id": 490708929,
                },
                {
                    "side": "SLD",
                    "quantity": 1.0,
                    "executed_at": "2026-04-28T13:11:03+00:00",
                    "perm_id": 490708930,
                },
            ],
            "matching_completed_order_rows": [],
        },
        exact_contract=_exact_contract(),
        reference_fill_report=None,
    )

    assert diagnosis["classification"] == "IBKR_POSITION_RECONCILED_FLAT"
    assert diagnosis["likely_root_cause"] == "position_flat_after_sell_execution"


def test_diagnose_reconciliation_partial_without_position_confirmation() -> None:
    diagnosis = _diagnose_reconciliation(
        snapshots=[
            {
                "snapshot_index": 0,
                "latest_exact_position_quantity": 0.0,
                "exact_position_rows": [{"quantity": 0.0}],
            }
        ],
        execution_truth={
            "matching_execution_rows": [
                {
                    "side": "BOT",
                    "quantity": 1.0,
                    "executed_at": "2026-04-28T13:08:03+00:00",
                    "perm_id": 490708929,
                }
            ],
            "matching_completed_order_rows": [],
        },
        exact_contract=_exact_contract(),
        reference_fill_report=None,
    )

    assert diagnosis["classification"] == "IBKR_POSITION_RECONCILIATION_PARTIAL"
    assert diagnosis["likely_root_cause"] == "execution_truth_without_position_confirmation"


def test_build_execution_position_match_rows_uses_reference_order_id() -> None:
    rows = _build_execution_position_match_rows(
        snapshots=[
            {
                "snapshot_index": 0,
                "generated_at": "2026-04-28T13:10:00+00:00",
                "exact_position_rows": [],
                "latest_exact_position_quantity": 0.0,
                "latest_portfolio_update_quantity": None,
            }
        ],
        execution_rows=[],
        completed_order_rows=[],
        exact_contract=_exact_contract(),
        reference_fill_report={"submit_cancel_lifecycle": {"submitted_order_id": 1}},
    )

    assert rows[0]["broker_order_id"] == 1


def test_collector_records_portfolio_updates() -> None:
    class _Client:
        pass

    collector = IbkrPositionReconciliationCollector(_Client())  # type: ignore[arg-type]
    collector.update_portfolio(
        account_id="DUM882026",
        contract={"conId": 712565978, "symbol": "MGC", "localSymbol": "MGCM6", "lastTradeDateOrContractMonth": "20260626"},
        quantity=1,
        market_price=4589.6,
        market_value=45896.0,
        average_cost=4589.6,
        unrealized_pnl=0.0,
        realized_pnl=0.0,
    )

    assert collector.portfolio_update_rows[0]["account_id"] == "DUM882026"
    assert collector.portfolio_update_rows[0]["quantity"] == 1.0


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    artifacts = IbkrPositionReconciliationArtifacts(
        classification="IBKR_POSITION_RECONCILED_LONG_MGC",
        report={
            "classification": "IBKR_POSITION_RECONCILED_LONG_MGC",
            "generated_at": "2026-04-28T13:20:00+00:00",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "account_id": "DUM882026",
            "connection_check": {"client_id": 9161},
            "contract_report": {"exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"}},
            "diagnosis": {
                "classification": "IBKR_POSITION_RECONCILED_LONG_MGC",
                "conclusion": "Long confirmed.",
                "likely_root_cause": "timing_or_request_sequencing_latency",
                "latest_exact_position_quantity": 1.0,
                "max_exact_position_quantity": 1.0,
                "latest_matching_execution_side": "BOT",
                "latest_matching_execution_time": "2026-04-28T13:08:03+00:00",
                "latest_matching_execution_quantity": 1.0,
                "latest_matching_perm_id": 490708929,
            },
            "execution_truth": {"matching_execution_count": 1, "matching_completed_order_count": 0},
            "portfolio_update_summary": {"matching_row_count": 1, "portfolio_callback_supported": True},
            "snapshots_observed": 3,
        },
        snapshots=[{"snapshot_index": 0}],
        match_rows=[
            {
                "snapshot_index": 0,
                "snapshot_generated_at": "2026-04-28T13:20:00+00:00",
                "position_match_status": "exact_quantity_match",
                "position_quantity": 1.0,
                "portfolio_update_quantity": 1.0,
                "execution_id": "abc",
                "execution_side": "BOT",
                "execution_quantity": 1.0,
                "execution_price": 4589.6,
                "execution_time": "2026-04-28T13:08:03+00:00",
                "completed_order_status": None,
                "perm_id": 490708929,
                "broker_order_id": 1,
                "note": "ok",
            }
        ],
    )

    write_ibkr_position_reconciliation_artifacts(output_dir=tmp_path, artifacts=artifacts)
    report = json.loads((tmp_path / "ibkr_position_reconciliation_report.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "ibkr_position_reconciliation_report.md").read_text(encoding="utf-8")

    assert report["classification"] == "IBKR_POSITION_RECONCILED_LONG_MGC"
    assert "classification: `IBKR_POSITION_RECONCILED_LONG_MGC`" in markdown
    assert (tmp_path / "ibkr_position_reconciliation_snapshots.jsonl").exists()
    assert (tmp_path / "ibkr_execution_position_match_table.csv").exists()


def test_render_markdown_calls_out_follow_up_for_partial() -> None:
    markdown = render_ibkr_position_reconciliation_markdown(
        {
            "classification": "IBKR_POSITION_RECONCILIATION_PARTIAL",
            "generated_at": "2026-04-28T13:20:00+00:00",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "account_id": "DUM882026",
            "connection_check": {"client_id": 9161},
            "contract_report": {"exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"}},
            "diagnosis": {
                "conclusion": "Execution truth confirms the fill but positions remain unresolved.",
                "likely_root_cause": "execution_truth_without_position_confirmation",
                "latest_exact_position_quantity": 0.0,
                "max_exact_position_quantity": 0.0,
                "latest_matching_execution_side": "BOT",
                "latest_matching_execution_time": "2026-04-28T13:08:03+00:00",
                "latest_matching_execution_quantity": 1.0,
                "latest_matching_perm_id": 490708929,
            },
            "execution_truth": {"matching_execution_count": 1, "matching_completed_order_count": 0},
            "portfolio_update_summary": {"matching_row_count": 0, "portfolio_callback_supported": True},
            "snapshots_observed": 3,
        }
    )

    assert "position truth is still not fully authoritative" in markdown
