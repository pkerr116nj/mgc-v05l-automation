from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)


NOW = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)


def test_reconciles_flat_lifecycle_with_fresh_broker_truth_and_unrelated_positions(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "AAPL",
                "local_symbol": "AAPL",
                "security_type": "STK",
                "quantity": "500",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_broker_position_count"] == 0
    assert report["track_b_broker_open_order_count"] == 0
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["source"] == "BROKER_RECONCILED"
    assert reconciled_position["broker_reconciled"] is True
    assert reconciled_position["broker_reconciled_state"] == "BROKER_AND_LIFECYCLE_FLAT"
    assert reconciled_position["open_position_count"] == 0
    assert reconciled_position["open_order_count"] == 0
    assert reconciled_position["live_money_eligible"] is False
    assert reconciled_position["paper_proof_invoked"] is False


def test_blocks_when_track_b_broker_position_exists_but_lifecycle_is_flat(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])
    assert not config.reconciled_live_position_status_path.exists()


def test_count_mismatch_still_reports_cost_basis_for_matched_positions(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "PL",
                "local_symbol": "PLN6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "107257.52",
                "multiplier": "50",
            },
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "57963.12",
                "multiplier": "2",
            },
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["position_match_report"]["state"] == "BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"
    assert report["position_match_report"]["matches"][0]["root"] == "MNQ"
    assert report["broker_cost_basis_adjustments"][0]["broker_minus_lifecycle_points_per_contract"] == "0.31"
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_reconciles_matching_track_b_broker_and_lifecycle_open_position(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "57963.12",
                "multiplier": "2",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["position_match_report"]["state"] == "BROKER_AND_LIFECYCLE_OPEN_MATCHED"
    assert report["broker_cost_basis_adjustments"] == [
        {
            "source": "IBKR_AVERAGE_PRICE_MINUS_TRACK_B_LIFECYCLE_ENTRY_PRICE",
            "root": "MNQ",
            "broker_local_symbol": "MNQM6",
            "lifecycle_local_symbol": "MNQM6",
            "quantity": "1",
            "lifecycle_average_entry_price": "28981.25",
            "broker_average_price": "28981.56",
            "broker_minus_lifecycle_points_per_contract": "0.31",
            "broker_minus_lifecycle_points_total": "0.31",
            "absolute_points_per_contract": "0.31",
            "absolute_points_total": "0.31",
            "note": "Captured for broker fee/cost-basis tracking only; IBKR broker truth remains authoritative for live PAPER position state.",
        }
    ]
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["broker_reconciled_state"] == "BROKER_AND_LIFECYCLE_OPEN_MATCHED"
    assert reconciled_position["open_position_count"] == 1
    assert reconciled_position["broker_track_b_position_count"] == 1
    assert reconciled_position["broker_cost_basis_adjustments"] == report["broker_cost_basis_adjustments"]
    assert reconciled_position["live_money_eligible"] is False
    assert reconciled_position["paper_proof_invoked"] is False


def test_blocks_when_track_b_open_order_exists(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        open_orders=[
            {
                "order_id": 42,
                "action": "BUY",
                "total_quantity": "1",
                "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "security_type": "FUT"},
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert any(blocker["code"] == "TRACK_B_BROKER_OPEN_ORDER_PRESENT" for blocker in report["blockers"])


def test_blocks_when_broker_truth_is_stale_or_incomplete(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(config, generated_at="2026-05-11T11:55:00+00:00", positions_complete=False)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    codes = {blocker["code"] for blocker in report["blockers"]}
    assert "BROKER_TRUTH_STATUS_STALE" in codes
    assert "BROKER_TRUTH_STATUS_FLAG_MISMATCH" in codes
    assert "BROKER_TRUTH_SNAPSHOT_INCOMPLETE" in codes
    assert report["broker_reconciled"] is False


def test_blocks_when_lifecycle_reports_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert any(blocker["code"] == "LIFECYCLE_REVIEW_REQUIRED_PRESENT" for blocker in report["blockers"])
    assert report["broker_reconciled"] is False


def test_blocks_when_bridge_fill_persistence_is_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "PL",
                "local_symbol": "PLN6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "107257.52",
                "multiplier": "50",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    codes = {blocker["code"] for blocker in report["blockers"]}
    assert "LIFECYCLE_REVIEW_REQUIRED_PRESENT" in codes
    assert "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" in codes
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    assert not config.reconciled_live_position_status_path.exists()


def _write_base_artifacts(
    tmp_path: Path,
    *,
    review_required_count: int = 0,
    open_position: dict[str, object] | None = None,
) -> ReconciliationConfig:
    ledger_root = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    broker_root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    report_path = tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest.json"
    ledger_root.mkdir(parents=True)
    broker_root.mkdir(parents=True)
    config = ReconciliationConfig(
        repo_root=tmp_path,
        ledger_root=ledger_root,
        broker_truth_root=broker_root,
        report_path=report_path,
        max_age_seconds=120.0,
    )
    _write_json(
        config.trade_summary_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "paper_trades_attempted_count": 2,
            "open_position_count": 1 if open_position else 0,
            "review_required_count": review_required_count,
            "recent_trades": [],
        },
    )
    _write_json(
        config.live_position_status_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "open_position_count": 1 if open_position else 0,
            "open_order_count": 0,
            "positions_by_instrument": {str(open_position["contract_key"]): open_position} if open_position else {},
            "positions_by_strategy": {str(open_position["strategy_id"]): open_position} if open_position else {},
            "review_required_positions": [],
        },
    )
    _write_json(
        config.pnl_summary_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "total_realized_pnl_today": "0",
            "total_unrealized_pnl": "0",
            "review_required_count": review_required_count,
            "by_strategy": {},
            "by_instrument": {},
        },
    )
    return config


def _write_broker_truth(
    config: ReconciliationConfig,
    *,
    generated_at: str = "2026-05-11T11:59:30+00:00",
    positions_complete: bool = True,
    open_orders_complete: bool = True,
    positions: list[dict[str, object]] | None = None,
    open_orders: list[dict[str, object]] | None = None,
) -> None:
    positions_path = config.broker_truth_root / "ibkr_positions_snapshot.json"
    open_orders_path = config.broker_truth_root / "ibkr_open_orders_snapshot.json"
    positions_payload = positions or []
    open_orders_payload = open_orders or []
    _write_json(
        config.broker_status_path,
        {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "generated_at": generated_at,
            "latest_refresh_time": generated_at,
            "last_success": True,
            "read_only": True,
            "account": "DUM882026",
            "positions_complete": positions_complete,
            "open_orders_complete": open_orders_complete,
            "position_count": len(positions_payload),
            "open_order_count": len(open_orders_payload),
            "positions_snapshot_path": str(positions_path),
            "open_orders_snapshot_path": str(open_orders_path),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        positions_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "positions_complete": positions_complete,
            "request_method": "reqPositions",
            "positions": positions_payload,
        },
    )
    _write_json(
        open_orders_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "open_orders_complete": open_orders_complete,
            "request_method": "reqAllOpenOrders",
            "auto_open_orders_requested": False,
            "order_binding_requested": False,
            "open_orders": open_orders_payload,
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
