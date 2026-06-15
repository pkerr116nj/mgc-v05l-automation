from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_open_order_truth import (
    BROKER_FLAT_WITH_OPEN_CLOSE_ORDER,
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    CLOSE_ORDER_MARKETABLE_NOT_FILLED,
    DUPLICATE_CLOSE_ORDER,
    NO_OPEN_ORDERS,
    OPEN_CLOSE_ORDER_WORKING,
    OPEN_ENTRY_ORDER_WORKING,
    PAPER_TEST_ORDER_PENDING_CANCEL_QUARANTINED,
    SUSPICIOUS_ORDER_STATE,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth,
    write_track_b_open_order_truth,
)


NOW = datetime(2026, 5, 22, 13, 5, tzinfo=UTC)


def test_open_order_truth_reports_no_open_orders(tmp_path: Path) -> None:
    _seed_reconciliation(tmp_path)

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == NO_OPEN_ORDERS
    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_one_working_close_order_is_classified(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        broker_positions=[_position("MNQ", "MNQM6", "1")],
        open_orders=[_order(symbol="MNQ", local_symbol="MNQM6", action="SELL", order_id=27)],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPEN_CLOSE_ORDER_WORKING
    assert payload["summary"]["working_close_order_count"] == 1
    assert payload["order_states"][0]["is_close_order"] is True


def test_duplicate_close_orders_are_classified(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        broker_positions=[_position("MNQ", "MNQM6", "1")],
        open_orders=[
            _order(symbol="MNQ", local_symbol="MNQM6", action="SELL", order_id=27, perm_id=1001),
            _order(symbol="MNQ", local_symbol="MNQM6", action="SELL", order_id=28, perm_id=1002),
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == DUPLICATE_CLOSE_ORDER
    assert payload["summary"]["duplicate_close_order_group_count"] == 1


def test_sentinel_filled_quantity_on_matching_working_close_is_diagnostic(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        broker_positions=[_position("MNQ", "MNQM6", "1")],
        open_orders=[
            _order(
                symbol="MNQ",
                local_symbol="MNQM6",
                action="SELL",
                order_id=27,
                filled_quantity="1.7976931348623157e+308",
                remaining_quantity=None,
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPEN_CLOSE_ORDER_WORKING
    state = payload["order_states"][0]
    assert state["classification"] == OPEN_CLOSE_ORDER_WORKING
    assert state["suspicious"] is False
    assert state["suspicious_reasons"] == []
    assert state["diagnostic_status_gaps"] == ["missing_remaining_quantity", "sentinel_filled_quantity"]
    assert state["ibkr_order_status_quantity_unreliable"] is True


def test_sentinel_filled_quantity_on_entry_order_remains_suspicious(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                symbol="MNQ",
                local_symbol="MNQM6",
                action="BUY",
                order_id=27,
                filled_quantity="1.7976931348623157e+308",
                remaining_quantity=None,
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUSPICIOUS_ORDER_STATE
    assert "sentinel_filled_quantity" in payload["order_states"][0]["suspicious_reasons"]
    assert "missing_remaining_quantity" in payload["order_states"][0]["suspicious_reasons"]


def test_pending_cancel_paper_test_order_is_quarantined(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                symbol="MES",
                local_symbol="MESM6",
                action="BUY",
                order_id=2,
                client_id=9088,
                perm_id=1773955119,
                status="PendingCancel",
                order_ref="TRACK_B_API_LIFECYCLE_TEST_20260606T062305Z_MESM6_BUY_REST_CANCEL",
                filled_quantity="1.7976931348623157e+308",
                remaining_quantity=None,
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == PAPER_TEST_ORDER_PENDING_CANCEL_QUARANTINED
    assert payload["summary"]["quarantined_test_order_count"] == 1
    assert payload["summary"]["strategy_submit_allowed"] is False
    assert payload["summary"]["test_harness_allowed"] is True
    state = payload["order_states"][0]
    assert state["quarantined"] is True
    assert state["suspicious"] is False
    assert "sentinel_filled_quantity" in state["quarantine_evidence"]["tolerated_status_gaps"]
    assert "missing_remaining_quantity" in state["quarantine_evidence"]["tolerated_status_gaps"]


def test_multiple_pending_cancel_paper_test_orders_are_quarantined(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                symbol="MES",
                local_symbol="MESM6",
                action="BUY",
                order_id=2,
                client_id=9088,
                status="PendingCancel",
                order_ref="TRACK_B_API_LIFECYCLE_TEST_20260606T062305Z_MESM6_BUY_REST_CANCEL",
            ),
            _order(
                symbol="MNQ",
                local_symbol="MNQM6",
                action="BUY",
                order_id=3,
                client_id=9089,
                status="PendingCancel",
                order_ref="TRACK_B_API_LIFECYCLE_TEST_20260606T062610Z_MNQM6_BUY_REST_CANCEL",
            ),
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == PAPER_TEST_ORDER_PENDING_CANCEL_QUARANTINED
    assert payload["summary"]["quarantined_test_order_count"] == 2


def test_filled_paper_test_order_is_not_quarantined(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                symbol="MES",
                local_symbol="MESM6",
                action="BUY",
                order_id=2,
                status="PendingCancel",
                order_ref="TRACK_B_API_LIFECYCLE_TEST_20260606T062305Z_MESM6_BUY_REST_CANCEL",
                filled_quantity="1",
                remaining_quantity="0",
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPEN_ENTRY_ORDER_WORKING
    assert payload["order_states"][0]["quarantine_evidence"]["blockers"] == ["filled_quantity_nonzero"]


def test_missing_order_ref_is_not_quarantined(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(symbol="MES", local_symbol="MESM6", action="BUY", order_id=2, status="PendingCancel")
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPEN_ENTRY_ORDER_WORKING
    assert "order_ref_not_paper_lifecycle_test" in payload["order_states"][0]["quarantine_evidence"]["blockers"]


def test_non_paper_account_test_order_is_not_quarantined(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                account_id="U1234567",
                symbol="MES",
                local_symbol="MESM6",
                action="BUY",
                order_id=2,
                status="PendingCancel",
                order_ref="TRACK_B_API_LIFECYCLE_TEST_20260606T062305Z_MESM6_BUY_REST_CANCEL",
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPEN_ENTRY_ORDER_WORKING
    assert "account_not_paper_test_account" in payload["order_states"][0]["quarantine_evidence"]["blockers"]


def test_broker_flat_with_open_close_order_is_classified(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        open_orders=[
            _order(
                symbol="MGC",
                local_symbol="MGCM6",
                action="SELL",
                order_id=4,
                intent_type="SELL_TO_CLOSE",
            )
        ],
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == BROKER_FLAT_WITH_OPEN_CLOSE_ORDER
    assert payload["summary"]["broker_flat_with_open_close_order_count"] == 1


def test_broker_position_without_close_order_is_classified(tmp_path: Path) -> None:
    _seed_reconciliation(tmp_path, broker_positions=[_position("MGC", "MGCM6", "1")])

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == NO_OPEN_ORDERS
    assert payload["summary"]["broker_position_without_close_order_count"] == 1
    assert payload["broker_positions_without_close_order"][0]["local_symbol"] == "MGCM6"


def test_marketable_sell_limit_unfilled_is_classified(tmp_path: Path) -> None:
    _seed_reconciliation(
        tmp_path,
        broker_positions=[_position("MNQ", "MNQM6", "1")],
        open_orders=[
            _order(
                symbol="MNQ",
                local_symbol="MNQM6",
                action="SELL",
                order_id=27,
                limit_price="29555.50",
                updated_at=(NOW - timedelta(seconds=120)).isoformat(),
            )
        ],
    )
    _write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "MNQ"
        / "1m"
        / "latest_runtime_candles.json",
        {"generated_at": NOW.isoformat(), "bars": [{"bar_end": NOW.isoformat(), "close": "29556.00"}]},
    )

    payload = build_track_b_open_order_truth(config=TrackBOpenOrderTruthConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == CLOSE_ORDER_MARKETABLE_NOT_FILLED
    assert payload["order_states"][0]["marketable"] is True
    assert "marketable_unfilled_beyond_threshold" in payload["order_states"][0]["condition_flags"]


def test_authority_event_log_and_dashboard_projection_paths(tmp_path: Path) -> None:
    config = TrackBOpenOrderTruthConfig(repo_root=tmp_path)
    _seed_reconciliation(tmp_path)
    payload = build_track_b_open_order_truth(config=config, now=NOW)

    authority_path, events = write_track_b_open_order_truth(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
    assert config.resolve(config.event_log_path) == (
        tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "open_order_truth_events.jsonl"
    )
    assert authority_path.exists()
    assert projection_path.exists()
    assert events
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert projection["authority_owner"] == "execution_core"


def test_critical_paths_do_not_consume_dashboard_projection_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json"
    critical_paths = [
        repo_root / "src/mgc_v05l/app/probationary_runtime.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_runtime_environment_truth.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_position_truth_monitor.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_paper_broker_reconciliation.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_broker_truth_lease.py",
    ]

    offenders = [str(path) for path in critical_paths if forbidden in path.read_text(encoding="utf-8")]

    assert offenders == []


def _seed_reconciliation(
    root: Path,
    *,
    broker_positions: list[dict] | None = None,
    open_orders: list[dict] | None = None,
    unknown_open_orders: list[dict] | None = None,
) -> None:
    _write_json(
        _reconciliation_path(root),
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": NOW.isoformat(),
            "broker_reconciled": not bool(open_orders),
            "symbols": ["MGC", "MNQ"],
            "track_b_broker_positions": broker_positions or [],
            "track_b_broker_open_orders": open_orders or [],
            "unknown_broker_open_orders": unknown_open_orders or [],
            "known_managed_exit_orders": [],
            "track_b_lifecycle_positions": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_open_order_count": len(open_orders or []),
            "track_b_broker_position_count": len(broker_positions or []),
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {"summary": {"overall_classification": "CLEAN_FLAT_READY"}, "generated_at": NOW.isoformat()},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json",
        {"open_position_count": len(broker_positions or []), "review_required_positions": [], "generated_at": NOW.isoformat()},
    )


def _position(symbol: str, local_symbol: str, quantity: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": symbol,
        "track_b_root": symbol,
        "local_symbol": local_symbol,
        "quantity": quantity,
        "average_cost": "100.0",
    }


def _order(
    *,
    account_id: str = "DUM882026",
    symbol: str,
    local_symbol: str,
    action: str,
    order_id: int,
    client_id: int = 17102,
    perm_id: int = 347000001,
    limit_price: str = "100.0",
    filled_quantity: str = "0",
    remaining_quantity: str | None = "1",
    status: str = "Submitted",
    order_ref: str | None = None,
    updated_at: str | None = None,
    intent_type: str | None = None,
) -> dict:
    row = {
        "account_id": account_id,
        "symbol": symbol,
        "track_b_root": symbol,
        "local_symbol": local_symbol,
        "broker_order_id": order_id,
        "client_id": client_id,
        "perm_id": perm_id,
        "action": action,
        "quantity": "1",
        "filled_quantity": filled_quantity,
        "remaining_quantity": remaining_quantity,
        "order_type": "LMT",
        "limit_price": limit_price,
        "status": status,
        "updated_at": updated_at or NOW.isoformat(),
    }
    if order_ref is not None:
        row["order_ref"] = order_ref
    if intent_type:
        row["intent_type"] = intent_type
    return row


def _reconciliation_path(root: Path) -> Path:
    return (
        root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
